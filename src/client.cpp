#include "common.hpp"
#include "ec.hpp"
#include "protocol.hpp"
#include <algorithm>
#include <chrono>
#include <numeric>
#include <random>
#include <string>
#include <vector>

using Clock = std::chrono::high_resolution_clock;

// ── Latency stats ─────────────────────────────────────────────────────────────

static void print_latency(std::vector<long long> &ns, const char *label,
                           size_t object_bytes) {
    std::sort(ns.begin(), ns.end());
    size_t n   = ns.size();
    double avg = (double)std::accumulate(ns.begin(), ns.end(), 0LL) / n;
    double p50 = ns[n * 50 / 100] / 1e3;
    double p99 = ns[n * 99 / 100] / 1e3;
    double p999= ns[std::min(n-1, n*999/1000)] / 1e3;
    double bw  = (double)object_bytes / (avg / 1e9) / (1024.0 * 1024.0);
    printf("%-6s lat(us): avg=%.1f  p50=%.1f  p99=%.1f  p999=%.1f  "
           "throughput=%.0f ops/s  %.1f MB/s\n",
           label, avg/1e3, p50, p99, p999,
           1e9 / avg, bw);
}

// ── Per-server connection ─────────────────────────────────────────────────────

struct ServerConn {
    Buffer     ctrl_send;
    Buffer     ctrl_recv;
    Buffer     data_buf;
    Connection conn;

    ServerConn(Network &net, const FabricAddress &server_addr)
        : ctrl_send(Buffer::Alloc(kCtrlBufSize)),
          ctrl_recv(Buffer::Alloc(kCtrlBufSize)),
          data_buf(Buffer::Alloc(kDataBufSize)),
          conn(net.Connect(server_addr)) {
        conn.RegMem(ctrl_send);
        conn.RegMem(ctrl_recv);
        conn.RegMemRma(data_buf);
    }
};

// ── ErasureClient ─────────────────────────────────────────────────────────────

struct ErasureClient {
    int   k, m;
    ErasureCoder ec;
    std::vector<ServerConn> servers;

    ErasureClient(Network &net, int k, int m,
                  const std::vector<FabricAddress> &addrs)
        : k(k), m(m), ec(k, m) {
        CHECK((int)addrs.size() == k + m);
        for (auto &a : addrs) servers.emplace_back(net, a);
        printf("Connected to %d servers (k=%d m=%d).\n", k + m, k, m);
    }

    // PUT: encode → request slots → RDMA write shards → commit → wait acks
    bool Put(const std::string &key, const uint8_t *data, size_t data_len) {
        auto shards = ec.Encode(data, data_len);

        // Phase 1: send PutRequest to all k+m servers
        for (int i = 0; i < k + m; i++) {
            auto &srv = servers[i];
            auto *req       = (PutRequestMsg *)srv.ctrl_send.data;
            req->type       = MsgType::kPutRequest;
            memset(req->_pad, 0, sizeof(req->_pad));
            req->key_len    = (uint32_t)key.size();
            req->shard_len  = (uint32_t)shards[i].size();
            req->object_size= (uint32_t)data_len;
            req->shard_idx  = (uint8_t)i;
            req->k          = (uint8_t)k;
            req->m          = (uint8_t)m;
            req->_pad2      = 0;
            memcpy((char *)srv.ctrl_send.data + sizeof(PutRequestMsg),
                   key.data(), key.size());
            srv.conn.PostSend(srv.ctrl_send,
                              sizeof(PutRequestMsg) + key.size(),
                              [](Connection &, RdmaOp &) {});
        }
        for (auto &srv : servers)
            while (!srv.conn.pending.empty()) srv.conn.Poll();

        // Phase 2: receive SlotGrants from all servers
        int inflight = k + m;
        std::vector<SlotGrantMsg> grants(k + m);
        for (int i = 0; i < k + m; i++) {
            int idx = i;
            servers[i].conn.PostRecv(servers[i].ctrl_recv,
                [&, idx](Connection &, RdmaOp &op) {
                    memcpy(&grants[idx], op.buf->data, sizeof(SlotGrantMsg));
                    --inflight;
                });
        }
        while (inflight > 0)
            for (auto &srv : servers) srv.conn.Poll();

        // Phase 3: RDMA write shard data into each server's slot
        inflight = k + m;
        for (int i = 0; i < k + m; i++) {
            auto &srv = servers[i];
            CHECK(shards[i].size() <= srv.data_buf.size);
            memcpy(srv.data_buf.data, shards[i].data(), shards[i].size());
            srv.conn.PostWrite(srv.data_buf, shards[i].size(),
                               grants[i].remote_addr, grants[i].rkey,
                               [&inflight](Connection &, RdmaOp &) { --inflight; });
        }
        while (inflight > 0)
            for (auto &srv : servers) srv.conn.Poll();

        // Phase 4: send PutCommit to all servers
        for (int i = 0; i < k + m; i++) {
            auto &srv       = servers[i];
            auto *commit    = (PutCommitMsg *)srv.ctrl_send.data;
            commit->type    = MsgType::kPutCommit;
            memset(commit->_pad, 0, sizeof(commit->_pad));
            commit->slot_idx = grants[i].slot_idx;
            srv.conn.PostSend(srv.ctrl_send, sizeof(PutCommitMsg),
                              [](Connection &, RdmaOp &) {});
        }
        for (auto &srv : servers)
            while (!srv.conn.pending.empty()) srv.conn.Poll();

        // Phase 5: receive Acks
        inflight = k + m;
        for (int i = 0; i < k + m; i++) {
            servers[i].conn.PostRecv(servers[i].ctrl_recv,
                [&inflight](Connection &, RdmaOp &) { --inflight; });
        }
        while (inflight > 0)
            for (auto &srv : servers) srv.conn.Poll();

        return true;
    }

    // GET: request slot info → RDMA read shard data → decode
    std::vector<uint8_t> Get(const std::string &key) {
        // Phase 1: send GetRequest to k data servers
        for (int i = 0; i < k; i++) {
            auto &srv    = servers[i];
            auto *req    = (GetRequestMsg *)srv.ctrl_send.data;
            req->type    = MsgType::kGetRequest;
            req->shard_idx = (uint8_t)i;
            req->_pad[0] = req->_pad[1] = 0;
            req->key_len = (uint32_t)key.size();
            memcpy((char *)srv.ctrl_send.data + sizeof(GetRequestMsg),
                   key.data(), key.size());
            srv.conn.PostSend(srv.ctrl_send,
                              sizeof(GetRequestMsg) + key.size(),
                              [](Connection &, RdmaOp &) {});
        }
        for (int i = 0; i < k; i++)
            while (!servers[i].conn.pending.empty()) servers[i].conn.Poll();

        // Phase 2: receive SlotInfo from each server
        int inflight = k;
        std::vector<SlotInfoMsg> infos(k + m);
        for (int i = 0; i < k; i++) {
            int idx = i;
            servers[i].conn.PostRecv(servers[i].ctrl_recv,
                [&, idx](Connection &, RdmaOp &op) {
                    memcpy(&infos[idx], op.buf->data, sizeof(SlotInfoMsg));
                    --inflight;
                });
        }
        while (inflight > 0)
            for (int i = 0; i < k; i++) servers[i].conn.Poll();

        // Phase 3: RDMA read shard data from each server's slot
        std::vector<std::vector<uint8_t>> shards(k + m);
        std::vector<bool>                 present(k + m, false);
        uint32_t object_size = 0;
        inflight = k;

        for (int i = 0; i < k; i++) {
            CHECK(infos[i].status == StatusCode::kOk);
            int      idx       = i;
            uint32_t shard_len = infos[i].shard_len;
            servers[i].conn.PostRead(servers[i].data_buf, shard_len,
                infos[i].remote_addr, infos[i].rkey,
                [&, idx, shard_len](Connection &, RdmaOp &op) {
                    int sidx = infos[idx].shard_idx;
                    object_size = infos[idx].object_size;
                    const uint8_t *sd = (const uint8_t *)op.buf->data;
                    shards[sidx] = std::vector<uint8_t>(sd, sd + shard_len);
                    present[sidx] = true;
                    --inflight;
                });
        }
        while (inflight > 0)
            for (int i = 0; i < k; i++) servers[i].conn.Poll();

        return ec.Decode(shards, present, object_size);
    }

    // DELETE: send to all k+m servers, wait for acks
    void Delete(const std::string &key) {
        for (int i = 0; i < k + m; i++) {
            auto &srv    = servers[i];
            auto *req    = (DelRequestMsg *)srv.ctrl_send.data;
            req->type    = MsgType::kDelRequest;
            req->shard_idx = (uint8_t)i;
            req->_pad[0] = req->_pad[1] = 0;
            req->key_len = (uint32_t)key.size();
            memcpy((char *)srv.ctrl_send.data + sizeof(DelRequestMsg),
                   key.data(), key.size());
            srv.conn.PostSend(srv.ctrl_send,
                              sizeof(DelRequestMsg) + key.size(),
                              [](Connection &, RdmaOp &) {});
        }
        for (auto &srv : servers)
            while (!srv.conn.pending.empty()) srv.conn.Poll();

        int inflight = k + m;
        for (int i = 0; i < k + m; i++) {
            servers[i].conn.PostRecv(servers[i].ctrl_recv,
                [&inflight](Connection &, RdmaOp &) { --inflight; });
        }
        while (inflight > 0)
            for (auto &srv : servers) srv.conn.Poll();
    }
};

// ── Benchmarks ────────────────────────────────────────────────────────────────

static void bench_put(ErasureClient &c, size_t num_ops, size_t obj_size) {
    std::mt19937 rng(42);
    std::vector<uint8_t> data(obj_size);
    std::generate(data.begin(), data.end(), [&]{ return (uint8_t)(rng() & 0xff); });

    std::vector<long long> lats;
    lats.reserve(num_ops);

    for (size_t i = 0; i < num_ops; i++) {
        char key[64];
        snprintf(key, sizeof(key), "obj-%08zu", i);
        auto t0 = Clock::now();
        bool ok = c.Put(std::string(key), data.data(), obj_size);
        lats.push_back((Clock::now() - t0).count());
        CHECK(ok);
    }

    printf("\n=== PUT  n=%zu  obj=%zuB  k=%d  m=%d ===\n",
           num_ops, obj_size, c.k, c.m);
    print_latency(lats, "PUT", obj_size);
}

static void bench_get(ErasureClient &c, size_t num_ops, size_t obj_size) {
    std::vector<uint8_t> data(obj_size, 0xab);
    for (size_t i = 0; i < num_ops; i++) {
        char key[64]; snprintf(key, sizeof(key), "obj-%08zu", i);
        CHECK(c.Put(std::string(key), data.data(), obj_size));
    }

    std::vector<long long> lats;
    lats.reserve(num_ops);

    for (size_t i = 0; i < num_ops; i++) {
        char key[64]; snprintf(key, sizeof(key), "obj-%08zu", i);
        auto t0 = Clock::now();
        auto got = c.Get(std::string(key));
        lats.push_back((Clock::now() - t0).count());
        CHECK(got.size() == obj_size);
    }

    printf("\n=== GET  n=%zu  obj=%zuB  k=%d  m=%d ===\n",
           num_ops, obj_size, c.k, c.m);
    print_latency(lats, "GET", obj_size);
}

// ── main ──────────────────────────────────────────────────────────────────────

static void usage(const char *prog) {
    fprintf(stderr,
        "Usage: %s <addr0> [<addr1> ...] [-k <k>] <mode> <num_ops> <obj_bytes>\n"
        "\n"
        "  Addresses are hex fabric addresses printed by each server.\n"
        "  n_servers = k + m (total servers).  Default: k = n-1, m = 1.\n"
        "  Use -k to set k explicitly; m = n_servers - k.\n"
        "\n"
        "  mode: put | get | bench\n"
        "\n"
        "Examples:\n"
        "  %s <a0> <a1> <a2> bench 1000 65536          # k=2 m=1\n"
        "  %s <a0>..<a5> -k 4 bench 10000 65536        # k=4 m=2\n",
        prog, prog, prog);
    std::exit(1);
}

int main(int argc, char **argv) {
    if (argc < 5) usage(argv[0]);

    std::vector<FabricAddress> addrs;
    int i = 1;
    while (i < argc) {
        size_t l = strlen(argv[i]);
        if (l < 2 || l % 2 != 0 || l > 128) break;
        bool is_hex = true;
        for (size_t j = 0; j < l && is_hex; j++)
            is_hex = isxdigit((unsigned char)argv[i][j]);
        if (!is_hex) break;
        addrs.push_back(FabricAddress::Parse(argv[i]));
        i++;
    }
    if (addrs.empty()) usage(argv[0]);

    // Optional -k flag.
    int explicit_k = -1;
    if (i < argc && strcmp(argv[i], "-k") == 0) {
        i++;
        if (i >= argc) usage(argv[0]);
        explicit_k = std::stoi(argv[i++]);
    }

    if (i + 2 >= argc) usage(argv[0]);

    std::string mode      = argv[i++];
    size_t      num_ops   = std::stoull(argv[i++]);
    size_t      obj_bytes = std::stoull(argv[i]);

    int n = (int)addrs.size();
    int k, m;
    if (explicit_k > 0) {
        k = explicit_k;
        m = n - k;
        if (m < 0 || k + m != n) {
            fprintf(stderr, "ERROR: -k %d incompatible with %d servers\n", k, n);
            std::exit(1);
        }
    } else {
        k = (n > 1) ? n - 1 : 1;
        m = (n > 1) ? 1 : 0;
    }

    if (obj_bytes == 0 || obj_bytes > (size_t)k * kMaxShardSize) {
        fprintf(stderr, "obj_bytes must be 1..%zu\n", (size_t)k * kMaxShardSize);
        std::exit(1);
    }

    auto net = Network::Open();
    ErasureClient c(net, k, m, addrs);

    if (mode == "put") {
        bench_put(c, num_ops, obj_bytes);
    } else if (mode == "get") {
        bench_get(c, num_ops, obj_bytes);
    } else if (mode == "bench") {
        bench_put(c, num_ops, obj_bytes);
        bench_get(c, num_ops, obj_bytes);
    } else {
        usage(argv[0]);
    }

    return 0;
}
