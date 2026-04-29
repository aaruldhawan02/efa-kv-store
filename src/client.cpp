#include "common.hpp"
#include "ec.hpp"
#include "protocol.hpp"
#include <algorithm>
#include <chrono>
#include <cstdio>
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
        // Pre-fault all registered pages so first real op doesn't pay page-fault cost.
        memset(ctrl_send.data, 0, ctrl_send.size);
        memset(ctrl_recv.data, 0, ctrl_recv.size);
        memset(data_buf.data,  0, data_buf.size);
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
    // GET: contact all k+m servers, use any k that have the shard.
    // Tolerates up to m unavailable servers (degraded read).
    std::vector<uint8_t> Get(const std::string &key) {
        // Phase 1: send GetRequest to ALL k+m servers in parallel
        for (int i = 0; i < k + m; i++) {
            auto &srv      = servers[i];
            auto *req      = (GetRequestMsg *)srv.ctrl_send.data;
            req->type      = MsgType::kGetRequest;
            req->shard_idx = (uint8_t)i;
            req->_pad[0]   = req->_pad[1] = 0;
            req->key_len   = (uint32_t)key.size();
            memcpy((char *)srv.ctrl_send.data + sizeof(GetRequestMsg),
                   key.data(), key.size());
            srv.conn.PostSend(srv.ctrl_send,
                              sizeof(GetRequestMsg) + key.size(),
                              [](Connection &, RdmaOp &) {});
        }
        for (auto &srv : servers)
            while (!srv.conn.pending.empty()) srv.conn.Poll();

        // Phase 2: receive SlotInfo from ALL k+m servers
        int inflight = k + m;
        std::vector<SlotInfoMsg> infos(k + m);
        for (int i = 0; i < k + m; i++) {
            int idx = i;
            servers[i].conn.PostRecv(servers[i].ctrl_recv,
                [&, idx](Connection &, RdmaOp &op) {
                    memcpy(&infos[idx], op.buf->data, sizeof(SlotInfoMsg));
                    --inflight;
                });
        }
        while (inflight > 0)
            for (auto &srv : servers) srv.conn.Poll();

        // Phase 3: pick k available shards — data shards preferred,
        // fall back to parity shards for degraded reads.
        std::vector<int> to_read;
        to_read.reserve(k);
        for (int i = 0; i < k + m && (int)to_read.size() < k; i++) {
            if (infos[i].status == StatusCode::kOk)
                to_read.push_back(i);
        }
        if ((int)to_read.size() < k) {
            fprintf(stderr, "GET '%s' failed: only %zu/%d shards available\n",
                    key.c_str(), to_read.size(), k);
            return {};
        }

        bool degraded = false;
        for (int i : to_read) {
            if (infos[i].shard_idx >= (uint8_t)k) { degraded = true; break; }
        }
        if (degraded)
            fprintf(stderr, "degraded read for '%s' (using parity shards)\n",
                    key.c_str());

        // Phase 4: RDMA read from the chosen k servers in parallel
        std::vector<std::vector<uint8_t>> shards(k + m);
        std::vector<bool>                 present(k + m, false);
        uint32_t object_size = 0;
        inflight = k;

        for (int i : to_read) {
            uint32_t shard_len = infos[i].shard_len;
            servers[i].conn.PostRead(servers[i].data_buf, shard_len,
                infos[i].remote_addr, infos[i].rkey,
                [&, i, shard_len](Connection &, RdmaOp &op) {
                    int sidx = infos[i].shard_idx;
                    object_size = infos[i].object_size;
                    const uint8_t *sd = (const uint8_t *)op.buf->data;
                    shards[sidx] = std::vector<uint8_t>(sd, sd + shard_len);
                    present[sidx] = true;
                    --inflight;
                });
        }
        while (inflight > 0)
            for (int i : to_read) servers[i].conn.Poll();

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

// ── File helpers ──────────────────────────────────────────────────────────────

static std::vector<uint8_t> read_file(const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) { perror(path); std::exit(1); }
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    rewind(f);
    std::vector<uint8_t> buf(sz);
    if (fread(buf.data(), 1, sz, f) != (size_t)sz) {
        fprintf(stderr, "read error: %s\n", path); std::exit(1);
    }
    fclose(f);
    return buf;
}

static void write_file(const char *path, const std::vector<uint8_t> &data) {
    FILE *f = fopen(path, "wb");
    if (!f) { perror(path); std::exit(1); }
    if (fwrite(data.data(), 1, data.size(), f) != data.size()) {
        fprintf(stderr, "write error: %s\n", path); std::exit(1);
    }
    fclose(f);
}

static void usage(const char *prog) {
    fprintf(stderr,
        "Usage: %s <addr0> [<addr1> ...] [-k <k>] <mode> [args...]\n"
        "\n"
        "  Addresses are hex fabric addresses printed by each server.\n"
        "  n_servers = k + m.  Default: k = n-1, m = 1.\n"
        "  Use -k to set k explicitly; m = n_servers - k.\n"
        "\n"
        "  Modes:\n"
        "    putfile <key> <file>        store a file\n"
        "    getfile <key> <outfile>     retrieve a file\n"
        "    delkey  <key>               delete a key\n"
        "    put   <num_ops> <obj_bytes> benchmark puts\n"
        "    get   <num_ops> <obj_bytes> benchmark gets (pre-populates)\n"
        "    bench <num_ops> <obj_bytes> benchmark put then get\n"
        "\n"
        "Examples:\n"
        "  %s <a0>..<a5> -k 4 putfile mykey photo.jpg\n"
        "  %s <a0>..<a5> -k 4 getfile mykey out.jpg\n"
        "  %s <a0>..<a5> -k 4 bench 10000 65536\n",
        prog, prog, prog, prog);
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

    if (i >= argc) usage(argv[0]);
    std::string mode = argv[i++];

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

    auto net = Network::Open();
    ErasureClient c(net, k, m, addrs);

    if (mode == "putfile") {
        if (i + 1 >= argc) usage(argv[0]);
        std::string key  = argv[i++];
        const char *path = argv[i];
        auto data = read_file(path);
        if (data.empty() || data.size() > (size_t)k * kMaxShardSize) {
            fprintf(stderr, "file size must be 1..%zu bytes\n",
                    (size_t)k * kMaxShardSize);
            std::exit(1);
        }
        auto t0 = Clock::now();
        bool ok = c.Put(key, data.data(), data.size());
        double us = (Clock::now() - t0).count() / 1e3;
        if (ok) printf("PUT '%s' (%zu bytes) in %.1f us\n", key.c_str(), data.size(), us);
        else    { fprintf(stderr, "PUT failed\n"); std::exit(1); }

    } else if (mode == "getfile") {
        if (i + 1 >= argc) usage(argv[0]);
        std::string key     = argv[i++];
        const char *outpath = argv[i];
        auto t0 = Clock::now();
        auto data = c.Get(key);
        double us = (Clock::now() - t0).count() / 1e3;
        if (data.empty()) { fprintf(stderr, "GET failed or key not found\n"); std::exit(1); }
        write_file(outpath, data);
        printf("GET '%s' (%zu bytes) in %.1f us → %s\n",
               key.c_str(), data.size(), us, outpath);

    } else if (mode == "delkey") {
        if (i >= argc) usage(argv[0]);
        std::string key = argv[i];
        c.Delete(key);
        printf("DEL '%s'\n", key.c_str());

    } else if (mode == "put" || mode == "get" || mode == "bench") {
        if (i + 1 >= argc) usage(argv[0]);
        size_t num_ops   = std::stoull(argv[i++]);
        size_t obj_bytes = std::stoull(argv[i]);
        if (obj_bytes == 0 || obj_bytes > (size_t)k * kMaxShardSize) {
            fprintf(stderr, "obj_bytes must be 1..%zu\n", (size_t)k * kMaxShardSize);
            std::exit(1);
        }
        if (mode == "put")        bench_put(c, num_ops, obj_bytes);
        else if (mode == "get")   bench_get(c, num_ops, obj_bytes);
        else { bench_put(c, num_ops, obj_bytes); bench_get(c, num_ops, obj_bytes); }

    } else {
        usage(argv[0]);
    }

    return 0;
}
