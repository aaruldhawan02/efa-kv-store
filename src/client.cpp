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

// ── Per-server connection ──────────────────────────────────────────────────────

struct ServerConn {
    fi_addr_t addr;
    Buffer    ctrl_send;   // control messages (send/recv)
    Buffer    ctrl_recv;
    Buffer    data_buf;    // local RMA buffer (RDMA write source / read dest)

    ServerConn(Network &net, const EfaAddress &server_efa)
        : ctrl_send(Buffer::Alloc(kCtrlBufSize)),
          ctrl_recv(Buffer::Alloc(kCtrlBufSize)),
          data_buf(Buffer::Alloc(kDataBufSize)) {
        addr = net.AddPeer(server_efa);
        net.RegMem(ctrl_send);
        net.RegMem(ctrl_recv);
        net.RegMemRma(data_buf);
    }
};

// ── ErasureClient ─────────────────────────────────────────────────────────────

struct ErasureClient {
    Network             &net;
    int                  k, m;
    ErasureCoder         ec;
    std::vector<ServerConn> servers;

    ErasureClient(Network &net, int k, int m,
                  const std::vector<EfaAddress> &addrs)
        : net(net), k(k), m(m), ec(k, m) {
        CHECK((int)addrs.size() == k + m);
        for (auto &a : addrs) servers.emplace_back(net, a);
    }

    void Connect() {
        for (int i = 0; i < k + m; i++) {
            auto *msg  = (ConnectMsg *)servers[i].ctrl_send.data;
            msg->type  = MsgType::kConnect;
            memcpy(msg->addr, net.addr.bytes, 32);

            bool done = false;
            net.PostSend(servers[i].addr, servers[i].ctrl_send,
                         sizeof(ConnectMsg),
                         [&done](Network &, RdmaOp &) { done = true; });
            while (!done) net.Poll();
        }
        printf("Connected to %d servers (k=%d m=%d).\n", k + m, k, m);
    }

    // PUT: encode → request slots → RDMA write shards → commit → wait acks
    bool Put(const std::string &key, const uint8_t *data, size_t data_len) {
        auto shards = ec.Encode(data, data_len);

        // Phase 1: send PutRequest to all k+m servers in parallel
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
            net.PostSend(srv.addr, srv.ctrl_send,
                         sizeof(PutRequestMsg) + key.size(),
                         [](Network &, RdmaOp &) {});
        }
        while (!net.pending.empty()) net.Poll();

        // Phase 2: receive SlotGrants from all servers
        int inflight = k + m;
        std::vector<SlotGrantMsg> grants(k + m);
        for (int i = 0; i < k + m; i++) {
            int idx = i;
            net.PostRecv(servers[i].ctrl_recv,
                         [&, idx](Network &, RdmaOp &op) {
                memcpy(&grants[idx], op.buf->data, sizeof(SlotGrantMsg));
                --inflight;
            });
        }
        while (inflight > 0) net.Poll();

        // Phase 3: RDMA write shard data directly into each server's slot
        inflight = k + m;
        for (int i = 0; i < k + m; i++) {
            auto &srv = servers[i];
            CHECK(shards[i].size() <= srv.data_buf.size);
            memcpy(srv.data_buf.data, shards[i].data(), shards[i].size());
            net.PostWrite(srv.addr, srv.data_buf, shards[i].size(),
                          grants[i].remote_addr, grants[i].rkey,
                          [&inflight](Network &, RdmaOp &) { --inflight; });
        }
        while (inflight > 0) net.Poll();  // wait for all writes before committing

        // Phase 4: send PutCommit to all servers (writes are done)
        for (int i = 0; i < k + m; i++) {
            auto &srv       = servers[i];
            auto *commit    = (PutCommitMsg *)srv.ctrl_send.data;
            commit->type    = MsgType::kPutCommit;
            memset(commit->_pad, 0, sizeof(commit->_pad));
            commit->slot_idx = grants[i].slot_idx;
            net.PostSend(srv.addr, srv.ctrl_send, sizeof(PutCommitMsg),
                         [](Network &, RdmaOp &) {});
        }
        while (!net.pending.empty()) net.Poll();

        // Phase 5: receive Acks
        inflight = k + m;
        for (int i = 0; i < k + m; i++) {
            net.PostRecv(servers[i].ctrl_recv,
                         [&inflight](Network &, RdmaOp &) { --inflight; });
        }
        while (inflight > 0) net.Poll();

        return true;
    }

    // GET: request slot info → RDMA read shard data → decode
    std::vector<uint8_t> Get(const std::string &key) {
        // Phase 1: send GetRequest to k data servers in parallel
        for (int i = 0; i < k; i++) {
            auto &srv    = servers[i];
            auto *req    = (GetRequestMsg *)srv.ctrl_send.data;
            req->type    = MsgType::kGetRequest;
            req->shard_idx = (uint8_t)i;
            req->_pad[0] = req->_pad[1] = 0;
            req->key_len = (uint32_t)key.size();
            memcpy((char *)srv.ctrl_send.data + sizeof(GetRequestMsg),
                   key.data(), key.size());
            net.PostSend(srv.addr, srv.ctrl_send,
                         sizeof(GetRequestMsg) + key.size(),
                         [](Network &, RdmaOp &) {});
        }
        while (!net.pending.empty()) net.Poll();

        // Phase 2: receive SlotInfo from each server
        int inflight = k;
        std::vector<SlotInfoMsg> infos(k + m);
        for (int i = 0; i < k; i++) {
            int idx = i;
            net.PostRecv(servers[i].ctrl_recv,
                         [&, idx](Network &, RdmaOp &op) {
                memcpy(&infos[idx], op.buf->data, sizeof(SlotInfoMsg));
                --inflight;
            });
        }
        while (inflight > 0) net.Poll();

        // Phase 3: RDMA read shard data directly from each server's slot
        std::vector<std::vector<uint8_t>> shards(k + m);
        std::vector<bool>                 present(k + m, false);
        uint32_t object_size = 0;
        inflight = k;

        for (int i = 0; i < k; i++) {
            CHECK(infos[i].status == StatusCode::kOk);
            int      idx       = i;
            uint32_t shard_len = infos[i].shard_len;
            net.PostRead(servers[i].addr, servers[i].data_buf, shard_len,
                         infos[i].remote_addr, infos[i].rkey,
                         [&, idx, shard_len](Network &, RdmaOp &op) {
                int sidx = infos[idx].shard_idx;
                object_size = infos[idx].object_size;
                const uint8_t *sd = (const uint8_t *)op.buf->data;
                shards[sidx] = std::vector<uint8_t>(sd, sd + shard_len);
                present[sidx] = true;
                --inflight;
            });
        }
        while (inflight > 0) net.Poll();

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
            net.PostSend(srv.addr, srv.ctrl_send,
                         sizeof(DelRequestMsg) + key.size(),
                         [](Network &, RdmaOp &) {});
        }
        while (!net.pending.empty()) net.Poll();

        int inflight = k + m;
        for (int i = 0; i < k + m; i++) {
            net.PostRecv(servers[i].ctrl_recv,
                         [&inflight](Network &, RdmaOp &) { --inflight; });
        }
        while (inflight > 0) net.Poll();
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
        "Usage: %s <addr0> [<addr1> ...] <mode> <num_ops> <obj_bytes>\n"
        "\n"
        "  Addresses are 64-char hex EFA addresses (one per server).\n"
        "  n_servers determines k+m.  k = n_servers-1, m = 1.\n"
        "  For 1 server: k=1, m=0 (no erasure).  For 3 servers: k=2, m=1.\n"
        "\n"
        "  mode: put | get | bench\n"
        "\n"
        "Example:\n"
        "  %s <addr0> <addr1> <addr2> bench 1000 65536\n",
        prog, prog);
    std::exit(1);
}

int main(int argc, char **argv) {
    if (argc < 5) usage(argv[0]);

    std::vector<EfaAddress> addrs;
    int i = 1;
    while (i < argc && strlen(argv[i]) == 64) {
        addrs.push_back(EfaAddress::Parse(argv[i]));
        i++;
    }
    if (addrs.empty() || i + 2 >= argc) usage(argv[0]);

    std::string mode      = argv[i++];
    size_t      num_ops   = std::stoull(argv[i++]);
    size_t      obj_bytes = std::stoull(argv[i]);

    int n = (int)addrs.size();
    int k = (n > 1) ? n - 1 : 1;
    int m = (n > 1) ? 1 : 0;

    if (obj_bytes == 0 || obj_bytes > (size_t)k * kMaxShardSize) {
        fprintf(stderr, "obj_bytes must be 1..%zu\n", (size_t)k * kMaxShardSize);
        std::exit(1);
    }

    auto net = Network::Open();
    ErasureClient c(net, k, m, addrs);
    c.Connect();

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
