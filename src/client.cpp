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
    Buffer    send_buf;
    Buffer    recv_buf;

    ServerConn(Network &net, const EfaAddress &server_efa)
        : send_buf(Buffer::Alloc(kMsgBufSize)),
          recv_buf(Buffer::Alloc(kMsgBufSize)) {
        addr = net.AddPeer(server_efa);
        net.RegMem(send_buf);
        net.RegMem(recv_buf);
    }
};

// ── ErasureClient ─────────────────────────────────────────────────────────────

struct ErasureClient {
    Network             &net;
    int                  k, m;
    ErasureCoder         ec;
    std::vector<ServerConn> servers;  // one per shard (k+m total)

    ErasureClient(Network &net, int k, int m,
                  const std::vector<EfaAddress> &addrs)
        : net(net), k(k), m(m), ec(k, m) {
        CHECK((int)addrs.size() == k + m);
        for (auto &a : addrs) servers.emplace_back(net, a);
    }

    // Send our EFA address to every server so they can reply
    void Connect() {
        for (int i = 0; i < k + m; i++) {
            auto *msg  = (ConnectMsg *)servers[i].send_buf.data;
            msg->type  = MsgType::kConnect;
            memcpy(msg->addr, net.addr.bytes, 32);

            bool done = false;
            net.PostSend(servers[i].addr, servers[i].send_buf,
                         connect_msg_bytes(),
                         [&done](Network &, RdmaOp &) { done = true; });
            while (!done) net.Poll();
        }
        printf("Connected to %d servers (k=%d m=%d).\n", k + m, k, m);
    }

    // PUT: encode object → send k+m shards in parallel → wait for all acks
    bool Put(const std::string &key, const uint8_t *data, size_t data_len) {
        auto shards = ec.Encode(data, data_len);

        // Post all sends
        int inflight = k + m;
        for (int i = 0; i < k + m; i++) {
            auto &srv = servers[i];
            CHECK(shard_request_bytes(key.size(), shards[i].size()) <= srv.send_buf.size);

            auto *hdr       = (ShardRequestHeader *)srv.send_buf.data;
            hdr->type       = MsgType::kPutShard;
            hdr->key_len    = (uint32_t)key.size();
            hdr->shard_len  = (uint32_t)shards[i].size();
            hdr->object_size= (uint32_t)data_len;
            hdr->shard_idx  = (uint8_t)i;
            hdr->k          = (uint8_t)k;
            hdr->m          = (uint8_t)m;
            hdr->_pad       = 0;
            memcpy((char *)srv.send_buf.data + sizeof(ShardRequestHeader),
                   key.data(), key.size());
            memcpy((char *)srv.send_buf.data + sizeof(ShardRequestHeader) + key.size(),
                   shards[i].data(), shards[i].size());

            net.PostSend(srv.addr, srv.send_buf,
                         shard_request_bytes(key.size(), shards[i].size()),
                         [](Network &, RdmaOp &) {});
        }
        // Wait for all sends to be posted (Progress() handles EAGAIN)
        while (!net.pending.empty()) net.Poll();

        // Post all recvs for acks, then wait
        for (int i = 0; i < k + m; i++) {
            net.PostRecv(servers[i].recv_buf,
                         [&inflight](Network &, RdmaOp &) { --inflight; });
        }
        while (inflight > 0) net.Poll();

        return true;
    }

    // GET: request k data shards in parallel → decode → return object
    std::vector<uint8_t> Get(const std::string &key) {
        // Request shards from the k data servers (indices 0..k-1)
        int inflight = k;
        std::vector<std::vector<uint8_t>> shards(k + m);
        std::vector<bool>                 present(k + m, false);
        uint32_t object_size = 0;

        for (int i = 0; i < k; i++) {
            auto &srv = servers[i];
            auto *hdr       = (ShardRequestHeader *)srv.send_buf.data;
            hdr->type       = MsgType::kGetShard;
            hdr->key_len    = (uint32_t)key.size();
            hdr->shard_len  = 0;
            hdr->object_size= 0;
            hdr->shard_idx  = (uint8_t)i;
            hdr->k          = (uint8_t)k;
            hdr->m          = (uint8_t)m;
            hdr->_pad       = 0;
            memcpy((char *)srv.send_buf.data + sizeof(ShardRequestHeader),
                   key.data(), key.size());

            net.PostSend(srv.addr, srv.send_buf,
                         shard_request_bytes(key.size(), 0),
                         [](Network &, RdmaOp &) {});
        }
        while (!net.pending.empty()) net.Poll();

        // Post recvs; fill shards[] by shard_idx from response header
        for (int i = 0; i < k; i++) {
            net.PostRecv(servers[i].recv_buf,
                         [&](Network &, RdmaOp &op) {
                auto *resp = (const ResponseHeader *)op.buf->data;
                CHECK(resp->status == StatusCode::kOk);
                int idx = resp->shard_idx;
                object_size = resp->object_size;
                const uint8_t *sd = (const uint8_t *)op.buf->data + sizeof(ResponseHeader);
                shards[idx] = std::vector<uint8_t>(sd, sd + resp->shard_len);
                present[idx] = true;
                --inflight;
            });
        }
        while (inflight > 0) net.Poll();

        return ec.Decode(shards, present, object_size);
    }

    // DELETE: send to all k+m servers in parallel
    void Delete(const std::string &key) {
        int inflight = k + m;
        for (int i = 0; i < k + m; i++) {
            auto &srv = servers[i];
            auto *hdr       = (ShardRequestHeader *)srv.send_buf.data;
            hdr->type       = MsgType::kDelShard;
            hdr->key_len    = (uint32_t)key.size();
            hdr->shard_len  = 0;
            hdr->object_size= 0;
            hdr->shard_idx  = (uint8_t)i;
            hdr->k          = (uint8_t)k;
            hdr->m          = (uint8_t)m;
            hdr->_pad       = 0;
            memcpy((char *)srv.send_buf.data + sizeof(ShardRequestHeader),
                   key.data(), key.size());

            net.PostSend(srv.addr, srv.send_buf,
                         shard_request_bytes(key.size(), 0),
                         [](Network &, RdmaOp &) {});
        }
        while (!net.pending.empty()) net.Poll();

        for (int i = 0; i < k + m; i++) {
            net.PostRecv(servers[i].recv_buf,
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
    // Pre-populate
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
        "  n_servers: determines k+m.  k = n_servers-1, m = 1.\n"
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

    // Parse: collect all 64-char hex addresses, then mode/num_ops/obj_bytes
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

    int n  = (int)addrs.size();
    int k  = (n > 1) ? n - 1 : 1;
    int m  = (n > 1) ? 1 : 0;

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
