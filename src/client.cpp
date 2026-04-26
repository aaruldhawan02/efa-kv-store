#include "common.hpp"
#include "protocol.hpp"
#include <algorithm>
#include <chrono>
#include <inttypes.h>
#include <numeric>
#include <random>
#include <string>
#include <vector>

using Clock    = std::chrono::high_resolution_clock;
using Duration = std::chrono::nanoseconds;

static double ns_to_us(long long ns) { return ns / 1000.0; }

static void print_latency(std::vector<long long> &samples, const char *label) {
    std::sort(samples.begin(), samples.end());
    size_t n   = samples.size();
    double avg = (double)std::accumulate(samples.begin(), samples.end(), 0LL) / n;
    printf("%-8s lat(us): avg=%.1f  p50=%.1f  p99=%.1f  p999=%.1f  min=%.1f  max=%.1f\n",
           label,
           ns_to_us(avg),
           ns_to_us(samples[n * 50 / 100]),
           ns_to_us(samples[n * 99 / 100]),
           ns_to_us(samples[std::min(n - 1, n * 999 / 1000)]),
           ns_to_us(samples.front()),
           ns_to_us(samples.back()));
}

struct Client {
    Network  &net;
    fi_addr_t server_addr;
    Buffer    send_buf;
    Buffer    recv_buf;

    Client(Network &net, fi_addr_t server_addr)
        : net(net), server_addr(server_addr),
          send_buf(Buffer::Alloc(kMsgBufSize)),
          recv_buf(Buffer::Alloc(kMsgBufSize)) {
        net.RegMem(send_buf);
        net.RegMem(recv_buf);
    }

    // Send our EFA address so server can reply to us
    void Connect() {
        auto *msg  = (ConnectMsg *)send_buf.data;
        msg->type  = MsgType::kConnect;
        memcpy(msg->addr, net.addr.bytes, 32);

        bool sent = false;
        net.PostSend(server_addr, send_buf, connect_msg_size(),
                     [&sent](Network &, RdmaOp &) { sent = true; });
        while (!sent) net.Poll();
        printf("Connected to server.\n");
    }

    // Returns response status and fills val_out/vlen_out for GET
    StatusCode DoRequest(MsgType type, const std::string &key,
                         const uint8_t *val, size_t vlen,
                         std::vector<uint8_t> *val_out = nullptr) {
        CHECK(request_size(key.size(), vlen) <= send_buf.size);

        auto *hdr    = (RequestHeader *)send_buf.data;
        hdr->type    = type;
        hdr->key_len = (uint32_t)key.size();
        hdr->val_len = (uint32_t)vlen;
        memcpy((char *)send_buf.data + sizeof(RequestHeader), key.data(), key.size());
        if (val && vlen > 0)
            memcpy((char *)send_buf.data + sizeof(RequestHeader) + key.size(), val, vlen);

        bool sent = false;
        net.PostSend(server_addr, send_buf, request_size(key.size(), vlen),
                     [&sent](Network &, RdmaOp &) { sent = true; });
        while (!sent) net.Poll();

        bool   got_resp = false;
        size_t resp_len = 0;
        net.PostRecv(recv_buf, [&](Network &, RdmaOp &op) {
            got_resp = true;
            resp_len = op.len;
        });
        while (!got_resp) net.Poll();

        CHECK(resp_len >= sizeof(ResponseHeader));
        auto *resp = (const ResponseHeader *)recv_buf.data;
        if (val_out && resp->status == StatusCode::kOk && resp->val_len > 0) {
            const uint8_t *rv = (const uint8_t *)recv_buf.data + sizeof(ResponseHeader);
            val_out->assign(rv, rv + resp->val_len);
        }
        return resp->status;
    }

    StatusCode Put(const std::string &key, const uint8_t *val, size_t vlen) {
        return DoRequest(MsgType::kPut, key, val, vlen);
    }
    StatusCode Get(const std::string &key, std::vector<uint8_t> *out = nullptr) {
        return DoRequest(MsgType::kGet, key, nullptr, 0, out);
    }
    StatusCode Delete(const std::string &key) {
        return DoRequest(MsgType::kDelete, key, nullptr, 0);
    }
};

static void bench_put(Client &c, size_t num_ops, size_t value_size) {
    std::mt19937 rng(42);
    std::vector<uint8_t> val(value_size);
    std::generate(val.begin(), val.end(), [&]{ return rng() & 0xff; });

    std::vector<long long> latencies;
    latencies.reserve(num_ops);

    auto wall_start = Clock::now();

    for (size_t i = 0; i < num_ops; i++) {
        char key_buf[64];
        snprintf(key_buf, sizeof(key_buf), "key-%08zu", i);
        std::string key(key_buf);

        auto t0 = Clock::now();
        auto st = c.Put(key, val.data(), val.size());
        auto t1 = Clock::now();

        CHECK(st == StatusCode::kOk);
        latencies.push_back(
            std::chrono::duration_cast<Duration>(t1 - t0).count());
    }

    auto wall_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
        Clock::now() - wall_start).count();

    double ops_per_sec  = num_ops * 1e6 / wall_elapsed;
    double mbps         = ops_per_sec * value_size / (1024.0 * 1024.0);

    printf("\n=== PUT benchmark: %zu ops, %zu B values ===\n", num_ops, value_size);
    printf("throughput: %.0f ops/s,  %.1f MB/s\n", ops_per_sec, mbps);
    print_latency(latencies, "PUT");
}

static void bench_get(Client &c, size_t num_ops, size_t value_size) {
    // Pre-populate
    std::vector<uint8_t> val(value_size, 0xab);
    for (size_t i = 0; i < num_ops; i++) {
        char key_buf[64];
        snprintf(key_buf, sizeof(key_buf), "key-%08zu", i);
        CHECK(c.Put(std::string(key_buf), val.data(), val.size()) == StatusCode::kOk);
    }
    printf("Pre-populated %zu keys.\n", num_ops);

    std::vector<long long> latencies;
    latencies.reserve(num_ops);

    auto wall_start = Clock::now();

    for (size_t i = 0; i < num_ops; i++) {
        char key_buf[64];
        snprintf(key_buf, sizeof(key_buf), "key-%08zu", i);

        auto t0 = Clock::now();
        auto st = c.Get(std::string(key_buf));
        auto t1 = Clock::now();

        CHECK(st == StatusCode::kOk);
        latencies.push_back(
            std::chrono::duration_cast<Duration>(t1 - t0).count());
    }

    auto wall_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
        Clock::now() - wall_start).count();

    double ops_per_sec = num_ops * 1e6 / wall_elapsed;
    double mbps        = ops_per_sec * value_size / (1024.0 * 1024.0);

    printf("\n=== GET benchmark: %zu ops, %zu B values ===\n", num_ops, value_size);
    printf("throughput: %.0f ops/s,  %.1f MB/s\n", ops_per_sec, mbps);
    print_latency(latencies, "GET");
}

static void usage(const char *prog) {
    fprintf(stderr,
            "Usage:\n"
            "  %s <server_addr> put  <num_ops> <value_bytes>\n"
            "  %s <server_addr> get  <num_ops> <value_bytes>\n"
            "  %s <server_addr> bench <num_ops> <value_bytes>   (put then get)\n",
            prog, prog, prog);
    std::exit(1);
}

int main(int argc, char **argv) {
    if (argc != 5) usage(argv[0]);

    std::string server_addr_str = argv[1];
    std::string mode            = argv[2];
    size_t      num_ops         = std::stoull(argv[3]);
    size_t      value_size      = std::stoull(argv[4]);

    if (value_size > kMaxValueSize) {
        fprintf(stderr, "value_size %zu exceeds max %zu\n", value_size, kMaxValueSize);
        std::exit(1);
    }

    auto net        = Network::Open();
    auto server_efa = EfaAddress::Parse(server_addr_str);
    auto server_fi  = net.AddPeer(server_efa);

    Client c(net, server_fi);
    c.Connect();

    if (mode == "put") {
        bench_put(c, num_ops, value_size);
    } else if (mode == "get") {
        bench_get(c, num_ops, value_size);
    } else if (mode == "bench") {
        bench_put(c, num_ops, value_size);
        bench_get(c, num_ops, value_size);
    } else {
        usage(argv[0]);
    }

    return 0;
}
