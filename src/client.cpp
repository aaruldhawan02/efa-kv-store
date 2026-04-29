#include "client_lib.hpp"
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
           label, avg/1e3, p50, p99, p999, 1e9 / avg, bw);
}

// ── Benchmarks ────────────────────────────────────────────────────────────────

static void bench_put(ErasureClient &c, size_t num_ops, size_t obj_size) {
    std::mt19937_64 rng(42);
    std::vector<uint8_t> data(obj_size);
    std::generate(data.begin(), data.end(), [&]{ return (uint8_t)(rng() & 0xff); });

    std::vector<long long> lats;
    lats.reserve(num_ops);
    for (size_t i = 0; i < num_ops; i++) {
        char key[64]; snprintf(key, sizeof(key), "obj-%08zu", i);
        auto t0 = Clock::now();
        CHECK(c.Put(std::string(key), data.data(), obj_size));
        lats.push_back((Clock::now() - t0).count());
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

// ── File helpers ──────────────────────────────────────────────────────────────

static std::vector<uint8_t> read_file(const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) { perror(path); std::exit(1); }
    fseek(f, 0, SEEK_END); long sz = ftell(f); rewind(f);
    std::vector<uint8_t> buf(sz);
    if (fread(buf.data(), 1, sz, f) != (size_t)sz) {
        fprintf(stderr, "read error: %s\n", path); std::exit(1);
    }
    fclose(f); return buf;
}

static void write_file(const char *path, const std::vector<uint8_t> &data) {
    FILE *f = fopen(path, "wb");
    if (!f) { perror(path); std::exit(1); }
    if (fwrite(data.data(), 1, data.size(), f) != data.size()) {
        fprintf(stderr, "write error: %s\n", path); std::exit(1);
    }
    fclose(f);
}

// ── main ──────────────────────────────────────────────────────────────────────

static void usage(const char *prog) {
    fprintf(stderr,
        "Usage:\n"
        "  %s --coord <host[:port]> <mode> [args...]\n"
        "\n"
        "  k and m are assigned automatically by the coordinator\n"
        "  based on the number of live server nodes.\n"
        "\n"
        "  Modes:\n"
        "    putfile <key> <file>        store a file\n"
        "    getfile <key> <outfile>     retrieve a file\n"
        "    delkey  <key>               delete a key\n"
        "    put   <num_ops> <obj_bytes> benchmark puts\n"
        "    get   <num_ops> <obj_bytes> benchmark gets\n"
        "    bench <num_ops> <obj_bytes> benchmark put then get\n"
        "\n"
        "Examples:\n"
        "  %s --coord node0 putfile mykey photo.jpg\n"
        "  %s --coord node0 bench 10000 65536\n",
        prog, prog, prog);
    std::exit(1);
}

int main(int argc, char **argv) {
    if (argc < 3) usage(argv[0]);

    const char *coord_host = nullptr;
    int         coord_port = 7777;
    int i = 1;

    while (i < argc) {
        if (strcmp(argv[i], "--coord") == 0 && i + 1 < argc) {
            char *arg = argv[++i]; i++;
            char *colon = strchr(arg, ':');
            if (colon) { *colon = '\0'; coord_port = atoi(colon + 1); }
            coord_host = arg;
        } else {
            break;
        }
    }

    if (!coord_host) { fprintf(stderr, "ERROR: --coord is required\n"); usage(argv[0]); }
    if (i >= argc)   usage(argv[0]);
    std::string mode = argv[i++];

    auto info = coord_discover(coord_host, coord_port);
    auto net  = Network::Open();
    ErasureClient c(net, info);
    int k = info.k;

    if (mode == "putfile") {
        if (i + 1 >= argc) usage(argv[0]);
        std::string key = argv[i++]; const char *path = argv[i];
        auto data = read_file(path);
        if (data.empty() || data.size() > (size_t)k * kMaxShardSize) {
            fprintf(stderr, "file size must be 1..%zu bytes\n", (size_t)k * kMaxShardSize);
            std::exit(1);
        }
        auto t0 = Clock::now();
        c.Put(key, data.data(), data.size());
        printf("PUT '%s' (%zu bytes) in %.1f us\n",
               key.c_str(), data.size(), (Clock::now()-t0).count()/1e3);

    } else if (mode == "getfile") {
        if (i + 1 >= argc) usage(argv[0]);
        std::string key = argv[i++]; const char *out = argv[i];
        auto t0 = Clock::now();
        auto data = c.Get(key);
        if (data.empty()) { fprintf(stderr, "key not found\n"); std::exit(1); }
        write_file(out, data);
        printf("GET '%s' (%zu bytes) in %.1f us → %s\n",
               key.c_str(), data.size(), (Clock::now()-t0).count()/1e3, out);

    } else if (mode == "delkey") {
        if (i >= argc) usage(argv[0]);
        c.Delete(argv[i]);
        printf("DEL '%s'\n", argv[i]);

    } else if (mode == "put" || mode == "get" || mode == "bench") {
        if (i + 1 >= argc) usage(argv[0]);
        size_t num_ops   = std::stoull(argv[i++]);
        size_t obj_bytes = std::stoull(argv[i]);
        if (obj_bytes == 0 || obj_bytes > (size_t)k * kMaxShardSize) {
            fprintf(stderr, "obj_bytes must be 1..%zu\n", (size_t)k * kMaxShardSize);
            std::exit(1);
        }
        if (mode == "put")       bench_put(c, num_ops, obj_bytes);
        else if (mode == "get")  bench_get(c, num_ops, obj_bytes);
        else { bench_put(c, num_ops, obj_bytes); bench_get(c, num_ops, obj_bytes); }

    } else {
        usage(argv[0]);
    }

    return 0;
}
