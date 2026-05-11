#include "common.hpp"
#include "protocol.hpp"
#include <algorithm>
#include <arpa/inet.h>
#include <atomic>
#include <fcntl.h>
#include <inttypes.h>
#include <mutex>
#include <netdb.h>
#include <string>
#include <sys/socket.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>

static constexpr int    kMaxSlots        = 65536;
static constexpr size_t kSlabSize        = 512ULL * 1024 * 1024; // 512 MB per slab
static constexpr int    kMaxSlabs        = 8;                     // 4 GB total RDMA pool
static constexpr int    kMaxStagingSlots = 64;                    // 64 MB staging area
static constexpr size_t kDiskPoolSize    = 64ULL * 1024 * 1024 * 1024; // 64 GB disk cap
static std::string      kDiskFileStr     = ([](){
    const char *h = getenv("HOME");
    return std::string(h ? h : "/tmp") + "/rdma-kv-spill.bin";
})();
static const char      *kDiskFile        = kDiskFileStr.c_str();

static std::string shard_key(const char *key, size_t klen, uint8_t shard_idx) {
    std::string s(key, klen);
    s += '\0';
    s += (char)shard_idx;
    return s;
}

// ── SlotEntry ─────────────────────────────────────────────────────────────────

struct SlotEntry {
    bool     in_use      = false;
    uint8_t  slab_id     = 0;    // which RDMA slab (ignored when on_disk)
    bool     on_disk     = false; // true when shard was spilled to NVMe
    uint32_t shard_len   = 0;
    uint32_t object_size = 0;
    uint64_t pool_off    = 0;    // byte offset within slab or disk file
};

struct PendingPut {
    std::string sk;
    uint32_t    shard_len;
    uint32_t    object_size;
    uint64_t    pool_off;
    uint8_t     slab_id;
    bool        on_disk;
    int         staging_slot; // staging slot used for disk PUT (-1 if RDMA)
};

// ── Free-list allocator ───────────────────────────────────────────────────────
// Variable-size: alloc(len) allocates exactly len bytes, not a fixed slot size.

struct PoolAlloc {
    struct Block { uint64_t off; uint32_t len; };
    std::vector<Block> free_list;
    uint64_t           pool_size;
    uint64_t           bytes_free;

    explicit PoolAlloc(uint64_t sz) : pool_size(sz), bytes_free(sz) {
        free_list.push_back({0, (uint32_t)std::min(sz, (uint64_t)UINT32_MAX)});
    }

    uint64_t alloc(uint32_t len) {
        for (auto it = free_list.begin(); it != free_list.end(); ++it) {
            if (it->len >= len) {
                uint64_t off = it->off;
                if (it->len == len) free_list.erase(it);
                else { it->off += len; it->len -= len; }
                bytes_free -= len;
                return off;
            }
        }
        return UINT64_MAX;
    }

    void free(uint64_t off, uint32_t len) {
        Block b{off, len};
        auto it = std::lower_bound(free_list.begin(), free_list.end(), b,
            [](const Block &a, const Block &x){ return a.off < x.off; });
        it = free_list.insert(it, b);
        auto nxt = std::next(it);
        if (nxt != free_list.end() && it->off + it->len == nxt->off) {
            it->len += nxt->len; free_list.erase(nxt);
        }
        if (it != free_list.begin()) {
            auto prv = std::prev(it);
            if (prv->off + prv->len == it->off) {
                prv->len += it->len; free_list.erase(it);
            }
        }
        bytes_free += len;
    }
};

// ── Multi-slab RDMA allocator ─────────────────────────────────────────────────
// Grows lazily: starts with one 512 MB slab; adds more when needed (up to 8).
// All methods must be called under SharedState::mu.

struct MultiSlabAlloc {
    struct Slab {
        Buffer    buf;
        PoolAlloc palloc;
        Slab() : buf(Buffer::Alloc(kSlabSize)), palloc(kSlabSize) {}
        Slab(Slab &&o) = default;
    };
    std::vector<Slab> slabs;

    struct AllocResult { uint8_t slab_id; uint64_t offset; bool ok; };

    MultiSlabAlloc() { slabs.emplace_back(); } // start with one slab

    AllocResult alloc(uint32_t len) {
        for (size_t i = 0; i < slabs.size(); i++) {
            uint64_t off = slabs[i].palloc.alloc(len);
            if (off != UINT64_MAX) return {(uint8_t)i, off, true};
        }
        if ((int)slabs.size() >= kMaxSlabs) return {0, 0, false};
        fprintf(stderr, "[server] RDMA pool full — allocating slab #%zu (%.0f GB total)\n",
                slabs.size(), (slabs.size() + 1) * kSlabSize / 1e9);
        slabs.emplace_back();
        uint8_t id = (uint8_t)(slabs.size() - 1);
        uint64_t off = slabs[id].palloc.alloc(len);
        return {id, off, off != UINT64_MAX};
    }

    void free(uint8_t slab_id, uint64_t off, uint32_t len) {
        slabs[slab_id].palloc.free(off, len);
    }

    uint64_t bytes_used() const {
        uint64_t free_bytes = 0;
        for (auto &s : slabs) free_bytes += s.palloc.bytes_free;
        return slabs.size() * kSlabSize - free_bytes;
    }
    uint64_t bytes_total() const { return slabs.size() * kSlabSize; }
};

// ── Shared state across all client threads ────────────────────────────────────

struct SharedState {
    std::mutex                           mu;
    SlotEntry                            slots[kMaxSlots];
    int                                  next_slot = 0;
    std::unordered_map<std::string, int> key_to_slot;
    MultiSlabAlloc                       multi_slab;

    // Staging buffer: pre-registered RDMA memory used as a bridge for disk shards.
    // On disk PUT: client RDMA-writes shard here; server copies to disk, then frees slot.
    // On disk GET: server reads from disk here; client RDMA-reads, then sends DiskRelease.
    Buffer                               staging_buf;
    std::atomic<bool>                    staging_in_use[kMaxStagingSlots];

    // Disk spill (activated when all RDMA slabs are exhausted)
    int       disk_fd    = -1;
    PoolAlloc disk_alloc;

    explicit SharedState()
        : staging_buf(Buffer::Alloc((size_t)kMaxStagingSlots * kMaxShardSize)),
          disk_alloc(kDiskPoolSize) {
        for (int i = 0; i < kMaxStagingSlots; i++) staging_in_use[i].store(false);
        disk_fd = open(kDiskFile, O_RDWR | O_CREAT, 0600);
        if (disk_fd < 0) {
            perror("open disk spill file");
            std::exit(1);
        }
    }
};

// ── Staging slot allocator (lock-free spin) ───────────────────────────────────

static int alloc_staging_slot(SharedState &shared) {
    for (;;) {
        for (int i = 0; i < kMaxStagingSlots; i++) {
            bool expected = false;
            if (shared.staging_in_use[i].compare_exchange_weak(expected, true,
                    std::memory_order_acquire, std::memory_order_relaxed))
                return i;
        }
        std::this_thread::yield();
    }
}

static void free_staging_slot(SharedState &shared, int idx) {
    shared.staging_in_use[idx].store(false, std::memory_order_release);
}

// ── Coordinator keepalive ─────────────────────────────────────────────────────

static void coord_keepalive(int fd, SharedState &shared) {
    char buf[64];
    while (true) {
        int r = recv(fd, buf, sizeof(buf) - 1, 0);
        if (r <= 0) {
            fprintf(stderr, "Coordinator connection lost.\n");
            close(fd);
            return;
        }
        buf[r] = '\0';
        if (strncmp(buf, "PING", 4) == 0) {
            size_t keys, bytes_used, bytes_total;
            {
                std::lock_guard<std::mutex> lk(shared.mu);
                keys        = shared.key_to_slot.size();
                bytes_used  = shared.multi_slab.bytes_used();
                bytes_total = shared.multi_slab.bytes_total();
            }
            char resp[128];
            snprintf(resp, sizeof(resp), "PONG %zu %zu %zu\n",
                     keys, bytes_used, bytes_total);
            send(fd, resp, strlen(resp), MSG_NOSIGNAL);
        }
    }
}

static int coord_register(const char *host, int port,
                           const std::string &hex_addr) {
    addrinfo hints{}, *res;
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    char port_str[16]; snprintf(port_str, sizeof(port_str), "%d", port);
    if (getaddrinfo(host, port_str, &hints, &res) != 0) {
        fprintf(stderr, "coordinator: getaddrinfo failed for %s\n", host);
        return -1;
    }
    int fd = socket(res->ai_family, res->ai_socktype, 0);
    if (connect(fd, res->ai_addr, res->ai_addrlen) < 0) {
        fprintf(stderr, "coordinator: connect to %s:%d failed\n", host, port);
        close(fd); freeaddrinfo(res); return -1;
    }
    freeaddrinfo(res);
    char msg[512];
    int n = snprintf(msg, sizeof(msg), "REGISTER %s\n", hex_addr.c_str());
    send(fd, msg, n, 0);
    char resp[64] = {};
    recv(fd, resp, sizeof(resp) - 1, 0);
    int idx = -1;
    sscanf(resp, "OK %d", &idx);
    fprintf(stderr, "Registered as shard %d with coordinator %s:%d\n",
            idx, host, port);
    return fd;
}

// ── Per-client thread ─────────────────────────────────────────────────────────

static void handle_client(Connection conn, SharedState &shared) {
    // Per-connection control buffers.
    Buffer ctrl_recv = Buffer::Alloc(kCtrlBufSize);
    Buffer ctrl_send = Buffer::Alloc(kCtrlBufSize);
    conn.RegMem(ctrl_recv);
    conn.RegMem(ctrl_send);

    // Register staging buffer for this connection (one MR per connection).
    conn.RegMemRma(shared.staging_buf);
    uint64_t staging_rkey = conn.RKey(shared.staging_buf);
    uint64_t staging_base = conn.VAddr(shared.staging_buf);

    // Per-connection tracking of registered RDMA slab MRs.
    // New slabs added by other threads are registered lazily here before use.
    struct SlabConn { uint64_t rkey; uint64_t base_addr; };
    std::vector<SlabConn> slab_conns;

    // Register all currently existing slabs and any new ones that appear.
    auto ensure_slabs_registered = [&]() {
        // Called under shared.mu by the caller (PutRequest path), or separately.
        while (slab_conns.size() < shared.multi_slab.slabs.size()) {
            auto &slab = shared.multi_slab.slabs[slab_conns.size()];
            conn.RegMemRma(slab.buf);
            slab_conns.push_back({conn.RKey(slab.buf), conn.VAddr(slab.buf)});
            fprintf(stderr, "[server] Registered slab #%zu for new connection\n",
                    slab_conns.size() - 1);
        }
    };

    // Initial registration under the lock.
    {
        std::lock_guard<std::mutex> lk(shared.mu);
        ensure_slabs_registered();
    }

    uint64_t ops_put = 0, ops_get = 0, ops_del = 0;

    // pending_puts is per-connection: tracks PutRequest→PutCommit within one client.
    std::unordered_map<uint32_t, PendingPut> pending_puts;

    // Staging slots allocated by this connection (for cleanup on disconnect).
    std::vector<int> my_staging_slots;

    // free_slot and alloc_slot must be called under shared.mu.
    auto alloc_slot = [&shared]() -> int {
        for (int i = 0; i < kMaxSlots; i++) {
            int s = (shared.next_slot + i) % kMaxSlots;
            if (!shared.slots[s].in_use) {
                shared.slots[s].in_use = true;
                shared.next_slot = (s + 1) % kMaxSlots;
                return s;
            }
        }
        return -1;
    };

    auto free_slot = [&shared](int s) {
        if (!shared.slots[s].in_use) return;
        if (shared.slots[s].on_disk) {
            shared.disk_alloc.free(shared.slots[s].pool_off, shared.slots[s].shard_len);
        } else {
            shared.multi_slab.free(shared.slots[s].slab_id,
                                   shared.slots[s].pool_off,
                                   shared.slots[s].shard_len);
        }
        shared.slots[s] = SlotEntry{};
    };

    bool running = true;

    while (running) {
        bool got = false;
        conn.PostRecv(ctrl_recv, [&](Connection &, RdmaOp &op) {
            got = true; (void)op;
        });
        while (!got) {
            conn.Poll();
            if (conn.IsShutdown()) { running = false; break; }
        }
        if (!running) break;

        auto *raw  = (const uint8_t *)ctrl_recv.data;
        auto  type = (MsgType)raw[0];

        // ── PutRequest ───────────────────────────────────────────────────────
        if (type == MsgType::kPutRequest) {
            auto *req = (const PutRequestMsg *)raw;
            const char *key = (const char *)(raw + sizeof(PutRequestMsg));
            auto sk = shard_key(key, req->key_len, req->shard_idx);

            int      slot;
            uint8_t  slab_id   = 0;
            uint64_t pool_off  = 0;
            bool     spill     = false;
            int      stg_slot  = -1;

            {
                std::lock_guard<std::mutex> lk(shared.mu);

                // Free old slot for this shard key if overwriting.
                auto it = shared.key_to_slot.find(sk);
                if (it != shared.key_to_slot.end()) {
                    free_slot(it->second);
                    shared.key_to_slot.erase(it);
                }

                slot = alloc_slot();
                if (slot < 0) {
                    fprintf(stderr, "ERROR: slot metadata exhausted\n");
                    std::exit(1);
                }

                auto r = shared.multi_slab.alloc(req->shard_len);
                // Register any slabs created by alloc() (including new ones just grown).
                ensure_slabs_registered();
                if (r.ok) {
                    slab_id  = r.slab_id;
                    pool_off = r.offset;
                } else {
                    // All RDMA slabs full — spill to disk.
                    pool_off = shared.disk_alloc.alloc(req->shard_len);
                    if (pool_off == UINT64_MAX) {
                        fprintf(stderr, "ERROR: disk pool exhausted\n");
                        std::exit(1);
                    }
                    spill = true;
                    fprintf(stderr, "[server] Spilling shard to disk at offset %" PRIu64 "\n",
                            pool_off);
                }
                shared.slots[slot].pool_off = pool_off;
                shared.slots[slot].slab_id  = spill ? 0 : slab_id;
            }

            // For disk spill: client writes to a staging slot; server copies to disk after commit.
            // For RDMA: client writes directly to the slab.
            uint64_t grant_addr;
            uint64_t grant_rkey;

            if (spill) {
                stg_slot   = alloc_staging_slot(shared);
                grant_addr = staging_base + (uint64_t)stg_slot * kMaxShardSize;
                grant_rkey = staging_rkey;
            } else {
                grant_addr = slab_conns[slab_id].base_addr + pool_off;
                grant_rkey = slab_conns[slab_id].rkey;
            }

            pending_puts[slot] = {sk, req->shard_len, req->object_size,
                                  pool_off, slab_id, spill, stg_slot};

            auto *grant     = (SlotGrantMsg *)ctrl_send.data;
            grant->type     = MsgType::kSlotGrant;
            memset(grant->_pad, 0, sizeof(grant->_pad));
            grant->slot_idx    = (uint32_t)slot;
            grant->remote_addr = grant_addr;
            grant->rkey        = grant_rkey;

            bool done = false;
            conn.PostSend(ctrl_send, sizeof(SlotGrantMsg),
                          [&done](Connection &, RdmaOp &) { done = true; });
            while (!done) conn.Poll();

        // ── PutCommit ────────────────────────────────────────────────────────
        } else if (type == MsgType::kPutCommit) {
            auto *commit = (const PutCommitMsg *)raw;
            auto  it     = pending_puts.find(commit->slot_idx);
            CHECK(it != pending_puts.end());
            auto &pp = it->second;

            if (pp.on_disk) {
                // Client wrote to staging buffer; copy to disk now.
                uint64_t stg_off = (uint64_t)pp.staging_slot * kMaxShardSize;
                ssize_t w = pwrite(shared.disk_fd,
                                   (const char *)shared.staging_buf.data + stg_off,
                                   pp.shard_len, (off_t)pp.pool_off);
                if (w != (ssize_t)pp.shard_len)
                    fprintf(stderr, "[server] pwrite error: %s\n", strerror(errno));
                free_staging_slot(shared, pp.staging_slot);
            }

            {
                std::lock_guard<std::mutex> lk(shared.mu);
                shared.slots[commit->slot_idx].shard_len   = pp.shard_len;
                shared.slots[commit->slot_idx].object_size = pp.object_size;
                shared.slots[commit->slot_idx].on_disk     = pp.on_disk;
                shared.key_to_slot[pp.sk]                  = commit->slot_idx;
            }
            pending_puts.erase(it);
            ++ops_put;

            auto *ack    = (AckMsg *)ctrl_send.data;
            ack->type    = MsgType::kAck;
            ack->status  = StatusCode::kOk;
            ack->shard_idx = 0;
            ack->_pad    = 0;

            bool done = false;
            conn.PostSend(ctrl_send, sizeof(AckMsg),
                          [&done](Connection &, RdmaOp &) { done = true; });
            while (!done) conn.Poll();

        // ── GetRequest ───────────────────────────────────────────────────────
        } else if (type == MsgType::kGetRequest) {
            auto *req = (const GetRequestMsg *)raw;
            const char *key = (const char *)(raw + sizeof(GetRequestMsg));
            auto sk = shard_key(key, req->key_len, req->shard_idx);

            uint64_t   addr = 0, rkey = 0;
            uint32_t   shard_len = 0, object_size = 0;
            StatusCode status;
            uint8_t    on_disk_flag   = 0;
            uint32_t   stg_slot_idx   = 0;

            {
                std::lock_guard<std::mutex> lk(shared.mu);
                // Ensure any new slabs (added since last check) are registered.
                ensure_slabs_registered();
                auto it = shared.key_to_slot.find(sk);
                if (it == shared.key_to_slot.end()) {
                    status = StatusCode::kNotFound;
                } else {
                    int slot    = it->second;
                    status      = StatusCode::kOk;
                    shard_len   = shared.slots[slot].shard_len;
                    object_size = shared.slots[slot].object_size;

                    if (shared.slots[slot].on_disk) {
                        on_disk_flag = 1;
                    } else {
                        uint8_t sid = shared.slots[slot].slab_id;
                        addr = slab_conns[sid].base_addr + shared.slots[slot].pool_off;
                        rkey = slab_conns[sid].rkey;
                    }
                    ++ops_get;
                }
            }

            if (status == StatusCode::kOk && on_disk_flag) {
                // Read disk shard into a staging slot; client will RDMA-read from there.
                int stg = alloc_staging_slot(shared);
                my_staging_slots.push_back(stg);
                stg_slot_idx = (uint32_t)stg;

                uint64_t stg_off = (uint64_t)stg * kMaxShardSize;
                // disk_alloc offset was stored in pool_off; retrieve it under the lock.
                uint64_t disk_off = 0;
                {
                    std::lock_guard<std::mutex> lk(shared.mu);
                    auto it = shared.key_to_slot.find(sk);
                    if (it != shared.key_to_slot.end())
                        disk_off = shared.slots[it->second].pool_off;
                }
                ssize_t r = pread(shared.disk_fd,
                                  (char *)shared.staging_buf.data + stg_off,
                                  shard_len, (off_t)disk_off);
                if (r != (ssize_t)shard_len)
                    fprintf(stderr, "[server] pread error: %s\n", strerror(errno));

                addr = staging_base + stg_off;
                rkey = staging_rkey;
            }

            auto *info        = (SlotInfoMsg *)ctrl_send.data;
            info->type        = MsgType::kSlotInfo;
            info->shard_idx   = req->shard_idx;
            info->on_disk     = on_disk_flag;
            info->_pad2        = 0;
            info->status      = status;
            info->shard_len   = shard_len;
            info->object_size = object_size;
            info->remote_addr = addr;
            info->rkey        = rkey;
            info->staging_slot_idx = stg_slot_idx;

            bool done = false;
            conn.PostSend(ctrl_send, sizeof(SlotInfoMsg),
                          [&done](Connection &, RdmaOp &) { done = true; });
            while (!done) conn.Poll();

        // ── DelRequest ───────────────────────────────────────────────────────
        } else if (type == MsgType::kDelRequest) {
            auto *req = (const DelRequestMsg *)raw;
            const char *key = (const char *)(raw + sizeof(DelRequestMsg));
            auto sk = shard_key(key, req->key_len, req->shard_idx);

            StatusCode status;
            {
                std::lock_guard<std::mutex> lk(shared.mu);
                auto it = shared.key_to_slot.find(sk);
                if (it != shared.key_to_slot.end()) {
                    free_slot(it->second);
                    shared.key_to_slot.erase(it);
                    status = StatusCode::kOk;
                } else {
                    status = StatusCode::kNotFound;
                }
            }
            ++ops_del;

            auto *ack      = (AckMsg *)ctrl_send.data;
            ack->type      = MsgType::kAck;
            ack->shard_idx = req->shard_idx;
            ack->_pad      = 0;
            ack->status    = status;

            bool done = false;
            conn.PostSend(ctrl_send, sizeof(AckMsg),
                          [&done](Connection &, RdmaOp &) { done = true; });
            while (!done) conn.Poll();

        // ── DiskRelease ──────────────────────────────────────────────────────
        } else if (type == MsgType::kDiskRelease) {
            auto *rel = (const DiskReleaseMsg *)raw;
            free_staging_slot(shared, (int)rel->staging_slot_idx);
            // Remove from our tracking list.
            auto &v = my_staging_slots;
            v.erase(std::remove(v.begin(), v.end(), (int)rel->staging_slot_idx), v.end());
            // No reply needed.

        } else {
            fprintf(stderr, "unknown msg type %d\n", (int)type);
        }

        uint64_t total = ops_put + ops_get + ops_del;
        if (total > 0 && total % 10000 == 0) {
            size_t keys, mb_used, mb_total;
            {
                std::lock_guard<std::mutex> lk(shared.mu);
                keys     = shared.key_to_slot.size();
                mb_used  = (size_t)(shared.multi_slab.bytes_used() >> 20);
                mb_total = (size_t)(shared.multi_slab.bytes_total() >> 20);
            }
            printf("stats: put=%" PRIu64 " get=%" PRIu64 " del=%" PRIu64
                   " keys=%zu pool=%zuMB/%zuMB\n",
                   ops_put, ops_get, ops_del, keys, mb_used, mb_total);
            fflush(stdout);
        }
    }

    // Release any staging slots still held (e.g., client disconnected before DiskRelease).
    for (int s : my_staging_slots) free_staging_slot(shared, s);

    // Reclaim slots allocated but never committed (client died mid-PUT).
    {
        std::lock_guard<std::mutex> lk(shared.mu);
        for (auto &[slot_idx, pp] : pending_puts) {
            if (pp.staging_slot >= 0)
                free_staging_slot(shared, pp.staging_slot);
            free_slot(slot_idx);
        }
    }

    fprintf(stderr, "Client disconnected (put=%" PRIu64 " get=%" PRIu64
            " del=%" PRIu64 ").\n", ops_put, ops_get, ops_del);
}

// ── main ──────────────────────────────────────────────────────────────────────

int main(int argc, char **argv) {
    const char *coord_host = nullptr;
    int         coord_port = 7777;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--coord") == 0 && i + 1 < argc) {
            char *colon = strchr(argv[++i], ':');
            if (colon) { *colon = '\0'; coord_port = atoi(colon + 1); }
            coord_host = argv[i];
        }
    }

    auto net = Network::Open();
    auto listen_addr = net.Listen();
    printf("address: %s\n", listen_addr.ToString().c_str());
    fflush(stdout);

    SharedState shared;

    if (coord_host) {
        int coord_fd = coord_register(coord_host, coord_port, listen_addr.ToString());
        if (coord_fd >= 0)
            std::thread([coord_fd, &shared]() {
                coord_keepalive(coord_fd, shared);
            }).detach();
    }

    for (;;) {
        {
            size_t keys, mb_used, mb_total;
            {
                std::lock_guard<std::mutex> lk(shared.mu);
                keys     = shared.key_to_slot.size();
                mb_used  = (size_t)(shared.multi_slab.bytes_used() >> 20);
                mb_total = (size_t)(shared.multi_slab.bytes_total() >> 20);
            }
            fprintf(stderr, "Waiting for client... (%zu keys, pool %zuMB/%zuMB)\n",
                    keys, mb_used, mb_total);
        }
        Connection conn = net.Accept();
        fprintf(stderr, "Client connected.\n");
        std::thread([c = std::move(conn), &shared]() mutable {
            handle_client(std::move(c), shared);
        }).detach();
    }
}
