#include "common.hpp"
#include "protocol.hpp"
#include <algorithm>
#include <inttypes.h>
#include <string>
#include <unordered_map>
#include <vector>

// Metadata slots are cheap; pool memory is the real constraint.
static constexpr int    kMaxSlots = 65536;
static constexpr size_t kPoolSize = 512u * 1024 * 1024; // 512 MB RDMA pool

static std::string shard_key(const char *key, size_t klen, uint8_t shard_idx) {
    std::string s(key, klen);
    s += '\0';
    s += (char)shard_idx;
    return s;
}

struct SlotEntry {
    bool     in_use      = false;
    uint32_t shard_len   = 0;
    uint32_t object_size = 0;
    uint64_t pool_off    = 0; // byte offset within pool buffer
};

struct PendingPut {
    std::string sk;
    uint32_t    shard_len;
    uint32_t    object_size;
    uint64_t    pool_off;
};

// Free-list allocator over a contiguous pool buffer.
struct PoolAlloc {
    struct Block { uint64_t off; uint32_t len; };
    std::vector<Block> free_list;
    uint64_t           pool_size;

    explicit PoolAlloc(uint64_t sz) : pool_size(sz) {
        free_list.push_back({0, (uint32_t)sz});
    }

    // First-fit allocation; returns offset or UINT64_MAX on failure.
    uint64_t alloc(uint32_t len) {
        for (auto it = free_list.begin(); it != free_list.end(); ++it) {
            if (it->len >= len) {
                uint64_t off = it->off;
                if (it->len == len)
                    free_list.erase(it);
                else { it->off += len; it->len -= len; }
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
        // Merge with successor.
        auto nxt = std::next(it);
        if (nxt != free_list.end() && it->off + it->len == nxt->off) {
            it->len += nxt->len;
            free_list.erase(nxt);
        }
        // Merge with predecessor.
        if (it != free_list.begin()) {
            auto prv = std::prev(it);
            if (prv->off + prv->len == it->off) {
                prv->len += it->len;
                free_list.erase(it);
            }
        }
    }

    void reset() {
        free_list.clear();
        free_list.push_back({0, (uint32_t)pool_size});
    }
};

int main() {
    auto net = Network::Open();
    auto listen_addr = net.Listen();
    printf("address: %s\n", listen_addr.ToString().c_str());
    fflush(stdout);

    Buffer pool      = Buffer::Alloc(kPoolSize);
    Buffer ctrl_recv = Buffer::Alloc(kCtrlBufSize);
    Buffer ctrl_send = Buffer::Alloc(kCtrlBufSize);

    SlotEntry slots[kMaxSlots];
    int next_slot = 0;
    std::unordered_map<uint32_t, PendingPut> pending_puts;
    std::unordered_map<std::string, int>     key_to_slot;
    PoolAlloc palloc(kPoolSize);

    auto alloc_slot = [&]() -> int {
        for (int i = 0; i < kMaxSlots; i++) {
            int s = (next_slot + i) % kMaxSlots;
            if (!slots[s].in_use) {
                slots[s].in_use = true;
                next_slot = (s + 1) % kMaxSlots;
                return s;
            }
        }
        return -1;
    };

    auto free_slot = [&](int s) {
        if (slots[s].in_use) {
            palloc.free(slots[s].pool_off, slots[s].shard_len);
            slots[s] = SlotEntry{};
        }
    };

    // Accept clients in a loop so re-running the client doesn't require
    // restarting the server.  State (slots, keys) persists across reconnects
    // so data stored in one client session is readable in the next.
    for (;;) {
        fprintf(stderr, "Waiting for client connection... (%zu keys stored)\n",
                key_to_slot.size());
        Connection conn = net.Accept();
        fprintf(stderr, "Client connected.\n");

        // Re-register memory on each new connection (MRs are freed on disconnect).
        conn.RegMemRma(pool);
        conn.RegMem(ctrl_recv);
        conn.RegMem(ctrl_send);

        uint64_t pool_rkey = conn.RKey(pool);
        uint64_t pool_base = conn.VAddr(pool);

        uint64_t ops_put = 0, ops_get = 0, ops_del = 0;
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

            if (type == MsgType::kPutRequest) {
                auto *req = (const PutRequestMsg *)raw;
                const char *key = (const char *)(raw + sizeof(PutRequestMsg));
                auto sk = shard_key(key, req->key_len, req->shard_idx);

                // Free the old slot for this shard key if it exists.
                auto it = key_to_slot.find(sk);
                if (it != key_to_slot.end()) {
                    free_slot(it->second);
                    key_to_slot.erase(it);
                }

                int slot = alloc_slot();
                if (slot < 0) {
                    fprintf(stderr, "ERROR: slot metadata exhausted\n");
                    std::exit(1);
                }

                uint64_t pool_off = palloc.alloc(req->shard_len);
                if (pool_off == UINT64_MAX) {
                    fprintf(stderr, "ERROR: pool memory exhausted\n");
                    std::exit(1);
                }
                slots[slot].pool_off = pool_off;

                pending_puts[slot] = {sk, req->shard_len, req->object_size, pool_off};

                auto *grant        = (SlotGrantMsg *)ctrl_send.data;
                grant->type        = MsgType::kSlotGrant;
                memset(grant->_pad, 0, sizeof(grant->_pad));
                grant->slot_idx    = (uint32_t)slot;
                grant->remote_addr = pool_base + pool_off;
                grant->rkey        = pool_rkey;

                bool done = false;
                conn.PostSend(ctrl_send, sizeof(SlotGrantMsg),
                              [&done](Connection &, RdmaOp &) { done = true; });
                while (!done) conn.Poll();

            } else if (type == MsgType::kPutCommit) {
                auto *commit = (const PutCommitMsg *)raw;
                auto  it     = pending_puts.find(commit->slot_idx);
                CHECK(it != pending_puts.end());

                auto &pp = it->second;
                slots[commit->slot_idx].shard_len   = pp.shard_len;
                slots[commit->slot_idx].object_size = pp.object_size;
                key_to_slot[pp.sk]                  = commit->slot_idx;
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

            } else if (type == MsgType::kGetRequest) {
                auto *req = (const GetRequestMsg *)raw;
                const char *key = (const char *)(raw + sizeof(GetRequestMsg));
                auto sk = shard_key(key, req->key_len, req->shard_idx);

                auto *info      = (SlotInfoMsg *)ctrl_send.data;
                info->type      = MsgType::kSlotInfo;
                info->shard_idx = req->shard_idx;
                info->_pad      = 0;

                auto it = key_to_slot.find(sk);
                if (it == key_to_slot.end()) {
                    info->status      = StatusCode::kNotFound;
                    info->shard_len   = 0;
                    info->object_size = 0;
                    info->remote_addr = 0;
                    info->rkey        = 0;
                } else {
                    int slot          = it->second;
                    info->status      = StatusCode::kOk;
                    info->shard_len   = slots[slot].shard_len;
                    info->object_size = slots[slot].object_size;
                    info->remote_addr = pool_base + slots[slot].pool_off;
                    info->rkey        = pool_rkey;
                    ++ops_get;
                }

                bool done = false;
                conn.PostSend(ctrl_send, sizeof(SlotInfoMsg),
                              [&done](Connection &, RdmaOp &) { done = true; });
                while (!done) conn.Poll();

            } else if (type == MsgType::kDelRequest) {
                auto *req = (const DelRequestMsg *)raw;
                const char *key = (const char *)(raw + sizeof(DelRequestMsg));
                auto sk = shard_key(key, req->key_len, req->shard_idx);

                auto *ack      = (AckMsg *)ctrl_send.data;
                ack->type      = MsgType::kAck;
                ack->shard_idx = req->shard_idx;
                ack->_pad      = 0;

                auto it = key_to_slot.find(sk);
                if (it != key_to_slot.end()) {
                    free_slot(it->second);
                    key_to_slot.erase(it);
                    ack->status = StatusCode::kOk;
                    ++ops_del;
                } else {
                    ack->status = StatusCode::kNotFound;
                }

                bool done = false;
                conn.PostSend(ctrl_send, sizeof(AckMsg),
                              [&done](Connection &, RdmaOp &) { done = true; });
                while (!done) conn.Poll();

            } else {
                fprintf(stderr, "unknown msg type %d\n", (int)type);
            }

            uint64_t total = ops_put + ops_get + ops_del;
            if (total > 0 && total % 10000 == 0)
                printf("stats: put=%" PRIu64 " get=%" PRIu64 " del=%" PRIu64
                       " keys=%zu\n", ops_put, ops_get, ops_del, key_to_slot.size());
        }

        fprintf(stderr, "Client disconnected (put=%" PRIu64 " get=%" PRIu64
                " del=%" PRIu64 " keys=%zu).\n",
                ops_put, ops_get, ops_del, key_to_slot.size());
        // conn destructor closes endpoint and frees MRs; outer loop re-accepts.
    }
}
