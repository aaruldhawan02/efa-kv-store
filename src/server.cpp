#include "common.hpp"
#include "protocol.hpp"
#include <inttypes.h>
#include <string>
#include <unordered_map>

static constexpr int kMaxSlots = 1024;

static std::string shard_key(const char *key, size_t klen, uint8_t shard_idx) {
    std::string s(key, klen);
    s += '\0';
    s += (char)shard_idx;
    return s;
}

struct SlotEntry {
    bool     in_use     = false;
    uint32_t shard_len  = 0;
    uint32_t object_size = 0;
};

struct PendingPut {
    std::string sk;
    uint32_t    shard_len;
    uint32_t    object_size;
};

int main() {
    auto net = Network::Open();
    printf("address: %s\n", net.addr.ToString().c_str());

    // Pre-allocate and register the slot pool for remote read/write.
    // Each slot holds one shard; clients RDMA-write/read directly into slots.
    Buffer pool = Buffer::Alloc((size_t)kMaxSlots * kMaxShardSize);
    net.RegMemRma(pool);
    uint64_t pool_rkey = net.RKey(pool);
    uint64_t pool_base = net.VAddr(pool);

    SlotEntry slots[kMaxSlots];
    int next_slot = 0;

    // pending_puts: slot_idx -> info, updated to key_to_slot on kPutCommit
    std::unordered_map<uint32_t, PendingPut> pending_puts;
    std::unordered_map<std::string, int>     key_to_slot;

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

    Buffer ctrl_recv = Buffer::Alloc(kCtrlBufSize);
    Buffer ctrl_send = Buffer::Alloc(kCtrlBufSize);
    net.RegMem(ctrl_recv);
    net.RegMem(ctrl_send);

    fi_addr_t client_addr = FI_ADDR_UNSPEC;
    uint64_t  ops_put = 0, ops_get = 0, ops_del = 0;

    for (;;) {
        bool   got  = false;
        size_t mlen = 0;
        net.PostRecv(ctrl_recv, [&](Network &, RdmaOp &op) { got = true; mlen = op.len; });
        while (!got) net.Poll();
        (void)mlen;

        auto *raw  = (const uint8_t *)ctrl_recv.data;
        auto  type = (MsgType)raw[0];

        if (type == MsgType::kConnect) {
            auto *msg = (const ConnectMsg *)raw;
            EfaAddress peer(msg->addr);
            client_addr = net.AddPeer(peer);
            printf("client connected: %s\n", peer.ToString().c_str());
            continue;
        }

        CHECK(client_addr != FI_ADDR_UNSPEC);

        if (type == MsgType::kPutRequest) {
            auto *req = (const PutRequestMsg *)raw;
            const char *key = (const char *)(raw + sizeof(PutRequestMsg));
            auto sk = shard_key(key, req->key_len, req->shard_idx);

            // Free old slot so it can be reused after the new write commits
            auto it = key_to_slot.find(sk);
            if (it != key_to_slot.end()) {
                slots[it->second].in_use = false;
                key_to_slot.erase(it);
            }

            int slot = alloc_slot();
            if (slot < 0) {
                fprintf(stderr, "ERROR: slot pool exhausted (%d slots)\n", kMaxSlots);
                std::exit(1);
            }

            pending_puts[slot] = {sk, req->shard_len, req->object_size};

            auto *grant         = (SlotGrantMsg *)ctrl_send.data;
            grant->type         = MsgType::kSlotGrant;
            memset(grant->_pad, 0, sizeof(grant->_pad));
            grant->slot_idx     = (uint32_t)slot;
            grant->remote_addr  = pool_base + (uint64_t)slot * kMaxShardSize;
            grant->rkey         = pool_rkey;

            bool done = false;
            net.PostSend(client_addr, ctrl_send, sizeof(SlotGrantMsg),
                         [&done](Network &, RdmaOp &) { done = true; });
            while (!done) net.Poll();

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
            net.PostSend(client_addr, ctrl_send, sizeof(AckMsg),
                         [&done](Network &, RdmaOp &) { done = true; });
            while (!done) net.Poll();

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
                info->remote_addr = pool_base + (uint64_t)slot * kMaxShardSize;
                info->rkey        = pool_rkey;
                ++ops_get;
            }

            bool done = false;
            net.PostSend(client_addr, ctrl_send, sizeof(SlotInfoMsg),
                         [&done](Network &, RdmaOp &) { done = true; });
            while (!done) net.Poll();

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
                slots[it->second].in_use = false;
                key_to_slot.erase(it);
                ack->status = StatusCode::kOk;
                ++ops_del;
            } else {
                ack->status = StatusCode::kNotFound;
            }

            bool done = false;
            net.PostSend(client_addr, ctrl_send, sizeof(AckMsg),
                         [&done](Network &, RdmaOp &) { done = true; });
            while (!done) net.Poll();

        } else {
            fprintf(stderr, "unknown msg type %d\n", (int)type);
        }

        uint64_t total = ops_put + ops_get + ops_del;
        if (total > 0 && total % 10000 == 0)
            printf("stats: put=%" PRIu64 " get=%" PRIu64 " del=%" PRIu64
                   " keys=%zu\n", ops_put, ops_get, ops_del, key_to_slot.size());
    }
}
