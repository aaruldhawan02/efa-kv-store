#include "common.hpp"
#include "protocol.hpp"
#include <inttypes.h>
#include <string>
#include <unordered_map>
#include <vector>

// Storage key = object key + '\0' + shard_idx byte
static std::string shard_key(const char *key, size_t klen, uint8_t shard_idx) {
    std::string s(key, klen);
    s += '\0';
    s += (char)shard_idx;
    return s;
}

static void send_response(Network &net, fi_addr_t dest, Buffer &send_buf,
                           StatusCode status, uint8_t shard_idx,
                           const uint8_t *shard_data, size_t shard_len,
                           uint32_t object_size) {
    CHECK(response_bytes(shard_len) <= send_buf.size);
    auto *hdr        = (ResponseHeader *)send_buf.data;
    hdr->status      = status;
    hdr->shard_idx   = shard_idx;
    hdr->shard_len   = (uint32_t)shard_len;
    hdr->object_size = object_size;
    if (shard_data && shard_len > 0)
        memcpy((uint8_t *)send_buf.data + sizeof(ResponseHeader), shard_data, shard_len);

    bool done = false;
    net.PostSend(dest, send_buf, response_bytes(shard_len),
                 [&done](Network &, RdmaOp &) { done = true; });
    while (!done) net.Poll();
}

int main() {
    auto net = Network::Open();
    printf("address: %s\n", net.addr.ToString().c_str());
    printf("Run client: ./build/client <num_servers> %s [addr2 ...] bench 1000 65536\n\n",
           net.addr.ToString().c_str());

    // shard storage: shard_key -> (object_size, shard_bytes)
    struct ShardEntry { uint32_t object_size; std::vector<uint8_t> data; };
    std::unordered_map<std::string, ShardEntry> store;

    Buffer recv_buf = Buffer::Alloc(kMsgBufSize);
    Buffer send_buf = Buffer::Alloc(kMsgBufSize);
    net.RegMem(recv_buf);
    net.RegMem(send_buf);

    fi_addr_t client_addr = FI_ADDR_UNSPEC;
    uint64_t  ops_put = 0, ops_get = 0, ops_del = 0;

    for (;;) {
        bool   got  = false;
        size_t mlen = 0;
        net.PostRecv(recv_buf, [&](Network &, RdmaOp &op) { got = true; mlen = op.len; });
        while (!got) net.Poll();

        auto *raw  = (const uint8_t *)recv_buf.data;
        auto  type = (MsgType)raw[0];

        if (type == MsgType::kConnect) {
            auto *msg   = (const ConnectMsg *)raw;
            EfaAddress peer(msg->addr);
            client_addr = net.AddPeer(peer);
            printf("client connected: %s\n", peer.ToString().c_str());
            continue;
        }

        CHECK(client_addr != FI_ADDR_UNSPEC);
        CHECK(mlen >= sizeof(ShardRequestHeader));
        auto *hdr = (const ShardRequestHeader *)raw;
        const char *key = (const char *)(raw + sizeof(ShardRequestHeader));
        const uint8_t *shard_data = (const uint8_t *)key + hdr->key_len;
        auto sk = shard_key(key, hdr->key_len, hdr->shard_idx);

        switch (type) {
        case MsgType::kPutShard:
            store[sk] = {hdr->object_size,
                         std::vector<uint8_t>(shard_data, shard_data + hdr->shard_len)};
            send_response(net, client_addr, send_buf,
                          StatusCode::kOk, hdr->shard_idx, nullptr, 0, 0);
            ++ops_put;
            break;

        case MsgType::kGetShard: {
            auto it = store.find(sk);
            if (it == store.end()) {
                send_response(net, client_addr, send_buf,
                              StatusCode::kNotFound, hdr->shard_idx, nullptr, 0, 0);
            } else {
                auto &entry = it->second;
                send_response(net, client_addr, send_buf, StatusCode::kOk,
                              hdr->shard_idx,
                              entry.data.data(), entry.data.size(), entry.object_size);
            }
            ++ops_get;
            break;
        }

        case MsgType::kDelShard: {
            bool found = store.erase(sk) > 0;
            send_response(net, client_addr, send_buf,
                          found ? StatusCode::kOk : StatusCode::kNotFound,
                          hdr->shard_idx, nullptr, 0, 0);
            ++ops_del;
            break;
        }

        default:
            fprintf(stderr, "unknown msg type %d\n", (int)type);
            send_response(net, client_addr, send_buf,
                          StatusCode::kError, 0, nullptr, 0, 0);
        }

        uint64_t total = ops_put + ops_get + ops_del;
        if (total > 0 && total % 10000 == 0)
            printf("stats: put=%" PRIu64 " get=%" PRIu64 " del=%" PRIu64
                   " keys=%zu\n", ops_put, ops_get, ops_del, store.size());
    }
}
