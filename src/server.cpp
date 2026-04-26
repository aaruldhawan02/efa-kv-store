#include "common.hpp"
#include "protocol.hpp"
#include <string>
#include <unordered_map>
#include <vector>
#include <inttypes.h>

// In-memory key-value store
struct KVStore {
    std::unordered_map<std::string, std::vector<uint8_t>> data;

    void Put(const char *key, size_t klen, const uint8_t *val, size_t vlen) {
        data[std::string(key, klen)] = std::vector<uint8_t>(val, val + vlen);
    }

    bool Get(const char *key, size_t klen, const uint8_t **val_out, size_t *vlen_out) const {
        auto it = data.find(std::string(key, klen));
        if (it == data.end()) return false;
        *val_out  = it->second.data();
        *vlen_out = it->second.size();
        return true;
    }

    bool Delete(const char *key, size_t klen) {
        return data.erase(std::string(key, klen)) > 0;
    }
};

static void send_response(Network &net, fi_addr_t dest, Buffer &send_buf,
                           StatusCode status, const uint8_t *val, size_t vlen) {
    CHECK(response_size(vlen) <= send_buf.size);
    auto *hdr  = (ResponseHeader *)send_buf.data;
    hdr->status  = status;
    hdr->val_len = (uint32_t)vlen;
    if (val && vlen > 0)
        memcpy((uint8_t *)send_buf.data + sizeof(ResponseHeader), val, vlen);

    bool done = false;
    net.PostSend(dest, send_buf, response_size(vlen),
                 [&done](Network &, RdmaOp &) { done = true; });
    while (!done) net.Poll();
}

int main() {
    auto net = Network::Open();
    printf("address: %s\n", net.addr.ToString().c_str());
    printf("Run client: ./build/client %s\n\n", net.addr.ToString().c_str());

    KVStore  kv;
    Buffer   recv_buf = Buffer::Alloc(kMsgBufSize);
    Buffer   send_buf = Buffer::Alloc(kMsgBufSize);
    net.RegMem(recv_buf);
    net.RegMem(send_buf);

    fi_addr_t client_addr = FI_ADDR_UNSPEC;
    uint64_t  ops_put = 0, ops_get = 0, ops_del = 0;

    for (;;) {
        // Wait for one message
        bool     got_msg  = false;
        size_t   msg_len  = 0;
        net.PostRecv(recv_buf, [&](Network &, RdmaOp &op) {
            got_msg = true;
            msg_len = op.len;
        });
        while (!got_msg) net.Poll();

        auto *data     = (const uint8_t *)recv_buf.data;
        auto  msg_type = (MsgType)data[0];

        if (msg_type == MsgType::kConnect) {
            auto *msg  = (const ConnectMsg *)data;
            EfaAddress peer(msg->addr);
            client_addr = net.AddPeer(peer);
            printf("Client connected: %s\n", peer.ToString().c_str());
            continue;
        }

        CHECK(client_addr != FI_ADDR_UNSPEC);
        auto *hdr = (const RequestHeader *)data;
        CHECK(msg_len >= sizeof(RequestHeader));
        const char    *key = (const char *)(data + sizeof(RequestHeader));
        const uint8_t *val = (const uint8_t *)key + hdr->key_len;

        switch (msg_type) {
        case MsgType::kPut:
            kv.Put(key, hdr->key_len, val, hdr->val_len);
            send_response(net, client_addr, send_buf, StatusCode::kOk, nullptr, 0);
            ++ops_put;
            break;

        case MsgType::kGet: {
            const uint8_t *resp_val  = nullptr;
            size_t         resp_vlen = 0;
            bool found = kv.Get(key, hdr->key_len, &resp_val, &resp_vlen);
            send_response(net, client_addr, send_buf,
                          found ? StatusCode::kOk : StatusCode::kNotFound,
                          resp_val, found ? resp_vlen : 0);
            ++ops_get;
            break;
        }

        case MsgType::kDelete: {
            bool found = kv.Delete(key, hdr->key_len);
            send_response(net, client_addr, send_buf,
                          found ? StatusCode::kOk : StatusCode::kNotFound,
                          nullptr, 0);
            ++ops_del;
            break;
        }

        default:
            fprintf(stderr, "Unknown message type %d\n", (int)msg_type);
            send_response(net, client_addr, send_buf, StatusCode::kError, nullptr, 0);
        }

        if ((ops_put + ops_get + ops_del) % 10000 == 0)
            printf("stats: put=%" PRIu64 " get=%" PRIu64 " del=%" PRIu64
                   " kv_size=%zu\n",
                   ops_put, ops_get, ops_del, kv.data.size());
    }
}
