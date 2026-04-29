#pragma once
#include "common.hpp"
#include "ec.hpp"
#include "protocol.hpp"
#include <arpa/inet.h>
#include <chrono>
#include <netdb.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

using _Clock = std::chrono::high_resolution_clock;
using _ns    = long long;
static inline _ns _now() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               _Clock::now().time_since_epoch()).count();
}

struct PhaseTimes {
    _ns encode_ns       = 0; // ISA-L encode (PUT) or decode (GET)
    _ns ctrl_rtt_ns     = 0; // control send → grant/info received
    _ns rdma_ns         = 0; // PostWrite (PUT) or PostRead (GET) → completion
    _ns commit_rtt_ns   = 0; // commit send → ack received (PUT only)
};


// ── Per-server connection ──────────────────────────────────────────────────────

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
        memset(ctrl_send.data, 0, ctrl_send.size);
        memset(ctrl_recv.data, 0, ctrl_recv.size);
        memset(data_buf.data,  0, data_buf.size);
    }
};

// ── ErasureClient ──────────────────────────────────────────────────────────────

struct ErasureClient {
    int   k, m;
    ErasureCoder ec;
    std::vector<ServerConn> servers;
    PhaseTimes last_put_phases;
    PhaseTimes last_get_phases;

    ErasureClient(Network &net, int k, int m,
                  const std::vector<FabricAddress> &addrs)
        : k(k), m(m), ec(k, m) {
        CHECK((int)addrs.size() == k + m);
        for (auto &a : addrs) servers.emplace_back(net, a);
        printf("Connected to %d servers (k=%d m=%d).\n", k + m, k, m);
        fflush(stdout);
    }

    bool Put(const std::string &key, const uint8_t *data, size_t data_len) {
        _ns t0, t1;

        t0 = _now();
        auto shards = ec.Encode(data, data_len);
        last_put_phases.encode_ns = _now() - t0;

        t0 = _now();
        for (int i = 0; i < k + m; i++) {
            auto &srv = servers[i];
            auto *req        = (PutRequestMsg *)srv.ctrl_send.data;
            req->type        = MsgType::kPutRequest;
            memset(req->_pad, 0, sizeof(req->_pad));
            req->key_len     = (uint32_t)key.size();
            req->shard_len   = (uint32_t)shards[i].size();
            req->object_size = (uint32_t)data_len;
            req->shard_idx   = (uint8_t)i;
            req->k           = (uint8_t)k;
            req->m           = (uint8_t)m;
            req->_pad2       = 0;
            memcpy((char *)srv.ctrl_send.data + sizeof(PutRequestMsg),
                   key.data(), key.size());
            srv.conn.PostSend(srv.ctrl_send,
                              sizeof(PutRequestMsg) + key.size(),
                              [](Connection &, RdmaOp &) {});
        }
        for (auto &srv : servers)
            while (!srv.conn.pending.empty()) srv.conn.Poll();

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
        last_put_phases.ctrl_rtt_ns = _now() - t0;

        t0 = _now();
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
        last_put_phases.rdma_ns = _now() - t0;

        t0 = _now();
        for (int i = 0; i < k + m; i++) {
            auto &srv    = servers[i];
            auto *commit = (PutCommitMsg *)srv.ctrl_send.data;
            commit->type = MsgType::kPutCommit;
            memset(commit->_pad, 0, sizeof(commit->_pad));
            commit->slot_idx = grants[i].slot_idx;
            srv.conn.PostSend(srv.ctrl_send, sizeof(PutCommitMsg),
                              [](Connection &, RdmaOp &) {});
        }
        for (auto &srv : servers)
            while (!srv.conn.pending.empty()) srv.conn.Poll();

        inflight = k + m;
        for (int i = 0; i < k + m; i++) {
            servers[i].conn.PostRecv(servers[i].ctrl_recv,
                [&inflight](Connection &, RdmaOp &) { --inflight; });
        }
        while (inflight > 0)
            for (auto &srv : servers) srv.conn.Poll();
        last_put_phases.commit_rtt_ns = _now() - t0;

        return true;
    }

    std::vector<uint8_t> Get(const std::string &key) {
        _ns t0;

        t0 = _now();
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
        last_get_phases.ctrl_rtt_ns = _now() - t0;

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
        for (int i : to_read)
            if (infos[i].shard_idx >= (uint8_t)k) { degraded = true; break; }
        if (degraded)
            fprintf(stderr, "degraded read for '%s' (using parity shards)\n",
                    key.c_str());

        std::vector<std::vector<uint8_t>> shards(k + m);
        std::vector<bool>                 present(k + m, false);
        uint32_t object_size = 0;
        inflight = k;

        t0 = _now();
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
        last_get_phases.rdma_ns = _now() - t0;

        t0 = _now();
        auto result = ec.Decode(shards, present, object_size);
        last_get_phases.encode_ns = _now() - t0;  // decode time

        return result;
    }

    void Delete(const std::string &key) {
        for (int i = 0; i < k + m; i++) {
            auto &srv      = servers[i];
            auto *req      = (DelRequestMsg *)srv.ctrl_send.data;
            req->type      = MsgType::kDelRequest;
            req->shard_idx = (uint8_t)i;
            req->_pad[0]   = req->_pad[1] = 0;
            req->key_len   = (uint32_t)key.size();
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

// ── Coordinator discovery ──────────────────────────────────────────────────────

inline std::vector<FabricAddress>
coord_discover(const char *host, int port, int k, int m) {
    addrinfo hints{}, *res;
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    char ps[16]; snprintf(ps, sizeof(ps), "%d", port);
    if (getaddrinfo(host, ps, &hints, &res) != 0) {
        fprintf(stderr, "coordinator: getaddrinfo failed for %s\n", host);
        exit(1);
    }
    int need = k + m;
    for (int attempt = 0; attempt < 60; attempt++) {
        int fd = socket(res->ai_family, res->ai_socktype, 0);
        if (connect(fd, res->ai_addr, res->ai_addrlen) < 0) {
            fprintf(stderr, "coordinator: connect failed, retrying...\n");
            close(fd); sleep(1); continue;
        }
        char msg[64];
        int n = snprintf(msg, sizeof(msg), "LIST %d %d\n", k, m);
        send(fd, msg, n, 0);
        std::string resp; char buf[512];
        while (true) {
            int r = recv(fd, buf, sizeof(buf)-1, 0);
            if (r <= 0) break;
            buf[r] = '\0'; resp += buf;
            if (resp.find("END\n")  != std::string::npos) break;
            if (resp.find("WAIT")   != std::string::npos) break;
            if (resp.find("ERROR")  != std::string::npos) break;
        }
        close(fd);
        if (resp.substr(0, 4) == "WAIT") {
            fprintf(stderr, "coordinator: %s", resp.c_str());
            sleep(1); continue;
        }
        std::vector<std::string> strs;
        size_t pos = 0;
        while (pos < resp.size()) {
            size_t nl = resp.find('\n', pos); if (nl == std::string::npos) break;
            std::string line = resp.substr(pos, nl - pos); pos = nl + 1;
            if (line == "END") break;
            if (!line.empty()) strs.push_back(line);
        }
        if ((int)strs.size() == need) {
            freeaddrinfo(res);
            std::vector<FabricAddress> addrs;
            for (auto &s : strs) addrs.push_back(FabricAddress::Parse(s.c_str()));
            return addrs;
        }
    }
    fprintf(stderr, "coordinator: timed out waiting for %d servers\n", need);
    exit(1);
}
