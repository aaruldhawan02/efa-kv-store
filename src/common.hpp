#pragma once

#include <deque>
#include <functional>
#include <rdma/fabric.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_errno.h>
#include <rdma/fi_eq.h>
#include <rdma/fi_rma.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <unordered_map>

#define CHECK(stmt)                                                           \
  do {                                                                        \
    if (!(stmt)) {                                                            \
      fprintf(stderr, "CHECK failed %s:%d: %s\n", __FILE__, __LINE__, #stmt);\
      std::exit(1);                                                           \
    }                                                                         \
  } while (0)

#define FI_CHECK(stmt)                                                        \
  do {                                                                        \
    int _rc = (stmt);                                                         \
    if (_rc) {                                                                \
      fprintf(stderr, "FI_CHECK failed %s:%d: %s: %s\n",                     \
              __FILE__, __LINE__, #stmt, fi_strerror(-_rc));                  \
      std::exit(1);                                                           \
    }                                                                         \
  } while (0)

static constexpr size_t kBufAlign   = 128;
static constexpr size_t kCQBatch    = 16;
static constexpr size_t kMaxAddrLen = 64;

// Fabric address (variable-length, up to 64 bytes).
struct FabricAddress {
    uint8_t bytes[kMaxAddrLen];
    size_t  len;

    FabricAddress() : len(0) { memset(bytes, 0, kMaxAddrLen); }
    FabricAddress(const uint8_t *b, size_t l) : len(l) {
        CHECK(l <= kMaxAddrLen);
        memcpy(bytes, b, l);
        if (l < kMaxAddrLen) memset(bytes + l, 0, kMaxAddrLen - l);
    }

    std::string ToString() const {
        std::string s(len * 2, '\0');
        for (size_t i = 0; i < len; i++)
            snprintf(&s[i * 2], 3, "%02x", bytes[i]);
        return s;
    }

    static FabricAddress Parse(const std::string &s) {
        if (s.size() % 2 != 0 || s.size() < 2) {
            fprintf(stderr, "Invalid fabric address: %s\n", s.c_str());
            std::exit(1);
        }
        size_t l = s.size() / 2;
        CHECK(l <= kMaxAddrLen);
        uint8_t b[kMaxAddrLen];
        for (size_t i = 0; i < l; i++)
            sscanf(s.c_str() + 2 * i, "%02hhx", &b[i]);
        return FabricAddress(b, l);
    }
};

static inline void *align_up(void *p, size_t align) {
    uintptr_t a = (uintptr_t)p;
    return (void *)((a + align - 1) & ~(align - 1));
}

struct Buffer {
    void  *data;
    size_t size;

    static Buffer Alloc(size_t sz) {
        void *raw = malloc(sz + kBufAlign);
        CHECK(raw != nullptr);
        return Buffer(raw, sz);
    }

    Buffer(Buffer &&o) : data(o.data), size(o.size), raw_(o.raw_) {
        o.raw_ = nullptr; o.data = nullptr;
    }
    ~Buffer() { free(raw_); }

private:
    void *raw_;
    Buffer(void *raw, size_t sz) : raw_(raw) {
        data = align_up(raw, kBufAlign);
        size = sz;
    }
    Buffer(const Buffer &) = delete;
};

// ── Per-connection endpoint (FI_EP_MSG) ──────────────────────────────────────
//
// One Connection per peer.  Server creates these via Accept(); client via
// Connect().  All send/recv/write/read operate on the connected endpoint —
// no address vector or fi_addr_t needed.

struct Connection;
enum class RdmaOpType : uint8_t { kRecv, kSend, kWrite, kRead };

struct RdmaOp {
    RdmaOpType  type;
    Buffer     *buf;
    size_t      len;
    uint64_t    remote_addr;
    uint64_t    remote_key;
    std::function<void(Connection &, RdmaOp &)> cb;
};

struct Connection {
    struct fid_ep *ep  = nullptr;
    struct fid_cq *cq  = nullptr;
    struct fid_eq *eq  = nullptr;
    bool           virt_addr_mode = false;
    std::unordered_map<void *, struct fid_mr *> mr_map;
    std::deque<RdmaOp *> pending;

    void RegMem(Buffer &buf);
    void RegMemRma(Buffer &buf);
    uint64_t       RKey(const Buffer &buf);
    uint64_t       VAddr(const Buffer &buf);
    struct fid_mr *MR(const Buffer &buf);

    void PostRecv(Buffer &buf, std::function<void(Connection &, RdmaOp &)> cb);
    void PostSend(Buffer &buf, size_t len,
                  std::function<void(Connection &, RdmaOp &)> cb);
    void PostWrite(Buffer &local_buf, size_t len,
                   uint64_t remote_addr, uint64_t remote_key,
                   std::function<void(Connection &, RdmaOp &)> cb);
    void PostRead(Buffer &local_buf, size_t len,
                  uint64_t remote_addr, uint64_t remote_key,
                  std::function<void(Connection &, RdmaOp &)> cb);
    void Poll();
    bool IsShutdown();  // true if peer has closed the connection

    ~Connection();
    Connection() = default;
    Connection(const Connection &) = delete;
    Connection(Connection &&o);

private:
    struct fid_domain *domain_ = nullptr;  // borrowed, not owned

    void Progress();
    void HandleCQE(const struct fi_cq_data_entry &cqe);

    friend struct Network;
};

// ── Network: fabric + domain, creates Connections ────────────────────────────

struct Network {
    struct fi_info    *fi     = nullptr;
    struct fid_fabric *fabric = nullptr;
    struct fid_domain *domain = nullptr;
    bool               virt_addr_mode = false;
    FabricAddress      addr;   // passive endpoint address (server) or empty (client)

    // Server: open a passive endpoint and start listening.
    // Returns the address clients should connect to.
    FabricAddress Listen();

    // Server: block until one client connects; returns the Connection.
    Connection Accept();

    // Client: connect to a server; returns the Connection.
    Connection Connect(const FabricAddress &server_addr);

    static Network Open();
    ~Network();
    Network(const Network &) = delete;
    Network(Network &&o);

private:
    struct fid_pep *pep_ = nullptr;  // passive endpoint (server only)
    struct fid_eq  *eq_  = nullptr;  // EQ for connection events

    Network(fi_info *, fid_fabric *, fid_domain *, bool virt_addr);
    Connection make_connection(fid_ep *ep);
};

// ── Implementation ────────────────────────────────────────────────────────────

static struct fi_info *make_hints(bool server) {
    struct fi_info *h = fi_allocinfo();
    h->ep_attr->type          = FI_EP_MSG;
    h->fabric_attr->prov_name = strdup("verbs");
    h->caps                   = FI_MSG | FI_RMA;
    h->mode                   = FI_CONTEXT;
    h->domain_attr->mr_mode   = FI_MR_LOCAL | FI_MR_ALLOCATED |
                                  FI_MR_PROV_KEY | FI_MR_VIRT_ADDR;
    (void)server;
    return h;
}

inline Network Network::Open() {
    struct fi_info *hints = make_hints(false);
    struct fi_info *fi;
    FI_CHECK(fi_getinfo(FI_VERSION(FI_MAJOR_VERSION, FI_MINOR_VERSION),
                        nullptr, nullptr, 0, hints, &fi));
    fi_freeinfo(hints);

    bool virt_addr = (fi->domain_attr->mr_mode & FI_MR_VIRT_ADDR) != 0;
    fprintf(stderr, "provider: %s  domain: %s  link: %.0fGbps  virt_addr: %d\n",
            fi->fabric_attr->prov_name, fi->domain_attr->name,
            fi->nic ? fi->nic->link_attr->speed / 1e9 : 0.0, (int)virt_addr);

    struct fid_fabric *fabric;
    FI_CHECK(fi_fabric(fi->fabric_attr, &fabric, nullptr));
    struct fid_domain *domain;
    FI_CHECK(fi_domain(fabric, fi, &domain, nullptr));

    return Network(fi, fabric, domain, virt_addr);
}

inline FabricAddress Network::Listen() {
    struct fi_eq_attr eq_attr = {};
    eq_attr.size     = 16;
    eq_attr.wait_obj = FI_WAIT_UNSPEC;
    FI_CHECK(fi_eq_open(fabric, &eq_attr, &eq_, nullptr));

    FI_CHECK(fi_passive_ep(fabric, fi, &pep_, nullptr));
    FI_CHECK(fi_pep_bind(pep_, &eq_->fid, 0));
    FI_CHECK(fi_listen(pep_));

    uint8_t addrbuf[kMaxAddrLen];
    size_t  addrlen = sizeof(addrbuf);
    FI_CHECK(fi_getname(&pep_->fid, addrbuf, &addrlen));
    CHECK(addrlen <= kMaxAddrLen);
    addr = FabricAddress(addrbuf, addrlen);
    return addr;
}

inline Connection Network::Accept() {
    // Wait for connection request
    struct fi_eq_cm_entry entry;
    uint32_t event;
    for (;;) {
        ssize_t n = fi_eq_read(eq_, &event, &entry, sizeof(entry), 0);
        if (n >= (ssize_t)sizeof(entry) && event == FI_CONNREQ) break;
        if (n < 0 && n != -FI_EAGAIN) {
            fprintf(stderr, "fi_eq_read error: %s\n", fi_strerror(-(int)n));
            std::exit(1);
        }
    }

    struct fid_ep *ep;
    FI_CHECK(fi_endpoint(domain, entry.info, &ep, nullptr));
    fi_freeinfo(entry.info);

    Connection c = make_connection(ep);

    struct fi_eq_attr eq_attr = {};
    eq_attr.size = 4; eq_attr.wait_obj = FI_WAIT_UNSPEC;
    FI_CHECK(fi_eq_open(fabric, &eq_attr, &c.eq, nullptr));
    FI_CHECK(fi_ep_bind(ep, &c.eq->fid, 0));
    FI_CHECK(fi_accept(ep, nullptr, 0));

    // Wait for CONNECTED
    for (;;) {
        ssize_t n = fi_eq_read(c.eq, &event, &entry, sizeof(entry), 0);
        if (n >= 0 && event == FI_CONNECTED) break;
        if (n < 0 && n != -FI_EAGAIN) {
            fprintf(stderr, "fi_eq_read (accept): %s\n", fi_strerror(-(int)n));
            std::exit(1);
        }
    }
    return c;
}

inline Connection Network::Connect(const FabricAddress &server_addr) {
    // For FI_EP_MSG, fi_endpoint needs a fi_info with dest_addr set.
    // Decode the server's sockaddr_in and re-query fi_getinfo with it.
    struct sockaddr_in sa;
    CHECK(server_addr.len >= sizeof(sa));
    memcpy(&sa, server_addr.bytes, sizeof(sa));
    char ip[INET_ADDRSTRLEN], port[8];
    inet_ntop(AF_INET, &sa.sin_addr, ip, sizeof(ip));
    snprintf(port, sizeof(port), "%u", ntohs(sa.sin_port));

    struct fi_info *hints = make_hints(false);
    struct fi_info *fi_dest;
    FI_CHECK(fi_getinfo(FI_VERSION(FI_MAJOR_VERSION, FI_MINOR_VERSION),
                        ip, port, 0, hints, &fi_dest));
    fi_freeinfo(hints);

    struct fid_ep *ep;
    FI_CHECK(fi_endpoint(domain, fi_dest, &ep, nullptr));

    Connection c = make_connection(ep);

    struct fi_eq_attr eq_attr = {};
    eq_attr.size = 4; eq_attr.wait_obj = FI_WAIT_UNSPEC;
    FI_CHECK(fi_eq_open(fabric, &eq_attr, &c.eq, nullptr));
    FI_CHECK(fi_ep_bind(ep, &c.eq->fid, 0));
    FI_CHECK(fi_connect(ep, fi_dest->dest_addr, nullptr, 0));
    fi_freeinfo(fi_dest);

    // Wait for CONNECTED
    struct fi_eq_cm_entry entry;
    uint32_t event;
    for (;;) {
        ssize_t n = fi_eq_read(c.eq, &event, &entry, sizeof(entry), 0);
        if (n >= 0 && event == FI_CONNECTED) break;
        if (n < 0 && n != -FI_EAGAIN) {
            fprintf(stderr, "fi_eq_read (connect): %s\n", fi_strerror(-(int)n));
            std::exit(1);
        }
    }
    return c;
}

inline Connection Network::make_connection(fid_ep *ep) {
    Connection c;
    c.domain_       = domain;
    c.virt_addr_mode = virt_addr_mode;
    c.ep            = ep;

    struct fi_cq_attr cq_attr = {};
    cq_attr.format   = FI_CQ_FORMAT_DATA;
    cq_attr.size     = 1024;
    FI_CHECK(fi_cq_open(domain, &cq_attr, &c.cq, nullptr));
    FI_CHECK(fi_ep_bind(ep, &c.cq->fid, FI_SEND | FI_RECV));
    return c;
}

inline Network::~Network() {
    if (pep_) fi_close(&pep_->fid);
    if (eq_)  fi_close(&eq_->fid);
    if (domain) fi_close(&domain->fid);
    if (fabric) fi_close(&fabric->fid);
    fi_freeinfo(fi);
}

inline Network::Network(fi_info *fi, fid_fabric *fabric, fid_domain *domain,
                         bool virt_addr)
    : fi(fi), fabric(fabric), domain(domain), virt_addr_mode(virt_addr) {}

inline Network::Network(Network &&o)
    : fi(o.fi), fabric(o.fabric), domain(o.domain),
      virt_addr_mode(o.virt_addr_mode), addr(o.addr),
      pep_(o.pep_), eq_(o.eq_) {
    o.fi = nullptr; o.fabric = nullptr; o.domain = nullptr;
    o.pep_ = nullptr; o.eq_ = nullptr;
}

// ── Connection implementation ─────────────────────────────────────────────────

inline Connection::Connection(Connection &&o)
    : ep(o.ep), cq(o.cq), eq(o.eq),
      virt_addr_mode(o.virt_addr_mode),
      mr_map(std::move(o.mr_map)),
      pending(std::move(o.pending)),
      domain_(o.domain_) {
    o.ep = nullptr; o.cq = nullptr; o.eq = nullptr; o.domain_ = nullptr;
}

inline bool Connection::IsShutdown() {
    if (!eq) return false;
    struct fi_eq_cm_entry entry;
    uint32_t event;
    ssize_t n = fi_eq_read(eq, &event, &entry, sizeof(entry), 0);
    return n >= 0 && event == FI_SHUTDOWN;
}

inline Connection::~Connection() {
    for (auto &[_, mr] : mr_map) fi_close(&mr->fid);
    if (ep) fi_close(&ep->fid);
    if (cq) fi_close(&cq->fid);
    if (eq) fi_close(&eq->fid);
}

inline void Connection::RegMem(Buffer &buf) {
    struct iovec iov = {buf.data, buf.size};
    struct fi_mr_attr mr_attr = {};
    mr_attr.mr_iov    = &iov;
    mr_attr.iov_count = 1;
    mr_attr.access    = FI_SEND | FI_RECV;
    struct fid_mr *mr;
    FI_CHECK(fi_mr_regattr(domain_, &mr_attr, 0, &mr));
    mr_map[buf.data] = mr;
}

inline void Connection::RegMemRma(Buffer &buf) {
    struct iovec iov = {buf.data, buf.size};
    struct fi_mr_attr mr_attr = {};
    mr_attr.mr_iov    = &iov;
    mr_attr.iov_count = 1;
    mr_attr.access    = FI_READ | FI_WRITE | FI_REMOTE_READ | FI_REMOTE_WRITE;
    struct fid_mr *mr;
    FI_CHECK(fi_mr_regattr(domain_, &mr_attr, 0, &mr));
    mr_map[buf.data] = mr;
}

inline uint64_t Connection::RKey(const Buffer &buf) {
    return fi_mr_key(MR(buf));
}

inline uint64_t Connection::VAddr(const Buffer &buf) {
    return virt_addr_mode ? (uint64_t)buf.data : 0;
}

inline struct fid_mr *Connection::MR(const Buffer &buf) {
    auto it = mr_map.find(buf.data);
    CHECK(it != mr_map.end());
    return it->second;
}

inline void Connection::PostRecv(Buffer &buf,
                                  std::function<void(Connection &, RdmaOp &)> cb) {
    auto *op = new RdmaOp{RdmaOpType::kRecv, &buf, 0, 0, 0, std::move(cb)};
    pending.push_back(op);
    Progress();
}

inline void Connection::PostSend(Buffer &buf, size_t len,
                                  std::function<void(Connection &, RdmaOp &)> cb) {
    CHECK(len <= buf.size);
    auto *op = new RdmaOp{RdmaOpType::kSend, &buf, len, 0, 0, std::move(cb)};
    pending.push_back(op);
    Progress();
}

inline void Connection::PostWrite(Buffer &local_buf, size_t len,
                                   uint64_t remote_addr, uint64_t remote_key,
                                   std::function<void(Connection &, RdmaOp &)> cb) {
    CHECK(len <= local_buf.size);
    auto *op = new RdmaOp{RdmaOpType::kWrite, &local_buf, len,
                           remote_addr, remote_key, std::move(cb)};
    pending.push_back(op);
    Progress();
}

inline void Connection::PostRead(Buffer &local_buf, size_t len,
                                  uint64_t remote_addr, uint64_t remote_key,
                                  std::function<void(Connection &, RdmaOp &)> cb) {
    CHECK(len <= local_buf.size);
    auto *op = new RdmaOp{RdmaOpType::kRead, &local_buf, len,
                           remote_addr, remote_key, std::move(cb)};
    pending.push_back(op);
    Progress();
}

inline void Connection::Progress() {
    while (!pending.empty()) {
        auto *op = pending.front();
        pending.pop_front();

        ssize_t ret;
        void *desc = MR(*op->buf)->mem_desc;

        if (op->type == RdmaOpType::kRecv || op->type == RdmaOpType::kSend) {
            struct iovec iov;
            iov.iov_base = op->buf->data;
            iov.iov_len  = (op->type == RdmaOpType::kRecv) ? op->buf->size : op->len;
            struct fi_msg msg = {};
            msg.msg_iov   = &iov;
            msg.desc      = &desc;
            msg.iov_count = 1;
            msg.addr      = FI_ADDR_UNSPEC;  // already connected
            msg.context   = op;
            ret = (op->type == RdmaOpType::kRecv)
                ? fi_recvmsg(ep, &msg, FI_COMPLETION)
                : fi_sendmsg(ep, &msg, FI_COMPLETION);
        } else {
            struct iovec iov = {op->buf->data, op->len};
            struct fi_rma_iov rma_iov = {op->remote_addr, op->len, op->remote_key};
            struct fi_msg_rma msg = {};
            msg.msg_iov       = &iov;
            msg.desc          = &desc;
            msg.iov_count     = 1;
            msg.addr          = FI_ADDR_UNSPEC;
            msg.rma_iov       = &rma_iov;
            msg.rma_iov_count = 1;
            msg.context       = op;
            ret = (op->type == RdmaOpType::kWrite)
                ? fi_writemsg(ep, &msg, FI_COMPLETION)
                : fi_readmsg(ep, &msg, FI_COMPLETION);
        }

        if (ret == -FI_EAGAIN) { pending.push_front(op); break; }
        if (ret) {
            fprintf(stderr, "post %s failed: %s\n",
                    (op->type == RdmaOpType::kRecv ? "recv" :
                     op->type == RdmaOpType::kSend ? "send" :
                     op->type == RdmaOpType::kWrite ? "write" : "read"),
                    fi_strerror(-ret));
            delete op;
        }
    }
}

inline void Connection::HandleCQE(const struct fi_cq_data_entry &cqe) {
    auto *op = (RdmaOp *)cqe.op_context;
    if (!op) return;
    if (cqe.flags & FI_RECV) op->len = cqe.len;
    if (op->cb) op->cb(*this, *op);
    delete op;
}

inline void Connection::Poll() {
    struct fi_cq_data_entry cqe[kCQBatch];
    for (;;) {
        ssize_t n = fi_cq_read(cq, cqe, kCQBatch);
        if (n > 0) {
            for (ssize_t i = 0; i < n; i++) HandleCQE(cqe[i]);
        } else if (n == -FI_EAVAIL) {
            struct fi_cq_err_entry e;
            if (fi_cq_readerr(cq, &e, 0) > 0)
                fprintf(stderr, "cq error: %s\n",
                        fi_cq_strerror(cq, e.prov_errno, e.err_data, nullptr, 0));
        } else if (n == -FI_EAGAIN) {
            break;
        } else {
            fprintf(stderr, "fi_cq_read error: %s\n", fi_strerror(-n));
            std::exit(1);
        }
    }
    Progress();
}
