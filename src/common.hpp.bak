#pragma once

#include <deque>
#include <functional>
#include <rdma/fabric.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_errno.h>
#include <rdma/fi_rma.h>
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

static constexpr size_t kBufAlign = 128;
static constexpr size_t kCQBatch  = 16;

struct EfaAddress {
    uint8_t bytes[32];

    EfaAddress() { memset(bytes, 0, 32); }
    explicit EfaAddress(const uint8_t b[32]) { memcpy(bytes, b, 32); }

    std::string ToString() const {
        char s[65];
        for (int i = 0; i < 32; i++) snprintf(s + 2 * i, 3, "%02x", bytes[i]);
        return std::string(s, 64);
    }

    static EfaAddress Parse(const std::string &s) {
        if (s.size() != 64) {
            fprintf(stderr, "Invalid EFA address length %zu (expected 64)\n", s.size());
            std::exit(1);
        }
        uint8_t b[32];
        for (int i = 0; i < 32; i++) sscanf(s.c_str() + 2 * i, "%02hhx", &b[i]);
        return EfaAddress(b);
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

struct Network;
enum class RdmaOpType : uint8_t { kRecv, kSend, kWrite, kRead };

struct RdmaOp {
    RdmaOpType type;
    Buffer    *buf;
    size_t     len;
    fi_addr_t  addr;
    uint64_t   remote_addr;  // kWrite/kRead: remote virtual address
    uint64_t   remote_key;   // kWrite/kRead: remote memory key
    std::function<void(Network &, RdmaOp &)> cb;
};

struct Network {
    struct fi_info    *fi;
    struct fid_fabric *fabric;
    struct fid_domain *domain;
    struct fid_cq     *cq;
    struct fid_av     *av;
    struct fid_ep     *ep;
    EfaAddress         addr;
    std::unordered_map<void *, struct fid_mr *> mr_map;
    std::deque<RdmaOp *>                        pending;

    static Network Open();
    fi_addr_t      AddPeer(const EfaAddress &peer);
    void           RegMem(Buffer &buf);     // FI_SEND | FI_RECV
    void           RegMemRma(Buffer &buf);  // FI_READ | FI_WRITE | FI_REMOTE_READ | FI_REMOTE_WRITE
    uint64_t       RKey(const Buffer &buf);
    uint64_t       VAddr(const Buffer &buf);
    struct fid_mr *MR(const Buffer &buf);

    void PostRecv(Buffer &buf, std::function<void(Network &, RdmaOp &)> cb);
    void PostSend(fi_addr_t dest, Buffer &buf, size_t len,
                  std::function<void(Network &, RdmaOp &)> cb);
    void PostWrite(fi_addr_t dest, Buffer &local_buf, size_t len,
                   uint64_t remote_addr, uint64_t remote_key,
                   std::function<void(Network &, RdmaOp &)> cb);
    void PostRead(fi_addr_t dest, Buffer &local_buf, size_t len,
                  uint64_t remote_addr, uint64_t remote_key,
                  std::function<void(Network &, RdmaOp &)> cb);
    void Poll();

    ~Network();
    Network(const Network &) = delete;
    Network(Network &&o);

private:
    Network(fi_info *, fid_fabric *, fid_domain *, fid_cq *, fid_av *, fid_ep *, EfaAddress);
    void Progress();
    void HandleCQE(const struct fi_cq_data_entry &cqe);
};

inline Network Network::Open() {
    struct fi_info *hints = fi_allocinfo();
    hints->ep_attr->type          = FI_EP_RDM;
    hints->fabric_attr->prov_name = strdup("efa");
    hints->caps                   = FI_MSG;

    struct fi_info *fi;
    FI_CHECK(fi_getinfo(FI_VERSION(FI_MAJOR_VERSION, FI_MINOR_VERSION),
                        nullptr, nullptr, 0, hints, &fi));
    fi_freeinfo(hints);

    fprintf(stderr, "fabric: %s, domain: %s, link: %.0fGbps\n",
            fi->fabric_attr->prov_name,
            fi->domain_attr->name,
            fi->nic ? fi->nic->link_attr->speed / 1e9 : 0.0);

    struct fid_fabric *fabric;
    FI_CHECK(fi_fabric(fi->fabric_attr, &fabric, nullptr));

    struct fid_domain *domain;
    FI_CHECK(fi_domain(fabric, fi, &domain, nullptr));

    struct fi_cq_attr cq_attr = {};
    cq_attr.format = FI_CQ_FORMAT_DATA;
    struct fid_cq *cq;
    FI_CHECK(fi_cq_open(domain, &cq_attr, &cq, nullptr));

    struct fi_av_attr av_attr = {};
    struct fid_av *av;
    FI_CHECK(fi_av_open(domain, &av_attr, &av, nullptr));

    struct fid_ep *ep;
    FI_CHECK(fi_endpoint(domain, fi, &ep, nullptr));
    FI_CHECK(fi_ep_bind(ep, &cq->fid, FI_SEND | FI_RECV));
    FI_CHECK(fi_ep_bind(ep, &av->fid, 0));
    FI_CHECK(fi_enable(ep));

    uint8_t addrbuf[64];
    size_t  addrlen = sizeof(addrbuf);
    FI_CHECK(fi_getname(&ep->fid, addrbuf, &addrlen));
    CHECK(addrlen == 32);

    return Network(fi, fabric, domain, cq, av, ep, EfaAddress(addrbuf));
}

inline fi_addr_t Network::AddPeer(const EfaAddress &peer) {
    fi_addr_t a = FI_ADDR_UNSPEC;
    int ret = fi_av_insert(av, peer.bytes, 1, &a, 0, nullptr);
    if (ret != 1) { fprintf(stderr, "fi_av_insert failed: %d\n", ret); std::exit(1); }
    return a;
}

inline void Network::RegMem(Buffer &buf) {
    struct iovec iov = {buf.data, buf.size};
    struct fi_mr_attr mr_attr = {};
    mr_attr.mr_iov    = &iov;
    mr_attr.iov_count = 1;
    mr_attr.access    = FI_SEND | FI_RECV;
    struct fid_mr *mr;
    FI_CHECK(fi_mr_regattr(domain, &mr_attr, 0, &mr));
    mr_map[buf.data] = mr;
}

inline void Network::RegMemRma(Buffer &buf) {
    struct iovec iov = {buf.data, buf.size};
    struct fi_mr_attr mr_attr = {};
    mr_attr.mr_iov    = &iov;
    mr_attr.iov_count = 1;
    mr_attr.access    = FI_READ | FI_WRITE | FI_REMOTE_READ | FI_REMOTE_WRITE;
    struct fid_mr *mr;
    FI_CHECK(fi_mr_regattr(domain, &mr_attr, 0, &mr));
    mr_map[buf.data] = mr;
}

inline uint64_t Network::RKey(const Buffer &buf) {
    return fi_mr_key(MR(buf));
}

inline uint64_t Network::VAddr(const Buffer &buf) {
    return (uint64_t)buf.data;
}

inline struct fid_mr *Network::MR(const Buffer &buf) {
    auto it = mr_map.find(buf.data);
    CHECK(it != mr_map.end());
    return it->second;
}

inline void Network::PostRecv(Buffer &buf, std::function<void(Network &, RdmaOp &)> cb) {
    auto *op = new RdmaOp{RdmaOpType::kRecv, &buf, 0, FI_ADDR_UNSPEC, 0, 0, std::move(cb)};
    pending.push_back(op);
    Progress();
}

inline void Network::PostSend(fi_addr_t dest, Buffer &buf, size_t len,
                               std::function<void(Network &, RdmaOp &)> cb) {
    CHECK(len <= buf.size);
    auto *op = new RdmaOp{RdmaOpType::kSend, &buf, len, dest, 0, 0, std::move(cb)};
    pending.push_back(op);
    Progress();
}

inline void Network::PostWrite(fi_addr_t dest, Buffer &local_buf, size_t len,
                                uint64_t remote_addr, uint64_t remote_key,
                                std::function<void(Network &, RdmaOp &)> cb) {
    CHECK(len <= local_buf.size);
    auto *op = new RdmaOp{RdmaOpType::kWrite, &local_buf, len, dest,
                           remote_addr, remote_key, std::move(cb)};
    pending.push_back(op);
    Progress();
}

inline void Network::PostRead(fi_addr_t dest, Buffer &local_buf, size_t len,
                               uint64_t remote_addr, uint64_t remote_key,
                               std::function<void(Network &, RdmaOp &)> cb) {
    CHECK(len <= local_buf.size);
    auto *op = new RdmaOp{RdmaOpType::kRead, &local_buf, len, dest,
                           remote_addr, remote_key, std::move(cb)};
    pending.push_back(op);
    Progress();
}

inline void Network::Progress() {
    while (!pending.empty()) {
        auto *op = pending.front();
        pending.pop_front();

        ssize_t ret;

        if (op->type == RdmaOpType::kRecv || op->type == RdmaOpType::kSend) {
            struct iovec iov;
            iov.iov_base = op->buf->data;
            iov.iov_len  = (op->type == RdmaOpType::kRecv) ? op->buf->size : op->len;
            struct fi_msg msg = {};
            msg.msg_iov   = &iov;
            msg.desc      = &MR(*op->buf)->mem_desc;
            msg.iov_count = 1;
            msg.addr      = (op->type == RdmaOpType::kRecv) ? FI_ADDR_UNSPEC : op->addr;
            msg.context   = op;
            ret = (op->type == RdmaOpType::kRecv)
                ? fi_recvmsg(ep, &msg, 0)
                : fi_sendmsg(ep, &msg, FI_COMPLETION);
            fprintf(stderr, "DBG: post %s ret=%zd\n",
                    op->type == RdmaOpType::kRecv ? "recv" : "send", ret);
        } else {
            struct iovec iov = {op->buf->data, op->len};
            struct fi_rma_iov rma_iov = {op->remote_addr, op->len, op->remote_key};
            struct fi_msg_rma msg = {};
            msg.msg_iov       = &iov;
            msg.desc          = &MR(*op->buf)->mem_desc;
            msg.iov_count     = 1;
            msg.addr          = op->addr;
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

inline void Network::HandleCQE(const struct fi_cq_data_entry &cqe) {
    auto *op = (RdmaOp *)cqe.op_context;
    if (!op) return;
    if (cqe.flags & FI_RECV) op->len = cqe.len;
    if (op->cb) op->cb(*this, *op);
    delete op;
}

inline void Network::Poll() {
    static uint64_t poll_count = 0;
    if (++poll_count % 1000000 == 0)
        fprintf(stderr, "DBG: polled %lu times, pending=%zu\n",
                poll_count, pending.size());
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

inline Network::~Network() {
    for (auto &[_, mr] : mr_map) fi_close(&mr->fid);
    if (ep)     fi_close(&ep->fid);
    if (av)     fi_close(&av->fid);
    if (cq)     fi_close(&cq->fid);
    if (domain) fi_close(&domain->fid);
    if (fabric) fi_close(&fabric->fid);
    fi_freeinfo(fi);
}

inline Network::Network(fi_info *fi, fid_fabric *fabric, fid_domain *domain,
                         fid_cq *cq, fid_av *av, fid_ep *ep, EfaAddress addr)
    : fi(fi), fabric(fabric), domain(domain), cq(cq), av(av), ep(ep), addr(addr) {}

inline Network::Network(Network &&o)
    : fi(o.fi), fabric(o.fabric), domain(o.domain),
      cq(o.cq), av(o.av), ep(o.ep), addr(o.addr) {
    o.fi = nullptr; o.fabric = nullptr; o.domain = nullptr;
    o.cq = nullptr; o.av    = nullptr; o.ep     = nullptr;
}
