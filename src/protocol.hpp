#pragma once
#include <stdint.h>
#include <stddef.h>

static constexpr size_t kMaxKeySize   = 256;
static constexpr size_t kMaxValueSize = 1u * 1024 * 1024;  // 1 MB
static constexpr size_t kMsgBufSize   = 32 + kMaxKeySize + kMaxValueSize;

enum class MsgType : uint8_t {
    kConnect  = 0,
    kPut      = 1,
    kGet      = 2,
    kDelete   = 3,
    kResponse = 4,
};

enum class StatusCode : uint8_t {
    kOk       = 0,
    kNotFound = 1,
    kError    = 2,
};

// CONNECT  → [MsgType=kConnect][EFA addr: 32 bytes]
struct ConnectMsg {
    MsgType type;
    uint8_t addr[32];
} __attribute__((packed));

// PUT      → [RequestHeader][key: key_len bytes][value: val_len bytes]
// GET/DEL  → [RequestHeader][key: key_len bytes]  (val_len == 0)
struct RequestHeader {
    MsgType  type;
    uint32_t key_len;
    uint32_t val_len;
} __attribute__((packed));

// Response → [ResponseHeader][value: val_len bytes]  (val only for GET)
struct ResponseHeader {
    StatusCode status;
    uint32_t   val_len;
} __attribute__((packed));

static inline size_t connect_msg_size()            { return sizeof(ConnectMsg); }
static inline size_t request_size(size_t kl, size_t vl) {
    return sizeof(RequestHeader) + kl + vl;
}
static inline size_t response_size(size_t vl)      { return sizeof(ResponseHeader) + vl; }
