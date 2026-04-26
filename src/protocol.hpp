#pragma once
#include <stdint.h>
#include <stddef.h>

// Max sizes.  Shard size = ceil(object_size / k), so with k=2 and 1MB objects
// each shard is ≤ 512KB.  We size the buffer for worst-case 1 shard = 1MB.
static constexpr size_t kMaxKeySize   = 256;
static constexpr size_t kMaxShardSize = 1u * 1024 * 1024;   // 1 MB per shard
static constexpr size_t kMsgBufSize   =
    64 + kMaxKeySize + kMaxShardSize;  // header + key + shard payload

// ── Message types ────────────────────────────────────────────────────────────

enum class MsgType : uint8_t {
    kConnect   = 0,  // exchange EFA addresses
    kPutShard  = 1,  // store one shard
    kGetShard  = 2,  // retrieve one shard
    kDelShard  = 3,  // delete one shard
    kResponse  = 4,
};

enum class StatusCode : uint8_t {
    kOk       = 0,
    kNotFound = 1,
    kError    = 2,
};

// ── Wire structures (packed, no padding) ─────────────────────────────────────

// kConnect: [ConnectMsg]
struct ConnectMsg {
    MsgType type;       // kConnect
    uint8_t addr[32];   // sender's EFA address
} __attribute__((packed));

// kPutShard / kGetShard / kDelShard: [ShardRequestHeader][key bytes][shard bytes]
//   For GET and DEL, shard_len == 0 and shard_size == 0.
//   shard_size is the unpadded object size (needed for reconstruction).
struct ShardRequestHeader {
    MsgType  type;
    uint32_t key_len;
    uint32_t shard_len;    // bytes of shard data that follow (0 for GET/DEL)
    uint32_t object_size;  // original full object size (for Decode)
    uint8_t  shard_idx;    // which shard this is (0..k+m-1)
    uint8_t  k;            // data shards (needed by server to label storage)
    uint8_t  m;            // parity shards
    uint8_t  _pad;
} __attribute__((packed));

// kResponse: [ResponseHeader][shard bytes]
//   shard bytes are present only for kGetShard responses.
//   shard_idx lets the client match replies that arrive out of order.
struct ResponseHeader {
    StatusCode status;
    uint8_t    shard_idx;
    uint32_t   shard_len;   // bytes of shard data that follow
    uint32_t   object_size;
} __attribute__((packed));

// ── Helpers ───────────────────────────────────────────────────────────────────

static inline size_t connect_msg_bytes()                     { return sizeof(ConnectMsg); }
static inline size_t shard_request_bytes(size_t kl, size_t sl) {
    return sizeof(ShardRequestHeader) + kl + sl;
}
static inline size_t response_bytes(size_t sl) {
    return sizeof(ResponseHeader) + sl;
}
