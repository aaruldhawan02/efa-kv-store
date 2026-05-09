#pragma once
#include <stdint.h>
#include <stddef.h>

static constexpr size_t kMaxKeySize   = 256;
static constexpr size_t kMaxShardSize = 1u * 1024 * 1024;  // 1 MB per slot
static constexpr size_t kCtrlBufSize  = 128 + kMaxKeySize;  // control messages only
static constexpr size_t kDataBufSize  = kMaxShardSize;      // local RMA buffer

// ── Message types ─────────────────────────────────────────────────────────────
// Bulk shard data moves via one-sided RDMA write/read; these messages are
// small control signals only.

enum class MsgType : uint8_t {
    kPutRequest  = 1,  // client requests a slot to write into
    kSlotGrant   = 2,  // server grants slot: remote_addr + rkey
    kPutCommit   = 3,  // client signals RDMA write is complete
    kGetRequest  = 4,  // client requests location of a stored shard
    kSlotInfo    = 5,  // server replies with remote_addr + rkey + lengths
    kDelRequest  = 6,  // client requests deletion
    kAck         = 7,  // generic ack (status)
    kDiskRelease = 8,  // client → server: release staging slot after RDMA read
};

enum class StatusCode : uint8_t {
    kOk       = 0,
    kNotFound = 1,
    kFull     = 2,
    kError    = 3,
};

// ── Wire structures ───────────────────────────────────────────────────────────

// [PutRequestMsg][key_len bytes]
struct PutRequestMsg {
    MsgType  type;
    uint8_t  _pad[3];
    uint32_t key_len;
    uint32_t shard_len;
    uint32_t object_size;
    uint8_t  shard_idx;
    uint8_t  k;
    uint8_t  m;
    uint8_t  _pad2;
} __attribute__((packed));

struct SlotGrantMsg {
    MsgType  type;
    uint8_t  _pad[3];
    uint32_t slot_idx;
    uint64_t remote_addr;
    uint64_t rkey;
} __attribute__((packed));

struct PutCommitMsg {
    MsgType  type;
    uint8_t  _pad[3];
    uint32_t slot_idx;
} __attribute__((packed));

// [GetRequestMsg][key_len bytes]
struct GetRequestMsg {
    MsgType  type;
    uint8_t  shard_idx;
    uint8_t  _pad[2];
    uint32_t key_len;
} __attribute__((packed));

struct SlotInfoMsg {
    MsgType    type;
    StatusCode status;
    uint8_t    shard_idx;
    uint8_t    on_disk;          // 0 = RDMA pool, 1 = disk staging buffer
    uint32_t   shard_len;
    uint32_t   object_size;
    uint64_t   remote_addr;
    uint64_t   rkey;
    uint32_t   staging_slot_idx; // valid when on_disk=1; client echoes in DiskRelease
    uint32_t   _pad2;
} __attribute__((packed));

// [DelRequestMsg][key_len bytes]
struct DelRequestMsg {
    MsgType  type;
    uint8_t  shard_idx;
    uint8_t  _pad[2];
    uint32_t key_len;
} __attribute__((packed));

struct AckMsg {
    MsgType    type;
    StatusCode status;
    uint8_t    shard_idx;
    uint8_t    _pad;
} __attribute__((packed));

// Client → server: release staging slot after RDMA read completes.
struct DiskReleaseMsg {
    MsgType  type;
    uint8_t  _pad[3];
    uint32_t staging_slot_idx;
} __attribute__((packed));
