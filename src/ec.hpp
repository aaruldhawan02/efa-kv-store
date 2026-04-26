#pragma once
// ISA-L Reed-Solomon erasure coding wrapper.
// k = data shards, m = parity shards.  Object is split into k equal fragments;
// m parity fragments are computed.  Any k of k+m fragments can reconstruct.

#include <isa-l.h>
#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <vector>

static constexpr size_t kECAlign = 64;  // ISA-L works best on 64-byte aligned sizes

// Round up n to the nearest multiple of align
static inline size_t round_up(size_t n, size_t align) {
    return (n + align - 1) & ~(align - 1);
}

struct ErasureCoder {
    int k;  // data shards
    int m;  // parity shards

    // encode_matrix: (k+m) x k, where the first k rows are identity
    std::vector<uint8_t> encode_matrix;
    // encode_gftbls: GF tables for the m parity rows
    std::vector<uint8_t> encode_gftbls;

    ErasureCoder(int k, int m) : k(k), m(m),
        encode_matrix((k + m) * k),
        encode_gftbls(32 * k * m) {
        gf_gen_cauchy1_matrix(encode_matrix.data(), k + m, k);
        ec_init_tables(k, m, encode_matrix.data() + k * k, encode_gftbls.data());
    }

    // Encode `data` (any size) into k+m shards.
    // Returns vector of (k+m) shard buffers, each of size shard_size().
    // shard_size is ceil(data.size() / k) rounded to kECAlign.
    std::vector<std::vector<uint8_t>> Encode(const uint8_t *data, size_t data_len) const {
        size_t frag = round_up((data_len + k - 1) / k, kECAlign);

        // Build k data shard pointers (padded)
        std::vector<std::vector<uint8_t>> shards(k + m, std::vector<uint8_t>(frag, 0));

        // Split input into k data shards
        for (int i = 0; i < k; i++) {
            size_t off = (size_t)i * frag;
            size_t copy = (off < data_len) ? std::min(frag, data_len - off) : 0;
            if (copy > 0) memcpy(shards[i].data(), data + off, copy);
        }

        // Build pointer arrays for ISA-L
        std::vector<uint8_t *> data_ptrs(k), parity_ptrs(m);
        for (int i = 0; i < k; i++) data_ptrs[i]   = shards[i].data();
        for (int i = 0; i < m; i++) parity_ptrs[i] = shards[k + i].data();

        ec_encode_data((int)frag, k, m,
                       const_cast<uint8_t *>(encode_gftbls.data()),
                       data_ptrs.data(), parity_ptrs.data());

        return shards;
    }

    // Decode k shards (from any k of k+m positions) back to original data.
    // `present[i]` = true means shards[i] is available (i in 0..k+m-1).
    // `original_len` is needed to strip padding from the last data shard.
    // Returns the reconstructed object, or empty vector on failure.
    std::vector<uint8_t> Decode(
        const std::vector<std::vector<uint8_t>> &shards,  // size k+m, missing ones can be empty
        const std::vector<bool> &present,
        size_t original_len) const {

        size_t frag = shards[0].empty() ? 0 : shards[0].size();
        if (!frag) {
            // find frag from any present shard
            for (int i = 0; i < k + m; i++)
                if (present[i]) { frag = shards[i].size(); break; }
        }
        if (!frag) return {};

        // Collect the first k present shard indices
        std::vector<int> src_idx;
        for (int i = 0; i < k + m && (int)src_idx.size() < k; i++)
            if (present[i]) src_idx.push_back(i);
        if ((int)src_idx.size() < k) return {};

        // Check if we already have all k data shards
        bool all_data = true;
        for (int i = 0; i < k; i++) if (src_idx[i] != i) { all_data = false; break; }

        if (all_data) {
            // No decoding needed — just concatenate
            std::vector<uint8_t> out(original_len);
            for (int i = 0; i < k; i++) {
                size_t off  = (size_t)i * frag;
                size_t copy = (off < original_len) ? std::min(frag, original_len - off) : 0;
                if (copy > 0) memcpy(out.data() + off, shards[i].data(), copy);
            }
            return out;
        }

        // Build sub-matrix from encode_matrix rows for src_idx
        std::vector<uint8_t> sub_matrix(k * k);
        for (int i = 0; i < k; i++)
            memcpy(sub_matrix.data() + i * k,
                   encode_matrix.data() + src_idx[i] * k, k);

        // Invert sub-matrix
        std::vector<uint8_t> inv_matrix(k * k);
        if (gf_invert_matrix(sub_matrix.data(), inv_matrix.data(), k) < 0) {
            fprintf(stderr, "EC decode: matrix inversion failed\n");
            return {};
        }

        // Decode tables
        std::vector<uint8_t> decode_gftbls(32 * k * k);
        ec_init_tables(k, k, inv_matrix.data(), decode_gftbls.data());

        // Input: the k available shards
        std::vector<const uint8_t *> in_ptrs(k);
        for (int i = 0; i < k; i++) in_ptrs[i] = shards[src_idx[i]].data();

        // Output: k recovered data shards
        std::vector<std::vector<uint8_t>> recovered(k, std::vector<uint8_t>(frag));
        std::vector<uint8_t *> out_ptrs(k);
        for (int i = 0; i < k; i++) out_ptrs[i] = recovered[i].data();

        ec_encode_data((int)frag, k, k, decode_gftbls.data(),
                       const_cast<uint8_t **>(in_ptrs.data()), out_ptrs.data());

        // Reassemble
        std::vector<uint8_t> result(original_len);
        for (int i = 0; i < k; i++) {
            size_t off  = (size_t)i * frag;
            size_t copy = (off < original_len) ? std::min(frag, original_len - off) : 0;
            if (copy > 0) memcpy(result.data() + off, recovered[i].data(), copy);
        }
        return result;
    }
};
