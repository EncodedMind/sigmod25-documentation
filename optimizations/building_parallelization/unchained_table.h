#pragma once
// Unchained hash table implementation based on:
// https://db.in.tum.de/~birler/papers/hashtable.pdf

#include <cstdint>
#include <vector>

extern const uint16_t tags[1 << 11];

// Entry: key + row index
struct HashEntry {
    int32_t key;
    size_t row_idx;
    
    HashEntry() : key(0), row_idx(0) {}
    HashEntry(int32_t k, size_t r) : key(k), row_idx(r) {}
};

struct UnchainedHashTable {
    HashEntry* tuple_storage;
    uint64_t* directory;
    uint64_t shift;
    uint64_t capacity;
    size_t num_elements;
    std::vector<HashEntry> temp_entries; // Temporary storage during build
    
    UnchainedHashTable() 
        : tuple_storage(nullptr)
        , directory(nullptr)
        , shift(0)
        , capacity(0)
        , num_elements(0) {}
    
    ~UnchainedHashTable() {
        delete[] tuple_storage;
        if (directory) delete[] (directory - 1);
    }
    
    UnchainedHashTable(const UnchainedHashTable&) = delete;
    UnchainedHashTable& operator=(const UnchainedHashTable&) = delete;
    
    void reserve(size_t build_size) {
        // Find next power of 2, minimum 1024 slots
        shift = 10;
        while ((1ull << shift) < build_size) shift++;
        capacity = 1ull << shift;
        
        tuple_storage = new HashEntry[build_size]();
        uint64_t* dir_alloc = new uint64_t[capacity + 1]();
        dir_alloc[0] = reinterpret_cast<uint64_t>(tuple_storage) << 16;
        directory = dir_alloc + 1;
        shift = 64 - shift;
        
        temp_entries.reserve(build_size);
    }
    
    // Just accumulate entries - no duplicate checking needed
    void insert(int32_t key, size_t row_idx) {
        temp_entries.emplace_back(key, row_idx);
    }
    
    // Three-phase build process
    void finalize() {
        num_elements = temp_entries.size();
        
        // Phase 1: Count tuples per slot and build Bloom filters
        for (const auto& entry : temp_entries) {
            uint64_t h = hash(entry.key);
            uint64_t slot = h >> shift;
            directory[slot] += static_cast<uint64_t>(sizeof(HashEntry)) << 16;
            directory[slot] |= compute_tag(h);
        }
        
        // Phase 2: Prefix sum to compute final positions
        uint8_t* cur = reinterpret_cast<uint8_t*>(tuple_storage);
        for (uint64_t i = 0; i < capacity; ++i) {
            uint64_t byte_count = directory[i] >> 16;
            uint16_t bloom = static_cast<uint16_t>(directory[i]);
            directory[i] = (reinterpret_cast<uint64_t>(cur) << 16) | bloom;
            cur += byte_count;
        }
        
        // Phase 3: Place entries in their final positions
        for (const auto& entry : temp_entries) {
            uint64_t h = hash(entry.key);
            uint64_t slot = h >> shift;
            HashEntry* target = reinterpret_cast<HashEntry*>(directory[slot] >> 16);
            *target = entry;
            directory[slot] += static_cast<uint64_t>(sizeof(HashEntry)) << 16;
        }
        
        // Free temporary storage
        temp_entries.clear();
    }
    
    // Find all row indices matching the key
    const HashEntry* find_range(int32_t key, size_t& len) const {
        uint64_t h = hash(key);
        uint64_t slot = h >> shift;
        
        // Bloom filter check
        uint16_t bloom = static_cast<uint16_t>(directory[slot]);
        if (!could_contain(bloom, h)){
            len = 0;
            return nullptr;
        }
        
        // Get range of entries for this slot
        uint64_t prev_dir = (slot == 0) ? directory[-1] : directory[slot - 1];
        HashEntry* start = reinterpret_cast<HashEntry*>(prev_dir >> 16);
        HashEntry* end = reinterpret_cast<HashEntry*>(directory[slot] >> 16);
        
        len = end - start;
        return start;
    }
    
    size_t size() const { return num_elements; }
    
    static uint64_t hash(int32_t key) {

        uint32_t crc = 0;
        #if defined(__x86_64__) || defined(__i386__)
            crc = __builtin_ia32_crc32si(static_cast<uint32_t>(key), 0);
        #elif defined(__aarch64__)
            crc = __builtin_arm_crc32w(static_cast<uint32_t>(key), 0);
        #else
            crc = static_cast<uint32_t>(key);
        #endif
        return static_cast<uint64_t>(crc) * ((0x8648DBDull << 32) + 1);

        // CRC32 hash with Fibonacci multiplicative constant
        // uint32_t crc = __builtin_ia32_crc32si(static_cast<uint32_t>(key), 0);
        // return static_cast<uint64_t>(crc) * ((0x8648DBDull << 32) + 1);
    }
    
    uint16_t compute_tag(uint64_t h) const {
        uint16_t prefix = (static_cast<uint32_t>(h) >> 21) & 0x7FF; // 11 bits
        return tags[prefix];
    }
    
    bool could_contain(uint16_t bloom, uint64_t h) const {
        uint16_t tag = compute_tag(h);
        return (tag & ~bloom) == 0;
    }
};