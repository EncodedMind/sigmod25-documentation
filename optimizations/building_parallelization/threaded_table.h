#pragma once
// Unchained hash table implementation based on:
// https://db.in.tum.de/~birler/papers/hashtable.pdf

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <vector>

extern const uint16_t tags[1 << 11];

namespace threaded{

// Entry: key + row index + hash
struct HashEntry {
    int32_t key;
    size_t row_idx;
    uint64_t hash;

    static uint64_t compute_hash(int32_t key) {
        uint32_t crc = 0;
        #if defined(__x86_64__) || defined(__i386__)
            crc = __builtin_ia32_crc32si(static_cast<uint32_t>(key), 0);
        #elif defined(__aarch64__)
            crc = __builtin_arm_crc32w(static_cast<uint32_t>(key), 0);
        #else
            crc = static_cast<uint32_t>(key);
        #endif
        return static_cast<uint64_t>(crc) * ((0x8648DBDull << 32) + 1);
    } 
    
    HashEntry() : key(0), row_idx(0), hash(compute_hash(0)) {}
    HashEntry(int32_t k, size_t r) : key(k), row_idx(r), hash(compute_hash(k)) {}
};

struct Block{
    Block* next;
    uint8_t* end_of_tuples;
};

struct GlobalAllocator{
    static constexpr size_t LARGE_CHUNK_SIZE = (2u << 20); // 2 MB

    void* allocateLargeChunk(){
        void* chunk = malloc(LARGE_CHUNK_SIZE);
        return chunk;
    }
};

struct BumpAllocL2{
    static constexpr size_t LARGE_CHUNK_SIZE = (2u << 20); // 2 MB
    static constexpr size_t SMALL_CHUNK_SIZE = (64u << 10); // 64 KB

    uint8_t* large_chunk = nullptr;  // where we are in the large chunk
    uint8_t* large_chunk_end = nullptr; // the end of the large chunk

    Block* head = nullptr; // the start of the list of large chunks

    ~BumpAllocL2(){ // to free all allocated chunks
        Block* current = head;
        while(current){
            Block* next = current->next;
            free(current);
            current = next;
        }
    }

    void addSpace(void* chunk){
        Block* new_block = static_cast<Block*>(chunk); // the start of the chunk will be attributed for the list node
        new_block->next = head;
        head = new_block;

        // add chunk to internal storage
        // we must skip the first sizeof(Block) bytes
        large_chunk = static_cast<uint8_t*>(chunk) + sizeof(Block);
        large_chunk_end = large_chunk + LARGE_CHUNK_SIZE - sizeof(Block);
    }

    void* allocateSmallChunk(){
        // take memory from the large chunk and return pointer
        void* chunk = large_chunk; // allocate 64KB from the pointer large_chunk
        large_chunk = large_chunk + SMALL_CHUNK_SIZE; // move pointer large_chunk 64KB ahead
        return chunk;
    }

    size_t freeSpace(){
        if(!large_chunk) return 0;
        return static_cast<size_t>(large_chunk_end - large_chunk);
    }
};

struct BumpAllocL3{
    static constexpr size_t SMALL_CHUNK_SIZE = (64u << 10); // 64 KB

    uint8_t* small_chunk = nullptr;  // where we are in the small chunk
    uint8_t* small_chunk_end = nullptr; // the end of the small chunk

    Block* head = nullptr; // the start of the list of small chunks

    // no destructor needed, all memory will be freed by the higher level allocator

    void addSpace(void* chunk){
        Block* new_block = static_cast<Block*>(chunk); // the start of the chunk will be attributed for the list node
        new_block->next = head;
        head = new_block;

        // add chunk to internal storage
        small_chunk = static_cast<uint8_t*>(chunk) + sizeof(Block);
        small_chunk_end = small_chunk + SMALL_CHUNK_SIZE - sizeof(Block);
        head->end_of_tuples = small_chunk; // initialization
    }
    
    HashEntry* allocate(){
        // take memory from the small chunk and return pointer
        HashEntry* entry = reinterpret_cast<HashEntry*>(small_chunk); // allocate sizeof(HashEntry) from the pointer small_chunk
        small_chunk = small_chunk + sizeof(HashEntry); // move pointer small_chunk sizeof(HashEntry) ahead
        head->end_of_tuples = small_chunk;
        return entry;
    }

    size_t freeSpace(){
        if(!small_chunk) return 0;
        return static_cast<size_t>(small_chunk_end - small_chunk);
    }
};

inline size_t log2_pow2(size_t n){
    size_t bits = 0;
    while ((static_cast<size_t>(1) << bits) < n) ++bits;
    return bits;
}

struct TupleCollector {
    uint64_t shift;
    size_t numPartitions;
    GlobalAllocator& level1;
    BumpAllocL2 level2;
    std::vector<BumpAllocL3> level3;
    std::vector<size_t> counts;
    
    TupleCollector(GlobalAllocator& globalAlloc, size_t partitions) : level1(globalAlloc), numPartitions(partitions) {
        level3.resize(numPartitions);
        counts.resize(numPartitions, 0);
        shift = static_cast<uint64_t>(log2_pow2(numPartitions));
    }
    
    TupleCollector(const TupleCollector&) = delete;
    TupleCollector& operator=(const TupleCollector&) = delete;

    void consume(HashEntry tuple){
        uint64_t part = (shift == 0) ? 0ull : (tuple.hash >> (uint64_t)(64u - shift));
        if(level3[part].freeSpace() < sizeof(HashEntry)){
            if(level2.freeSpace() < BumpAllocL2::SMALL_CHUNK_SIZE){
                void* LargeChunk = level1.allocateLargeChunk();
                level2.addSpace(LargeChunk);
            }
            void* SmallChunk = level2.allocateSmallChunk();
            level3[part].addSpace(SmallChunk);
        }
        *level3[part].allocate() = tuple;
        counts[part]++;
    }
};

struct FinalTable{
    HashEntry* tupleStorage;
    uint64_t* directory;
    uint64_t shift;
    size_t numPartitions;
    size_t num_elements;

    FinalTable(size_t total_tuples, size_t partitions) : tupleStorage(nullptr), directory(nullptr), numPartitions(partitions), num_elements(total_tuples){
        shift = 10;
        while ((1ull << shift) < total_tuples) shift++;
        size_t capacity = 1ull << shift;
        
        tupleStorage = static_cast<HashEntry*>(malloc(total_tuples * sizeof(HashEntry)));
        uint64_t* dir_alloc = new uint64_t[capacity + 1]();
        dir_alloc[0] = reinterpret_cast<uint64_t>(tupleStorage) << 16;
        directory = dir_alloc + 1;
        shift = 64 - shift;
    }

    ~FinalTable() {
        if(tupleStorage) free(tupleStorage);
        if(directory) delete[] (directory - 1);
    }

    FinalTable(const FinalTable&) = delete;
    FinalTable& operator=(const FinalTable&) = delete;

    void postProcessBuild(uint64_t partition, uint64_t prevCount, const std::vector<Block*>& partitionTuples){
        Block* curr_block = partitionTuples[partition];
        while(curr_block){
            uint8_t* tuple_ptr = reinterpret_cast<uint8_t*>(curr_block) + sizeof(Block);
            while(tuple_ptr < curr_block->end_of_tuples){
                HashEntry* tuple = reinterpret_cast<HashEntry*>(reinterpret_cast<uintptr_t>(tuple_ptr));
                uint64_t slot = tuple->hash >> shift;
                directory[slot] += static_cast<uint64_t>(sizeof(HashEntry)) << 16;
                directory[slot] |= computeTag(tuple->hash);
                tuple_ptr += sizeof(HashEntry);
            }
            curr_block = curr_block->next;
        }

        // prevCount is the total tuple count of previous partitions
        uint64_t cur = reinterpret_cast<uint64_t>(tupleStorage) + prevCount * sizeof(HashEntry);
        uint64_t k = 64 - shift ;
        uint64_t start = (partition << k) / numPartitions;
        uint64_t end = ((partition + 1) << k) / numPartitions;
        for(uint64_t i = start ; i < end ; ++i){
            uint64_t val = directory[i] >> 16;
            directory[i] = (cur << 16) | ((uint16_t)directory[i]);
            cur += val ;
        }

        curr_block = partitionTuples[partition];
        while(curr_block){
            uint8_t* tuple_ptr = reinterpret_cast<uint8_t*>(curr_block) + sizeof(Block);
            while(tuple_ptr < curr_block->end_of_tuples){
                HashEntry* tuple = reinterpret_cast<HashEntry*>(reinterpret_cast<uintptr_t>(tuple_ptr));
                uint64_t slot = tuple->hash >> shift;
                HashEntry* target = reinterpret_cast<HashEntry*>(directory[slot] >> 16);
                *target = *tuple;
                directory[slot] += sizeof(HashEntry) << 16;
                tuple_ptr += sizeof(HashEntry);
            }
            curr_block = curr_block->next;
        }
    }

    // Find all row indices matching the key
    const HashEntry* find_range(int32_t key, size_t& len) const {
        uint64_t h = HashEntry::compute_hash(key);
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

    uint16_t computeTag(uint64_t h) const {
        uint16_t prefix = (static_cast<uint32_t>(h) >> 21) & 0x7FF; // 11 bits
        return tags[prefix];
    }
    
    bool could_contain(uint16_t bloom, uint64_t h) const {
        uint16_t tag = computeTag(h);
        return (tag & ~bloom) == 0;
    }
};

// thread merging stage
// merge all lists of chunks of partition into one list

inline std::vector<Block*> merge_partitions(const std::vector<std::unique_ptr<TupleCollector>>& threadTables, size_t numPartitions){

    std::vector<Block*> partition_heads(numPartitions, nullptr);

    for(size_t p = 0; p < numPartitions; ++p){
        Block* link_head = nullptr;
        Block* tail = nullptr;

        for(const auto& threadTablePtr : threadTables){
            const auto& threadTable = *threadTablePtr;
            Block* current = threadTable.level3[p].head;
            if(!current) continue;
            if(!link_head){
                link_head = current;
                tail = link_head;
            }
            else{
                tail->next = current;
            }
            while(tail->next){
                tail = tail->next;
            }
        }
        partition_heads[p] = link_head;
    }

    return partition_heads;
}

} // namespace threaded