#pragma once
#include <vector>
#include <bit>
#include <functional>
#include <cstddef>
#include <limits>

template <typename T, typename Value = size_t>
struct Hashalgorithm{
    size_t N;

    struct Entry{
        T key;
        std::vector<Value> values;
        int psl;
        bool occupied;

        Entry(T k = T{}, const std::vector<Value>& vec = {}, int p = -1, bool occ = false) : key(k), values(vec), psl(p), occupied(occ){};
    };

    std::vector<Entry> hashtable;

    size_t nextpow2(size_t n){
        if(n <= 1) return 1;
        n--;
        n |= n >> 1;
        n |= n >> 2;
        n |= n >> 4;
        n |= n >> 8;
        n |= n >> 16;
    #if ULONG_MAX > 0xFFFFFFFF
        n |= n >> 32; // for 64 bit values
    #endif
        return n + 1;
    }

    Hashalgorithm(size_t size){ // constructor
        N = nextpow2(size) * 2;
        hashtable.resize(N, Entry());
    }

    size_t hash_function(const T& key) const{
        if constexpr(std::is_same_v<T, int32_t> || std::is_same_v<T, uint32_t>){  
            // CRC 
            uint32_t x = static_cast<uint32_t>(key);
            x ^= x >> 16;
            x *= 0x85ebca6b;
            x ^= x >> 13;
            x *= 0xc2b2ae35;
            x ^= x >> 16;
            return x & (N-1);

        }
        else{
            return std::hash<T>{}(key) & (N-1);
        }
    }

    void insert(const T& inputkey, const std::vector<Value>& inputvalues){
        T key = inputkey;
        std::vector<Value> values = inputvalues;
        size_t pos = hash_function(key);
        int psl = 0;
        Entry current(key, values, psl, true);

        while(1){
            if(!hashtable[pos].occupied){ // empty spot
                hashtable[pos] = current;
                hashtable[pos].occupied = true;
                return;
            }
            if(hashtable[pos].key == key){ // key already exists, just add the value
                hashtable[pos].values.insert(hashtable[pos].values.end(), values.begin(), values.end());
                return;
            }
            if(current.psl > hashtable[pos].psl){
                std::swap(hashtable[pos], current);
                hashtable[pos].occupied = true;
            }
            current.psl++;
            if(++pos == N) pos = 0; // in case the table ends, we have to go check at the beginning
        }
    }

    std::vector<Value> find_values(const T& key) const{
        size_t pos = hash_function(key);
        int psl = 0;

        while(1){
            if(!hashtable[pos].occupied) return {}; // empty spot
            if(hashtable[pos].key == key) return hashtable[pos].values; // found
            if(psl > hashtable[pos].psl) return {}; // if the psl of the key we are looking for is greater than the psl of the current element, then it means that the key is not in the table
            psl++;
            if(++pos == N) pos = 0; // in case the table ends, we have to go check at the beginning
        }
    }
};