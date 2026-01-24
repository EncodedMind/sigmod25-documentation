#pragma once
#include <vector>
#include <bit>
#include <functional>
#include <cstddef>
#include <limits>

template <typename T, typename Value = size_t>
struct Hashalgorithm{
    size_t N;
    size_t inserted;

    struct Entry{
        T key;
        std::vector<Value> values;
        bool occupied;

        Entry(T k = T{}) : key(k), occupied(false){}
    };

    std::vector<Entry> hashtable1, hashtable2;

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
        if(N < 16) N = 16;  // minimum size
        hashtable1.resize(N, Entry());
        hashtable2.resize(N, Entry());
        inserted = 0;
    }

    size_t hash_function1(const T& key) const{
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

    size_t hash_function2(const T& key) const{
        if constexpr(std::is_same_v<T, int32_t> || std::is_same_v<T, uint32_t>){
            // Fibonacci constant
            constexpr uint64_t fib32 = 2654435769U; // 2^32 / golden ratio
            uint32_t x = static_cast<uint32_t>(key);
            return (x * fib32) & (N-1);
        }
        else{
            return std::hash<T>{}(key) & (N-1);
        }
    }

    void rehash(){
        std::vector<Entry> old1 = std::move(hashtable1);
        std::vector<Entry> old2 = std::move(hashtable2);

        N *= 2;
        inserted = 0;

        hashtable1.clear();
        hashtable1.resize(N);
        hashtable2.clear();
        hashtable2.resize(N);

        for(auto& e : old1){
            if(e.occupied) insert(e.key, e.values);
        }
        for(auto& e : old2){
            if(e.occupied) insert(e.key, e.values);
        }
    }

    void insert(const T& inputkey, const std::vector<Value>& inputvalues){
        T key = inputkey;
        std::vector<Value> values = inputvalues;
        bool table1 = true;
        Entry current;
        current.key = key;
        current.values = values;
        current.occupied = true;

        int kicks = 0;

        // if key already exists, check both tables and append the value
        size_t hash1 = hash_function1(key);
        size_t hash2 = hash_function2(key);

        if(hashtable1[hash1].occupied && hashtable1[hash1].key == key){
            hashtable1[hash1].values.insert(hashtable1[hash1].values.end(), values.begin(), values.end());
            return;
        }
        
        if(hashtable2[hash2].occupied && hashtable2[hash2].key == key){
            hashtable2[hash2].values.insert(hashtable2[hash2].values.end(), values.begin(), values.end());
            return;
        }

        while(1){
            if(table1){
                size_t hash1 = hash_function1(current.key);
                if(!hashtable1[hash1].occupied){
                    hashtable1[hash1] = std::move(current);
                    inserted++;
                    return;
                }
                std::swap(hashtable1[hash1], current);

                kicks++;
                table1 = false;
            }
            else{
                size_t hash2 = hash_function2(current.key);
                if(!hashtable2[hash2].occupied){
                    hashtable2[hash2] = std::move(current);
                    inserted++;
                    return;
                }
                std::swap(hashtable2[hash2], current);

                kicks++;
                table1 = true;
            }

            if(kicks >= inserted){
                rehash();
                insert(current.key, current.values);
                return;
            }
        }
    }

    std::vector<Value> find_values(const T& key) const{
        size_t pos1 = hash_function1(key);
        if(hashtable1[pos1].occupied && hashtable1[pos1].key == key) return hashtable1[pos1].values;
        size_t pos2 = hash_function2(key);
        if(hashtable2[pos2].occupied && hashtable2[pos2].key == key) return hashtable2[pos2].values;
        return {};
    }
};