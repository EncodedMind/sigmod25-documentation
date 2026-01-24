#pragma once
#include <vector>
#include <bit>
#include <functional>
#include <bitset>
#include <climits>
#include <cstdint>

template <typename T, typename Value = size_t>
struct Hashalgorithm{
    static constexpr int H = 32; // Neighborhood size (original paper suggests 32 or 64)
    size_t N;

    struct Entry{
        T key;
        std::vector<Value> values;
        std::bitset<H> bitmap;
        bool occupied;

        Entry(T k = T{}, const std::vector<Value>& vec = {}, bool occ = false) : key(k), values(vec), bitmap(0), occupied(occ){};
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

    void rehash(){
        std::vector<Entry> old = hashtable;

        N *= 2;

        hashtable.clear();
        hashtable.resize(N);

        for(auto& e : old){
            if(e.occupied) insert(e.key, e.values);
        }
    }

    void insert(const T& inputkey, const std::vector<Value>& inputvalues){
        T key = inputkey;
        std::vector<Value> values = inputvalues;
        size_t i = hash_function(key);

        // if key already exists, check both tables and append the value (iterate only 1s)
        unsigned long long bits = hashtable[i].bitmap.to_ullong();
        while(bits){
            int offset = __builtin_ctzll(bits);
            size_t pos = (i + offset) & (N-1);
            if(hashtable[pos].occupied && hashtable[pos].key == key){
                hashtable[pos].values.insert(hashtable[pos].values.end(), values.begin(), values.end());
                return;
            }
            bits &= bits - 1;
        }

        if(hashtable[i].bitmap.all()){
            rehash();
            insert(key, values);
            return;
        }

        size_t j = i;
        while(hashtable[j].occupied){ // Find next free spot
            j = (j+1) & (N-1);
            // if(j == i){ // Full table - Will never happen
            //     cout << "RESIZE NEEDED-2\n";
            //     exit(2);
            // }
        }
        // now j is the free spot

        while(((j-i+N) & (N-1)) >= (size_t)H){
            int y = -1;

            for(int offset = H-1; offset > 0; --offset){
                size_t k = ((j - offset + N) & (N-1));
                if(!hashtable[k].occupied) continue;

                size_t hash = hash_function(hashtable[k].key);

                size_t distk = (k-hash+N)&(N-1);
                if(distk >= (size_t)H) continue;

                if(((j-hash+N)&(N-1)) < (size_t)H && hashtable[hash].bitmap.test((int)distk)){
                    y = (int)k;
                    break;
                }
            }

            if(y == -1){  // y not found
                rehash();
                insert(key, values);
                return;
            }

            size_t hashy = hash_function(hashtable[y].key);
            hashtable[j].key = std::move(hashtable[y].key);
            hashtable[j].values = std::move(hashtable[y].values);
            hashtable[j].occupied = true;
            // hashtable[j].home = hashy;
            hashtable[y].occupied = false;
            hashtable[y].values.clear();

            hashtable[hashy].bitmap.reset((y-hashy+N)&(N-1));
            hashtable[hashy].bitmap.set((j-hashy+N)&(N-1));
            j = (size_t)y; // New free spot
        }

        hashtable[j].key = key;
        hashtable[j].values = values;
        hashtable[j].occupied = true;
        hashtable[i].bitmap.set((j-i+N)&(N-1));
    }

    std::vector<Value> find_values(const T& key) const{
        size_t i = hash_function(key);

        unsigned long long bits = hashtable[i].bitmap.to_ullong();
        while(bits){
            int offset = __builtin_ctzll(bits);
            size_t pos = (i + offset) & (N-1);
            if(hashtable[pos].occupied && hashtable[pos].key == key) return hashtable[pos].values;
            bits &= bits - 1;
        }  
        return {};
    }
};
