#pragma once
#include <vector>
#include <cstdint>
#include <cstring>
#include <value_t.h>
#include <plan.h>

namespace columnt{

    constexpr size_t PAGE_SIZE = 8192;
    constexpr size_t VALUES_PER_PAGE = PAGE_SIZE / sizeof(valuet::value_t);

    struct alignas(8) Intermediate_Page{
        valuet::value_t data[VALUES_PER_PAGE];

        Intermediate_Page() = default;
    };

    struct column_t{
        std::vector<Intermediate_Page*> pages;
        size_t num_values = 0;
        Column* ref = nullptr;

        mutable valuet::value_t ref_cache;

        column_t() = default;

        ~column_t(){
            if(!ref){
                for(auto* page : pages){
                    delete page;
                }
            }
        }

        column_t(column_t&& other) noexcept : pages(std::move(other.pages)), num_values(other.num_values), ref(other.ref){
            other.num_values = 0;
            other.ref = nullptr;
        }

        column_t(const column_t&) = delete;
        column_t& operator=(const column_t&) = delete;
        column_t& operator=(column_t&&) = delete;

        void push_back(const valuet::value_t& value){
            size_t page_idx = num_values / VALUES_PER_PAGE;
            size_t offset = num_values % VALUES_PER_PAGE;
            
            if(page_idx >= pages.size()){
                pages.push_back(new Intermediate_Page());
            }
            
            pages[page_idx]->data[offset] = value;
            num_values++;
        }

        size_t size() const{
            return num_values;
        }

        valuet::value_t& operator[](size_t idx){
            if(!ref){
                size_t page_idx = idx / VALUES_PER_PAGE;
                size_t offset = idx % VALUES_PER_PAGE;
                return pages[page_idx]->data[offset];
            }
            else{
                // For INT32 page:
                // header + data + bitman <= 8192
                // 4 + 4*n + ceil(n/8) <= 8192
                // n <= 1984.97
                // max(n) = 1984
                constexpr size_t ROWS_PER_PAGE = 1984;

                size_t page_idx = idx / ROWS_PER_PAGE;
                size_t offset = idx % ROWS_PER_PAGE;

                const std::byte* page = ref->pages[page_idx]->data;
                const int32_t* data_begin = reinterpret_cast<const int32_t*>(page + 4);
                int32_t value = data_begin[offset];
                ref_cache = valuet::value_t(value);
                return ref_cache;
            }
        }
        
        const valuet::value_t& operator[](size_t idx) const{
            if(!ref){
                size_t page_idx = idx / VALUES_PER_PAGE;
                size_t offset = idx % VALUES_PER_PAGE;
                return pages[page_idx]->data[offset];
            }
            else{
                // For INT32 page:
                // header + data + bitman <= 8192
                // 4 + 4*n + ceil(n/8) <= 8192
                // n <= 1984.97
                // max(n) = 1984
                constexpr size_t ROWS_PER_PAGE = 1984;

                size_t page_idx = idx / ROWS_PER_PAGE;
                size_t offset = idx % ROWS_PER_PAGE;

                const std::byte* page = ref->pages[page_idx]->data;
                const int32_t* data_begin = reinterpret_cast<const int32_t*>(page + 4);
                int32_t value = data_begin[offset];
                ref_cache = valuet::value_t(value);
                return ref_cache;
            }
        }

        void reference_column(const ColumnarTable& table, size_t in_col_idx){
            ref = const_cast<Column*>(&table.columns[in_col_idx]);
            num_values = table.num_rows;
        }
    };
}