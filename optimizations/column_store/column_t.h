#pragma once
#include <vector>
#include <cstdint>
#include <cstring>
#include <value_t.h>

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

        column_t() = default;

        ~column_t(){
            for(auto* page : pages){
                delete page;
            }
        }

        column_t(column_t&& other) noexcept : pages(std::move(other.pages)), num_values(other.num_values){
            other.num_values = 0;
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
            size_t page_idx = idx / VALUES_PER_PAGE;
            size_t offset = idx % VALUES_PER_PAGE;
            return pages[page_idx]->data[offset];
        }
        
        const valuet::value_t& operator[](size_t idx) const{
            size_t page_idx = idx / VALUES_PER_PAGE;
            size_t offset = idx % VALUES_PER_PAGE;
            return pages[page_idx]->data[offset];
        }
    };
}