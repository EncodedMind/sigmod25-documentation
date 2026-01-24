#pragma once

#include <common.h>
#include <inner_column.h>

#include <plan.h>
#include <table.h>
#include <value_t.h>
#include <column_t.h>

namespace mytocolumnar{

    std::string materialize_string(const Plan& plan, const valuet::NewString& stringref){
        uint8_t table_id = stringref.table_id;
        uint8_t column_id = stringref.column_id;
        uint32_t page_id = stringref.page_id;
        uint16_t offset_idx = stringref.offset_idx;

        const auto& column = plan.inputs[table_id].columns[column_id];
        auto* page = column.pages[page_id]->data;

        uint16_t num_rows = *reinterpret_cast<const uint16_t*>(page);
        const uint16_t num_values = *reinterpret_cast<const uint16_t*>(page + 2);
        const auto* offsets = reinterpret_cast<const uint16_t*>(page + 4);
        const auto* data_base = reinterpret_cast<const char*>(page + 4 + num_values * 2);

        if(num_rows != 0xffff && num_rows != 0xfffe){
            uint16_t start = (offset_idx == 0) ? 0 : offsets[offset_idx - 1];
            uint16_t length = offsets[offset_idx] - start;
            return std::string(data_base + start, length);
        }
        
        // long string materialization
        std::string result;
        uint32_t current_page_id = page_id;

        // Process first page (0xffff)
        page = column.pages[current_page_id]->data;
        uint16_t length = *reinterpret_cast<const uint16_t*>(page + 2);
        const char* start = reinterpret_cast<const char*>(page + 4);
        result.append(start, length);
        current_page_id++;

        // Process continuation pages (0xfffe) until we hit something else
        while(current_page_id < column.pages.size()){
            page = column.pages[current_page_id]->data;
            num_rows = *reinterpret_cast<const uint16_t*>(page);
            
            if (num_rows != 0xfffe) break;  // Stop if not a continuation page
            
            length = *reinterpret_cast<const uint16_t*>(page + 2);
            start = reinterpret_cast<const char*>(page + 4);
            result.append(start, length);
            current_page_id++;
        }
        return result;
    }

    void set_bitmap(std::vector<uint8_t>& bitmap, uint16_t idx) {
        while (bitmap.size() < idx / 8 + 1) {
            bitmap.emplace_back(0);
        }
        auto byte_idx     = idx / 8;
        auto bit          = idx % 8;
        bitmap[byte_idx] |= (1u << bit);
    }

    void unset_bitmap(std::vector<uint8_t>& bitmap, uint16_t idx) {
        while (bitmap.size() < idx / 8 + 1) {
            bitmap.emplace_back(0);
        }
        auto byte_idx     = idx / 8;
        auto bit          = idx % 8;
        bitmap[byte_idx] &= ~(1u << bit);
    }

    ColumnarTable to_columnar_value_t(const std::vector<columnt::column_t>& table, const Plan& plan){
        const std::vector<std::tuple<size_t, DataType>>& output_attrs = plan.nodes[plan.root].output_attrs;
        namespace views  = ranges::views;
        ColumnarTable ret;
        ret.num_rows = table.empty() ? 0 : table[0].size();
        for (const auto& [out_idx, attr]: output_attrs | views::enumerate) {
            const auto& [table_col_idx, data_type] = attr;
            (void)table_col_idx; // to silence the unused variable warning

            ret.columns.emplace_back(data_type);
            auto& column = ret.columns.back();
            switch (data_type) {

            case DataType::INT32: {
                uint16_t             num_rows = 0;
                std::vector<int32_t> data;
                std::vector<uint8_t> bitmap;
                data.reserve(2048);
                bitmap.reserve(256);
                auto save_page = [&column, &num_rows, &data, &bitmap]() {
                    auto* page                             = column.new_page()->data;
                    *reinterpret_cast<uint16_t*>(page)     = num_rows;
                    *reinterpret_cast<uint16_t*>(page + 2) = static_cast<uint16_t>(data.size());
                    memcpy(page + 4, data.data(), data.size() * 4);
                    memcpy(page + PAGE_SIZE - bitmap.size(), bitmap.data(), bitmap.size());
                    num_rows = 0;
                    data.clear();
                    bitmap.clear();
                };

                if(table.size() <= out_idx) continue;
                for (size_t row_idx = 0; row_idx < table[out_idx].size(); ++row_idx) {
                    const valuet::value_t& value = table[out_idx][row_idx];

                    if(value.is_null_int32()){
                        if (4 + (data.size()) * 4 + (num_rows / 8 + 1) > PAGE_SIZE) {
                            save_page();
                        }
                        unset_bitmap(bitmap, num_rows);
                        ++num_rows;
                    }
                    else{
                        if (4 + (data.size() + 1) * 4 + (num_rows / 8 + 1) > PAGE_SIZE) {
                            save_page();
                        }
                        set_bitmap(bitmap, num_rows);
                        data.emplace_back(value.intvalue);
                        ++num_rows;
                    }
                }
                if (num_rows != 0) {
                    save_page();
                }
                break;
            }

            case DataType::VARCHAR: {
                uint16_t              num_rows = 0;
                std::vector<char>     data;
                std::vector<uint16_t> offsets;
                std::vector<uint8_t>  bitmap;
                data.reserve(8192);
                offsets.reserve(4096);
                bitmap.reserve(512);

                auto save_long_string = [&column](const std::string& str) {
                    size_t offset     = 0;
                    auto   first_page = true;
                    while (offset < str.size()) {
                        auto* page = column.new_page()->data;
                        if (first_page) {
                            *reinterpret_cast<uint16_t*>(page) = 0xffff;
                            first_page                         = false;
                        } else {
                            *reinterpret_cast<uint16_t*>(page) = 0xfffe;
                        }
                        auto page_data_len = std::min(str.size() - offset, PAGE_SIZE - 4);
                        *reinterpret_cast<uint16_t*>(page + 2) = page_data_len;
                        memcpy(page + 4, str.data() + offset, page_data_len);
                        offset += page_data_len;
                    }
                };

                auto save_page = [&column, &num_rows, &data, &offsets, &bitmap]() {
                    auto* page                             = column.new_page()->data;
                    *reinterpret_cast<uint16_t*>(page)     = num_rows;
                    *reinterpret_cast<uint16_t*>(page + 2) = static_cast<uint16_t>(offsets.size());
                    memcpy(page + 4, offsets.data(), offsets.size() * 2);
                    memcpy(page + 4 + offsets.size() * 2, data.data(), data.size());
                    memcpy(page + PAGE_SIZE - bitmap.size(), bitmap.data(), bitmap.size());
                    num_rows = 0;
                    data.clear();
                    offsets.clear();
                    bitmap.clear();
                };

                if (table.size() <= out_idx) continue;
                for (size_t row_idx = 0; row_idx < table[out_idx].size(); ++row_idx) {
                    const valuet::value_t& value = table[out_idx][row_idx];

                    if(value.is_null_string()){
                        if (4 + offsets.size() * 2 + data.size() + (num_rows / 8 + 1) > PAGE_SIZE) {
                            save_page();
                        }
                        unset_bitmap(bitmap, num_rows);
                        ++num_rows;
                    }
                    else{
                        // Materialize the string
                        std::string materialized_string = materialize_string(plan, value.stringvalue);

                        if (materialized_string.size() > PAGE_SIZE - 7) {
                            if (num_rows > 0) {
                                save_page();
                            }
                            save_long_string(materialized_string);
                        }
                        else{
                            if (4 + (offsets.size() + 1) * 2 + (data.size() + materialized_string.size()) + (num_rows / 8 + 1) > PAGE_SIZE) {
                                save_page();
                            }
                            set_bitmap(bitmap, num_rows);
                            data.insert(data.end(), materialized_string.begin(), materialized_string.end());
                            offsets.emplace_back(data.size());
                            ++num_rows;
                        }
                    }
                }
                if (num_rows != 0) {
                    save_page();
                }
                break;
            }
            }
        }
        return ret;
    }
} // namespace mytocolumnar