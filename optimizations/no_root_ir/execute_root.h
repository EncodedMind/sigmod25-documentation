#pragma once
#include <hardware.h>
#include <plan.h>
#include <table.h>
#include <value_t.h>
#include <column_t.h>

namespace Contest {
    using ExecuteResult = std::vector<columnt::column_t>;
    ExecuteResult execute_impl(const Plan& plan, size_t node_idx);

    struct JoinAlgorithmColumnar{
        bool                                             build_left;
        ExecuteResult&                                   left;
        ExecuteResult&                                   right;
        ColumnarTable&                                   results;
        size_t                                           left_col, right_col;
        const std::vector<std::tuple<size_t, DataType>>& output_attrs;
        const Plan&                                      plan;

        // Persistent buffer state for each output column
        struct IntColumnBuffer{
            uint16_t num_rows = 0;
            std::vector<int32_t> data;
            std::vector<uint8_t> bitmap;

            IntColumnBuffer(){
                data.reserve(2048);
                bitmap.reserve(256);
            }

            void save_page(Column& column){
                auto* page                             = column.new_page()->data;
                *reinterpret_cast<uint16_t*>(page)     = num_rows;
                *reinterpret_cast<uint16_t*>(page + 2) = static_cast<uint16_t>(data.size());
                memcpy(page + 4, data.data(), data.size() * 4);
                memcpy(page + PAGE_SIZE - bitmap.size(), bitmap.data(), bitmap.size());
                num_rows = 0;
                data.clear();
                bitmap.clear();
            }
        };

        struct VarcharColumnBuffer{
            uint16_t num_rows = 0;
            std::vector<char> data;
            std::vector<uint16_t> offsets;
            std::vector<uint8_t> bitmap;

            VarcharColumnBuffer(){
                data.reserve(8192);
                offsets.reserve(4096);
                bitmap.reserve(512);
            }

            void save_page(Column& column){
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
        };

        std::vector<IntColumnBuffer> int_buffers;
        std::vector<VarcharColumnBuffer> varchar_buffers;

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

        void insert_value(size_t out_idx, const valuet::value_t& value){

            const auto& [col_idx, data_type] = output_attrs[out_idx];
            auto& column = results.columns[out_idx];

            switch (data_type) {

                case DataType::INT32: {
                    
                    size_t int_idx = 0;
                    for(size_t i = 0; i < out_idx; ++i){
                        if(std::get<1>(output_attrs[i]) == DataType::INT32){
                            int_idx++;
                        }
                    }
                    auto& buf = int_buffers[int_idx];

                    if(value.is_null_int32()){
                        if (4 + (buf.data.size()) * 4 + (buf.num_rows / 8 + 1) > PAGE_SIZE) {
                            buf.save_page(column);
                        }
                        unset_bitmap(buf.bitmap, buf.num_rows);
                        ++buf.num_rows;
                    }
                    else{
                        if (4 + (buf.data.size() + 1) * 4 + (buf.num_rows / 8 + 1) > PAGE_SIZE) {
                            buf.save_page(column);
                        }
                        set_bitmap(buf.bitmap, buf.num_rows);
                        buf.data.emplace_back(value.intvalue);
                        ++buf.num_rows;
                    }
                    break;
                }

                case DataType::VARCHAR: {

                    size_t varchar_idx = 0;
                    for(size_t i = 0; i < out_idx; ++i){
                        if(std::get<1>(output_attrs[i]) == DataType::VARCHAR){
                            varchar_idx++;
                        }
                    }
                    auto& buf = varchar_buffers[varchar_idx];

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

                    if(value.is_null_string()){
                        if (4 + buf.offsets.size() * 2 + buf.data.size() + (buf.num_rows / 8 + 1) > PAGE_SIZE) {
                            buf.save_page(column);
                        }
                        unset_bitmap(buf.bitmap, buf.num_rows);
                        ++buf.num_rows;
                    }
                    else{
                        // Materialize the string
                        std::string materialized_string = materialize_string(plan, value.stringvalue);

                        if (materialized_string.size() > PAGE_SIZE - 7) {
                            if (buf.num_rows > 0) {
                                buf.save_page(column);
                            }
                            save_long_string(materialized_string);
                        }
                        else{
                            if (4 + (buf.offsets.size() + 1) * 2 + (buf.data.size() + materialized_string.size()) + (buf.num_rows / 8 + 1) > PAGE_SIZE) {
                                buf.save_page(column);
                            }
                            set_bitmap(buf.bitmap, buf.num_rows);
                            buf.data.insert(buf.data.end(), materialized_string.begin(), materialized_string.end());
                            buf.offsets.emplace_back(buf.data.size());
                            ++buf.num_rows;
                        }
                    }
                    break;
                }
            }
        }

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

        auto run(){
            namespace views = ranges::views;

            // Initialize column buffers once before probe
            for(size_t out_idx = 0; out_idx < output_attrs.size(); ++out_idx) {
                auto [col_idx, data_type] = output_attrs[out_idx];
                results.columns.emplace_back(data_type);

                if(data_type == DataType::INT32){
                    int_buffers.emplace_back();
                }
                else if(data_type == DataType::VARCHAR){
                    varchar_buffers.emplace_back();
                }
            }

            size_t build_size = build_left ? left[left_col].size() : right[right_col].size();
            build_size = nextpow2(build_size) * 2; // next power of 2, doubled
            std::unordered_map<int32_t, std::vector<size_t>> hash_table;
            hash_table.reserve(build_size);

            if (build_left) {
                for(size_t row_idx = 0; row_idx < left[left_col].size(); ++row_idx){
                    const auto& key = left[left_col][row_idx];
                    if(key.is_null_int32()) continue;

                    if (auto itr = hash_table.find(key.intvalue); itr == hash_table.end()) {
                        hash_table.emplace(key.intvalue, std::vector<size_t>(1, row_idx));
                    } else {
                        itr->second.push_back(row_idx);
                    }
                }
                for(size_t right_idx = 0; right_idx < right[right_col].size(); ++right_idx){
                    const auto& key = right[right_col][right_idx];
                    if(key.is_null_int32()) continue;

                    if (auto itr = hash_table.find(key.intvalue); itr != hash_table.end()) {
                        for (auto left_idx: itr->second) {
                            for (size_t out_idx = 0; out_idx < output_attrs.size(); ++out_idx) {
                                auto [col_idx, _] = output_attrs[out_idx];
                                if (col_idx < left.size()) {
                                    insert_value(out_idx, left[col_idx][left_idx]);
                                } else {
                                    insert_value(out_idx, right[col_idx - left.size()][right_idx]);
                                }
                            }
                            results.num_rows++;
                        }
                    }
                }
            } else {
                for(size_t row_idx = 0; row_idx < right[right_col].size(); ++row_idx){
                    const auto& key = right[right_col][row_idx];
                    if(key.is_null_int32()) continue;

                    if (auto itr = hash_table.find(key.intvalue); itr == hash_table.end()) {
                        hash_table.emplace(key.intvalue, std::vector<size_t>(1, row_idx));
                    } else {
                        itr->second.push_back(row_idx);
                    }
                }            
                for (size_t left_idx = 0; left_idx < left[left_col].size(); ++left_idx) {
                    const auto& key = left[left_col][left_idx];
                    if(key.is_null_int32()) continue;

                    if (auto itr = hash_table.find(key.intvalue); itr != hash_table.end()) {
                        for (auto right_idx: itr->second) {
                            for (size_t out_idx = 0; out_idx < output_attrs.size(); ++out_idx) {
                                auto [col_idx, _] = output_attrs[out_idx];
                                if (col_idx < left.size()) {
                                    insert_value(out_idx, left[col_idx][left_idx]);
                                } else {
                                    insert_value(out_idx, right[col_idx - left.size()][right_idx]);
                                }
                            }
                            results.num_rows++;
                        }
                    }
                }
            }

            // Finalize all columns (flush remaining pages)
            size_t int_idx = 0;
            size_t varchar_idx = 0;
            for(size_t out_idx = 0; out_idx < output_attrs.size(); ++out_idx) {
                auto [col_idx, data_type] = output_attrs[out_idx];
                if(data_type == DataType::INT32) {
                    auto& buf = int_buffers[int_idx++];
                    if(buf.num_rows != 0) {
                        buf.save_page(results.columns[out_idx]);
                    }
                }
                else if(data_type == DataType::VARCHAR) {
                    auto& buf = varchar_buffers[varchar_idx++];
                    if(buf.num_rows != 0) {
                        buf.save_page(results.columns[out_idx]);
                    }
                }
            }
        }
    };

    inline ColumnarTable execute_hash_join_root(const Plan& plan, const JoinNode& join, const std::vector<std::tuple<size_t, DataType>>& output_attrs){
        auto                           left_idx    = join.left;
        auto                           right_idx   = join.right;
        auto&                          left_node   = plan.nodes[left_idx];
        auto&                          right_node  = plan.nodes[right_idx];
        auto&                          left_types  = left_node.output_attrs;
        auto&                          right_types = right_node.output_attrs;
        auto                           left        = execute_impl(plan, left_idx);
        auto                           right       = execute_impl(plan, right_idx);
        ColumnarTable results;

        JoinAlgorithmColumnar join_algorithm{.build_left = join.build_left,
            .left                                        = left,
            .right                                       = right,
            .results                                     = results,
            .left_col                                    = join.left_attr,
            .right_col                                   = join.right_attr,
            .output_attrs                                = output_attrs,
            .plan                                        = plan};
        
        join_algorithm.run();
        return results;
    }

    inline ColumnarTable execute_impl_root(const Plan& plan, size_t node_idx){
        auto& node = plan.nodes[node_idx];
        auto& value = std::get<JoinNode>(node.data);
        return execute_hash_join_root(plan, value, node.output_attrs); // root is always join node
    }

} // namespace Contest