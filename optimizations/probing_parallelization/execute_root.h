#pragma once
#include <hardware.h>
#include <plan.h>
#include <table.h>
#include <value_t.h>
#include <column_t.h>

#include <algorithm>
#include <cstdlib>
#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include <threaded_table.h>
#include <unchained_table.h>

namespace Contest {
    using ExecuteResult = std::vector<columnt::column_t>;
    ExecuteResult execute_impl(const Plan& plan, size_t node_idx);

    namespace {

    inline size_t parse_env_threads(const char* s) {
        if (!s || !*s) return 0;
        char* end = nullptr;
        unsigned long v = std::strtoul(s, &end, 10);
        if (end == s) return 0;
        return static_cast<size_t>(v);
    }
    
    inline size_t threaded_min_build_rows() {
        if (const char* v = std::getenv("SPC_THREADED_MIN_BUILD")) {
            const size_t parsed = parse_env_threads(v);
            if (parsed > 0) return parsed;
        }
        return 600000;
    }

    } // namespace

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

        std::vector<int32_t> out_to_int_idx;
        std::vector<int32_t> out_to_varchar_idx;

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

                    const size_t int_idx = static_cast<size_t>(out_to_int_idx[out_idx]);
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

                    const size_t varchar_idx = static_cast<size_t>(out_to_varchar_idx[out_idx]);
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

        struct ThreadLocalWriter {
            const Plan&                                      plan;
            const std::vector<std::tuple<size_t, DataType>>& output_attrs;
            const std::vector<int32_t>&                      out_to_int_idx;
            const std::vector<int32_t>&                      out_to_varchar_idx;

            ColumnarTable                  table;
            std::vector<IntColumnBuffer>   int_buffers;
            std::vector<VarcharColumnBuffer> varchar_buffers;

            ThreadLocalWriter(const Plan& plan,
                const std::vector<std::tuple<size_t, DataType>>& output_attrs,
                const std::vector<int32_t>& out_to_int_idx,
                const std::vector<int32_t>& out_to_varchar_idx)
            : plan(plan)
            , output_attrs(output_attrs)
            , out_to_int_idx(out_to_int_idx)
            , out_to_varchar_idx(out_to_varchar_idx) {
                table.num_rows = 0;
                table.columns.reserve(output_attrs.size());

                // Allocate buffers only for the types that exist.
                size_t int_count = 0;
                size_t varchar_count = 0;
                for (size_t out_idx = 0; out_idx < output_attrs.size(); ++out_idx) {
                    auto [_, dt] = output_attrs[out_idx];
                    table.columns.emplace_back(dt);
                    if (dt == DataType::INT32) ++int_count;
                    else if (dt == DataType::VARCHAR) ++varchar_count;
                }
                int_buffers.reserve(int_count);
                varchar_buffers.reserve(varchar_count);
                for (size_t i = 0; i < int_count; ++i) int_buffers.emplace_back();
                for (size_t i = 0; i < varchar_count; ++i) varchar_buffers.emplace_back();
            }

            static void set_bitmap(std::vector<uint8_t>& bitmap, uint16_t idx) {
                while (bitmap.size() < idx / 8 + 1) {
                    bitmap.emplace_back(0);
                }
                auto byte_idx     = idx / 8;
                auto bit          = idx % 8;
                bitmap[byte_idx] |= (1u << bit);
            }

            static void unset_bitmap(std::vector<uint8_t>& bitmap, uint16_t idx) {
                while (bitmap.size() < idx / 8 + 1) {
                    bitmap.emplace_back(0);
                }
                auto byte_idx     = idx / 8;
                auto bit          = idx % 8;
                bitmap[byte_idx] &= ~(1u << bit);
            }

            std::string materialize_string(const valuet::NewString& stringref) {
                uint8_t  table_id   = stringref.table_id;
                uint8_t  column_id  = stringref.column_id;
                uint32_t page_id    = stringref.page_id;
                uint16_t offset_idx = stringref.offset_idx;

                const auto& column = plan.inputs[table_id].columns[column_id];
                auto*       page   = column.pages[page_id]->data;

                uint16_t num_rows = *reinterpret_cast<const uint16_t*>(page);
                const uint16_t num_values = *reinterpret_cast<const uint16_t*>(page + 2);
                const auto* offsets = reinterpret_cast<const uint16_t*>(page + 4);
                const auto* data_base = reinterpret_cast<const char*>(page + 4 + num_values * 2);

                if (num_rows != 0xffff && num_rows != 0xfffe) {
                    uint16_t start  = (offset_idx == 0) ? 0 : offsets[offset_idx - 1];
                    uint16_t length = offsets[offset_idx] - start;
                    return std::string(data_base + start, length);
                }

                std::string result;
                uint32_t current_page_id = page_id;

                // first page (0xffff)
                page = column.pages[current_page_id]->data;
                uint16_t length = *reinterpret_cast<const uint16_t*>(page + 2);
                const char* start = reinterpret_cast<const char*>(page + 4);
                result.append(start, length);
                current_page_id++;

                // continuation pages (0xfffe)
                while (current_page_id < column.pages.size()) {
                    page = column.pages[current_page_id]->data;
                    num_rows = *reinterpret_cast<const uint16_t*>(page);
                    if (num_rows != 0xfffe) break;
                    length = *reinterpret_cast<const uint16_t*>(page + 2);
                    start = reinterpret_cast<const char*>(page + 4);
                    result.append(start, length);
                    current_page_id++;
                }
                return result;
            }

            void insert_value(size_t out_idx, const valuet::value_t& value) {
                const auto& [col_idx, data_type] = output_attrs[out_idx];
                auto& column = table.columns[out_idx];

                switch (data_type) {
                case DataType::INT32: {
                    const size_t int_idx = static_cast<size_t>(out_to_int_idx[out_idx]);
                    auto& buf = int_buffers[int_idx];

                    if (value.is_null_int32()) {
                        if (4 + (buf.data.size()) * 4 + (buf.num_rows / 8 + 1) > PAGE_SIZE) {
                            buf.save_page(column);
                        }
                        unset_bitmap(buf.bitmap, buf.num_rows);
                        ++buf.num_rows;
                    } else {
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
                    const size_t varchar_idx = static_cast<size_t>(out_to_varchar_idx[out_idx]);
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

                    if (value.is_null_string()) {
                        if (4 + buf.offsets.size() * 2 + buf.data.size() + (buf.num_rows / 8 + 1) > PAGE_SIZE) {
                            buf.save_page(column);
                        }
                        unset_bitmap(buf.bitmap, buf.num_rows);
                        ++buf.num_rows;
                    } else {
                        std::string materialized_string = materialize_string(value.stringvalue);

                        if (materialized_string.size() > PAGE_SIZE - 7) {
                            if (buf.num_rows > 0) {
                                buf.save_page(column);
                            }
                            save_long_string(materialized_string);
                        } else {
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

            void finalize() {
                size_t int_idx = 0;
                size_t varchar_idx = 0;
                for (size_t out_idx = 0; out_idx < output_attrs.size(); ++out_idx) {
                    auto [_, data_type] = output_attrs[out_idx];
                    if (data_type == DataType::INT32) {
                        auto& buf = int_buffers[int_idx++];
                        if (buf.num_rows != 0) {
                            buf.save_page(table.columns[out_idx]);
                        }
                    } else if (data_type == DataType::VARCHAR) {
                        auto& buf = varchar_buffers[varchar_idx++];
                        if (buf.num_rows != 0) {
                            buf.save_page(table.columns[out_idx]);
                        }
                    }
                }
            }
        };

        auto run(){
            out_to_int_idx.assign(output_attrs.size(), -1);
            out_to_varchar_idx.assign(output_attrs.size(), -1);
            int32_t int_counter = 0;
            int32_t varchar_counter = 0;
            for (size_t out_idx = 0; out_idx < output_attrs.size(); ++out_idx) {
                auto [_, data_type] = output_attrs[out_idx];
                if (data_type == DataType::INT32) {
                    out_to_int_idx[out_idx] = int_counter++;
                } else if (data_type == DataType::VARCHAR) {
                    out_to_varchar_idx[out_idx] = varchar_counter++;
                }
            }

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

            const size_t threaded_min_build = threaded_min_build_rows();
            const bool use_threaded = build_size >= threaded_min_build;

            if (!use_threaded) {
                ::UnchainedHashTable ht;
                ht.reserve(build_size);

                constexpr size_t PROBE_CHUNK_ROWS = 1984;

                size_t probe_threads = static_cast<size_t>(SPC__THREAD_COUNT);
                if (probe_threads == 0) probe_threads = 4;

                if (const char* force = std::getenv("SPC_FORCE_THREADS")) {
                    const size_t forced = parse_env_threads(force);
                    if (forced > 0) probe_threads = forced;
                }

                size_t probe_partitions = 1;
                while (probe_partitions < probe_threads) probe_partitions *= 2;
                probe_threads = probe_partitions;

                if (build_left) {
                    for (size_t row_idx = 0; row_idx < left[left_col].size(); ++row_idx) {
                        const auto& key = left[left_col][row_idx];
                        if (key.is_null_int32()) continue;
                        ht.insert(key.intvalue, row_idx);
                    }
                    ht.finalize();

                    const size_t probe_rows = right[right_col].size();
                    if (probe_threads <= 1 || probe_rows < PROBE_CHUNK_ROWS) {
                        for (size_t right_idx = 0; right_idx < probe_rows; ++right_idx) {
                            const auto& key = right[right_col][right_idx];
                            if (key.is_null_int32()) continue;

                            size_t len = 0;
                            const ::HashEntry* entries = ht.find_range(key.intvalue, len);
                            if (!entries || len == 0) continue;

                            for (size_t i = 0; i < len; ++i) {
                                if (entries[i].key != key.intvalue) continue;
                                const size_t left_idx = entries[i].row_idx;
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
                    } else {
                        std::atomic<size_t> next_start{0};
                        std::vector<std::thread> probe_workers;
                        probe_workers.reserve(probe_threads);
                        std::vector<std::vector<std::pair<size_t, size_t>>> local_matches(probe_threads);

                        for (size_t t = 0; t < probe_threads; ++t) {
                            probe_workers.emplace_back([&, t]() {
                                auto& matches = local_matches[t];
                                while (true) {
                                    const size_t start = next_start.fetch_add(PROBE_CHUNK_ROWS, std::memory_order_relaxed);
                                    if (start >= probe_rows) break;
                                    const size_t end = std::min(start + PROBE_CHUNK_ROWS, probe_rows);
                                    for (size_t right_idx = start; right_idx < end; ++right_idx) {
                                        const auto& key = right[right_col][right_idx];
                                        if (key.is_null_int32()) continue;

                                        size_t len = 0;
                                        const ::HashEntry* entries = ht.find_range(key.intvalue, len);
                                        if (!entries || len == 0) continue;

                                        for (size_t i = 0; i < len; ++i) {
                                            if (entries[i].key != key.intvalue) continue;
                                            matches.emplace_back(entries[i].row_idx, right_idx);
                                        }
                                    }
                                }
                            });
                        }
                        for (auto& t : probe_workers) t.join();

                        for (size_t t = 0; t < probe_threads; ++t) {
                            for (const auto& m : local_matches[t]) {
                                const size_t left_idx = m.first;
                                const size_t right_idx = m.second;
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
                    for (size_t row_idx = 0; row_idx < right[right_col].size(); ++row_idx) {
                        const auto& key = right[right_col][row_idx];
                        if (key.is_null_int32()) continue;
                        ht.insert(key.intvalue, row_idx);
                    }
                    ht.finalize();

                    const size_t probe_rows = left[left_col].size();
                    if (probe_threads <= 1 || probe_rows < PROBE_CHUNK_ROWS) {
                        for (size_t left_idx = 0; left_idx < probe_rows; ++left_idx) {
                            const auto& key = left[left_col][left_idx];
                            if (key.is_null_int32()) continue;

                            size_t len = 0;
                            const ::HashEntry* entries = ht.find_range(key.intvalue, len);
                            if (!entries || len == 0) continue;

                            for (size_t i = 0; i < len; ++i) {
                                if (entries[i].key != key.intvalue) continue;
                                const size_t right_idx = entries[i].row_idx;
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
                    } else {
                        std::atomic<size_t> next_start{0};
                        std::vector<std::thread> probe_workers;
                        probe_workers.reserve(probe_threads);
                        std::vector<std::vector<std::pair<size_t, size_t>>> local_matches(probe_threads);

                        for (size_t t = 0; t < probe_threads; ++t) {
                            probe_workers.emplace_back([&, t]() {
                                auto& matches = local_matches[t];
                                while (true) {
                                    const size_t start = next_start.fetch_add(PROBE_CHUNK_ROWS, std::memory_order_relaxed);
                                    if (start >= probe_rows) break;
                                    const size_t end = std::min(start + PROBE_CHUNK_ROWS, probe_rows);
                                    for (size_t left_idx = start; left_idx < end; ++left_idx) {
                                        const auto& key = left[left_col][left_idx];
                                        if (key.is_null_int32()) continue;

                                        size_t len = 0;
                                        const ::HashEntry* entries = ht.find_range(key.intvalue, len);
                                        if (!entries || len == 0) continue;

                                        for (size_t i = 0; i < len; ++i) {
                                            if (entries[i].key != key.intvalue) continue;
                                            matches.emplace_back(left_idx, entries[i].row_idx);
                                        }
                                    }
                                }
                            });
                        }
                        for (auto& t : probe_workers) t.join();

                        for (size_t t = 0; t < probe_threads; ++t) {
                            for (const auto& m : local_matches[t]) {
                                const size_t left_idx = m.first;
                                const size_t right_idx = m.second;
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

            } else {

                size_t num_threads = static_cast<size_t>(SPC__THREAD_COUNT);
                if(num_threads == 0) num_threads = 4;

                if (const char* force = std::getenv("SPC_FORCE_THREADS")) {
                    const size_t forced = parse_env_threads(force);
                    if (forced > 0) num_threads = forced;
                }

            size_t num_partitions = 1;
            while(num_partitions < num_threads) num_partitions *= 2;
            num_threads = num_partitions;

            if (build_left) {
                // Phase 1: Collect (build left)

                threaded::GlobalAllocator globalAlloc;
                std::vector<std::unique_ptr<threaded::TupleCollector>> collectors;
                collectors.reserve(num_threads);
                for(size_t i=0; i<num_threads; ++i) {
                    collectors.push_back(std::make_unique<threaded::TupleCollector>(globalAlloc, num_partitions));
                }

                if (num_threads == 1) {
                    auto& collector = *collectors[0];
                    for(size_t row_idx = 0; row_idx < build_size; ++row_idx){
                        const auto& key = left[left_col][row_idx];
                        if (key.is_null_int32()) continue;
                        collector.consume(threaded::HashEntry(key.intvalue, row_idx));
                    }
                } else {
                    std::vector<std::thread> threads;
                    size_t rows_per_thread = (build_size + num_threads - 1) / num_threads;

                    for(size_t t = 0; t < num_threads; ++t){
                        threads.emplace_back([&, t](){
                            size_t start = t * rows_per_thread;
                            size_t end = std::min(start + rows_per_thread, build_size);
                            
                            auto& collector = *collectors[t];

                            for(size_t row_idx = start; row_idx < end; ++row_idx){
                                const auto& key = left[left_col][row_idx];
                                if (key.is_null_int32()) continue;
                                collector.consume(threaded::HashEntry(key.intvalue, row_idx));
                            }
                        });
                    }

                    for (auto& t : threads) t.join();
                }

                // Merge
                std::vector<threaded::Block*> partition_heads = threaded::merge_partitions(collectors, num_partitions);

                // Phase 2/3: Count and Copy (one thread per partition)

                size_t total_tuples = 0;
                for(const auto& col : collectors){
                    for(size_t c : col->counts) total_tuples += c;
                }

                threaded::FinalTable final_table(total_tuples, num_partitions);

                std::vector<size_t> partition_offsets(num_partitions, 0);
                size_t running_count = 0;

                std::vector<size_t> global_partition_counts(num_partitions, 0);
                for(size_t p=0; p<num_partitions; ++p){
                    for(const auto& col : collectors) {
                        global_partition_counts[p] += col->counts[p];
                    }
                }

                for(size_t p=0; p<num_partitions; ++p) {
                    partition_offsets[p] = running_count;
                    running_count += global_partition_counts[p];
                }

                if (num_partitions == 1) {
                    final_table.postProcessBuild(0, static_cast<uint64_t>(partition_offsets[0]), partition_heads);
                } else {
                    std::vector<std::thread> build_threads;
                    build_threads.reserve(num_partitions);
                    for (size_t p = 0; p < num_partitions; ++p) {
                        build_threads.emplace_back([&, p]() {
                            final_table.postProcessBuild(
                                static_cast<uint64_t>(p),
                                static_cast<uint64_t>(partition_offsets[p]),
                                partition_heads);
                        });
                    }
                    for (auto& t : build_threads) t.join();
                }

                // Probing (right) - parallel with per-thread output tables
                const size_t probe_rows = right[right_col].size();
                constexpr size_t PROBE_CHUNK_ROWS = 1984;
                if (num_threads <= 1 || probe_rows < PROBE_CHUNK_ROWS) {
                    for(size_t right_idx = 0; right_idx < probe_rows; ++right_idx){
                        const auto& key = right[right_col][right_idx];
                        if(key.is_null_int32()) continue;

                        size_t len = 0;
                        const threaded::HashEntry* entries = final_table.find_range(key.intvalue, len);
                        if (!entries || len == 0) continue;

                        for (size_t i = 0; i < len; ++i) {
                            if (entries[i].key != key.intvalue) continue;
                            size_t left_idx = entries[i].row_idx;
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
                } else {
                    std::atomic<size_t> next_start{0};
                    std::vector<std::vector<std::pair<size_t, size_t>>> local_matches(num_threads);

                    std::vector<std::thread> probe_threads;
                    probe_threads.reserve(num_threads);
                    for (size_t t = 0; t < num_threads; ++t) {
                        probe_threads.emplace_back([&, t]() {
                            auto& matches = local_matches[t];
                            while (true) {
                                const size_t start = next_start.fetch_add(PROBE_CHUNK_ROWS, std::memory_order_relaxed);
                                if (start >= probe_rows) break;
                                const size_t end = std::min(start + PROBE_CHUNK_ROWS, probe_rows);
                                for (size_t right_idx = start; right_idx < end; ++right_idx) {
                                    const auto& key = right[right_col][right_idx];
                                    if (key.is_null_int32()) continue;

                                    size_t len = 0;
                                    const threaded::HashEntry* entries = final_table.find_range(key.intvalue, len);
                                    if (!entries || len == 0) continue;
                                    for (size_t i = 0; i < len; ++i) {
                                        if (entries[i].key != key.intvalue) continue;
                                        matches.emplace_back(entries[i].row_idx, right_idx);
                                    }
                                }
                            }
                        });
                    }
                    for (auto& t : probe_threads) t.join();

                    for (size_t t = 0; t < num_threads; ++t) {
                        for (const auto& m : local_matches[t]) {
                            const size_t left_idx = m.first;
                            const size_t right_idx = m.second;
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
                // Phase 1: Collect (build right)

                threaded::GlobalAllocator globalAlloc;
                std::vector<std::unique_ptr<threaded::TupleCollector>> collectors;
                collectors.reserve(num_threads);
                for(size_t i=0; i<num_threads; ++i) {
                    collectors.push_back(std::make_unique<threaded::TupleCollector>(globalAlloc, num_partitions));
                }

                if (num_threads == 1) {
                    auto& collector = *collectors[0];
                    for(size_t row_idx = 0; row_idx < build_size; ++row_idx){
                        const auto& key = right[right_col][row_idx];
                        if (key.is_null_int32()) continue;
                        collector.consume(threaded::HashEntry(key.intvalue, row_idx));
                    }
                } else {
                    std::vector<std::thread> threads;
                    size_t rows_per_thread = (build_size + num_threads - 1) / num_threads;

                    for(size_t t = 0; t < num_threads; ++t){
                        threads.emplace_back([&, t](){
                            size_t start = t * rows_per_thread;
                            size_t end = std::min(start + rows_per_thread, build_size);

                            auto& collector = *collectors[t];

                            for(size_t row_idx = start; row_idx < end; ++row_idx){
                                const auto& key = right[right_col][row_idx];
                                if (key.is_null_int32()) continue;
                                collector.consume(threaded::HashEntry(key.intvalue, row_idx));
                            }
                        });
                    }

                    for (auto& t : threads) t.join();
                }

                // Merge
                std::vector<threaded::Block*> partition_heads = threaded::merge_partitions(collectors, num_partitions);

                // Phase 2: Count and Copy

                size_t total_tuples = 0;
                for(const auto& col : collectors){
                    for(size_t c : col->counts) total_tuples += c;
                }

                threaded::FinalTable final_table(total_tuples, num_partitions);

                std::vector<size_t> partition_offsets(num_partitions, 0);
                size_t running_count = 0;

                std::vector<size_t> global_partition_counts(num_partitions, 0);
                for(size_t p=0; p<num_partitions; ++p){
                    for(const auto& col : collectors) {
                        global_partition_counts[p] += col->counts[p];
                    }
                }

                for(size_t p=0; p<num_partitions; ++p) {
                    partition_offsets[p] = running_count;
                    running_count += global_partition_counts[p];
                }

                if (num_partitions == 1) {
                    final_table.postProcessBuild(0, static_cast<uint64_t>(partition_offsets[0]), partition_heads);
                } else {
                    std::vector<std::thread> build_threads;
                    build_threads.reserve(num_partitions);
                    for (size_t p = 0; p < num_partitions; ++p) {
                        build_threads.emplace_back([&, p]() {
                            final_table.postProcessBuild(
                                static_cast<uint64_t>(p),
                                static_cast<uint64_t>(partition_offsets[p]),
                                partition_heads);
                        });
                    }
                    for (auto& t : build_threads) t.join();
                }

                // Probing (left) - parallel with per-thread output tables
                const size_t probe_rows = left[left_col].size();
                constexpr size_t PROBE_CHUNK_ROWS = 1984;
                if (num_threads <= 1 || probe_rows < PROBE_CHUNK_ROWS) {
                    for (size_t left_idx = 0; left_idx < probe_rows; ++left_idx) {
                        const auto& key = left[left_col][left_idx];
                        if(key.is_null_int32()) continue;

                        size_t len = 0;
                        const threaded::HashEntry* entries = final_table.find_range(key.intvalue, len);
                        if (!entries || len == 0) continue;

                        for (size_t i = 0; i < len; ++i) {
                            if (entries[i].key != key.intvalue) continue;
                            size_t right_idx = entries[i].row_idx;
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
                } else {
                    std::atomic<size_t> next_start{0};
                    std::vector<std::vector<std::pair<size_t, size_t>>> local_matches(num_threads);

                    std::vector<std::thread> probe_threads;
                    probe_threads.reserve(num_threads);
                    for (size_t t = 0; t < num_threads; ++t) {
                        probe_threads.emplace_back([&, t]() {
                            auto& matches = local_matches[t];
                            while (true) {
                                const size_t start = next_start.fetch_add(PROBE_CHUNK_ROWS, std::memory_order_relaxed);
                                if (start >= probe_rows) break;
                                const size_t end = std::min(start + PROBE_CHUNK_ROWS, probe_rows);
                                for (size_t left_idx = start; left_idx < end; ++left_idx) {
                                    const auto& key = left[left_col][left_idx];
                                    if (key.is_null_int32()) continue;

                                    size_t len = 0;
                                    const threaded::HashEntry* entries = final_table.find_range(key.intvalue, len);
                                    if (!entries || len == 0) continue;
                                    for (size_t i = 0; i < len; ++i) {
                                        if (entries[i].key != key.intvalue) continue;
                                        matches.emplace_back(left_idx, entries[i].row_idx);
                                    }
                                }
                            }
                        });
                    }
                    for (auto& t : probe_threads) t.join();

                    for (size_t t = 0; t < num_threads; ++t) {
                        for (const auto& m : local_matches[t]) {
                            const size_t left_idx = m.first;
                            const size_t right_idx = m.second;
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

        // Compute build_left based on actual cardinalities (paper recommendation)
        bool build_left = left[join.left_attr].size() <= right[join.right_attr].size();

        JoinAlgorithmColumnar join_algorithm{.build_left = build_left,
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