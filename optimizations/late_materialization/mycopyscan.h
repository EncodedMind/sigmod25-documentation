#pragma once

#include <common.h>
#include <inner_column.h>

#include <plan.h>
#include <table.h>
#include <value_t.h>

namespace mycopyscan{

    bool get_bitmap(const uint8_t* bitmap, uint16_t idx) {
        auto byte_idx = idx / 8;
        auto bit      = idx % 8;
        return bitmap[byte_idx] & (1u << bit);
    }

    std::vector<std::vector<valuet::value_t>> copy_scan_value_t(const ColumnarTable& table,
        const std::vector<std::tuple<size_t, DataType>>& output_attrs, uint8_t table_id){
        namespace views = ranges::views;
        std::vector<std::vector<valuet::value_t>> results(table.num_rows, std::vector<valuet::value_t>(output_attrs.size(), valuet::value_t()));
        std::vector<DataType> types(table.columns.size());

        auto task = [&](size_t begin, size_t end) {
            for (size_t column_idx = begin; column_idx < end; ++column_idx) {
                size_t in_col_idx = std::get<0>(output_attrs[column_idx]);
                auto& column = table.columns[in_col_idx];
                types[in_col_idx] = column.type;
                size_t row_idx = 0;
                uint32_t page_id = 0;

                for (auto* page: column.pages | views::transform([](auto* page) { return page->data; })) {
                    switch (column.type) {

                    case DataType::INT32: {
                        auto num_rows = *reinterpret_cast<uint16_t*>(page);
                        // page+2 num_values
                        auto* data_begin = reinterpret_cast<int32_t*>(page + 4);
                        auto* bitmap = reinterpret_cast<uint8_t*>(page + PAGE_SIZE - (num_rows + 7) / 8);
                        uint16_t data_idx = 0;

                        for (uint16_t i = 0; i < num_rows; ++i) {
                            if (get_bitmap(bitmap, i)) {
                                int32_t value = data_begin[data_idx++];
                                if (row_idx >= table.num_rows) {
                                    throw std::runtime_error("row_idx");
                                }
                                results[row_idx++][column_idx] = valuet::value_t(value);
                            } else {
                                // mark it as null and store to value_t
                                results[row_idx++][column_idx] = valuet::value_t::null_int32();
                            }
                        }
                        break;
                    }

                    case DataType::VARCHAR: { // update value_t with all the information needed
                        auto num_rows = *reinterpret_cast<uint16_t*>(page);
                        if (num_rows == 0xffff) { // long string page
                            // we don't need offset index
                            results[row_idx++][column_idx] = valuet::value_t(valuet::NewString(table_id, static_cast<uint8_t>(in_col_idx), page_id, 0)); // add the value_t
                        } else if(num_rows == 0xfffe){
                            // Long string continuation page - skip, will be handled during materialization
                        } else {
                            auto* offset_begin = reinterpret_cast<uint16_t*>(page + 4); // where the string ends in page
                            auto* bitmap = reinterpret_cast<uint8_t*>(page + PAGE_SIZE - (num_rows + 7) / 8);
                            uint16_t data_idx = 0;

                            for (uint16_t i = 0; i < num_rows; ++i) {
                                if (get_bitmap(bitmap, i)) {
                                    results[row_idx++][column_idx] = valuet::value_t(valuet::NewString(table_id, static_cast<uint8_t>(in_col_idx), page_id, data_idx)); // add the value_t
                                    data_idx++;
                                } else {
                                    // mark it as null and store to value_t
                                    results[row_idx++][column_idx] = valuet::value_t::null_string();
                                }
                            }
                        }
                        break;
                    }
                    }
                    ++page_id;
                }
            }
        };
        filter_tp.run(task, output_attrs.size());
        return results;
    }

} // namespace mycopyscan