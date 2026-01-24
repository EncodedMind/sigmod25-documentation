// Columnar store joins version

#include <hardware.h>
#include <plan.h>
#include <table.h>
#include <iostream>

#include <value_t.h>
#include <column_t.h>
#include <mycopyscan.h>
#include <execute_root.h>

namespace Contest {

using ExecuteResult = std::vector<columnt::column_t>;

ExecuteResult execute_impl(const Plan& plan, size_t node_idx);

struct JoinAlgorithm {
    bool                                             build_left;
    ExecuteResult&                                   left;
    ExecuteResult&                                   right;
    ExecuteResult&                                   results;
    size_t                                           left_col, right_col;
    const std::vector<std::tuple<size_t, DataType>>& output_attrs;

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

    auto run() {
        namespace views = ranges::views;

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
                                results[out_idx].push_back(left[col_idx][left_idx]);
                            } else {
                                results[out_idx].push_back(right[col_idx - left.size()][right_idx]);
                            }
                        }
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
                                results[out_idx].push_back(left[col_idx][left_idx]);
                            } else {
                                results[out_idx].push_back(right[col_idx - left.size()][right_idx]);
                            }
                        }
                    }
                }
            }
        }       
    }
};

ExecuteResult execute_hash_join(const Plan&          plan,
    const JoinNode&                                  join,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs) {
    auto                           left_idx    = join.left;
    auto                           right_idx   = join.right;
    auto&                          left_node   = plan.nodes[left_idx];
    auto&                          right_node  = plan.nodes[right_idx];
    auto&                          left_types  = left_node.output_attrs;
    auto&                          right_types = right_node.output_attrs;
    auto                           left        = execute_impl(plan, left_idx);
    auto                           right       = execute_impl(plan, right_idx);
    ExecuteResult results(output_attrs.size());

    JoinAlgorithm join_algorithm{.build_left = join.build_left,
        .left                                = left,
        .right                               = right,
        .results                             = results,
        .left_col                            = join.left_attr,
        .right_col                           = join.right_attr,
        .output_attrs                        = output_attrs};
    
    join_algorithm.run();
    return results;
}

ExecuteResult execute_scan(const Plan&               plan,
    const ScanNode&                                  scan,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs) {
    auto                           table_id = scan.base_table_id;
    auto&                          input    = plan.inputs[table_id];
    return mycopyscan::copy_scan_value_t(input, output_attrs, static_cast<uint8_t>(table_id));
}

ExecuteResult execute_impl(const Plan& plan, size_t node_idx) {
    auto& node = plan.nodes[node_idx];
    return std::visit(
        [&](const auto& value) {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, JoinNode>) {
                return execute_hash_join(plan, value, node.output_attrs);
            } else {
                return execute_scan(plan, value, node.output_attrs);
            }
        },
        node.data);
}

ColumnarTable execute(const Plan& plan, [[maybe_unused]] void* context) {
    return execute_impl_root(plan, plan.root);
}

void* build_context() {
    return nullptr;
}

void destroy_context([[maybe_unused]] void* context) {}

} // namespace Contest