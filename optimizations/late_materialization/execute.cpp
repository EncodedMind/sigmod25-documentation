// Late materialization version

#include <hardware.h>
#include <plan.h>
#include <table.h>
#include <iostream>

#include <value_t.h>
#include <mycopyscan.h>
#include <mytocolumnar.h>

namespace Contest {

using ExecuteResult = std::vector<std::vector<valuet::value_t>>;

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
        
        size_t build_size = build_left ? left.size() : right.size();
        build_size = nextpow2(build_size) * 2; // next power of 2, doubled
        std::unordered_map<int32_t, std::vector<size_t>> hash_table;
        hash_table.reserve(build_size);

        if (build_left) {
            for(auto&& [idx, record]: left | views::enumerate){
                const auto& key = record[left_col];
                if(key.is_null_int32()) continue;

                if (auto itr = hash_table.find(key.intvalue); itr == hash_table.end()) {
                    hash_table.emplace(key.intvalue, std::vector<size_t>(1, idx));
                } else {
                    itr->second.push_back(idx);
                }
            }
            for(auto& right_record: right){
                const auto& key = right_record[right_col];
                if(key.is_null_int32()) continue;

                if (auto itr = hash_table.find(key.intvalue); itr != hash_table.end()) {
                    for (auto left_idx: itr->second) {
                        auto& left_record = left[left_idx];
                        std::vector<valuet::value_t> new_record;
                        new_record.reserve(output_attrs.size());
                        for (auto [col_idx, _]: output_attrs) {
                            if (col_idx < left_record.size()) {
                                new_record.emplace_back(left_record[col_idx]);
                            } else {
                                new_record.emplace_back(
                                    right_record[col_idx - left_record.size()]);
                            }
                        }
                        results.emplace_back(std::move(new_record));
                    }
                }
            }
        } else {
            for(auto&& [idx, record]: right | views::enumerate){
                const auto& key = record[right_col];
                if(key.is_null_int32()) continue;

                if (auto itr = hash_table.find(key.intvalue); itr == hash_table.end()) {
                    hash_table.emplace(key.intvalue, std::vector<size_t>(1, idx));
                } else {
                    itr->second.push_back(idx);
                }
            }
            for (auto& left_record: left) {
                const auto& key = left_record[left_col];
                if(key.is_null_int32()) continue;

                if (auto itr = hash_table.find(key.intvalue); itr != hash_table.end()) {
                    for (auto right_idx: itr->second) {
                        auto& right_record = right[right_idx];
                        std::vector<valuet::value_t> new_record;
                        new_record.reserve(output_attrs.size());
                        for (auto [col_idx, _]: output_attrs) {
                            if (col_idx < left_record.size()) {
                                new_record.emplace_back(left_record[col_idx]);
                            } else {
                                new_record.emplace_back(
                                    right_record[col_idx - left_record.size()]);
                            }
                        }
                        results.emplace_back(std::move(new_record));
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
    std::vector<std::vector<valuet::value_t>> results;

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
    auto ret = execute_impl(plan, plan.root);
    return mytocolumnar::to_columnar_value_t(ret, plan);
}

void* build_context() {
    return nullptr;
}

void destroy_context([[maybe_unused]] void* context) {}

} // namespace Contest