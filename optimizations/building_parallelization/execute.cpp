// Unchained hash version

#include <hardware.h>
#include <plan.h>
#include <table.h>
#include <iostream>

#include <value_t.h>
#include <column_t.h>
#include <mycopyscan.h>
#include <execute_root.h>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <vector>
#include <memory>

#include <threaded_table.h>
#include <unchained_table.h>

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

    auto run() {
        size_t build_size = build_left ? left[left_col].size() : right[right_col].size();

        auto parse_env_threads = [](const char* s) -> size_t {
            if (!s || !*s) return 0;
            char* end = nullptr;
            unsigned long v = std::strtoul(s, &end, 10);
            if (end == s) return 0;
            return static_cast<size_t>(v);
        };

        size_t num_threads = static_cast<size_t>(SPC__THREAD_COUNT);
        if(num_threads == 0) num_threads = 4;

        if(const char* force = std::getenv("SPC_FORCE_THREADS")){
            const size_t forced = parse_env_threads(force);
            if (forced > 0) num_threads = forced;
        }

        if(build_size < 200000) num_threads = 1;

        size_t threaded_min_build = 600000; // 600,000 rows default
        if(const char* v = std::getenv("SPC_THREADED_MIN_BUILD")){
            const size_t parsed = parse_env_threads(v);
            if (parsed > 0) threaded_min_build = parsed;
        }

        const bool use_threaded = build_size >= threaded_min_build;

        size_t num_partitions = 1;
        while(num_partitions < num_threads) num_partitions *= 2;
        num_threads = num_partitions;

        // original version 
        if(!use_threaded){
            ::UnchainedHashTable hash_table;
            hash_table.reserve(build_size);

            if(build_left){
                for(size_t row_idx = 0; row_idx < left[left_col].size(); ++row_idx){
                    const auto& key = left[left_col][row_idx];
                    if(key.is_null_int32()) continue;
                    hash_table.insert(key.intvalue, row_idx);
                }
                hash_table.finalize();

                for(size_t right_idx = 0; right_idx < right[right_col].size(); ++right_idx){
                    const auto& key = right[right_col][right_idx];
                    if(key.is_null_int32()) continue;

                    size_t len = 0;
                    const ::HashEntry* entries = hash_table.find_range(key.intvalue, len);
                    if(!entries || len == 0) continue;

                    for(size_t i = 0; i < len; ++i){
                        if(entries[i].key != key.intvalue) continue;
                        size_t left_idx = entries[i].row_idx;
                        for(size_t out_idx = 0; out_idx < output_attrs.size(); ++out_idx){
                            auto [col_idx, _] = output_attrs[out_idx];
                            if(col_idx < left.size()){
                                results[out_idx].push_back(left[col_idx][left_idx]);
                            }
                            else{
                                results[out_idx].push_back(right[col_idx - left.size()][right_idx]);
                            }
                        }
                    }
                }
            }
            else{
                for(size_t row_idx = 0; row_idx < right[right_col].size(); ++row_idx){
                    const auto& key = right[right_col][row_idx];
                    if (key.is_null_int32()) continue;
                    hash_table.insert(key.intvalue, row_idx);
                }
                hash_table.finalize();

                for(size_t left_idx = 0; left_idx < left[left_col].size(); ++left_idx){
                    const auto& key = left[left_col][left_idx];
                    if(key.is_null_int32()) continue;

                    size_t len = 0;
                    const ::HashEntry* entries = hash_table.find_range(key.intvalue, len);
                    if(!entries || len == 0) continue;

                    for(size_t i = 0; i < len; ++i){
                        if (entries[i].key != key.intvalue) continue;
                        size_t right_idx = entries[i].row_idx;
                        for(size_t out_idx = 0; out_idx < output_attrs.size(); ++out_idx){
                            auto [col_idx, _] = output_attrs[out_idx];
                            if(col_idx < left.size()){
                                results[out_idx].push_back(left[col_idx][left_idx]);
                            }
                            else{
                                results[out_idx].push_back(right[col_idx - left.size()][right_idx]);
                            }
                        }
                    }
                }
            }
            return;
        }

        // threaded version
        if(build_left){
            
            // Phase 1: Collect

            threaded::GlobalAllocator globalAlloc;
            std::vector<std::unique_ptr<threaded::TupleCollector>> collectors;
            collectors.reserve(num_threads);
            for(size_t i=0; i<num_threads; ++i){
                collectors.push_back(std::make_unique<threaded::TupleCollector>(globalAlloc, num_partitions));
            }

            if(num_threads == 1){
                auto& collector = *collectors[0];
                for(size_t row_idx = 0; row_idx < build_size; ++row_idx){
                    const auto& key = left[left_col][row_idx];
                    if(key.is_null_int32()) continue;
                    collector.consume(threaded::HashEntry(key.intvalue, row_idx));
                }
            }
            else{
                std::vector<std::thread> threads;
                size_t rows_per_thread = (build_size + num_threads - 1) / num_threads;

                for(size_t t = 0; t < num_threads; ++t){
                    threads.emplace_back([&, t](){
                        size_t start = t * rows_per_thread;
                        size_t end = std::min(start + rows_per_thread, build_size);
                        
                        auto& collector = *collectors[t];

                        for(size_t row_idx = start; row_idx < end; ++row_idx){
                            const auto& key = left[left_col][row_idx];
                            if(key.is_null_int32()) continue;
                            collector.consume(threaded::HashEntry(key.intvalue, row_idx));
                        }
                    });
                }

                for(auto& t : threads) t.join();
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

            // Probing

            for(size_t right_idx = 0; right_idx < right[right_col].size(); ++right_idx){
                const auto& key = right[right_col][right_idx];
                if(key.is_null_int32()) continue;

                size_t len = 0;
                const threaded::HashEntry* entries = final_table.find_range(key.intvalue, len);
                if(!entries || len == 0) continue;

                for(size_t i = 0; i < len; ++i){
                    if(entries[i].key != key.intvalue) continue; // Bloom may yield false positives
                    size_t left_idx = entries[i].row_idx;
                    for(size_t out_idx = 0; out_idx < output_attrs.size(); ++out_idx){
                        auto [col_idx, _] = output_attrs[out_idx];
                        if(col_idx < left.size()){
                            results[out_idx].push_back(left[col_idx][left_idx]);
                        }
                        else{
                            results[out_idx].push_back(right[col_idx - left.size()][right_idx]);
                        }
                    }
                }
            }
        }
        else{

            // Phase 1: Collect

            threaded::GlobalAllocator globalAlloc;
            std::vector<std::unique_ptr<threaded::TupleCollector>> collectors;
            collectors.reserve(num_threads);
            for(size_t i=0; i<num_threads; ++i) {
                collectors.push_back(std::make_unique<threaded::TupleCollector>(globalAlloc, num_partitions));
            }

            if(num_threads == 1){
                auto& collector = *collectors[0];
                for(size_t row_idx = 0; row_idx < build_size; ++row_idx){
                    const auto& key = right[right_col][row_idx];
                    if(key.is_null_int32()) continue;
                    collector.consume(threaded::HashEntry(key.intvalue, row_idx));
                }
            }
            else{
                std::vector<std::thread> threads;
                size_t rows_per_thread = (build_size + num_threads - 1) / num_threads;

                for(size_t t = 0; t < num_threads; ++t){
                    threads.emplace_back([&, t](){
                        size_t start = t * rows_per_thread;
                        size_t end = std::min(start + rows_per_thread, build_size);

                        auto& collector = *collectors[t];

                        for(size_t row_idx = start; row_idx < end; ++row_idx){
                            const auto& key = right[right_col][row_idx];
                            if(key.is_null_int32()) continue;
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

            if(num_partitions == 1){
                final_table.postProcessBuild(0, static_cast<uint64_t>(partition_offsets[0]), partition_heads);
            }
            else{
                std::vector<std::thread> build_threads;
                build_threads.reserve(num_partitions);
                for(size_t p = 0; p < num_partitions; ++p){
                    build_threads.emplace_back([&, p]() {
                        final_table.postProcessBuild(
                            static_cast<uint64_t>(p),
                            static_cast<uint64_t>(partition_offsets[p]),
                            partition_heads);
                    });
                }
                for(auto& t : build_threads) t.join();
            }

            for(size_t left_idx = 0; left_idx < left[left_col].size(); ++left_idx){
                const auto& key = left[left_col][left_idx];
                if(key.is_null_int32()) continue;

                size_t len = 0;
                const threaded::HashEntry* entries = final_table.find_range(key.intvalue, len);
                if(!entries || len == 0) continue;
                
                for(size_t i = 0; i < len; ++i) {
                    if(entries[i].key != key.intvalue) continue;
                    size_t right_idx = entries[i].row_idx;
                    for(size_t out_idx = 0; out_idx < output_attrs.size(); ++out_idx) {
                        auto [col_idx, _] = output_attrs[out_idx];
                        if(col_idx < left.size()){
                            results[out_idx].push_back(left[col_idx][left_idx]);
                        }
                        else{
                            results[out_idx].push_back(right[col_idx - left.size()][right_idx]);
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

    // Compute build_left based on actual cardinalities (paper recommendation)
    bool build_left = left[join.left_attr].size() <= right[join.right_attr].size();

    JoinAlgorithm join_algorithm{.build_left = build_left,
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