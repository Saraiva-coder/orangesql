// tests/benchmark/bench_btree.cpp
#include <benchmark/benchmark.h>
#include "../../index/btree.h"
#include <random>

using namespace orangesql;

static void BM_BTreeInsert(benchmark::State& state) {
    for (auto _ : state) {
        BTree<int64_t> btree;
        for (int i = 0; i < state.range(0); i++) {
            btree.insert(i, i);
        }
    }
    state.SetItemsProcessed(state.iterations() * state.range(0));
}

static void BM_BTreeSearch(benchmark::State& state) {
    BTree<int64_t> btree;
    for (int i = 0; i < state.range(0); i++) {
        btree.insert(i, i);
    }
    
    for (auto _ : state) {
        for (int i = 0; i < state.range(0); i++) {
            std::vector<uint64_t> results;
            btree.search(i, results);
            benchmark::DoNotOptimize(results);
        }
    }
    state.SetItemsProcessed(state.iterations() * state.range(0));
}

static void BM_BTreeRangeSearch(benchmark::State& state) {
    BTree<int64_t> btree;
    for (int i = 0; i < state.range(0); i++) {
        btree.insert(i, i);
    }
    
    for (auto _ : state) {
        std::vector<uint64_t> results;
        btree.searchRange(0, state.range(0) - 1, results);
        benchmark::DoNotOptimize(results);
    }
    state.SetItemsProcessed(state.iterations() * state.range(0));
}

static void BM_BTreeBulkLoad(benchmark::State& state) {
    std::vector<std::pair<int64_t, uint64_t>> data;
    for (int i = 0; i < state.range(0); i++) {
        data.push_back({i, i});
    }
    
    for (auto _ : state) {
        BTree<int64_t> btree;
        btree.bulkLoad(data);
    }
    state.SetItemsProcessed(state.iterations() * state.range(0));
}

BENCHMARK(BM_BTreeInsert)->Arg(1000)->Arg(10000)->Arg(100000);
BENCHMARK(BM_BTreeSearch)->Arg(1000)->Arg(10000)->Arg(100000);
BENCHMARK(BM_BTreeRangeSearch)->Arg(1000)->Arg(10000)->Arg(100000);
BENCHMARK(BM_BTreeBulkLoad)->Arg(1000)->Arg(10000)->Arg(100000);

BENCHMARK_MAIN();