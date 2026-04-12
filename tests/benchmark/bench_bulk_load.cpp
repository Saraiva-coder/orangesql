// tests/benchmark/bench_bulk_load.cpp
#include <benchmark/benchmark.h>
#include "../../index/bulk_loader.h"
#include "../../index/btree.h"
#include <random>
#include <algorithm>

using namespace orangesql;

static void BM_BulkLoadSequential(benchmark::State& state) {
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

static void BM_BulkLoadRandom(benchmark::State& state) {
    std::vector<std::pair<int64_t, uint64_t>> data;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int64_t> dist(0, state.range(0) * 10);
    
    for (int i = 0; i < state.range(0); i++) {
        data.push_back({dist(gen), i});
    }
    
    for (auto _ : state) {
        BTree<int64_t> btree;
        btree.bulkLoad(data);
    }
    state.SetItemsProcessed(state.iterations() * state.range(0));
}

static void BM_InsertSequential(benchmark::State& state) {
    for (auto _ : state) {
        BTree<int64_t> btree;
        for (int i = 0; i < state.range(0); i++) {
            btree.insert(i, i);
        }
    }
    state.SetItemsProcessed(state.iterations() * state.range(0));
}

static void BM_InsertRandom(benchmark::State& state) {
    std::vector<int64_t> keys;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int64_t> dist(0, state.range(0) * 10);
    
    for (int i = 0; i < state.range(0); i++) {
        keys.push_back(dist(gen));
    }
    
    for (auto _ : state) {
        BTree<int64_t> btree;
        for (int i = 0; i < state.range(0); i++) {
            btree.insert(keys[i], i);
        }
    }
    state.SetItemsProcessed(state.iterations() * state.range(0));
}

BENCHMARK(BM_BulkLoadSequential)->Arg(1000)->Arg(10000)->Arg(100000);
BENCHMARK(BM_BulkLoadRandom)->Arg(1000)->Arg(10000)->Arg(100000);
BENCHMARK(BM_InsertSequential)->Arg(1000)->Arg(10000)->Arg(100000);
BENCHMARK(BM_InsertRandom)->Arg(1000)->Arg(10000)->Arg(100000);

BENCHMARK_MAIN();