# tests/benchmark/CMakeLists.txt
add_executable(bench_btree bench_btree.cpp)
target_link_libraries(bench_btree orangesql benchmark::benchmark Threads::Threads)

add_executable(bench_bulk_load bench_bulk_load.cpp)
target_link_libraries(bench_bulk_load orangesql benchmark::benchmark Threads::Threads)