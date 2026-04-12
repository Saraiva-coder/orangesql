# tests/CMakeLists.txt
add_executable(unit_tests
    unit/test_btree.cpp
    unit/test_buffer_pool.cpp
    unit/test_wal.cpp
    unit/test_concurrency.cpp
)

target_link_libraries(unit_tests
    orangesql
    GTest::gtest
    GTest::gtest_main
    Threads::Threads
)

target_include_directories(unit_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/..)

add_test(NAME BTreeTest COMMAND unit_tests --gtest_filter=BTreeTest.*)
add_test(NAME BufferPoolTest COMMAND unit_tests --gtest_filter=BufferPoolTest.*)
add_test(NAME WALTest COMMAND unit_tests --gtest_filter=WALTest.*)
add_test(NAME ConcurrencyTest COMMAND unit_tests --gtest_filter=ConcurrencyTest.*)

add_executable(integration_tests
    integration/test_index.cpp
    integration/test_recovery.cpp
)

target_link_libraries(integration_tests
    orangesql
    GTest::gtest
    GTest::gtest_main
    Threads::Threads
)

target_include_directories(integration_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/..)

add_test(NAME IndexIntegrationTest COMMAND integration_tests --gtest_filter=IndexIntegrationTest.*)
add_test(NAME RecoveryIntegrationTest COMMAND integration_tests --gtest_filter=RecoveryIntegrationTest.*)