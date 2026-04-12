cmake_minimum_required(VERSION 3.15)
project(OrangeSQL VERSION 1.0.0 LANGUAGES CXX)

# ========================
# C++ CONFIG
# ========================
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ========================
# BUILD TYPE DEFAULT
# ========================
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# ========================
# PLATFORM DETECTION
# ========================
if(WIN32)
    add_definitions(-D_CRT_SECURE_NO_WARNINGS)
elseif(APPLE)
    set(OS_MACOS 1)
else()
    set(OS_LINUX 1)
endif()

# ========================
# COMPILER FLAGS
# ========================
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")

# Release
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -DNDEBUG")

# Debug
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g -DDEBUG")

# Linux/macOS threading
if(NOT WIN32)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
endif()

# ========================
# DEPENDENCIES
# ========================
find_package(Threads REQUIRED)
find_package(nlohmann_json REQUIRED)
find_package(GTest REQUIRED)

# Optional: Readline (Linux/macOS)
if(NOT WIN32)
    find_package(Curses)
    find_library(READLINE_LIBRARY readline)

    if(READLINE_LIBRARY AND CURSES_FOUND)
        set(HAVE_READLINE 1)
        message(STATUS "Readline support enabled")
    endif()
endif()

# ========================
# SOURCE FILES
# ========================
set(SOURCES
    cli/main.cpp
    cli/console.cpp

    storage/page.cpp
    storage/buffer_pool.cpp
    storage/table.cpp
    storage/record.cpp

    index/btree.cpp
    index/index_manager.cpp

    transaction/wal.cpp
    transaction/lock_manager.cpp
    transaction/mvcc.cpp

    engine/query_executor.cpp
    engine/optimizer.cpp

    metadata/catalog.cpp
    metadata/schema.cpp
    metadata/statistics.cpp

    cache/cache_manager.cpp

    parser/sql_parser.cpp
    parser/lexer.cpp
)

# ========================
# EXECUTABLE
# ========================
add_executable(orangesql ${SOURCES})

# ========================
# INCLUDE DIRECTORIES
# ========================
target_include_directories(orangesql PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# ========================
# LINK LIBRARIES
# ========================
target_link_libraries(orangesql
    Threads::Threads
    nlohmann_json::nlohmann_json
)

# Platform-specific libs
if(WIN32)
    target_link_libraries(orangesql ws2_32)
elseif(APPLE)
    find_library(CORE_FOUNDATION CoreFoundation)
    target_link_libraries(orangesql ${CORE_FOUNDATION})
else()
    target_link_libraries(orangesql dl rt)
endif()

# Readline support
if(HAVE_READLINE)
    target_link_libraries(orangesql ${READLINE_LIBRARY} ${CURSES_LIBRARIES})
    target_compile_definitions(orangesql PRIVATE HAVE_READLINE)
endif()

# ========================
# TESTING
# ========================
enable_testing()
add_subdirectory(tests)

# ========================
# INSTALLATION
# ========================
install(TARGETS orangesql DESTINATION bin)

install(DIRECTORY include/
    DESTINATION include/orangesql
)

# ========================
# PERFORMANCE PROFILING
# ========================
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    target_compile_options(orangesql PRIVATE -fprofile-generate)
    target_link_options(orangesql PRIVATE -fprofile-generate)
endif()

# ========================
# FINAL MESSAGE
# ========================
message(STATUS "OrangeSQL configured for ${CMAKE_SYSTEM_NAME}")