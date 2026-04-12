// include/constants.h
#ifndef CONSTANTS_H
#define CONSTANTS_H

#include <cstddef>

namespace orangesql {

constexpr int PAGE_SIZE = 4096;
constexpr int BUFFER_POOL_SIZE = 8192;
constexpr int MAX_TRANSACTIONS = 1024;
constexpr int MAX_LOCKS = 10000;
constexpr int MAX_COLUMNS = 1000;
constexpr int MAX_ROW_SIZE = PAGE_SIZE / 4;
constexpr int WAL_SEGMENT_SIZE = 16 * 1024 * 1024;
constexpr int CHECKPOINT_INTERVAL_MS = 1000;
constexpr int BTREE_ORDER = 128;
constexpr int CACHE_SIZE = 10000;
constexpr int BULK_INSERT_THRESHOLD = 1000;
constexpr int PARALLEL_SCAN_THRESHOLD = 10000;
constexpr int MAX_CONNECTION_POOL = 50;
constexpr int MAX_QUERY_TIMEOUT_SEC = 300;
constexpr int MAX_LOCK_TIMEOUT_MS = 5000;
constexpr int DEADLOCK_TIMEOUT_MS = 10000;
constexpr int GC_INTERVAL_MS = 60000;

constexpr double DEFAULT_SAMPLE_RATE = 0.1;
constexpr double DEFAULT_SELECTIVITY_THRESHOLD = 0.1;

constexpr uint32 MAGIC_NUMBER_TABLE = 0x5441424C;
constexpr uint32 MAGIC_NUMBER_INDEX = 0x494E4458;
constexpr uint32 MAGIC_NUMBER_WAL = 0x57414C5F;

constexpr uint32 VERSION_MAJOR = 1;
constexpr uint32 VERSION_MINOR = 0;
constexpr uint32 VERSION_PATCH = 0;

}

#endif