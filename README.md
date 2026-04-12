# 🟠 OrangeSQL

**A high-performance, ACID-compliant SQL database engine written in modern C++17.**

Built from scratch with production-ready performance and a clean, modular architecture.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![C++17](https://img.shields.io/badge/C%2B%2B-17-blue.svg)](https://isocpp.org/)
[![Platform](https://img.shields.io/badge/platform-Windows%20%7C%20Linux%20%7C%20macOS-lightgrey)]()


---

## 📖 What is OrangeSQL?

OrangeSQL is a relational database engine developed entirely from the ground up in C++17. It implements fundamental database concepts — from storage management and transaction control to query optimization and crash recovery — without relying on third-party database libraries.

The goal is to deliver a solid, educational, and extensible SQL engine that balances correctness, performance, and clean code.

---

## ✅ Features

### Core Engine
- **ACID Transactions** — full atomicity, consistency, isolation, and durability
- **MVCC** — Multi-Version Concurrency Control for non-blocking reads
- **WAL** — Write-Ahead Logging for crash safety
- **Checkpointing & Crash Recovery** — automatic recovery after unexpected shutdowns

### Storage Layer
- Page-based storage with 4KB pages
- Buffer pool with LRU eviction policy
- File manager and record management
- Support for multiple buffer pools

### Indexing
- B-Tree index with caching and concurrent access
- Bulk loading support
- Index statistics for query planning

### Query Processing
- Hand-written SQL parser and lexer
- Query optimizer with index-aware planning
- Index scan and join processing

### Transaction Management
- Lock Manager with shared and exclusive locks
- Four isolation levels (Read Uncommitted → Serializable)
- Transaction and log managers

### Metadata & Caching
- Catalog, schema management, and statistics
- LRU cache, page cache, and cache manager

### CLI (Interactive Shell)
- Interactive REPL with formatted output
- Meta-commands and query timing
- CSV import/export

### Testing
- Unit tests via Google Test
- Integration tests
- Performance benchmarks
- Concurrency tests

---

## 🔄 Roadmap

| Short-term | Medium-term | Long-term |
|:-----------|:------------|:----------|
| Hash Joins | Full-Text Search | Distributed Queries |
| Parallel Query Execution | JSON Support | Vectorized Execution |
| Prepared Statements | Materialized Views | Columnar Storage |
| Foreign Keys | Replication | Encryption |
| Query Result Caching | Partitioning | Graph Queries |
| | Stored Procedures | Time-series Extensions |

---

## 📋 Requirements

### Compiler & Build Tools
| Tool | Minimum Version |
|:-----|:----------------|
| GCC / Clang / MSVC | GCC 9+ · Clang 10+ · MSVC 2019+ |
| CMake | 3.15+ |
| Git | 2.20+ |

### Dependencies
| Library | Version | Purpose |
|:--------|:--------|:--------|
| nlohmann/json | 3.11.0+ | JSON configuration |
| Google Test | 1.11+ | Unit testing *(optional)* |
| Google Benchmark | 1.7+ | Benchmarking *(optional)* |
| pthread | — | Threading on Linux/macOS |
| readline | 8.0+ | CLI history *(optional)* |

### System Resources
| Resource | Minimum | Recommended |
|:---------|:--------|:------------|
| RAM | 512 MB | 4 GB+ |
| Disk | 100 MB | 1 GB+ |
| CPU | 1 core | 4+ cores |

---

## 🔧 Installation

### 1. Install Prerequisites

**Ubuntu / Debian**
```bash
sudo apt update
sudo apt install -y build-essential cmake git
sudo apt install -y libgtest-dev libbenchmark-dev nlohmann-json3-dev libreadline-dev
```

**macOS (Homebrew)**
```bash
brew install cmake git
brew install googletest google-benchmark nlohmann-json readline
```

**Windows (MSYS2)**
```bash
pacman -Syu
pacman -S mingw-w64-ucrt-x86_64-gcc mingw-w64-ucrt-x86_64-cmake mingw-w64-ucrt-x86_64-make git
pacman -S mingw-w64-ucrt-x86_64-nlohmann-json mingw-w64-ucrt-x86_64-gtest
```

**Windows (Visual Studio + vcpkg)**
```bash
git clone https://github.com/Microsoft/vcpkg.git
cd vcpkg && .\bootstrap-vcpkg.bat
.\vcpkg install nlohmann-json gtest benchmark
```

---

### 2. Build OrangeSQL

**Linux & macOS**
```bash
git clone https://github.com/orangesql/orangesql.git
cd orangesql && mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . --parallel $(nproc)
ctest --output-on-failure   # optional: run tests
sudo make install            # optional: install system-wide
```

**Windows (MSYS2)**
```bash
git clone https://github.com/orangesql/orangesql.git
cd orangesql && mkdir build && cd build
cmake .. -G "MSYS Makefiles" -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
ctest --output-on-failure
```

**Windows (Visual Studio)**
```bash
git clone https://github.com/orangesql/orangesql.git
cd orangesql && mkdir build && cd build
cmake .. -G "Visual Studio 17 2022" -A x64
cmake --build . --config Release --parallel
ctest -C Release --output-on-failure
```

### Build Variants
```bash
# Debug build with address sanitizer
cmake .. -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS="-fsanitize=address"

# Release with tests enabled
cmake .. -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=ON

# Full build (tests + benchmarks)
cmake .. -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=ON -DBUILD_BENCHMARKS=ON
```

---

## 🚀 Usage

### Starting the Shell
```bash
./orangesql

# With options:
./orangesql --config /path/to/config.json --data-dir /path/to/data --verbose
```

### SQL Examples

**DDL — Schema Management**
```sql
CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), email TEXT, age INT);
CREATE INDEX idx_users_email ON users(email);
DROP TABLE users;
DROP INDEX idx_users_email;
```

**DML — Data Manipulation**
```sql
INSERT INTO users VALUES (1, 'Anita', 'anita@example.com', 25);
SELECT * FROM users WHERE age > 25;
UPDATE users SET age = 26 WHERE id = 1;
DELETE FROM users WHERE id = 1;
```

**Transactions**
```sql
BEGIN;
  UPDATE users SET age = age + 1;
COMMIT;
-- or ROLLBACK;
```

**Inspection**
```sql
SHOW TABLES;
DESCRIBE users;
EXPLAIN SELECT * FROM users WHERE age > 25;
```

### Meta-Commands (Shell Only)

| Command | Description |
|:--------|:------------|
| `\h` or `help` | Show help |
| `\v` or `version` | Show version |
| `\c` or `clear` | Clear screen |
| `\s` or `status` | Show database status |
| `export file.csv` | Export results to CSV |
| `import file.csv` | Import from CSV |
| `exit` or `quit` | Exit OrangeSQL |

### Running SQL Scripts
```bash
# From the command line:
./orangesql < script.sql

# From inside the shell:
orangesql> \i script.sql
```

---

## 📊 Benchmarks

```bash
./bench_btree --benchmark_min_time=3 --benchmark_repetitions=5
./bench_bulk_load
```

**Sample Results:**
```
Benchmark                           Time           CPU
BM_BTreeInsert/1000              0.123 ms      0.123 ms
BM_BTreeInsert/10000             1.456 ms      1.456 ms
BM_BTreeSearch/1000              0.089 ms      0.089 ms
BM_BTreeSearch/10000             0.956 ms      0.956 ms
BM_BulkLoadSequential/10000      0.567 ms      0.567 ms
BM_BulkLoadRandom/10000          1.234 ms      1.234 ms
```

---

## 🧪 Testing

```bash
# Run all tests
ctest

# Run a specific test suite
ctest -R BTreeTest

# Verbose output
ctest --output-on-failure -V

# Run with filter
./unit_tests --gtest_filter=BTreeTest.InsertAndSearchInt

# Generate coverage report
cmake .. -DCMAKE_BUILD_TYPE=Debug -DCOVERAGE=ON
cmake --build .
make coverage
```

---

## 🗂️ Project Structure

```
orangesql/
├── cli/              # Interactive shell (REPL, meta-commands, output formatting)
├── parser/           # SQL lexer and parser
├── engine/           # Query executor and optimizer
├── storage/          # Page manager, buffer pool, record management
├── index/            # B-Tree index and index management
├── transaction/      # MVCC, WAL, lock manager, crash recovery
├── metadata/         # Catalog, schema, and statistics
├── cache/            # LRU cache implementations
├── include/          # Public header files
├── tests/
│   ├── unit/         # Unit tests (Google Test)
│   ├── integration/  # Integration tests
│   └── benchmark/    # Performance benchmarks
├── data/             # Runtime database storage directory
└── CMakeLists.txt    # Build configuration
```

---

## 📄 License

This project is licensed under the **MIT License** — see the [LICENSE](LICENSE) file for details.

---

<div align="center">
  <sub>Built with ☕ and C++17</sub>
</div>