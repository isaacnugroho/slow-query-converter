# MariaDB Slow Query to CSV Converter v0.2.0

## 🚀 Release Notes

A high-performance Rust utility to parse MariaDB slow query logs and convert them into clean, multiline CSV format suitable for analysis in spreadsheet software.

### ✨ Features

- **Fast Processing**: Optimized for large log files with minimal memory allocations
- **Multiline Query Support**: Properly handles multi-line SQL queries with CSV escaping
- **Metadata Extraction**: Extracts and separates SET timestamp and USE schema statements
- **Flexible Output**: Output to file or stdout
- **Clean CSV Format**: Quoted fields compatible with Excel and other tools

### 🔧 Performance Optimizations

- Reduced string allocations and cloning operations
- Single-pass query processing for better performance
- Pre-allocated string capacity to minimize reallocations
- Efficient regex matching with static compilation

### 📊 Output Columns

- time, user, host, thread_id, schema, qc_hit
- set_timestamp, use_schema, query
- query_time, lock_time, rows_sent, rows_examined, rows_affected, bytes_sent

### 🛠️ Usage

```bash
# Convert to CSV file
slow-query-converter -i slow.log -o output.csv

# Output to stdout
slow-query-converter -i slow.log
```

### 📦 Installation

```bash
cargo install slow-query-converter
```

### 🏗️ Build from Source

```bash
git clone https://github.com/isaacnugroho/slow-query-converter.git
cd slow-query-converter
cargo build --release
```

### 🔗 Repository

https://github.com/isaacnugroho/slow-query-converter

### 📝 Changelog

#### v0.2.0
- Version bump to 0.2.0
- Updated dependencies and build configuration
- Enhanced CI/CD workflows for Windows and Linux
- Added comprehensive integration tests
- Fixed clippy warnings and code quality improvements

#### v0.1.0
- Initial release with performance optimizations
- Support for MariaDB slow query log format
- CSV output with proper multiline query handling
- Command-line interface with flexible output options
