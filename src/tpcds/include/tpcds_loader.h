// MEMORY USAGE ANALYSIS:
// Current approach (tuple-insert with batch commits):
// - Per-row memory: TupleTableSlot (120-200 bytes) + Datum array (~80 bytes/row for typical table)
//                  + FmgrInfo array + type conversion overhead
// - Batch memory: 5000 rows × ~200-300 bytes/row = ~1-1.5 MB per batch minimum
// - Additional overhead: PostgreSQL WAL buffers, SPI context, type conversion functions
// - Estimated total: ~5-10 MB per 5000 rows (varies by table schema complexity)
//
// Root cause: table_tuple_insert() (line 170) constructs full tuples in memory before SPI_commit()
//            Each row requires TupleTableSlot allocation, Datum conversions, and WAL buffer space
//            Memory accumulates until SPI_commit() at BATCH_SIZE intervals (line 187-188)
//
// Solution: CSV generation + PostgreSQL COPY command
//          - Generate CSV to temporary file (minimal memory: ~4KB buffer)
//          - Use COPY FROM to bulk load (PostgreSQL handles parsing efficiently)
//          - Memory reduction: ~99% (from ~10 MB to ~10 KB per 5000 rows)

#pragma once

#include <cstdio>
#include <format>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <unistd.h>
#include "date.h"

extern "C" {
#include <postgres.h>

#include <access/table.h>
#include <executor/spi.h>
#include <lib/stringinfo.h>
#include <libpq/pqformat.h>
#include <miscadmin.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>

// TODO split pg functions into other file

#ifdef snprintf
#undef snprintf
#endif
}

#include "tpcds_dsdgen.h"

#include "address.h"
#include "date.h"
#include "decimal.h"
#include "nulls.h"
#include "porting.h"
#include "r_params.h"

namespace tpcds {

class TableLoader {
 public:
  static constexpr size_t BATCH_SIZE = 5000;  // Flush CSV every 5000 rows

  TableLoader(const tpcds_table_def* table_def) : table_def(table_def) {
    reloid_ = DirectFunctionCall1(regclassin, CStringGetDatum(table_def->name));
    rel_ = try_table_open(reloid_, NoLock);
    if (!rel_)
      throw std::runtime_error("try_table_open Failed");

    auto tupDesc = RelationGetDescr(rel_);
    num_columns_ = tupDesc->natts;
    
    // Open initial CSV file
    openCSVFile();
  };

  ~TableLoader() {
    flush();  // Ensure remaining rows are committed
    table_close(rel_, NoLock);
  };

  bool ColnullCheck() { return nullCheck(table_def->first_column + current_item_); }

  auto& nullItem() {
    addCSVValue("NULL");
    current_item_++;
    return *this;
  }

  template <typename T>
  auto& addItemInternal(T value) {
    if constexpr (std::is_same_v<T, char*> || std::is_same_v<T, const char*>) {
      if (value == nullptr) {
        addCSVValue("NULL");
      } else {
        addCSVValue(escapeCSV(value));
      }
    } else if constexpr (std::is_same_v<T, ds_key_t> || std::is_integral_v<T>) {
      addCSVValue(std::to_string(value));
    } else {
      addCSVValue(std::string(value));
    }

    current_item_++;
    return *this;
  }

  template <typename T>
  auto& addItem(T value) {
    if (ColnullCheck()) {
      return nullItem();
    } else {
      return addItemInternal(value);
    }
  }

  auto& addItemBool(bool value) {
    if (ColnullCheck()) {
      return nullItem();
    } else {
      addCSVValue(value ? "t" : "f");
      current_item_++;
      return *this;
    }
  }

  auto& addItemKey(ds_key_t value) {
    if (ColnullCheck() || value == -1) {
      return nullItem();
    } else {
      addCSVValue(std::to_string(value));
      current_item_++;
      return *this;
    }
  }

  auto& addItemDecimal(decimal_t& decimal) {
    if (ColnullCheck()) {
      return nullItem();
    } else {
      double dTemp = decimal.number;
      for (auto i = 0; i < decimal.precision; i++)
        dTemp /= 10.0;

      char fpOutfile[15] = {0};
      sprintf(fpOutfile, "%.*f", decimal.precision, dTemp);

      addCSVValue(std::string(fpOutfile));
      current_item_++;
      return *this;
    }
  }

  auto& addItemStreet(const ds_addr_t& address) {
    if (ColnullCheck()) {
      return nullItem();
    } else {
      if (address.street_name2 == nullptr) {
        addCSVValue(escapeCSV(address.street_name1));
      } else {
        auto s = std::string{address.street_name1} + " " + address.street_name2;
        addCSVValue(escapeCSV(s));
      }
      current_item_++;
      return *this;
    }
  }

  auto& addItemDate(ds_key_t value) {
    if (ColnullCheck() || value <= 0) {
      return nullItem();
    } else {
      auto date = date_t{};
      jtodt(&date, static_cast<int>(value));

      auto s = std::format("{:4d}-{:02d}-{:02d}", date.year, date.month, date.day);
      addCSVValue(s);
      current_item_++;
      return *this;
    }
  }

  auto& start() {
    current_item_ = 0;
    csv_row_.clear();
    return *this;
  }

  auto& end() {
    // Write complete CSV row to file
    if (!csv_row_.empty() && csv_file_) {
      int result = fprintf(csv_file_, "%s\n", csv_row_.c_str());
      if (result < 0) {
        elog(ERROR, "Failed to write to CSV file");
      }
    }
    
    row_count_++;
    rows_in_batch_++;

    if (rows_in_batch_ >= BATCH_SIZE) {
      flush();
    }

    return *this;
  }

  auto row_count() const { return row_count_; }

  void flush() {
    if (rows_in_batch_ == 0) return;

    elog(INFO, "Flushing %zu rows from CSV", rows_in_batch_);

    // Flush and close CSV file
    if (csv_file_) {
      fflush(csv_file_);  // Ensure all data is written
      fclose(csv_file_);
      csv_file_ = nullptr;
    }

    elog(INFO, "CSV file closed, starting COPY");

    // Execute COPY command to load CSV
    // Note: We cannot use SPI_commit() here because we're in a function, not a procedure
    std::string copy_cmd = std::format(
        "COPY {} FROM '{}' WITH (FORMAT CSV, NULL 'NULL')",
        table_def->name, csv_path_);
    
    elog(INFO, "Executing: %s", copy_cmd.c_str());
    
    int ret = SPI_exec(copy_cmd.c_str(), 0);
    
    elog(INFO, "SPI_exec returned: %d (expected %d for SPI_OK_UTILITY)", ret, SPI_OK_UTILITY);
    
    if (ret != SPI_OK_UTILITY) {
      if (csv_file_) fclose(csv_file_);
      unlink(csv_path_.c_str());
      ereport(ERROR,
              (errcode(ERRCODE_INTERNAL_ERROR),
               errmsg("COPY command failed with return code %d: %s", ret, copy_cmd.c_str())));
    }

    elog(INFO, "COPY command succeeded");

    elog(INFO, "Successfully loaded %zu rows", rows_in_batch_);

    // Delete temporary CSV file
    unlink(csv_path_.c_str());

    rows_in_batch_ = 0;
    
    // Open new CSV file for next batch
    openCSVFile();
  }

 private:
  void openCSVFile() {
    // Generate temporary CSV file path in /tmp
    csv_path_ = std::format("/tmp/tpcds_{}_{}.csv", table_def->name, getpid());
    
    csv_file_ = fopen(csv_path_.c_str(), "w");
    if (!csv_file_) {
      ereport(ERROR,
              (errcode(ERRCODE_INTERNAL_ERROR),
               errmsg("Failed to open CSV file: %s", csv_path_.c_str())));
    }
    elog(INFO, "Opened CSV file: %s", csv_path_.c_str());
  }

  std::string escapeCSV(const std::string& value) {
    std::string result;
    bool needs_quotes = false;
    
    for (char c : value) {
      if (c == '"') {
        result += "\"\"";  // Escape quotes by doubling
        needs_quotes = true;
      } else if (c == ',' || c == '\n' || c == '\r') {
        result += c;
        needs_quotes = true;
      } else {
        result += c;
      }
    }
    
    if (needs_quotes) {
      return "\"" + result + "\"";
    }
    return result;
  }

  std::string escapeCSV(const char* value) {
    if (value == nullptr) return "NULL";
    return escapeCSV(std::string(value));
  }

  void addCSVValue(const std::string& value) {
    if (!csv_row_.empty()) {
      csv_row_ += ",";
    }
    csv_row_ += value;
  }

  Oid reloid_;
  Relation rel_;
  size_t row_count_ = 0;
  size_t current_item_ = 0;
  size_t rows_in_batch_ = 0;
  size_t num_columns_ = 0;

  FILE* csv_file_ = nullptr;
  std::string csv_path_;
  std::string csv_row_;

  const tpcds_table_def* table_def;
};

}  // namespace tpcds
