#pragma once

#include <format>
#include <stdexcept>
#include <fstream>
#include <iostream>
#include <iomanip>
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
#include <commands/copy.h>
#include <utils/guc.h>

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

// Escape string for COPY
static void escape_copy_string(std::ostream& os, const char* str) {
    if (str == nullptr) {
        return;  // NULL
    }
    for (const char* p = str; *p; p++) {
        if (*p == '\\') {
            os << "\\\\";
        } else if (*p == '\t') {
            os << "\\t";
        } else if (*p == '\n') {
            os << "\\n";
        } else if (*p == '\r') {
            os << "\\r";
        } else {
            os << *p;
        }
    }
}

class TableLoader {
 public:
  static constexpr size_t BATCH_SIZE = 100000;  // Rows per file batch

  TableLoader(const tpcds_table_def* table_def) : table_def(table_def) {
    reloid_ = DirectFunctionCall1(regclassin, CStringGetDatum(table_def->name));
    rel_ = try_table_open(reloid_, NoLock);
    if (!rel_)
      throw std::runtime_error("try_table_open Failed");

    auto tupDesc = RelationGetDescr(rel_);
    Oid in_func_oid;

    in_functions = new FmgrInfo[tupDesc->natts];
    typioparams = new Oid[tupDesc->natts];
    att_type_oids = new Oid[tupDesc->natts];

    for (auto attnum = 1; attnum <= tupDesc->natts; attnum++) {
      Form_pg_attribute att = TupleDescAttr(tupDesc, attnum - 1);

      getTypeInputInfo(att->atttypid, &in_func_oid, &typioparams[attnum - 1]);
      fmgr_info(in_func_oid, &in_functions[attnum - 1]);
      att_type_oids[attnum - 1] = att->atttypid;
    }

    ncols_ = tupDesc->natts;
    
    slot = MakeSingleTupleTableSlot(tupDesc, &TTSOpsMinimalTuple);
    slot->tts_tableOid = RelationGetRelid(rel_);
    
    // Open temp file for CSV output
    current_batch_ = 0;
    openBatchFile();
  };

  ~TableLoader() {
    flush();  // Ensure remaining rows are written
    closeBatchFile();
    table_close(rel_, NoLock);
    free(in_functions);
    free(typioparams);
    free(att_type_oids);
    ExecDropSingleTupleTableSlot(slot);
  };

  void openBatchFile() {
    if (csv_file_.is_open()) {
      csv_file_.close();
    }
    
    // Build filename in /tmp
    std::string filename = std::string("/tmp/") + table_def->name + "_" + std::to_string(current_batch_) + ".csv";
    csv_file_.open(filename, std::ios::out | std::ios::binary);
    if (!csv_file_.is_open()) {
      throw std::runtime_error(std::string("Failed to open batch file: ") + filename);
    }
    rows_in_batch_ = 0;
  }

  void closeBatchFile() {
    if (csv_file_.is_open()) {
      csv_file_.close();
    }
  }

  bool ColnullCheck() { return nullCheck(table_def->first_column + current_item_); }

  auto& nullItem() {
    slot->tts_isnull[current_item_] = true;
    current_item_++;
    return *this;
  }

  template <typename T>
  auto& addItemInternal(T value) {
    Datum datum;
    if constexpr (std::is_same_v<T, char*> || std::is_same_v<T, const char*> || std::is_same_v<T, char>) {
      if (value == nullptr)
        slot->tts_isnull[current_item_] = true;
      else
        slot->tts_values[current_item_] = DirectFunctionCall3(
            in_functions[current_item_].fn_addr, CStringGetDatum(value), ObjectIdGetDatum(typioparams[current_item_]),
            TupleDescAttr(RelationGetDescr(rel_), current_item_)->atttypmod);
    } else
      slot->tts_values[current_item_] = value;

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
      return addItemInternal(value ? "t" : "f");
    }
  }

  auto& addItemKey(ds_key_t value) {
    if (ColnullCheck() || value == -1) {
      return nullItem();
    } else {
      return addItemInternal(value);
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

      return addItemInternal(fpOutfile);
    }
  }

  auto& addItemStreet(const ds_addr_t& address) {
    if (ColnullCheck()) {
      return nullItem();
    } else {
      if (address.street_name2 == nullptr) {
        return addItemInternal(address.street_name1);
      } else {
        auto s = std::string{address.street_name1} + " " + address.street_name2;
        return addItemInternal(s.c_str());
      }
    }
  }

  auto& addItemDate(ds_key_t value) {
    if (ColnullCheck() || value <= 0) {
      return nullItem();
    } else {
      auto date = date_t{};
      jtodt(&date, static_cast<int>(value));

      auto s = std::format("{:4d}-{:02d}-{:02d}", date.year, date.month, date.day);
      return addItemInternal(s.data());
    }
  }

  auto& start() {
    ExecClearTuple(slot);
    MemSet(slot->tts_values, 0, RelationGetDescr(rel_)->natts * sizeof(Datum));
    MemSet(slot->tts_isnull, false, RelationGetDescr(rel_)->natts * sizeof(bool));
    current_item_ = 0;
    return *this;
  }

  // Write current tuple to CSV file
  void writeTupleToCSV() {
    if (rows_in_batch_ > 0) {
      csv_file_ << '\n';
    }
    
    for (auto i = 0; i < ncols_; i++) {
      if (i > 0) {
        csv_file_ << '\t';
      }
      
      if (slot->tts_isnull[i]) {
        continue;  // NULL - empty field
      }
      
      Datum value = slot->tts_values[i];
      Oid typid = att_type_oids[i];
      
      // Handle different types
      if (typid == BOOLOID) {
        csv_file_ << (DatumGetBool(value) ? 't' : 'f');
      } else if (typid == INT2OID) {
        csv_file_ << (int)DatumGetInt16(value);
      } else if (typid == INT4OID) {
        csv_file_ << DatumGetInt32(value);
      } else if (typid == INT8OID) {
        csv_file_ << (long long)DatumGetInt64(value);
      } else if (typid == FLOAT4OID) {
        csv_file_ << std::setprecision(6) << (double)DatumGetFloat4(value);
      } else if (typid == FLOAT8OID) {
        csv_file_ << std::setprecision(6) << DatumGetFloat8(value);
      } else {
        // Text types - get as cstring and escape
        Datum textValue = DirectFunctionCall1(textout, value);
        escape_copy_string(csv_file_, (const char*)textValue);
      }
    }
  }

  auto& end() {
    ExecStoreVirtualTuple(slot);

    // Write to CSV file
    writeTupleToCSV();
    
    row_count_++;
    rows_in_batch_++;

    // Open new file when batch is full
    if (rows_in_batch_ >= BATCH_SIZE) {
      flush();
    }

    return *this;
  }

  auto row_count() const { return row_count_; }

  void flush() {
    if (rows_in_batch_ == 0) return;

    // Close current file
    csv_file_.flush();
    csv_file_.close();

    // Load this batch via COPY
    std::string filename = std::string("/tmp/") + table_def->name + "_" + std::to_string(current_batch_) + ".csv";
    
    std::string copy_sql = "COPY " + std::string(table_def->name) + 
                           " FROM '" + filename + "' WITH (FORMAT CSV, DELIMITER '\t', NULL '')";
    
    SPI_exec(copy_sql.c_str(), 0);
    
    // Delete the temp file
    std::remove(filename.c_str());
    
    // Move to next batch
    current_batch_++;
    openBatchFile();
  }

  Oid reloid_;
  Relation rel_;
  size_t row_count_ = 0;
  size_t current_item_ = 0;
  size_t rows_in_batch_ = 0;
  size_t current_batch_;
  int ncols_;

  std::ofstream csv_file_;

  FmgrInfo* in_functions;
  Oid* typioparams;
  Oid* att_type_oids;
  TupleTableSlot* slot;
  CommandId mycid = GetCurrentCommandId(true);
  int ti_options = (TABLE_INSERT_SKIP_FSM | TABLE_INSERT_FROZEN | TABLE_INSERT_NO_LOGICAL);

  const tpcds_table_def* table_def;
};

}  // namespace tpcds
