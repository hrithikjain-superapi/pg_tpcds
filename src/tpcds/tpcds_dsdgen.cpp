#include "tpcds_dsdgen.h"

#include <algorithm>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <format>
#include <optional>
#include <string>
#include <unordered_map>

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

#define DECLARER
#include "address.h"
#include "columns.h"
#include "date.h"
#include "decimal.h"
#include "dist.h"
#include "genrand.h"
#include "nulls.h"
#include "parallel.h"
#include "params.h"
#include "porting.h"
#include "r_params.h"
#include "scaling.h"
#include "tables.h"
#include "tdefs.h"
#include "tpcds_loader.h"
#include "w_call_center.h"
#include "w_catalog_page.h"
#include "w_catalog_returns.h"
#include "w_catalog_sales.h"
#include "w_customer.h"
#include "w_customer_address.h"
#include "w_customer_demographics.h"
#include "w_datetbl.h"
#include "w_household_demographics.h"
#include "w_income_band.h"
#include "w_inventory.h"
#include "w_item.h"
#include "w_promotion.h"
#include "w_reason.h"
#include "w_ship_mode.h"
#include "w_store.h"
#include "w_store_returns.h"
#include "w_store_sales.h"
#include "w_timetbl.h"
#include "w_warehouse.h"
#include "w_web_page.h"
#include "w_web_returns.h"
#include "w_web_sales.h"
#include "w_web_site.h"

namespace tpcds {

static const std::unordered_map<std::string, int> table_id_map = {
    {"call_center", CALL_CENTER},
    {"catalog_page", CATALOG_PAGE},
    {"catalog_returns", CATALOG_RETURNS},
    {"catalog_sales", CATALOG_SALES},
    {"customer", CUSTOMER},
    {"customer_address", CUSTOMER_ADDRESS},
    {"customer_demographics", CUSTOMER_DEMOGRAPHICS},
    {"date_dim", DATET},
    {"household_demographics", HOUSEHOLD_DEMOGRAPHICS},
    {"income_band", INCOME_BAND},
    {"inventory", INVENTORY},
    {"item", ITEM},
    {"promotion", PROMOTION},
    {"reason", REASON},
    {"ship_mode", SHIP_MODE},
    {"store", STORE},
    {"store_returns", STORE_RETURNS},
    {"store_sales", STORE_SALES},
    {"time_dim", TIME},
    {"warehouse", WAREHOUSE},
    {"web_page", WEB_PAGE},
    {"web_returns", WEB_RETURNS},
    {"web_sales", WEB_SALES},
    {"web_site", WEB_SITE},
    {"dbgen_version", DBGEN_VERSION},
};

tpcds_table_def GetTDefByNumber(int table_id) {
  auto tdef = getSimpleTdefsByNumber(table_id);
  tpcds_table_def def;
  def.name = tdef->name;
  def.table_id = table_id;
  def.fl_child = tdef->flags & FL_CHILD ? 1 : 0;
  def.fl_small = tdef->flags & FL_SMALL ? 1 : 0;
  def.first_column = tdef->nFirstColumn;
  return def;
}

TPCDSTableGenerator::TPCDSTableGenerator(double scale_factor, const std::string& table) : _scale_factor{scale_factor} {
  InitConstants::Reset();
  resetCountCount();
  std::string t = std::to_string(scale_factor);
  set_str("SCALE", (char*)t.c_str());  // set SF, which also does a default init (e.g. random seed)
  init_rand();                         // no random numbers without this
  table_def_ = GetTDefByNumber(table_id_map.at(table));
}

std::tuple<ds_key_t, ds_key_t> TPCDSTableGenerator::prepare_loader() {
  ds_key_t k_row_count = get_rowcount(table_def_.table_id);
  ds_key_t k_first_row = 1;

  if (table_def_.fl_small)
    resetCountCount();

  return {k_first_row, k_row_count};
}

int TPCDSTableGenerator::generate_call_center() {
  auto [k_first_row, k_row_count] = prepare_loader();

  TableLoader loader(&table_def_);

  for (auto i = k_first_row; k_row_count; i++, k_row_count--) {
    mk_w_call_center(&loader, i);
  }

  loader.flush();  // Ensure all remaining rows are committed

  return loader.row_count();
}

int TPCDSTableGenerator::generate_catalog_page() {
  auto [k_first_row, k_row_count] = prepare_loader();

  TableLoader loader(&table_def_);

  for (auto catalog_page_index = k_first_row; k_row_count; catalog_page_index++, k_row_count--) {
    // need a pointer to the previous result of mk_w_catalog_page, because
    // cp_department is only set once
    mk_w_catalog_page(&loader, catalog_page_index);
  }

  loader.flush();  // Ensure all remaining rows are committed

  return loader.row_count();
}

int TPCDSTableGenerator::generate_catalog_sales_and_returns() {
  auto [k_first_row, k_row_count] = prepare_loader();

  TableLoader loader_catalog_sales(&table_def_);
  auto ret = GetTDefByNumber(CATALOG_RETURNS);
  TableLoader loader_catalog_returns(&ret);

  TableLoader* loaders[2] = {&loader_catalog_sales, &loader_catalog_returns};

  for (auto catalog_page_index = k_first_row; k_row_count; catalog_page_index++, k_row_count--) {
    mk_w_catalog_sales(&loaders, catalog_page_index);
  }

  // Flush both loaders to commit remaining rows
  loader_catalog_sales.flush();
  loader_catalog_returns.flush();

  // TODO: result
  return std::max(loader_catalog_returns.row_count(), loader_catalog_sales.row_count());
}

int TPCDSTableGenerator::generate_customer_address() {
  auto [k_first_row, k_row_count] = prepare_loader();

  TableLoader loader(&table_def_);

  for (auto catalog_page_index = k_first_row; k_row_count; catalog_page_index++, k_row_count--) {
    mk_w_customer_address(&loader, catalog_page_index);
  }

  loader.flush();  // Ensure all remaining rows are committed

  return loader.row_count();
}

int TPCDSTableGenerator::generate_customer() {
  auto [k_first_row, k_row_count] = prepare_loader();

  TableLoader loader(&table_def_);

  for (auto catalog_page_index = k_first_row; k_row_count; catalog_page_index++, k_row_count--) {
    mk_w_customer(&loader, catalog_page_index);
  }

  loader.flush();  // Ensure all remaining rows are committed

  return loader.row_count();
}

int TPCDSTableGenerator::generate_customer_demographics() {
  auto [k_first_row, k_row_count] = prepare_loader();

  TableLoader loader(&table_def_);

  for (auto catalog_page_index = k_first_row; k_row_count; catalog_page_index++, k_row_count--) {
    mk_w_customer_demographics(&loader, catalog_page_index);
  }

  loader.flush();  // Ensure all remaining rows are committed

  return loader.row_count();
}

int TPCDSTableGenerator::generate_date_dim() {
  auto [k_first_row, k_row_count] = prepare_loader();

  TableLoader loader(&table_def_);

  for (auto catalog_page_index = k_first_row; k_row_count; catalog_page_index++, k_row_count--) {
    mk_w_date(&loader, catalog_page_index);
  }

  loader.flush();  // Ensure all remaining rows are committed

  return loader.row_count();
}

int TPCDSTableGenerator::generate_household_demographics() {
  auto [k_first_row, k_row_count] = prepare_loader();

  TableLoader loader(&table_def_);

  for (auto catalog_page_index = k_first_row; k_row_count; catalog_page_index++, k_row_count--) {
    mk_w_household_demographics(&loader, catalog_page_index);
  }

  loader.flush();  // Ensure all remaining rows are committed

  return loader.row_count();
}

int TPCDSTableGenerator::generate_income_band() {
  auto [k_first_row, k_row_count] = prepare_loader();

  TableLoader loader(&table_def_);

  for (auto catalog_page_index = k_first_row; k_row_count; catalog_page_index++, k_row_count--) {
    mk_w_income_band(&loader, catalog_page_index);
  }

  loader.flush();  // Ensure all remaining rows are committed

  return loader.row_count();
}

int TPCDSTableGenerator::generate_inventory() {
  auto [k_first_row, k_row_count] = prepare_loader();

  TableLoader loader(&table_def_);

  for (auto catalog_page_index = k_first_row; k_row_count; catalog_page_index++, k_row_count--) {
    mk_w_inventory(&loader, catalog_page_index);
  }

  loader.flush();  // Ensure all remaining rows are committed

  return loader.row_count();
}

int TPCDSTableGenerator::generate_item() {
  auto [k_first_row, k_row_count] = prepare_loader();

  TableLoader loader(&table_def_);

  for (auto catalog_page_index = k_first_row; k_row_count; catalog_page_index++, k_row_count--) {
    mk_w_item(&loader, catalog_page_index);
  }

  loader.flush();  // Ensure all remaining rows are committed

  return loader.row_count();
}

int TPCDSTableGenerator::generate_promotion() {
  auto [k_first_row, k_row_count] = prepare_loader();

  TableLoader loader(&table_def_);

  for (auto catalog_page_index = k_first_row; k_row_count; catalog_page_index++, k_row_count--) {
    mk_w_promotion(&loader, catalog_page_index);
  }

  loader.flush();  // Ensure all remaining rows are committed

  return loader.row_count();
}

int TPCDSTableGenerator::generate_reason() {
  auto [k_first_row, k_row_count] = prepare_loader();

  TableLoader loader(&table_def_);

  for (auto catalog_page_index = k_first_row; k_row_count; catalog_page_index++, k_row_count--) {
    mk_w_reason(&loader, catalog_page_index);
  }

  loader.flush();  // Ensure all remaining rows are committed

  return loader.row_count();
}

int TPCDSTableGenerator::generate_ship_mode() {
  auto [k_first_row, k_row_count] = prepare_loader();

  TableLoader loader(&table_def_);

  for (auto catalog_page_index = k_first_row; k_row_count; catalog_page_index++, k_row_count--) {
    mk_w_ship_mode(&loader, catalog_page_index);
  }

  loader.flush();  // Ensure all remaining rows are committed

  return loader.row_count();
}

int TPCDSTableGenerator::generate_store() {
  auto [k_first_row, k_row_count] = prepare_loader();

  TableLoader loader(&table_def_);

  for (auto catalog_page_index = k_first_row; k_row_count; catalog_page_index++, k_row_count--) {
    mk_w_store(&loader, catalog_page_index);
  }

  loader.flush();  // Ensure all remaining rows are committed

  return loader.row_count();
}

int TPCDSTableGenerator::generate_store_sales_and_returns() {
  auto [k_first_row, k_row_count] = prepare_loader();

  TableLoader loader_store_sales(&table_def_);
  auto ret = GetTDefByNumber(STORE_RETURNS);
  TableLoader loader_store_returns(&ret);

  TableLoader* loaders[2] = {&loader_store_sales, &loader_store_returns};

  for (auto catalog_page_index = k_first_row; k_row_count; catalog_page_index++, k_row_count--) {
    mk_w_store_sales(&loaders, catalog_page_index);
  }

  // Flush both loaders to commit remaining rows
  loader_store_sales.flush();
  loader_store_returns.flush();

  return std::max(loader_store_sales.row_count(), loader_store_returns.row_count());
}

int TPCDSTableGenerator::generate_time_dim() {
  auto [k_first_row, k_row_count] = prepare_loader();

  TableLoader loader(&table_def_);

  for (auto catalog_page_index = k_first_row; k_row_count; catalog_page_index++, k_row_count--) {
    mk_w_time(&loader, catalog_page_index);
  }

  loader.flush();  // Ensure all remaining rows are committed

  return loader.row_count();
}

int TPCDSTableGenerator::generate_warehouse() {
  auto [k_first_row, k_row_count] = prepare_loader();

  TableLoader loader(&table_def_);

  for (auto catalog_page_index = k_first_row; k_row_count; catalog_page_index++, k_row_count--) {
    mk_w_warehouse(&loader, catalog_page_index);
  }

  loader.flush();  // Ensure all remaining rows are committed

  return loader.row_count();
}

int TPCDSTableGenerator::generate_web_page() {
  auto [k_first_row, k_row_count] = prepare_loader();

  TableLoader loader(&table_def_);

  for (auto catalog_page_index = k_first_row; k_row_count; catalog_page_index++, k_row_count--) {
    mk_w_web_page(&loader, catalog_page_index);
  }

  loader.flush();  // Ensure all remaining rows are committed

  return loader.row_count();
}

int TPCDSTableGenerator::generate_web_sales_and_returns() {
  auto [k_first_row, k_row_count] = prepare_loader();

  TableLoader loader_web_sales(&table_def_);
  auto ret = GetTDefByNumber(WEB_RETURNS);
  TableLoader loader_web_returns(&ret);

  TableLoader* loaders[2] = {&loader_web_sales, &loader_web_returns};

  for (auto catalog_page_index = k_first_row; k_row_count; catalog_page_index++, k_row_count--) {
    mk_w_web_sales(&loaders, catalog_page_index);
  }

  // Flush both loaders to commit remaining rows
  loader_web_sales.flush();
  loader_web_returns.flush();

  return std::max(loader_web_sales.row_count(), loader_web_returns.row_count());
}

int TPCDSTableGenerator::generate_web_site() {
  auto [k_first_row, k_row_count] = prepare_loader();

  TableLoader loader(&table_def_);

  for (auto catalog_page_index = k_first_row; k_row_count; catalog_page_index++, k_row_count--) {
    mk_w_web_site(&loader, catalog_page_index);
  }

  loader.flush();  // Ensure all remaining rows are committed

  return loader.row_count();
}

}  // namespace tpcds
