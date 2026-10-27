// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The global-config half of `pkg/ddl/db_integration_test.go`
//! (`TestCreateTableTooLarge`, which stores
//! `config.GetGlobalConfig().TableColumnCountLimit` mid-test), moved out of
//! `db_integration_ddl_types_source.rs` into the serial binary.

use crate::support::testkit::{update_global_config, TestKit};

// The column-count half of Go's TestCreateTableTooLarge
// (db_integration_test.go:948) is executable here; the later
// kv.ErrEntryTooLarge assertion still needs a metadata transaction backend,
// which this tier deliberately does not model.
#[test]
fn create_table_column_limit_matches_go() {
    // Go `originLimit := ...` + the restoring store at the end: the guard
    // restores the global config (and holds the serial lock) for the whole
    // body.
    let _restore = update_global_config(|_| {});
    let mut tk = TestKit::new();
    let column_count = 1_018;
    let columns = (1..=column_count)
        .map(|index| format!("c{index} double"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("create table t_too_many ({columns})");
    // Go's default TableColumnCountLimit is 1017: ErrTooManyFields.
    tk.must_get_err_code(&sql, 1117);

    // Go `atomic.StoreUint32(&config.GetGlobalConfig().TableColumnCountLimit, ...)`.
    tidb_config::config_tree::config::update_global(|conf| {
        conf.table_column_count_limit = u32::try_from(column_count).expect("a column count");
    });
    let sql = sql.replace("t_too_many", "t_with_limit");
    // Raising the Go-compatible limit admits the same column count.
    tk.must_exec(&sql);
    assert!(tk.info_schema().contains_in("test", "t_with_limit"));
}
