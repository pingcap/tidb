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

//! The multi-schema-change admission check that reads the global config
//! (Go `config.GetGlobalConfig().TableColumnCountLimit`, read by
//! `checkAddColumnTooManyColumns`): moved out of
//! `tests_ddl_multi_schema_change_sql.rs` so the `all` suite never observes
//! the lowered limit.

use crate::support::testkit::{update_global_config, TestKit};
use tidb_session::WarningLevel;

/// The column limit counts the ORIGINAL table plus every added column of the
/// whole statement (a sibling DROP does not make room), and an
/// `ADD COLUMN IF NOT EXISTS` over an existing column still meets the limit
/// first, filing no note.
#[test]
fn multi_schema_change_column_limits_use_original_and_combined_counts() {
    // Holds the serial lock and restores the config at the end; the limit is
    // lowered after the kit's bootstrap, which creates wider system tables.
    let _restore = update_global_config(|_| {});
    let mut tk = TestKit::new();
    tidb_config::config_tree::config::update_global(|conf| conf.table_column_count_limit = 2);
    tk.must_exec("create table t (a int, b int)");
    tk.must_get_err_code("alter table t drop column b, add column c int", 1117);
    tk.must_get_err_code("alter table t add column if not exists a int", 1117);
    let notes: Vec<_> = tk
        .session()
        .warnings()
        .iter()
        .filter(|warning| warning.warning_level() != WarningLevel::Error)
        .map(|warning| (warning.code(), warning.message().to_owned()))
        .collect();
    assert!(notes.is_empty(), "{notes:?}");
}
