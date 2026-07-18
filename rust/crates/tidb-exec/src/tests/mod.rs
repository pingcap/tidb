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
// See the License for the specific language governing permissions and
// limitations under the License.

//! The `tidb-exec` unit test suite: one test per feature area, exercising
//! `Database::run` end to end via the local `step` helper (a stateful,
//! multi-statement session) or the table-less `run` helper, against
//! `gorun`-verified expected outcomes (see each test's own doc for what it
//! covers and, where relevant, which `gorun` probe confirmed the
//! assertion). Split by feature area, mirroring the source modules
//! (`aggregate`/`cte`/`ddl`/`table_ddl`/`dml`/`select`/`setopr`/`transaction`/`window`), plus
//! [`expr`] (evaluation-level behavior reached THROUGH execution) and
//! [`session`] (nontransactional system/user variables) — so two agents extending
//! different feature areas never touch the same test file. Shared helpers
//! (`run`, `step`) and imports live here; every submodule starts with
//! `use super::*;`.
//!
//! Historical note: a swath of this suite was once rebuilt after an
//! earlier refactor accidentally lost the crate's original unit tests
//! (see `rust/README.md`'s "Source layout" section and the project memory
//! for the full incident writeup) — those assertions were copied from
//! `difftests/corpus/table/*.golden.txt` (statements and results already
//! verified against real TiDB by `gorun`) rather than hand-derived, so
//! they are known-correct by construction.

use super::*;

/// Parses and executes a table-less SELECT to its result label.
fn run(sql: &str) -> String {
    let stmt = tidb_parser::parse(sql).expect("parse");
    match execute(&stmt) {
        Ok(rs) => rs.label(),
        Err(err) => format!("{err:?}"),
    }
}

/// Runs a script statement against `db`, returning its outcome label.
fn step(db: &mut Database, sql: &str) -> String {
    match db.run(&tidb_parser::parse(sql).unwrap()) {
        Ok(Outcome::Done) => "OK".to_string(),
        Ok(Outcome::Rows(rs)) => rs.label(),
        Err(e) => format!("{e:?}"),
    }
}

mod admin_alter_ddl_jobs;
mod admin_cleanup_table_lock;
mod aggregate;
mod alter_analyze_partition;
mod alter_order_qualified_modify;
mod alter_table_engine_row_format;
mod alter_table_generic_options;
mod alter_table_multi_spec;
mod alter_table_validation;
mod cluster;
mod cte;
mod cte_scalar_union;
mod ddl;
mod ddl_affinity;
mod ddl_partition_metadata;
mod dml;
mod drop_database;
mod execute_sql_source;
mod exists_setopr;
mod explain_values;
mod expr;
mod load_data;
mod partition_add_empty;
mod partition_check_import;
mod partition_discard;
mod partition_interval;
mod partition_merge_first;
mod partition_split_maxvalue;
mod placement;
mod read_only_path;
mod select;
mod sequence;
mod session;
mod setopr;
mod show_builtins_full_tables;
mod show_charset;
mod show_engines;
mod show_master_privileges;
mod table_ddl;
mod transaction;
mod window;
