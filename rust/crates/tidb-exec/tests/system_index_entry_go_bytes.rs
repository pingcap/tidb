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

//! Go-authoritative index entries for the `mysql.*` writer.
//!
//! The metadata is the real thing: `tests/data/mysql_bootstrap_tableinfos.json`
//! is the JSON of the `*model.TableInfo` values Go's own
//! `ddl.BuildTableInfoFromAST` builds for the bootstrap tables. The expected
//! bytes come from
//! `rust/difftests/transaction-tests/fixtures/generate_index_entries.go`.
//!
//! Two tables carry the whole point between them:
//!
//! * `mysql.global_variables` keys on one `VARCHAR(64) utf8mb4_bin` column.
//!   `utf8mb4_bin` is a bin collation, but `NeedRestoredDataWithCollate`'s
//!   VARCHAR carve-out puts it back in scope, so every `SET GLOBAL` this tier
//!   persists must write restored data.
//! * `mysql.db`'s PRIMARY mixes a `utf8mb4_bin` CHAR with `utf8mb4_general_ci`
//!   ones, whose sort keys CASE-FOLD -- the stored spelling of a database name
//!   survives only in the entry value.

use std::collections::BTreeMap;

use tidb_datatype::Datum;
use tidb_exec::system_row_write::{insert_row, RowValues};
use tidb_model::table_info::TableInfo;
use tidb_txnkv::transaction::OptimisticMutation;

const GO_TABLE_INFOS: &str = include_str!("data/mysql_bootstrap_tableinfos.json");
const FIXTURE: &str =
    include_str!("../../../difftests/transaction-tests/fixtures/index_entries.hex");

fn table(name: &str) -> TableInfo {
    let tables: BTreeMap<String, TableInfo> =
        serde_json::from_str(GO_TABLE_INFOS).expect("the captured Go TableInfos decode");
    tables
        .get(name)
        .unwrap_or_else(|| panic!("the bootstrap capture has no mysql.{name}"))
        .clone()
}

fn fixture(name: &str) -> Vec<u8> {
    let prefix = format!("{name}=");
    let hex = FIXTURE
        .lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .unwrap_or_else(|| panic!("fixture has no {name} entry"));
    hex.as_bytes()
        .chunks_exact(2)
        .map(|pair| (nibble(pair[0]) << 4) | nibble(pair[1]))
        .collect()
}

fn nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        other => panic!("fixture has non-hex byte {other:#x}"),
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Every column of `table` set to `NULL`, then the named ones overwritten.
fn row(table: &TableInfo, named: &[(&str, &str)]) -> RowValues {
    let mut values = RowValues::new();
    for column in table.cols() {
        values.insert(column.id, Datum::Null);
    }
    for (name, text) in named {
        let column = table
            .cols()
            .into_iter()
            .find(|column| column.name.lowercase() == *name)
            .unwrap_or_else(|| panic!("no column {name}"));
        values.insert(column.id, Datum::Bytes(text.as_bytes().to_vec()));
    }
    values
}

/// The one index-entry mutation `insert_row` produced for `index_id`.
fn entry(mutations: &[OptimisticMutation], index_id: i64) -> (Vec<u8>, Vec<u8>) {
    let mut found: Vec<(Vec<u8>, Vec<u8>)> = mutations
        .iter()
        // `_i` marks an index key; the id follows it, sign-flipped
        // big-endian (`codec.EncodeInt`).
        .filter(|mutation| mutation.key().get(10) == Some(&b'i'))
        .filter(|mutation| {
            mutation.key().get(11..19)
                == Some(&((index_id as u64) ^ (1 << 63)).to_be_bytes()[..])
        })
        .map(|mutation| (mutation.key().to_vec(), mutation.value().to_vec()))
        .collect();
    assert_eq!(found.len(), 1, "expected one entry for index {index_id}");
    found.pop().unwrap()
}

fn assert_entry(name: &str, mutations: &[OptimisticMutation], index_id: i64) {
    let (key, value) = entry(mutations, index_id);
    assert_eq!(
        hex(&key),
        hex(&fixture(&format!("{name}_key"))),
        "{name}: index key"
    );
    assert_eq!(
        hex(&value),
        hex(&fixture(&format!("{name}_value"))),
        "{name}: index value"
    );
}

/// A `SET GLOBAL` writes `mysql.global_variables`; its PRIMARY entry must
/// carry the variable name's restored data, or a Go reader rebuilding the
/// column from the index gets the space-trimmed sort key.
#[test]
fn a_global_variable_entry_carries_gos_restored_data() {
    let table = table("global_variables");
    let values = row(
        &table,
        &[
            ("variable_name", "max_connections"),
            ("variable_value", "0"),
        ],
    );
    let mutations = insert_row(&table, 1, &values).expect("the row encodes");
    assert_entry("mysql_global_variables", &mutations, 1);
}

/// `GRANT ... ON db.*` writes `mysql.db`, whose PRIMARY holds two
/// `utf8mb4_general_ci` columns: their sort keys case-fold, so `Test` and
/// `test` share one key and only the restored data says which was stored.
#[test]
fn a_database_grant_entry_restores_the_case_its_key_folded() {
    let table = table("db");
    let values = row(&table, &[("host", "%"), ("db", "Test"), ("user", "root")]);
    let mutations = insert_row(&table, 1, &values).expect("the row encodes");
    assert_entry("mysql_db", &mutations, 1);
}
