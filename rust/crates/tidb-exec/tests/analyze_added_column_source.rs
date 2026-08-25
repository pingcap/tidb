// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A column added by `ALTER TABLE` is not NULL in the rows that predate it.
//!
//! `ALTER TABLE t ADD COLUMN c INT DEFAULT 5` rewrites no rows: every row
//! already stored simply has no entry for `c`, and TiDB substitutes the
//! column's `OriginDefaultValue` when it reads one. Go's `ANALYZE` gets that
//! substitution from the coprocessor -- `pkg/executor/builder.go:3246` calls
//! `tables.SetPBColumnsDefaultValue`, which encodes
//! `GetColOriginDefaultValueWithoutStrictSQLMode` into the scan request -- so
//! the sample carries `5`, not NULL.
//!
//! Captured from a real TiDB (100 rows, `ADD COLUMN c INT DEFAULT 5`,
//! `tidb_analyze_version=2`, `ANALYZE TABLE zzd`):
//!
//! ```text
//! mysql.stats_histograms (hist_id 2): distinct_count 1, null_count 0
//! SHOW STATS_TOPN:                    c  value 5  count 100
//! SHOW STATS_BUCKETS:                 no buckets for c -- all of it is TopN
//! ```
//!
//! Reading those rows as NULL instead would give `c` a `null_count` of every
//! old row and no bucket covering `5`, so `WHERE c = 5` would estimate ~0
//! rows.

use std::collections::BTreeMap;

use tidb_codec::encode_row_key;
use tidb_datatype::Datum;
use tidb_exec::cluster_analyze::{analyze_table, AnalyzeOptions};
use tidb_exec::cluster_catalog::{
    ClusterCatalogError, MetaPairs, MetaSnapshot, PagedMetaSnapshot,
};
use tidb_exec::table_info_build::{build_table_info, ClusteredIndexDefMode};
use tidb_model::column::ColumnDefaultValue;
use tidb_model::table_info::TableInfo;
use tidb_tablecodec::encode_table_row;

#[derive(Default)]
struct RowStore {
    pairs: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl MetaSnapshot for RowStore {
    fn get(&mut self, raw_key: &[u8]) -> Result<Option<Vec<u8>>, ClusterCatalogError> {
        Ok(self.pairs.get(raw_key).cloned())
    }

    fn scan_prefix(&mut self, prefix: &[u8]) -> Result<MetaPairs, ClusterCatalogError> {
        Ok(self
            .pairs
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }
}

impl PagedMetaSnapshot for RowStore {
    fn scan_page(
        &mut self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<MetaPairs, ClusterCatalogError> {
        Ok(self
            .pairs
            .range(start.to_vec()..end.to_vec())
            .take(limit)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }
}

/// `CREATE TABLE zzd (a INT)` followed by
/// `ALTER TABLE zzd ADD COLUMN c INT DEFAULT 5`, as the catalog records it:
/// the added column is public and carries the origin default the DDL
/// computed.
fn table_after_add_column() -> TableInfo {
    let statement = tidb_parser::parse("CREATE TABLE zzd (a INT, c INT DEFAULT 5)")
        .expect("the fixture parses");
    let tidb_ast::Stmt::Ddl(ddl) = statement else {
        panic!("the fixture is a CREATE TABLE");
    };
    let tidb_ast::DdlStmt::CreateTable(create) = ddl.as_ref() else {
        panic!("the fixture is a CREATE TABLE");
    };
    let mut table = build_table_info(create, "utf8mb4", "utf8mb4_bin", ClusteredIndexDefMode::On)
        .expect("the fixture is a table this node can express");
    table.id = 4242;
    let added = table
        .columns
        .iter_deref()
        .find(|column| column.read().name.lowercase() == "c")
        .expect("the fixture declares c");
    added
        .write()
        .set_origin_default_value(Some(ColumnDefaultValue::str("5")))
        .expect("an INT origin default is valid");
    table
}

/// One hundred rows written before `c` existed: each carries `a` only.
fn rows_predating_the_added_column(table: &TableInfo) -> RowStore {
    let column_a = table
        .columns
        .iter_deref()
        .find(|column| column.read().name.lowercase() == "a")
        .expect("the fixture declares a");
    let column_a_id = column_a.read().id;
    let mut store = RowStore::default();
    for handle in 1..=100_i64 {
        let value = encode_table_row(None, &[Datum::Int(handle)], &[column_a_id], true, None)
            .expect("a one-column row encodes");
        let key = encode_row_key(
            table.id,
            &tidb_codec::encode_key(&[Datum::Int(handle)]).expect("a handle encodes"),
        );
        store.pairs.insert(key, value);
    }
    store
}

#[test]
fn a_column_added_after_the_rows_analyzes_as_its_origin_default() {
    let table = table_after_add_column();
    let mut store = rows_predating_the_added_column(&table);
    let report = analyze_table(
        &mut store,
        &table,
        &AnalyzeOptions::default(),
        None,
        440_000_000_000_000_000,
    )
    .expect("a table whose added column has a materialisable default analyzes");
    assert_eq!(report.scanned_rows, 100);
    let added = table
        .columns
        .iter_deref()
        .find(|column| column.read().name.lowercase() == "c")
        .expect("the fixture declares c");
    let added_id = added.read().id;
    let stats = report
        .stats
        .column(added_id)
        .expect("the added column gets a histogram");
    assert_eq!(
        stats.histogram.null_count, 0,
        "every row predating the added column was analyzed as NULL"
    );
    assert_eq!(stats.histogram.ndv, 1);
    let topn = stats.topn.as_ref().expect("the one value is a TopN entry");
    assert_eq!(topn.num(), 1);
    let entry = topn.entries().first().expect("one entry");
    assert_eq!(entry.count, 100);
    assert_eq!(
        entry.encoded,
        tidb_codec::encode_key(&[Datum::Int(5)]).expect("the default encodes"),
        "the TopN entry is not the column's default value 5"
    );
}
