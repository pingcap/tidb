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

use std::collections::BTreeMap;

use tidb_codec::encode_row_key;
use tidb_datatype::Datum;
use tidb_exec::cluster_analyze::{analyze_table, AnalyzeOptions};
use tidb_exec::cluster_catalog::{ClusterCatalogError, MetaPairs, MetaSnapshot, PagedMetaSnapshot};
use tidb_exec::table_info_build::{build_table_info, ClusteredIndexDefMode};
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

fn generated_table(stored: bool) -> TableInfo {
    let statement = tidb_parser::parse("CREATE TABLE generated_stats (a INT, b INT)")
        .expect("the fixture parses");
    let tidb_ast::Stmt::Ddl(ddl) = statement else {
        panic!("the fixture is CREATE TABLE");
    };
    let tidb_ast::DdlStmt::CreateTable(create) = ddl.as_ref() else {
        panic!("the fixture is CREATE TABLE");
    };
    let mut table = build_table_info(create, "utf8mb4", "utf8mb4_bin", ClusteredIndexDefMode::On)
        .expect("the fixture table builds");
    table.id = 4243;
    let generated = table.columns.get(1).expect("the fixture declares b");
    generated.write().generated_expr_string = "`a` + 1".to_owned();
    generated.write().generated_stored = stored;
    table
}

fn stored_rows(table: &TableInfo) -> RowStore {
    let column_ids = table
        .columns
        .iter_deref()
        .map(|column| column.read().id)
        .collect::<Vec<_>>();
    let mut store = RowStore::default();
    for handle in 1..=10_i64 {
        let value = encode_table_row(
            None,
            &[Datum::Int(handle), Datum::Int(handle + 1)],
            &column_ids,
            true,
            None,
        )
        .expect("the stored generated value encodes");
        let key = encode_row_key(
            table.id,
            &tidb_codec::encode_key(&[Datum::Int(handle)]).expect("the handle encodes"),
        );
        store.pairs.insert(key, value);
    }
    store
}

#[test]
fn stored_generated_column_is_analyzed_like_go() {
    let table = generated_table(true);
    let generated_id = table.columns.get(1).unwrap().read().id;
    let mut store = stored_rows(&table);
    let report = analyze_table(
        &mut store,
        &table,
        &AnalyzeOptions::default(),
        None,
        440_000_000_000_000_002,
        None,
    )
    .expect("stored generated columns carry ordinary row bytes");

    assert!(report.stats.column(generated_id).is_some());
}

#[test]
fn virtual_generated_column_is_skipped_like_go() {
    let table = generated_table(false);
    let generated_id = table.columns.get(1).unwrap().read().id;
    let mut store = stored_rows(&table);
    let report = analyze_table(
        &mut store,
        &table,
        &AnalyzeOptions::default(),
        None,
        440_000_000_000_000_003,
        None,
    )
    .expect("virtual generated columns are not sampled");

    assert!(report.stats.column(generated_id).is_none());
}
