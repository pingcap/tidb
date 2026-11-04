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

//! Go `buildMemTable`: a `memtableRetriever` table's reader carries a memory
//! tracker its retrieval charges (`recordMemoryConsume`, Go's
//! `types.EstimatedMemUsage` of each row), which EXPLAIN ANALYZE reports.
//! The values are asked of Go: ENGINES' one row is six 72-byte datums and
//! 80 bytes of text, and KEYSPACE_META's `setData` charges nothing.

use crate::support::testkit::TestKit;

fn scan_memory(tk: &mut TestKit, table: &str) -> String {
    let rows = tk.must_query(&format!(
        "explain analyze select * from information_schema.{table}"
    ));
    let scan = rows
        .rows()
        .iter()
        .find(|row| row[0].starts_with("MemTableScan"))
        .unwrap_or_else(|| panic!("a memory-table scan: {:?}", rows.rows()))
        .clone();
    scan[scan.len() - 2].clone()
}

#[test]
fn a_retriever_memory_table_reports_the_memory_its_rows_take() {
    let mut tk = TestKit::new();
    assert_eq!(scan_memory(&mut tk, "engines"), "512 Bytes");
    assert_eq!(scan_memory(&mut tk, "keyspace_meta"), "0 Bytes");
}
