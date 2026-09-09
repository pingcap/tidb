// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, 2.0 (the "License");
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

//! Final-state metadata checks derived from the pinned Go
//! `pkg/ddl/db_change_test.go` tests. These exercise behavior implemented by
//! this crate; online schema-state and parallel-job behavior belongs to the
//! DDL job layer and is not represented by inert executor tests.

use crate::driver::{TableEntry, DEFAULT_DATABASE};
use crate::{run_alter_table_in, run_create_index_in, run_create_table_on, Catalog, StmtContext};

fn ctx() -> StmtContext {
    StmtContext::for_query()
}

fn create(catalog: &mut Catalog, sql: &str) {
    run_create_table_on(sql, catalog).unwrap_or_else(|error| panic!("{sql}: {error}"));
}

fn index_names(catalog: &Catalog, table: &str) -> Vec<(String, bool)> {
    match catalog.get_table_for_test(table) {
        Some(TableEntry::Kv(kv)) => kv
            .indexes()
            .iter()
            .map(|index| (index.name.clone(), index.unique))
            .collect(),
        _ => panic!("{table} is not a KV table"),
    }
}

/// `db_change_test.go:946::TestAlterIndexVisibility` — regression test for
/// issue 70049.
///
/// Go asserts, through `information_schema.tidb_indexes`
/// (key_name, is_visible), that `ALTER TABLE .. ALTER INDEX idx_k INVISIBLE`
/// flips ONLY the exact name, leaving the underscore-suffixed siblings
/// `idx_k_1`/`idx_k_copy` alone — Go's `setIndexVisibility`
/// (`pkg/ddl/index.go:740-747`) matches `idx.Name.L == name.L` (plus a
/// changing/temp-index leg keyed on `GetChangingOriginName`, which is what
/// the suffix names must not be mistaken for). The second fixture flips
/// `idx_k` back VISIBLE among three create-time-invisible indexes.
///
/// Assertion surface: `tidb_indexes` is rendered by `tidb-session`; this tier
/// pins the field that table renders, `KvIndex.visible`
/// (`kv_table/table_meta.rs:541`), exactly as the in-crate test
/// `an_index_declared_invisible_is_maintained_but_never_planned` does.
#[test]
fn alter_index_visibility_matches_the_exact_index_not_its_suffix_siblings() {
    let mut catalog = Catalog::default();

    create(
        &mut catalog,
        "create table t_invisible (k int, key idx_k(k), key idx_k_1(k), key idx_k_copy(k))",
    );
    run_alter_table_in(
        "alter table t_invisible alter index idx_k invisible",
        &mut catalog,
        DEFAULT_DATABASE,
        &ctx(),
    )
    .unwrap();
    let visible: Vec<bool> = match catalog.get_table_for_test("t_invisible") {
        Some(TableEntry::Kv(kv)) => {
            let mut names: Vec<(String, bool)> = kv
                .indexes()
                .iter()
                .map(|index| (index.name.to_ascii_lowercase(), index.visible))
                .collect();
            names.sort_by(|a, b| a.0.cmp(&b.0));
            names.into_iter().map(|(_, visible)| visible).collect()
        }
        _ => panic!("t_invisible is not a KV table"),
    };
    // Go's rows, ordered by key_name: idx_k NO, idx_k_1 YES, idx_k_copy YES.
    assert_eq!(
        visible,
        vec![false, true, true],
        "idx_k_1/idx_k_copy must keep Go's YES"
    );

    create(
        &mut catalog,
        "create table t_visible (k int, key idx_k(k) invisible, key idx_k_1(k) invisible, \
         key idx_k_copy(k) invisible)",
    );
    run_alter_table_in(
        "alter table t_visible alter index idx_k visible",
        &mut catalog,
        DEFAULT_DATABASE,
        &ctx(),
    )
    .unwrap();
    let visible: Vec<bool> = match catalog.get_table_for_test("t_visible") {
        Some(TableEntry::Kv(kv)) => {
            let mut names: Vec<(String, bool)> = kv
                .indexes()
                .iter()
                .map(|index| (index.name.to_ascii_lowercase(), index.visible))
                .collect();
            names.sort_by(|a, b| a.0.cmp(&b.0));
            names.into_iter().map(|(_, visible)| visible).collect()
        }
        _ => panic!("t_visible is not a KV table"),
    };
    // Go's rows: idx_k YES, idx_k_1 NO, idx_k_copy NO.
    assert_eq!(visible, vec![true, false, false]);
}

/// `db_change_test.go:867::TestShowIndex`, final-state halves.
///
/// Go's SHOW INDEX / information_schema.tidb_indexes rows across four
/// primary-key storage variants and one partitioned table. The renderer is
/// `tidb-session`'s (and Go's `infoschema_reader.go:1520-1561`), but the
/// metadata it reads is built here, so the storage-shape facts are pinned at
/// their source:
///
/// - nonclustered int PK: PRIMARY is a REAL index entry (Go's tidb_indexes
///   row `PRIMARY ... Clustered NO` comes from `tb.Indices`).
/// - clustered int PK (`PKIsHandle`): NO PRIMARY entry — Go's row is
///   SYNTHESIZED by the reader (`infoschema_reader.go:1527-1553`), not stored
///   (this mirrors Go's `TableInfo`, whose `Indices` holds no PRIMARY for a
///   PKIsHandle table).
/// - clustered char(100) PK: the table is a COMMON handle and stores a real
///   PRIMARY index entry, rendered with `Clustered YES` by Go.
/// - nonclustered char(100) PK: a real PRIMARY entry again, rowid handle.
/// - the range-partitioned `tr`: `create index idx1` adds exactly one entry.
///
#[test]
fn show_index_entries_follow_the_primary_key_storage() {
    let mut catalog = Catalog::default();

    // :872-923 — t (c1 int primary key nonclustered, c2 int); after
    // `alter table t add index c2(c2)` both entries exist, PRIMARY first.
    create(
        &mut catalog,
        "create table t (c1 int primary key nonclustered, c2 int)",
    );
    run_create_index_in(
        "create index c2 on t (c2)",
        &mut catalog,
        DEFAULT_DATABASE,
        &ctx(),
    )
    .unwrap();
    assert_eq!(
        index_names(&catalog, "t"),
        vec![("PRIMARY".to_owned(), true), ("c2".to_owned(), false)],
        "Go's final SHOW INDEX: PRIMARY row then c2 row"
    );

    // :908-922 — the range-partitioned tr with `create index idx1 on tr
    // (purchased)`: exactly one index entry, no primary.
    create(
        &mut catalog,
        "create table tr (id int, name varchar(50), purchased date) \
         partition by range (year(purchased)) (partition p0 values less than (1990), \
         partition p1 values less than (1995), partition p2 values less than (2000), \
         partition p3 values less than (2005), partition p4 values less than (2010), \
         partition p5 values less than (2015))",
    );
    run_create_index_in(
        "create index idx1 on tr (purchased)",
        &mut catalog,
        DEFAULT_DATABASE,
        &ctx(),
    )
    .unwrap();
    assert_eq!(
        index_names(&catalog, "tr"),
        vec![("idx1".to_owned(), false)],
        "Go's SHOW INDEX from tr: one idx1 row"
    );

    // :924-927 — clustered int PK: handle IS the key, no PRIMARY entry.
    create(
        &mut catalog,
        "create table tr1 (id int primary key clustered, v int, key vv(v))",
    );
    let kv = catalog.get_table_for_test("tr1").unwrap();
    if let TableEntry::Kv(kv) = kv {
        assert!(kv.pk_handle_offset().is_some());
        assert_eq!(
            index_names(&catalog, "tr1"),
            vec![("vv".to_owned(), false)],
            "Go's reader SYNTHESIZES this table's PRIMARY row (Clustered YES); nothing is stored"
        );
    }

    // :929-932 — nonclustered int PK: PRIMARY is a real entry, Clustered NO.
    // Go's own expected ROW order puts vv first (the table constraint becomes
    // IndexInfo[0], the inline PK is appended after it), which is exactly this
    // tier's `indexes()` order.
    create(
        &mut catalog,
        "create table tr2 (id int primary key nonclustered, v int, key vv(v))",
    );
    let kv = catalog.get_table_for_test("tr2").unwrap();
    if let TableEntry::Kv(kv) = kv {
        assert!(kv.pk_handle_offset().is_none());
        assert_eq!(
            index_names(&catalog, "tr2"),
            vec![("vv".to_owned(), false), ("PRIMARY".to_owned(), true)]
        );
    }

    // :934-937 — clustered char(100) PK: a COMMON handle table. Go stores the
    // primary as a `Primary: true` IndexInfo and renders Clustered YES.
    create(
        &mut catalog,
        "create table tr3 (id char(100) primary key clustered, v int, key vv(v))",
    );
    let kv = catalog.get_table_for_test("tr3").unwrap();
    if let TableEntry::Kv(kv) = kv {
        assert!(
            !kv.common_handle_offsets().is_empty(),
            "clustered char PK is a common handle"
        );
        assert_eq!(
            index_names(&catalog, "tr3"),
            vec![("vv".to_owned(), false), ("PRIMARY".to_owned(), true)]
        );
    }

    // :939-942 — nonclustered char(100) PK: PRIMARY entry, Clustered NO; row
    // order vv-then-PERIMARY as Go pins it for this shape too.
    create(
        &mut catalog,
        "create table tr4 (id char(100) primary key nonclustered, v int, key vv(v))",
    );
    let kv = catalog.get_table_for_test("tr4").unwrap();
    if let TableEntry::Kv(kv) = kv {
        assert!(kv.pk_handle_offset().is_none());
        assert!(
            kv.common_handle_offsets().is_empty(),
            "a nonclustered char PK is NOT the handle"
        );
        assert_eq!(
            index_names(&catalog, "tr4"),
            vec![("vv".to_owned(), false), ("PRIMARY".to_owned(), true)]
        );
    }
}
