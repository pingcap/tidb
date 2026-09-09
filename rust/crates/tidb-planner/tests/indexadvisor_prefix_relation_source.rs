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

//! Port ledger for `pkg/planner/indexadvisor/optimizer_test.go:68
//! TestOptimizerPrefixContainIndex` (`pkg/planner.part21` item 1226 on
//! `origin/master`).
//!
//! NARROWED REAL PORT. Go drives the check through the catalog:
//! `optimizerImpl.PrefixContainIndex` looks the table up by name
//! (`opt.is().TableByName`, pkg/planner/indexadvisor/optimizer.go:135-139) and
//! scans its existing index metas for one whose leading columns equal the
//! candidate's columns case-insensitively (:140-157). The crate has no catalog,
//! but the exact relational primitive survives as
//! [`tidb_planner::index_advisor_model::Index::prefix_contains`]
//! (`pkg/planner/indexadvisor/model.go:99` `Index.PrefixContain`: schema/table
//! equality + length >= + ordered column-name equality). Because every index in
//! the Go test is built by `indexadvisor.NewIndex` — which lowercases all
//! components (model.go:72-76) — the two sites agree on these inputs; the port
//! reconstructs each fixture table's existing-index list from the same DDL the
//! Go test executes and evaluates the identical 15-case battery against it.
//! Catalog plumbing itself stays an unported boundary (see
//! indexadvisor_optimizer_catalog_source.rs for the sibling gaps).

use tidb_planner::index_advisor_model::Index;

/// Builds a normalized candidate exactly like `indexadvisor.NewIndex`
/// (model.go:72-76): identifiers are lowercased into Columns.
fn candidate(schema: &str, table: &str, columns: &[&str]) -> Index {
    Index::new(schema, table, "idx", columns)
}

/// GO PORT of `pkg/planner/indexadvisor/optimizer_test.go:68
/// TestOptimizerPrefixContainIndex`.
///
/// Fixture DDL (:73-75) gives t1 `key(a), key(b, c)` and t2
/// `key(a, b, c, d), key(d, c, b, a)`. The check closure (:79-83) asks whether
/// each candidate column sequence is a prefix of ANY existing index on that
/// table; every expected boolean below is copied from :84-99. Index names in
/// the fixtures carry no meaning for containment — only schema/table/columns.
#[test]
fn optimizer_prefix_contain_index_accepts_exactly_prefix_column_sequences() {
    // Existing indexes reconstructed from the fixture DDL key definitions;
    // index names never influence containment (only schema/table/columns).
    let t1_existing = [
        Index::new("test", "t1", "ka", &["a"]),
        Index::new("test", "t1", "kbc", &["b", "c"]),
    ];
    let t2_existing = [
        Index::new("test", "t2", "kabcd", &["a", "b", "c", "d"]),
        Index::new("test", "t2", "kdcba", &["d", "c", "b", "a"]),
    ];

    let contained = |table_indexes: &[Index], cand: &Index| -> bool {
        table_indexes
            .iter()
            .any(|existing| existing.prefix_contains(cand))
    };

    // t1 battery (optimizer_test.go:84-89).
    assert!(contained(&t1_existing, &candidate("test", "t1", &["a"])));
    assert!(contained(&t1_existing, &candidate("test", "t1", &["b"])));
    assert!(contained(
        &t1_existing,
        &candidate("test", "t1", &["b", "c"])
    ));
    assert!(!contained(&t1_existing, &candidate("test", "t1", &["c"])));
    assert!(!contained(
        &t1_existing,
        &candidate("test", "t1", &["a", "b"])
    ));
    assert!(!contained(
        &t1_existing,
        &candidate("test", "t1", &["b", "c", "a"])
    ));

    // t2 battery (optimizer_test.go:90-99).
    assert!(contained(&t2_existing, &candidate("test", "t2", &["a"])));
    assert!(contained(
        &t2_existing,
        &candidate("test", "t2", &["a", "b"])
    ));
    assert!(contained(
        &t2_existing,
        &candidate("test", "t2", &["a", "b", "c"])
    ));
    assert!(contained(
        &t2_existing,
        &candidate("test", "t2", &["a", "b", "c", "d"])
    ));
    assert!(contained(&t2_existing, &candidate("test", "t2", &["d"])));
    assert!(contained(
        &t2_existing,
        &candidate("test", "t2", &["d", "c"])
    ));
    assert!(!contained(&t2_existing, &candidate("test", "t2", &["b"])));
    assert!(!contained(
        &t2_existing,
        &candidate("test", "t2", &["b", "a"])
    ));
    assert!(!contained(
        &t2_existing,
        &candidate("test", "t2", &["b", "a", "c"])
    ));

    // Cross-table identity must not leak: t1's key(a) does not make a t2
    // candidate contained (schema/table guard in model.go:101).
    assert!(!contained(&t1_existing, &candidate("test", "t2", &["a"])));
}
