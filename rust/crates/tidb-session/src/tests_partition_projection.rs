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

//! What a narrowed schema means to the reader built under it.
//!
//! Go's `DataSource` carries `Columns []*model.ColumnInfo` beside its schema,
//! so a physical reader built after `rule_column_pruning` still knows which
//! STORED column each output slot is. This tier hands the reader the narrowed
//! schema alone, which is only unambiguous while the schema IS the table's
//! leading columns.

use crate::tests_support::row_text;
use crate::Session;

/// `SELECT <one column> FROM t PARTITION (p) USE INDEX (i)` answered with the
/// FIRST table column's value, whatever column was asked for.
///
/// The `PARTITION` clause is what exposed it, and only because of ORDER: it is
/// the shape whose leaf demand prunes the `FROM` scope BEFORE the access path
/// replaces the source, so the index reader was the first one ever built over
/// an already-narrowed schema. Its decode list defaulted to `0..schema.len()`
/// -- one slot, read from table column 0 -- while the scope said that slot was
/// `b`. Source rows: `tests/integrationtest/t/globalindex/mem_index_non_unique.test`.
#[test]
fn a_pruned_index_reader_decodes_the_column_the_scope_names() {
    let mut session = Session::new();
    // A GLOBAL index is how the source test reaches this shape; a LOCAL one
    // over a partitioned table reaches it too, so both are pinned.
    for index in ["KEY idx1 (b) GLOBAL", "KEY idx1 (b)"] {
        session.run("DROP TABLE IF EXISTS t").unwrap();
        session
            .run(&format!(
                "CREATE TABLE t (a int, b int, {index}) PARTITION BY HASH (a) PARTITIONS 5"
            ))
            .unwrap();
        session
            .run("INSERT INTO t VALUES (1, 2), (2, 3), (3, 4), (4, 5), (5, 1)")
            .unwrap();

        // `a = 5` hashes to p0 and `a = 1` to p1, so each clause names one row.
        assert_eq!(
            row_text(session.run("SELECT b FROM t PARTITION (p0) USE INDEX (idx1) WHERE b <= 2")),
            vec![vec!["1"]],
            "{index}: the one output slot is `b`, not the table's first column"
        );
        assert_eq!(
            row_text(session.run(
                "SELECT b FROM t PARTITION (p0, p1) USE INDEX (idx1) WHERE b <= 2 ORDER BY b"
            )),
            vec![vec!["1"], vec!["2"]],
            "{index}: both partitions answer with `b`"
        );
        // The full-width read was always right; it is the pinned control that
        // says the rows themselves were never in doubt.
        assert_eq!(
            row_text(session.run("SELECT * FROM t PARTITION (p0) USE INDEX (idx1) WHERE b <= 2")),
            vec![vec!["5", "1"]],
            "{index}: the unpruned read was already correct"
        );
        // A projection that keeps both columns cannot prune, and a second
        // column in the `WHERE` puts `a` back in the demand -- the two shapes
        // that hid this for as long as they were the only ones tried.
        assert_eq!(
            row_text(
                session.run("SELECT b, a FROM t PARTITION (p0) USE INDEX (idx1) WHERE b <= 2")
            ),
            vec![vec!["1", "5"]]
        );
        assert_eq!(
            row_text(
                session
                    .run("SELECT b FROM t PARTITION (p0) USE INDEX (idx1) WHERE b <= 2 AND a > 0")
            ),
            vec![vec!["1"]]
        );
    }
}
