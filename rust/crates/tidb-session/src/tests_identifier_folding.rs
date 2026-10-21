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

//! Identifier comparisons outside column resolution fold like Go's
//! `ast.CIStr.L` (`strings.ToLower`, Unicode simple mappings), or like
//! `strings.EqualFold` where Go uses that: hint tables, index names,
//! partition names and window names. Every expectation below is what a Go
//! TiDB answered for the same statements (database `ss_tt` there, `test`
//! here).

#![cfg(test)]

use crate::tests_support::*;
use crate::*;

fn folding_session() -> Session {
    let mut session = Session::new();
    session
        .run("CREATE TABLE tσ (a INT, b INT, KEY idx_b(b), KEY ÉX(a))")
        .unwrap();
    session
        .run(
            "CREATE TABLE pσ (a INT, KEY ia(a)) PARTITION BY RANGE (a) (PARTITION PÉ VALUES \
             LESS THAN (10), PARTITION ſ VALUES LESS THAN (20), PARTITION p2 VALUES LESS THAN \
             MAXVALUE)",
        )
        .unwrap();
    session
}

/// The access object column of every `EXPLAIN` row.
fn access_objects(session: &mut Session, sql: &str) -> Vec<String> {
    row_text(session.run(sql))
        .into_iter()
        .map(|row| row[3].clone())
        .collect()
}

fn error_of(session: &mut Session, sql: &str) -> (u16, String) {
    let error = session.run(sql).unwrap_err().to_mysql_error();
    (error.code, error.message)
}

/// Go `HintedIndex.Match` / `getPathByIndexName`: `TΣ` names `tσ` and `éx`
/// names `ÉX`, in a comment hint and in a table-level index hint alike.
#[test]
fn hint_tables_and_index_names_fold_like_cistr_l() {
    let mut session = folding_session();

    let objects = access_objects(
        &mut session,
        "EXPLAIN FORMAT='brief' SELECT /*+ use_index(TΣ, idx_b) */ * FROM tσ WHERE b = 2",
    );
    assert!(objects
        .iter()
        .any(|object| object == "table:tσ, index:idx_b(b)"));
    assert_eq!(warnings_of(&session), vec![]);

    for sql in [
        "EXPLAIN FORMAT='brief' SELECT * FROM tσ USE INDEX(éx) WHERE a = 1",
        "EXPLAIN FORMAT='brief' SELECT /*+ use_index(tσ, éx) */ * FROM tσ WHERE a = 1",
    ] {
        let objects = access_objects(&mut session, sql);
        assert!(
            objects
                .iter()
                .any(|object| object == "table:tσ, index:ÉX(a)"),
            "{sql}"
        );
        assert_eq!(warnings_of(&session), vec![], "{sql}");
    }

    // The missing name keeps its written spelling in 1176; the applicable
    // `ÉX` beside it still drives the plan.
    let objects = access_objects(
        &mut session,
        "EXPLAIN FORMAT='brief' SELECT /*+ use_index(tσ, nopé, ÉX) */ * FROM tσ WHERE a = 1",
    );
    assert!(objects
        .iter()
        .any(|object| object == "table:tσ, index:ÉX(a)"));
    assert_eq!(
        warnings_of(&session),
        vec![(1176, "Key 'nopé' doesn't exist in table 'tσ'".to_owned())]
    );

    // USE_INDEX_MERGE's own validation (`idxName.L == path.Index.Name.L`).
    let objects = access_objects(
        &mut session,
        "EXPLAIN FORMAT='brief' SELECT /*+ use_index_merge(tσ, éx, IDX_B) */ * FROM tσ \
         WHERE a = 1 OR b = 2",
    );
    assert!(objects
        .iter()
        .any(|object| object == "table:tσ, index:ÉX(a)"));
    assert!(objects
        .iter()
        .any(|object| object == "table:tσ, index:idx_b(b)"));
    assert_eq!(warnings_of(&session), vec![]);
}

/// Go `Restore2JoinHint` and the storage-conflict warning print the hint
/// table's `TblName.L`, folding `TΣ` to `tσ` and `PÉ` to `pé`.
#[test]
fn hint_warnings_print_the_folded_names() {
    let mut session = folding_session();

    session
        .run(
            "EXPLAIN FORMAT='brief' SELECT /*+ hash_join(TΣ PARTITION(PÉ)) */ * FROM tσ, tσ x \
             WHERE tσ.a = x.a",
        )
        .unwrap();
    assert_eq!(
        warnings_of(&session),
        vec![(
            1105,
            "Optimizer Hint /*+ HASH_JOIN(tσ PARTITION(pé)) */ is inapplicable on specified \
             partitions"
                .to_owned()
        )]
    );

    session
        .run("EXPLAIN FORMAT='brief' SELECT /*+ read_from_storage(tikv[TΣ], tiflash[tΣ]) */ * FROM tσ")
        .unwrap();
    assert_eq!(
        warnings_of(&session),
        vec![(
            1815,
            "Storage hints are conflict, you can only specify one storage type of table test.tσ"
                .to_owned()
        )]
    );
}

/// Go `tables.FindPartitionByName`: `strings.EqualFold` over both lowered
/// names, so `pé` names `PÉ` and `S` names `ſ`; the error carries the
/// lowered written name (`İ` lowers to `i`). The pruner then keeps a
/// partition only when its `Name.L` equals a written `.L`, which `s` and
/// `ſ` do not, so `PARTITION (S)` reads nothing.
#[test]
fn partition_names_fold_like_find_partition_by_name() {
    let mut session = folding_session();

    assert_eq!(
        access_objects(
            &mut session,
            "EXPLAIN FORMAT='brief' SELECT * FROM pσ PARTITION (pé) WHERE a = 1"
        )[0],
        "partition:PÉ"
    );
    assert_eq!(
        access_objects(
            &mut session,
            "EXPLAIN FORMAT='brief' SELECT * FROM pσ PARTITION (S)"
        )[0],
        "partition:dual"
    );
    assert_eq!(
        error_of(&mut session, "SELECT * FROM pσ PARTITION (NOPÉ)"),
        (1735, "Unknown partition 'nopé' in table 'pσ'".to_owned())
    );
    assert_eq!(
        error_of(&mut session, "SELECT * FROM pσ PARTITION (İ)"),
        (1735, "Unknown partition 'i' in table 'pσ'".to_owned())
    );
}

/// Go `checkTableHintsApplicableForPartition` reports each unknown hint
/// partition by its `.L`, and `Restore2IndexHint` lowers table and
/// partitions. `appendWarnForUnknownPartitions` raises it through
/// `SetHintWarningFromError` of a `fmt.Errorf`: ErrUnknown, 1105.
#[test]
fn unknown_hint_partitions_are_reported_lowered() {
    let mut session = folding_session();
    session
        .run("SET @@tidb_partition_prune_mode = 'static'")
        .unwrap();

    let objects = access_objects(
        &mut session,
        "EXPLAIN FORMAT='brief' SELECT /*+ use_index(pσ partition(PÉ, PZZ, NÉ), ia) */ * \
         FROM pσ WHERE a > 1",
    );
    assert!(objects
        .iter()
        .any(|object| object == "table:pσ, partition:PÉ, index:ia(a)"));
    assert_eq!(
        warnings_of(&session),
        vec![(
            1105,
            "unknown partitions (pzz,né) in optimizer hint /*+ USE_INDEX(pσ PARTITION(pé, pzz, \
             né) ia) */"
                .to_owned()
        )]
    );
}

/// The other partition-hint warnings of Go's static partition processor and
/// hint parser, captured from a Go TiDB: `ParsePlanHints`' join-hint
/// refusal (`SetHintWarningFromError(fmt.Errorf(...))`) and
/// `checkHintsApplicable`'s read-from-storage check are both 1105.
#[test]
fn partition_hint_warnings_are_plain_errors() {
    let mut session = folding_session();
    session.run("CREATE TABLE t2 (a INT, KEY ia(a))").unwrap();
    session
        .run("SET @@tidb_partition_prune_mode = 'static'")
        .unwrap();

    session
        .run(
            "EXPLAIN FORMAT='brief' SELECT /*+ hash_join(pσ partition(PÉ)) */ * FROM pσ, t2 \
             WHERE pσ.a = t2.a",
        )
        .unwrap();
    assert_eq!(
        warnings_of(&session),
        vec![(
            1105,
            "Optimizer Hint /*+ HASH_JOIN(pσ PARTITION(pé)) */ is inapplicable on specified \
             partitions"
                .to_owned()
        )]
    );

    session
        .run(
            "EXPLAIN FORMAT='brief' SELECT /*+ read_from_storage(tikv[pσ partition(PÉ, PZZ)]) */ \
             * FROM pσ WHERE a > 1",
        )
        .unwrap();
    assert_eq!(
        warnings_of(&session),
        vec![(
            1105,
            "unknown partitions (pzz) in optimizer hint read_from_storage".to_owned()
        )]
    );

    session
        .run(
            "EXPLAIN FORMAT='brief' SELECT /*+ use_index_merge(pσ partition(QQ), ia) */ * FROM \
             pσ WHERE a > 1",
        )
        .unwrap();
    assert_eq!(
        warnings_of(&session),
        vec![(
            1105,
            "unknown partitions (qq) in optimizer hint /*+ USE_INDEX_MERGE(pσ PARTITION(qq) ia) */"
                .to_owned()
        )]
    );
}

/// Go `resolveOptimizeHint` keeps a hint for a partition only when
/// `p.String() == partitionName.String()` -- the names as written -- while
/// unknown partitions are judged by `.L`. Captured from a Go TiDB:
/// `PARTITION(P1)` on partition `p1` and `PARTITION(pé)` on `PÉ` raise no
/// warning and do not apply; `PARTITION(PÉ)` does.
#[test]
fn hint_partitions_apply_by_written_name() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE pz (a INT, b INT, KEY ia(a)) PARTITION BY RANGE (a) (PARTITION PÉ \
             VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    session
        .run("SET @@tidb_partition_prune_mode = 'static'")
        .unwrap();
    let readers = |session: &mut Session, sql: &str| -> Vec<String> {
        row_text(session.run(sql))
            .into_iter()
            .map(|row| {
                row[0]
                    .trim_start_matches(['├', '└', '─', '│', ' '])
                    .to_owned()
            })
            .filter(|id| id.starts_with("IndexLookUp") || id.starts_with("TableReader"))
            .collect()
    };
    for hint in ["pz partition(P1)", "pz partition(pé)"] {
        let sql = format!(
            "EXPLAIN FORMAT='brief' SELECT /*+ use_index({hint}, ia) */ * FROM pz WHERE a > 1"
        );
        assert_eq!(
            readers(&mut session, &sql),
            ["TableReader", "TableReader"],
            "{sql}"
        );
        assert_eq!(warnings_of(&session), vec![], "{sql}");
    }
    assert_eq!(
        readers(
            &mut session,
            "EXPLAIN FORMAT='brief' SELECT /*+ use_index(pz partition(PÉ), ia) */ * FROM pz \
             WHERE a > 1"
        ),
        ["IndexLookUp", "TableReader"]
    );
}

/// Go's hint lexer finds a hint's token by `strings.ToUpper` of its written
/// name, so `MERGE_JOıN` (dotless `ı` uppercases to `I`) parses as
/// MERGE_JOIN; but `ParsePlanHints` switches on `HintName.L`, `merge_joın`,
/// which names no hint. Captured from a Go TiDB: no warning, and the join
/// stays a hash join, with or without table names.
#[test]
fn hint_names_compare_by_go_lowered_written_name() {
    let mut session = folding_session();
    session.run("CREATE TABLE t2 (a INT, KEY ia(a))").unwrap();
    let joins = |session: &mut Session, sql: &str| -> Vec<String> {
        row_text(session.run(sql))
            .into_iter()
            .map(|row| {
                row[0]
                    .trim_start_matches(['├', '└', '─', '│', ' '])
                    .to_owned()
            })
            .filter(|id| id.ends_with("Join"))
            .collect()
    };
    assert_eq!(
        joins(
            &mut session,
            "EXPLAIN FORMAT='brief' SELECT /*+ merge_join(tσ) */ * FROM tσ, t2 WHERE tσ.a = t2.a"
        ),
        ["MergeJoin"]
    );
    for hint in ["MERGE_JOıN(tσ)", "MERGE_JOıN()"] {
        let sql =
            format!("EXPLAIN FORMAT='brief' SELECT /*+ {hint} */ * FROM tσ, t2 WHERE tσ.a = t2.a");
        assert_eq!(joins(&mut session, &sql), ["HashJoin"], "{sql}");
        assert_eq!(warnings_of(&session), vec![], "{sql}");
    }
}

/// Go `CheckIsDropPrimaryKey`: `indexName.L == "primary" && (indexInfo ==
/// nil || indexInfo.Primary)`. A secondary index named `PRİMARY` (Go's
/// `checkIndexInfo` refuses only `strings.EqualFold` with PRIMARY, and `İ`
/// folds to nothing else) has `.L` `primary`, yet dropping it is an
/// ordinary DROP INDEX job. Captured from a Go TiDB.
#[test]
fn dropping_a_secondary_index_named_primary_is_drop_index() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE lp (a INT, b INT, KEY `PRİMARY`(a))")
        .unwrap();
    assert_eq!(
        row_text(session.run(
            "SELECT CONSTRAINT_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_NAME = \
             'lp'"
        )),
        Vec::<Vec<String>>::new()
    );
    session.run("ALTER TABLE lp DROP INDEX `primary`").unwrap();
    let jobs = row_text(session.run("ADMIN SHOW DDL JOBS 1"));
    assert_eq!(jobs[0][3], "drop index");
    assert_eq!(
        show_create(&mut session, "lp"),
        "CREATE TABLE `lp` (\n  `a` int DEFAULT NULL,\n  `b` int DEFAULT NULL\n) ENGINE=InnoDB \
         DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    );
}

/// Go keys `b.windowSpecs` by `Name.L`: `wΣ` references the `Wσ` window.
#[test]
fn window_names_fold_like_cistr_l() {
    let mut session = folding_session();
    session.run("INSERT INTO tσ VALUES (1, 1), (2, 2)").unwrap();
    assert_eq!(
        row_text(session.run("SELECT SUM(a) OVER wΣ FROM tσ WINDOW Wσ AS ()")),
        vec![vec!["3".to_owned()], vec!["3".to_owned()]]
    );
}

/// ALTER TABLE's own name checks. Go's `table.FindCol` is
/// `strings.EqualFold(col.Name.O, name)`: RENAME COLUMN `Cσ` finds `cΣ`,
/// and ADD COLUMN `s` collides with an existing `ſ` (while CREATE TABLE,
/// which keys its duplicate check by `.L`, admits `ſ` beside `s`). DROP
/// COLUMN `S` checks with `FindCol` but drops the `.L` match, `s`. RENAME
/// INDEX and ALTER COLUMN ... SET DEFAULT find `ÉX` from `éx`.
#[test]
fn alter_table_names_fold_like_go() {
    let mut session = Session::new();
    session.run("CREATE TABLE f (cΣ INT, ſ INT)").unwrap();
    session.run("ALTER TABLE f RENAME COLUMN Cσ TO d").unwrap();
    assert_eq!(
        show_create(&mut session, "f"),
        "CREATE TABLE `f` (\n  `d` int DEFAULT NULL,\n  `ſ` int DEFAULT NULL\n) \
         ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    );
    assert_eq!(
        error_of(&mut session, "ALTER TABLE f ADD COLUMN s INT"),
        (1060, "Duplicate column name 's'".to_owned())
    );
    assert_eq!(
        error_of(&mut session, "ALTER TABLE f ADD COLUMN D INT"),
        (1060, "Duplicate column name 'D'".to_owned())
    );

    session.run("CREATE TABLE f2 (ſ INT, s INT)").unwrap();
    session.run("ALTER TABLE f2 DROP COLUMN S").unwrap();
    assert_eq!(
        show_create(&mut session, "f2"),
        "CREATE TABLE `f2` (\n  `ſ` int DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 \
         COLLATE=utf8mb4_bin"
    );

    session
        .run("CREATE TABLE f4 (a INT, b INT, KEY ÉX(a))")
        .unwrap();
    session
        .run("ALTER TABLE f4 RENAME INDEX éx TO ix2")
        .unwrap();
    assert_eq!(
        show_create(&mut session, "f4"),
        "CREATE TABLE `f4` (\n  `a` int DEFAULT NULL,\n  `b` int DEFAULT NULL,\n  KEY `ix2` \
         (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    );

    session
        .run("CREATE TABLE f5 (a INT, ÉX INT, KEY (ÉX))")
        .unwrap();
    session
        .run("ALTER TABLE f5 ALTER COLUMN éx SET DEFAULT 3")
        .unwrap();
    assert_eq!(
        show_create(&mut session, "f5"),
        "CREATE TABLE `f5` (\n  `a` int DEFAULT NULL,\n  `ÉX` int DEFAULT '3',\n  KEY `ÉX` \
         (`ÉX`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    );
}

/// Go's hint lexer finds every keyword argument by `strings.ToUpper`
/// (`hintTokenMap`), keeps a storage type as written and lets
/// `ParsePlanHints` switch on its `.L`; an argument of the wrong kind is
/// dropped silently (`skipToCloseParen`). Captured from a Go TiDB:
/// `TIFLA\u{17F}H[t2]` and `T\u{131}KV[t2]` name no store (no warning, no
/// effect), `FAL\u{17F}E` is FALSE, and `1 \u{1D0D}b` (no uppercase mapping)
/// drops MEMORY_QUOTA without a warning.
#[test]
fn hint_keyword_arguments_fold_with_go_to_upper() {
    let mut session = folding_session();
    session.run("CREATE TABLE t2 (a INT, KEY ia(a))").unwrap();
    session
        .run("EXPLAIN FORMAT='brief' SELECT /*+ read_from_storage(TIFLA\u{17F}H[t2]) */ * FROM t2")
        .unwrap();
    assert_eq!(warnings_of(&session), vec![]);
    session
        .run(
            "EXPLAIN FORMAT='brief' SELECT /*+ read_from_storage(T\u{131}KV[t2], tiflash[t2]) */ \
             * FROM t2",
        )
        .unwrap();
    let warnings = warnings_of(&session);
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert_eq!(warnings[0].0, 1815);
    assert!(
        warnings[0]
            .1
            .starts_with("No available path for table test.t2 with the store type tiflash"),
        "{warnings:?}"
    );
    session
        .run("SELECT /*+ use_toja(FAL\u{17F}E) memory_quota(1 \u{1D0D}b) */ 1")
        .unwrap();
    assert_eq!(warnings_of(&session), vec![]);
}

/// Go's point-get fast path (`buildSchemaFromFields`, `getNameValuePairs`)
/// never reads a column's schema qualifier: captured from a Go TiDB,
/// `SELECT nosuch.pk.a ... WHERE id = 1` and `... WHERE id IN (1, 2)` answer,
/// while a range read refuses the qualifier with 1054.
#[test]
fn point_get_fast_path_ignores_schema_qualifiers() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE pk (id INT PRIMARY KEY, a INT, b INT, UNIQUE KEY ub(b))")
        .unwrap();
    session
        .run("INSERT INTO pk VALUES (1, 10, 100), (2, 20, 200)")
        .unwrap();
    for (sql, expected) in [
        ("SELECT nosuch.pk.a FROM pk WHERE id = 1", vec![vec!["10"]]),
        (
            "SELECT nosuch.pk.* FROM pk WHERE id = 1",
            vec![vec!["1", "10", "100"]],
        ),
        ("SELECT a FROM pk WHERE nosuch.pk.id = 1", vec![vec!["10"]]),
        ("SELECT a FROM pk WHERE nosuch.pk.b = 200", vec![vec!["20"]]),
        (
            "SELECT nosuch.pk.a FROM pk WHERE id IN (1, 2)",
            vec![vec!["10"], vec!["20"]],
        ),
    ] {
        let rows = row_text(session.run(sql));
        let expected = expected
            .into_iter()
            .map(|row| row.into_iter().map(str::to_owned).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        assert_eq!(rows, expected, "{sql}");
    }
    assert_eq!(
        error_of(&mut session, "SELECT nosuch.pk.a FROM pk WHERE id > 0"),
        (
            1054,
            "Unknown column 'nosuch.pk.a' in 'field list'".to_owned()
        )
    );
}
