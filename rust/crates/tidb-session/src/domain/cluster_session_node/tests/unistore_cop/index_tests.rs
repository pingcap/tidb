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

use super::*;

/// Go's partial indexes on the production node. The builders dropped `WHERE`
/// (CREATE TABLE) or refused it (CREATE INDEX); now Go's
/// `CheckAndBuildIndexConditionString` / `buildAffectColumn` store the
/// condition, the loader and the backfill compile it, and Go's
/// `checkIndexCondition`, partitioned-table, primary-key and 8272 refusals
/// apply. Answers and messages are the Go oracle's for the same script.
#[test]
fn a_partial_index_holds_only_the_rows_its_condition_admits() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(142)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.t (a INT, b INT, c VARCHAR(10), KEY ia (b) WHERE a > 2)",
    );
    rows(
        &mut session,
        "INSERT INTO test.t VALUES (1, 10, 'x'), (3, 30, 'y'), (5, 50, 'z')",
    );
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.t"));
    assert!(
        created[0][1].contains("  KEY `ia` (`b`) WHERE `a` > 2\n"),
        "{}",
        created[0][1]
    );
    let through_ia = "SELECT b FROM test.t USE INDEX (ia) WHERE a > 2 AND b > 0 ORDER BY b";
    assert_eq!(displayed(rows(&mut session, through_ia)), [["30"], ["50"]]);
    rows(&mut session, "UPDATE test.t SET a = 10 WHERE b = 10");
    assert_eq!(
        displayed(rows(&mut session, through_ia)),
        [["10"], ["30"], ["50"]]
    );
    // Adding a partial index to an existing table needs Go's ingest
    // reorganization; without Lightning the owner picks txn-merge and
    // cancels the job (Go `checkAndBuildIndexInfo`'s refusal), as the Go
    // oracle answers on the embedded store.
    let error = session
        .node_conn()
        .execute_write("CREATE INDEX ib ON test.t (b) WHERE a < 5")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (
            8200,
            "Unsupported add partial index: add partial index without fast reorg is not supported"
        )
    );
    rows(&mut session, "ADMIN CHECK TABLE test.t");
    let referenced =
        "Cannot drop, change or modify column 'a': it is referenced in partial index 'ia'";
    for (sql, code, message) in [
        (
            "ALTER TABLE test.t MODIFY COLUMN a BIGINT",
            8272,
            referenced,
        ),
        ("ALTER TABLE test.t DROP COLUMN a", 8272, referenced),
        (
            "ALTER TABLE test.t CHANGE COLUMN a aa INT",
            8272,
            referenced,
        ),
        (
            "ALTER TABLE test.t ADD INDEX bad1 (b) WHERE a + 1 > 2",
            8200,
            "Unsupported add partial index: partial index condition must include a column name \
             in the binary operation",
        ),
        (
            "ALTER TABLE test.t ADD INDEX bad3 (b) WHERE a = NULL",
            8200,
            "Unsupported add partial index: = NULL is not supported in partial index condition \
             because it is always false",
        ),
        (
            "ALTER TABLE test.t ADD INDEX bad5 (b) WHERE d > 1",
            8200,
            "Unsupported add partial index: column name `d` referenced in partial index \
             condition is not found in table",
        ),
        (
            "ALTER TABLE test.t ADD INDEX bad6 (b) WHERE a IS TRUE",
            8200,
            "Unsupported add partial index: the kind of partial index condition is not supported",
        ),
        (
            "CREATE TABLE test.tp (a INT, b INT, KEY i (b) WHERE a > 1, \
             PRIMARY KEY (a) WHERE a > 2)",
            8200,
            "Unsupported add partial index: create an primary key with partial index is not \
             supported",
        ),
        (
            "CREATE TABLE test.tq (a INT, b INT, KEY i (b) WHERE a > 5) \
             PARTITION BY RANGE (b) (PARTITION p0 VALUES LESS THAN (5))",
            8200,
            "Unsupported add partial index: partial index is not supported on partitioned table",
        ),
    ] {
        let error = session.node_conn().execute_write(sql).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (code, message),
            "{sql}"
        );
    }
    rows(
        &mut session,
        "CREATE TABLE test.tr (a INT PRIMARY KEY, b INT) PARTITION BY HASH (a) PARTITIONS 2",
    );
    let error = session
        .node_conn()
        .execute_write("CREATE INDEX ir ON test.tr (b) WHERE b > 1")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (
            8200,
            "Unsupported add partial index: partial index on partitioned table is not supported"
        )
    );
    session
        .node_conn()
        .execute_write("ALTER TABLE test.t DROP INDEX ia")
        .unwrap();
    session
        .node_conn()
        .execute_write("ALTER TABLE test.t MODIFY COLUMN a BIGINT")
        .unwrap();
}

/// Go's foreign-key guards on the production node, answers from the Go
/// oracle for the same script: `AlterTable` names every anonymous key of one
/// ALTER before any spec runs (`fk_1`, `fk_2`); `checkDropColumnWithForeignKeyConstraint`
/// keeps a key's columns, on either side (1828, 1829); and
/// `checkIndexNeededInForeignKey` / `checkOperateDropIndexUseByForeignKey`
/// keep the index a key needs, however the ALTER is bundled (1553). This node
/// refused the bundled ALTER and accepted every drop.
#[test]
fn foreign_keys_guard_their_columns_and_indexes_as_go_does() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(138)).unwrap();
    rows(&mut session, "SET @@foreign_key_checks = 1");
    rows(
        &mut session,
        "CREATE TABLE test.p (id INT KEY, b INT, INDEX ib (b))",
    );
    rows(&mut session, "CREATE TABLE test.c (a INT, b INT)");
    session
        .node_conn()
        .execute_write(
            "ALTER TABLE test.c ADD FOREIGN KEY (a) REFERENCES test.p (id), \
             ADD FOREIGN KEY (b) REFERENCES test.p (b)",
        )
        .unwrap();
    let keys = "  KEY `fk_1` (`a`),\n  KEY `fk_2` (`b`),\n  \
                CONSTRAINT `fk_1` FOREIGN KEY (`a`) REFERENCES `p` (`id`),\n  \
                CONSTRAINT `fk_2` FOREIGN KEY (`b`) REFERENCES `p` (`b`)\n";
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.c"));
    assert!(created[0][1].contains(keys), "{}", created[0][1]);
    for (sql, code, message) in [
        (
            "ALTER TABLE test.c DROP COLUMN a",
            1828,
            "Cannot drop column 'a': needed in a foreign key constraint 'fk_1'",
        ),
        (
            "ALTER TABLE test.p DROP COLUMN b",
            1829,
            "Cannot drop column 'b': needed in a foreign key constraint 'fk_2' of table 'c'",
        ),
        (
            "ALTER TABLE test.p DROP INDEX ib",
            1553,
            "Cannot drop index 'ib': needed in a foreign key constraint",
        ),
        (
            "ALTER TABLE test.c DROP INDEX fk_1",
            1553,
            "Cannot drop index 'fk_1': needed in a foreign key constraint",
        ),
        (
            "ALTER TABLE test.c ADD INDEX ia (a), DROP INDEX fk_1",
            1553,
            "Cannot drop index 'fk_1': needed in a foreign key constraint",
        ),
        (
            "ALTER TABLE test.c DROP FOREIGN KEY fk_1, DROP INDEX fk_1",
            1553,
            "Cannot drop index 'fk_1': needed in a foreign key constraint",
        ),
    ] {
        let error = session.node_conn().execute_write(sql).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (code, message),
            "{sql}"
        );
    }
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.c"));
    assert!(created[0][1].contains(keys), "{}", created[0][1]);
    session
        .node_conn()
        .execute_write(
            "ALTER TABLE test.c DROP FOREIGN KEY fk_2, ADD FOREIGN KEY (b) REFERENCES test.p (b)",
        )
        .unwrap();
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.c"));
    assert!(
        created[0][1].contains(
            "  KEY `fk_2` (`b`),\n  CONSTRAINT `fk_1` FOREIGN KEY (`a`) REFERENCES `p` (`id`),\n  \
             CONSTRAINT `fk_3` FOREIGN KEY (`b`) REFERENCES `p` (`b`)\n"
        ),
        "{}",
        created[0][1]
    );
}

/// Go `checkIndexNameAndColumns`: an ADD INDEX written without a name takes
/// `GetName4AnonymousIndex`'s name against the stored table, and an expression
/// part indexes a hidden generated column backfilled over the existing rows.
/// This node refused both ("needs an explicit index name", "an expression
/// index is not supported by this node"). The names are the Go oracle's.
#[test]
fn an_added_index_takes_gos_anonymous_name_and_indexes_expressions() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(135)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.ai (a INT, b VARCHAR(10), KEY (a))",
    );
    rows(
        &mut session,
        "INSERT INTO test.ai VALUES (1, 'X'), (2, 'y')",
    );
    for sql in [
        "ALTER TABLE test.ai ADD INDEX (a)",
        "ALTER TABLE test.ai ADD INDEX ((a + 1))",
        "CREATE INDEX ei ON test.ai ((LOWER(b)))",
        "ALTER TABLE test.ai ADD INDEX (b), ADD INDEX ((a * 2))",
    ] {
        rows(&mut session, sql);
    }
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.ai"));
    assert!(
        created[0][1].contains(
            "  KEY `a` (`a`),\n  KEY `a_2` (`a`),\n  KEY `expression_index` ((`a` + 1)),\n  \
             KEY `ei` ((lower(`b`))),\n  KEY `b` (`b`),\n  KEY `expression_index_2` ((`a` * 2))\n"
        ),
        "{}",
        created[0][1]
    );
    for (sql, expected) in [
        (
            "SELECT a FROM test.ai USE INDEX(expression_index) WHERE a + 1 = 2",
            "1",
        ),
        (
            "SELECT a FROM test.ai USE INDEX(ei) WHERE LOWER(b) = 'x'",
            "1",
        ),
        (
            "SELECT a FROM test.ai USE INDEX(expression_index_2) WHERE a * 2 = 4",
            "2",
        ),
    ] {
        assert_eq!(displayed(rows(&mut session, sql)), [[expected]], "{sql}");
    }
    rows(&mut session, "ADMIN CHECK TABLE test.ai");
}

/// An INDEX read and a TABLE scan of the same rows must agree, and both must
/// agree with what was stored.
///
/// The index path decodes from the index KEY rather than the row value, so it
/// is a second decode of the same data under the same signedness rules. The
/// table-scan path got those rules wrong once already (it rebuilt column
/// types from the DAG without the UNSIGNED flag); this pins the pair so a
/// change to either side cannot drift from the other.
#[test]
fn an_index_read_and_a_table_scan_agree_over_unsigned_keys() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(41))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.ix (id int primary key, u bigint unsigned, KEY ku(u))",
    );
    rows(
        &mut session,
        "INSERT INTO test.ix VALUES (1, 18446744073709551615), (2, 1), \
         (3, 9223372036854775808), (4, NULL), (5, 0)",
    );

    // Each predicate straddles the signed/unsigned boundary, where a signed
    // reading would answer differently.
    for predicate in [
        "u > 2",
        "u >= 9223372036854775808",
        "u = 18446744073709551615",
        "u < 9223372036854775808",
        "u IS NULL",
    ] {
        let indexed = displayed(rows(
            &mut session,
            &format!("SELECT count(*) FROM test.ix WHERE {predicate}"),
        ));
        let scanned = displayed(rows(
            &mut session,
            &format!("SELECT count(*) FROM test.ix IGNORE INDEX (ku) WHERE {predicate}"),
        ));
        assert_eq!(indexed, scanned, "`{predicate}`: index and scan disagree");
    }

    // The index also ORDERS in the unsigned domain, which is the reading a
    // signed key encoding would reverse at the top of the range.
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT u FROM test.ix WHERE u IS NOT NULL ORDER BY u"
        )),
        [
            ["0"],
            ["1"],
            ["9223372036854775808"],
            ["18446744073709551615"],
        ]
    );
}

/// Pinned `globalstats.TestGlobalIndexStatistics`: all three ANALYZE forms
/// populate a partitioned table's global unique-index statistics and the
/// optimizer uses the global index across every partition.
#[test]
fn global_index_statistics_match_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(110))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");

    let create = |session: &mut _, clustered: bool| {
        rows(session, "DROP TABLE IF EXISTS global_index_stats");
        let key = if clustered {
            "PRIMARY KEY(b,a) CLUSTERED"
        } else {
            "KEY(a)"
        };
        rows(
            session,
            &format!(
                "CREATE TABLE global_index_stats (a INT, b INT, c INT DEFAULT 0, {key}) \
                 PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (10), \
                 PARTITION p1 VALUES LESS THAN (20), PARTITION p2 VALUES LESS THAN (30), \
                 PARTITION p3 VALUES LESS THAN (40))"
            ),
        );
        rows(
            session,
            "INSERT INTO global_index_stats(a,b) VALUES \
             (1,1),(2,2),(3,3),(15,15),(25,25),(35,35)",
        );
        rows(
            session,
            "ALTER TABLE global_index_stats ADD UNIQUE INDEX idx(b) GLOBAL",
        );
    };
    let expected = [
        [
            "IndexReader",
            "5.00",
            "root",
            "partition:all",
            "index:IndexRangeScan",
        ],
        [
            "└─IndexRangeScan",
            "5.00",
            "cop[tikv]",
            "table:global_index_stats, index:idx(b)",
            "range:[-inf,16), keep order:true",
        ],
    ];

    create(&mut session, false);
    rows(&mut session, "ANALYZE TABLE global_index_stats");
    // A global index's physical-table ID belongs to each entry, including
    // entries selected by an explicit partition list and a descending scan.
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT b FROM global_index_stats PARTITION(p0,p1) USE INDEX(idx) ORDER BY b DESC"
        )),
        [["15"], ["3"], ["2"], ["1"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT b,c FROM global_index_stats USE INDEX(idx) WHERE b < 16 ORDER BY b"
        )),
        [["1", "0"], ["2", "0"], ["3", "0"], ["15", "0"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT b FROM global_index_stats USE INDEX(idx) WHERE b < 16 ORDER BY b",
        )),
        [["1"], ["2"], ["3"], ["15"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "EXPLAIN FORMAT = 'brief' SELECT b FROM global_index_stats \
             USE INDEX(idx) WHERE b < 16 ORDER BY b",
        )),
        expected
    );

    create(&mut session, true);
    rows(&mut session, "ANALYZE TABLE global_index_stats INDEX idx");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT b FROM global_index_stats PARTITION(p0,p1) USE INDEX(idx) ORDER BY b DESC"
        )),
        [["15"], ["3"], ["2"], ["1"]]
    );
    rows(&mut session, "BEGIN");
    rows(
        &mut session,
        "INSERT INTO global_index_stats(a,b) VALUES (4,4),(16,16)",
    );
    let mut unordered = displayed(rows(
        &mut session,
        "SELECT b,c FROM global_index_stats USE INDEX(idx) WHERE b < 17",
    ));
    unordered.sort_unstable();
    assert_eq!(unordered,
        [["1", "0"], ["15", "0"], ["16", "0"], ["2", "0"], ["3", "0"], ["4", "0"]]);
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT b,c FROM global_index_stats USE INDEX(idx) WHERE b < 17 ORDER BY b DESC"
        )),
        [
            ["16", "0"],
            ["15", "0"],
            ["4", "0"],
            ["3", "0"],
            ["2", "0"],
            ["1", "0"]
        ]
    );
    rows(&mut session, "ROLLBACK");
    let explain = displayed(rows(
        &mut session,
        "EXPLAIN FORMAT = 'brief' SELECT b FROM global_index_stats \
         USE INDEX(idx) WHERE b < 16 ORDER BY b",
    ));
    assert_eq!(explain[0][1], "5.00");

    create(&mut session, true);
    rows(&mut session, "ANALYZE TABLE global_index_stats INDEX");
    assert_eq!(
        displayed(rows(
            &mut session,
            "EXPLAIN FORMAT = 'brief' SELECT b FROM global_index_stats \
             USE INDEX(idx) WHERE b < 16 ORDER BY b",
        )),
        expected
    );
}

/// A write takes the same access paths a `SELECT` does, which is
/// `crate::explain`'s divergence 8 as it now stands.
///
/// That paragraph claimed the opposite for a while -- "none is offered to a
/// write" -- after `write_index_range_path` landed and nothing checked the
/// prose against the code. A doc that describes a gap which no longer exists
/// sends the next reader to build what is already there, so the claim is
/// pinned here rather than trusted.
#[test]
fn a_write_reaches_the_index_path_like_a_select() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(59))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.wi (id int primary key, a int, b int, KEY ka(a), UNIQUE KEY ub(b))",
    );
    rows(
        &mut session,
        "INSERT INTO test.wi VALUES (1,10,100),(2,10,200),(3,20,300)",
    );

    let plan_of = |session: &mut _, sql: &str| {
        displayed(rows(session, sql))
            .into_iter()
            .map(|row| row.join(" "))
            .collect::<Vec<_>>()
            .join("\n")
    };

    // A non-unique secondary index is chosen for a write, as it is for a read.
    for sql in [
        "EXPLAIN UPDATE test.wi SET b = b + 1 WHERE a = 10",
        "EXPLAIN DELETE FROM test.wi WHERE a = 10",
    ] {
        let plan = plan_of(&mut session, sql);
        assert!(
            plan.contains("IndexRangeScan") && plan.contains("index:ka(a)"),
            "`{sql}` did not reach the index path:\n{plan}"
        );
        // Go's exact integer equality is fully enforced by the index range.
        assert!(plan.contains("range:[10,10]"), "{plan}");
        assert!(!plan.contains("Selection"), "{plan}");
    }

    // A predicate on a non-access column must still filter the looked-up rows.
    let residual = plan_of(
        &mut session,
        "EXPLAIN UPDATE test.wi SET b = b + 1 WHERE a = 10 AND b > 100",
    );
    assert!(
        residual.contains("index:ka(a)")
            && residual.contains("Selection")
            && residual.contains("gt(test.wi.b, 100)"),
        "{residual}"
    );

    // A WHERE that pins a whole UNIQUE index still takes the point plan.
    let plan = plan_of(
        &mut session,
        "EXPLAIN UPDATE test.wi SET a = 1 WHERE b = 100",
    );
    assert!(plan.contains("Point_Get"), "{plan}");

    // And the rows a write touches are the rows the predicate names,
    // whichever path carried it there.
    rows(&mut session, "UPDATE test.wi SET b = b + 1 WHERE a = 10");
    assert_eq!(
        displayed(rows(&mut session, "SELECT id, b FROM test.wi ORDER BY id")),
        [["1", "101"], ["2", "201"], ["3", "300"]]
    );
    rows(&mut session, "DELETE FROM test.wi WHERE a = 10");
    assert_eq!(
        displayed(rows(&mut session, "SELECT id FROM test.wi ORDER BY id")),
        [["3"]]
    );
}

/// Go reorg.go derives expression error levels and location from ReorgMeta.
/// Exercise every SQL producer through the same real storage/backfiller.
#[test]
fn index_lifecycle_uses_reorg_sql_mode_and_preserves_failed_metadata() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(812)).unwrap();
    for (i, ddl) in [
        "CREATE INDEX ig ON test.reorg0(g)",
        "ALTER TABLE test.reorg1 ADD INDEX ig(g)",
        "ALTER TABLE test.reorg2 ADD INDEX ig(g), ADD COLUMN c INT",
    ]
    .into_iter()
    .enumerate()
    {
        rows(&mut session, "SET sql_mode = ''");
        rows(
            &mut session,
            &format!("CREATE TABLE test.reorg{i}(a INT,g INT AS (10 DIV a) VIRTUAL)"),
        );
        rows(
            &mut session,
            &format!("INSERT INTO test.reorg{i}(a) VALUES(0)"),
        );
        rows(
            &mut session,
            "SET sql_mode = 'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO'",
        );
        let error = session
            .node_conn()
            .execute_write(ddl)
            .expect_err("strict reorg rejects zero divisor");
        assert_eq!(error.code, 1365, "{ddl}: {error:?}");
        assert!(rows(&mut session, &format!("SHOW INDEX FROM test.reorg{i}")).is_empty());
        assert_eq!(
            rows(&mut session, &format!("SHOW COLUMNS FROM test.reorg{i}")).len(),
            2
        );
        rows(&mut session, "SET sql_mode = ''");
        rows(&mut session, ddl);
        assert!(!rows(&mut session, &format!("SHOW INDEX FROM test.reorg{i}")).is_empty());
        rows(
            &mut session,
            "SET sql_mode = 'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO'",
        );
        rows(&mut session, &format!("DROP INDEX ig ON test.reorg{i}"));
    }
}

#[test]
fn index_lifecycle_duplicate_backfill_preserves_sql_error_and_rolls_back() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(814)).unwrap();
    rows(&mut session, "CREATE TABLE test.reorg_dup(a INT)");
    rows(&mut session, "INSERT INTO test.reorg_dup VALUES(1),(1)");
    let error = session
        .node_conn()
        .execute_write("CREATE UNIQUE INDEX ia ON test.reorg_dup(a)")
        .unwrap_err();
    assert_eq!(error.code, 1062);
    assert_eq!(error.state, *b"23000");
    assert_eq!(error.message, "Duplicate entry '1' for key 'reorg_dup.ia'");
    assert!(rows(&mut session, "SHOW INDEX FROM test.reorg_dup").is_empty());
    assert_eq!(
        displayed(rows(&mut session, "SELECT count(*) FROM test.reorg_dup")),
        [["2"]]
    );
}

#[test]
fn index_lifecycle_backfill_uses_the_submitting_time_zone() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(813)).unwrap();
    rows(&mut session, "SET time_zone = '+08:00'");
    rows(
        &mut session,
        "CREATE TABLE test.reorg_tz(ts TIMESTAMP,g INT AS (HOUR(ts)) VIRTUAL)",
    );
    rows(
        &mut session,
        "INSERT INTO test.reorg_tz(ts) VALUES('2020-01-02 08:00:00')",
    );
    rows(&mut session, "CREATE INDEX ig ON test.reorg_tz(g)");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT g FROM test.reorg_tz FORCE INDEX(ig) WHERE g=8"
        )),
        [["8"]]
    );
}

/// Go keeps hypothetical indexes and TiFlash replicas in `SessionVars`
/// (`addHypoIndexIntoCtx`, `dropHypoIndexFromCtx`, `setHypoTiFlashReplica`):
/// only EXPLAIN plans them and SHOW CREATE TABLE prints the indexes. This
/// node stored them as real indexes.
#[test]
fn hypothetical_indexes_are_session_state_as_go_does() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1008)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.t (a INT, b INT, c INT, KEY (a))",
    );
    session
        .node_conn()
        .execute_write("CREATE INDEX hypo_bc TYPE HYPO ON test.t (b, c)")
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE UNIQUE INDEX hypo_c TYPE HYPO ON test.t (c)")
        .unwrap();
    let error = session
        .node_conn()
        .execute_write("CREATE INDEX hypo_bc TYPE HYPO ON test.t (b)")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1105, "conflict hypo index name hypo_bc")
    );
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.t"));
    assert!(
        created[0][1].contains(
            "  KEY `a` (`a`),\n  KEY `hypo_bc` (`b`,`c`) /* HYPO INDEX */,\n  \
             UNIQUE KEY `hypo_c` (`c`) /* HYPO INDEX */\n"
        ),
        "{}",
        created[0][1]
    );
    let explain = |session: &mut Session, sql: &str| {
        displayed(rows(session, sql))
            .into_iter()
            .map(|row| row[3].clone())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        explain(
            &mut session,
            "EXPLAIN FORMAT='brief' SELECT b FROM test.t WHERE b = 1 AND c = 2"
        )
        .last()
        .unwrap(),
        // The unique hypothetical index answers `c = 2` with a point get,
        // which beats the two-column one (the oracle's plan, now that a
        // hypothetical index gets a filled path like any other).
        "table:t, index:hypo_c(c)"
    );
    assert_eq!(
        explain(
            &mut session,
            "EXPLAIN FORMAT='brief' SELECT b FROM test.t WHERE b = 1 AND c > 2"
        )
        .last()
        .unwrap(),
        "table:t, index:hypo_bc(b, c)"
    );
    assert_eq!(
        explain(
            &mut session,
            "EXPLAIN FORMAT='brief' SELECT c FROM test.t WHERE c = 1"
        ),
        ["table:t, index:hypo_c(c)"]
    );
    // Only EXPLAIN sees them: the table has no such index to read.
    assert!(rows(&mut session, "SELECT c FROM test.t WHERE c = 1").is_empty());
    session
        .node_conn()
        .execute_write("DROP HYPO INDEX hypo_bc ON test.t")
        .unwrap();
    let error = session
        .node_conn()
        .execute_write("DROP HYPO INDEX hypo_bc ON test.t")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1091, "index hypo_bc doesn't exist")
    );
    session
        .node_conn()
        .execute_write("DROP HYPO INDEX IF EXISTS hypo_bc ON test.t")
        .unwrap();
    session
        .node_conn()
        .execute_write("ALTER TABLE test.t SET HYPO TIFLASH REPLICA 1")
        .unwrap();
    assert!(
        explain(&mut session, "EXPLAIN FORMAT='brief' SELECT b FROM test.t")
            .iter()
            .any(|object| object == "table:t")
    );
    assert!(displayed(rows(
        &mut session,
        "EXPLAIN FORMAT='brief' SELECT b FROM test.t"
    ))
    .iter()
    .any(|row| row[2] == "mpp[tiflash]"));
    session
        .node_conn()
        .execute_write("ALTER TABLE test.t SET HYPO TIFLASH REPLICA 0")
        .unwrap();
    assert!(!displayed(rows(
        &mut session,
        "EXPLAIN FORMAT='brief' SELECT b FROM test.t"
    ))
    .iter()
    .any(|row| row[2] == "mpp[tiflash]"));
    // Session state: another session sees none of it.
    let mut other = stack.factory.open_session(session_context(1009)).unwrap();
    let created = displayed(rows(&mut other, "SHOW CREATE TABLE test.t"));
    assert!(!created[0][1].contains("HYPO"), "{}", created[0][1]);
}

/// Two expression indexes on one expression own different hidden columns,
/// and Go's `EqualByExprAndID` treats them as one by `FieldType.Equal`,
/// which ignores key flags. Go stamps no key flag on a hidden column
/// (`setColumnFlagWithConstraint` skips expression parts), and pushes an
/// index filter on one through `PushDownExprs` alone. This node stamped
/// `MultipleKeyFlag`, compared flags, and kept the filter at root.
#[test]
fn expression_indexes_on_one_expression_cover_each_other_as_go_does() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1010)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.t1 (a VARCHAR(10), b VARCHAR(10), KEY e1 ((lower(a))), \
         KEY e2 ((lower(a))), KEY idx ((lower(a)), a), KEY idx1 (a, (lower(a))), \
         KEY idx2 (a, (lower(a)), b))",
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "EXPLAIN FORMAT='brief' SELECT * FROM test.t1 WHERE lower(a) LIKE \"aadwa\" AND a = \"a\""
        )),
        [
            ["IndexReader", "1.00", "root", "", "index:Projection"],
            ["└─Projection", "1.00", "cop[tikv]", "", "test.t1.a, test.t1.b"],
            [
                "  └─Selection",
                "1.00",
                "cop[tikv]",
                "",
                "like(lower(test.t1.a), \"aadwa\", 92)"
            ],
            [
                "    └─IndexRangeScan",
                "1.25",
                "cop[tikv]",
                "table:t1, index:idx2(a, lower(`a`), b)",
                "range:[\"a\" \"aadwa\",\"a\" \"aadwa\"], keep order:false, stats:pseudo"
            ],
        ]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT column_name, column_key FROM information_schema.columns \
             WHERE table_schema = 'test' AND table_name = 't1' ORDER BY ordinal_position"
        )),
        [["a", "MUL"], ["b", ""]]
    );
}

/// Go `checkGlobalIndexes` on the production CREATE TABLE: a GLOBAL index
/// needs a partitioned table (8200), and one with a prefix part warns that
/// auto analyze cannot sample it (8265); Go's ADD INDEX check reads
/// `tblInfo.Columns[col.Offset]`, where an expression part's new hidden
/// column still has `Offset` 0, so a GLOBAL expression index does not warn.
#[test]
fn global_index_rules_on_the_production_create_table() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1023)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    let error = session
        .node_conn()
        .execute_write("CREATE TABLE plain (a INT, b INT, UNIQUE INDEX idx (a) GLOBAL)")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (8200, "Unsupported Global Index on non-partitioned table")
    );
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE gp (a INT, d VARCHAR(10), UNIQUE INDEX d_s (d(3)) GLOBAL) \
             PARTITION BY HASH (a) PARTITIONS 3",
        )
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        [[
            "Warning",
            "8265",
            "Auto analyze is not effective for index 'd_s', need analyze manually"
        ]]
    );
    session
        .node_conn()
        .execute_write("ALTER TABLE gp ADD UNIQUE INDEX ie ((a + 2)) GLOBAL")
        .unwrap();
    assert!(rows(&mut session, "SHOW WARNINGS").is_empty());
}

/// Go `buildCheckIndexSchema`: `ADMIN CHECK INDEX t idx (lo, hi)` returns the
/// indexed columns under their own types and a BIGINT `extra_handle`; the
/// production text writer refused rows declared all-VARCHAR.
#[test]
fn admin_check_index_range_rows_carry_the_index_column_types() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1024)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE ci (a INT, b VARCHAR(10), INDEX a_b (a, b))")
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO ci VALUES (3, 'ab'), (2, 'cd'), (1, 'ef'), (-1, 'hi')")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "ADMIN CHECK INDEX ci a_b (2, 4)")),
        [["1", "ef", "3"], ["2", "cd", "2"]]
    );
}

/// Go's CREATE TABLE and DROP INDEX on the production node:
/// - a written `PLACEMENT POLICY 'default'` is a reference Go's
///   `checkAndNormalizePlacementPolicy` clears afterwards, so the table does
///   not take the schema's default policy;
/// - `onDropIndex` removes an expression index's hidden columns
///   (`RemoveDependentHiddenColumns`), so their names are free again.
#[test]
fn create_table_default_policy_and_drop_index_hidden_columns_as_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1027)).unwrap();
    session
        .node_conn()
        .execute_write("CREATE PLACEMENT POLICY px FOLLOWERS=1")
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE DATABASE pdb PLACEMENT POLICY px")
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE pdb.t1 (a INT) PLACEMENT POLICY 'default'")
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE pdb.t2 (a INT)")
        .unwrap();
    let show = |session: &mut Session, table: &str| {
        displayed(rows(session, &format!("SHOW CREATE TABLE {table}")))[0][1].clone()
    };
    assert!(!show(&mut session, "pdb.t1").contains("PLACEMENT POLICY"));
    assert!(show(&mut session, "pdb.t2").contains("PLACEMENT POLICY=`px`"));
    session
        .node_conn()
        .execute_write("CREATE TEMPORARY TABLE pdb.tmp LIKE pdb.t1")
        .unwrap();

    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE h (a INT, b REAL)")
        .unwrap();
    session
        .node_conn()
        .execute_write("ALTER TABLE h ADD INDEX idx ((a + 1))")
        .unwrap();
    let error = session
        .node_conn()
        .execute_write("ALTER TABLE h ADD COLUMN (_V$_idx_0 INT)")
        .unwrap_err();
    assert_eq!(error.code, 1060);
    session
        .node_conn()
        .execute_write("ALTER TABLE h DROP INDEX idx")
        .unwrap();
    session
        .node_conn()
        .execute_write("ALTER TABLE h ADD COLUMN (_V$_idx_0 INT)")
        .unwrap();
}

/// Go `CreateIndex` turns `ErrDupKeyName` under `IF NOT EXISTS` into a Note
/// and succeeds; the production planner returned success with no Note.
#[test]
fn create_index_if_not_exists_notes_the_duplicate_key_name() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1036)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE t (a INT, KEY idx (a))")
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE INDEX IF NOT EXISTS idx ON t (a)")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        [["Note", "1061", "Duplicate key name 'idx'"]]
    );
}

/// Go `checkAlterTableCharset` runs `checkIndexLengthWithNewCharset` before a
/// CONVERT TO CHARACTER SET: `(varchar(500) utf8, varchar(500) utf8)` keys at
/// 3000 bytes and would key at 4000 as utf8mb4, past `MaxIndexLength`.
#[test]
fn convert_charset_refuses_an_index_that_would_grow_too_long() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1037)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE posts (id INT AUTO_INCREMENT PRIMARY KEY, \
             title VARCHAR(500) CHARACTER SET utf8, subtitle VARCHAR(500) CHARACTER SET utf8, \
             UNIQUE KEY (title, subtitle))",
        )
        .unwrap();
    let error = session
        .node_conn()
        .execute_write("ALTER TABLE posts CONVERT TO CHARACTER SET utf8mb4")
        .unwrap_err();
    assert_eq!(error.code, 1071);
    assert_eq!(
        error.message,
        "Specified key was too long (4000 bytes); max key length is 3072 bytes"
    );
    session
        .node_conn()
        .execute_write("CREATE TABLE short (a VARCHAR(10) CHARACTER SET utf8, KEY (a))")
        .unwrap();
    session
        .node_conn()
        .execute_write("ALTER TABLE short CONVERT TO CHARACTER SET utf8mb4")
        .unwrap();
}

/// Go's pushed-down index lookup (`LocalIndexLookUp`) runs its build side --
/// `Limit | Selection | IndexScan` -- on index entries, sorts the cut handle
/// batch, and only then applies the root `IndexLookUp`'s embedded offset; a
/// root `Selection` stays above all of it. The port took the root Selection
/// into the source (filtering before the offset), lowered the build-side
/// Selection as a table-side residual (so the entry stream was never cut),
/// and let the direct chunk path run past the pushed count.
#[test]
fn index_lookup_pushdown_cuts_entries_before_the_root_filter() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1046)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE ilp1 (id INT PRIMARY KEY, a VARCHAR(32), b INT, c INT, INDEX i(a, b))",
        )
        .unwrap();
    session
        .node_conn()
        .execute_write(
            "INSERT INTO ilp1 VALUES (1, '9a', 10, 100), (2, '8b', 20, 200), (3, '7c', 30, 300), \
             (4, '6d', 40, 400), (5, '5e', 50, 500), (6, '4f', 60, 600), (7, '3g', 70, 700), \
             (8, '2h', 80, 800), (9, '1i', 90, 900), (10, '0j', 100, 1000)",
        )
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT id FROM (SELECT /*+ index_lookup_pushdown(ilp1, i) */ * FROM ilp1 \
             WHERE a > '1i' LIMIT 1, 6) tx WHERE tx.c != 500 LIMIT 4",
        )),
        [["3"], ["4"], ["6"], ["7"]]
    );
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE ilp2 (a VARCHAR(32), b INT, c INT, d INT, e INT, \
             PRIMARY KEY (a, b) CLUSTERED, INDEX i(c), UNIQUE INDEX u(e)) \
             CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
        )
        .unwrap();
    session
        .node_conn()
        .execute_write(
            "INSERT INTO ilp2 VALUES ('a', 1, 9010, 100, 90), ('A', 2, 8020, 200, 80), \
             ('a', 3, 7030, 300, 70), ('b', 1, 6040, 400, 60), ('B', 2, 5050, 500, 50), \
             ('b', 3, 4060, 600, 40), ('c', 1, 3070, 700, 30), ('C', 2, 2080, 800, 20), \
             ('c', 3, 1090, 900, 10)",
        )
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT /*+ index_lookup_pushdown(ilp2, i) */ * FROM ilp2 \
             WHERE b < 3 AND a < 'c' LIMIT 3",
        )),
        [
            ["A", "2", "8020", "200", "80"],
            ["b", "1", "6040", "400", "60"],
            ["B", "2", "5050", "500", "50"]
        ]
    );
    // Pruned to the clustered key, the same read is a covering IndexReader:
    // its cop `Limit` counts the entries that pass the cop Selection below
    // it, in index order.
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT /*+ index_lookup_pushdown(ilp2, i) */ a, b FROM ilp2 \
             WHERE b < 3 AND a < 'c' LIMIT 3",
        )),
        [["B", "2"], ["b", "1"], ["A", "2"]]
    );
}

/// A cache hit rebinds the index-merge reader's TABLE side too: its recheck
/// Selection carries the same parameter as the partial `a = ?`. Rebinding only
/// the partials left `a = 1` in the recheck, which dropped every `a = 3` row
/// the rebuilt partial found.
#[test]
fn cached_index_merge_rebinds_the_table_side_recheck() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1047)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE cim (id INT NOT NULL, a INT, b INT, c INT, \
             PRIMARY KEY (id) CLUSTERED, KEY ia (a, c), KEY ib (b, c))",
        )
        .unwrap();
    session
        .node_conn()
        .execute_write(
            "INSERT INTO cim VALUES (1,1,1,10), (2,1,2,20), (3,2,1,30), (4,2,2,40), \
             (5,1,3,50), (6,3,1,60), (7,3,2,70), (8,1,1,5), (9,2,3,15), (10,3,3,25)",
        )
        .unwrap();
    rows(
        &mut session,
        "PREPARE im FROM 'SELECT /*+ use_index_merge(cim) */ id FROM cim \
         WHERE a = ? OR b = 1 ORDER BY id'",
    );
    for (value, expected) in [
        (1, vec!["1", "2", "3", "5", "6", "8"]),
        (3, vec!["1", "3", "6", "7", "8", "10"]),
    ] {
        rows(&mut session, &format!("SET @a = {value}"));
        let got: Vec<String> = displayed(rows(&mut session, "EXECUTE im USING @a"))
            .into_iter()
            .map(|row| row[0].clone())
            .collect();
        assert_eq!(got, expected, "a = {value}");
    }
}

/// Go's `tryPointGetPlan`/`tryBatchPointGetPlan` skip a unique index with a
/// condition (`idxInfo.HasCondition()`): a partial index stores only the rows
/// its predicate admits, so pinning `c = 1` through it misses a row that never
/// entered it. The fast plan chose `unique index idx3(c) where b > 10` and the
/// row with `b = 5` came back empty.
#[test]
fn point_get_never_pins_a_partial_unique_index() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1053)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE pidx (a INT, b INT, c INT, UNIQUE INDEX idx3 (c) WHERE b > 10)",
        )
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO pidx VALUES (1, 5, 1), (2, 20, 2)")
        .unwrap();
    for sql in [
        "SELECT a, b, c FROM pidx WHERE c = 1",
        "SELECT a, b, c FROM pidx WHERE c IN (1, 2) ORDER BY c",
    ] {
        let plan = displayed(rows(
            &mut session,
            &format!("EXPLAIN FORMAT = 'brief' {sql}"),
        ));
        assert!(
            plan.iter().all(|row| !row[0].contains("Point_Get")),
            "{sql}: {plan:?}"
        );
    }
    assert_eq!(
        displayed(rows(&mut session, "SELECT a, b, c FROM pidx WHERE c = 1")),
        [["1", "5", "1"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT a, b, c FROM pidx WHERE c IN (1, 2) ORDER BY c"
        )),
        [["1", "5", "1"], ["2", "20", "2"]]
    );
}

/// A non-unique index scan with an equality prefix is ordered by the integer
/// handle the index appends, so `ORDER BY` the handle needs no Sort (Go's
/// `fillIndexPath` appends the handle to `IdxCols`).
#[test]
fn index_with_equality_prefix_provides_the_handle_order() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1105)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE tho (a INT PRIMARY KEY, b INT, c INT, KEY idx(b))")
        .unwrap();
    let operators = |session: &mut Session, sql: &str| -> Vec<String> {
        displayed(rows(session, sql))
            .into_iter()
            .map(|row| {
                row[0]
                    .trim_start_matches(['├', '└', '─', '│', ' '])
                    .to_owned()
            })
            .map(|id| id.split('_').next().unwrap_or_default().to_owned())
            .collect()
    };
    // Expected: Go 1.27 on this branch (`TestZZ` probe, `explain`).
    assert_eq!(
        operators(
            &mut session,
            "EXPLAIN SELECT a FROM tho WHERE b = 3 ORDER BY a"
        ),
        ["IndexReader", "Projection", "IndexRangeScan"]
    );
}

/// Go's `canPushToIndexPlan` refuses a TopN over a column the index holds only
/// a prefix of (`IsPrefix`), so the TopN sits on the TABLE side of the lookup
/// and orders by the full value.
#[test]
fn top_n_over_a_prefix_index_column_stays_on_the_table_side() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1058)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE tpx (id INT PRIMARY KEY, name VARCHAR(50), INDEX ip (name(3)))",
        )
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO tpx VALUES (1,'abcZ'),(2,'abcY'),(3,'abcA'),(4,'abcB')")
        .unwrap();
    let plan = displayed(rows(
        &mut session,
        "EXPLAIN FORMAT = 'brief' SELECT /*+ use_index(tpx, ip) */ id, name FROM tpx \
         ORDER BY name LIMIT 2",
    ));
    let text = plan
        .iter()
        .map(|row| row[0].clone())
        .collect::<Vec<_>>()
        .join(" | ");
    assert!(text.contains("TopN(Probe)"), "{text}");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT /*+ use_index(tpx, ip) */ id, name FROM tpx ORDER BY name LIMIT 2",
        )),
        [["3", "abcA"], ["4", "abcB"]]
    );
}

/// `executor/union_scan` `TestUpdateScanningHandles`: the inner side of an
/// index join over a table the transaction wrote is `UnionScan -> reader`, which
/// has to be rebuilt per probe like any other inner plan. Go updates every
/// row `a > 10` to `b = a - 10`.
#[test]
fn index_join_inner_side_under_a_union_scan_updates_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1119)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE tij (a INT PRIMARY KEY, b INT)")
        .unwrap();
    let values = (2..100)
        .map(|i| format!("({i}, {i})"))
        .collect::<Vec<_>>()
        .join(",");
    session
        .node_conn()
        .execute_write(&format!("INSERT INTO tij VALUES {values}"))
        .unwrap();
    session.node_conn().control_transaction("BEGIN").unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO tij VALUES (1, 1)")
        .unwrap();
    session
        .node_conn()
        .execute_write(
            "UPDATE /*+ INL_JOIN(t1) */ tij t1, (SELECT a, b FROM tij) t2 \
             SET t1.b = t2.b WHERE t1.a = t2.a + 10",
        )
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT COUNT(*) FROM tij WHERE a > 10 AND a - b != 10"
        )),
        [["0"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT COUNT(*) FROM tij WHERE a > 10 AND a - b = 10"
        )),
        [["89"]]
    );
    session.node_conn().control_transaction("ROLLBACK").unwrap();
}

/// The skyline's preferRange filter keeps an index path that serves a required
/// order next to a table range scan that does not: a stream aggregation over
/// `id < 10 GROUP BY k` reads the index `k` (the handle column answers the
/// filter) rather than sorting a table range. Expected rows: Go 1.27 on this
/// branch (`TestZZ` probe `SB3`).
#[test]
fn ordered_request_keeps_the_index_path_next_to_a_table_range() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1124)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE tsk (id INT NOT NULL, k INT NOT NULL DEFAULT 0, c CHAR(120) NOT NULL \
             DEFAULT '', PRIMARY KEY (id) /*T![clustered_index] CLUSTERED */, KEY k (k))",
        )
        .unwrap();
    let plan = displayed(rows(
        &mut session,
        "EXPLAIN FORMAT = 'verbose' SELECT /*+ STREAM_AGG() */ k, COUNT(*) FROM tsk \
         WHERE id < 10 GROUP BY k",
    ))
    .into_iter()
    .take(3)
    .map(|row| format!("{} {} {}", row[0], row[1], row[2]))
    .collect::<Vec<_>>();
    assert_eq!(
        plan,
        [
            "Projection_5 8.00 169804.71",
            "└─StreamAgg_16 8.00 169803.12",
            "  └─IndexReader_17 8.00 169000.72",
        ]
    );
}

/// Go keeps the index join candidates of the side a force hint does not name:
/// the hint only prefers a built candidate afterwards
/// (`preferIndexJoinFamily`), so the other side's inner task still spends its
/// ids. Expected ids: Go 1.27 on this branch (`TestZZ` probe `IJ1`).
#[test]
fn force_index_join_hint_still_builds_the_other_sides_inner_task() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1127)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    for ddl in [
        "CREATE TABLE ihs1 (j1 JSON, a INT, b INT, KEY idx_a (a), KEY idx_b (b), KEY idx_a_b (a, b))",
        "CREATE TABLE ihs2 (j1 JSON, a INT, b INT, KEY idx_a (a), KEY idx_b (b), KEY idx_a_b (a, b))",
        "INSERT INTO ihs1 VALUES ('{\"a\": 1}',1,1),('{\"a\": 1}',1,1),('{\"a\": 1}',1,1),('{\"a\": 1}',1,1),('{\"a\": 1}',1,1)",
        "INSERT INTO ihs2 VALUES ('{\"a\": 1}',1,1)",
        "ANALYZE TABLE ihs1, ihs2",
    ] {
        session.node_conn().execute_write(ddl).unwrap();
    }
    let ids = |session: &mut Session, hint: &str| -> Vec<String> {
        displayed(rows(
            session,
            &format!(
                "EXPLAIN FORMAT = 'verbose' SELECT /*+ {hint}(ihs2) */ * FROM ihs1 JOIN ihs2 ON ihs1.a = ihs2.a"
            ),
        ))
        .into_iter()
        .filter(|row| row.len() > 5 && row[3] == "root")
        .skip(1)
        .map(|row| row[0].clone())
        .collect()
    };
    assert_eq!(
        ids(&mut session, "INL_JOIN"),
        ["└─IndexJoin_22", "  ├─TableReader_62(Build)", "  └─IndexLookUp_59(Probe)"]
    );
    assert_eq!(
        ids(&mut session, "INL_HASH_JOIN"),
        ["└─IndexHashJoin_26", "  ├─TableReader_62(Build)", "  └─IndexLookUp_59(Probe)"]
    );
}

/// A multi-table `UPDATE` whose index-join inner side is a covering
/// `IndexReader` over the target table reads that side through the
/// coprocessor like a SELECT, and the write still needs each inner row's
/// handle, which the covering index response carries. Expected rows:
/// `tests/integrationtest/r/executor/write.result` (Go).
#[test]
fn a_covering_index_join_update_reads_its_target_handle_from_the_coprocessor() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1136)).unwrap();
    session
        .node_conn()
        .execute_write("CREATE DATABASE IF NOT EXISTS ij_dml")
        .unwrap();
    session.node_conn().execute_write("USE ij_dml").unwrap();
    for statement in [
        "CREATE TABLE t (a VARCHAR(1), INDEX idx(a))",
        "CREATE TABLE t1 (a VARCHAR(5), INDEX idx(a))",
        "INSERT INTO t VALUES ('a'), ('b')",
        "INSERT INTO t1 VALUES ('a'), ('bbbbb')",
        "UPDATE /*+ INL_JOIN(t) */ t, t1 SET t.a = 'a' WHERE t.a = t1.a",
        "UPDATE /*+ INL_HASH_JOIN(t) */ t, t1 SET t.a = 'a' WHERE t.a = t1.a",
    ] {
        session.node_conn().execute_write(statement).unwrap();
    }
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t ORDER BY a")),
        [["a"], ["b"]]
    );
}

/// Go's prefer-range-scan pruning reads the candidate's own eq/in count, which
/// a DNF derives from its minimum access conditions, so `a = 1 OR a = 2` over
/// a pseudo-statistics table reads the index (the same range as
/// `a >= 1 AND a <= 2` loses on cost). An OR over an unindexed column keeps
/// the OR tree as Go's `ComposeDNFCondition` shaped it: balanced, not a chain.
#[test]
fn dnf_over_an_index_prefers_the_range_scan_and_keeps_go_or_shape() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1135)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE dn (a INT, b INT, KEY (a))")
        .unwrap();
    let plan = |session: &mut Session, sql: &str| {
        displayed(rows(session, &format!("explain format='brief' {sql}")))
            .into_iter()
            .map(|row| row.join(" "))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let index = plan(&mut session, "select * from dn where a = 1 or a = 2");
    assert!(
        index.contains("IndexLookUp") && index.contains("range:[1,2]"),
        "{index}"
    );
    let range = plan(&mut session, "select * from dn where a >= 1 and a <= 2");
    assert!(range.contains("TableFullScan"), "{range}");
    let chain = plan(
        &mut session,
        "select * from dn where b = 1 or b = 2 or b = 3 or b = 4",
    );
    assert!(
        chain.contains(
            "or(or(eq(test.dn.b, 1), eq(test.dn.b, 2)), or(eq(test.dn.b, 3), eq(test.dn.b, 4)))"
        ),
        "{chain}"
    );
}

/// ADD INDEX runs as Go's persisted job: delete-only, write-only, the txn-merge
/// reorganization and public, with the job's comments; a duplicate rolls the
/// job back and a partial index is cancelled by the owner. Captured from the
/// Go oracle on this branch.
#[test]
fn add_index_runs_as_a_persisted_job_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1935)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    rows(&mut session, "create table t (a int, b int, c varchar(10))");
    rows(
        &mut session,
        "insert into t values (1,1,'x'),(2,2,'y'),(3,2,'z')",
    );
    // (type, schema_state, rows, state, comments) of the newest job.
    let mut last_job = |session: &mut _| {
        let row = displayed(rows(session, "admin show ddl jobs 1")).remove(0);
        [3, 4, 7, 11, 12]
            .map(|column| row[column].clone())
            .join("|")
    };
    for (sql, job) in [
        (
            "alter table t add index i(b)",
            "add index|public|3|synced|txn-merge, max_node_count=3",
        ),
        (
            "alter table t add index j(c(2))",
            "add index|public|3|synced|txn-merge, max_node_count=3",
        ),
        (
            "alter table t add index k((a+b))",
            "add index|public|3|synced|txn-merge, max_node_count=3",
        ),
        (
            "alter table t add primary key (a)",
            "add primary key|public|3|synced|txn-merge, max_node_count=3",
        ),
        (
            "create index ci on t (c)",
            "add index|public|3|synced|txn-merge, max_node_count=3",
        ),
    ] {
        rows(&mut session, sql);
        assert_eq!(last_job(&mut session), job, "{sql}");
    }
    let error = session
        .node_conn()
        .execute_write("alter table t add unique index u(b)")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1062, "Duplicate entry '2' for key 't.u'")
    );
    assert_eq!(
        last_job(&mut session),
        "add index|none|0|rollback done|txn-merge, max_node_count=3"
    );
    rows(&mut session, "create table e (a int)");
    for (table, sql) in [
        ("t", "alter table t add index p(a) where a > 1"),
        ("e", "alter table e add index p(a) where a > 1"),
    ] {
        let error = session.node_conn().execute_write(sql).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (
                8200,
                "Unsupported add partial index: add partial index without fast reorg is not supported"
            ),
            "{table}"
        );
        assert_eq!(
            last_job(&mut session),
            "add index|none|0|cancelled|txn-merge, max_node_count=3",
            "{table}"
        );
    }
    assert_eq!(
        displayed(rows(
            &mut session,
            "select * from t use index(i) where b = 2"
        )),
        vec![
            vec!["2".to_owned(), "2".to_owned(), "y".to_owned()],
            vec!["3".to_owned(), "2".to_owned(), "z".to_owned()],
        ]
    );
    rows(&mut session, "admin check table t");
    let created = displayed(rows(&mut session, "show create table t")).remove(0);
    assert!(!created[1].contains("`u`"), "{}", created[1]);
    let error = session
        .node_conn()
        .execute_write("alter table t add index i(b)")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1061, "Duplicate key name 'i'")
    );
    rows(&mut session, "alter table t add index if not exists i(b)");
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        vec![vec![
            "Note".to_owned(),
            "1061".to_owned(),
            "Duplicate key name 'i'".to_owned()
        ]]
    );
    // More rows than one batch (`tidb_ddl_reorg_batch_size` 256), over
    // partitions: every batch commits and the handle moves on.
    rows(&mut session, "create table d (x int)");
    rows(
        &mut session,
        "insert into d values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)",
    );
    rows(
        &mut session,
        "create table big (a int, b int) partition by hash(a) partitions 3",
    );
    rows(
        &mut session,
        "insert into big select p.x*100+q.x*10+r.x, p.x from d p, d q, d r",
    );
    rows(&mut session, "alter table big add unique index ua(a)");
    assert_eq!(
        last_job(&mut session),
        "add index|public|1000|synced|txn-merge, max_node_count=3"
    );
    rows(&mut session, "admin check table big");
    // A duplicate found batches in rolls the job back.
    rows(&mut session, "create table big1 (a int, b int)");
    rows(
        &mut session,
        "insert into big1 select a, if(a = 999, 5, a) from big order by a",
    );
    let error = session
        .node_conn()
        .execute_write("alter table big1 add unique index ub(b)")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1062, "Duplicate entry '5' for key 'big1.ub'")
    );
    // Go counts the three batches committed before the failing one.
    assert_eq!(
        last_job(&mut session),
        "add index|none|768|rollback done|txn-merge, max_node_count=3"
    );
    rows(&mut session, "admin check table big1");
    // Go `cleanupDDLReorgHandles`: nothing is left behind. Go's
    // `handleJobDone` cleans after the history commit and only then notifies
    // the submitter, but the submitter's ticker (`getJobCheckInterval`, 500ms
    // for ADD INDEX) may read the history row inside that window, so the
    // cleanup is waited for rather than assumed to precede the answer.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let left = displayed(rows(&mut session, "select count(*) from mysql.tidb_ddl_reorg"));
        if left == vec![vec!["0".to_owned()]] {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "reorg handles left behind: {left:?}");
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}

/// Go runs ADD INDEX through delete-only, write-only and the txn-merge
/// reorganization precisely so that DML running meanwhile keeps the new
/// index whole: writes during the backfill go to the temporary index and the
/// merge replays them. A second session inserts, updates and deletes rows
/// throughout the job; the table and every index agree afterwards.
#[test]
fn add_index_stays_consistent_with_concurrent_dml() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut session = factory.open_session(session_context(1936)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    rows(&mut session, "create table d (x int)");
    rows(
        &mut session,
        "insert into d values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)",
    );
    for table in ["cd", "ce"] {
        rows(
            &mut session,
            &format!("create table {table} (a int primary key, b int, c int)"),
        );
        rows(
            &mut session,
            &format!(
                "insert into {table} select p.x*1000+q.x*100+r.x*10+s.x, p.x, q.x from d p, d q, d r, d s"
            ),
        );
    }
    let done = std::sync::atomic::AtomicBool::new(false);
    let rounds = std::sync::atomic::AtomicUsize::new(0);
    std::thread::scope(|scope| {
        let (done, rounds) = (&done, &rounds);
        scope.spawn(move || {
            let mut session = factory.open_session(session_context(1937)).unwrap();
            rows(&mut session, "USE test");
            while !done.load(std::sync::atomic::Ordering::Acquire) {
                let round = rounds.load(std::sync::atomic::Ordering::Acquire);
                let key = (round * 397) % 10000;
                for table in ["cd", "ce"] {
                    for sql in [
                        format!(
                            "insert into {table} values ({}, {round}, {round})",
                            10000 + round
                        ),
                        format!("update {table} set b = b + 1 where a = {key}"),
                        format!("delete from {table} where a = {}", (key + 5000) % 10000),
                    ] {
                        rows(&mut session, &sql);
                    }
                }
                rounds.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            }
        });
        for sql in [
            "alter table cd add index ib(b)",
            "alter table ce add index ibc((b + c))",
        ] {
            let before = rounds.load(std::sync::atomic::Ordering::Acquire);
            rows(&mut session, sql);
            // The writer must have run while the job did.
            assert!(
                rounds.load(std::sync::atomic::Ordering::Acquire) > before + 1,
                "{sql}"
            );
        }
        done.store(true, std::sync::atomic::Ordering::Release);
    });
    for table in ["cd", "ce"] {
        rows(&mut session, &format!("admin check table {table}"));
    }
    assert_eq!(
        displayed(rows(
            &mut session,
            "select count(*) from cd use index(ib) where b >= 0"
        )),
        displayed(rows(
            &mut session,
            "select count(*) from cd ignore index(ib)"
        )),
    );
}

/// DROP INDEX / DROP PRIMARY KEY run as Go's persisted job through
/// `onDropIndex`'s states; the refusals are the executor's. Captured from
/// the Go oracle on this branch.
#[test]
fn drop_index_runs_as_a_persisted_job_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1939)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "create table t (a int, b int, c int, key ib(b), key ic(c), primary key (a) nonclustered)",
    );
    rows(&mut session, "insert into t values (1,1,1),(2,2,2)");
    let mut last_job = |session: &mut _| {
        let row = displayed(rows(session, "admin show ddl jobs 1")).remove(0);
        [3, 4, 7, 11, 12]
            .map(|column| row[column].clone())
            .join("|")
    };
    for (sql, job) in [
        ("alter table t drop index ib", "drop index|none|0|synced|"),
        ("drop index ic on t", "drop index|none|0|synced|"),
        (
            "alter table t drop primary key",
            "drop primary key|none|0|synced|",
        ),
    ] {
        rows(&mut session, sql);
        assert_eq!(last_job(&mut session), job, "{sql}");
    }
    for sql in [
        "alter table t drop index if exists ib",
        "drop index if exists ib on t",
    ] {
        rows(&mut session, sql);
        assert_eq!(
            displayed(rows(&mut session, "SHOW WARNINGS")),
            vec![vec![
                "Note".to_owned(),
                "1091".to_owned(),
                "index ib doesn't exist".to_owned()
            ]],
            "{sql}"
        );
    }
    rows(&mut session, "create table tc (a int primary key, b int)");
    rows(
        &mut session,
        "create table p (id int primary key, k int, key kk(k))",
    );
    rows(
        &mut session,
        "create table c (id int primary key, pk int, key fk_idx(pk), foreign key fkc (pk) references p(id))",
    );
    for (sql, code, message) in [
        (
            "alter table t drop index ib",
            1091,
            "index ib doesn't exist",
        ),
        (
            "alter table t drop primary key",
            1091,
            "Can't DROP 'PRIMARY'; check that column/key exists",
        ),
        (
            "alter table tc drop primary key",
            8200,
            "Unsupported drop primary key when the table is using clustered index",
        ),
        (
            "alter table c drop index fk_idx",
            1553,
            "Cannot drop index 'fk_idx': needed in a foreign key constraint",
        ),
    ] {
        let error = session.node_conn().execute_write(sql).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (code, message),
            "{sql}"
        );
    }
    rows(&mut session, "admin check table t");
    let created = displayed(rows(&mut session, "show create table t")).remove(0);
    assert_eq!(
        created[1],
        "CREATE TABLE `t` (\n  `a` int NOT NULL,\n  `b` int DEFAULT NULL,\n  `c` int DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    );
}

/// Go `onRenameIndex` renames an expression index's hidden columns with it
/// (`renameHiddenColumns`, Issue 51431), so a later index may take the old
/// name. Captured from the Go oracle.
#[test]
fn renaming_an_expression_index_frees_its_name_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1942)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    rows(&mut session, "create table t(j json)");
    rows(
        &mut session,
        "alter table t add index idx1((cast(j as char(10) array)))",
    );
    rows(&mut session, "alter table t rename index idx1 to idx2");
    rows(
        &mut session,
        "alter table t add index idx1((cast(j as char(10) array)))",
    );
    assert_eq!(
        displayed(rows(&mut session, "show create table t")).remove(0)[1],
        "CREATE TABLE `t` (\n  `j` json DEFAULT NULL,\n  KEY `idx2` ((cast(`j` as char(10) array))),\n  KEY `idx1` ((cast(`j` as char(10) array)))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    );
    rows(&mut session, "insert into t values ('[\"a\",\"b\"]')");
    assert_eq!(
        displayed(rows(
            &mut session,
            "select count(*) from t where 'a' member of (j)"
        )),
        [["1"]]
    );
}

/// Go `TestAlterIndexVisibility` (bd29ad1ac3f): an ordinary index whose name
/// has an underscore-delimited suffix is not the temporary index a MODIFY
/// COLUMN builds, so it keeps its visibility; and (gomaster probe) the
/// temporary index of a MODIFY COLUMN in the same statement takes the
/// change, so the replaced index ends up with it.
#[test]
fn test_alter_index_visibility() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = testkit(&stack, 9311);
    tk.must_exec("use test");
    let query = |table: &str| {
        format!(
            "select key_name, is_visible from information_schema.tidb_indexes \
             where table_schema = 'test' and table_name = '{table}' order by key_name"
        )
    };
    tk.must_exec("create table t_invisible (k int, key idx_k(k), key idx_k_1(k), key idx_k_copy(k))");
    tk.must_exec("alter table t_invisible alter index idx_k invisible");
    tk.must_query(&query("t_invisible")).check(["idx_k NO", "idx_k_1 YES", "idx_k_copy YES"]);
    tk.must_exec("create table t_visible (k int, key idx_k(k) invisible, key idx_k_1(k) invisible, key idx_k_copy(k) invisible)");
    tk.must_exec("alter table t_visible alter index idx_k visible");
    tk.must_query(&query("t_visible")).check(["idx_k YES", "idx_k_1 NO", "idx_k_copy NO"]);

    tk.must_exec("create table t (a int, b int, key idx(a), key idx_1(a))");
    tk.must_exec("insert into t values (1, 1)");
    tk.must_exec("alter table t modify column a bigint, alter index idx invisible");
    tk.must_query(&query("t")).check(["idx NO", "idx_1 YES"]);
    tk.must_exec("admin check table t");
    // Go `TestMultiSchemaChangeAlterIndex`'s order: the visibility sub-job
    // runs while the MODIFY COLUMN's temporary index stands in for `idx`.
    tk.must_exec("create table t2 (a int, b int, key idx(a, b), key idx_1(b))");
    tk.must_exec("insert into t2 values (1, 2)");
    tk.must_exec("alter table t2 alter index idx invisible, modify column a tinyint, alter index idx_1 invisible");
    // One row per index column, as Go's TIDB_INDEXES has.
    tk.must_query(&query("t2")).check(["idx NO", "idx NO", "idx_1 NO"]);
    assert_eq!(tk.must_exec_to_err("select * from t2 use index (idx)").code, 1176);
    tk.must_exec("admin check table t2");
}
