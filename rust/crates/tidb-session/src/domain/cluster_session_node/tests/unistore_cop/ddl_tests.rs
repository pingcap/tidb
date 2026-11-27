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

/// Go `onModifyColumn` on the production node: each kind checks or rewrites
/// the rows in the DDL transaction, and the answers -- values, errors and the
/// final definitions -- are the Go oracle's for the same script. This node
/// refused every one of these changes ("Unsupported modify column: ...").
#[test]
fn a_modify_column_checks_or_rewrites_rows_as_go_does() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(136)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.mc (a BIGINT, b VARCHAR(20), c INT, d VARCHAR(10), \
         KEY ib (b), KEY ic (c))",
    );
    rows(
        &mut session,
        "INSERT INTO test.mc VALUES (1, 'x  ', 5, 'p'), (2, 'yy', NULL, 'q ')",
    );
    for sql in [
        // A checked narrowing.
        "ALTER TABLE test.mc MODIFY COLUMN a INT",
        // A sign change: a rewrite under a new column id.
        "ALTER TABLE test.mc MODIFY COLUMN a INT UNSIGNED",
        // VARCHAR -> CHAR over trailing spaces: the precheck fails, and the
        // statement is reorganized, which strips them.
        "ALTER TABLE test.mc MODIFY COLUMN b CHAR(10)",
        "ALTER TABLE test.mc MODIFY COLUMN d CHAR(5)",
    ] {
        session
            .node_conn()
            .execute_write(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }
    // Go `checkForNullValue`: a NULL row refuses NOT NULL.
    let error = session
        .node_conn()
        .execute_write("ALTER TABLE test.mc MODIFY COLUMN c INT NOT NULL")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1265, "Data truncated for column 'c' at row 1")
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT a, CONCAT('[', b, ']'), c, CONCAT('[', d, ']') FROM test.mc ORDER BY a"
        )),
        [["1", "[x]", "5", "[p]"], ["2", "[yy]", "NULL", "[q]"]]
    );
    rows(&mut session, "UPDATE test.mc SET c = 0 WHERE c IS NULL");
    session
        .node_conn()
        .execute_write("ALTER TABLE test.mc MODIFY COLUMN c INT NOT NULL DEFAULT 7 COMMENT 'cc'")
        .unwrap();
    // Go `checkModifyColumnData`: the first row the new length cannot hold.
    let error = session
        .node_conn()
        .execute_write("ALTER TABLE test.mc MODIFY COLUMN b VARCHAR(1)")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1265, "Data truncated for column 'b', value is 'yy'")
    );
    session
        .node_conn()
        .execute_write("ALTER TABLE test.mc CHANGE COLUMN a aa BIGINT FIRST")
        .unwrap();
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.mc"));
    assert!(
        created[0][1].contains(
            "  `aa` bigint DEFAULT NULL,\n  `b` char(10) DEFAULT NULL,\n  \
             `c` int NOT NULL DEFAULT '7' COMMENT 'cc',\n  `d` char(5) DEFAULT NULL,\n"
        ),
        "{}",
        created[0][1]
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM test.mc ORDER BY aa")),
        [["1", "x", "5", "p"], ["2", "yy", "0", "q"]]
    );
    rows(&mut session, "ADMIN CHECK TABLE test.mc");
}

/// Go `AlterTableCache` / `AlterTableNoCache` and the `ErrOptOnCacheTable`
/// refusals on the production node, answers from the Go oracle for the same
/// script. CACHE writes the `mysql.table_cache_meta` lock row and stages
/// `checkCacheTableSize`; every DDL Go's executor refuses on a cached table is
/// refused (8242) before a job. This node refused CACHE / NOCACHE and ran the
/// rest.
#[test]
fn a_cached_table_refuses_ddl_until_nocache_as_go_does() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(145)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.c1 (a INT PRIMARY KEY, b INT, KEY ib (b))",
    );
    rows(&mut session, "INSERT INTO test.c1 VALUES (1, 1), (2, 2)");
    session
        .node_conn()
        .execute_write("ALTER TABLE test.c1 CACHE")
        .unwrap();
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.c1"));
    assert!(
        created[0][1].ends_with(" /* CACHED ON */"),
        "{}",
        created[0][1]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT lock_type, lease, oldReadLease FROM mysql.table_cache_meta WHERE tid = \
             (SELECT tidb_table_id FROM information_schema.tables \
              WHERE table_schema = 'test' AND table_name = 'c1')"
        )),
        [["NONE", "0", "0"]]
    );
    rows(&mut session, "INSERT INTO test.c1 VALUES (3, 3)");
    assert_eq!(
        displayed(rows(&mut session, "SELECT a FROM test.c1 ORDER BY a")),
        [["1"], ["2"], ["3"]]
    );
    for (sql, operation) in [
        ("ALTER TABLE test.c1 ADD COLUMN c INT", "Alter Table"),
        ("ALTER TABLE test.c1 CACHE, ADD COLUMN d INT", "Alter Table"),
        ("CREATE INDEX i2 ON test.c1 (b)", "Create Index"),
        ("DROP INDEX ib ON test.c1", "Drop Index"),
        ("TRUNCATE TABLE test.c1", "Truncate Table"),
        ("RENAME TABLE test.c1 TO test.c2", "Rename Table"),
        ("DROP TABLE test.c1", "Drop Table"),
    ] {
        let error = session.node_conn().execute_write(sql).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (
                8242,
                format!("'{operation}' is unsupported on cache tables.").as_str()
            ),
            "{sql}"
        );
    }
    session
        .node_conn()
        .execute_write("ALTER TABLE test.c1 CACHE")
        .unwrap();
    session
        .node_conn()
        .execute_write("ALTER TABLE test.c1 NOCACHE")
        .unwrap();
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.c1"));
    assert!(!created[0][1].contains("CACHED ON"), "{}", created[0][1]);
    // Go `finishDDLJob`: NOCACHE deletes the lock row on `w.sess`.
    assert_eq!(
        displayed(rows(&mut session, "SELECT count(*) FROM mysql.table_cache_meta")),
        [["0"]]
    );
    session
        .node_conn()
        .execute_write("ALTER TABLE test.c1 ADD COLUMN c INT")
        .unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.p (a INT) PARTITION BY HASH (a) PARTITIONS 2",
    );
    for (sql, code, message) in [
        (
            "ALTER TABLE test.p CACHE",
            8242,
            "'partition mode' is unsupported on cache tables.",
        ),
        (
            "ALTER TABLE mysql.user CACHE",
            8200,
            "ALTER table cache for tables in system database is currently unsupported",
        ),
    ] {
        let error = session.node_conn().execute_write(sql).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (code, message),
            "{sql}"
        );
    }
}

/// Go `generateOriginDefaultValue` for an ADD COLUMN ... NOT NULL with no
/// default, and `getColDefaultValue`'s cast of the stored text on read; the
/// answers are the Go oracle's. This node refused SET, ENUM and JSON, stamped
/// BIT as `"0"` (stored as the byte 0x30) and BINARY(n) as no bytes, and read
/// a JSON origin default back as raw text ("binary JSON document").
#[test]
fn an_added_not_null_column_reads_gos_zero_value_on_existing_rows() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(143)).unwrap();
    rows(&mut session, "CREATE TABLE test.t (id INT PRIMARY KEY)");
    rows(&mut session, "INSERT INTO test.t VALUES (1), (2)");
    for column in [
        "s SET('a','b')",
        "e ENUM('x','y')",
        "b BIT(8)",
        "j JSON",
        "bn BINARY(3)",
        "d DECIMAL(6,2)",
        "f FLOAT",
        "y YEAR",
        "tm TIME(2)",
    ] {
        let sql = format!("ALTER TABLE test.t ADD COLUMN {column} NOT NULL");
        session
            .node_conn()
            .execute_write(&sql)
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }
    let zero = [
        "",
        "x",
        "0",
        "null",
        "000000",
        "0.00",
        "0",
        "0",
        "00:00:00.00",
    ];
    let read = displayed(rows(
        &mut session,
        "SELECT s, e, b + 0, j, HEX(bn), d, f, y + 0, tm FROM test.t ORDER BY id",
    ));
    assert_eq!(read, [zero, zero]);
    let error = session
        .node_conn()
        .execute_write("INSERT INTO test.t (id) VALUES (3)")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1364, "Field 's' doesn't have a default value")
    );
    rows(&mut session, "ADMIN CHECK TABLE test.t");
}

/// Go's table checks on the production node, answers from the Go oracle for
/// the same script: `checkTooLongSchema` / `checkTableInfoValidExtra` / ADD
/// COLUMN's name limit (1059), `buildColumnAndConstraint`'s reserved
/// `_tidb_rowid` (1166), a clustered or implicit primary key that would be
/// invisible (3522, from `buildTableInfo` and from
/// `updateVersionAndTableInfoWithCheck` after a change makes an invisible
/// unique index the primary key), `CreatePrimaryKey`'s clustered (8200) and
/// expression (3756) refusals, and a view's duplicate column (1060). This
/// node accepted every one of them.
#[test]
fn a_table_is_checked_as_go_checks_it_before_it_is_published() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(141)).unwrap();
    let long = "a".repeat(65);
    let too_long = format!("Identifier name '{long}' is too long");
    rows(&mut session, "CREATE TABLE test.t (a INT NOT NULL, b INT)");
    rows(
        &mut session,
        "ALTER TABLE test.t ADD UNIQUE ub (b) INVISIBLE",
    );
    let invisible = "A primary key index cannot be invisible";
    for (sql, code, message) in [
        (format!("CREATE DATABASE {long}"), 1059, too_long.as_str()),
        (
            format!("CREATE TABLE test.t_long (c1 INT, {long} INT)"),
            1059,
            too_long.as_str(),
        ),
        (
            format!("CREATE TABLE test.{long} (a INT)"),
            1059,
            too_long.as_str(),
        ),
        (
            format!("ALTER TABLE test.t ADD COLUMN {long} INT"),
            1059,
            too_long.as_str(),
        ),
        (
            "ALTER TABLE test.t ADD COLUMN `_tidb_rowid` INT".to_owned(),
            1166,
            "Incorrect column name '_tidb_rowid'",
        ),
        (
            "CREATE TABLE test.tpk (c1 INT NOT NULL, PRIMARY KEY (c1) INVISIBLE)".to_owned(),
            3522,
            invisible,
        ),
        (
            "ALTER TABLE test.t MODIFY COLUMN b INT NOT NULL".to_owned(),
            3522,
            invisible,
        ),
        (
            "ALTER TABLE test.t ADD PRIMARY KEY (a) CLUSTERED".to_owned(),
            8200,
            "Adding clustered primary key is not supported. Please consider adding \
             NONCLUSTERED primary key instead",
        ),
        (
            "ALTER TABLE test.t ADD PRIMARY KEY ((a + b)) NONCLUSTERED".to_owned(),
            3756,
            "The primary key cannot be an expression index",
        ),
        (
            "ALTER TABLE test.t ADD PRIMARY KEY (a) NONCLUSTERED INVISIBLE".to_owned(),
            3522,
            invisible,
        ),
        (
            "CREATE VIEW test.v AS SELECT 1 AS x, 1 AS x".to_owned(),
            1060,
            "Duplicate column name 'x'",
        ),
    ] {
        let error = session.node_conn().execute_write(&sql).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (code, message),
            "{sql}"
        );
    }
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.t"));
    assert!(
        created[0][1].contains(
            "  `a` int NOT NULL,\n  `b` int DEFAULT NULL,\n  \
             UNIQUE KEY `ub` (`b`) /*!80000 INVISIBLE */\n"
        ),
        "{}",
        created[0][1]
    );
}

/// A store answers a split with `EpochNotMatch` carrying the new regions'
/// memcomparable-encoded bounds, and client-go's V1 codec decodes them in
/// place before they reach the region cache. A Cop read's error went in
/// encoded, and a raw key just past a split (`t<id2>_r`, whose ninth byte is
/// below the encoded bound's 0xFF group marker) then sorted inside the
/// encoded left region: the new table's reads went to a region that does not
/// hold them, so `COUNT(*)` answered 0 and snapshot reads failed with
/// `InvalidReqRange` until the node restarted.
#[test]
fn a_split_seen_by_a_cop_read_keeps_routing_new_tables_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(141)).unwrap();
    rows(&mut session, "CREATE TABLE test.t1 (a INT, b INT)");
    rows(&mut session, "INSERT INTO test.t1 VALUES (1, 1), (2, NULL), (3, 3)");
    assert_eq!(
        displayed(rows(&mut session, "SELECT COUNT(*) FROM test.t1")),
        [["3"]]
    );
    for table in ["t2", "t3"] {
        // CREATE TABLE splits the region the cop read above cached, so the
        // next cop read of `t1` meets the split as an `EpochNotMatch`.
        rows(
            &mut session,
            &format!("CREATE TABLE test.{table} (a INT, b INT, KEY ia (a))"),
        );
        assert_eq!(
            displayed(rows(&mut session, "SELECT COUNT(*) FROM test.t1")),
            [["3"]]
        );
        rows(
            &mut session,
            &format!("INSERT INTO test.{table} VALUES (1, 1), (2, NULL)"),
        );
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!(
                    "SELECT COUNT(*), COUNT(b), COUNT(1) FROM test.{table} IGNORE INDEX (ia)"
                ),
            )),
            [["2", "1", "2"]]
        );
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!("SELECT COUNT(*) FROM test.{table} USE INDEX (ia)"),
            )),
            [["2"]]
        );
    }
}

/// Go `checkIsDroppableColumn`, `RenameColumn` and `AddColumn`'s generated
/// checks on the production node, answers from the Go oracle: a column a
/// generated column, an expression index or the partitioning reads stays
/// (3108, 3837), a hidden column can be neither read nor dropped (1054, 1091),
/// and an added generated column may read only generated columns before it
/// (3107). This node accepted every one of these. The table with a
/// multi-valued index is the loader's: it rebuilt the hidden
/// `CAST(... AS ... ARRAY)` column with the DDL checks and dropped the table.
#[test]
fn a_column_others_depend_on_stays_as_go_keeps_it() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(139)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.h (a INT, b INT AS (a + 1), c INT, KEY ((a + c))) \
         PARTITION BY HASH (c) PARTITIONS 2",
    );
    for (sql, code, message) in [
        (
            "ALTER TABLE test.h DROP COLUMN a",
            3108,
            "Column 'a' has a generated column dependency.",
        ),
        (
            "ALTER TABLE test.h DROP COLUMN c",
            3837,
            "Column 'c' has an expression index dependency and cannot be dropped or renamed",
        ),
        (
            "ALTER TABLE test.h RENAME COLUMN c TO cc",
            3837,
            "Column 'c' has an expression index dependency and cannot be dropped or renamed",
        ),
        (
            "ALTER TABLE test.h ADD COLUMN d INT AS (_V$_expression_index_0 + 1)",
            1054,
            "Unknown column '_v$_expression_index_0' in 'generated column function'",
        ),
        (
            "ALTER TABLE test.h ADD COLUMN e INT AS (b + 1) FIRST",
            3107,
            "Generated column can refer only to generated columns defined prior to it.",
        ),
        (
            "ALTER TABLE test.h DROP COLUMN _V$_expression_index_0",
            1091,
            "Can't DROP '_V$_expression_index_0'; check that column/key exists",
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
        "CREATE TABLE test.m (id INT PRIMARY KEY, a JSON, \
         INDEX idx ((CAST(a->'$.d' AS SIGNED ARRAY))))",
    );
    rows(
        &mut session,
        "INSERT INTO test.m VALUES (1, '{\"d\": [1, 2]}')",
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT id FROM test.m WHERE 1 MEMBER OF (a->'$.d')"
        )),
        [["1"]]
    );
}

#[test]
fn system_table_hidden_ids_use_the_full_counter_key() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(119))
        .expect("session");
    rows(&mut session, "INSERT INTO mysql.bind_info (original_sql, bind_sql, default_db, status, create_time, update_time, charset, collation, source, sql_digest) VALUES ('system counter allocation', 'SELECT 1', 'test', 'disabled', '2026-09-07 00:00:00', '2026-09-07 00:00:00', 'utf8mb4', 'utf8mb4_bin', 'manual', 'system_counter_probe')");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT original_sql FROM mysql.bind_info WHERE sql_digest='system_counter_probe'"
        )),
        [["system counter allocation"]]
    );
}

/// Pinned `pkg/ddl/notifier.TestPublishToTableStore`, `TestBasicPubSub`, and
/// `TestDeliverOrderAndCleanup` over the real bootstrapped notifier table.
#[test]
fn ddl_notifier_table_store_delivers_in_order_and_cleans_up() {
    let (stack, _users) = cop_backed_stack();
    let factory = stack.factory;
    let mut client = factory
        .open_session(session_context(151))
        .expect("client session opens");
    assert_eq!(
        displayed(rows(
            &mut client,
            "SELECT variable_value FROM mysql.tidb \
             WHERE variable_name = 'ddl_table_version'",
        )),
        [["4"]]
    );
    rows(&mut client, "DELETE FROM mysql.tidb_ddl_notifier");

    let pool: Arc<dyn SessionPool> = Arc::new(ClusterNotifierSessionPool::new(
        factory.advanced_sys_session_pool(),
    ));
    let store = Arc::new(ClusterNotifierTableStore);
    let mut publisher = pool.get().expect("publisher session");
    for (job_id, table_name) in [(1, "t1"), (2, "t2#special-char?in'name"), (3, "t3")] {
        pub_scheme_change_to_store(
            publisher.as_mut(),
            job_id,
            -1,
            SchemaChangeEvent::create_table(tidb_model::TableInfo {
                id: 999 + job_id,
                name: tidb_ast::CiString::new(table_name),
                ..tidb_model::TableInfo::default()
            }),
            store.as_ref(),
        )
        .expect("event publishes");
    }
    pool.put(publisher);

    let mut reader = pool.get().expect("list session");
    let mut list = store.list(reader.as_mut()).expect("list starts");
    let listed = list.read(reader.as_mut(), 2).expect("first page");
    assert_eq!(listed.len(), 2);
    assert_eq!(listed[0].event.get_create_table_info().name.original(), "t1");
    assert_eq!(
        listed[1].event.get_create_table_info().name.original(),
        "t2#special-char?in'name"
    );
    let listed = list.read(reader.as_mut(), 2).expect("second page");
    assert_eq!(
        listed
            .iter()
            .map(|change| (change.ddl_job_id, change.sub_job_id))
            .collect::<Vec<_>>(),
        [(3, -1)]
    );
    assert_eq!(listed[0].event.get_create_table_info().name.original(), "t3");
    list.close(reader.as_mut());
    pool.put(reader);

    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let handler: Handler = {
        let seen = Arc::clone(&seen);
        Arc::new(move |_, event| {
            seen.lock().unwrap().push(event.get_create_table_info().id);
            Ok(())
        })
    };
    let notifier = DdlNotifier::new(
        Arc::clone(&pool),
        Arc::clone(&store) as Arc<dyn Store>,
        Duration::from_millis(10),
    );
    notifier.register_handler(TEST_HANDLER_ID, handler);
    tidb_owner::Listener::on_become_owner(&notifier);
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        let remaining = displayed(rows(
            &mut client,
            "SELECT count(*) FROM mysql.tidb_ddl_notifier",
        ));
        if remaining == [["0"]] {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "notifier did not process and clean up every event"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    tidb_owner::Listener::on_retire_owner(&notifier);
    assert_eq!(*seen.lock().unwrap(), [1000, 1001, 1002]);
}

/// Pinned DDL subscriber `ActionCreateTable`, `ActionTruncateTable`, and
/// `ActionDropTable`: create every physical table's zero-valued statistics
/// placeholders, then retire the old physical ID by advancing stats_meta.
#[test]
fn table_lifecycle_ddl_updates_statistics_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(74))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE TABLE stats_lifecycle (a INT, b INT, INDEX idx_b(b))",
    );
    drain_stats_ddl_events(&stack.factory, &mut session);
    let old_id = stack
        .factory
        .catalog
        .get_latest()
        .table_info_by_name("test", "stats_lifecycle")
        .expect("created table is published")
        .1
        .read().id;
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!("SELECT modify_count, count FROM mysql.stats_meta WHERE table_id = {old_id}"),
        )),
        [["0", "0"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!("SELECT count(*) FROM mysql.stats_histograms WHERE table_id = {old_id}"),
        )),
        [["3"]]
    );

    rows(
        &mut session,
        "CREATE TABLE stats_lifecycle_like LIKE stats_lifecycle",
    );
    drain_stats_ddl_events(&stack.factory, &mut session);
    let like_id = stack
        .factory
        .catalog
        .get_latest()
        .table_info_by_name("test", "stats_lifecycle_like")
        .expect("CREATE TABLE LIKE result is published")
        .1
        .read().id;
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!("SELECT count(*) FROM mysql.stats_histograms WHERE table_id = {like_id}"),
        )),
        [["3"]]
    );

    rows(&mut session, "INSERT INTO stats_lifecycle VALUES (1, 2)");
    rows(&mut session, "ANALYZE TABLE stats_lifecycle");
    let old_version = displayed(rows(
        &mut session,
        &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {old_id}"),
    ))[0][0]
        .parse::<u64>()
        .expect("stats version is an unsigned integer");
    rows(&mut session, "TRUNCATE TABLE stats_lifecycle");
    drain_stats_ddl_events(&stack.factory, &mut session);
    let new_id = stack
        .factory
        .catalog
        .get_latest()
        .table_info_by_name("test", "stats_lifecycle")
        .expect("truncated table is published")
        .1
        .read().id;
    assert_ne!(new_id, old_id, "truncate allocates a new physical ID");
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!("SELECT modify_count, count FROM mysql.stats_meta WHERE table_id = {new_id}"),
        )),
        [["0", "0"]]
    );
    let retired_version = displayed(rows(
        &mut session,
        &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {old_id}"),
    ))[0][0]
        .parse::<u64>()
        .expect("retired stats version is an unsigned integer");
    assert!(retired_version > old_version);

    let new_version = displayed(rows(
        &mut session,
        &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {new_id}"),
    ))[0][0]
        .parse::<u64>()
        .expect("new stats version is an unsigned integer");
    rows(&mut session, "DROP TABLE stats_lifecycle");
    drain_stats_ddl_events(&stack.factory, &mut session);
    let dropped_version = displayed(rows(
        &mut session,
        &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {new_id}"),
    ))[0][0]
        .parse::<u64>()
        .expect("dropped stats version is an unsigned integer");
    assert!(dropped_version > new_version);
}

/// Pinned `pkg/statistics/handle/ddl.TestDDLAfterLoad`: ADD COLUMN remains
/// valid after the table has been analyzed, populated, analyzed again, and is
/// therefore backed by an initialized statistics-cache entry.
#[test]
fn ddl_after_loaded_statistics_matches_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(75))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE TABLE stats_ddl_after_load (c1 INT, c2 INT, INDEX idx(c1, c2))",
    );
    rows(&mut session, "ANALYZE TABLE stats_ddl_after_load");
    let table_id = stack
        .factory
        .catalog
        .get_latest()
        .table_info_by_name("test", "stats_ddl_after_load")
        .expect("created table is published")
        .1
        .read().id;
    assert!(stack
        .factory
        .stats()
        .load()
        .get(&table_id)
        .and_then(tidb_stats_handle_syncload::stats_watch::TableStatsState::loaded)
        .is_some_and(|stats| !stats.hist_coll.pseudo));

    let values = (0..1000)
        .map(|value| format!("({value},{})", value + 1))
        .collect::<Vec<_>>()
        .join(",");
    rows(
        &mut session,
        &format!("INSERT INTO stats_ddl_after_load VALUES {values}"),
    );
    rows(&mut session, "ANALYZE TABLE stats_ddl_after_load");
    assert!(stack
        .factory
        .stats()
        .load()
        .get(&table_id)
        .and_then(tidb_stats_handle_syncload::stats_watch::TableStatsState::loaded)
        .is_some_and(|stats| !stats.hist_coll.pseudo));

    // Go keeps Domain.StatsHandle independently of an InfoSchema refresh.
    // Demand must still reload evicted payloads after a stats-only refresh
    // and after the DDL below replaces the connection's schema catalog.
    let check_reload = |session: &mut Session| {
        let peer = stack.factory.open_session(session_context(76)).unwrap();
        let peer_catalog = peer.shared_catalog();
        assert!(!peer_catalog
            .lock()
            .unwrap()
            .table_statistics_in(&peer_catalog.stats_handle(), table_id)
            .unwrap()
            .column_load_needed(1, true).1);
        let mut snapshot = (*stack.factory.stats().load()).clone();
        let table = snapshot.get(&table_id).unwrap().loaded().unwrap();
        let table = table.copy_as(tidb_stats::CopyIntent::AllDataWritable);
        for column in table.hist_coll.stable_columns() {
            column.write().unwrap().drop_unnecessary_data();
        }
        for index in table.hist_coll.stable_indices() {
            index.write().unwrap().evict_all_stats();
        }
        snapshot.insert(
            table_id,
            tidb_stats_handle_syncload::stats_watch::TableStatsState::Loaded(Arc::new(table)),
        );
        stack.factory.stats().store_after_analyze(snapshot);
        assert!(peer_catalog
            .lock()
            .unwrap()
            .table_statistics_in(&peer_catalog.stats_handle(), table_id)
            .unwrap()
            .column_load_needed(1, true).1);
        let read_count = tidb_stats_handle_metrics::READ_STATS_HISTOGRAM.get_sample_count();
        rows(
            session,
            "EXPLAIN SELECT * FROM stats_ddl_after_load WHERE c1 = 42 AND c2 = 43",
        );
        assert!(
            tidb_stats_handle_metrics::READ_STATS_HISTOGRAM.get_sample_count() > read_count,
            "successful storage loads must record read latency"
        );
        let snapshot = stack.factory.stats().load();
        let table = snapshot.get(&table_id).unwrap().loaded().unwrap();
        assert!(
            table
                .hist_coll
                .get_column(1)
                .unwrap()
                .read()
                .unwrap()
                .stats_loaded_status
                .is_full_load(),
            "column payload must reload after catalog refresh"
        );
        assert!(
            table
                .hist_coll
                .get_index(1)
                .unwrap()
                .read()
                .unwrap()
                .stats_loaded_status
                .is_full_load(),
            "index payload must reload after catalog refresh"
        );
        // Go publishes into Domain.StatsHandle, not just the loading session.
        // This peer must see the payload without rebuilding its schema catalog.
        assert!(
            !peer_catalog
                .lock()
                .unwrap()
                .table_statistics_in(&peer_catalog.stats_handle(), table_id)
                .unwrap()
                .column_load_needed(1, true).1,
            "the peer still sees an evicted column after the domain load completes"
        );
        assert!(
            Arc::ptr_eq(
                &peer_catalog
                    .lock()
                    .unwrap()
                    .table_statistics_in(&peer_catalog.stats_handle(), table_id)
                    .unwrap(),
                &session
                    .shared_catalog()
                    .lock()
                    .unwrap()
                    .table_statistics_in(&session.stats_handle(), table_id)
                    .unwrap(),
            ),
            "sessions must share the same converted statistics"
        );
    };
    check_reload(&mut session);

    rows(
        &mut session,
        "ALTER TABLE stats_ddl_after_load ADD COLUMN c10 INT",
    );
    assert!(stack
        .factory
        .catalog
        .get_latest()
        .table_info_by_name("test", "stats_ddl_after_load")
        .is_some_and(|(_, table)| table.read().columns.iter_deref().any(|column| column
            .read()
            .name
            .lowercase()
            == "c10")));
    check_reload(&mut session);
}

/// Pinned `ActionFlashbackCluster`: both table-wide version updates use one
/// transaction start TS, while `last_stats_histograms_version` is untouched.
#[test]
fn flashback_cluster_event_refreshes_statistics_versions_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(82))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE TABLE stats_flashback_event (a INT, b INT, INDEX idx(b))",
    );
    rows(
        &mut session,
        "INSERT INTO stats_flashback_event VALUES (1,2),(3,4)",
    );
    rows(&mut session, "ANALYZE TABLE stats_flashback_event");
    let table_id = stack
        .factory
        .catalog
        .get_latest()
        .table_info_by_name("test", "stats_flashback_event")
        .expect("table is published")
        .1
        .read().id;
    let before = displayed(rows(
        &mut session,
        &format!(
            "SELECT version, last_stats_histograms_version FROM mysql.stats_meta \
             WHERE table_id = {table_id}"
        ),
    ));
    handle_stats_schema_change(&mut session, &SchemaChangeEvent::flashback_cluster());
    let after = displayed(rows(
        &mut session,
        &format!(
            "SELECT version, last_stats_histograms_version FROM mysql.stats_meta \
             WHERE table_id = {table_id}"
        ),
    ));
    assert_ne!(after[0][0], before[0][0]);
    assert_eq!(after[0][1], before[0][1]);
    let histogram_versions = displayed(rows(
        &mut session,
        &format!("SELECT version FROM mysql.stats_histograms WHERE table_id = {table_id}"),
    ));
    assert!(!histogram_versions.is_empty());
    assert!(histogram_versions.iter().all(|row| row[0] == after[0][0]));
}

/// Pinned `TestDumpStatsDeltaBeforeHandleDDLEvent` and
/// `TestDumpStatsDeltaBeforeHandleAddColumnEvent`: a delayed subscriber event
/// must not overwrite metadata or histogram rows already written by a delta
/// flush or a later analyze.
#[test]
fn delayed_create_and_add_column_events_preserve_newer_statistics_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(84))
        .expect("session opens");
    rows(&mut session, "USE test");

    rows(&mut session, "CREATE TABLE stats_delayed_create (c1 INT)");
    rows(
        &mut session,
        "INSERT INTO stats_delayed_create VALUES (1),(2),(3)",
    );
    rows(&mut session, "FLUSH STATS_DELTA *.*");
    let create_table = stack
        .factory
        .catalog
        .get_latest()
        .table_info_by_name("test", "stats_delayed_create")
        .expect("created table is published")
        .1
        .read().clone_like_go();
    let create_id = create_table.id;
    let create_before = displayed(rows(
        &mut session,
        &format!(
            "SELECT version, modify_count, count, last_stats_histograms_version \
             FROM mysql.stats_meta WHERE table_id = {create_id}"
        ),
    ));
    handle_stats_schema_change(&mut session, &SchemaChangeEvent::create_table(create_table));
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT version, modify_count, count, last_stats_histograms_version \
                 FROM mysql.stats_meta WHERE table_id = {create_id}"
            ),
        )),
        create_before
    );

    rows(
        &mut session,
        "CREATE TABLE stats_delayed_column \
         (c1 INT, c2 INT, INDEX idx(c1, c2))",
    );
    rows(
        &mut session,
        "INSERT INTO stats_delayed_column VALUES (1,2),(2,3),(3,4)",
    );
    rows(
        &mut session,
        "ALTER TABLE stats_delayed_column ADD COLUMN c3 INT",
    );
    rows(
        &mut session,
        "INSERT INTO stats_delayed_column VALUES (4,5,6)",
    );
    rows(
        &mut session,
        "ALTER TABLE stats_delayed_column ADD COLUMN c4 INT NOT NULL DEFAULT 0",
    );
    rows(
        &mut session,
        "INSERT INTO stats_delayed_column(c1,c2) VALUES (6,7)",
    );
    rows(
        &mut session,
        "ANALYZE TABLE stats_delayed_column ALL COLUMNS",
    );
    let (column_table, delayed_columns) = {
        let catalog = stack.factory.catalog.get_latest();
        let table = catalog
            .table_info_by_name("test", "stats_delayed_column")
            .expect("altered table is published")
            .1
            .read().clone_like_go();
        let columns = table
            .columns
            .iter_deref()
            .filter_map(|column| {
                let column = column.read();
                matches!(column.name.lowercase(), "c3" | "c4").then(|| column.clone_like_go())
            })
            .collect::<Vec<_>>();
        (table, columns)
    };
    assert_eq!(delayed_columns.len(), 2);
    let column_table_id = column_table.id;
    let column_before = displayed(rows(
        &mut session,
        &format!(
            "SELECT version, last_stats_histograms_version FROM mysql.stats_meta \
             WHERE table_id = {column_table_id}"
        ),
    ));
    for column in delayed_columns {
        handle_stats_schema_change(
            &mut session,
            &SchemaChangeEvent::add_columns(column_table.clone_like_go(), vec![column]),
        );
    }
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT version, last_stats_histograms_version FROM mysql.stats_meta \
                 WHERE table_id = {column_table_id}"
            ),
        )),
        column_before
    );
}

/// Pinned DDL subscriber `ActionDropSchema` visits every table's partition
/// IDs in definition order and then its logical table ID, advancing each
/// extant stats row for delayed GC without failing the DROP on stats errors.
#[test]
fn drop_schema_ddl_retires_all_statistics_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(72))
        .expect("session opens");
    rows(
        &mut session,
        "SET GLOBAL tidb_partition_prune_mode = 'dynamic'",
    );
    rows(&mut session, "CREATE DATABASE stats_drop_schema");
    rows(&mut session, "USE stats_drop_schema");
    rows(&mut session, "CREATE TABLE ordinary (a INT)");
    rows(
        &mut session,
        "CREATE TABLE partitioned (a INT) PARTITION BY RANGE (a) (\
         PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN MAXVALUE)",
    );
    drain_stats_ddl_events(&stack.factory, &mut session);
    let retired_ids = {
        let catalog = stack.factory.catalog.get_latest();
        let database = catalog
            .schema_by_name("stats_drop_schema")
            .expect("created database is published");
        let mut ids = Vec::new();
        for table in catalog
            .schema_table_infos(database.read().name.original())
            .unwrap()
        {
            let table = table.read();
            if let Some(partition) = &table.partition {
                ids.extend(
                    partition
                        .read()
                        .definitions
                        .snapshot()
                        .into_iter()
                        .map(|definition| definition.id),
                );
            }
            ids.push(table.id);
        }
        ids
    };
    assert_eq!(retired_ids.len(), 4);
    let old_versions = retired_ids
        .iter()
        .map(|physical_id| {
            displayed(rows(
                &mut session,
                &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {physical_id}"),
            ))[0][0]
                .parse::<u64>()
                .expect("stats version is an unsigned integer")
        })
        .collect::<Vec<_>>();

    rows(&mut session, "DROP DATABASE stats_drop_schema");
    drain_stats_ddl_events(&stack.factory, &mut session);
    for (physical_id, old_version) in retired_ids.iter().zip(old_versions) {
        let retired_version = displayed(rows(
            &mut session,
            &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {physical_id}"),
        ))[0][0]
            .parse::<u64>()
            .expect("retired stats version is an unsigned integer");
        assert!(retired_version > old_version);
    }
}

/// Pinned DDL subscriber `ActionAddColumn`: a defaultless nullable column is
/// NULL for every existing row, and `InsertColStats2KV` persists that fact
/// before advancing the physical table's statistics version.
#[test]
fn add_column_ddl_initializes_statistics_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(73))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "CREATE TABLE stats_add_column (a INT)");
    drain_stats_ddl_events(&stack.factory, &mut session);
    rows(
        &mut session,
        "INSERT INTO stats_add_column VALUES (1),(2),(3)",
    );
    rows(&mut session, "ANALYZE TABLE stats_add_column");
    let table_id = stack
        .factory
        .catalog
        .get_latest()
        .table_info_by_name("test", "stats_add_column")
        .expect("created table is published")
        .1
        .read().id;
    let old_version = displayed(rows(
        &mut session,
        &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {table_id}"),
    ))[0][0]
        .parse::<u64>()
        .expect("stats version is an unsigned integer");

    rows(
        &mut session,
        "ALTER TABLE stats_add_column ADD COLUMN b INT",
    );
    drain_stats_ddl_events(&stack.factory, &mut session);
    let column_id = stack
        .factory
        .catalog
        .get_latest()
        .table_info_by_name("test", "stats_add_column")
        .expect("altered table is published")
        .1
        .read().columns
        .iter_deref()
        .find(|column| column.read().name.lowercase() == "b")
        .expect("added column is published")
        .read()
        .id;
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT distinct_count, null_count, stats_ver FROM mysql.stats_histograms \
                 WHERE table_id = {table_id} AND is_index = 0 AND hist_id = {column_id}"
            ),
        )),
        [["0", "3", "0"]]
    );
    let new_version = displayed(rows(
        &mut session,
        &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {table_id}"),
    ))[0][0]
        .parse::<u64>()
        .expect("stats version is an unsigned integer");
    assert!(new_version > old_version);

    rows(
        &mut session,
        "ALTER TABLE stats_add_column ADD COLUMN c VARCHAR(15) DEFAULT '123'",
    );
    drain_stats_ddl_events(&stack.factory, &mut session);
    let defaulted_column_id = stack
        .factory
        .catalog
        .get_latest()
        .table_info_by_name("test", "stats_add_column")
        .expect("altered table is published")
        .1
        .read().columns
        .iter_deref()
        .find(|column| column.read().name.lowercase() == "c")
        .expect("defaulted column is published")
        .read()
        .id;
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT distinct_count, null_count, tot_col_size, stats_ver \
                 FROM mysql.stats_histograms WHERE table_id = {table_id} \
                 AND is_index = 0 AND hist_id = {defaulted_column_id}"
            ),
        )),
        [["1", "0", "9", "0"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT repeats, count, lower_bound, upper_bound FROM mysql.stats_buckets \
                 WHERE table_id = {table_id} AND is_index = 0 \
                 AND hist_id = {defaulted_column_id}"
            ),
        )),
        [["3", "3", "123", "123"]]
    );

    rows(
        &mut session,
        "ALTER TABLE stats_add_column ADD COLUMN d BIGINT NOT NULL",
    );
    drain_stats_ddl_events(&stack.factory, &mut session);
    let zeroed_column_id = stack
        .factory
        .catalog
        .get_latest()
        .table_info_by_name("test", "stats_add_column")
        .expect("altered table is published")
        .1
        .read().columns
        .iter_deref()
        .find(|column| column.read().name.lowercase() == "d")
        .expect("NOT NULL column is published")
        .read()
        .id;
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT distinct_count, null_count, tot_col_size, stats_ver \
                 FROM mysql.stats_histograms WHERE table_id = {table_id} \
                 AND is_index = 0 AND hist_id = {zeroed_column_id}"
            ),
        )),
        [["1", "0", "0", "0"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT repeats, count, lower_bound, upper_bound FROM mysql.stats_buckets \
                 WHERE table_id = {table_id} AND is_index = 0 \
                 AND hist_id = {zeroed_column_id}"
            ),
        )),
        [["3", "3", "0", "0"]]
    );

    rows(
        &mut session,
        "ALTER TABLE stats_add_column ADD COLUMN e INT GENERATED ALWAYS AS (a + 1) VIRTUAL",
    );
    drain_stats_ddl_events(&stack.factory, &mut session);
    let virtual_column_id = {
        let catalog = stack.factory.catalog.get_latest();
        let column = catalog
            .table_info_by_name("test", "stats_add_column")
            .expect("altered table is published")
            .1
            .read().columns
            .iter_deref()
            .find(|column| column.read().name.lowercase() == "e")
            .expect("virtual column is published")
            .read()
            .clone_like_go();
        assert!(column.is_virtual_generated());
        assert_eq!(column.generated_expr_string, "`a` + 1");
        assert!(column.dependences.contains("a"));
        column.id
    };
    session.node_conn().rebuild_catalog_if_stale();
    {
        let catalog = session.shared_catalog();
        let catalog = catalog.lock().expect("session catalog is available");
        let tidb_infoschema::TableEntry::Kv(table) = catalog
            .table_in("test", "stats_add_column")
            .expect("rebuilt executor table is available")
        else {
            panic!("rebuilt executor table is storage-backed");
        };
        assert!(table
            .columns()
            .iter()
            .find(|column| column.read().name.original().eq_ignore_ascii_case("e"))
            .expect("executor virtual column exists")
            .generated
            .is_some());
    }
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT a, e FROM stats_add_column ORDER BY a",
        )),
        [["1", "2"], ["2", "3"], ["3", "4"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT distinct_count, null_count, stats_ver FROM mysql.stats_histograms \
                 WHERE table_id = {table_id} AND is_index = 0 AND hist_id = {virtual_column_id}"
            ),
        )),
        [["0", "0", "0"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT count(*) FROM mysql.stats_buckets WHERE table_id = {table_id} \
                 AND is_index = 0 AND hist_id = {virtual_column_id}"
            ),
        )),
        [["0"]]
    );

    // Pinned `TestDDLHistogram` performs both additions in one
    // ActionMultiSchemaChange. Go publishes one ActionAddColumn event for
    // each applied sub-job, in SQL order, and the statistics subscriber
    // initializes both histograms.
    rows(
        &mut session,
        "ALTER TABLE stats_add_column \
         ADD COLUMN f VARCHAR(15) DEFAULT '123', \
         ADD COLUMN g VARCHAR(15) DEFAULT '123'",
    );
    drain_stats_ddl_events(&stack.factory, &mut session);
    let (f_id, g_id) = {
        let catalog = stack.factory.catalog.get_latest();
        let table = catalog
            .table_info_by_name("test", "stats_add_column")
            .expect("altered table is published")
            .1;
        let table = table.read();
        let column_id = |name: &str| {
            table
                .columns
                .iter_deref()
                .find(|column| column.read().name.lowercase() == name)
                .expect("added column is published")
                .read()
                .id
        };
        (column_id("f"), column_id("g"))
    };
    for column_id in [f_id, g_id] {
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!(
                    "SELECT distinct_count, null_count, tot_col_size, stats_ver \
                     FROM mysql.stats_histograms WHERE table_id = {table_id} \
                     AND is_index = 0 AND hist_id = {column_id}"
                ),
            )),
            [["1", "0", "9", "0"]]
        );
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!(
                    "SELECT repeats, count, lower_bound, upper_bound \
                     FROM mysql.stats_buckets WHERE table_id = {table_id} \
                     AND is_index = 0 AND hist_id = {column_id}"
                ),
            )),
            [["3", "3", "123", "123"]]
        );
    }

    rows(
        &mut session,
        &format!(
            "DELETE FROM mysql.stats_buckets WHERE table_id = {table_id} \
             AND is_index = 0 AND hist_id = {f_id}"
        ),
    );
    rows(
        &mut session,
        &format!(
            "DELETE FROM mysql.stats_histograms WHERE table_id = {table_id} \
             AND is_index = 0 AND hist_id = {f_id}"
        ),
    );
    rows(
        &mut session,
        "ALTER TABLE stats_add_column \
         ADD COLUMN IF NOT EXISTS f VARCHAR(15) DEFAULT '123', \
         ADD COLUMN h VARCHAR(15) DEFAULT '123'",
    );
    drain_stats_ddl_events(&stack.factory, &mut session);
    let h_id = stack
        .factory
        .catalog
        .get_latest()
        .table_info_by_name("test", "stats_add_column")
        .expect("altered table is published")
        .1
        .read().columns
        .iter_deref()
        .find(|column| column.read().name.lowercase() == "h")
        .expect("applied sub-job column is published")
        .read()
        .id;
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT count(*) FROM mysql.stats_histograms WHERE table_id = {table_id} \
                 AND is_index = 0 AND hist_id = {f_id}"
            ),
        )),
        [["0"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT distinct_count, null_count, tot_col_size, stats_ver \
                 FROM mysql.stats_histograms WHERE table_id = {table_id} \
                 AND is_index = 0 AND hist_id = {h_id}"
            ),
        )),
        [["1", "0", "9", "0"]]
    );
}

/// Pinned `TestSystemTableDDLHasNoEvent`: `asyncNotifyEvent` suppresses every
/// stats subscriber event for `metadef.IsMemOrSysDB`, so system-table DDL
/// changes schema metadata but never creates or refreshes stats rows.
#[test]
fn system_table_ddl_does_not_publish_statistics_events_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(76))
        .expect("session opens");
    let assert_no_stats = |session: &mut Session, ids: &[i64]| {
        let ids = ids.iter().map(i64::to_string).collect::<Vec<_>>().join(",");
        assert_eq!(
            displayed(rows(
                session,
                &format!("SELECT count(*) FROM mysql.stats_meta WHERE table_id IN ({ids})"),
            )),
            [["0"]]
        );
    };

    rows(&mut session, "CREATE TABLE mysql.stats_no_event (a INT)");
    let old_id = stack
        .factory
        .catalog
        .get_latest()
        .table_info_by_name("mysql", "stats_no_event")
        .expect("system table is published")
        .1
        .read().id;
    assert_no_stats(&mut session, &[old_id]);
    rows(
        &mut session,
        "ALTER TABLE mysql.stats_no_event ADD COLUMN b INT",
    );
    rows(
        &mut session,
        "ALTER TABLE mysql.stats_no_event ADD COLUMN c INT, ADD COLUMN d INT",
    );
    rows(
        &mut session,
        "ALTER TABLE mysql.stats_no_event MODIFY COLUMN a BIGINT",
    );
    assert_no_stats(&mut session, &[old_id]);
    rows(&mut session, "TRUNCATE TABLE mysql.stats_no_event");
    let new_id = stack
        .factory
        .catalog
        .get_latest()
        .table_info_by_name("mysql", "stats_no_event")
        .expect("truncated system table is published")
        .1
        .read().id;
    assert_ne!(new_id, old_id);
    assert_no_stats(&mut session, &[old_id, new_id]);
    rows(&mut session, "DROP TABLE mysql.stats_no_event");
    assert_no_stats(&mut session, &[old_id, new_id]);

    rows(
        &mut session,
        "CREATE TABLE mysql.stats_no_event_part (a INT) PARTITION BY RANGE (a) (\
         PARTITION p0 VALUES LESS THAN (10))",
    );
    let (logical_id, p0_id) = {
        let catalog = stack.factory.catalog.get_latest();
        let table = catalog
            .table_info_by_name("mysql", "stats_no_event_part")
            .expect("partitioned system table is published")
            .1;
        let table = table.read();
        let definitions = table
            .partition
            .as_ref()
            .expect("partition metadata exists")
            .read()
            .definitions
            .snapshot();
        (table.id, definitions[0].id)
    };
    rows(
        &mut session,
        "ALTER TABLE mysql.stats_no_event_part ADD PARTITION (\
         PARTITION p1 VALUES LESS THAN MAXVALUE)",
    );
    let p1_id = {
        let catalog = stack.factory.catalog.get_latest();
        let table = catalog
            .table_info_by_name("mysql", "stats_no_event_part")
            .expect("partitioned system table is published")
            .1;
        let table = table.read();
        let definitions = table
            .partition
            .as_ref()
            .expect("partition metadata exists")
            .read()
            .definitions
            .snapshot();
        definitions
            .into_iter()
            .find(|definition| definition.name.lowercase() == "p1")
            .expect("p1 exists")
            .id
    };
    assert_no_stats(&mut session, &[logical_id, p0_id, p1_id]);
    rows(
        &mut session,
        "ALTER TABLE mysql.stats_no_event_part TRUNCATE PARTITION p1",
    );
    let replacement_p1_id = {
        let catalog = stack.factory.catalog.get_latest();
        let table = catalog
            .table_info_by_name("mysql", "stats_no_event_part")
            .expect("partitioned system table is published")
            .1;
        let table = table.read();
        let definitions = table
            .partition
            .as_ref()
            .expect("partition metadata exists")
            .read()
            .definitions
            .snapshot();
        definitions
            .into_iter()
            .find(|definition| definition.name.lowercase() == "p1")
            .expect("p1 exists")
            .id
    };
    assert_ne!(replacement_p1_id, p1_id);
    assert_no_stats(&mut session, &[logical_id, p0_id, p1_id, replacement_p1_id]);
    rows(
        &mut session,
        "ALTER TABLE mysql.stats_no_event_part DROP PARTITION p1",
    );
    rows(&mut session, "DROP TABLE mysql.stats_no_event_part");
    assert_no_stats(&mut session, &[logical_id, p0_id, p1_id, replacement_p1_id]);
}

/// Go writes a modify-column event only when the rows are rewritten
/// (`doModifyColumnTypeWithData`); a change of metadata alone and a RENAME
/// COLUMN write none, so the statistics subscriber recreates nothing for
/// them (oracle: `mysql.tidb_ddl_notifier` gains a row for `INT -> VARCHAR`
/// only).
#[test]
fn modify_column_ddl_events_follow_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    // The test counts `mysql.tidb_ddl_notifier` rows, which the domain's own
    // notifier deletes once its subscribers have handled them.
    stop_domain_ddl_notifier(&stack.factory);
    let mut session = stack
        .factory
        .open_session(session_context(74))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE TABLE stats_modify_column (a INT DEFAULT 7)",
    );
    drain_stats_ddl_events(&stack.factory, &mut session);
    rows(
        &mut session,
        "INSERT INTO stats_modify_column VALUES (DEFAULT),(DEFAULT),(DEFAULT)",
    );
    rows(&mut session, "ANALYZE TABLE stats_modify_column");
    let (table_id, column_id) = {
        let catalog = stack.factory.catalog.get_latest();
        let table = catalog
            .table_info_by_name("test", "stats_modify_column")
            .expect("created table is published")
            .1;
        let table = table.read();
        (
            table.id,
            table.columns.get(0).expect("column exists").read().id,
        )
    };

    let remove_column_stats = |session: &mut Session| {
        rows(
            session,
            &format!(
                "DELETE FROM mysql.stats_buckets WHERE table_id = {table_id} \
                 AND is_index = 0 AND hist_id = {column_id}"
            ),
        );
        rows(
            session,
            &format!(
                "DELETE FROM mysql.stats_histograms WHERE table_id = {table_id} \
                 AND is_index = 0 AND hist_id = {column_id}"
            ),
        );
    };
    let notifier_rows = |session: &mut Session| {
        displayed(rows(
            session,
            "SELECT count(*) FROM mysql.tidb_ddl_notifier",
        ))[0][0]
            .clone()
    };
    let assert_absent = |session: &mut Session| {
        assert_eq!(
            displayed(rows(
                session,
                &format!(
                    "SELECT count(*) FROM mysql.stats_histograms WHERE table_id = {table_id} \
                     AND is_index = 0 AND hist_id = {column_id}"
                ),
            )),
            [["0"]]
        );
    };

    remove_column_stats(&mut session);
    let before = notifier_rows(&mut session);
    rows(
        &mut session,
        "ALTER TABLE stats_modify_column MODIFY COLUMN a BIGINT",
    );
    assert_eq!(notifier_rows(&mut session), before);
    drain_stats_ddl_events(&stack.factory, &mut session);
    assert_absent(&mut session);

    let before = notifier_rows(&mut session);
    rows(
        &mut session,
        "ALTER TABLE stats_modify_column RENAME COLUMN a TO b",
    );
    assert_eq!(notifier_rows(&mut session), before);
    drain_stats_ddl_events(&stack.factory, &mut session);
    assert_absent(&mut session);

    // A rewrite does write the event.
    let before = notifier_rows(&mut session);
    rows(
        &mut session,
        "ALTER TABLE stats_modify_column MODIFY COLUMN b VARCHAR(10)",
    );
    let after: i64 = notifier_rows(&mut session).parse().unwrap();
    assert_eq!(after, before.parse::<i64>().unwrap() + 1);
}

/// Go `ALTER TABLE ... [FORCE] AUTO_INCREMENT = n` over the real node.
///
/// The DDL half -- the stored `AutoIncID` and the counter key -- is pinned in
/// `tidb-exec`. What only this stack can show is that the node's LIVE
/// allocator notices: it caches a reserved range that outlives schema
/// reloads by design, so a rebase that moved only the meta keys would leave
/// the next INSERT allocating from the range reserved before the change, and
/// the statement would look like it did nothing.
#[test]
fn a_rebased_auto_increment_reaches_the_next_insert() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(13))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.seq (id bigint primary key auto_increment, v int)",
    );
    rows(&mut session, "INSERT INTO test.seq (v) VALUES (1)");

    // FORCE sets the base exactly, even below the counter the first
    // reservation already wrote.
    rows(
        &mut session,
        "ALTER TABLE test.seq FORCE AUTO_INCREMENT = 500",
    );
    rows(&mut session, "INSERT INTO test.seq (v) VALUES (2)");
    assert_eq!(
        displayed(rows(&mut session, "SELECT id FROM test.seq ORDER BY v")),
        [["1"], ["500"]],
        "the forced base is what the next INSERT allocates"
    );

    // Without FORCE the base is floored at the allocator's next id, and Go
    // says so rather than silently doing something else. The reservation
    // taken above ends at 500 + the default step, so the floor is well past
    // the 5 that was asked for.
    rows(&mut session, "ALTER TABLE test.seq AUTO_INCREMENT = 5");
    let warnings = displayed(rows(&mut session, "SHOW WARNINGS"));
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert_eq!(warnings[0][0], "Warning");
    assert_eq!(warnings[0][1], "1105");
    assert!(
        warnings[0][2].starts_with("Can't reset AUTO_INCREMENT to 5 without FORCE option, using "),
        "{}",
        warnings[0][2]
    );
}

/// Go `ShowDDLExec.Next` (`executor/show_ddl.go`): six columns describing the
/// DDL owner and this node.
///
/// `SCHEMA_VER` is the version this node currently follows, so it moves when a
/// catalog change lands. The owner columns name THIS node, which is what a
/// single-node deployment reports and what this node truthfully is: it runs no
/// election, and every catalog change it accepts, it performs itself. The two
/// job-list columns are structurally empty because a change is published in
/// one transaction rather than queued, so no later statement can observe one
/// in flight.
#[test]
fn admin_show_ddl_reports_this_node_and_the_followed_version() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(17))
        .expect("session opens");

    let before = displayed(rows(&mut session, "ADMIN SHOW DDL"));
    assert_eq!(before.len(), 1);
    let row = &before[0];
    let version: i64 = row[0].parse().expect("SCHEMA_VER is an integer");
    // Go's DDL_ID is a uuid, and the owner and self are the same node here.
    assert_eq!(row[1].len(), 36, "OWNER_ID is a uuid: {}", row[1]);
    assert_eq!(row[1], row[4], "this node is its own owner");
    assert!(
        row[2].contains(':'),
        "OWNER_ADDRESS is host:port: {}",
        row[2]
    );
    assert_eq!(row[3], "", "no job is ever observably in flight");
    assert_eq!(row[5], "", "and so no query is either");

    // The reported version follows the catalog, so a change moves it.
    rows(
        &mut session,
        "CREATE TABLE test.ddl_probe (id int primary key)",
    );
    let after = displayed(rows(&mut session, "ADMIN SHOW DDL"));
    let moved: i64 = after[0][0].parse().expect("SCHEMA_VER is an integer");
    assert!(
        moved > version,
        "a published change moves SCHEMA_VER: {version} -> {moved}"
    );
    assert_eq!(after[0][4], row[4], "the node identity is stable");

    // The identity is the one TIDB_SERVERS_INFO reports for this node.
    let servers = displayed(rows(
        &mut session,
        "SELECT DDL_ID FROM information_schema.tidb_servers_info",
    ));
    assert_eq!(servers, [[row[4].clone()]]);
}

/// THE INVARIANT: every `TableInfo` this node's DDL publishes, its own
/// catalog loader must load.
///
/// Breaking it produces the worst shape a DDL can take -- the CREATE reports
/// success, a later CREATE of the same name reports 1050, and every read
/// reports 1146 -- and it broke for real on
/// `DATETIME(n) DEFAULT CURRENT_TIMESTAMP(n)`. Neither half's own tests could
/// catch that: the DDL wrote correct metadata and the loader correctly
/// refused what it was given. Only the two together show it.
///
/// The shapes below are the column and key forms this node admits. A new
/// admitted shape belongs here.
#[test]
fn every_shape_the_ddl_admits_the_loader_loads() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(29))
        .expect("session opens");

    const SHAPES: &[&str] = &[
        // Literal defaults, one per storage family.
        "a int DEFAULT 5",
        "a varchar(10) DEFAULT 'x'",
        "a decimal(10,2) DEFAULT 1.5",
        "a double DEFAULT 1.5",
        "a bit(8) DEFAULT b'101'",
        "a enum('x','y') DEFAULT 'y'",
        "a set('p','q') DEFAULT 'q'",
        "a date DEFAULT '2020-01-01'",
        "a time(3) DEFAULT '01:02:03.400'",
        "a year DEFAULT 2020",
        "a binary(4) DEFAULT 'ab'",
        "a char(3) CHARACTER SET latin1 DEFAULT 'q'",
        "a int UNSIGNED ZEROFILL DEFAULT 7",
        "a json",
        "a text",
        // The clock marker, at every fsp -- the shape that broke.
        "a timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "a datetime DEFAULT CURRENT_TIMESTAMP",
        "a datetime(3) DEFAULT CURRENT_TIMESTAMP(3)",
        "a timestamp(6) DEFAULT CURRENT_TIMESTAMP(6)",
        // Keys and indexes.
        "id bigint PRIMARY KEY AUTO_INCREMENT, a int, KEY k(a)",
        "id varchar(20) PRIMARY KEY, a int",
        "id bigint, a int, PRIMARY KEY (id, a)",
        "id bigint PRIMARY KEY AUTO_RANDOM",
        "a int, b int, UNIQUE KEY u(a,b)",
        "a int, KEY k(a) COMMENT 'c'",
        "a int, KEY k(a) INVISIBLE",
        "id bigint PRIMARY KEY NONCLUSTERED, a int",
        "a int COMMENT 'col comment'",
    ];

    for (index, shape) in SHAPES.iter().enumerate() {
        let name = format!("test.shape{index}");
        rows(&mut session, &format!("CREATE TABLE {name} ({shape})"));
        // The read is the assertion: a table the loader dropped answers 1146
        // here while still colliding with a second CREATE.
        let loaded = displayed(rows(
            &mut session,
            &format!(
                "SELECT count(*) FROM information_schema.tables \
                 WHERE table_schema = 'test' AND table_name = 'shape{index}'"
            ),
        ));
        assert_eq!(
            loaded,
            [["1"]],
            "the DDL published `{shape}` and the loader dropped it"
        );
        // And it is actually usable, not merely listed.
        rows(&mut session, &format!("SELECT * FROM {name}"));
    }
}

/// Go `fieldTypeFromPBColumn`: the coprocessor rebuilds each column's type
/// from the DAG request, and the type CODE alone is not the type.
///
/// Dropping the rest decoded the stored bytes under the wrong rules. An
/// `INT UNSIGNED` holding 4294967295 came back as -1 on the SCAN path while
/// the point-get path -- which builds its types from the catalog -- returned
/// the stored value: one table, two paths, two answers. Carrying the flag
/// then exposed the second half, since the region's filter understood only
/// `Datum::Int` and silently dropped every row of an unsigned column.
#[test]
fn unsigned_columns_survive_the_coprocessor_scan() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(31))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.un (id int primary key, a tinyint unsigned, \
         b smallint unsigned, d int unsigned, e bigint unsigned)",
    );
    rows(
        &mut session,
        "INSERT INTO test.un VALUES (1, 255, 65535, 4294967295, 18446744073709551615), \
         (2, 1, 1, 1, 1)",
    );

    // The scan path returns the stored values, not their signed
    // reinterpretation.
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT a, b, d, e FROM test.un WHERE id > 0"
        )),
        [
            ["255", "65535", "4294967295", "18446744073709551615"],
            ["1", "1", "1", "1"],
        ]
    );
    // And it agrees with the point-get path, which never lost the flag.
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT a, d, e FROM test.un WHERE id = 1"
        )),
        [["255", "4294967295", "18446744073709551615"]]
    );

    // A pushed-down predicate over an unsigned column compares in the
    // unsigned domain, including past i64::MAX.
    for (predicate, expected) in [
        ("d > 2", "1"),
        ("a < 255", "1"),
        ("e > 9223372036854775807", "1"),
        ("e = 18446744073709551615", "1"),
        ("a IN (255, 1)", "2"),
    ] {
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!("SELECT count(*) FROM test.un WHERE {predicate}"),
            )),
            [[expected.to_owned()]],
            "`{predicate}` over the scan path"
        );
    }
}

/// Go `handleUnsignedCol`: a NEGATIVE bound on an unsigned column is either
/// rewritten to `>= 0` or makes the range invalid, and an invalid range folds
/// to a `TableDual` (`crate::explain`'s divergence 9).
///
/// The distinction that matters is negative VALUE, not negative-looking
/// predicate: `a < 0` compares against zero, which Go treats as non-negative
/// and rewrites nothing, so it keeps a real `IndexRangeScan`. Mis-reading
/// that cost an incorrect doc edit once; it is pinned here so the next reader
/// gets the boundary from a test rather than from prose.
#[test]
fn a_negative_bound_on_an_unsigned_column_follows_gos_rewrite() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(61))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.ud (id int primary key, a int unsigned, KEY ka(a))",
    );
    rows(
        &mut session,
        "INSERT INTO test.ud VALUES (1,0),(2,5),(3,4294967295)",
    );

    let plan_of = |session: &mut _, sql: &str| {
        displayed(rows(session, sql))
            .into_iter()
            .map(|row| row.join(" "))
            .collect::<Vec<_>>()
            .join("\n")
    };

    // A negative value with LT/LE/EQ makes the range invalid -> TableDual.
    for predicate in ["a < -1", "a <= -1", "a = -1"] {
        let plan = plan_of(
            &mut session,
            &format!("EXPLAIN SELECT id FROM test.ud USE INDEX(ka) WHERE {predicate}"),
        );
        assert!(
            plan.contains("TableDual"),
            "`{predicate}` should fold to a dual:\n{plan}"
        );
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!("SELECT count(*) FROM test.ud WHERE {predicate}")
            )),
            [["0"]],
        );
    }

    // A negative value with GT/GE/NE is rewritten to `>= 0`, so every row
    // qualifies rather than none.
    for predicate in ["a > -1", "a >= -5", "a <> -1"] {
        let plan = plan_of(
            &mut session,
            &format!("EXPLAIN SELECT id FROM test.ud USE INDEX(ka) WHERE {predicate}"),
        );
        assert!(
            plan.contains("range:[0"),
            "`{predicate}` should start at 0:\n{plan}"
        );
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!("SELECT count(*) FROM test.ud WHERE {predicate}")
            )),
            [["3"]],
        );
    }

    // `a < 0` is NOT a negative value: Go rewrites nothing, so the range
    // survives as a real scan that happens to find no rows.
    let plan = plan_of(
        &mut session,
        "EXPLAIN SELECT id FROM test.ud USE INDEX(ka) WHERE a < 0",
    );
    assert!(plan.contains("IndexRangeScan"), "{plan}");
    assert!(!plan.contains("TableDual"), "{plan}");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM test.ud WHERE a < 0"
        )),
        [["0"]],
    );
}

/// `CREATE VIEW` over the real embedded store creates the view, and it reads
/// back.
///
/// This is the path `-store unistore` actually serves, and
/// it had NO coverage: the mock-seam modules in this directory serve
/// `kv_client: None`, and the pipeline session's own `CREATE VIEW` wire test
/// exercises a different session. That blind spot is how seven commits landed
/// on a working feature and broke it in silence.
///
/// Bisected, one build and one live server per point: `9b893f4abd` still
/// creates the view; `b1f979cc76` ("rust: complete unistore transaction batch
/// get") answers
///
/// ```text
/// ERROR 1105 (HY000): table bytes failed to decode
/// ```
///
/// and leaves no view behind. The real cause is
/// `Storage("Backend(\"query deadline exceeded\")")`, and the shape of the
/// failure is the clue: this test sits for ~20s before reporting, so the
/// coprocessor request never COMPLETES and the wait runs out. It is a request
/// that goes unanswered, not a deadline that was mis-set.
#[test]
fn a_view_over_the_coprocessor_is_created_and_reads_back() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(9))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.vsrc (id int primary key, v int)",
    );
    rows(
        &mut session,
        "INSERT INTO test.vsrc VALUES (1, 10), (2, 20)",
    );
    rows(
        &mut session,
        "CREATE VIEW test.vview AS SELECT id FROM test.vsrc WHERE id > 1",
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT id FROM test.vview")),
        [["2"]],
        "the view reads back its defining query"
    );
}

/// Two sessions racing their `CREATE TABLE`s both succeed -- sysbench's
/// parallel `prepare` (`--threads=4 --tables=2`) is exactly this shape, and
/// it was the workload that found the gap: `sbtest2` never existed and every
/// later statement on it failed.
///
/// Every catalog change writes `SchemaVersionKey`, so concurrent DDL is a
/// GUARANTEED optimistic write conflict. Go runs each DDL meta write under
/// `kv.RunInNewTxn(retryable=true)` (`pkg/ddl/ddl.go`), which rolls the
/// loser back and re-runs it from a fresh snapshot -- re-read, re-plan,
/// re-commit -- so no client ever sees the conflict. Before
/// `commit_cluster_ddl` carried that loop, the loser here surfaced error
/// 1105 ("... refuses to interleave: ... WriteConflict").
#[test]
fn concurrent_creates_both_succeed_like_gos_ddl_queue() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let barrier = std::sync::Barrier::new(2);
    std::thread::scope(|scope| {
        for worker in 0u64..2 {
            let barrier = &barrier;
            scope.spawn(move || {
                let mut session = factory
                    .open_session(session_context(70 + worker))
                    .expect("session opens");
                barrier.wait();
                for table in 0..4 {
                    rows(
                        &mut session,
                        &format!(
                            "CREATE TABLE test.race_{worker}_{table} (id int primary key, v int)"
                        ),
                    );
                }
            });
        }
    });
    // Every one of the eight racing tables exists and serves reads and
    // writes: the retry made both sessions' schedules land, in some order.
    let mut session = factory
        .open_session(session_context(79))
        .expect("session opens");
    for worker in 0..2 {
        for table in 0..4 {
            rows(
                &mut session,
                &format!("INSERT INTO test.race_{worker}_{table} VALUES (1, 10)"),
            );
            assert_eq!(
                displayed(rows(
                    &mut session,
                    &format!("SELECT v FROM test.race_{worker}_{table} WHERE id = 1"),
                )),
                [["10"]],
                "table race_{worker}_{table} must exist and answer"
            );
        }
    }
}

#[test]
fn prepared_select_replans_after_another_session_changes_columns() {
    use crate::domain::resultset_source::ResultSetSource;
    use tidb_param::PreparedValue;

    let (stack, _users) = cop_backed_stack();
    let mut reader = stack
        .factory
        .open_session(session_context(90))
        .expect("reader");
    let mut ddl = stack
        .factory
        .open_session(session_context(91))
        .expect("DDL session");
    // Table count must not select a different SQL engine or stop schema refresh.
    for name in ["schema_anchor_a", "schema_anchor_b", "schema_anchor_c"] {
        rows(
            &mut ddl,
            &format!("CREATE TABLE test.{name} (id INT PRIMARY KEY)"),
        );
    }
    // Equality exercises the retained point reader; range exercises a physical SELECT.
    for (table, comparison) in [
        ("prepared_point_schema", "="),
        ("prepared_range_schema", ">="),
    ] {
        rows(
            &mut ddl,
            &format!("CREATE TABLE test.{table} (id INT PRIMARY KEY, v INT)"),
        );
        rows(
            &mut ddl,
            &format!("INSERT INTO test.{table} VALUES (1, 10)"),
        );
        let statement = reader
            .node_conn().prepare_general(&format!(
                "SELECT * FROM test.{table} WHERE id {comparison} ?"
            ))
            .expect("prepare");
        let select = |session: &mut Session| {
            let crate::domain::session_api::GeneralExecuteOutcome::Rows(mut result) = session
                .node_conn().execute_general(&statement, &[PreparedValue::SignedLongLong(1)])
                .expect("execute")
            else {
                panic!("SELECT returns rows")
            };
            let source = result.source();
            let mut selected = Vec::new();
            loop {
                let batch = source.next_batch(8).expect("read rows");
                if batch.is_empty() {
                    break;
                }
                selected.extend(batch);
            }
            source.finish().expect("finish");
            source.close().expect("close");
            displayed(selected)
        };
        assert_eq!(select(&mut reader), [["1", "10"]]);
        assert_eq!(select(&mut reader), [["1", "10"]]);
        rows(
            &mut ddl,
            &format!("ALTER TABLE test.{table} ADD COLUMN extra INT DEFAULT 7"),
        );
        assert_eq!(select(&mut reader), [["1", "10", "7"]]);
        rows(
            &mut ddl,
            &format!("ALTER TABLE test.{table} DROP COLUMN extra"),
        );
        assert_eq!(select(&mut reader), [["1", "10"]]);
    }
}

#[test]
fn prepared_update_replans_after_another_session_adds_a_column() {
    use tidb_param::PreparedValue;

    let (stack, _users) = cop_backed_stack();
    let mut writer = stack
        .factory
        .open_session(session_context(90))
        .expect("writer");
    let mut ddl = stack
        .factory
        .open_session(session_context(91))
        .expect("DDL session");
    rows(
        &mut writer,
        "CREATE TABLE test.prepared_schema (id INT PRIMARY KEY, v INT)",
    );
    rows(
        &mut writer,
        "INSERT INTO test.prepared_schema VALUES (1, 10)",
    );
    let statement = writer
        .node_conn().prepare_general("UPDATE test.prepared_schema SET v = v + 1 WHERE id = ?")
        .expect("prepare");
    writer
        .node_conn().execute_general(&statement, &[PreparedValue::SignedLongLong(1)])
        .expect("populate prepared cache");
    // Keep the prepared handle alive while a separate connection publishes DDL.
    rows(
        &mut ddl,
        "ALTER TABLE test.prepared_schema ADD COLUMN extra INT DEFAULT 7",
    );
    writer
        .node_conn().execute_general(&statement, &[PreparedValue::SignedLongLong(1)])
        .expect("execute against the new schema");
    assert_eq!(
        displayed(rows(&mut writer, "SELECT * FROM test.prepared_schema")),
        [["1", "12", "7"]]
    );
    rows(
        &mut ddl,
        "ALTER TABLE test.prepared_schema DROP COLUMN extra",
    );
    writer
        .node_conn().execute_general(&statement, &[PreparedValue::SignedLongLong(1)])
        .expect("execute after dropping the column");
    assert_eq!(
        displayed(rows(&mut writer, "SELECT * FROM test.prepared_schema")),
        [["1", "13"]]
    );
}

/// Go `TestCreateTableWithForeignKeyMetaInfo`/`checkTableForeignKeysValid`:
/// a cluster CREATE TABLE resolves the parent through the catalog instead of
/// refusing every FOREIGN KEY with 1824.
#[test]
fn cluster_create_table_foreign_key_resolves_parent_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut s = stack.factory.open_session(session_context(901)).expect("session");
    let code = |s: &mut crate::Session, sql: &str| {
        s.node_conn()
            .execute_write(sql)
            .err()
            .map(|error| error.code)
    };
    rows(
        &mut s,
        "CREATE TABLE test.fk_p (id INT PRIMARY KEY, u INT, KEY iu(u))",
    );
    assert_eq!(
        code(&mut s, "CREATE TABLE test.fk_c (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES fk_p(id) ON DELETE CASCADE)"),
        None
    );
    let meta = rows(&mut s, "SHOW CREATE TABLE test.fk_c");
    let ddl = displayed(meta)[0][1].clone();
    assert!(ddl.contains("CONSTRAINT `fk_1` FOREIGN KEY (`pid`) REFERENCES `fk_p` (`id`) ON DELETE CASCADE"), "{ddl}");
    assert!(ddl.contains("KEY `fk_1` (`pid`)"), "auto FK index: {ddl}");
    // Missing parent: 1824 (checks on); accepted with checks off.
    assert_eq!(
        code(&mut s, "CREATE TABLE test.fk_c2 (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES fk_none(id))"),
        Some(1824)
    );
    // Incompatible types: 3780; parent column without index: 1822;
    // missing parent column: 3734.
    assert_eq!(
        code(&mut s, "CREATE TABLE test.fk_c3 (id INT PRIMARY KEY, pid VARCHAR(10), FOREIGN KEY (pid) REFERENCES fk_p(id))"),
        Some(3780)
    );
    rows(&mut s, "CREATE TABLE test.fk_q (id INT PRIMARY KEY, v INT)");
    assert_eq!(
        code(&mut s, "CREATE TABLE test.fk_c4 (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES fk_q(v))"),
        Some(1822)
    );
    assert_eq!(
        code(&mut s, "CREATE TABLE test.fk_c5 (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES fk_q(nope))"),
        Some(3734)
    );
    s.node_conn()
        .execute_write("SET foreign_key_checks = 0")
        .expect("fk checks off");
    assert_eq!(
        code(&mut s, "CREATE TABLE test.fk_c6 (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES fk_none(id))"),
        None
    );
}

#[test]
fn historical_read_batch_schema_data_and_timestamp_lifetime() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(914)).unwrap();
    rows(&mut session, "CREATE DATABASE history_batch");
    rows(&mut session, "USE history_batch");
    rows(&mut session, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    rows(&mut session, "INSERT INTO t VALUES (1,10)");
    session.node_conn().control_transaction("BEGIN").unwrap();
    rows(&mut session, "SELECT * FROM t");
    let ts: u64 = displayed(rows(&mut session, "SELECT @@tidb_current_ts"))[0][0]
        .parse()
        .unwrap();
    session.node_conn().control_transaction("COMMIT").unwrap();
    std::thread::sleep(Duration::from_millis(5));
    rows(&mut session, "UPDATE t SET v=20 WHERE id=1");
    rows(&mut session, "ALTER TABLE t ADD COLUMN extra INT DEFAULT 7");
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!("SELECT * FROM t AS OF TIMESTAMP {ts}")
        )),
        vec![vec!["1", "10"]]
    );
    assert!(!tidb_txnkv::ACTIVE_START_TS.snapshot().contains(&ts));
    session
        .node_conn()
        .control_transaction(&format!("START TRANSACTION READ ONLY AS OF TIMESTAMP {ts}"))
        .unwrap();
    assert!(tidb_txnkv::ACTIVE_START_TS.snapshot().contains(&ts));
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t")),
        vec![vec!["1", "10"]]
    );
    assert!(
        tidb_txnkv::ACTIVE_START_TS.snapshot().contains(&ts),
        "transaction protects time between reads"
    );
    for sql in [
        format!("SET tidb_snapshot='{ts}'"),
        "SET tidb_snapshot=DEFAULT".into(),
    ] {
        assert_eq!(session.node_conn().execute(&sql).err().unwrap().code, 1568);
    }
    assert_eq!(
        session
            .node_conn()
            .execute("SET tidb_snapshot=JSON_EXTRACT('invalid', '$')")
            .err()
            .unwrap()
            .code,
        3140,
        "SET evaluates before refusing transaction changes"
    );
    assert!(session
        .node_conn()
        .execute_write("UPDATE t SET v=30")
        .is_err());
    assert!(session
        .node_conn()
        .execute("SELECT * FROM t FOR UPDATE")
        .is_err());
    assert!(session
        .node_conn()
        .execute("EXPLAIN ANALYZE UPDATE t SET v=30")
        .is_err());
    session.node_conn().control_transaction("ROLLBACK").unwrap();
    assert!(!tidb_txnkv::ACTIVE_START_TS.snapshot().contains(&ts));
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t")),
        vec![vec!["1", "20", "7"]]
    );
    let future = ts + (1_000_000 << 18);
    assert!(session
        .node_conn()
        .execute(&format!("SELECT * FROM t AS OF TIMESTAMP {future}"))
        .is_err());
    assert!(!tidb_txnkv::ACTIVE_START_TS.snapshot().contains(&future));
    assert!(session
        .node_conn()
        .execute(&format!("SET tidb_snapshot='{ts}'"))
        .err()
        .unwrap()
        .message
        .contains("can not get 'tikv_gc_safe_point'"));
    rows(
        &mut session,
        "INSERT INTO mysql.tidb VALUES ('tikv_gc_safe_point', '20060102-15:04:05.000 -0700', '')",
    );
    rows(&mut session, &format!("SET tidb_snapshot='{ts}'"));
    assert!(
        !tidb_txnkv::ACTIVE_START_TS.snapshot().contains(&ts),
        "idle snapshot SET retains schema, not an active native timestamp"
    );
    assert!(session
        .node_conn()
        .execute(&format!("SET tidb_snapshot='{future}'"))
        .is_err());
    assert_eq!(session.vars().snapshot_ts(), ts);
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t")),
        vec![vec!["1", "10"]]
    );
    assert!(session
        .node_conn()
        .execute_write("UPDATE t SET v=30")
        .is_err());
    assert!(session
        .node_conn()
        .execute("EXPLAIN ANALYZE UPDATE t SET v=30")
        .is_err());
    assert!(session
        .node_conn()
        .execute_write("ALTER TABLE t ADD COLUMN forbidden INT")
        .is_err());
    assert!(session
        .node_conn()
        .execute_write("CREATE DATABASE forbidden_history")
        .is_err());
    // Go `handlePessimisticSelectForUpdate`: a locking read is refused only
    // where its lock is taken in a pessimistic transaction; EXPLAIN without
    // ANALYZE runs nothing. Oracle: real TiDB under the same snapshot.
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t FOR UPDATE")),
        vec![vec!["1", "10"]]
    );
    rows(&mut session, "EXPLAIN UPDATE t SET v=30");
    session.node_conn().control_transaction("BEGIN PESSIMISTIC").unwrap();
    let error = session
        .node_conn()
        .execute("SELECT * FROM t FOR UPDATE")
        .err()
        .expect("a pessimistic locking read under tidb_snapshot");
    assert_eq!(
        error.message,
        "can not execute write statement when 'tidb_snapshot' is set"
    );
    session.node_conn().control_transaction("ROLLBACK").unwrap();
    session.node_conn().control_transaction("BEGIN OPTIMISTIC").unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t FOR UPDATE")),
        vec![vec!["1", "10"]]
    );
    session.node_conn().control_transaction("ROLLBACK").unwrap();
    rows(&mut session, "SET autocommit=0");
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t")),
        vec![vec!["1", "10"]]
    );
    assert!(
        session.historical_read_ts().is_none(),
        "SnapshotTS is a statement pin"
    );
    rows(&mut session, "SET autocommit=1");
    rows(&mut session, "SET tidb_snapshot='' ");
    // A one-second staleness reads at @@timestamp - 1s, which Go's
    // `GoTimeToTS` floors to the millisecond, and @@timestamp reaches the
    // clock through a float64 (`math.Modf`): `<ts_ms>.925` arrives as
    // .924999952s. A nominal 1ms margin can therefore floor to `ts`'s own
    // millisecond, before the INSERT that committed in that millisecond.
    // Two milliseconds always clear it.
    rows(
        &mut session,
        &format!("SET timestamp={:.3}", (ts >> 18) as f64 / 1000.0 + 1.002),
    );
    rows(&mut session, "SET tidb_read_staleness=-1");
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t")),
        vec![vec!["1", "10"]]
    );
    rows(&mut session, "SET autocommit=0");
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t")),
        vec![vec!["1", "10"]]
    );
    let held = session.historical_read_ts().unwrap();
    assert!(tidb_txnkv::ACTIVE_START_TS.snapshot().contains(&held));
    session.node_conn().control_transaction("ROLLBACK").unwrap();
    assert!(!tidb_txnkv::ACTIVE_START_TS.snapshot().contains(&held));
    rows(&mut session, "UPDATE t SET v=20 WHERE id=1");
    assert!(
        session.historical_read_ts().is_none(),
        "a write starts an ordinary transaction"
    );
    session.node_conn().control_transaction("ROLLBACK").unwrap();
    rows(&mut session, "SET autocommit=1");
    rows(&mut session, "SET timestamp=0, tidb_read_staleness=0");
    rows(
        &mut session,
        &format!("PREPARE history_query FROM 'SELECT * FROM t AS OF TIMESTAMP {ts}'"),
    );
    assert_eq!(
        displayed(rows(&mut session, "EXECUTE history_query")),
        vec![vec!["1", "10"]]
    );
    rows(&mut session, "DEALLOCATE PREPARE history_query");
    session
        .node_conn()
        .control_transaction(&format!("START TRANSACTION READ ONLY AS OF TIMESTAMP {ts}"))
        .unwrap();
    session.node_conn().control_transaction("ROLLBACK").unwrap();
    assert!(
        !tidb_txnkv::ACTIVE_START_TS.snapshot().contains(&ts),
        "rollback without a read retires the metadata snapshot"
    );
    session
        .node_conn()
        .control_transaction(&format!("START TRANSACTION READ ONLY AS OF TIMESTAMP {ts}"))
        .unwrap();
    drop(session);
    assert!(
        !tidb_txnkv::ACTIVE_START_TS.snapshot().contains(&ts),
        "disconnect retires the historical owner"
    );
}

/// Go keeps the BDR role in the meta key `BDRRole`, and refuses a DDL its
/// role denies: the submitter (`SubmitBatch`'s `IsDenied`) and the ADD /
/// MODIFY COLUMN builders. A cluster node reads and writes the key in TiKV
/// meta, and its planner admits each change it publishes the same way.
#[test]
fn cluster_bdr_role_lives_in_meta_and_refuses_denied_ddl() {
    fn run(
        session: &mut Session,
        sql: &str,
    ) -> Result<(), tidb_store_driver_error::sql_query_error::SqlQueryError> {
        if session.node_conn().execute_write(sql)?.is_some() {
            return Ok(());
        }
        use crate::domain::resultset_source::ResultSetSource;
        let mut result = session.node_conn().execute(sql)?;
        let source = result.source();
        while !source.next_batch(8).expect("batch").is_empty() {}
        source.finish().expect("finish");
        source.close().expect("close");
        Ok(())
    }
    const DENIED: &str =
        "The operation is not allowed while the bdr role of this cluster is set to primary.";
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(931)).unwrap();
    run(&mut session, "create table test.bdr_t(a int)").unwrap();
    run(&mut session, "admin set bdr role primary").unwrap();
    // A second connection reads the role from meta, not from the first
    // connection's catalog.
    let mut other = stack.factory.open_session(session_context(932)).unwrap();
    assert_eq!(
        displayed(rows(&mut other, "admin show bdr role")),
        [["primary"]]
    );
    run(&mut session, "alter table test.bdr_t add column b int").unwrap();
    for sql in [
        "alter table test.bdr_t drop column b",
        "alter table test.bdr_t add column c int not null",
        "alter table test.bdr_t modify column a bigint",
        "create unique index u on test.bdr_t(a)",
        "truncate table test.bdr_t",
    ] {
        let error = run(&mut session, sql).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (8263, DENIED),
            "{sql}"
        );
    }
    run(&mut session, "create index i on test.bdr_t(a)").unwrap();
    run(
        &mut session,
        "alter table test.bdr_t modify column a int default 1",
    )
    .unwrap();
    // TiCDC's own DDL skips the submitter's admission.
    run(&mut session, "set @@tidb_cdc_write_source = 1").unwrap();
    run(&mut session, "alter table test.bdr_t drop column b").unwrap();
    run(&mut session, "set @@tidb_cdc_write_source = 0").unwrap();
    run(&mut session, "admin unset bdr role").unwrap();
    assert_eq!(displayed(rows(&mut other, "admin show bdr role")), [[""]]);
    run(&mut session, "truncate table test.bdr_t").unwrap();
    run(&mut session, "drop table test.bdr_t").unwrap();
}

/// Go's multi-schema change from `ddl/multi_schema_change.test`: the index,
/// default and table-option sub-jobs (`fillMultiSchemaInfo`), a MODIFY whose
/// rows change inside the bundle, a drop's `len(Columns)` check seeing the
/// columns other sub-jobs add, a later AUTO_INCREMENT sub-job adjusted
/// against the counter the earlier one left (a note), ADD PRIMARY KEY's NOT
/// NULL key columns, and an expression index's columns counted as relative.
/// This node refused every bundle with one of those sub-jobs.
#[test]
fn multi_schema_change_sub_jobs_run_as_go_runs_them() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(996)).unwrap();
    rows(&mut session, "CREATE TABLE test.t1 (a INT, b INT)");
    rows(&mut session, "INSERT INTO test.t1 VALUES (1, 2)");
    session
        .node_conn().execute_write("ALTER TABLE test.t1 DROP COLUMN a, DROP COLUMN b, ADD COLUMN c INT DEFAULT 3, ADD COLUMN d INT DEFAULT 4")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM test.t1")),
        [["3", "4"]]
    );

    rows(
        &mut session,
        "CREATE TABLE test.t2 (a INT, b INT, c INT, PRIMARY KEY (a) NONCLUSTERED, KEY i3 (b), KEY i (c))",
    );
    session
        .node_conn().execute_write("ALTER TABLE test.t2 DROP INDEX i3, DROP PRIMARY KEY, RENAME INDEX i TO i1, CHANGE COLUMN c d INT")
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT key_name, column_name FROM information_schema.tidb_indexes \
             WHERE table_schema = 'test' AND table_name = 't2'"
        )),
        [["i1", "d"]]
    );

    rows(
        &mut session,
        "CREATE TABLE test.t3 (a INT, b INT, c INT, KEY ib (b))",
    );
    rows(
        &mut session,
        "INSERT INTO test.t3 VALUES (1, 2, 3), (11, 22, 33)",
    );
    session
        .node_conn().execute_write("ALTER TABLE test.t3 MODIFY COLUMN a TINYINT AFTER c, MODIFY COLUMN b TINYINT, MODIFY COLUMN c TINYINT FIRST")
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT * FROM test.t3 USE INDEX (ib) ORDER BY b"
        )),
        [["3", "2", "1"], ["33", "22", "11"]]
    );
    rows(&mut session, "ADMIN CHECK TABLE test.t3");

    rows(
        &mut session,
        "CREATE TABLE test.t4 (a INT AUTO_INCREMENT PRIMARY KEY, b INT)",
    );
    session
        .node_conn()
        .execute_write("ALTER TABLE test.t4 AUTO_INCREMENT = 110, AUTO_INCREMENT = 90")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        [[
            "Note",
            "1105",
            "Can't reset AUTO_INCREMENT to 90 without FORCE option, using 110 instead"
        ]]
    );
    session
        .node_conn().execute_write("ALTER TABLE test.t4 MODIFY COLUMN b TINYINT, AUTO_INCREMENT = 200, COMMENT = 'abc', CHARSET = utf8mb4")
        .unwrap();
    rows(&mut session, "INSERT INTO test.t4 (b) VALUES (1)");
    assert_eq!(
        displayed(rows(&mut session, "SELECT a FROM test.t4")),
        [["200"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT table_comment FROM information_schema.tables \
             WHERE table_schema = 'test' AND table_name = 't4'"
        )),
        [["abc"]]
    );

    rows(&mut session, "CREATE TABLE test.t5 (a INT)");
    session
        .node_conn()
        .execute_write("ALTER TABLE test.t5 ADD INDEX i (a), ADD PRIMARY KEY (a)")
        .unwrap();
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.t5"));
    assert!(
        created[0][1].contains("`a` int NOT NULL"),
        "{}",
        created[0][1]
    );

    rows(&mut session, "CREATE TABLE test.t6 (a INT, b INT)");
    let error = session
        .node_conn()
        .execute_write("ALTER TABLE test.t6 MODIFY COLUMN b DOUBLE, ADD INDEX idx ((a + b))")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (8200, "Unsupported operate same column 'b'")
    );
}

/// Go `CreateSequence` / `onCreateSequence`, `AlterSequence` /
/// `onAlterSequence` and `dropTableObject` for sequences, from
/// `ddl/sequence.test`: the counter is seeded one step outside START, an ALTER
/// discards the reserved batch and RESTART moves the counter, a DROP reports
/// a missing name (4139) once the list ends and a non-sequence (1347), and
/// TRUNCATE reads a sequence as missing (1146). This node refused every
/// sequence statement.
#[test]
fn sequence_ddl_runs_as_go_runs_it() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(998)).unwrap();
    session
        .node_conn()
        .execute_write("CREATE SEQUENCE test.s START 10 INCREMENT 2 CACHE 3 COMMENT 'c'")
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT nextval(test.s), nextval(test.s)"
        )),
        [["10", "12"]]
    );
    session
        .node_conn()
        .execute_write("CREATE SEQUENCE IF NOT EXISTS test.s")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        [["Note", "1050", "Table 'test.s' already exists"]]
    );
    session
        .node_conn()
        .execute_write("ALTER SEQUENCE test.s INCREMENT 5")
        .unwrap();
    // Seeded from the discarded batch's end, 14 (oracle).
    assert_eq!(
        displayed(rows(&mut session, "SELECT nextval(test.s)")),
        [["15"]]
    );
    session
        .node_conn()
        .execute_write("ALTER SEQUENCE test.s RESTART WITH 100")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT nextval(test.s)")),
        [["100"]]
    );
    let created = displayed(rows(&mut session, "SHOW CREATE SEQUENCE test.s"));
    assert_eq!(
        created[0][1],
        "CREATE SEQUENCE `s` start with 10 minvalue 1 maxvalue 9223372036854775806 \
         increment by 5 cache 3 nocycle ENGINE=InnoDB COMMENT='c'"
    );
    let error = session
        .node_conn()
        .execute_write("ALTER SEQUENCE test.s MINVALUE 50")
        .unwrap_err();
    assert_eq!(error.code, 4136, "{}", error.message);

    rows(&mut session, "CREATE TABLE test.t (a INT)");
    let error = session
        .node_conn()
        .execute_write("TRUNCATE TABLE test.s")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1146, "Table 'test.s' doesn't exist")
    );
    let error = session
        .node_conn()
        .execute_write("DROP SEQUENCE test.t")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1347, "'test.t' is not SEQUENCE")
    );
    let error = session
        .node_conn()
        .execute_write("DROP SEQUENCE test.s, test.nosuch")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (4139, "Unknown SEQUENCE: 'test.nosuch'")
    );
    session
        .node_conn()
        .execute_write("DROP SEQUENCE IF EXISTS test.s, test.t")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        [
            ["Note", "1347", "'test.t' is not SEQUENCE"],
            ["Note", "4139", "Unknown SEQUENCE: 'test.s'"]
        ]
    );
}

/// Go `columnDefToCol` records a generated column's text and dependences,
/// and `checkGeneratedColumn` resolves every expression against the whole
/// table once it is built: a generated column reads a LATER ordinary column,
/// and the AUTO_INCREMENT column only when allowed (3109, also ADD COLUMN's
/// `checkAutoIncrementRef`). `BuildIndexInfo` refuses an index name over 64
/// characters (1059). This node resolved against the preceding columns only
/// (1054), and checked neither.
#[test]
fn generated_columns_resolve_against_the_whole_table_as_go_does() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(999)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.g (gc1 INT AS (r + 1), gc2 INT AS (r + 1) STORED, r INT)",
    );
    rows(&mut session, "INSERT INTO test.g (r) VALUES (1)");
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM test.g")),
        [["2", "2", "1"]]
    );
    for (sql, code, message) in [
        (
            "CREATE TABLE test.a1 (a BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT, b BIGINT AS (a + 1))"
                .to_owned(),
            3109,
            "Generated column 'b' cannot refer to auto-increment column.".to_owned(),
        ),
        (
            format!("CREATE TABLE test.a2 (c INT, INDEX {} (c))", "d".repeat(65)),
            1059,
            format!("Identifier name '{}' is too long", "d".repeat(65)),
        ),
    ] {
        let error = session.node_conn().execute_write(&sql).unwrap_err();
        assert_eq!((error.code, error.message.as_str()), (code, message.as_str()), "{sql}");
    }
    rows(
        &mut session,
        "CREATE TABLE test.a3 (a BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT, b INT)",
    );
    let error = session
        .node_conn()
        .execute_write("ALTER TABLE test.a3 ADD COLUMN d BIGINT GENERATED ALWAYS AS (a + 1)")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (
            3109,
            "Generated column 'd' cannot refer to auto-increment column."
        )
    );
}

/// Go's TTL table options: `handleTableOptions` and `checkTTLInfoValid` at
/// CREATE, `AlterTableTTLInfoOrEnable` / `onTTLInfoChange` keeping the stored
/// enable flag and interval a new `TTL=` leaves unset, `onTTLInfoRemove`, and
/// the multi-schema refusal (8200). This node refused every TTL option.
#[test]
fn ttl_table_options_run_as_go_runs_them() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1000)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.t (created_at DATETIME, updated_at DATE, n INT) \
         TTL = `created_at` + INTERVAL 5 DAY TTL_ENABLE = 'OFF'",
    );
    session
        .node_conn()
        .execute_write("ALTER TABLE test.t TTL_JOB_INTERVAL = '2h'")
        .unwrap();
    session
        .node_conn()
        .execute_write("ALTER TABLE test.t TTL = `updated_at` + INTERVAL 1 YEAR")
        .unwrap();
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.t"));
    assert!(
        created[0][1].ends_with(
            "/*T![ttl] TTL=`updated_at` + INTERVAL 1 YEAR */ /*T![ttl] TTL_ENABLE='OFF' */ \
             /*T![ttl] TTL_JOB_INTERVAL='2h' */"
        ),
        "{}",
        created[0][1]
    );
    for (sql, code, message) in [
        (
            "ALTER TABLE test.t TTL = `n` + INTERVAL 1 YEAR",
            8148,
            "Field 'n' is of a not supported type for TTL config, expect DATETIME, DATE or TIMESTAMP",
        ),
        (
            "ALTER TABLE test.t TTL = `nosuch` + INTERVAL 1 YEAR",
            1054,
            "Unknown column 'nosuch' in 'TTL config'",
        ),
        (
            "ALTER TABLE test.t TTL_ENABLE = 'ON' REMOVE TTL",
            8200,
            "Unsupported multi schema change for alter table ttl",
        ),
        (
            "CREATE TABLE test.p (id1 INT, id2 DOUBLE, t TIMESTAMP, PRIMARY KEY (id1, id2) CLUSTERED) \
             TTL = `t` + INTERVAL 1 DAY",
            8153,
            "Unsupported clustered primary key type FLOAT/DOUBLE for TTL",
        ),
        (
            "CREATE TABLE test.q (id INT) TTL_JOB_INTERVAL = '1h'",
            8150,
            "Cannot set TTL_JOB_INTERVAL on a table without TTL config",
        ),
    ] {
        let error = session.node_conn().execute_write(sql).unwrap_err();
        assert_eq!((error.code, error.message.as_str()), (code, message), "{sql}");
    }
    session
        .node_conn()
        .execute_write("ALTER TABLE test.t REMOVE TTL")
        .unwrap();
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.t"));
    assert!(!created[0][1].contains("TTL"), "{}", created[0][1]);
    let error = session
        .node_conn()
        .execute_write("ALTER TABLE test.t TTL_ENABLE = 'ON'")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (8150, "Cannot set TTL_ENABLE on a table without TTL config")
    );
}

/// Go `AlterTablePlacement` / `onAlterTablePlacement`: the table takes the
/// policy (or loses it under DEFAULT), an unknown one is 8239 and a
/// temporary table 8006. This node refused the statement.
#[test]
fn alter_table_placement_policy_as_go_does() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1005)).unwrap();
    rows(&mut session, "CREATE PLACEMENT POLICY p1 FOLLOWERS = 1");
    rows(&mut session, "CREATE TABLE test.t (a INT)");
    session
        .node_conn()
        .execute_write("ALTER TABLE test.t PLACEMENT POLICY p1")
        .unwrap();
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.t"));
    assert!(
        created[0][1].ends_with("/*T![placement] PLACEMENT POLICY=`p1` */"),
        "{}",
        created[0][1]
    );
    session
        .node_conn()
        .execute_write("ALTER TABLE test.t PLACEMENT POLICY DEFAULT")
        .unwrap();
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.t"));
    assert!(!created[0][1].contains("PLACEMENT"), "{}", created[0][1]);
    for (sql, code, message) in [
        (
            "ALTER TABLE test.t PLACEMENT POLICY nosuch",
            8239,
            "Unknown placement policy 'nosuch'",
        ),
        (
            "ALTER TABLE test.g PLACEMENT POLICY p1",
            8006,
            "`placement` is unsupported on temporary tables.",
        ),
    ] {
        if sql.contains("test.g") {
            rows(
                &mut session,
                "CREATE GLOBAL TEMPORARY TABLE test.g (a INT) ON COMMIT DELETE ROWS",
            );
        }
        let error = session.node_conn().execute_write(sql).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (code, message),
            "{sql}"
        );
    }
}

/// Go `handleTableOptions` records COMPRESSION and ignores every option
/// outside its switch, and the parser's "parsed but ignored" warning reaches
/// SHOW WARNINGS after the statement's own (`clientConn.handleStmt`). This
/// node refused the options, and its routed DDL reset the warning buffer a
/// second time, dropping the parser's warnings.
#[test]
fn ignored_table_options_warn_through_the_parser_as_go_does() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1011)).unwrap();
    // The connection's COM_QUERY order: parse at the statement boundary,
    // then the write.
    let sql = "CREATE TABLE test.a (id INT PRIMARY KEY) PAGE_CHECKSUM=1 COMPRESSION='zlib'";
    let parsed = session.parse_at_statement_boundary(sql).unwrap();
    assert!(session.node_conn().execute_stmt(sql, &parsed).unwrap().is_none());
    let warnings = displayed(rows(&mut session, "SHOW WARNINGS"));
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert_eq!(warnings[0][..2], ["Warning", "1064"]);
    assert!(
        warnings[0][2]
            .contains("The PAGE_CHECKSUM option is parsed but ignored by all storage engines."),
        "{warnings:?}"
    );
    let sql = "CREATE TABLE test.b (a INT)";
    let parsed = session.parse_at_statement_boundary(sql).unwrap();
    assert!(session.node_conn().execute_stmt(sql, &parsed).unwrap().is_none());
    assert!(rows(&mut session, "SHOW WARNINGS").is_empty());
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.a"));
    assert!(
        created[0][1].ends_with(" COMPRESSION='zlib'"),
        "{}",
        created[0][1]
    );
}

/// Go `handleTableOptions` / `AlterTableAffinity` record a table's AFFINITY,
/// the DDL job creates its PD affinity groups through `pkg/domain/affinity`
/// (the mock manager on unistore) and deletes them when the affinity goes,
/// partition management on such a table is refused, and a view is not a
/// BASE TABLE. This node refused the option outright.
#[test]
fn table_affinity_and_its_groups_as_go_does() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1014)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.t1 (a INT) AFFINITY = 'TABLE'",
    );
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.t1"));
    assert!(
        created[0][1].ends_with("/*T![affinity] AFFINITY='table' */"),
        "{}",
        created[0][1]
    );
    let table_id = displayed(rows(
        &mut session,
        "SELECT tidb_table_id FROM information_schema.tables \
         WHERE table_schema = 'test' AND table_name = 't1'",
    ))[0][0]
        .clone();
    let group = format!("_tidb_t_{table_id}");
    let held = |group: &str| {
        tidb_domain_affinity::get_groups(&[group.to_owned()])
            .unwrap()
            .contains_key(group)
    };
    assert!(held(&group));
    session
        .node_conn()
        .execute_write("ALTER TABLE test.t1 AFFINITY = ''")
        .unwrap();
    assert!(!held(&group));
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.t1"));
    assert!(!created[0][1].contains("AFFINITY"), "{}", created[0][1]);
    rows(
        &mut session,
        "CREATE TABLE test.t2 (a INT) AFFINITY = 'table'",
    );
    let t2_group = format!(
        "_tidb_t_{}",
        displayed(rows(
            &mut session,
            "SELECT tidb_table_id FROM information_schema.tables \
             WHERE table_schema = 'test' AND table_name = 't2'",
        ))[0][0]
    );
    assert!(held(&t2_group));
    rows(&mut session, "DROP TABLE test.t2");
    assert!(!held(&t2_group));

    rows(
        &mut session,
        "CREATE TABLE test.tp (a INT) AFFINITY = 'partition' PARTITION BY RANGE (a) \
         (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))",
    );
    for (sql, message) in [
        (
            "ALTER TABLE test.tp ADD PARTITION (PARTITION p2 VALUES LESS THAN (30))",
            "Unsupported ADD PARTITION of a table with AFFINITY option",
        ),
        (
            "ALTER TABLE test.tp DROP PARTITION p1",
            "Unsupported DROP PARTITION of a table with AFFINITY option",
        ),
    ] {
        let error = session.node_conn().execute_write(sql).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (8200, message),
            "{sql}"
        );
    }
    let error = session
        .node_conn()
        .execute_write("ALTER TABLE test.t1 AFFINITY = 'partition'")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (
            8266,
            "Can not set AFFINITY='partition' on a non-partition table."
        )
    );
    rows(&mut session, "CREATE VIEW test.v1 AS SELECT 1");
    let error = session
        .node_conn()
        .execute_write("ALTER TABLE test.v1 AFFINITY = 'table'")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1347, "'test.v1' is not BASE TABLE")
    );
}

/// Go `fetchShowAffinity`: a row per affinity table or partition, schema then
/// table id descending, with NULL group state where PD holds no group (always
/// on unistore, whose PD HTTP client is nil); LIKE matches the table name.
/// This node answered no rows.
#[test]
fn show_affinity_lists_affinity_tables_as_go_does() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1015)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.a1 (a INT) AFFINITY = 'table'",
    );
    rows(
        &mut session,
        "CREATE TABLE test.a2 (a INT) AFFINITY = 'partition' PARTITION BY HASH (a) PARTITIONS 2",
    );
    rows(&mut session, "CREATE TABLE test.plain (a INT)");
    let null = "NULL".to_owned();
    let row = |table: &str, partition: &str| {
        let mut row = vec!["test".to_owned(), table.to_owned(), partition.to_owned()];
        row.extend(std::iter::repeat_n(null.clone(), 5));
        row
    };
    assert_eq!(
        displayed(rows(&mut session, "SHOW AFFINITY")),
        [row("a2", "p0"), row("a2", "p1"), row("a1", "NULL")]
    );
    assert_eq!(
        displayed(rows(&mut session, "SHOW AFFINITY LIKE 'A1'")),
        [row("a1", "NULL")]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SHOW AFFINITY WHERE Partition_name = 'p1'"
        )),
        [row("a2", "p1")]
    );
}

/// Go's resource-group DDL (`onCreateResourceGroup` / `onAlterResourceGroup`
/// / `onDropResourceGroup`) keeps the group in meta and in the resource
/// manager (the mock on unistore), behind the executor's existence checks.
/// This node refused the statements outright.
#[test]
fn resource_group_ddl_as_go_does() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1016)).unwrap();
    session
        .node_conn()
        .execute_write("CREATE RESOURCE GROUP rg1 RU_PER_SEC = 100")
        .unwrap();
    session
        .node_conn()
        .execute_write("ALTER RESOURCE GROUP rg1 RU_PER_SEC = 200")
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT name, ru_per_sec FROM information_schema.resource_groups WHERE name = 'rg1'"
        )),
        [["rg1", "200"]]
    );
    let error = session
        .node_conn()
        .execute_write("CREATE RESOURCE GROUP rg1 RU_PER_SEC = 100")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (8248, "Resource group 'rg1' already exists")
    );
    session
        .node_conn()
        .execute_write("CREATE RESOURCE GROUP IF NOT EXISTS rg1 RU_PER_SEC = 100")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        [["Note", "8248", "Resource group 'rg1' already exists"]]
    );
    session
        .node_conn()
        .execute_write("SET RESOURCE GROUP rg1")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT CURRENT_RESOURCE_GROUP()")),
        [["rg1"]]
    );
    session
        .node_conn()
        .execute_write("SET RESOURCE GROUP default")
        .unwrap();
    session
        .node_conn()
        .execute_write("DROP RESOURCE GROUP rg1")
        .unwrap();
    assert!(rows(
        &mut session,
        "SELECT name FROM information_schema.resource_groups WHERE name = 'rg1'"
    )
    .is_empty());
    let error = session
        .node_conn()
        .execute_write("DROP RESOURCE GROUP rg1")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (8249, "Unknown resource group 'rg1'")
    );
}

/// The domain's `SHOW STATUS` providers (Go `ddl/stat.go`, registered by
/// `ddl.Start`, and the binding handle's): the DDL id is this server's id,
/// the schema version is the one `ADMIN SHOW DDL` reports, and the binding
/// handle reports the zero time until a binding is loaded.
#[test]
fn the_domain_reports_its_ddl_and_binding_statuses() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1016)).unwrap();
    let server_id = displayed(rows(
        &mut session,
        "SELECT DDL_ID FROM information_schema.TIDB_SERVERS_INFO",
    ));
    let schema_version = displayed(rows(&mut session, "ADMIN SHOW DDL"))[0][0].clone();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SHOW GLOBAL STATUS WHERE Variable_name IN ('server_id', 'ddl_schema_version')"
        )),
        [
            ["ddl_schema_version".to_owned(), schema_version],
            ["server_id".to_owned(), server_id[0][0].clone()],
        ]
    );
    // Session scope only: GLOBAL drops it.
    assert_eq!(
        displayed(rows(&mut session, "SHOW STATUS LIKE 'last_plan_binding_update_time'")),
        [["last_plan_binding_update_time", "0000-00-00 00:00:00"]]
    );
    assert!(
        displayed(rows(&mut session, "SHOW GLOBAL STATUS LIKE 'last_plan_binding%'")).is_empty()
    );
}

/// Go's label-rule DDL on the production node: `ALTER TABLE [PARTITION]
/// ATTRIBUTES` writes the node's label manager (Go `onAlterTableAttributes`),
/// RENAME TABLE re-keys the rules to the new names, `ATTRIBUTES=DEFAULT`
/// deletes one, DROP DATABASE deletes its tables' rules (`onDropSchema`), and
/// `information_schema.attributes` reads the same manager.
#[test]
fn label_rule_ddl_writes_the_nodes_label_manager() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1018)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE t1 (a INT) PARTITION BY RANGE (a) \
             (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    session
        .node_conn()
        .execute_write("ALTER TABLE t1 ATTRIBUTES='key=value'")
        .unwrap();
    session
        .node_conn()
        .execute_write("ALTER TABLE t1 PARTITION p0 ATTRIBUTES='k2=v2'")
        .unwrap();
    let attributes = |session: &mut Session| {
        displayed(rows(
            session,
            "SELECT ID, TYPE, ATTRIBUTES FROM information_schema.attributes ORDER BY ID",
        ))
    };
    assert_eq!(
        attributes(&mut session),
        [
            ["schema/test/t1", "key-range", "\"key=value\""],
            ["schema/test/t1/p0", "key-range", "\"k2=v2\""],
        ]
    );
    session
        .node_conn()
        .execute_write("RENAME TABLE t1 TO t2")
        .unwrap();
    session
        .node_conn()
        .execute_write("TRUNCATE TABLE t2")
        .unwrap();
    assert_eq!(
        attributes(&mut session),
        [
            ["schema/test/t2", "key-range", "\"key=value\""],
            ["schema/test/t2/p0", "key-range", "\"k2=v2\""],
        ]
    );
    session
        .node_conn()
        .execute_write("ALTER TABLE t2 ATTRIBUTES=DEFAULT")
        .unwrap();
    assert_eq!(
        attributes(&mut session),
        [["schema/test/t2/p0", "key-range", "\"k2=v2\""]]
    );
    let error = session
        .node_conn()
        .execute_write("ALTER TABLE t2 ATTRIBUTES='nokey'")
        .unwrap_err();
    assert_eq!(error.code, 8237);
    session
        .node_conn()
        .execute_write("CREATE TABLE plain (a INT)")
        .unwrap();
    let error = session
        .node_conn()
        .execute_write("ALTER TABLE plain PARTITION p0 ATTRIBUTES='k=v'")
        .unwrap_err();
    assert_eq!(error.code, 1505);

    session
        .node_conn()
        .execute_write("CREATE DATABASE d2")
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE d2.x (a INT)")
        .unwrap();
    session
        .node_conn()
        .execute_write("ALTER TABLE d2.x ATTRIBUTES='a=b'")
        .unwrap();
    assert_eq!(attributes(&mut session).len(), 2);
    session
        .node_conn()
        .execute_write("DROP DATABASE d2")
        .unwrap();
    assert_eq!(
        tidb_domain_infosync::label_manager::label_rule_manager()
            .get_label_rules(&["schema/d2/x".to_owned()])
            .unwrap()
            .len(),
        0
    );
}

/// Go's generated-column rules on the production node: a table whose only
/// read column is VIRTUAL still scans its rows (Go's `TableScan.Columns`
/// carries the virtual column and `FillVirtualColumnValue` computes it);
/// `CheckPKOnGeneratedColumn` refuses a virtual primary key; and MODIFY of a
/// virtual column's expression runs through `checkModifyGeneratedColumn`
/// while a change of a column a generated column reads is refused.
#[test]
fn generated_columns_read_build_and_modify_as_go_on_the_production_node() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1020)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE only_virtual (c0 TEXT AS ('a\\\\b'))")
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO only_virtual VALUES ()")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM only_virtual")),
        [["a\\b"]]
    );

    let error = session
        .node_conn()
        .execute_write("CREATE TABLE bad_pk (a INT, b INT, c INT AS (a+b), PRIMARY KEY (c))")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (
            3106,
            "'Defining a virtual generated column as primary key' is not supported for \
             generated columns."
        )
    );

    session
        .node_conn()
        .execute_write("CREATE TABLE g (a INT, b INT AS (a+1))")
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO g (a) VALUES (1)")
        .unwrap();
    session
        .node_conn()
        .execute_write("ALTER TABLE g MODIFY b INT AS (a+2)")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT a, b FROM g")),
        [["1", "3"]]
    );
    let error = session
        .node_conn()
        .execute_write("ALTER TABLE g MODIFY b INT AS (a+2) STORED")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (
            3106,
            "'Changing the STORED status' is not supported for generated columns."
        )
    );
    let error = session
        .node_conn()
        .execute_write("ALTER TABLE g MODIFY a BIGINT")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (
            3106,
            "'[ddl:3108]Column 'a' has a generated column dependency.' is not supported for \
             generated columns."
        )
    );
}

/// Go `finishDDLJob` files every finished job in the meta `DDLJobHistory`
/// hash, and `information_schema.DDL_JOBS` / `ADMIN SHOW DDL JOBS` read it
/// (running jobs first): on the production node each direct DDL leaves one
/// Go-shaped job -- its schema and table, type, multi-schema sub-jobs and
/// its statement text, with Go's INTERVAL query rewrite.
#[test]
fn production_ddl_jobs_land_in_the_meta_history_as_go_files_them() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1021)).unwrap();
    session
        .node_conn()
        .execute_write("CREATE DATABASE hist_db")
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE hist_db.t (a INT)")
        .unwrap();
    // Go `buildIndexColumns`: an index may not name a column the same
    // statement adds.
    let error = session
        .node_conn()
        .execute_write("ALTER TABLE hist_db.t ADD COLUMN b INT, ADD INDEX ib (b)")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1072, "column does not exist: b")
    );
    session
        .node_conn()
        .execute_write("ALTER TABLE hist_db.t ADD COLUMN b INT, ADD INDEX ib (a)")
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT db_name, table_name, job_type FROM information_schema.DDL_JOBS LIMIT 5"
        )),
        [
            ["hist_db", "t", "alter table multi-schema change"],
            ["hist_db", "t", "add column /* subjob */"],
            ["hist_db", "t", "add index /* subjob */"],
            ["hist_db", "t", "create table"],
            ["hist_db", "", "create schema"],
        ]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT query FROM information_schema.DDL_JOBS WHERE job_type = 'create table' LIMIT 1"
        )),
        [["CREATE TABLE hist_db.t (a INT)"]]
    );
    // ADMIN SHOW DDL JOBS 1: one history job, with its sub-job rows.
    assert_eq!(rows(&mut session, "ADMIN SHOW DDL JOBS 1").len(), 3);

    session
        .node_conn()
        .execute_write(
            "CREATE TABLE hist_db.tp (id INT) PARTITION BY RANGE (id) INTERVAL (100) \
             FIRST PARTITION LESS THAN (100) LAST PARTITION LESS THAN (300)",
        )
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT query FROM information_schema.DDL_JOBS LIMIT 1"
        )),
        [[
            "CREATE TABLE hist_db.tp (id INT) PARTITION BY RANGE (id) (PARTITION `P_LT_100` \
             VALUES LESS THAN (100),\n PARTITION `P_LT_200` VALUES LESS THAN (200),\n PARTITION \
             `P_LT_300` VALUES LESS THAN (300))"
        ]]
    );
}

/// Go's preprocess judges a table-qualified column name in CREATE TABLE
/// (1102 for another schema, 1103 for another table) and the builder then
/// reads the bare name; the production builder refused every qualifier.
#[test]
fn create_table_accepts_a_column_qualified_by_its_own_table() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1026)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE q2 (a CHAR, q2.b INT)")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SHOW COLUMNS FROM q2"))
            .into_iter()
            .map(|row| row[0].clone())
            .collect::<Vec<_>>(),
        ["a", "b"]
    );
    let error = session
        .node_conn()
        .execute_write("CREATE TABLE q3 (q9.a CHAR)")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1103, "Incorrect table name 'q9'")
    );
}

/// Go `table.GetColDefaultValue` casts `types.NewDatum(defaultVal)` -- a
/// UTF-8 `utf8mb4_bin` string -- into the column. The production loader built
/// a binary datum instead, so the cast into a GBK column decoded the stored
/// UTF-8 as GBK: `DEFAULT 0xC4E3BAC3` read back as mojibake and `DEFAULT '好'`
/// refused to insert at all.
#[test]
fn gbk_column_defaults_insert_as_the_text_they_store() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1029)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE t (a CHAR(10) CHARSET gbk DEFAULT 0xC4E3BAC3)")
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO t VALUES (DEFAULT)")
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO t () VALUES ()")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT a, HEX(a) FROM t")),
        [["你好", "C4E3BAC3"], ["你好", "C4E3BAC3"]]
    );
    session
        .node_conn()
        .execute_write("CREATE TABLE t2 (a CHAR(10) CHARSET gbk DEFAULT '好')")
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO t2 VALUES (DEFAULT)")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT a, HEX(a) FROM t2")),
        [["好", "BAC3"]]
    );
}

/// Go `NewWindowFuncDesc` applies its NOT NULL fixups by the name AS WRITTEN,
/// and a view's body is stored restored -- `ROW_NUMBER()` in upper case -- so
/// `DESC` of the view re-plans a window column that missed the `row_number`
/// arm and is nullable. The port lowered the name first and answered NO.
#[test]
fn view_window_columns_describe_as_nullable() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1032)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE t1 (id INT NOT NULL, num INT)")
        .unwrap();
    session
        .node_conn()
        .execute_write(
            "CREATE VIEW v AS SELECT id, row_number() OVER (PARTITION BY num) AS r FROM t1",
        )
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "DESC v")),
        [
            ["id", "int", "NO", "", "NULL", ""],
            ["r", "bigint", "YES", "", "NULL", ""]
        ]
    );
}

/// The column flags a client reads are Go's `ConvertColumnInfo` over the
/// plan's result types: `DefaultTypeForValue` makes every non-NULL literal NOT
/// NULL, every non-string builtin result BINARY (`newReturnField...`), and
/// `adjustNullFlagForReturnType` settles NOT NULL by function name. Every
/// expectation below was measured on this branch's Go (low 16 bits of the
/// column flag): `'a'`=1, `1e0`=129, `NULL`=128, `a+1`=129 over a NOT NULL
/// `a`, `b+1`=128, `isnull(b)`=129, `abs(a)`/`length(a)`=128,
/// `a IN (1,2)`=128, `a IS NOT NULL`=129, `LEAST(a,1)`=4225.
#[test]
fn result_column_flags_follow_gos_null_and_binary_stamps() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1033)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE t (a INT NOT NULL, b INT)")
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO t VALUES (1, 1)")
        .unwrap();
    for (sql, flag) in [
        ("SELECT 'a' FROM t", 1),
        ("SELECT 1e0 FROM t", 129),
        ("SELECT NULL FROM t", 128),
        ("SELECT TRUE FROM t", 129),
        ("SELECT a FROM t", 4097),
        ("SELECT a + 1 FROM t", 129),
        ("SELECT b + 1 FROM t", 128),
        ("SELECT a + b FROM t", 128),
        ("SELECT a DIV 1 FROM t", 128),
        ("SELECT isnull(b) FROM t", 129),
        ("SELECT a <=> b FROM t", 129),
        ("SELECT abs(a) FROM t", 128),
        ("SELECT length(a) FROM t", 128),
        ("SELECT a IN (1, 2) FROM t", 128),
        ("SELECT a BETWEEN 1 AND 2 FROM t", 128),
        ("SELECT a IS NOT NULL FROM t", 129),
        ("SELECT LEAST(a, 1) FROM t", 4225),
        ("SELECT GREATEST(a, b) FROM t", 4224),
    ] {
        let mut result = session.node_conn().execute(sql).unwrap();
        let fields = crate::domain::resultset_source::ResultSetSource::fields(result.source()).unwrap();
        assert_eq!(fields[0].field_type.flags, flag, "{sql}");
    }
}

/// Go's `CompactTableStmt` validates the table and partition names and, with
/// no TiFlash replica, answers a warning; it does not change the catalog. The
/// production node refused it as an unroutable catalog change.
#[test]
fn compact_table_without_a_replica_warns_instead_of_refusing() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1044)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE ct (a INT)")
        .unwrap();
    session
        .node_conn()
        .execute_write("ALTER TABLE ct COMPACT TIFLASH REPLICA")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        [[
            "Warning",
            "1105",
            "compact skipped: no tiflash replica in the table"
        ]]
    );
    let error = session
        .node_conn()
        .execute_write("ALTER TABLE ct COMPACT PARTITION p1 TIFLASH REPLICA")
        .unwrap_err();
    assert_eq!(error.code, 1105);
    let error = session
        .node_conn()
        .execute_write("ALTER TABLE missing COMPACT")
        .unwrap_err();
    assert_eq!(error.code, 1146);
}

/// Go `getNameValuePairs` declines the fast point plan when a column is
/// qualified by anything but the table's visible name; the ordinary planner
/// then reports the unknown column (1054). An alias hides the real name.
#[test]
fn point_get_fast_path_checks_the_column_qualifier() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1048)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE qpg (a CHAR(2) PRIMARY KEY, b CHAR(2))")
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO qpg VALUES ('aa', 'bb')")
        .unwrap();
    for sql in [
        "SELECT a FROM qpg WHERE xxxxx.a = 'aa'",
        "SELECT a FROM qpg tmp WHERE qpg.a = 'aa'",
    ] {
        assert_eq!(query_error(&mut session, sql).0, 1054, "{sql}");
    }
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT a FROM qpg tmp WHERE tmp.a = 'aa'"
        )),
        [["aa"]]
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT a FROM qpg WHERE qpg.a = 'aa'")),
        [["aa"]]
    );
}

/// Go's `ExecuteExec` runs a prepared DDL as the statement's body, so it must
/// reach the cluster catalog like any DDL: the prepared `CREATE TABLE` was
/// applied to the session's own copy and a later `DROP TABLE` found nothing.
/// Also pinned here: the table-build warnings (`LINEAR KEY` is ignored, 8200),
/// the schema-qualified sequence in a default, and the refusal of a stale-read
/// view body (1356).
#[test]
fn prepared_ddl_and_create_table_build_warnings_follow_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1050)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    rows(
        &mut session,
        "PREPARE pc FROM 'CREATE TABLE pdl (id BIGINT PRIMARY KEY) PARTITION BY RANGE (id) \
         INTERVAL (100) FIRST PARTITION LESS THAN (100) LAST PARTITION LESS THAN (500)'",
    );
    rows(&mut session, "EXECUTE pc");
    assert_eq!(
        displayed(rows(&mut session, "SHOW TABLES LIKE 'pdl'")),
        [["pdl"]]
    );
    session.node_conn().execute_write("DROP TABLE pdl").unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SHOW TABLES LIKE 'pdl'")),
        Vec::<Vec<String>>::new()
    );

    session
        .node_conn().execute_write(
            "CREATE TABLE lk (c1 INT, c2 CHAR(5), c3 DATE) PARTITION BY LINEAR KEY (c3) PARTITIONS 5",
        )
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        [[
            "Warning",
            "8200",
            "LINEAR KEY is not supported, using non-linear KEY instead"
        ]]
    );

    session
        .node_conn()
        .execute_write("CREATE SEQUENCE sq")
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE dseq (a INT DEFAULT (NEXTVAL(sq)))")
        .unwrap();
    let shown = displayed(rows(&mut session, "SHOW CREATE TABLE dseq"));
    assert!(
        shown[0][1].contains("DEFAULT (nextval(`test`.`sq`))"),
        "{shown:?}"
    );

    session
        .node_conn()
        .execute_write("CREATE TABLE src (a INT)")
        .unwrap();
    let error = session
        .node_conn()
        .execute_write(
            "CREATE VIEW v_stale AS SELECT * FROM src \
             AS OF TIMESTAMP DATE_SUB(CURRENT_TIMESTAMP(3), INTERVAL 1 SECOND)",
        )
        .unwrap_err();
    assert_eq!(error.code, 1356);
}

/// Go `checkTempTableSize` refuses a row once a temporary table holds more
/// than `@@tidb_tmp_table_max_size` -- what it committed plus this
/// transaction's own writes -- with 1114, before the row is written.
#[test]
fn temporary_table_refuses_rows_past_tmp_table_max_size() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1052)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write(
            "CREATE GLOBAL TEMPORARY TABLE tg (c1 INT, c2 MEDIUMTEXT) ON COMMIT DELETE ROWS",
        )
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE TEMPORARY TABLE tl (c1 INT, c2 MEDIUMTEXT)")
        .unwrap();
    session
        .node_conn()
        .execute_write("SET @@session.tidb_tmp_table_max_size = 1048576")
        .unwrap();
    let row = |table: &str| format!("INSERT INTO {table} VALUES (1, REPEAT('x', 512*1024))");
    for table in ["tg", "tl"] {
        session.node_conn().control_transaction("BEGIN").unwrap();
        session.node_conn().execute_write(&row(table)).unwrap();
        session.node_conn().execute_write(&row(table)).unwrap();
        let error = session.node_conn().execute_write(&row(table)).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (1114, format!("The table '{table}' is full").as_str())
        );
        session.node_conn().control_transaction("ROLLBACK").unwrap();
    }
    // A LOCAL table's committed rows count against the next transaction.
    session.node_conn().execute_write(&row("tl")).unwrap();
    session.node_conn().control_transaction("BEGIN").unwrap();
    session.node_conn().execute_write(&row("tl")).unwrap();
    let error = session.node_conn().execute_write(&row("tl")).unwrap_err();
    assert_eq!(error.code, 1114);
    session.node_conn().control_transaction("ROLLBACK").unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT COUNT(*) FROM tl")),
        [["1"]]
    );
}

/// Go names a result column from the logical plan's `OutputNames` captured
/// BEFORE optimization, and an unaliased column reference keeps the column AS
/// WRITTEN (`buildProjectionFieldNameFromColumns`: `colNameField.Name.Name`),
/// not the declared spelling. A set operation's names are its first branch's,
/// also when the optimizer replaced its root (an always-false filter leaves a
/// TableDual that names nothing, and the columns read `Column#1`).
#[test]
fn result_columns_keep_the_names_the_query_wrote() {
    use crate::domain::resultset_source::ResultSetSource;
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1054)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    let names = |session: &mut Session, sql: &str| -> Vec<String> {
        let mut result = session.node_conn().execute(sql).expect("the query runs");
        let source = result.source();
        let columns = source
            .fields()
            .expect("fields")
            .into_iter()
            .map(|field| field.name)
            .collect();
        let _ = source.next_batch(8);
        columns
    };
    for (sql, expected) in [
        (
            "(SELECT table_name FROM information_schema.tables WHERE 1 = 0) \
             UNION ALL (SELECT table_name FROM information_schema.tables WHERE 1 = 0)",
            vec!["table_name"],
        ),
        (
            "SELECT * FROM ((SELECT table_name FROM information_schema.tables WHERE 1 = 0) \
             UNION ALL (SELECT table_name FROM information_schema.tables WHERE 1 = 0)) u",
            vec!["table_name"],
        ),
        (
            "(SELECT digest, digest_text FROM information_schema.statements_summary WHERE 1 = 0 \
             UNION ALL SELECT digest, digest_text FROM information_schema.statements_summary_history \
             WHERE 1 = 0) ORDER BY digest",
            vec!["digest", "digest_text"],
        ),
    ] {
        assert_eq!(names(&mut session, sql), expected, "{sql}");
    }
}

/// Go `pruneAndBuildColPositionInfoForDelete`: a single-table DELETE reads
/// only the handle's and the indexes' columns, so `delete from t` scans the
/// narrowest covering index and `delete ... where a = 1` projects the rest
/// away. The rows it removes, and every index entry of them, are unchanged.
#[test]
fn single_table_delete_reads_only_the_columns_removal_needs() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1055)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    let plan = |session: &mut Session, sql: &str| {
        displayed(rows(session, &format!("EXPLAIN FORMAT = 'brief' {sql}")))
            .into_iter()
            .map(|row| format!("{} {}", row[0], row[3]))
            .collect::<Vec<_>>()
            .join(" | ")
    };
    // A heap table: the handle is the hidden _tidb_rowid.
    session
        .node_conn()
        .execute_write("CREATE TABLE dp1 (a INT, b INT, c VARCHAR(20), INDEX ib (b))")
        .unwrap();
    session
        .node_conn()
        .execute_write(
            "INSERT INTO dp1 VALUES (1, 10, 'x'), (2, 20, 'y'), (3, 20, 'z'), (4, 40, 'w')",
        )
        .unwrap();
    let full = plan(&mut session, "DELETE FROM dp1");
    assert!(
        full.contains("IndexFullScan") && full.contains("index:ib(b)"),
        "{full}"
    );
    let filtered = plan(&mut session, "DELETE FROM dp1 WHERE a = 1");
    assert!(filtered.contains("Projection"), "{filtered}");
    session
        .node_conn()
        .execute_write("DELETE FROM dp1 WHERE a = 1")
        .unwrap();
    session
        .node_conn()
        .execute_write("DELETE FROM dp1 WHERE b = 20 AND c = 'y'")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT a, b, c FROM dp1 ORDER BY a")),
        [["3", "20", "z"], ["4", "40", "w"]]
    );
    // The index lost exactly the removed rows' entries.
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT b FROM dp1 USE INDEX (ib) ORDER BY b"
        )),
        [["20"], ["40"]]
    );
    session
        .node_conn()
        .execute_write("DELETE FROM dp1 ORDER BY a LIMIT 1")
        .unwrap();
    assert_eq!(displayed(rows(&mut session, "SELECT a FROM dp1")), [["4"]]);
    // A clustered integer key and a unique secondary index.
    session
        .node_conn().execute_write(
            "CREATE TABLE dp2 (id INT PRIMARY KEY CLUSTERED, u INT, pad VARCHAR(20), UNIQUE KEY uu (u))",
        )
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO dp2 VALUES (1, 100, 'p'), (2, 200, 'q'), (3, 300, 'r')")
        .unwrap();
    session
        .node_conn()
        .execute_write("DELETE FROM dp2 WHERE pad = 'q'")
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT id, u FROM dp2 USE INDEX (uu) ORDER BY u"
        )),
        [["1", "100"], ["3", "300"]]
    );
    // The freed unique value can be inserted again.
    session
        .node_conn()
        .execute_write("INSERT INTO dp2 VALUES (9, 200, 's')")
        .unwrap();
    // A common handle.
    session
        .node_conn().execute_write(
            "CREATE TABLE dp3 (k1 VARCHAR(10), k2 INT, v INT, w INT, PRIMARY KEY (k1, k2) CLUSTERED, KEY iv (v))",
        )
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO dp3 VALUES ('a', 1, 5, 0), ('b', 2, 6, 0), ('c', 3, 7, 0)")
        .unwrap();
    session
        .node_conn()
        .execute_write("DELETE FROM dp3 WHERE w = 0 AND v = 6")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT k1, k2 FROM dp3 ORDER BY k1")),
        [["a", "1"], ["c", "3"]]
    );
    // A referenced table keeps every column (its foreign keys read them).
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE dpp (id INT PRIMARY KEY, name VARCHAR(10), INDEX iname (name))",
        )
        .unwrap();
    session
        .node_conn().execute_write(
            "CREATE TABLE dpc (id INT PRIMARY KEY, pid INT, CONSTRAINT fk_dp FOREIGN KEY (pid) REFERENCES dpp (id))",
        )
        .unwrap();
    let referenced = plan(&mut session, "DELETE FROM dpp");
    assert!(!referenced.contains("Projection"), "{referenced}");
}

/// Go prunes a multi-table write's read too: a multi-table UPDATE keeps every
/// column of an updated alias and what its SET expressions read from the
/// other sources (`buildUpdate`'s `used` set), so the source that only joins
/// is scanned through its covering index; a multi-table DELETE keeps the
/// handle's and the indexes' columns of each target. The rows written are
/// unchanged.
#[test]
fn multi_table_writes_read_only_the_columns_they_need() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1056)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    let plan = |session: &mut Session, sql: &str| {
        displayed(rows(session, &format!("EXPLAIN FORMAT = 'brief' {sql}")))
            .into_iter()
            .map(|row| format!("{} {}", row[0], row[3]))
            .collect::<Vec<_>>()
            .join(" | ")
    };
    session
        .node_conn()
        .execute_write("CREATE TABLE mu1 (a INT, b INT, pad INT)")
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE mu2 (a INT, b INT, other INT, INDEX ib (b))")
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO mu1 VALUES (1, 1, 0), (2, 2, 0), (3, 9, 0)")
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO mu2 VALUES (10, 1, 100), (20, 2, 200), (30, 7, 300)")
        .unwrap();
    // `mu2` is only joined: its read is the covering index.
    let update = plan(
        &mut session,
        "UPDATE mu1, mu2 SET mu1.a = 99 WHERE mu1.b = mu2.b",
    );
    assert!(
        update.contains("IndexFullScan") && update.contains("index:ib(b)"),
        "{update}"
    );
    session
        .node_conn()
        .execute_write("UPDATE mu1, mu2 SET mu1.a = 99 WHERE mu1.b = mu2.b")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT a, b FROM mu1 ORDER BY b")),
        [["99", "1"], ["99", "2"], ["3", "9"]]
    );
    // A column of `mu2` the SET expression reads is kept.
    session
        .node_conn()
        .execute_write("UPDATE mu1, mu2 SET mu1.pad = mu2.other WHERE mu1.b = mu2.b AND mu2.a = 20")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT b, pad FROM mu1 ORDER BY b")),
        [["1", "0"], ["2", "200"], ["9", "0"]]
    );
    // Updating both sources keeps both whole.
    session
        .node_conn()
        .execute_write("UPDATE mu1, mu2 SET mu1.pad = 5, mu2.other = mu1.b WHERE mu1.b = mu2.b")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT b, other FROM mu2 ORDER BY b")),
        [["1", "1"], ["2", "2"], ["7", "300"]]
    );
    // A multi-table DELETE of one target keeps the other source whole and
    // prunes the target to the index and handle.
    session
        .node_conn()
        .execute_write("DELETE mu1 FROM mu1, mu2 WHERE mu1.b = mu2.b AND mu2.other = 2")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT b FROM mu1 ORDER BY b")),
        [["1"], ["9"]]
    );
    session
        .node_conn()
        .execute_write("DELETE mu1, mu2 FROM mu1, mu2 WHERE mu1.b = mu2.b")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT b FROM mu2 ORDER BY b")),
        [["2"], ["7"]]
    );
    // A multi-table DELETE over one aliased source behaves as its
    // single-table form (this shape used to inherit the single-table prune).
    session
        .node_conn()
        .execute_write("DELETE m FROM mu2 AS m WHERE m.b = 2")
        .unwrap();
    assert_eq!(displayed(rows(&mut session, "SELECT b FROM mu2")), [["7"]]);
}

/// The table path of a derived table's data source counts its rows from the
/// pushed-down range: the statement's own WHERE (which the bridge replays)
/// is not where the predicate lives, and the row count must not stand in for
/// the range (`TableRangeScan` 10 rows for Go, 10000 here). The wrong count
/// then priced an ordered index scan below the Sort Go chose.
#[test]
fn derived_table_range_scan_is_estimated_from_its_range() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1106)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn().execute_write(
            "CREATE TABLE tdr (id INT UNSIGNED NOT NULL AUTO_INCREMENT, k INT UNSIGNED NOT NULL DEFAULT 0, \
             PRIMARY KEY (id) /*T![clustered_index] CLUSTERED */, KEY k (k))",
        )
        .unwrap();
    let plan = displayed(rows(
        &mut session,
        "EXPLAIN SELECT a.k FROM (SELECT * FROM tdr WHERE id < 10) a ORDER BY a.k",
    ));
    let kinds: Vec<&str> = plan
        .iter()
        .map(|row| row[0].trim_start_matches(['├', '└', '─', '│', ' ']))
        .collect();
    assert!(kinds[0].starts_with("Sort"), "{kinds:?}");
    let scan = plan
        .iter()
        .find(|row| row[0].contains("TableRangeScan"))
        .expect("a range scan below the sort");
    assert_eq!(scan[1], "10.00", "{plan:?}");
}

/// Go reads a table through a `LogicalUnionScan` when the open transaction has
/// written it, and always for a local temporary table; the plan shows a
/// `UnionScan` above the reader, a `Point_Get` loses it again
/// (`eliminateUnionScanAndLock`), and a temporary table's scans cost nothing
/// (`tidb_temp_table_factor`), so an ordered read prefers its index to a sort.
/// Expected shapes: Go 1.27 on this branch (`TestZZ` probe, 26 statements).
#[test]
fn dirty_and_temporary_tables_are_read_through_a_union_scan() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1118)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE tus (a INT PRIMARY KEY, b INT, c INT, KEY(b))")
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO tus VALUES (1,1,1),(2,2,2),(3,3,3)")
        .unwrap();
    let operators = |session: &mut Session, sql: &str| -> Vec<String> {
        displayed(rows(session, &format!("EXPLAIN FORMAT = 'brief' {sql}")))
            .into_iter()
            .map(|row| {
                row[0]
                    .trim_start_matches(['├', '└', '─', '│', ' '])
                    .split('_')
                    .next()
                    .unwrap_or_default()
                    .to_owned()
            })
            .collect()
    };
    // A clean table has no union scan.
    assert_eq!(
        operators(&mut session, "SELECT * FROM tus WHERE b = 1")[0],
        "IndexLookUp"
    );
    session.node_conn().control_transaction("BEGIN").unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO tus VALUES (4,4,4)")
        .unwrap();
    let dirty = operators(&mut session, "SELECT * FROM tus WHERE b = 1");
    assert_eq!(&dirty[..2], ["UnionScan", "IndexLookUp"], "{dirty:?}");
    // A point get reads its own transaction's rows and carries no union scan.
    assert_eq!(
        operators(&mut session, "SELECT * FROM tus WHERE a = 1"),
        ["Point"]
    );
    let ordered = operators(&mut session, "SELECT * FROM tus ORDER BY b");
    assert_eq!(&ordered[..2], ["Sort", "UnionScan"], "{ordered:?}");
    let limited = operators(&mut session, "SELECT * FROM tus WHERE b > 1 LIMIT 1");
    assert_eq!(&limited[..2], ["Limit", "UnionScan"], "{limited:?}");
    session.node_conn().control_transaction("ROLLBACK").unwrap();
    assert_eq!(
        operators(&mut session, "SELECT * FROM tus WHERE b = 1")[0],
        "IndexLookUp"
    );

    // A local temporary table is always read through a union scan, and its
    // free scans make the index the cheaper way to deliver an order.
    session
        .node_conn()
        .execute_write("CREATE TEMPORARY TABLE ttus (a INT PRIMARY KEY, b INT, KEY(b))")
        .unwrap();
    let temp = operators(&mut session, "SELECT * FROM ttus WHERE b = 1");
    assert_eq!(&temp[..2], ["UnionScan", "IndexReader"], "{temp:?}");
    assert_eq!(
        operators(&mut session, "SELECT * FROM ttus WHERE a = 1"),
        ["Point"]
    );
    let temp_ordered = operators(&mut session, "SELECT * FROM ttus ORDER BY b");
    assert_eq!(
        &temp_ordered[..2],
        ["UnionScan", "IndexReader"],
        "{temp_ordered:?}"
    );
}

/// Metadata answers captured from the oracle: a table reports the creating
/// job's time as `CREATE_TIME` (`TableInfo.UpdateTS`), an expression index
/// names its key part by the string `NULL`, a FULLTEXT key is refused with
/// 8200, and `ALTER INDEX` validates against the table as submitted.
#[test]
fn table_metadata_answers_follow_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1139)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE md (a INT, b INT, KEY ((a + b)))")
        .unwrap();
    let created = displayed(rows(
        &mut session,
        "select create_time is null, create_time > now() - interval 1 hour \
         from information_schema.tables where table_schema = 'test' and table_name = 'md'",
    ));
    assert_eq!(created, [["0", "1"]], "{created:?}");
    let status = displayed(rows(&mut session, "show table status like 'md'"));
    assert_ne!(status[0][11], "NULL", "{status:?}");
    let index = displayed(rows(&mut session, "show index from md"));
    assert_eq!(index[0][4], "NULL", "{index:?}");
    let fulltext = session
        .node_conn()
        .execute_write("CREATE TABLE ft (a TEXT, FULLTEXT KEY k (a))")
        .unwrap_err();
    let fulltext = format!("{fulltext:?}");
    assert!(
        fulltext.contains("FULLTEXT index is only supported in starter deployment mode"),
        "{fulltext}"
    );
    let invisible = session
        .node_conn()
        .execute_write("ALTER TABLE md ADD INDEX kb (b), ALTER INDEX kb INVISIBLE")
        .unwrap_err();
    let invisible = format!("{invisible:?}");
    assert!(invisible.contains("Key 'kb' doesn't exist in table 'md'"), "{invisible}");
}

/// Bootstrap creates Go's `sys` schema with `schema_unused_indexes` and the
/// `mysql.tidb_mdl_view` by executing their `CREATE VIEW`s, a view column
/// takes its key flag from the base column, and a NOT NULL column without a
/// default reports a NULL `COLUMN_DEFAULT`. Each was captured from the oracle.
#[test]
fn bootstrap_views_and_column_defaults_follow_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1140)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    for view in ["sys.schema_unused_indexes", "mysql.tidb_mdl_view"] {
        let result = rows(&mut session, &format!("select * from {view} limit 0"));
        assert!(result.is_empty(), "{view}: {result:?}");
    }
    let described = displayed(rows(&mut session, "desc mysql.tidb_mdl_view"));
    assert!(!described.is_empty(), "{described:?}");
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE nd (a INT NOT NULL, b TEXT NOT NULL, c INT NOT NULL DEFAULT 5)",
        )
        .unwrap();
    let defaults = displayed(rows(
        &mut session,
        "select column_name, column_default from information_schema.columns \
         where table_schema = 'test' and table_name = 'nd' order by ordinal_position",
    ));
    assert_eq!(defaults, [["a", "NULL"], ["b", "NULL"], ["c", "5"]], "{defaults:?}");
}

/// Go refuses an unsupported ALTER specification only after the table
/// lookup (1146) and the base-table check (1347) (captured from Go on this
/// branch).
#[test]
fn an_unsupported_alter_spec_is_refused_after_the_table_checks() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1905)).unwrap();
    rows(&mut session, "CREATE DATABASE alter_order");
    rows(&mut session, "USE alter_order");
    rows(&mut session, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    rows(&mut session, "CREATE VIEW vv AS SELECT 1");
    for (sql, code, message) in [
        (
            "ALTER TABLE nosuch REBUILD PARTITION p0",
            1146,
            "Table 'alter_order.nosuch' doesn't exist",
        ),
        (
            "ALTER TABLE vv REBUILD PARTITION p0",
            1347,
            "'alter_order.vv' is not BASE TABLE",
        ),
        (
            "ALTER TABLE t REBUILD PARTITION p0",
            8200,
            "Unsupported rebuild partition",
        ),
    ] {
        let error = session.node_conn().execute_write(sql).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (code, message),
            "{sql}"
        );
    }
}

/// Go's DDL checks in Go's order, with Go's notes (captured from Go on this
/// branch): the preprocessor resolves a table before anything else (1146,
/// also for a missing database), CREATE TABLE checks its database before
/// building (1049), a LIKE source must be a base table (1347), IF [NOT]
/// EXISTS forms add their notes, `dropIndex` names the missing index, and
/// placement policies keep Go's IF EXISTS rules and reserved name.
#[test]
fn ddl_checks_and_notes_follow_go_order() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1908)).unwrap();
    rows(&mut session, "CREATE DATABASE ddl_order");
    rows(&mut session, "USE ddl_order");
    rows(&mut session, "CREATE TABLE t (a INT, b INT, INDEX ib (b))");
    rows(&mut session, "CREATE VIEW v AS SELECT 1 a");
    for (sql, code, message) in [
        (
            "ALTER TABLE nodb.t ADD COLUMN c INT",
            1146,
            "Table 'nodb.t' doesn't exist",
        ),
        (
            "TRUNCATE TABLE nodb.t",
            1146,
            "Table 'nodb.t' doesn't exist",
        ),
        (
            "DROP INDEX i ON nodb.t",
            1146,
            "Table 'nodb.t' doesn't exist",
        ),
        (
            "CREATE TABLE nodb.t (a INT, a INT)",
            1049,
            "Unknown database 'nodb'",
        ),
        (
            "CREATE TABLE t2 LIKE v",
            1347,
            "'ddl_order.v' is not BASE TABLE",
        ),
        (
            "ALTER TABLE v MODIFY COLUMN a INT DEFAULT 'abc'",
            1347,
            "'ddl_order.v' is not BASE TABLE",
        ),
        (
            "ALTER TABLE missing MODIFY COLUMN a INT DEFAULT 'abc'",
            1146,
            "Table 'ddl_order.missing' doesn't exist",
        ),
        ("DROP INDEX nosuch ON t", 1091, "index nosuch doesn't exist"),
        (
            "ALTER TABLE t DROP INDEX nosuch",
            1091,
            "index nosuch doesn't exist",
        ),
        (
            "ALTER TABLE t DROP INDEX nosuch, ADD COLUMN z INT",
            1091,
            "index nosuch doesn't exist",
        ),
        (
            "ALTER TABLE t DROP PRIMARY KEY",
            1091,
            "Can't DROP 'PRIMARY'; check that column/key exists",
        ),
        (
            "ALTER PLACEMENT POLICY IF EXISTS nosuch PRIMARY_REGION=\"r1\" REGIONS=\"r1\"",
            8239,
            "Unknown placement policy 'nosuch'",
        ),
        (
            "CREATE PLACEMENT POLICY `default` PRIMARY_REGION=\"r1\" REGIONS=\"r1\"",
            1382,
            "The 'default' syntax is reserved for purposes internal to the MySQL server",
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
        "CREATE PLACEMENT POLICY p PRIMARY_REGION=\"r1\" REGIONS=\"r1\"",
    );
    for (sql, code, message) in [
        (
            "CREATE TABLE IF NOT EXISTS t (a INT)",
            "1050",
            "Table 'ddl_order.t' already exists",
        ),
        (
            "CREATE TABLE IF NOT EXISTS t LIKE t",
            "1050",
            "Table 'ddl_order.t' already exists",
        ),
        (
            "DROP INDEX IF EXISTS nosuch ON t",
            "1091",
            "index nosuch doesn't exist",
        ),
        (
            "ALTER TABLE t DROP INDEX IF EXISTS nosuch",
            "1091",
            "index nosuch doesn't exist",
        ),
        (
            "ALTER TABLE t DROP INDEX IF EXISTS nosuch, ADD COLUMN z INT",
            "1091",
            "index nosuch doesn't exist",
        ),
        (
            "CREATE PLACEMENT POLICY IF NOT EXISTS p PRIMARY_REGION=\"r1\" REGIONS=\"r1\"",
            "8238",
            "Placement policy 'p' already exists",
        ),
        (
            "DROP PLACEMENT POLICY IF EXISTS nosuch",
            "8239",
            "Unknown placement policy 'nosuch'",
        ),
    ] {
        session.node_conn().execute_write(sql).unwrap();
        assert_eq!(
            displayed(rows(&mut session, "SHOW WARNINGS")),
            vec![vec!["Note", code, message]],
            "{sql}"
        );
    }
}

/// ADD COLUMN and DROP COLUMN run as Go's persisted jobs (`onAddColumn`,
/// `onDropColumn`): the executor's notes and refusals, the jobs, the column
/// order, the delete ranges of the single-column indexes a drop takes along,
/// and the CHECK constraints `LoadCheckConstraint` prunes once a column they
/// read is gone. Captured from the Go oracle.
#[test]
fn add_and_drop_column_run_as_persisted_jobs_like_go() {
    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }
    fn int_key(value: i64) -> Vec<u8> {
        let mut key = vec![0x03];
        key.extend_from_slice(&((value as u64) ^ (1 << 63)).to_be_bytes());
        key
    }
    let index = |id: i64, index: i64| tidb_tablecodec::table_key::encode_table_index_prefix(id, index);
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1941)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    let id_of = |session: &mut _, sql: &str| -> Vec<i64> {
        displayed(rows(session, sql))
            .into_iter()
            .map(|row| row[0].parse().unwrap())
            .collect()
    };
    let last_job = |session: &mut _| {
        let row = displayed(rows(session, "admin show ddl jobs 1")).remove(0);
        [0, 3, 4, 7, 11].map(|column| row[column].clone()).join("|")
    };
    let warnings = |session: &mut _| -> Vec<String> {
        displayed(rows(session, "SHOW WARNINGS"))
            .into_iter()
            .map(|row| row.join("|"))
            .collect()
    };
    rows(
        &mut session,
        "create table t (a int, b int, c int not null, key kb(b), key kac(a,c))",
    );
    rows(&mut session, "insert into t values (1,1,1),(2,2,2)");
    let t = id_of(
        &mut session,
        "select tidb_table_id from information_schema.tables where table_name='t'",
    )[0];
    for sql in [
        "alter table t add column d int default 5",
        "alter table t add column e int first",
        "alter table t add column f varchar(10) after a",
    ] {
        rows(&mut session, sql);
        let job = last_job(&mut session);
        assert!(job.ends_with("|add column|public|0|synced"), "{sql}: {job}");
    }
    let before = last_job(&mut session);
    rows(&mut session, "alter table t add column if not exists d int");
    assert_eq!(
        warnings(&mut session),
        ["Note|1060|Duplicate column name 'd'"]
    );
    // IF NOT EXISTS over an existing column queues nothing.
    assert_eq!(last_job(&mut session), before);
    rows(&mut session, "alter table t drop column b");
    let job = last_job(&mut session);
    assert!(job.ends_with("|drop column|none|0|synced"), "{job}");
    let before = last_job(&mut session);
    rows(&mut session, "alter table t drop column if exists nosuch");
    assert_eq!(
        warnings(&mut session),
        ["Note|1091|Can't DROP 'nosuch'; check that column/key exists"]
    );
    assert_eq!(last_job(&mut session), before);
    rows(&mut session, "create table only1 (a int)");
    for (sql, code, message) in [
        (
            "alter table t add column d int",
            1060,
            "Duplicate column name 'd'",
        ),
        (
            "alter table t add column g int after nosuch",
            1054,
            "Unknown column 'nosuch' in 't'",
        ),
        (
            "alter table t drop column nosuch",
            1091,
            "Can't DROP 'nosuch'; check that column/key exists",
        ),
        (
            "alter table t drop column a",
            8200,
            "can't drop column a with composite index covered or Primary Key covered now",
        ),
        (
            "alter table only1 drop column a",
            1090,
            "can't drop only column a in table only1",
        ),
    ] {
        let error = session.node_conn().execute_write(sql).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (code, message),
            "{sql}"
        );
    }
    let columns: Vec<String> = displayed(rows(&mut session, "show columns from t"))
        .into_iter()
        .map(|row| row[0].clone())
        .collect();
    assert_eq!(columns, ["e", "a", "f", "c", "d"]);
    assert_eq!(
        displayed(rows(&mut session, "select * from t order by a")),
        [
            ["NULL", "1", "NULL", "1", "5"],
            ["NULL", "2", "NULL", "2", "5"]
        ]
    );
    rows(&mut session, "admin check table t");

    // A NOT NULL column without a default drops cleanly.
    rows(&mut session, "create table tn (a int, b int not null)");
    rows(&mut session, "insert into tn values (1, 1)");
    rows(&mut session, "alter table tn drop column b");
    assert_eq!(displayed(rows(&mut session, "select * from tn")), [["1"]]);

    // The partitions' entries of a dropped single-column index.
    rows(
        &mut session,
        "create table tp (a int, b int, key kb(b)) partition by hash(a) partitions 2",
    );
    let tp_parts = id_of(
        &mut session,
        "select tidb_partition_id from information_schema.partitions where table_name='tp' order by 1",
    );
    rows(&mut session, "alter table tp drop column b");

    let pending = "select count(*) from mysql.gc_delete_range";
    let started = std::time::Instant::now();
    while displayed(rows(&mut session, pending)) != [["0"]] {
        assert!(
            started.elapsed() < std::time::Duration::from_secs(30),
            "emulator stuck"
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    let done: Vec<String> = displayed(rows(
        &mut session,
        "select element_id, start_key, end_key from mysql.gc_delete_range_done order by job_id, element_id",
    ))
    .into_iter()
    .map(|row| row.join(" "))
    .collect();
    let mut drop_b_start = index(t, 1);
    drop_b_start.extend(int_key(2));
    drop_b_start.extend(int_key(2));
    drop_b_start.push(0);
    assert_eq!(
        done,
        [
            format!("1 {} {}", hex(&drop_b_start), hex(&index(t, 2))),
            format!(
                "1 {} {}",
                hex(&index(tp_parts[0], 1)),
                hex(&index(tp_parts[0], 2))
            ),
            format!(
                "2 {} {}",
                hex(&index(tp_parts[1], 1)),
                hex(&index(tp_parts[1], 2))
            ),
        ]
    );

    // A CHECK reading a dropped column stays in the stored metadata but is
    // gone from every reader.
    rows(&mut session, "set global tidb_enable_check_constraint=1");
    rows(
        &mut session,
        "create table tc (a int, b int check (b > 0), c int, constraint cc check (c > a), constraint cb2 check (b < 100))",
    );
    let error = session
        .node_conn()
        .execute_write("alter table tc drop column a")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (
            3959,
            "Check constraint 'cc' uses column 'a', hence column cannot be dropped or renamed."
        )
    );
    rows(&mut session, "alter table tc drop column b");
    let created = displayed(rows(&mut session, "show create table tc")).remove(0);
    assert_eq!(
        created[1],
        "CREATE TABLE `tc` (\n  `a` int DEFAULT NULL,\n  `c` int DEFAULT NULL,\n  CONSTRAINT `cc` CHECK ((`c` > `a`))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    );
    let error = session
        .node_conn()
        .execute_write("insert into tc values (1, 0)")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (3819, "Check constraint 'cc' is violated.")
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "select * from information_schema.check_constraints where constraint_schema = 'test'"
        )),
        [["def", "test", "cc", "(`c` > `a`)"]]
    );
}

/// CREATE DATABASE and the ALTER TABLE jobs Go finishes in one step run as
/// persisted jobs: a no-op queues nothing, CREATE DATABASE leaves no
/// notifier event, and each job's type and state are Go's. Captured from
/// the Go oracle.
#[test]
fn one_step_alters_and_create_database_run_as_persisted_jobs_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1942)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    let last_job = |session: &mut _| {
        let row = displayed(rows(session, "admin show ddl jobs 1")).remove(0);
        [0, 3, 4, 7, 11].map(|column| row[column].clone()).join("|")
    };
    let job_kind = |job: &str| job.split_once('|').unwrap().1.to_owned();
    let notifier_rows = |session: &mut _| {
        displayed(rows(
            session,
            "select count(*) from mysql.tidb_ddl_notifier",
        ))[0][0]
            .clone()
    };
    let notified = notifier_rows(&mut session);
    rows(&mut session, "create database d1");
    assert_eq!(
        job_kind(&last_job(&mut session)),
        "create schema|public|0|synced"
    );
    assert_eq!(notifier_rows(&mut session), notified);
    let before = last_job(&mut session);
    rows(&mut session, "create database if not exists d1");
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        [[
            "Note",
            "1007",
            "Can't create database 'd1'; database exists"
        ]]
    );
    assert_eq!(last_job(&mut session), before);

    rows(&mut session, "create table p (id int primary key)");
    rows(
        &mut session,
        "create table t (a int, b int, c varchar(10), key ka(a), key kb(b), constraint fk foreign key (a) references p(id))",
    );
    for (sql, job) in [
        (
            "alter table t comment 'hello'",
            Some("modify table comment|public|0|synced"),
        ),
        (
            "alter table t alter index kb invisible",
            Some("alter index visibility|public|0|synced"),
        ),
        ("alter table t alter index kb invisible", None),
        (
            "alter table t rename index kb to kb2",
            Some("rename index|public|0|synced"),
        ),
        ("alter table t rename index kb2 to kb2", None),
        (
            "alter table t drop foreign key fk",
            Some("drop foreign key|none|0|synced"),
        ),
        (
            "alter table t alter column b set default 7",
            Some("set default value|public|0|synced"),
        ),
        (
            "alter table t alter column b drop default",
            Some("set default value|public|0|synced"),
        ),
        (
            "alter table t shard_row_id_bits = 4",
            Some("shard row ID|public|0|synced"),
        ),
        ("alter table t shard_row_id_bits = 4", None),
        (
            "alter table t auto_id_cache = 100",
            Some("modify auto id cache|public|0|synced"),
        ),
    ] {
        let before = last_job(&mut session);
        rows(&mut session, sql);
        let after = last_job(&mut session);
        match job {
            Some(job) => {
                assert_ne!(after, before, "{sql}");
                assert_eq!(job_kind(&after), job, "{sql}");
            }
            None => assert_eq!(after, before, "{sql}"),
        }
    }
    for (sql, code, message) in [
        (
            "alter table t alter index nosuch visible",
            1176,
            "Key 'nosuch' doesn't exist in table 't'",
        ),
        (
            "alter table t rename index nosuch to x",
            1176,
            "Key 'nosuch' doesn't exist in table 't'",
        ),
        (
            "alter table t rename index kb2 to ka",
            1061,
            "Duplicate key name 'ka'",
        ),
        (
            "alter table t drop foreign key fk",
            1091,
            "Can't DROP 'fk'; check that column/key exists",
        ),
        (
            "alter table t alter column nosuch set default 1",
            1054,
            "Unknown column 'nosuch' in 't'",
        ),
        (
            "alter table t convert to character set latin1",
            8200,
            "Unsupported modify charset from utf8mb4 to latin1",
        ),
    ] {
        let error = session.node_conn().execute_write(sql).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (code, message),
            "{sql}"
        );
    }
    let created = displayed(rows(&mut session, "show create table t")).remove(0);
    assert_eq!(
        created[1],
        "CREATE TABLE `t` (\n  `a` int DEFAULT NULL,\n  `b` int,\n  `c` varchar(10) DEFAULT NULL,\n  KEY `ka` (`a`),\n  KEY `kb2` (`b`) /*!80000 INVISIBLE */\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![auto_id_cache] AUTO_ID_CACHE=100 */ /*T! SHARD_ROW_ID_BITS=4 */ COMMENT='hello'"
    );
}

/// RENAME TABLE runs as Go's persisted `ActionRenameTable` /
/// `ActionRenameTables` jobs: the foreign keys naming a renamed parent
/// follow it, a chain inside one statement settles on its last name, and
/// the refusals are the executor's. Captured from the Go oracle.
#[test]
fn rename_tables_run_as_persisted_jobs_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1943)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    let last_job = |session: &mut _| {
        let row = displayed(rows(session, "admin show ddl jobs 1")).remove(0);
        [0, 1, 2, 3, 4, 7, 11]
            .map(|column| row[column].clone())
            .join("|")
    };
    let after_id = |job: &str| job.split_once('|').unwrap().1.to_owned();
    rows(&mut session, "create database d2");
    rows(&mut session, "create table p (id int primary key)");
    rows(
        &mut session,
        "create table c (id int primary key, pid int, constraint fkc foreign key (pid) references p(id))",
    );
    rows(&mut session, "insert into p values (1)");
    rows(&mut session, "insert into c values (1, 1)");
    rows(&mut session, "rename table p to p2");
    assert_eq!(
        after_id(&last_job(&mut session)),
        "test|p2|rename table|public|0|synced"
    );
    let fk_of_c = |session: &mut _| {
        let created = displayed(rows(session, "show create table c")).remove(0);
        created[1]
            .lines()
            .find(|line| line.contains("FOREIGN KEY"))
            .unwrap()
            .trim()
            .to_owned()
    };
    assert_eq!(
        fk_of_c(&mut session),
        "CONSTRAINT `fkc` FOREIGN KEY (`pid`) REFERENCES `p2` (`id`)"
    );
    let error = session
        .node_conn()
        .execute_write("insert into c values (2, 2)")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (
            1452,
            "Cannot add or update a child row: a foreign key constraint fails (`test`.`c`, CONSTRAINT `fkc` FOREIGN KEY (`pid`) REFERENCES `p2` (`id`))"
        )
    );
    rows(&mut session, "alter table p2 rename to p3");
    assert_eq!(
        after_id(&last_job(&mut session)),
        "test|p3|rename table|public|0|synced"
    );
    let before = last_job(&mut session);
    rows(&mut session, "alter table p3 rename to p3");
    assert_eq!(last_job(&mut session), before);
    rows(&mut session, "rename table p3 to d2.p4");
    assert_eq!(
        after_id(&last_job(&mut session)),
        "d2|p4|rename table|public|0|synced"
    );
    assert_eq!(
        fk_of_c(&mut session),
        "CONSTRAINT `fkc` FOREIGN KEY (`pid`) REFERENCES `d2`.`p4` (`id`)"
    );
    rows(&mut session, "create table a (x int)");
    rows(&mut session, "create table b (x int)");
    rows(&mut session, "rename table a to a1, b to b1, a1 to a2");
    assert_eq!(
        after_id(&last_job(&mut session)),
        "test|a2,b1,a2|rename tables|public|0|synced"
    );
    assert_eq!(
        displayed(rows(&mut session, "show tables")),
        [["a2"], ["b1"], ["c"]]
    );
    rows(&mut session, "create view v as select 1");
    for (sql, code, message) in [
        (
            "rename table nosuch to z",
            1146,
            "Table 'test.nosuch' doesn't exist",
        ),
        (
            "rename table a2 to b1",
            1050,
            "Table 'test.b1' already exists",
        ),
        (
            "rename table a2 to nodb.z",
            1025,
            "Error on rename of 'test.a2' to 'nodb.z' (errno: 168 - Database `nodb` doesn't exist)",
        ),
        (
            "rename table v to d2.v",
            1450,
            "Changing schema from 'test' to 'd2' is not allowed.",
        ),
    ] {
        let error = session.node_conn().execute_write(sql).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (code, message),
            "{sql}"
        );
    }
    rows(&mut session, "rename table v to v2");
    assert_eq!(
        after_id(&last_job(&mut session)),
        "test|v2|rename table|public|0|synced"
    );
    assert_eq!(displayed(rows(&mut session, "select * from v2")), [["1"]]);
    rows(&mut session, "insert into a2 values (1)");
    assert_eq!(displayed(rows(&mut session, "select * from a2")), [["1"]]);
}

/// MODIFY COLUMN with AUTO_RANDOM runs as Go's persisted
/// `ActionModifyColumn` job (`checkAutoRandom` at admission, the owner's
/// `checkAndApplyAutoRandomBits`): an AUTO_INCREMENT clustered key converts
/// and continues past the row IDs handed out, the shard bits may grow, and
/// every other change is Go's refusal (captured from Go on this branch).
#[test]
fn auto_random_changes_run_as_modify_column_jobs_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1956)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    rows(&mut session, "set @@tidb_allow_remove_auto_inc = 1");
    let version = |session: &mut Session| -> i64 {
        displayed(rows(session, "admin show ddl"))[0][0]
            .parse()
            .unwrap()
    };
    let job = |session: &mut Session| -> String {
        let row = displayed(rows(session, "admin show ddl jobs 1")).remove(0);
        [1, 2, 3, 4, 11].map(|column| row[column].clone()).join("|")
    };
    let refused = |session: &mut Session, sql: &str| -> (u16, String) {
        let error = session.node_conn().execute_write(sql).unwrap_err();
        (error.code, error.message)
    };
    rows(
        &mut session,
        "create table t (a bigint auto_increment primary key, b int)",
    );
    rows(&mut session, "insert into t (b) values (1),(2)");
    for sql in [
        "alter table t modify a bigint auto_random(5)",
        "alter table t modify a bigint auto_random(6)",
        "alter table t modify a bigint auto_random(6) comment 'x'",
    ] {
        let before = version(&mut session);
        rows(&mut session, sql);
        assert_eq!(version(&mut session) - before, 1, "{sql}");
        assert_eq!(
            job(&mut session),
            "test|t|modify column|public|synced",
            "{sql}"
        );
    }
    let shown = displayed(rows(&mut session, "show create table t"))
        .remove(0)
        .remove(1);
    assert!(
        shown.contains("`a` bigint NOT NULL /*T![auto_rand] AUTO_RANDOM(6) */ COMMENT 'x'"),
        "{shown}"
    );
    // The converted column continues past the AUTO_INCREMENT IDs.
    rows(&mut session, "insert into t (b) values (3)");
    let ids = displayed(rows(
        &mut session,
        "select a & 0x7FFFFFFFFFF from t order by b",
    ));
    let third: i64 = ids[2][0].parse().unwrap();
    assert_eq!((ids[0][0].as_str(), ids[1][0].as_str()), ("1", "2"));
    assert!(third > 2, "{ids:?}");
    let invalid = |reason: &str| (8216, format!("Invalid auto random: {reason}"));
    for (sql, expected) in [
        (
            "alter table t modify a bigint auto_random(4)",
            invalid("decreasing auto_random shard bits is not supported"),
        ),
        (
            "alter table t modify a bigint auto_random(16)",
            invalid("max allowed auto_random shard bits is 15, but got 16 on column `a`"),
        ),
        (
            "alter table t modify a bigint auto_random(6, 32)",
            invalid("alter the range bits of auto_random column is not supported"),
        ),
        (
            "alter table t modify a int auto_random(6)",
            (
                8200,
                "Unsupported modify column: this column has primary key flag".to_owned(),
            ),
        ),
        (
            "alter table t modify a bigint auto_random(6) default 3",
            invalid("auto_random is incompatible with default"),
        ),
        (
            "alter table t modify a bigint",
            invalid("adding/dropping/modifying auto_random is not supported"),
        ),
    ] {
        assert_eq!(refused(&mut session, sql), expected, "{sql}");
    }
    rows(&mut session, "create table u (a bigint primary key, b int)");
    rows(
        &mut session,
        "create table v (a bigint auto_increment primary key nonclustered, b int)",
    );
    for sql in [
        "alter table u modify a bigint auto_random(5)",
        "alter table v modify a bigint auto_random(5)",
    ] {
        assert_eq!(
            refused(&mut session, sql),
            invalid("auto_random can only be converted from auto_increment clustered primary key"),
            "{sql}"
        );
    }
}

/// Table placement, attributes, the region split policy, TiFlash replicas
/// and ALTER TABLE CACHE run as Go's persisted jobs: CACHE and NOCACHE pass
/// through `TableCacheStatusSwitching`, a schema version each step (oracle:
/// ADMIN SHOW DDL's SCHEMA_VER moves by 2 for each, by 1 for ATTRIBUTES),
/// and a repeated CACHE queues nothing. Captured from the Go oracle.
#[test]
fn table_options_run_as_persisted_jobs_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1948)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    let job = |session: &mut _| -> (i64, String) {
        let row = displayed(rows(session, "admin show ddl jobs 1")).remove(0);
        (
            row[0].parse().unwrap(),
            [1, 2, 3, 4, 11].map(|column| row[column].clone()).join("|"),
        )
    };
    let version = |session: &mut _| -> i64 {
        displayed(rows(session, "admin show ddl"))[0][0]
            .parse()
            .unwrap()
    };
    rows(&mut session, "create table c1 (a int, b int, key ib(b))");
    rows(&mut session, "create placement policy p1 followers=2");
    for (sql, expected) in [
        (
            "alter table c1 placement policy p1",
            "test|c1|alter table placement|public|synced",
        ),
        (
            "alter table c1 placement policy default",
            "test|c1|alter table placement|public|synced",
        ),
        (
            "alter table c1 split index ib between (0) and (100) regions 2",
            "test|c1|alter table set region split policy|public|synced",
        ),
        (
            "alter table c1 set tiflash replica 0",
            "test|c1|set tiflash replica|public|synced",
        ),
    ] {
        rows(&mut session, sql);
        assert_eq!(job(&mut session).1, expected, "{sql}");
    }
    let before = version(&mut session);
    rows(
        &mut session,
        "alter table c1 attributes 'merge_option=deny'",
    );
    assert_eq!(
        job(&mut session).1,
        "test|c1|alter table attributes|public|synced"
    );
    assert_eq!(version(&mut session), before + 1);

    let before = version(&mut session);
    rows(&mut session, "alter table c1 cache");
    let (cached, kind) = job(&mut session);
    assert_eq!(kind, "test|c1|alter table cache|public|synced");
    assert_eq!(version(&mut session), before + 2);
    rows(&mut session, "alter table c1 cache");
    assert_eq!(job(&mut session).0, cached, "already cached: no job");
    let before = version(&mut session);
    rows(&mut session, "alter table c1 nocache");
    assert_eq!(
        job(&mut session).1,
        "test|c1|alter table nocache|public|synced"
    );
    assert_eq!(version(&mut session), before + 2);

    rows(
        &mut session,
        "create table tp (a int) partition by range (a) \
         (partition p0 values less than (10), partition p1 values less than (20))",
    );
    rows(
        &mut session,
        "alter table tp partition p0 placement policy p1",
    );
    assert_eq!(
        job(&mut session).1,
        "test|tp|alter table partition placement|public|synced"
    );
    let error = session
        .node_conn()
        .execute_write("alter table tp cache")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (8242, "'partition mode' is unsupported on cache tables.")
    );
}

/// Go `TestAlterSchemaOptions` (zzprobe/d01): session collation and validation order.
#[test]
fn test_alter_schema_options() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1944)).unwrap();
    rows(
        &mut session,
        "create database d01_schema character set latin1",
    );
    rows(
        &mut session,
        "set default_collation_for_utf8mb4='utf8mb4_general_ci'",
    );
    rows(
        &mut session,
        "alter database d01_schema character set utf8mb4",
    );
    let metadata = "select default_character_set_name,default_collation_name from information_schema.schemata where schema_name='d01_schema'";
    assert_eq!(
        displayed(rows(&mut session, metadata)),
        [["utf8mb4", "utf8mb4_general_ci"]]
    );
    let error = session
        .node_conn()
        .execute_write(
            "alter database d01_schema charset latin1 collate utf8mb4_bin placement policy missing",
        )
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (
            1302,
            "Conflicting declarations: 'CHARACTER SET latin1' and 'CHARACTER SET utf8mb4'"
        )
    );
    assert_eq!(
        displayed(rows(&mut session, metadata)),
        [["utf8mb4", "utf8mb4_general_ci"]]
    );
}

/// A multi-schema ALTER runs as Go's persisted `ActionMultiSchemaChange`
/// job: ADMIN SHOW DDL JOBS lists every sub-job under it (with the comments
/// column one row down, as Go writes it), a sub-job that fails rolls every
/// sub-job back, an ADD FOREIGN KEY with no index for its columns is a
/// multi-schema change of its own (the index first), a key the rows break
/// rolls back, and AUTO_INCREMENT is rebased by its own job. Captured from
/// the Go oracle.
#[test]
fn multi_schema_alter_runs_as_a_persisted_job_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1945)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    let jobs = |session: &mut _| -> Vec<String> {
        displayed(rows(session, "admin show ddl jobs 1"))
            .into_iter()
            .map(|row| {
                assert_ne!(row[9], "NULL", "every sub-job ran: {row:?}");
                [3, 4, 7, 11, 12]
                    .map(|column| row[column].clone())
                    .join("|")
            })
            .collect()
    };
    rows(
        &mut session,
        "create table t (a int, b int, c int, key kc(c))",
    );
    rows(&mut session, "insert into t values (1,1,1),(2,2,2)");
    rows(
        &mut session,
        "alter table t add column d int, add index kb(b), modify column a bigint, drop index kc",
    );
    assert_eq!(
        jobs(&mut session),
        [
            "alter table multi-schema change|none|0|synced|",
            "add column /* subjob */|public|0|done|txn-merge, DXF",
            "add index /* subjob */|public|2|done|",
            "modify column /* subjob */|public|0|done|",
            "drop index /* subjob */|none|0|done|max_node_count=3",
        ]
    );
    assert_eq!(
        displayed(rows(&mut session, "select * from t")),
        [["1", "1", "1", "NULL"], ["2", "2", "2", "NULL"]]
    );
    rows(&mut session, "update t set c=1");
    let error = session
        .node_conn()
        .execute_write("alter table t add column f int, add unique index uc(c)")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1062, "Duplicate entry '1' for key 't.uc'")
    );
    assert_eq!(
        jobs(&mut session),
        [
            "alter table multi-schema change|none|0|rollback done|",
            "add column /* subjob */|none|0|rollback done|txn-merge, DXF",
            "add index /* subjob */|none|0|rollback done|max_node_count=3",
        ]
    );
    assert_eq!(
        displayed(rows(&mut session, "select * from t")),
        [["1", "1", "1", "NULL"], ["2", "2", "1", "NULL"]]
    );
    // Go `fillMultiSchemaInfo` has no CHECK sub-job.
    rows(&mut session, "set global tidb_enable_check_constraint = 1");
    let error = session
        .node_conn()
        .execute_write("alter table t add column j int, add constraint chk check (a > 0)")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (
            8200,
            "Unsupported multi schema change for add check constraint"
        )
    );
    rows(&mut session, "set global tidb_enable_check_constraint = 0");
    rows(&mut session, "alter table t add column (h int, i int)");
    assert_eq!(
        jobs(&mut session),
        [
            "alter table multi-schema change|none|0|synced|",
            "add column /* subjob */|public|0|done|",
            "add column /* subjob */|public|0|done|",
        ]
    );

    rows(&mut session, "create table p (id int primary key)");
    rows(&mut session, "create table ch (id int, pid int)");
    rows(
        &mut session,
        "alter table ch add foreign key (pid) references p(id)",
    );
    assert_eq!(
        jobs(&mut session),
        [
            "alter table multi-schema change|none|0|synced|txn-merge, DXF",
            "add index /* subjob */|public|0|done|",
            "add foreign key /* subjob */|public|0|done|max_node_count=3",
        ]
    );
    rows(
        &mut session,
        "create table ch2 (id int, pid int, key k(pid))",
    );
    rows(&mut session, "insert into ch2 values (1, 5)");
    let error = session
        .node_conn()
        .execute_write("alter table ch2 add constraint fk2 foreign key (pid) references p(id)")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (
            1452,
            "Cannot add or update a child row: a foreign key constraint fails (`test`.`ch2`, \
             CONSTRAINT `fk2` FOREIGN KEY (`pid`) REFERENCES `p` (`id`))"
        )
    );
    assert_eq!(
        jobs(&mut session),
        ["add foreign key|none|0|rollback done|"]
    );
    rows(&mut session, "insert into p values (5)");
    rows(
        &mut session,
        "alter table ch2 add constraint fk2 foreign key (pid) references p(id)",
    );
    assert_eq!(jobs(&mut session), ["add foreign key|public|0|synced|"]);

    rows(
        &mut session,
        "create table ai (id int primary key auto_increment)",
    );
    rows(&mut session, "insert into ai values (10)");
    rows(&mut session, "alter table ai auto_increment = 100");
    assert_eq!(
        jobs(&mut session),
        ["rebase auto_increment ID|public|0|synced|"]
    );
    rows(
        &mut session,
        "alter table ai auto_increment = 5000000, comment 'x'",
    );
    assert_eq!(
        jobs(&mut session),
        [
            "alter table multi-schema change|none|0|synced|",
            "rebase auto_increment ID /* subjob */|public|0|done|",
            "modify table comment /* subjob */|public|0|done|",
        ]
    );
    rows(&mut session, "insert into ai values ()");
    assert_eq!(
        displayed(rows(&mut session, "select max(id) from ai")),
        [["5000000"]]
    );
    rows(&mut session, "alter table ai force auto_increment = 3");
    rows(&mut session, "insert into ai values ()");
    assert_eq!(
        displayed(rows(&mut session, "select id from ai order by id")),
        [["3"], ["10"], ["5000000"]]
    );
}

/// MODIFY / CHANGE / RENAME COLUMN run as Go's persisted `ActionModifyColumn`
/// job through each of `onModifyColumn`'s courses: metadata only, a checked
/// narrowing (rolled back on a row that does not fit), a VARCHAR -> CHAR
/// precheck that falls back to a rewrite, and rewrites with their indexes
/// rebuilt (new index ids, the old entries and the merge's temporary index
/// left to the delete range). A non-strict rewrite reports its warning.
/// Captured from the Go oracle.
#[test]
fn modify_column_runs_as_a_persisted_job_like_go() {
    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }
    let index = |id: i64, index: i64| tidb_tablecodec::table_key::encode_table_index_prefix(id, index);
    const TEMP: i64 = 0x7fff_0000_0000_0000;
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1944)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    let last_job = |session: &mut _| {
        let row = displayed(rows(session, "admin show ddl jobs 1")).remove(0);
        [0, 3, 4, 7, 11].map(|column| row[column].clone()).join("|")
    };
    let kind = |job: &str| job.split_once('|').unwrap().1.to_owned();
    rows(
        &mut session,
        "create table t (a int, b varchar(10), c int, d varchar(10), key kc(c), key kd(d))",
    );
    rows(
        &mut session,
        "insert into t values (1, 'x', 1, 'p'), (2, 'yy ', 300, 'q '), (null, null, null, null)",
    );
    let t: i64 = displayed(rows(
        &mut session,
        "select tidb_table_id from information_schema.tables where table_name='t'",
    ))[0][0]
        .parse()
        .unwrap();
    for (sql, job) in [
        (
            "alter table t modify b varchar(20)",
            "modify column|public|0|synced",
        ),
        (
            "alter table t modify c bigint",
            "modify column|public|0|synced",
        ),
        (
            "alter table t change c c2 bigint",
            "modify column|public|0|synced",
        ),
        (
            "alter table t rename column c2 to c",
            "modify column|public|0|synced",
        ),
        (
            "alter table t modify a int first",
            "modify column|public|0|synced",
        ),
    ] {
        rows(&mut session, sql);
        assert_eq!(kind(&last_job(&mut session)), job, "{sql}");
    }
    let error = session
        .node_conn()
        .execute_write("alter table t modify c tinyint")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1265, "Data truncated for column 'c', value is '300'")
    );
    assert_eq!(
        kind(&last_job(&mut session)),
        "modify column|none|0|rollback done"
    );
    rows(&mut session, "alter table t modify c smallint");
    assert_eq!(
        kind(&last_job(&mut session)),
        "modify column|public|0|synced"
    );
    let error = session
        .node_conn()
        .execute_write("alter table t modify a int not null")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1265, "Data truncated for column 'a' at row 1")
    );
    rows(&mut session, "update t set a = 3 where a is null");
    for (sql, job) in [
        (
            "alter table t modify a int not null",
            "modify column|public|0|synced",
        ),
        // VARCHAR -> CHAR over a trailing space: the precheck fails and the
        // rows are rewritten.
        (
            "alter table t modify b char(10)",
            "modify column|public|3|synced",
        ),
        (
            "alter table t modify d char(10)",
            "modify column|public|6|synced",
        ),
        (
            "alter table t modify c varchar(10)",
            "modify column|public|6|synced",
        ),
    ] {
        rows(&mut session, sql);
        assert_eq!(kind(&last_job(&mut session)), job, "{sql}");
    }
    assert_eq!(
        displayed(rows(
            &mut session,
            "select key_name, index_id from information_schema.tidb_indexes where table_name='t' order by key_name"
        )),
        [["kc", "4"], ["kd", "3"]]
    );
    let columns: Vec<String> = displayed(rows(&mut session, "show columns from t"))
        .into_iter()
        .map(|row| format!("{} {}", row[0], row[1]))
        .collect();
    assert_eq!(
        columns,
        ["a int", "b char(10)", "c varchar(10)", "d char(10)"]
    );
    assert_eq!(
        displayed(rows(&mut session, "select * from t order by a")),
        [
            ["1", "x", "1", "p"],
            ["2", "yy", "300", "q"],
            ["3", "NULL", "NULL", "NULL"]
        ]
    );
    rows(&mut session, "admin check table t");

    rows(&mut session, "create table s (a varchar(10))");
    rows(&mut session, "insert into s values ('abc'), ('12')");
    let error = session
        .node_conn()
        .execute_write("alter table s modify a int")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1292, "Truncated incorrect DOUBLE value: 'abc'")
    );
    assert_eq!(
        kind(&last_job(&mut session)),
        "modify column|none|0|rollback done"
    );
    rows(&mut session, "set sql_mode=''");
    rows(&mut session, "alter table s modify a int");
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        [["Warning", "1292", "Truncated incorrect DOUBLE value: 'abc'"]]
    );
    assert_eq!(
        kind(&last_job(&mut session)),
        "modify column|public|2|synced"
    );
    assert_eq!(
        displayed(rows(&mut session, "select * from s order by a")),
        [["0"], ["12"]]
    );
    rows(&mut session, "set sql_mode=default");
    rows(&mut session, "create table r (a int, b int)");
    for (sql, code, message) in [
        (
            "alter table s modify nosuch int",
            1054,
            "Unknown column 'nosuch' in 's'",
        ),
        (
            "alter table s change a a int after nosuch",
            1054,
            "Unknown column 'nosuch' in 'generated column function'",
        ),
        (
            "alter table r modify a int after a",
            1054,
            "Unknown column 'a' in 'r'",
        ),
    ] {
        let error = session.node_conn().execute_write(sql).unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (code, message),
            "{sql}"
        );
    }
    // Go queues the self-anchored MODIFY and rolls it back at the owner.
    assert_eq!(
        kind(&last_job(&mut session)),
        "modify column|none|0|rollback done"
    );

    // The rewrites' old index entries and the merge's temporary indexes.
    let pending = "select count(*) from mysql.gc_delete_range";
    let started = std::time::Instant::now();
    while displayed(rows(&mut session, pending)) != [["0"]] {
        assert!(started.elapsed() < std::time::Duration::from_secs(30));
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    let done: Vec<String> = displayed(rows(
        &mut session,
        "select element_id, end_key from mysql.gc_delete_range_done order by job_id, element_id",
    ))
    .into_iter()
    .map(|row| row.join(" "))
    .collect();
    assert_eq!(
        done,
        [
            format!("1 {}", hex(&index(t, 3))),
            format!("2 {}", hex(&index(t, TEMP | 3 + 1))),
            format!("1 {}", hex(&index(t, 2))),
            format!("2 {}", hex(&index(t, TEMP | 4 + 1))),
        ]
    );
    // Go `updateColumnWorker.getRowRecord`: a NULL bound for a TIMESTAMP NOT
    // NULL column takes the current time, read in the job's time zone (Go
    // issue 56051; oracle: both answer 0).
    rows(&mut session, "create table tz (a int, b int)");
    rows(&mut session, "insert into tz values (null, null)");
    // The filled-in time lies between the clock readings around the ALTER,
    // however long the job took (a zone applied wrongly is hours off).
    let seconds = |session: &mut Session, sql: &str| -> i64 {
        displayed(rows(session, sql))[0][0].parse().expect("a unix timestamp")
    };
    let before = seconds(&mut session, "select unix_timestamp()");
    rows(&mut session, "alter table tz modify a timestamp not null");
    let filled = seconds(&mut session, "select unix_timestamp(a) from tz");
    let after = seconds(&mut session, "select unix_timestamp()");
    assert!((before..=after).contains(&filled), "{before} <= {filled} <= {after}");
    rows(&mut session, "set @@time_zone = 'Asia/Tokyo'");
    let before = seconds(&mut session, "select unix_timestamp()");
    rows(&mut session, "alter table tz modify b timestamp not null");
    let filled = seconds(&mut session, "select unix_timestamp(b) from tz");
    let after = seconds(&mut session, "select unix_timestamp()");
    assert!((before..=after).contains(&filled), "{before} <= {filled} <= {after}");
}

/// TRUNCATE TABLE runs as Go's persisted job (`executor.TruncateTable`, the
/// owner's `onTruncateTable`): new table and partition ids, the allocators
/// restarted, the refusals the executor's. Captured from the Go oracle.
#[test]
fn truncate_table_runs_as_a_persisted_job_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1941)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    let last_job = |session: &mut _| {
        let row = displayed(rows(session, "admin show ddl jobs 1")).remove(0);
        [3, 2, 4, 7, 11].map(|column| row[column].clone()).join("|")
    };
    rows(
        &mut session,
        "create table t (a int auto_increment primary key, b int)",
    );
    rows(&mut session, "insert into t (b) values (1),(2)");
    rows(&mut session, "truncate table t");
    assert_eq!(last_job(&mut session), "truncate table|t|public|0|synced");
    rows(&mut session, "insert into t (b) values (3)");
    assert_eq!(
        displayed(rows(&mut session, "select * from t")),
        [["1", "3"]]
    );
    rows(&mut session, "create table p (id int primary key)");
    rows(
        &mut session,
        "create table c (id int primary key, pid int, constraint fkc foreign key (pid) references p(id))",
    );
    rows(&mut session, "create view v as select 1");
    for (sql, code, message) in [
        (
            "truncate table p",
            1701,
            "Cannot truncate a table referenced in a foreign key constraint (`test`.`c` CONSTRAINT `fkc`)",
        ),
        ("truncate table v", 1146, "Table 'test.v' doesn't exist"),
        ("truncate table nosuch", 1146, "Table 'test.nosuch' doesn't exist"),
    ] {
        let error = session.node_conn().execute_write(sql).unwrap_err();
        assert_eq!((error.code, error.message.as_str()), (code, message), "{sql}");
    }
    rows(&mut session, "truncate table c");
    rows(
        &mut session,
        "create table tp (a int) partition by hash(a) partitions 2",
    );
    rows(&mut session, "insert into tp values (1),(2)");
    rows(&mut session, "truncate table tp");
    assert_eq!(last_job(&mut session), "truncate table|tp|public|0|synced");
    assert_eq!(
        displayed(rows(&mut session, "select count(*) from tp")),
        [["0"]]
    );
}

/// Go `kv.Storage.GetClient()`: a storage session's tables reach the node's
/// coprocessor client, which `ADMIN CHECKSUM TABLE` sends through. A session
/// opened without it answers unistore's in-process stub instead of
/// checksumming the regions.
#[test]
fn a_storage_sessions_tables_carry_the_nodes_coprocessor_client() {
    let (stack, _users) = cop_backed_stack();
    let mut opened = stack
        .factory
        .open_storage_session(141)
        .expect("storage session");
    // Go: the session's transaction (`sctx.Txn()`) carries the client every
    // table read goes through.
    let has_client = opened
        .transaction_store()
        .is_some_and(|store| store.client().is_some());
    assert!(has_client);
}

/// Go `JobSubmitter`: the CREATE TABLEs eight sessions submit at once go
/// through the node's one submit loop, which may merge those queued together
/// into CREATE TABLES jobs (`tidb_enable_fast_create_table`, on by default).
/// Merged or not, every statement succeeds with its own table, and the job
/// history accounts for each table once.
#[test]
fn concurrent_create_tables_go_through_one_submit_loop() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let barrier = std::sync::Barrier::new(8);
    std::thread::scope(|scope| {
        for worker in 0u64..8 {
            let barrier = &barrier;
            scope.spawn(move || {
                let mut session = factory
                    .open_session(session_context(170 + worker))
                    .expect("session opens");
                barrier.wait();
                rows(&mut session, &format!("CREATE TABLE test.fast_{worker} (id int primary key)"));
                rows(&mut session, &format!("INSERT INTO test.fast_{worker} VALUES ({worker})"));
            });
        }
    });
    let mut session = factory
        .open_session(session_context(179))
        .expect("session opens");
    for worker in 0..8 {
        assert_eq!(
            rows(&mut session, &format!("SELECT id FROM test.fast_{worker}"))[0][0].get_int64(),
            worker
        );
    }
    let jobs = displayed(rows(
        &mut session,
        "SELECT job_type, table_name FROM information_schema.ddl_jobs \
         WHERE table_name LIKE 'fast_%' OR job_type = 'create tables'",
    ));
    // A CREATE TABLES row names its tables, comma-separated.
    let mut created: Vec<String> = jobs
        .iter()
        .flat_map(|job| {
            assert!(job[0] == "create table" || job[0] == "create tables", "{jobs:?}");
            job[1].split(',').map(str::to_owned).collect::<Vec<_>>()
        })
        .collect();
    created.sort();
    assert_eq!(created, (0..8).map(|worker| format!("fast_{worker}")).collect::<Vec<_>>(), "{jobs:?}");
}

/// Go `executeFlashbackTable` / `executeRecoverTable` and `onRecoverTable`,
/// pinned to gorun: a dropped table comes back under its id with fresh
/// allocators over its recovered base (Go's mock store has already run the
/// drop's delete range, so the rows are gone and the next AUTO_INCREMENT id
/// is the persisted batch end's next), a second FLASHBACK is refused, `TO`
/// renames, RECOVER finds the drop by name or BY JOB, and GC is on again.
#[test]
fn flashback_and_recover_table_bring_a_dropped_table_back_as_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(182))
        .expect("session opens");
    fn query(session: &mut Session, sql: &str) -> Vec<Vec<String>> {
        displayed(rows(session, sql))
    }
    let auto_increment = "select auto_increment from information_schema.tables where table_name = 'rt'";
    for sql in [
        "replace into mysql.tidb values ('tikv_gc_safe_point', '20000101-00:00:00 +0000', '')",
        "create table test.rt (id int primary key auto_increment, v int)",
        "insert into test.rt (v) values (10), (20)",
    ] {
        query(&mut session, sql);
    }
    let table_id = "select tidb_table_id from information_schema.tables where table_name = 'rt'";
    let id = query(&mut session, table_id);
    assert_eq!(query(&mut session, auto_increment), [["3"]]);
    query(&mut session, "drop table test.rt");
    query(&mut session, "flashback table test.rt");
    assert_eq!(query(&mut session, table_id), id);
    assert_eq!(query(&mut session, auto_increment), [["0"]]);
    query(&mut session, "insert into test.rt (v) values (30)");
    assert_eq!(query(&mut session, "select id, v from test.rt"), [["30001", "30"]]);
    assert_eq!(
        query_error(&mut session, "flashback table test.rt"),
        (
            1050,
            "Table 'rt' already been flashback to 'rt', can't be flashback repeatedly".to_owned()
        )
    );

    query(&mut session, "drop table test.rt");
    query(&mut session, "flashback table test.rt to rt2");
    assert_eq!(query(&mut session, "show tables in test"), [["rt2"]]);
    assert_eq!(query(&mut session, "select id, v from test.rt2"), Vec::<Vec<String>>::new());
    assert_eq!(
        query_error(&mut session, "flashback table test.rt to rt2"),
        (
            1050,
            "Table 'rt' already been flashback to 'rt2', can't be flashback repeatedly".to_owned()
        )
    );

    query(&mut session, "drop table test.rt2");
    query(&mut session, "recover table test.rt2");
    assert_eq!(query(&mut session, "show tables in test"), [["rt2"]]);

    query(&mut session, "drop table test.rt2");
    let job = query(
        &mut session,
        "select job_id from information_schema.ddl_jobs \
         where table_name = 'rt2' and job_type = 'drop table' order by job_id desc limit 1",
    );
    let job = &job[0][0];
    query(&mut session, &format!("recover table by job {job}"));
    assert_eq!(query(&mut session, "show tables in test"), [["rt2"]]);
    assert_eq!(
        query_error(&mut session, &format!("recover table by job {job}")),
        (
            1050,
            "Table 'rt2' already been recover to 'rt2', can't be recover repeatedly".to_owned()
        )
    );
    assert_eq!(
        query_error(&mut session, "recover table by job 99999"),
        (8224, "DDL Job:99999 not found".to_owned())
    );
    assert_eq!(
        query_error(&mut session, "recover table test.nosuch"),
        (
            1105,
            "Can't find localTemporary/dropped/truncated table: nosuch in DDL history jobs"
                .to_owned()
        )
    );
    assert_eq!(query(&mut session, "select @@global.tidb_gc_enable"), [["1"]]);
}

/// Go `TestFlashbackTable`'s foreign-key cases (b1322c5d663): a standalone
/// FLASHBACK / RECOVER cannot prove that an external foreign-key endpoint
/// still names the same table, so it is refused (a historical parent name
/// reused by a new table is a different object, and the old endpoint must
/// not become an apparent self-reference under the parent's name); a
/// self-referencing table renamed by FLASHBACK ... TO keeps referencing
/// itself.
#[test]
fn test_flashback_table_foreign_keys() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(183))
        .expect("session opens");
    fn query(session: &mut Session, sql: &str) -> Vec<Vec<String>> {
        displayed(rows(session, sql))
    }
    const ERR_UNSUPPORTED_DDL_OPERATION: u16 = 8200;
    // Go `mockGC`: the delete-range emulator stopped, so a dropped table
    // keeps its rows, and the GC safe point 48 hours back.
    tidb_ddl_util::emulator_gc_disable();
    let time_before_drop =
        (chrono::Local::now() - chrono::Duration::hours(48)).format("%Y%m%d-%H:%M:%S %z").to_string();
    query(&mut session, "delete from mysql.tidb where variable_name in ( 'tikv_gc_safe_point','tikv_gc_enable' )");
    query(
        &mut session,
        &format!(
            "INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('tikv_gc_safe_point', '{time_before_drop}', '') \
             ON DUPLICATE KEY UPDATE variable_value = '{time_before_drop}'"
        ),
    );
    for sql in [
        "drop database if exists fk_flashback",
        "create database fk_flashback",
        "use fk_flashback",
        "create table p (id int primary key)",
        "create table c (id int primary key, pid int, foreign key (pid) references p(id))",
        "insert into p values (1)",
        "insert into c values (2, 1)",
        "drop table c",
        "drop table p",
    ] {
        query(&mut session, sql);
    }
    assert_eq!(query_error(&mut session, "flashback table c to p").0, ERR_UNSUPPORTED_DDL_OPERATION);
    assert_eq!(query(&mut session, "show tables like 'p'"), Vec::<Vec<String>>::new());
    query(&mut session, "create table p (id int primary key)");
    assert_eq!(query_error(&mut session, "flashback table c").0, ERR_UNSUPPORTED_DDL_OPERATION);
    assert_eq!(query(&mut session, "show tables like 'c'"), Vec::<Vec<String>>::new());

    // The same guard applies when recovering a parent another table refers to.
    query(&mut session, "drop table p");
    query(&mut session, "set foreign_key_checks = 0");
    query(&mut session, "create table c (id int primary key, pid int, foreign key (pid) references p(id))");
    assert_eq!(query_error(&mut session, "recover table p").0, ERR_UNSUPPORTED_DDL_OPERATION);

    // Renaming a self-referencing table also renames its foreign-key endpoint.
    for sql in [
        "drop database if exists fk_self_flashback",
        "create database fk_self_flashback",
        "use fk_self_flashback",
        "set foreign_key_checks = 1",
        "create table tree (id int primary key, parent_id int, foreign key (parent_id) references tree(id) on delete cascade)",
        "insert into tree values (1, null), (2, 1)",
        "drop table tree",
        "flashback table tree to restored_tree",
    ] {
        query(&mut session, sql);
    }
    assert_eq!(
        query(
            &mut session,
            "select referenced_table_name from information_schema.key_column_usage where table_schema = 'fk_self_flashback' \
             and table_name = 'restored_tree' and referenced_table_name is not null",
        ),
        [["restored_tree"]]
    );
    query(&mut session, "insert into restored_tree values (3, 1)");
    query(&mut session, "delete from restored_tree where id = 1");
    assert_eq!(query(&mut session, "select * from restored_tree"), Vec::<Vec<String>>::new());
}

/// Go `executeFlashbackDatabase` / `onRecoverSchema`, pinned to gorun: the
/// dropped schema comes back with every table it had at its drop's
/// snapshot (listed by the job on execute), each with fresh allocators over
/// its recovered base; an existing name and an already recovered schema are
/// refused, `TO` renames, and GC is on again afterwards.
#[test]
fn flashback_database_brings_a_dropped_schema_back_as_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(183))
        .expect("session opens");
    fn query(session: &mut Session, sql: &str) -> Vec<Vec<String>> {
        displayed(rows(session, sql))
    }
    for sql in [
        "replace into mysql.tidb values ('tikv_gc_safe_point', '20000101-00:00:00 +0000', '')",
        "create database d1",
        "create table d1.t (id int primary key auto_increment, v int)",
        "create table d1.u (a int)",
        "insert into d1.t (v) values (10), (20)",
        "drop database d1",
        "flashback database d1",
    ] {
        query(&mut session, sql);
    }
    assert_eq!(query(&mut session, "show tables in d1"), [["t"], ["u"]]);
    assert_eq!(
        query(
            &mut session,
            "select auto_increment from information_schema.tables \
             where table_schema = 'd1' and table_name = 't'"
        ),
        [["0"]]
    );
    query(&mut session, "insert into d1.t (v) values (30)");
    assert_eq!(query(&mut session, "select id, v from d1.t"), [["30001", "30"]]);
    assert_eq!(
        query_error(&mut session, "flashback database d1"),
        (1007, "Can't create database 'd1'; database exists".to_owned())
    );

    query(&mut session, "drop database d1");
    query(&mut session, "flashback database d1 to d2");
    assert_eq!(
        query(
            &mut session,
            "select schema_name from information_schema.schemata where schema_name like 'd%'"
        ),
        [["d2"]]
    );
    assert_eq!(query(&mut session, "show tables in d2"), [["t"], ["u"]]);
    assert_eq!(
        query_error(&mut session, "flashback database d1 to d3"),
        (
            1007,
            "Schema 'd1' already been recover to 'd2', can't be recover repeatedly".to_owned()
        )
    );
    assert_eq!(
        query_error(&mut session, "flashback database nosuch"),
        (1105, "Can't find dropped database: nosuch in DDL history jobs".to_owned())
    );
    assert_eq!(query(&mut session, "select @@global.tidb_gc_enable"), [["1"]]);
}

/// Go `addHistoryDDLJob2Table`, run on the DDL step's session: the history
/// row names the job's schema and table, its ids as strings, and
/// `create_time` is `TSConvert2Time(StartTS).String()` cast to the
/// `datetime` column, i.e. the job's start rounded to the second, as
/// `ddl_jobs.CREATE_TIME` (the same `StartTS`) cast the same way. Expected
/// relation: gorun. (`START_TIME` is the job's `RealStartTS`, which can round
/// to the next second.)
#[test]
fn ddl_history_row_is_gos_insert_on_the_step_session() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(185)).unwrap();
    session
        .node_conn()
        .execute_write("create table test.h1 (a int)")
        .unwrap();
    let ids = displayed(rows(
        &mut session,
        "select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'h1'",
    ));
    let schema_id = stack
        .factory
        .catalog
        .get_latest()
        .schema_by_name("test")
        .unwrap()
        .read()
        .id
        .to_string();
    assert_eq!(
        displayed(rows(
            &mut session,
            "select h.db_name, h.table_name, h.schema_ids, h.table_ids, \
             h.create_time = cast(j.create_time as datetime) \
             from mysql.tidb_ddl_history h join information_schema.ddl_jobs j on h.job_id = j.job_id \
             where h.table_name = 'h1'"
        )),
        [[
            "test".to_owned(),
            "h1".to_owned(),
            schema_id,
            ids[0][0].clone(),
            "1".to_owned()
        ]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "select count(*) from mysql.tidb_ddl_job"
        )),
        [["0"]]
    );
}

/// Go `registerMDLInfo`, run on the DDL step's session: while a transaction
/// holds the table's metadata lock, the ALTER waits on schema sync with its
/// `mysql.tidb_mdl_info` row (`table_ids` read back from
/// `mysql.tidb_ddl_job`) published; once the reader commits the DDL finishes
/// and `cleanMDLInfo` removes the row.
#[test]
fn ddl_mdl_info_row_is_gos_replace_on_the_step_session() {
    let (stack, _users) = cop_backed_stack();
    let mut reader = stack.factory.open_session(session_context(187)).unwrap();
    let mut probe = stack.factory.open_session(session_context(188)).unwrap();
    rows(&mut reader, "create table test.mdl1 (a int)");
    let table_id = displayed(rows(
        &mut probe,
        "select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'mdl1'",
    ))[0][0]
        .clone();
    rows(&mut reader, "begin");
    rows(&mut reader, "select * from test.mdl1");
    let factory = Arc::clone(&stack.factory);
    let alter = std::thread::spawn(move || {
        let mut writer = factory.open_session(session_context(189)).unwrap();
        rows(&mut writer, "alter table test.mdl1 add column b int");
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let registered = displayed(rows(&mut probe, "select table_ids from mysql.tidb_mdl_info"));
        if registered == [[table_id.clone()]] {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the waiting ALTER never published its MDL row: {registered:?}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    rows(&mut reader, "commit");
    alter.join().unwrap();
    assert_eq!(
        displayed(rows(&mut probe, "select count(*) from mysql.tidb_mdl_info")),
        [["0"]]
    );
}

/// Go `finishDDLJob` -> `delRange.addDelRangeJob`: a finished DROP TABLE's
/// `mysql.gc_delete_range` row is Go's `INSERT IGNORE` on a pooled session
/// in its own transaction, stamped with a fresh timestamp (`getNowTSO`), so
/// its `ts` is later than the finishing step's start (the history job's
/// `FinishedTS`); gorun answers `FinishedTS + 1`. The emulator may already
/// have moved the row to `mysql.gc_delete_range_done`.
#[test]
fn ddl_delete_range_row_is_gos_insert_on_a_pooled_session() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(190)).unwrap();
    rows(&mut session, "create table test.dr1 (a int)");
    rows(&mut session, "drop table test.dr1");
    let finished = displayed(rows(
        &mut session,
        "select job_id, json_extract(cast(job_meta as char), '$.binlog.FinishedTS') from mysql.tidb_ddl_history order by job_id desc limit 1",
    ));
    let (job_id, finished_ts) = (&finished[0][0], finished[0][1].parse::<u64>().unwrap());
    let recorded = displayed(rows(
        &mut session,
        &format!(
            "select element_id, ts from mysql.gc_delete_range where job_id = {job_id} \
             union all select element_id, ts from mysql.gc_delete_range_done where job_id = {job_id}"
        ),
    ));
    assert_eq!(recorded.len(), 1, "{recorded:?}");
    assert_eq!(recorded[0][0], "1");
    let ts = recorded[0][1].parse::<u64>().unwrap();
    assert!(ts > finished_ts, "delete-range ts {ts} is not after FinishedTS {finished_ts}");
}

/// Go `CommandDDLJobsExec` over `ddl.processJobs`: ADMIN PAUSE / RESUME /
/// CANCEL DDL JOBS rewrite a live job's row in `mysql.tidb_ddl_job`. An
/// ALTER waiting on a reader's metadata lock (its first step committed,
/// its MDL row published) is such a job. PAUSE leaves it `pausing` until
/// the worker's next step, so RESUME is refused (Go `IsResumable` wants
/// `paused`); CANCEL takes it, its next step rolls it back and the ALTER
/// answers `Cancelled DDL job`. An id with no job answers Go's not-found
/// error.
#[test]
fn admin_commands_on_a_live_ddl_job_as_go() {
    let (stack, _users) = cop_backed_stack();
    let mut reader = stack.factory.open_session(session_context(191)).unwrap();
    let mut probe = stack.factory.open_session(session_context(192)).unwrap();
    rows(&mut reader, "create table test.jc1 (a int)");
    rows(&mut reader, "begin");
    rows(&mut reader, "select * from test.jc1");
    let factory = Arc::clone(&stack.factory);
    let alter = std::thread::spawn(move || {
        let mut writer = factory.open_session(session_context(193)).unwrap();
        let result = writer
            .node_conn()
            .execute_write("alter table test.jc1 add column b int")
            .map(|_| ())
            .map_err(|error| (error.code, error.message));
        result
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let job_id = loop {
        let jobs = displayed(rows(&mut probe, "select job_id from mysql.tidb_mdl_info"));
        if let [job] = jobs.as_slice() {
            break job[0].clone();
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the ALTER never waited on the reader: {jobs:?}"
        );
        std::thread::sleep(Duration::from_millis(20));
    };
    let command = |probe: &mut Session, sql: String| displayed(rows(probe, &sql));
    assert_eq!(
        command(&mut probe, format!("admin pause ddl jobs {job_id}")),
        [[job_id.clone(), "successful".to_owned()]]
    );
    assert_eq!(
        command(&mut probe, format!("admin pause ddl jobs {job_id}")),
        [[
            job_id.clone(),
            format!("error: [ddl:8262]Job [{job_id}] has already been paused")
        ]]
    );
    assert_eq!(
        command(&mut probe, format!("admin resume ddl jobs {job_id}")),
        [[
            job_id.clone(),
            format!(
                "error: [ddl:8261]Job [{job_id}] can't be resumed: job has not been paused, \
                 job state:pausing, schema state:delete only"
            )
        ]]
    );
    assert_eq!(
        command(&mut probe, format!("admin cancel ddl jobs {job_id}, 99999")),
        [
            [job_id.clone(), "successful".to_owned()],
            [
                "99999".to_owned(),
                "error: [ddl:8224]DDL Job:99999 not found".to_owned()
            ]
        ]
    );
    rows(&mut reader, "commit");
    assert_eq!(
        alter.join().unwrap(),
        Err((8214, "Cancelled DDL job".to_owned()))
    );
    assert_eq!(
        displayed(rows(&mut probe, "select column_name from information_schema.columns where table_schema = 'test' and table_name = 'jc1'")),
        [["a"]]
    );
}

/// Go `JobSubmitter.SubmitBatch` and `ddl.SwitchMDL` read
/// `mysql.tidb_ddl_job` with SQL on a session of `ddl.sessPool`:
/// `sysTblMgr.HasFlashbackClusterJob` is `select count(1) from
/// mysql.tidb_ddl_job where job_id >= %d and type = %d` (type 62) and refuses
/// every submission with "Can't add ddl job, have flashback cluster job";
/// `SwitchMDL` is `select 1 from mysql.tidb_ddl_job` and answers "please
/// wait for all jobs done"; ADMIN SHOW DDL JOBS and ADMIN SHOW DDL select the
/// running jobs the same way (`getJobsBySQL`). A queued job's `type` column stands for a
/// flashback job here; the worker runs the job from `job_meta`.
#[test]
fn ddl_admission_and_switch_mdl_read_the_job_table_on_a_pooled_session_as_go() {
    let (stack, _users) = cop_backed_stack();
    let mut reader = stack.factory.open_session(session_context(1187)).unwrap();
    let mut probe = stack.factory.open_session(session_context(1188)).unwrap();
    rows(&mut reader, "create table test.fb1 (a int)");
    rows(&mut reader, "begin");
    rows(&mut reader, "select * from test.fb1");
    let alter = std::thread::spawn({
        let factory = Arc::clone(&stack.factory);
        move || {
            let mut writer = factory.open_session(session_context(1189)).unwrap();
            rows(&mut writer, "alter table test.fb1 add column b int");
        }
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while displayed(rows(&mut probe, "select count(*) from mysql.tidb_mdl_info")) != [["1"]] {
        assert!(
            std::time::Instant::now() < deadline,
            "the ALTER never waited on the reader"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    let error = probe
        .node_conn()
        .execute("set global tidb_enable_metadata_lock = 0")
        .map(|_| ())
        .unwrap_err();
    assert_eq!(error.message, "please wait for all jobs done");
    // Go `GetAllDDLJobs` (`select job_meta from mysql.tidb_ddl_job where 1
    // order by job_id`) and `get2JobsFromTable` (`not reorg order by job_id
    // limit 1`).
    let jobs = displayed(rows(&mut probe, "admin show ddl jobs"));
    let queued = jobs
        .iter()
        .find(|job| job[2] == "fb1" && job[3] == "add column")
        .expect("ADMIN SHOW DDL JOBS lists the waiting ALTER");
    assert_eq!(queued[11], "running");
    let info = displayed(rows(&mut probe, "admin show ddl"));
    assert!(info[0][3].contains("Type:add column"), "{}", info[0][3]);
    let job_type = displayed(rows(&mut probe, "select type from mysql.tidb_ddl_job"));
    rows(&mut probe, "update mysql.tidb_ddl_job set type = 62");
    let error = probe
        .node_conn()
        .execute("create table test.fb2 (a int)")
        .map(|_| ())
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1105, "Can't add ddl job, have flashback cluster job")
    );
    rows(
        &mut probe,
        &format!("update mysql.tidb_ddl_job set type = {}", job_type[0][0]),
    );
    rows(&mut probe, "create table test.fb2 (a int)");
    rows(&mut reader, "commit");
    alter.join().unwrap();
    rows(&mut probe, "set global tidb_enable_metadata_lock = 0");
    rows(&mut probe, "set global tidb_enable_metadata_lock = 1");
}

/// Go `DumpColStatsUsageEntries`: a first use creates the row, a use within
/// the throttle interval keeps the stored instant, and one after it advances
/// it (the stored instant is the UTC one the statement converts).
#[test]
fn column_usage_dump_throttles_last_used_at_as_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(208)).unwrap();
    rows(&mut session, "set time_zone = '+00:00'");
    let pool = stack.factory.advanced_sys_session_pool();
    let dump = |used_at: &str| {
        let used_at: std::time::SystemTime = chrono::DateTime::parse_from_rfc3339(used_at)
            .expect("the instant parses")
            .into();
        let item = tidb_model::TableItemID {
            table_id: 4243,
            id: 11,
            is_index: false,
            is_sync_load_failed: false,
        };
        tidb_stats_handle_util::call_with_sctx(
            pool.as_ref(),
            |sctx| {
                tidb_stats_handle_usage::session_stats_collect::dump_col_stats_usage_batch(
                    sctx,
                    &[(item, used_at)],
                )
            },
            &[],
        )
        .expect("the usage batch upserts");
    };
    let last_used = |session: &mut Session| {
        displayed(rows(
            session,
            "select last_used_at from mysql.column_stats_usage where table_id = 4243 and column_id = 11",
        ))
    };
    dump("2026-07-29T06:00:00Z");
    assert_eq!(last_used(&mut session), [["2026-07-29 06:00:00"]]);
    dump("2026-07-29T06:10:00Z");
    assert_eq!(last_used(&mut session), [["2026-07-29 06:00:00"]]);
    dump("2026-07-29T18:00:00Z");
    assert_eq!(last_used(&mut session), [["2026-07-29 18:00:00"]]);
}

/// Go `pkg/ddl/session` over the node: `sess.Pool.Get` hands out a system
/// session, `Session.RunInTxn` commits its statements together, `Rollback`
/// discards them, and `Txn` names the open transaction -- through the
/// session's transaction API, which runs no BEGIN/COMMIT statement.
#[test]
fn the_ddl_session_pool_is_gos_sess_pool_over_the_node() {
    use tidb_ddl_build::session_pool::DdlSessionPool;
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(304)).unwrap();
    rows(&mut session, "CREATE TABLE test.ddlsess (a INT PRIMARY KEY)");
    let pool = crate::ddl_session_pool::NodeDdlSessionPool::default();
    pool.bind(&stack.factory);
    let sess_pool = pool.sess_pool().expect("a bound pool has Go's session pool");
    let context = sess_pool.get().expect("a pooled session");
    let sess = tidb_ddl_session::Session::new(Arc::clone(&context));
    let background = tidb_sqlexec::BackgroundContext;
    sess.run_in_txn(|sess| {
        assert!(sess.txn().unwrap().is_some_and(|txn| txn.start_ts() > 0));
        sess.execute(&background, "INSERT INTO test.ddlsess VALUES (1)", "test", &[])
            .map(|_| ())
            .map_err(|error| tidb_ddl_session::Error::new(error.to_string()))
    })
    .expect("the transaction commits");
    sess.begin(&background).unwrap();
    sess.execute(&background, "INSERT INTO test.ddlsess VALUES (2)", "test", &[])
        .unwrap();
    sess.rollback();
    assert!(sess.txn().unwrap().is_none());
    let read = sess
        .execute(&background, "SELECT a FROM test.ddlsess", "test", &[])
        .unwrap()
        .unwrap();
    assert_eq!(displayed(read), [["1"]]);
    sess_pool.put(context);
}

/// Go `statsSyncLoad.handleOneItemTask`: an evicted column's histogram is
/// read back on the statistics handle's sessions (`readStatsForOneItem`:
/// `HistMetaFromStorageWithHighPriority`, then the buckets, CM sketch and
/// TopN) and the cache holds it fully loaded.
#[test]
fn a_sync_load_reads_an_evicted_column_on_the_domains_sessions_as_go() {
    use tidb_stats_handle_syncload::sync_load::StatisticsItemLoader as _;
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(310)).unwrap();
    rows(&mut session, "CREATE TABLE test.synced (id INT PRIMARY KEY, v INT)");
    rows(&mut session, "INSERT INTO test.synced VALUES (1, 1), (2, 2), (3, 3)");
    rows(&mut session, "ANALYZE TABLE test.synced");
    let table_id = match rows(
        &mut session,
        "SELECT TIDB_TABLE_ID FROM information_schema.tables \
         WHERE TABLE_SCHEMA = 'test' AND TABLE_NAME = 'synced'",
    )[0][0]
    {
        Datum::Int(id) => id,
        ref other => panic!("table id: {other:?}"),
    };
    let stats = &stack.factory.stats;
    // Go `LFU.dropMemory`: the cache keeps an evicted copy.
    let evicted = Arc::new(
        stats
            .get(table_id)
            .expect("the analyzed table is cached")
            .copy_as(tidb_stats::CopyIntent::AllDataWritable),
    );
    evicted.hist_coll.drop_evicted();
    stats.publish_analyzed_table(table_id, evicted);
    let v_column = 2;
    assert!(stats.get(table_id).unwrap().column_load_needed(v_column, true).1);

    let loader = tidb_domain_core::domain::ClusterStatisticsItemLoader {
        domain: Arc::downgrade(&stack.factory),
        catalog: Arc::clone(&stack.factory.catalog),
        stats: Arc::clone(stats),
        global_vars: stack.factory.global_vars.clone(),
    };
    loader
        .load_items(
            &[tidb_model::StatsLoadItem {
                table_item_id: tidb_model::TableItemID {
                    table_id,
                    id: v_column,
                    is_index: false,
                    is_sync_load_failed: false,
                },
                full_load: true,
            }],
        )
        .expect("the item loads on the domain's sessions");
    let table = stats.get(table_id).unwrap();
    assert!(!table.column_load_needed(v_column, true).1);
    let column = table.hist_coll.get_column(v_column).unwrap();
    let column = column.read().unwrap();
    assert!(column.stats_loaded_status.is_full_load());
    // ANALYZE v2 keeps the three values in the TopN, no bucket left over.
    assert_eq!(column.top_n.as_ref().map(|top_n| top_n.num()), Some(3));
    assert_eq!(column.histogram.ndv, 3);
}

/// Go `workloadlearning.TestSaveReadTableCostMetrics`.
#[test]
fn workload_learning_saves_read_table_cost_metrics() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(313)).unwrap();
    rows(&mut session, "CREATE TABLE test.test (a int, b int, index idx(a))");
    let handle = tidb_workloadlearning::Handle::new(workload_learning_pool(&stack));
    let metrics = std::collections::HashMap::from([(1, workload_metric("test", 10, 10, 10, 1.0))]);
    handle.save_table_read_cost_metrics(&metrics, now_naive(), now_naive());
    assert_eq!(rows(&mut session, "SELECT * FROM mysql.tidb_workload_values").len(), 1);
}

/// Go `workloadlearning.TestAccumulateMetricsGroupByTableID`.
#[test]
fn workload_learning_accumulates_metrics_by_table_id() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(314)).unwrap();
    rows(&mut session, "CREATE TABLE test.test1 (a int, b int, index idx(a))");
    rows(&mut session, "CREATE TABLE test.test2 (a int, b int, index idx(b))");
    let catalog = stack.factory.catalog.get_latest();
    let id = |name: &str| catalog.table_info_by_name("test", name).unwrap().1.read().id;
    let mut previous = std::collections::HashMap::from([(id("test1"), workload_metric("test1", 10, 10, 1, 0.0))]);
    tidb_workloadlearning::accumulate_metrics_group_by_table_id(
        vec![Some(workload_metric("test1", 10, 10, 0, 0.0)), Some(workload_metric("test2", 10, 10, 0, 0.0))],
        2,
        &mut previous,
        &catalog,
    );
    assert_eq!(previous.len(), 2);
    let test1 = &previous[&id("test1")];
    assert_eq!((test1.read_frequency, test1.table_scan_time, test1.table_mem_usage, test1.table_read_cost), (3, 30, 30, 0.0));
    let test2 = &previous[&id("test2")];
    assert_eq!((test2.read_frequency, test2.table_scan_time, test2.table_mem_usage, test2.table_read_cost), (2, 20, 20, 0.0));
}

/// Go `cache.TableRowStatsCache` is one process-wide cache whose
/// `UpdateByID` reads both `mysql.stats_meta` and `mysql.stats_histograms`
/// on the domain's sessions and only adds to the cache: a `TABLE_ROWS`-only
/// query in one session caches the column lengths a later `DATA_LENGTH`
/// query in another session serves after the histogram rows are gone.
#[test]
fn the_table_size_cache_is_gos_process_wide_cache_over_both_reads() {
    let (stack, _users) = cop_backed_stack();
    let mut first = stack.factory.open_session(session_context(308)).unwrap();
    rows(&mut first, "CREATE TABLE test.sized (id INT PRIMARY KEY, v VARCHAR(64))");
    rows(
        &mut first,
        "INSERT INTO test.sized VALUES (1, 'aaaaaaaaaaaaaaaa'), (2, 'bbbbbbbbbbbbbbbb')",
    );
    rows(&mut first, "ANALYZE TABLE test.sized");
    let length = |session: &mut Session| {
        displayed(rows(
            session,
            "SELECT DATA_LENGTH FROM information_schema.tables \
             WHERE TABLE_SCHEMA = 'test' AND TABLE_NAME = 'sized'",
        ))
    };
    let expected = length(&mut first);
    assert_ne!(expected, [["0"]], "the analyzed column has a size");
    rows(
        &mut first,
        "SELECT TABLE_ROWS FROM information_schema.tables \
         WHERE TABLE_SCHEMA = 'test' AND TABLE_NAME = 'sized'",
    );
    rows(
        &mut first,
        "DELETE FROM mysql.stats_histograms WHERE table_id = \
         (SELECT TIDB_TABLE_ID FROM information_schema.tables \
          WHERE TABLE_SCHEMA = 'test' AND TABLE_NAME = 'sized')",
    );
    let mut second = stack.factory.open_session(session_context(309)).unwrap();
    assert_eq!(length(&mut second), expected);
}

/// Go `ttl/session.ExecuteSQL` then `GetSessionVars().StmtCtx.AffectedRows()`,
/// which the task and job managers read after each status write: the rows
/// the session's last statement changed.
#[test]
fn a_ttl_session_reports_its_last_statements_affected_rows() {
    use tidb_ttl::session::Session as _;
    use tidb_ttl_ttlworker::SessionVars as _;
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let mut session = stack.factory.open_session(session_context(81)).expect("session opens");
    rows(&mut session, "create table test.t(a int primary key)");
    let affected = stack
        .factory
        .advanced_sys_session_pool()
        .with_session(|pooled| {
            pooled.with_session_context(|context| {
                let se = tidb_ttl::session::TtlSession::new(
                    tidb_ttl_ttlworker::SctxRef(tidb_ttl_ttlworker::SysSessionContext::ttl_session_context(context)),
                    Box::new(|| {}),
                );
                let run = |sql: &str| {
                    se.execute_sql(&tidb_gocontext::Context::background(), sql, &[])
                        .map(|_| se.get_session_vars().affected_rows())
                        .map_err(|error| tidb_syssession::SysSessionError::new(error.0))
                };
                Ok::<_, tidb_syssession::SysSessionError>([
                    run("insert into test.t values (1), (2)")?,
                    run("update test.t set a = a + 10 where a = 1")?,
                    run("delete from test.t where a > 100")?,
                ])
            })
        })
        .unwrap();
    assert_eq!(affected, [2, 1, 0]);
}

/// Go `ttlworker`'s scan and delete statements carry the expired rows' keys
/// as literals (`sqlbuilder.BuildDeleteSQL`) and run through
/// `ExecuteSQLWithCheck(ctx, sql)` with no arguments, which Go's
/// `ParseWithParams` leaves unescaped: a key holding `%%` or `%n` deletes
/// exactly its own row.
#[test]
fn a_ttl_job_deletes_string_keys_holding_percent_signs_as_written() {
    boost_ttl_job_schedule_for_test();
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let _ttl_job_manager = stack.factory.start_ttl_job_manager().expect("the TTL job manager starts");
    let mut session = stack.factory.open_session(session_context(81)).expect("session opens");
    rows(&mut session, "use test");
    rows(&mut session, "create table t(id varchar(10) primary key, t timestamp) TTL=`t` + INTERVAL 1 DAY");
    let table_id: i64 = displayed(rows(
        &mut session,
        "select TIDB_TABLE_ID from information_schema.tables where table_schema='test' and table_name='t'",
    ))[0][0]
        .parse()
        .unwrap();
    wait_ttl_job_finished(&mut session, table_id);

    rows(&mut session, "insert into t values('a%%b', now() - interval 25 hour)");
    rows(&mut session, "insert into t values('a%b', now())");
    rows(&mut session, "insert into t values('x%n', now() - interval 25 hour)");
    let cli = stack.factory.ttl_job_manager().expect("started").get_command_cli();
    let res = tidb_ttl_client::command::trigger_new_ttl_job(
        &tidb_ttl_client::Context::background(),
        cli.as_ref(),
        "test",
        "t",
    )
    .unwrap();
    let job_id = res.table_result.unwrap()[0].job_id.clone();
    wait_ttl_job_finished(&mut session, table_id);
    assert_eq!(displayed(rows(&mut session, "select id from t order by id")), [["a%b"]]);
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "select status, expired_rows, deleted_rows, error_delete_rows from mysql.tidb_ttl_job_history where job_id='{job_id}'"
            ),
        )),
        [["finished", "2", "2", "0"]]
    );
}

/// Go `TestTriggerTTLJob`: the table's first scheduled job runs, then a
/// manually triggered job deletes the expired rows and leaves the others.
#[test]
fn ttl_job_manager_runs_scheduled_and_triggered_jobs_like_go() {
    boost_ttl_job_schedule_for_test();
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let _ttl_job_manager = stack.factory.start_ttl_job_manager().expect("the TTL job manager starts");
    let mut session = stack.factory.open_session(session_context(81)).expect("session opens");
    rows(&mut session, "use test");
    rows(&mut session, "create table t(id int primary key, t timestamp) TTL=`t` + INTERVAL 1 DAY");
    let table_id: i64 = displayed(rows(
        &mut session,
        "select TIDB_TABLE_ID from information_schema.tables where table_schema='test' and table_name='t'",
    ))[0][0]
        .parse()
        .unwrap();

    // make sure the table had run a job one time to make the test stable
    wait_ttl_job_finished(&mut session, table_id);

    rows(&mut session, "insert into t values(1, now() - interval 25 hour)");
    rows(&mut session, "insert into t values(2, now())");
    rows(&mut session, "insert into t values(3, now() - interval 25 hour)");
    rows(&mut session, "insert into t values(4, now())");

    // Go `ttl/session.ExecuteSQL`: the job's statements run under
    // `kv.InternalTxnTTL`.
    let sources: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
    let recorded = Arc::clone(&sources);
    stack.rpc.set_send_hook(Some(Arc::new(move |request| {
        use tikv_client::proto::kvrpcpb;
        if let Some(request) = request.as_any().downcast_ref::<tikv_client::proto::kvrpcpb::PrewriteRequest>() {
            recorded
                .lock()
                .unwrap()
                .push(request.context.as_ref().map(|context| context.request_source.clone()).unwrap_or_default());
        }
    })));
    let cli = stack.factory.ttl_job_manager().expect("started").get_command_cli();
    let res = tidb_ttl_client::command::trigger_new_ttl_job(
        &tidb_ttl_client::Context::background(),
        cli.as_ref(),
        "test",
        "t",
    )
    .unwrap();
    let table_results = res.table_result.unwrap();
    assert_eq!(table_results.len(), 1);
    let table_result = &table_results[0];
    assert_eq!(table_result.table_id, table_id);
    assert!(!table_result.job_id.is_empty());
    assert_eq!(table_result.db_name, "test");
    assert_eq!(table_result.table_name, "t");
    assert_eq!(table_result.error_message, "");
    assert_eq!(table_result.partition_name, "");

    wait_ttl_job_finished(&mut session, table_id);
    stack.rpc.set_send_hook(None);
    let sources = sources.lock().unwrap().clone();
    assert!(sources.contains(&"internal_TTL".to_owned()), "{sources:?}");
    assert_eq!(displayed(rows(&mut session, "select id from t order by id asc")), [["2"], ["4"]]);
    // The finished job's history row carries Go's summary of the two deletions.
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "select status, expired_rows, deleted_rows, error_delete_rows from mysql.tidb_ttl_job_history where job_id='{}'",
                table_result.job_id
            ),
        )),
        [["finished", "2", "2", "0"]]
    );
}

/// Go `PrepareTxnCtx` creates the statement's `TxnCtx` before it plans, so
/// the tables a statement plans before its reader activates the transaction
/// are the transaction's `RELATED_TABLE_IDS` (Go tidb-server capture under
/// autocommit=0: both tables read and `TIDB_TRX` itself); a statement that
/// activates nothing leaves nothing behind.
#[test]
fn related_table_ids_include_tables_planned_before_activation_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1191)).unwrap();
    for sql in [
        "CREATE TABLE test.rt1 (a INT)",
        "CREATE TABLE test.rt2 (a INT)",
    ] {
        session.node_conn().execute_write(sql).unwrap();
    }
    let id = |session: &mut crate::Session, schema: &str, table: &str| {
        displayed(rows(
            session,
            &format!(
                "SELECT TIDB_TABLE_ID FROM information_schema.TABLES \
                 WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'"
            ),
        ))[0][0]
            .clone()
    };
    let (rt1, rt2) = (id(&mut session, "test", "rt1"), id(&mut session, "test", "rt2"));
    let trx = id(&mut session, "information_schema", "TIDB_TRX");
    let related = |session: &mut crate::Session| {
        let row = displayed(rows(
            session,
            "SELECT RELATED_TABLE_IDS FROM information_schema.TIDB_TRX WHERE SESSION_ID = 1191",
        ));
        let mut ids: Vec<String> = row[0][0].split(',').map(str::to_owned).collect();
        ids.sort();
        ids
    };
    session.node_conn().execute_write("SET autocommit = 0").unwrap();
    rows(&mut session, "SELECT * FROM test.rt1");
    rows(&mut session, "SELECT * FROM test.rt2");
    let mut expected = vec![rt1, rt2.clone(), trx.clone()];
    expected.sort();
    assert_eq!(related(&mut session), expected);
    session.node_conn().execute_write("ROLLBACK").unwrap();
    // A memory-table read activates nothing: its context is discarded.
    rows(&mut session, "SELECT COUNT(*) FROM information_schema.ENGINES");
    rows(&mut session, "SELECT * FROM test.rt2");
    let mut expected = vec![rt2, trx];
    expected.sort();
    assert_eq!(related(&mut session), expected);
}

/// Go `ShowDDLExec`: the DDL owner the owner manager names, its server's
/// `IP:Port`, this node's id, `GetDDLInfoWithNewTxn`'s schema version, and
/// each running job's `String()` and query, newline-joined.
#[test]
fn admin_show_ddl_reports_the_owner_and_running_jobs_like_go() {
    struct TwoJobs;
    impl tidb_ddl::ddl_history::DdlJobHistoryStore for TwoJobs {
        fn all_ddl_jobs(&self) -> Result<(Vec<tidb_model::Job>, Vec<tidb_model::Job>), String> {
            Ok((Vec::new(), Vec::new()))
        }
        fn scan_history_ddl_jobs(&self, _: i64, _: i32) -> Result<Vec<tidb_model::Job>, String> {
            Ok(Vec::new())
        }
        fn get_ddl_info(&self) -> Result<tidb_ddl::ddl_history::DdlInfo, String> {
            let job = |id, query: &str| {
                let mut job = tidb_model::Job::default();
                job.id = id;
                job.query = query.into();
                job
            };
            Ok(tidb_ddl::ddl_history::DdlInfo {
                schema_ver: 77,
                jobs: vec![job(4, "create table t (a int)"), job(3, "alter table u add index i (a)")],
            })
        }
    }
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1192)).unwrap();
    session.set_ddl_job_history_store(Arc::new(TwoJobs));
    let info = tidb_ddl::ddl_history::DdlJobHistoryStore::get_ddl_info(&TwoJobs).unwrap();
    let server = stack.factory.server_info().unwrap().get_local_server_info();
    let row = displayed(rows(&mut session, "ADMIN SHOW DDL")).remove(0);
    assert_eq!(
        row,
        [
            "77".to_owned(),
            server.static_info.id.clone(),
            format!("{}:{}", server.static_info.ip, server.static_info.port),
            format!("{}\n{}", info.jobs[0], info.jobs[1]),
            server.static_info.id.clone(),
            "create table t (a int)\nalter table u add index i (a)".to_owned(),
        ]
    );
}

/// Go `casetest/plancache` `TestPlanCacheSuite`'s `rename_table` case (Go
/// master 87d45a1d703): a cached plan is checked against the table its name
/// resolves to now; after `t` is renamed away and `t_new` renamed to `t`, an
/// EXECUTE reads the new `t`, inside a transaction too.
#[test]
fn plan_cache_rename_table_rebuilds_for_the_renamed_table() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = stack.factory.open_session(session_context(939)).unwrap();
    let mut tk_ddl = stack.factory.open_session(session_context(940)).unwrap();
    let last = |tk: &mut Session| displayed(rows(tk, "select @@last_plan_from_cache"));
    rows(&mut tk, "use test");
    rows(&mut tk, "set session transaction_isolation = 'REPEATABLE-READ'");
    rows(&mut tk, "set session tidb_enable_prepared_plan_cache = 1");
    rows(&mut tk, "create table t (a int)");
    rows(&mut tk, "create table t_new (a int)");
    rows(&mut tk, "insert into t values (1)");
    rows(&mut tk, "insert into t_new values (2)");
    rows(&mut tk, "prepare s from 'select a from t where a < ? order by a'");
    rows(&mut tk, "set @a = 10");
    assert_eq!(displayed(rows(&mut tk, "execute s using @a")), [["1"]]);
    assert_eq!(displayed(rows(&mut tk, "execute s using @a")), [["1"]]);
    assert_eq!(last(&mut tk), [["1"]]);

    rows(&mut tk_ddl, "use test");
    rows(&mut tk_ddl, "rename table t to t_old");
    rows(&mut tk_ddl, "rename table t_new to t");

    tk.node_conn().control_transaction("begin").unwrap();
    assert_eq!(displayed(rows(&mut tk, "execute s using @a")), [["2"]]);
    assert_eq!(displayed(rows(&mut tk, "execute s using @a")), [["2"]]);
    assert_eq!(last(&mut tk), [["1"]]);
    rows(&mut tk, "insert into t values (3)");
    assert_eq!(displayed(rows(&mut tk, "execute s using @a")), [["2"], ["3"]]);
    assert_eq!(last(&mut tk), [["0"]]);
    assert_eq!(displayed(rows(&mut tk, "execute s using @a")), [["2"], ["3"]]);
    assert_eq!(last(&mut tk), [["1"]]);
    tk.node_conn().control_transaction("rollback").unwrap();
}

/// Go `TestCorrelatedColumnPathNotPrunedByRisk` (Go master 38d990578c4):
/// idx_a is reachable only through the correlated equality, so it starts
/// from a full range; its correlated estimate must carry matching min/max
/// row counts, or skyline pruning drops it for phantom risk and idx_b wins.
#[test]
fn test_correlated_column_path_not_pruned_by_risk() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let mut tk = stack.factory.open_session(session_context(942)).unwrap();
    rows(&mut tk, "use test");
    rows(&mut tk, "set @@tidb_enable_collect_execution_info=0");
    rows(&mut tk, "create table t_outer (id char(36) primary key)");
    rows(&mut tk, "insert into t_outer values (lpad(5,36,'0')), (lpad(7,36,'0'))");
    rows(
        &mut tk,
        "create table t (id char(36) primary key, a char(36) default null, b char(36) not null, \
         v char(36) not null, key idx_a (a), key idx_b (b))",
    );
    let values = (1..=1000)
        .map(|i| format!("(lpad({i},36,'0'),lpad({i},36,'0'),lpad({},36,'0'),lpad({i},36,'0'))", i % 20))
        .collect::<Vec<_>>()
        .join(",");
    rows(&mut tk, &format!("insert into t values {values}"));
    rows(&mut tk, "analyze table t");
    rows(&mut tk, "analyze table t_outer");
    let query = "select o.id, (select t.v from t where t.a = o.id and t.b = lpad(5,36,'0') limit 1) from t_outer o";
    let plan = displayed(rows(&mut tk, &format!("explain format = 'brief' {query}")))
        .into_iter()
        .map(|row| row.join(" "))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(plan.contains("index:idx_a(a) range: decided by [eq(test.t.a, test.t_outer.id)]"), "{plan}");
    assert!(!plan.contains("index:idx_b(b)"), "{plan}");
    let mut result = displayed(rows(&mut tk, query));
    result.sort();
    assert_eq!(
        result,
        [
            ["000000000000000000000000000000000005", "000000000000000000000000000000000005"],
            ["000000000000000000000000000000000007", "NULL"],
        ]
    );
}

/// Go `TestProcessModifyColumnOptionsGenerated` (f0599c2f6cc) at the
/// statement: MODIFY COLUMN's generated expression drops a table qualifier
/// as CREATE and ADD COLUMN do (gomaster probe: `` `a` + 2 ``).
#[test]
fn test_process_modify_column_options_generated() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = testkit(&stack, 9321);
    tk.must_exec("use test");
    tk.must_exec("create table t (a int, b int as (a + 1) virtual)");
    tk.must_exec("alter table t modify column b int as (t.a + 2) virtual");
    tk.must_query("select generation_expression from information_schema.columns where table_schema = 'test' and table_name = 't' and column_name = 'b'")
        .check(["`a` + 2"]);
}
