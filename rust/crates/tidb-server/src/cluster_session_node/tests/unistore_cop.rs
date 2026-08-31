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

//! Sessions over the REAL embedded store with the coprocessor wired.
//!
//! Every other module in this directory serves `cop_scans: None`, so a plan
//! that only misbehaves when base-table scans are answered by the pushdown
//! coprocessor -- `CopScanSource` over the in-process unistore transport --
//! never fails in-tree. This module builds the same stack `--store unistore
//! --cluster-session` boots and pins those plans.

use std::sync::Arc;
use std::time::Duration;

use tidb_datatype::{Datum, MySqlDuration};
use tidb_ddl_notifier::{
    publish_schema_change_to_store, DdlNotifier, Handler, SchemaChangeEvent, SessionPool, Store,
    TEST_HANDLER_ID,
};

use super::super::ddl_notifier::{ClusterNotifierSessionPool, ClusterNotifierTableStore};
use super::super::{
    partition_id_map, ClusterHistoricalStatsHandle, ClusterPriorityQueueSource,
    ClusterServerSession,
};
use super::node_fixture::{rows, session_context, ABC_HASH};
use crate::configured_user_store::ConfiguredUserStore;
use crate::sql_node::QuerySession;
use crate::unistore_node::{unistore_cluster_session_stack, UnistoreClusterStack};
use crate::QuerySessionFactory;

fn cop_backed_stack() -> (UnistoreClusterStack, Arc<ConfiguredUserStore>) {
    cop_backed_stack_with_stats_lease(None)
}

fn cop_backed_stack_with_stats_lease(
    stats_lease: Option<crate::node_config::StatsLease>,
) -> (UnistoreClusterStack, Arc<ConfiguredUserStore>) {
    let mut config = crate::node_config::NodeConfig::parse([
        "tidb-server",
        "--store",
        "unistore",
        "--cluster-session",
        "--port",
        "0",
        // Parse-time requirement only: the test passes its own user store
        // below, so the flag never has to name real rows.
        "--auth-file",
        "/dev/null",
    ])
    .expect("node config");
    if let Some(stats_lease) = stats_lease {
        config.stats_lease = stats_lease;
    }
    let users = Arc::new(
        ConfiguredUserStore::parse(&format!("root\t%\tmysql_native_password\t{ABC_HASH}\n"))
            .expect("configured user store"),
    );
    let stack = unistore_cluster_session_stack(&config, &users).expect("unistore stack");
    (stack, users)
}

fn displayed(rows: Vec<Vec<Datum>>) -> Vec<Vec<String>> {
    rows.into_iter()
        .map(|row| {
            row.into_iter()
                // Every datum a served column can hold renders as the text
                // the wire would carry. A `{other:?}` fallback here reads as
                // a value mismatch when the value is in fact right, which
                // has cost this file three false failures.
                .map(|datum| match datum {
                    Datum::Int(v) => v.to_string(),
                    Datum::UInt(v) => v.to_string(),
                    Datum::Real(v) => v.to_string(),
                    Datum::Decimal(d) => d.to_string(),
                    Datum::String(text) => String::from_utf8_lossy(text.bytes()).into_owned(),
                    Datum::Bytes(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
                    Datum::Enum(value, _) => {
                        String::from_utf8_lossy(value.name().as_bytes()).into_owned()
                    }
                    Datum::Set(value, _) => {
                        String::from_utf8_lossy(value.name().as_bytes()).into_owned()
                    }
                    Datum::Time(time) => time.to_string(),
                    Datum::Duration(duration) => duration.to_string(),
                    Datum::Json(json) => json.to_string(),
                    Datum::Null => "NULL".to_owned(),
                    other => format!("{other:?}"),
                })
                .collect()
        })
        .collect()
}

/// Pinned `pkg/ddl/notifier.TestPublishToTableStore`, `TestBasicPubSub`, and
/// `TestDeliverOrderAndCleanup` over the real bootstrapped notifier table.
#[test]
fn ddl_notifier_table_store_delivers_in_order_and_cleans_up() {
    let (stack, _users) = cop_backed_stack();
    let factory = Arc::new(stack.factory);
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
        publish_schema_change_to_store(
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
    assert_eq!(listed[0].event.create_table_info().name.original(), "t1");
    assert_eq!(
        listed[1].event.create_table_info().name.original(),
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
    assert_eq!(listed[0].event.create_table_info().name.original(), "t3");
    list.close(reader.as_mut());
    pool.put(reader);

    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let handler: Handler = {
        let seen = Arc::clone(&seen);
        Arc::new(move |_, event| {
            seen.lock().unwrap().push(event.create_table_info().id);
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

/// Pinned Go `TestStatsCacheShouldNotCacheTemporaryTable`: GLOBAL temporary
/// metadata is published by an ordinary DDL job, its rows remain
/// connection-local and are deleted at commit, and only explicit `ANALYZE`
/// replaces pseudo statistics for that session.
#[test]
fn global_temporary_analyze_uses_session_rows_and_statistics() {
    let (stack, _users) = cop_backed_stack();
    let existing_stats_ids = stack
        .factory
        .stats()
        .load()
        .keys()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    let mut session = stack
        .factory
        .open_session(session_context(139))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE GLOBAL TEMPORARY TABLE global_stats (a INT) ON COMMIT DELETE ROWS",
    );
    let temporary_id = *stack
        .factory
        .stats()
        .load()
        .keys()
        .find(|table_id| !existing_stats_ids.contains(table_id))
        .expect("GLOBAL temporary DDL publishes its statistics metadata row");
    rows(
        &mut session,
        "INSERT INTO global_stats VALUES (1), (2), (3)",
    );
    assert!(
        rows(&mut session, "SELECT * FROM global_stats").is_empty(),
        "GLOBAL temporary rows are deleted when the autocommit transaction ends"
    );
    let before = displayed(rows(&mut session, "EXPLAIN SELECT * FROM global_stats"));
    assert!(
        before
            .iter()
            .flatten()
            .any(|value| value.contains("stats:pseudo")),
        "ordinary access must not publish temporary statistics: {before:?}"
    );

    rows(&mut session, "ANALYZE TABLE global_stats");
    let snapshot = stack.factory.stats().load();
    let analyzed = snapshot
        .get(&temporary_id)
        .and_then(tidb_exec::stats_watch::TableStatsState::loaded)
        .expect("explicit ANALYZE publishes a real GLOBAL temporary cache object");
    assert_eq!(analyzed.hist_coll.column_count(), 1);
    assert!(!analyzed.hist_coll.pseudo);

    let after = displayed(rows(&mut session, "EXPLAIN SELECT * FROM global_stats"));
    assert!(
        after
            .iter()
            .flatten()
            .any(|value| value.contains("stats:pseudo")),
        "an analyzed empty table retains Go's query-time pseudo policy: {after:?}"
    );
}

#[test]
fn auto_analyze_priority_queue_uses_shared_stats_ddl_and_ordinary_analyze_path() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let factory = Arc::new(stack.factory);
    factory
        .campaign_stats_owner()
        .expect("stats owner campaigns");
    let owner_deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !factory
        .stats_owner
        .as_ref()
        .expect("stats owner")
        .is_owner()
    {
        assert!(
            std::time::Instant::now() < owner_deadline,
            "statistics ownership was not acquired"
        );
        std::thread::yield_now();
    }
    let mut session = factory
        .open_session(session_context(140))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "CREATE TABLE queue_analyze (a INT, b INT)");
    rows(&mut session, "CREATE TABLE queue_drop (a INT)");
    rows(&mut session, "CREATE TABLE queue_locked (a INT)");
    let values = (0..1_000)
        .map(|_| "(1)".to_owned())
        .collect::<Vec<_>>()
        .join(",");
    let pairs = (0..1_000)
        .map(|_| "(1,1)".to_owned())
        .collect::<Vec<_>>()
        .join(",");
    rows(
        &mut session,
        &format!("INSERT INTO queue_analyze VALUES {pairs}"),
    );
    rows(
        &mut session,
        &format!("INSERT INTO queue_drop VALUES {values}"),
    );
    rows(
        &mut session,
        &format!("INSERT INTO queue_locked VALUES {values}"),
    );
    rows(&mut session, "LOCK STATS queue_locked");
    let reloads = stack._stats_reloader.stats().reloads;
    rows(&mut session, "FLUSH STATS_DELTA *.*");
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while stack._stats_reloader.stats().reloads == reloads {
        assert!(
            std::time::Instant::now() < deadline,
            "statistics update did not run after FLUSH STATS_DELTA"
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    let catalog = factory.catalog.load();
    let analyze_id = catalog
        .find_table("test", "queue_analyze")
        .expect("analyze table exists")
        .1
        .id;
    let dropped_id = catalog
        .find_table("test", "queue_drop")
        .expect("drop table exists")
        .1
        .id;
    let locked_id = catalog
        .find_table("test", "queue_locked")
        .expect("locked table exists")
        .1
        .id;
    drop(catalog);
    rows(
        &mut session,
        &format!(
            "INSERT INTO mysql.column_stats_usage(table_id,column_id,last_used_at) \
             VALUES ({analyze_id},1,CURRENT_TIMESTAMP)"
        ),
    );
    rows(
        &mut session,
        "SET GLOBAL tidb_analyze_column_options = 'PREDICATE'",
    );

    let refresher = factory.auto_analyze_refresher(Duration::ZERO);
    let queue = factory.auto_analyze_priority_queue(Duration::ZERO);
    queue.initialize().expect("priority queue initializes");
    assert_eq!(queue.len().unwrap(), 2);
    rows(&mut session, "DROP TABLE queue_drop");
    let snapshot = queue.snapshot().unwrap();
    assert_eq!(snapshot.current_jobs.len(), 1);
    assert_eq!(snapshot.current_jobs[0].table_id, analyze_id);
    assert_ne!(snapshot.current_jobs[0].table_id, dropped_id);

    factory.handle_auto_analyze_tick(true, Duration::ZERO);
    let refresher = refresher
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    refresher.wait_auto_analyze_finished();
    assert!(refresher.running_jobs().is_empty());
    assert_eq!(refresher.len(), 0);
    drop(refresher);
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT job_info, state FROM mysql.analyze_jobs \
             WHERE table_name = 'queue_analyze' ORDER BY id",
        )),
        [[
            "auto analyze table column a with 256 buckets, 100 topn, 1 samplerate",
            "finished",
        ]]
    );

    factory.handle_auto_analyze_tick(false, Duration::ZERO);
    assert!(!queue.is_initialized());
    rows(&mut session, "UNLOCK STATS queue_locked");
    let reloads = stack._stats_reloader.stats().reloads;
    rows(&mut session, "FLUSH STATS_DELTA *.*");
    let reload_deadline = std::time::Instant::now() + Duration::from_secs(5);
    while stack._stats_reloader.stats().reloads == reloads {
        assert!(
            std::time::Instant::now() < reload_deadline,
            "statistics update did not run after unlocking the table"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    rows(
        &mut session,
        "SET GLOBAL tidb_analyze_column_options = 'ALL'",
    );
    factory.handle_auto_analyze_tick(true, Duration::ZERO);
    assert!(queue.is_initialized());
    let refresher = factory.auto_analyze_refresher(Duration::ZERO);
    refresher
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .wait_auto_analyze_finished();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT job_info, state FROM mysql.analyze_jobs \
             WHERE table_name = 'queue_locked' ORDER BY id",
        )),
        [[
            "auto analyze table all columns with 256 buckets, 100 topn, 1 samplerate",
            "finished",
        ]]
    );
    assert_ne!(locked_id, analyze_id);
}

/// Pinned root `TestAutoAnalyzeSkipColumnTypes`: auto analyze reads the live
/// GLOBAL skip list, retains index-mandatory columns, and records the same
/// selected-column job shape as ordinary ANALYZE planning.
#[test]
fn auto_analyze_skips_configured_column_types_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let factory = Arc::new(stack.factory);
    factory
        .campaign_stats_owner()
        .expect("stats owner campaigns");
    let owner_deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !factory
        .stats_owner
        .as_ref()
        .expect("stats owner")
        .is_owner()
    {
        assert!(std::time::Instant::now() < owner_deadline);
        std::thread::yield_now();
    }
    let mut session = factory
        .open_session(session_context(142))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE TABLE queue_skip_types (a INT, c JSON, d VARCHAR(32), INDEX idx_d(d))",
    );
    let values = (0..1_000)
        .map(|_| "(1,NULL,'value')".to_owned())
        .collect::<Vec<_>>()
        .join(",");
    rows(
        &mut session,
        &format!("INSERT INTO queue_skip_types VALUES {values}"),
    );
    let reloads = stack._stats_reloader.stats().reloads;
    rows(&mut session, "FLUSH STATS_DELTA *.*");
    let reload_deadline = std::time::Instant::now() + Duration::from_secs(5);
    while stack._stats_reloader.stats().reloads == reloads {
        assert!(std::time::Instant::now() < reload_deadline);
        std::thread::sleep(Duration::from_millis(10));
    }
    rows(
        &mut session,
        "SET GLOBAL tidb_analyze_skip_column_types = 'json'",
    );

    factory.handle_auto_analyze_tick(true, Duration::ZERO);
    factory
        .auto_analyze_refresher(Duration::ZERO)
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .wait_auto_analyze_finished();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT job_info, state FROM mysql.analyze_jobs \
             WHERE table_name = 'queue_skip_types' ORDER BY id",
        )),
        [[
            "auto analyze table all indexes, columns a, d with 256 buckets, 100 topn, 1 samplerate",
            "finished",
        ]]
    );
}

/// Pinned planner `TestAutoAnalyzeForMissingPartition`: under dynamic pruning
/// and `tidb_skip_missing_partition_stats`, auto analyze fills the physical
/// partitions that ordinary partition ANALYZE intentionally left missing.
#[test]
fn auto_analyze_fills_missing_partition_statistics_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let factory = Arc::new(stack.factory);
    factory
        .campaign_stats_owner()
        .expect("stats owner campaigns");
    let owner_deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !factory
        .stats_owner
        .as_ref()
        .expect("stats owner")
        .is_owner()
    {
        assert!(
            std::time::Instant::now() < owner_deadline,
            "statistics ownership was not acquired"
        );
        std::thread::yield_now();
    }
    let mut session = factory
        .open_session(session_context(141))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "SET GLOBAL tidb_skip_missing_partition_stats = ON",
    );
    rows(
        &mut session,
        "SET GLOBAL tidb_partition_prune_mode = 'dynamic'",
    );
    rows(&mut session, "SET GLOBAL tidb_auto_analyze_ratio = 0.01");
    rows(
        &mut session,
        "CREATE TABLE missing_partition_stats (a INT, b INT, c INT, INDEX idx_b(b)) \
         PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (100), \
         PARTITION p1 VALUES LESS THAN (200), PARTITION p2 VALUES LESS THAN (300))",
    );
    let values = [1, 101, 201]
        .into_iter()
        .flat_map(|value| std::iter::repeat_n(format!("({value},{value},{value})"), 1_000))
        .collect::<Vec<_>>()
        .join(",");
    rows(
        &mut session,
        &format!("INSERT INTO missing_partition_stats VALUES {values}"),
    );
    let reloads = stack._stats_reloader.stats().reloads;
    rows(&mut session, "FLUSH STATS_DELTA *.*");
    let reload_deadline = std::time::Instant::now() + Duration::from_secs(5);
    while stack._stats_reloader.stats().reloads == reloads {
        assert!(
            std::time::Instant::now() < reload_deadline,
            "statistics update did not run after FLUSH STATS_DELTA"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    rows(
        &mut session,
        "ANALYZE TABLE missing_partition_stats PARTITION p1",
    );

    factory.handle_auto_analyze_tick(true, Duration::ZERO);
    let refresher = factory.auto_analyze_refresher(Duration::ZERO);
    refresher
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .wait_auto_analyze_finished();

    let meta = displayed(rows(
        &mut session,
        "SHOW STATS_META WHERE table_name = 'missing_partition_stats'",
    ));
    for partition in ["p0", "p2"] {
        assert!(
            meta.iter()
                .any(|row| row[2] == partition && row[5] == "1000"),
            "auto analyze did not publish {partition}: {meta:?}"
        );
    }
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT COUNT(*) FROM mysql.analyze_jobs WHERE \
             table_name = 'missing_partition_stats' AND state = 'finished' AND \
             job_info LIKE 'auto analyze table%'",
        )),
        [["2"]]
    );
}

/// Pinned DDL subscriber `ActionCreateTable`, `ActionTruncateTable`, and
/// `ActionDropTable`: create every physical table's zero-valued statistics
/// placeholders, then retire the old physical ID by advancing stats_meta.
#[test]
fn table_lifecycle_ddl_updates_statistics_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(74))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE TABLE stats_lifecycle (a INT, b INT, INDEX idx_b(b))",
    );
    let old_id = stack
        .factory
        .catalog
        .load()
        .find_table("test", "stats_lifecycle")
        .expect("created table is published")
        .1
        .id;
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
    let like_id = stack
        .factory
        .catalog
        .load()
        .find_table("test", "stats_lifecycle_like")
        .expect("CREATE TABLE LIKE result is published")
        .1
        .id;
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
    let new_id = stack
        .factory
        .catalog
        .load()
        .find_table("test", "stats_lifecycle")
        .expect("truncated table is published")
        .1
        .id;
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
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
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
        .load()
        .find_table("test", "stats_ddl_after_load")
        .expect("created table is published")
        .1
        .id;
    assert!(stack
        .factory
        .stats()
        .load()
        .get(&table_id)
        .and_then(tidb_exec::stats_watch::TableStatsState::loaded)
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
        .and_then(tidb_exec::stats_watch::TableStatsState::loaded)
        .is_some_and(|stats| !stats.hist_coll.pseudo));

    rows(
        &mut session,
        "ALTER TABLE stats_ddl_after_load ADD COLUMN c10 INT",
    );
    assert!(stack
        .factory
        .catalog
        .load()
        .find_table("test", "stats_ddl_after_load")
        .is_some_and(|(_, table)| table.columns.iter_deref().any(|column| column
            .read()
            .name
            .lowercase()
            == "c10")));
}

/// Pinned `pkg/statistics/handle/ddl.TestTruncateAPartitionedTable`: whole
/// table truncation gives every partition a fresh physical ID, initializes
/// each replacement stats row, and advances every retired partition version.
#[test]
fn truncate_partitioned_table_statistics_match_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(76))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE TABLE stats_truncate_partitioned (a INT PRIMARY KEY, b INT, INDEX idx(b)) \
         PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (6), \
         PARTITION p1 VALUES LESS THAN (11))",
    );
    rows(
        &mut session,
        "INSERT INTO stats_truncate_partitioned VALUES (1,2),(2,2),(6,2)",
    );
    rows(&mut session, "ANALYZE TABLE stats_truncate_partitioned");
    let old_ids = partition_id_map(
        &stack.factory.catalog.load(),
        "test",
        "stats_truncate_partitioned",
    )
    .expect("table is partitioned")
    .1
    .into_iter()
    .map(|(_, id)| id)
    .collect::<Vec<_>>();
    let old_versions = old_ids
        .iter()
        .map(|id| {
            displayed(rows(
                &mut session,
                &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {id}"),
            ))[0][0]
                .parse::<u64>()
                .expect("stats version is unsigned")
        })
        .collect::<Vec<_>>();

    rows(&mut session, "TRUNCATE TABLE stats_truncate_partitioned");
    let new_ids = partition_id_map(
        &stack.factory.catalog.load(),
        "test",
        "stats_truncate_partitioned",
    )
    .expect("truncated table remains partitioned")
    .1
    .into_iter()
    .map(|(_, id)| id)
    .collect::<Vec<_>>();
    assert_eq!(new_ids.len(), 2);
    assert!(new_ids.iter().all(|id| !old_ids.contains(id)));
    for new_id in new_ids {
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!("SELECT count(*) FROM mysql.stats_meta WHERE table_id = {new_id}"),
            )),
            [["1"]]
        );
    }
    for (old_id, old_version) in old_ids.into_iter().zip(old_versions) {
        let retired_version = displayed(rows(
            &mut session,
            &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {old_id}"),
        ))[0][0]
            .parse::<u64>()
            .expect("stats version is unsigned");
        assert!(retired_version > old_version);
    }
}

/// Pinned `pkg/statistics/handle/ddl.TestTruncateAHashPartition`: truncating
/// p0 removes its one persisted row from global count, keeps that removal as
/// one modification, replaces p0's physical ID, and retires the old stats row.
#[test]
fn truncate_hash_partition_statistics_match_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(77))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE TABLE stats_truncate_hash (a BIGINT PRIMARY KEY, b INT, INDEX idx(b)) \
         PARTITION BY HASH(a) PARTITIONS 4",
    );
    rows(
        &mut session,
        "INSERT INTO stats_truncate_hash VALUES (1,2),(2,2),(6,2),(11,2),(16,2)",
    );
    rows(&mut session, "ANALYZE TABLE stats_truncate_hash");
    let (logical_id, partitions) =
        partition_id_map(&stack.factory.catalog.load(), "test", "stats_truncate_hash")
            .expect("table is hash partitioned");
    let old_p0 = partitions
        .iter()
        .find(|(name, _)| name == "p0")
        .expect("p0 exists")
        .1;
    let old_version = displayed(rows(
        &mut session,
        &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {old_p0}"),
    ))[0][0]
        .parse::<u64>()
        .expect("stats version is unsigned");

    rows(
        &mut session,
        "ALTER TABLE stats_truncate_hash TRUNCATE PARTITION p0",
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT count, modify_count FROM mysql.stats_meta WHERE table_id = {logical_id}"
            ),
        )),
        [["4", "1"]]
    );
    let new_p0 = partition_id_map(&stack.factory.catalog.load(), "test", "stats_truncate_hash")
        .expect("table remains hash partitioned")
        .1
        .into_iter()
        .find(|(name, _)| name == "p0")
        .expect("replacement p0 exists")
        .1;
    assert_ne!(new_p0, old_p0);
    let retired_version = displayed(rows(
        &mut session,
        &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {old_p0}"),
    ))[0][0]
        .parse::<u64>()
        .expect("stats version is unsigned");
    assert!(retired_version > old_version);
}

/// Pinned `pkg/statistics/handle/ddl.TestDDLPartition`: ADD PARTITION creates
/// the new physical statistics row in both prune modes, while only dynamic
/// mode creates the logical/global row for the original table.
#[test]
fn add_partition_statistics_follow_global_prune_mode_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(78))
        .expect("session opens");
    rows(&mut session, "USE test");

    for mode in ["static", "dynamic"] {
        rows(
            &mut session,
            &format!("SET GLOBAL tidb_partition_prune_mode = '{mode}'"),
        );
        rows(
            &mut session,
            &format!("SET SESSION tidb_partition_prune_mode = '{mode}'"),
        );
        let table_name = format!("stats_add_partition_{mode}");
        rows(
            &mut session,
            &format!(
                "CREATE TABLE {table_name} (a INT PRIMARY KEY, b INT, INDEX idx(b)) \
                 PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (6), \
                 PARTITION p1 VALUES LESS THAN (11))"
            ),
        );
        let (logical_id, original) =
            partition_id_map(&stack.factory.catalog.load(), "test", &table_name)
                .expect("table is partitioned");
        for (_, physical_id) in original {
            assert_eq!(
                displayed(rows(
                    &mut session,
                    &format!(
                        "SELECT count(*) FROM mysql.stats_meta WHERE table_id = {physical_id}"
                    ),
                )),
                [["1"]]
            );
        }
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!("SELECT count(*) FROM mysql.stats_meta WHERE table_id = {logical_id}"),
            )),
            [[if mode == "dynamic" { "1" } else { "0" }]]
        );

        rows(
            &mut session,
            &format!(
                "ALTER TABLE {table_name} ADD PARTITION \
                 (PARTITION p2 VALUES LESS THAN (16))"
            ),
        );
        let new_id = partition_id_map(&stack.factory.catalog.load(), "test", &table_name)
            .expect("table remains partitioned")
            .1
            .into_iter()
            .find(|(name, _)| name == "p2")
            .expect("added partition exists")
            .1;
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!("SELECT count(*) FROM mysql.stats_meta WHERE table_id = {new_id}"),
            )),
            [["1"]]
        );
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!("SELECT count(*) FROM mysql.stats_histograms WHERE table_id = {new_id}"),
            )),
            [["3"]]
        );
    }
}

/// Pinned `pkg/statistics/handle/ddl.TestDropPartitions`: dropping p0 and p1
/// subtracts their three rows from global count, adds three modifications,
/// and advances both retired physical statistics versions.
#[test]
fn drop_partitions_statistics_match_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(79))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "SET GLOBAL tidb_partition_prune_mode = 'dynamic'",
    );
    rows(
        &mut session,
        "CREATE TABLE stats_drop_partitions (a INT PRIMARY KEY, b INT, INDEX idx(b)) \
         PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (6), \
         PARTITION p1 VALUES LESS THAN (11), PARTITION p2 VALUES LESS THAN (16), \
         PARTITION p3 VALUES LESS THAN (21))",
    );
    rows(
        &mut session,
        "INSERT INTO stats_drop_partitions VALUES (1,2),(2,2),(6,2),(11,2),(16,2)",
    );
    rows(&mut session, "ANALYZE TABLE stats_drop_partitions");
    let (logical_id, partitions) = partition_id_map(
        &stack.factory.catalog.load(),
        "test",
        "stats_drop_partitions",
    )
    .expect("table is partitioned");
    let retired_ids = partitions
        .into_iter()
        .filter_map(|(name, id)| matches!(name.as_str(), "p0" | "p1").then_some(id))
        .collect::<Vec<_>>();
    let versions = retired_ids
        .iter()
        .map(|id| {
            displayed(rows(
                &mut session,
                &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {id}"),
            ))[0][0]
                .parse::<u64>()
                .expect("stats version is unsigned")
        })
        .collect::<Vec<_>>();

    rows(
        &mut session,
        "ALTER TABLE stats_drop_partitions DROP PARTITION p0, p1",
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT count, modify_count FROM mysql.stats_meta WHERE table_id = {logical_id}"
            ),
        )),
        [["2", "3"]]
    );
    for (retired_id, version) in retired_ids.into_iter().zip(versions) {
        let retired_version = displayed(rows(
            &mut session,
            &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {retired_id}"),
        ))[0][0]
            .parse::<u64>()
            .expect("stats version is unsigned");
        assert!(retired_version > version);
    }
}

/// Pinned DDL subscriber `ActionDropSchema` visits every table's partition
/// IDs in definition order and then its logical table ID, advancing each
/// extant stats row for delayed GC without failing the DROP on stats errors.
#[test]
fn drop_schema_ddl_retires_all_statistics_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
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
    let retired_ids = {
        let catalog = stack.factory.catalog.load();
        let database = catalog
            .databases
            .iter()
            .find(|database| database.info.name.lowercase() == "stats_drop_schema")
            .expect("created database is published");
        let mut ids = Vec::new();
        for table in &database.tables {
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
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(73))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "CREATE TABLE stats_add_column (a INT)");
    rows(
        &mut session,
        "INSERT INTO stats_add_column VALUES (1),(2),(3)",
    );
    rows(&mut session, "ANALYZE TABLE stats_add_column");
    let table_id = stack
        .factory
        .catalog
        .load()
        .find_table("test", "stats_add_column")
        .expect("created table is published")
        .1
        .id;
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
    let column_id = stack
        .factory
        .catalog
        .load()
        .find_table("test", "stats_add_column")
        .expect("altered table is published")
        .1
        .columns
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
    let defaulted_column_id = stack
        .factory
        .catalog
        .load()
        .find_table("test", "stats_add_column")
        .expect("altered table is published")
        .1
        .columns
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
    let zeroed_column_id = stack
        .factory
        .catalog
        .load()
        .find_table("test", "stats_add_column")
        .expect("altered table is published")
        .1
        .columns
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
    let virtual_column_id = {
        let catalog = stack.factory.catalog.load();
        let column = catalog
            .find_table("test", "stats_add_column")
            .expect("altered table is published")
            .1
            .columns
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
    session.rebuild_catalog_if_stale();
    {
        let catalog = session.session.shared_catalog();
        let catalog = catalog.lock().expect("session catalog is available");
        let tidb_executor::driver::TableEntry::Kv(table) = catalog
            .table_in("test", "stats_add_column")
            .expect("rebuilt executor table is available")
        else {
            panic!("rebuilt executor table is storage-backed");
        };
        assert!(table
            .columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case("e"))
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
    let (f_id, g_id) = {
        let catalog = stack.factory.catalog.load();
        let table = catalog
            .find_table("test", "stats_add_column")
            .expect("altered table is published")
            .1;
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
    let h_id = stack
        .factory
        .catalog
        .load()
        .find_table("test", "stats_add_column")
        .expect("altered table is published")
        .1
        .columns
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

/// Pinned storage `TestDeleteAnalyzeJobs` begins with the ordinary ANALYZE
/// lifecycle: the job is inserted pending, run, and retained finished for
/// `SHOW ANALYZE STATUS` until timestamp-based cleanup removes it.
#[test]
fn analyze_job_lifecycle_is_persisted_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(77))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE TABLE stats_analyze_job (a INT, b INT)",
    );
    rows(
        &mut session,
        "INSERT INTO stats_analyze_job VALUES (1,2),(3,4)",
    );
    rows(&mut session, "ANALYZE TABLE stats_analyze_job");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT table_schema, table_name, partition_name, processed_rows, state, \
                    process_id IS NULL \
             FROM mysql.analyze_jobs IGNORE INDEX \
             (PRIMARY, update_time, idx_schema_table_state, idx_schema_table_partition_state)",
        )),
        [["test", "stats_analyze_job", "", "2", "finished", "1"]]
    );
    let cutoff = tidb_exec::mysql_bootstrap::utc_now_timestamp()
        .add_duration(MySqlDuration::from_nanoseconds(1_000_000_000, 0).unwrap())
        .unwrap();
    stack
        .factory
        .delete_analyze_jobs_before(cutoff)
        .expect("analyze-job cleanup commits");
    assert!(displayed(rows(
        &mut session,
        "SELECT id FROM mysql.analyze_jobs IGNORE INDEX \
         (PRIMARY, update_time, idx_schema_table_state, idx_schema_table_partition_state)",
    ))
    .is_empty());
}

/// Pinned executor `TestShowAnalyzeStatus`: the SHOW path reads the same
/// persisted job row as `mysql.analyze_jobs` and exposes Go's fourteen-column
/// shape rather than falling through as an unsupported inspection statement.
#[test]
fn show_analyze_status_reads_persisted_jobs_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(78))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE TABLE stats_show_analyze_job (a INT, b INT)",
    );
    rows(
        &mut session,
        "INSERT INTO stats_show_analyze_job VALUES (1,2),(3,4)",
    );
    rows(&mut session, "ANALYZE TABLE stats_show_analyze_job");

    let shown = displayed(rows(
        &mut session,
        "SHOW ANALYZE STATUS WHERE table_name = 'stats_show_analyze_job'",
    ));
    assert_eq!(shown.len(), 1);
    assert_eq!(shown[0].len(), 14);
    assert_eq!(shown[0][0], "test");
    assert_eq!(shown[0][1], "stats_show_analyze_job");
    assert_eq!(shown[0][2], "");
    assert_eq!(shown[0][4], "2");
    assert_ne!(shown[0][5], "NULL");
    assert_ne!(shown[0][6], "NULL");
    assert_eq!(shown[0][7], "finished");
    assert_eq!(shown[0][8], "NULL");
    assert_eq!(shown[0][10], "NULL");
    assert_eq!(shown[0][11], "NULL");
    assert_eq!(shown[0][12], "NULL");
    assert_eq!(shown[0][13], "NULL");

    rows(&mut session, "DELETE FROM mysql.analyze_jobs");
    rows(
        &mut session,
        "CREATE TABLE stats_show_analyze_partition (a INT PRIMARY KEY, b INT) \
         PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (6))",
    );
    rows(
        &mut session,
        "INSERT INTO stats_show_analyze_partition VALUES (1,1),(2,2)",
    );
    rows(&mut session, "ANALYZE TABLE stats_show_analyze_partition");
    let partition_jobs = displayed(rows(
        &mut session,
        "SHOW ANALYZE STATUS WHERE table_name = 'stats_show_analyze_partition'",
    ));
    assert_eq!(partition_jobs.len(), 2);
    let mut job_infos = partition_jobs
        .iter()
        .map(|row| row[3].clone())
        .collect::<Vec<_>>();
    job_infos.sort();
    assert_eq!(
        job_infos,
        [
            "analyze table all columns with 256 buckets, 100 topn, 1 samplerate",
            "merge global stats for test.stats_show_analyze_partition columns",
        ]
    );

    rows(&mut session, "DELETE FROM mysql.analyze_jobs");
    rows(
        &mut session,
        "ALTER TABLE stats_show_analyze_partition ADD INDEX idx(b)",
    );
    rows(
        &mut session,
        "ANALYZE TABLE stats_show_analyze_partition INDEX idx",
    );
    let partition_index_jobs = displayed(rows(
        &mut session,
        "SHOW ANALYZE STATUS WHERE table_name = 'stats_show_analyze_partition'",
    ));
    assert_eq!(partition_index_jobs.len(), 3);
    let mut job_infos = partition_index_jobs
        .iter()
        .map(|row| row[3].clone())
        .collect::<Vec<_>>();
    job_infos.sort();
    assert_eq!(
        job_infos,
        [
            "analyze table all indexes, all columns with 256 buckets, 100 topn, 1 samplerate",
            "merge global stats for test.stats_show_analyze_partition columns",
            "merge global stats for test.stats_show_analyze_partition's index idx",
        ]
    );

    rows(&mut session, "DELETE FROM mysql.analyze_jobs");
    let started_at = tidb_exec::mysql_bootstrap::utc_now_timestamp()
        .add_duration(MySqlDuration::from_nanoseconds(-60_000_000_000, 0).unwrap())
        .unwrap();
    let handle = ClusterHistoricalStatsHandle {
        transactions: Arc::clone(&stack.factory.transactions),
        catalog: Arc::clone(&stack.factory.catalog),
        global_vars: stack.factory.global_vars.clone(),
    };
    let mut job_id = 0;
    handle
        .commit_stats_plan(|snapshot, _| {
            let (created_job_id, plan) = tidb_exec::cluster_stats_write::plan_insert_analyze_job(
                snapshot,
                &stack.factory.catalog.load(),
                "test",
                "stats_show_analyze_job",
                "",
                b"analyze table all columns with 256 buckets, 100 topn, 1 samplerate",
                "127.0.0.1:4000",
                78,
                started_at,
            )
            .map_err(|error| error.to_string())?;
            job_id = created_job_id;
            Ok(plan)
        })
        .expect("pending job commits");
    handle
        .commit_stats_plan(|snapshot, _| {
            tidb_exec::cluster_stats_write::plan_start_analyze_job(
                snapshot,
                &stack.factory.catalog.load(),
                job_id,
                started_at,
            )
            .map_err(|error| error.to_string())
        })
        .expect("running job commits");
    handle
        .commit_stats_plan(|snapshot, _| {
            tidb_exec::cluster_stats_write::plan_update_analyze_job_progress(
                snapshot,
                &stack.factory.catalog.load(),
                job_id,
                3,
                started_at,
            )
            .map_err(|error| error.to_string())
        })
        .expect("running progress commits");
    let running = displayed(rows(
        &mut session,
        "SHOW ANALYZE STATUS WHERE table_name = 'stats_show_analyze_job' AND state = 'running'",
    ));
    assert_eq!(running.len(), 1);
    assert_eq!(running[0][4], "3");
    assert_eq!(running[0][7], "running");
    // The embedded store has no PD HTTP authority. Go caches and uses the
    // helper's zero return after processed rows exceed loaded RealtimeCount.
    assert_eq!(running[0][11], "0s");
    assert_eq!(running[0][12], "100");
    assert_eq!(running[0][13], "0");
}

/// Pinned
/// `pkg/statistics/handle/autoanalyze/priorityqueue/intervaltimezone::TestLastFailedAnalysisDurationUseCorrectTimezone`:
/// a reused statistics session must replace its stale timezone with the live
/// global value before evaluating the failed-job interval.
#[test]
fn failed_analysis_duration_resets_the_pooled_session_timezone() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let factory = Arc::new(stack.factory);
    let mut client = factory
        .open_session(session_context(80))
        .expect("client session opens");
    rows(&mut client, "SET GLOBAL time_zone = 'America/New_York'");
    let session_pool = factory.advanced_sys_session_pool();
    let source = ClusterPriorityQueueSource {
        factory: Arc::downgrade(&factory),
        stats_lease: Duration::ZERO,
        session_pool,
    };
    assert_eq!(
        source.scalar("SELECT @@time_zone").unwrap(),
        Some(Datum::String(tidb_datatype::StringDatum::new(
            b"America/New_York".to_vec(),
            tidb_datatype::Collation::Utf8Mb4Bin,
        )))
    );
    rows(&mut client, "SET GLOBAL time_zone = 'Europe/Berlin'");
    let now = tidb_exec::mysql_bootstrap::utc_now_timestamp();
    let started_at = now
        .add_duration(MySqlDuration::from_nanoseconds(-2_000_000_000, 0).unwrap())
        .unwrap();
    let handle = ClusterHistoricalStatsHandle {
        transactions: Arc::clone(&factory.transactions),
        catalog: Arc::clone(&factory.catalog),
        global_vars: factory.global_vars.clone(),
    };
    let mut job_id = 0;
    handle
        .commit_stats_plan(|snapshot, _| {
            let (created_job_id, plan) = tidb_exec::cluster_stats_write::plan_insert_analyze_job(
                snapshot,
                &factory.catalog.load(),
                "test",
                "t",
                "",
                b"analyze table `test`.`t`",
                "127.0.0.1:4000",
                1,
                started_at,
            )
            .map_err(|error| error.to_string())?;
            job_id = created_job_id;
            Ok(plan)
        })
        .expect("pending job commits");
    handle
        .commit_stats_plan(|snapshot, _| {
            tidb_exec::cluster_stats_write::plan_start_analyze_job(
                snapshot,
                &factory.catalog.load(),
                job_id,
                started_at,
            )
            .map_err(|error| error.to_string())
        })
        .expect("running job commits");
    handle
        .commit_stats_plan(|snapshot, _| {
            tidb_exec::cluster_stats_write::plan_finish_analyze_job(
                snapshot,
                &factory.catalog.load(),
                job_id,
                0,
                Some("simulated failure"),
                now,
            )
            .map_err(|error| error.to_string())
        })
        .expect("failed job commits");

    assert_eq!(
        source.scalar("SELECT @@time_zone").unwrap(),
        Some(Datum::String(tidb_datatype::StringDatum::new(
            b"Europe/Berlin".to_vec(),
            tidb_datatype::Collation::Utf8Mb4Bin,
        )))
    );
    let duration =
        tidb_stats_handle_autoanalyze_priorityqueue::AnalysisJobContext::last_failed_analysis_duration(
            &source,
            "test",
            "t",
            &[],
        )
        .expect("duration query");
    assert!(duration > 0, "duration must be positive: {duration}");
    assert!(
        duration < 60_000_000_000,
        "duration must be below one minute: {duration}"
    );
}

/// Pinned `TestCleanupCorruptedAnalyzeJobsOnCurrentInstance` and
/// `TestCleanupCorruptedAnalyzeJobsOnDeadInstances`: the two restricted
/// transactions select different corruptions and preserve the timestamps Go
/// does not update in `BatchUpdateAnalyzeJobSQL`.
#[test]
fn corrupted_analyze_job_cleanup_matches_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(79))
        .expect("session opens");
    let local_instance = "127.0.0.1:0";
    let now = tidb_exec::mysql_bootstrap::utc_now_timestamp();
    let old = now
        .add_duration(MySqlDuration::from_nanoseconds(-660_000_000_000, 0).unwrap())
        .unwrap();
    let handle = ClusterHistoricalStatsHandle {
        transactions: Arc::clone(&stack.factory.transactions),
        catalog: Arc::clone(&stack.factory.catalog),
        global_vars: stack.factory.global_vars.clone(),
    };
    for (process_id, instance, created_at) in [
        (1, local_instance, old),
        (2, local_instance, old),
        (3, local_instance, old),
        (4, local_instance, old),
        (5, local_instance, old),
        (6, local_instance, now),
        (7, "10.0.0.1:4000", old),
    ] {
        handle
            .commit_stats_plan(|snapshot, _| {
                let (_, plan) = tidb_exec::cluster_stats_write::plan_insert_analyze_job(
                    snapshot,
                    &stack.factory.catalog.load(),
                    "test",
                    "t",
                    "",
                    b"job",
                    instance,
                    process_id,
                    created_at,
                )
                .map_err(|error| error.to_string())?;
                Ok(plan)
            })
            .expect("pending job commits");
    }
    for id in [2, 3, 4] {
        handle
            .commit_stats_plan(|snapshot, _| {
                tidb_exec::cluster_stats_write::plan_start_analyze_job(
                    snapshot,
                    &stack.factory.catalog.load(),
                    id,
                    old,
                )
                .map_err(|error| error.to_string())
            })
            .expect("running job commits");
    }
    handle
        .commit_stats_plan(|snapshot, _| {
            tidb_exec::cluster_stats_write::plan_finish_analyze_job(
                snapshot,
                &stack.factory.catalog.load(),
                2,
                0,
                None,
                old,
            )
            .map_err(|error| error.to_string())
        })
        .expect("temporary finish commits");
    handle
        .commit_stats_plan(|snapshot, _| {
            tidb_exec::cluster_stats_write::plan_start_analyze_job(
                snapshot,
                &stack.factory.catalog.load(),
                2,
                old,
            )
            .map_err(|error| error.to_string())
        })
        .expect("NULL-process running job commits");
    handle
        .commit_stats_plan(|snapshot, _| {
            tidb_exec::cluster_stats_write::plan_finish_analyze_job(
                snapshot,
                &stack.factory.catalog.load(),
                5,
                0,
                None,
                old,
            )
            .map_err(|error| error.to_string())
        })
        .expect("finished job commits");

    let processes = stack.factory.processes();
    let _analyze = processes.register(
        3,
        "root".to_owned(),
        "local".to_owned(),
        "test".to_owned(),
        None,
    );
    processes.statement_started(3, "/*+ hint */ANALYZE TABLE test.t", "executing");
    let _ordinary = processes.register(
        4,
        "root".to_owned(),
        "local".to_owned(),
        "test".to_owned(),
        None,
    );
    processes.statement_started(4, "SELECT 1", "executing");

    let cutoff = now
        .add_duration(MySqlDuration::from_nanoseconds(-600_000_000_000, 0).unwrap())
        .unwrap();
    let update_times_before = displayed(rows(
        &mut session,
        "SELECT id, update_time FROM mysql.analyze_jobs ORDER BY id",
    ));
    stack
        .factory
        .cleanup_corrupted_analyze_jobs_on_current_instance(cutoff)
        .expect("current-instance cleanup commits");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT id, state, process_id IS NULL, fail_reason IS NULL \
             FROM mysql.analyze_jobs ORDER BY id",
        )),
        [
            ["1", "failed", "1", "0"],
            ["2", "running", "1", "1"],
            ["3", "running", "0", "1"],
            ["4", "failed", "1", "0"],
            ["5", "finished", "1", "1"],
            ["6", "pending", "0", "1"],
            ["7", "pending", "0", "1"],
        ]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT id, update_time FROM mysql.analyze_jobs ORDER BY id",
        )),
        update_times_before,
        "BatchUpdateAnalyzeJobSQL does not refresh update_time",
    );

    stack
        .factory
        .cleanup_corrupted_analyze_jobs_on_dead_instances(cutoff)
        .expect("dead-instance cleanup commits");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT state, process_id IS NULL, fail_reason FROM mysql.analyze_jobs WHERE id = 7",
        )),
        [[
            "failed",
            "1",
            tidb_exec::cluster_stats_write::CORRUPTED_ANALYZE_JOB_FAILURE,
        ]]
    );
}

#[test]
fn analyze_job_cleanup_worker_uses_gos_positive_lease_gate_and_stops() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let factory = Arc::new(stack.factory);
    factory.start_analyze_jobs_cleanup_worker(crate::node_config::StatsLease::Zero);
    assert!(factory.analyze_jobs_cleanup_worker.get().is_none());
    factory.start_analyze_jobs_cleanup_worker(crate::node_config::StatsLease::Disabled);
    assert!(factory.analyze_jobs_cleanup_worker.get().is_none());
    factory.start_analyze_jobs_cleanup_worker(crate::node_config::StatsLease::Positive(
        Duration::from_secs(60),
    ));
    assert!(factory.analyze_jobs_cleanup_worker.get().is_some());
    drop(factory);
}

#[test]
fn auto_analyze_worker_uses_gos_positive_lease_gate_and_stops() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let factory = Arc::new(stack.factory);
    factory.start_auto_analyze_worker(crate::node_config::StatsLease::Zero, true);
    assert!(factory.auto_analyze_worker.get().is_none());
    factory.start_auto_analyze_worker(crate::node_config::StatsLease::Disabled, true);
    assert!(factory.auto_analyze_worker.get().is_none());
    factory.start_auto_analyze_worker(
        crate::node_config::StatsLease::Positive(Duration::from_secs(60)),
        true,
    );
    assert!(factory.auto_analyze_worker.get().is_some());
    drop(factory);
}

/// Go creates one pending job for every physical partition task before
/// dispatch, then creates independent global-merge jobs after all partition
/// results have been saved. Global jobs never accumulate processed rows.
#[test]
fn partition_analyze_jobs_match_go_task_shapes() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(78))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'dynamic'",
    );
    rows(
        &mut session,
        "CREATE TABLE stats_analyze_partitioned (a INT, KEY idx(a)) \
         PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (10), \
         PARTITION p1 VALUES LESS THAN MAXVALUE)",
    );
    rows(
        &mut session,
        "INSERT INTO stats_analyze_partitioned VALUES (1),(11)",
    );
    rows(
        &mut session,
        "ANALYZE TABLE stats_analyze_partitioned WITH 0 TOPN, 1 BUCKETS",
    );

    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT partition_name, job_info, processed_rows, state, process_id IS NULL \
             FROM mysql.analyze_jobs IGNORE INDEX \
             (PRIMARY, update_time, idx_schema_table_state, idx_schema_table_partition_state) \
             WHERE table_schema = 'test' AND table_name = 'stats_analyze_partitioned' \
             ORDER BY id",
        )),
        [
            [
                "p0",
                "analyze table all indexes, all columns with 1 buckets, 0 topn, 1 samplerate",
                "1",
                "finished",
                "1",
            ],
            [
                "p1",
                "analyze table all indexes, all columns with 1 buckets, 0 topn, 1 samplerate",
                "1",
                "finished",
                "1",
            ],
            [
                "",
                "merge global stats for test.stats_analyze_partitioned columns",
                "0",
                "finished",
                "1",
            ],
            [
                "",
                "merge global stats for test.stats_analyze_partitioned's index idx",
                "0",
                "finished",
                "1",
            ],
        ]
    );
}

/// Pinned `pkg/statistics/handle/handletest/handle_test.go::TestIssue39336`:
/// permissive zero-in DATETIME values must not make the dynamic partition
/// global-statistics merge job fail.
#[test]
fn partition_global_analyze_finishes_with_zero_in_datetime_values() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(146))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE TABLE issue39336 (a DATETIME(3) DEFAULT NULL, b INT) \
         PARTITION BY RANGE (b) (\
         PARTITION p0 VALUES LESS THAN (1000),\
         PARTITION p1 VALUES LESS THAN MAXVALUE)",
    );
    rows(&mut session, "SET @@sql_mode = ''");
    rows(&mut session, "SET @@tidb_analyze_version = 2");
    rows(&mut session, "SET @@tidb_partition_prune_mode = 'dynamic'");
    rows(
        &mut session,
        "INSERT INTO issue39336 VALUES \
         ('1000-00-09 00:00:00.000',1),\
         ('1000-00-06 00:00:00.000',1),\
         ('1000-00-06 00:00:00.000',1),\
         ('2022-11-23 14:24:30.000',1),\
         ('2022-11-23 14:24:32.000',1),\
         ('2022-11-23 14:24:33.000',1),\
         ('2022-11-23 14:24:35.000',1),\
         ('2022-11-23 14:25:08.000',1001),\
         ('2022-11-23 14:25:09.000',1001)",
    );
    rows(&mut session, "ANALYZE TABLE issue39336 WITH 0 TOPN");

    assert_eq!(
        displayed(rows(
            &mut session,
            "SHOW ANALYZE STATUS WHERE job_info LIKE 'merge global stats%' \
             AND table_name = 'issue39336'",
        ))
        .into_iter()
        .map(|row| (row[3].clone(), row[7].clone()))
        .collect::<Vec<_>>(),
        [(
            "merge global stats for test.issue39336 columns".to_owned(),
            "finished".to_owned(),
        )]
    );
}

/// Pinned `TestSystemTableDDLHasNoEvent`: `asyncNotifyEvent` suppresses every
/// stats subscriber event for `metadef.IsMemOrSysDB`, so system-table DDL
/// changes schema metadata but never creates or refreshes stats rows.
#[test]
fn system_table_ddl_does_not_publish_statistics_events_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(76))
        .expect("session opens");
    let assert_no_stats = |session: &mut ClusterServerSession, ids: &[i64]| {
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
        .load()
        .find_table("mysql", "stats_no_event")
        .expect("system table is published")
        .1
        .id;
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
        .load()
        .find_table("mysql", "stats_no_event")
        .expect("truncated system table is published")
        .1
        .id;
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
        let catalog = stack.factory.catalog.load();
        let table = catalog
            .find_table("mysql", "stats_no_event_part")
            .expect("partitioned system table is published")
            .1;
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
        let catalog = stack.factory.catalog.load();
        let table = catalog
            .find_table("mysql", "stats_no_event_part")
            .expect("partitioned system table is published")
            .1;
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
        let catalog = stack.factory.catalog.load();
        let table = catalog
            .find_table("mysql", "stats_no_event_part")
            .expect("partitioned system table is published")
            .1;
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

/// Pinned `pkg/statistics/handle/handletest/handle_test.go::
/// TestStatsCacheShouldNotCacheSystemTable`. The SHOW scans may enumerate
/// every schema, but they must not publish system-table objects into the
/// canonical statistics cache.
#[test]
fn show_stats_does_not_cache_system_table_statistics() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(147))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "CREATE TABLE system_cache_guard (a INT)");
    rows(
        &mut session,
        "INSERT INTO system_cache_guard VALUES (1),(2),(3)",
    );
    rows(&mut session, "ANALYZE TABLE system_cache_guard");

    let before = stack.factory.stats().receipt();
    assert_eq!(before.loaded, 1);
    rows(&mut session, "SHOW STATS_META");
    rows(&mut session, "SHOW STATS_HEALTHY");
    assert_eq!(stack.factory.stats().receipt(), before);
}

/// Pinned `pkg/statistics/handle/handletest/handle_test.go::
/// TestUninitializedStatsStatus`. DDL placeholders and delta-only metadata
/// are not initialized histograms, and both settings of the outdated-stats
/// switch retain the planner's pseudo fallback.
#[test]
fn uninitialized_statistics_remain_hidden_and_pseudo() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(148))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE TABLE uninitialized_stats (a INT, b INT, c INT, INDEX idx_a(a))",
    );
    rows(
        &mut session,
        "INSERT INTO uninitialized_stats VALUES (1,2,2), (3,4,4), (5,6,6), (7,8,8), (9,10,10)",
    );
    rows(&mut session, "FLUSH STATS_DELTA *.*");

    assert!(rows(
        &mut session,
        "SHOW STATS_HISTOGRAMS WHERE db_name = 'test' AND table_name = 'uninitialized_stats'",
    )
    .is_empty());
    for enabled in ["ON", "OFF"] {
        rows(
            &mut session,
            &format!("SET @@tidb_enable_pseudo_for_outdated_stats = {enabled}"),
        );
        let explain = displayed(rows(
            &mut session,
            "EXPLAIN SELECT * FROM uninitialized_stats",
        ));
        assert!(
            explain
                .iter()
                .flatten()
                .any(|value| value.contains("stats:pseudo")),
            "uninitialized statistics must stay pseudo with the switch {enabled}: {explain:?}"
        );
    }
}

/// Pinned `pkg/statistics/handle/handletest/handle_test.go::
/// TestSkipMissingPartitionStats`. Dynamic global merge keeps the logical
/// row count, accounts for the unanalyzed partition as modifications, and
/// publishes every merged column/index when missing partition statistics are
/// explicitly skipped.
#[test]
fn global_statistics_skip_missing_partition_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(149))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'dynamic'",
    );
    rows(
        &mut session,
        "SET SESSION tidb_skip_missing_partition_stats = ON",
    );
    rows(
        &mut session,
        "CREATE TABLE skip_missing_stats (a INT, b INT, c INT, INDEX idx_b(b)) \
         PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (100), \
         PARTITION p1 VALUES LESS THAN (200), PARTITION p2 VALUES LESS THAN (300))",
    );
    rows(
        &mut session,
        "INSERT INTO skip_missing_stats VALUES (1,1,1), (2,2,2), \
         (101,101,101), (102,102,102), (201,201,201), (202,202,202)",
    );
    rows(&mut session, "FLUSH STATS_DELTA *.*");
    rows(
        &mut session,
        "ANALYZE TABLE skip_missing_stats PARTITION p0, p1",
    );

    let global = displayed(rows(
        &mut session,
        "SHOW STATS_META WHERE table_name = 'skip_missing_stats' AND partition_name = 'global'",
    ));
    assert_eq!(global.len(), 1, "missing global statistics row: {global:?}");
    assert_eq!((global[0][4].as_str(), global[0][5].as_str()), ("2", "6"));
    assert_eq!(
        displayed(rows(
            &mut session,
            "SHOW STATS_HISTOGRAMS WHERE table_name = 'skip_missing_stats' AND partition_name = 'global'",
        ))
        .len(),
        4,
        "all three columns and idx_b must be initialized"
    );
}

/// Pinned DDL subscriber `ActionModifyColumn`: `InsertColStats2KV` is an
/// `INSERT IGNORE`, but if the histogram is absent it recreates it before
/// refreshing meta. An original CREATE TABLE column has no origin default,
/// even when it has a declared INSERT default, so Go records all old rows as
/// NULL and creates no bucket. Both MODIFY and RENAME COLUMN are this action.
#[test]
fn modify_column_ddl_recreates_missing_default_statistics_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(74))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE TABLE stats_modify_column (a INT DEFAULT 7)",
    );
    rows(
        &mut session,
        "INSERT INTO stats_modify_column VALUES (DEFAULT),(DEFAULT),(DEFAULT)",
    );
    rows(&mut session, "ANALYZE TABLE stats_modify_column");
    let (table_id, column_id) = {
        let catalog = stack.factory.catalog.load();
        let table = catalog
            .find_table("test", "stats_modify_column")
            .expect("created table is published")
            .1;
        (
            table.id,
            table.columns.get(0).expect("column exists").read().id,
        )
    };

    let remove_column_stats = |session: &mut ClusterServerSession| {
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
    let assert_recreated = |session: &mut ClusterServerSession| {
        assert_eq!(
            displayed(rows(
                session,
                &format!(
                    "SELECT distinct_count, null_count, tot_col_size, stats_ver \
                     FROM mysql.stats_histograms WHERE table_id = {table_id} \
                     AND is_index = 0 AND hist_id = {column_id}"
                ),
            )),
            [["0", "3", "0", "0"]]
        );
        assert_eq!(
            displayed(rows(
                session,
                &format!(
                    "SELECT count(*) FROM mysql.stats_buckets WHERE table_id = {table_id} \
                     AND is_index = 0 AND hist_id = {column_id}"
                ),
            )),
            [["0"]]
        );
    };

    remove_column_stats(&mut session);
    rows(
        &mut session,
        "ALTER TABLE stats_modify_column MODIFY COLUMN a BIGINT",
    );
    assert_recreated(&mut session);

    remove_column_stats(&mut session);
    rows(
        &mut session,
        "ALTER TABLE stats_modify_column RENAME COLUMN a TO b",
    );
    assert_recreated(&mut session);
}

/// Pinned `TestRecordHistoryStatsAfterAnalyze`: the global switch suppresses
/// task creation while off; once enabled, a successful ANALYZE posts its
/// physical ID and the domain dump path writes canonical blocks to
/// `mysql.stats_history`.
#[test]
fn analyze_records_historical_stats_through_the_domain_worker() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(75))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");
    rows(
        &mut session,
        "CREATE TABLE history_analyze (a INT, b VARCHAR(10), INDEX idx(a, b))",
    );

    rows(
        &mut session,
        "SET GLOBAL tidb_enable_historical_stats = OFF",
    );
    rows(&mut session, "ANALYZE TABLE history_analyze WITH 2 TOPN");
    assert_eq!(
        session
            .historical_stats_worker
            .get_one_historical_stats_table(),
        tidb_domain::historical_stats::NO_HISTORICAL_STATS_TABLE
    );

    rows(&mut session, "SET GLOBAL tidb_enable_historical_stats = ON");
    rows(&mut session, "ANALYZE TABLE history_analyze WITH 2 TOPN");
    let table_id = session
        .historical_stats_worker
        .get_one_historical_stats_table();
    assert!(table_id > 0, "a successful ANALYZE posts its table ID");
    let handle = ClusterHistoricalStatsHandle {
        transactions: Arc::clone(&stack.factory.transactions),
        catalog: Arc::clone(&stack.factory.catalog),
        global_vars: stack.factory.global_vars.clone(),
    };
    session
        .historical_stats_worker
        .dump_historical_stats(
            table_id,
            &handle,
            &tidb_domain::historical_stats::NoopHistoricalStatsMetrics,
        )
        .expect("historical statistics dump succeeds");
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!("SELECT count(*) FROM mysql.stats_history WHERE table_id = {table_id}"),
        )),
        [["1"]]
    );
    let catalog = stack.factory.catalog.load();
    let (_, table) = catalog
        .find_table("test", "history_analyze")
        .expect("analyzed table remains published");
    let table = table.clone();
    drop(catalog);
    let (historical, fallbacks) = stack
        .factory
        .dump_historical_stats_by_snapshot("test", &table, u64::MAX)
        .expect("historical statistics reload succeeds");
    assert!(fallbacks.is_empty(), "unexpected fallbacks: {fallbacks:?}");
    assert!(
        historical
            .expect("historical statistics exist")
            .is_historical_stats
    );
}

/// Pinned `TestGCOutdatedHistoryStats`: the global retention duration drives
/// explicit historical metadata and payload cleanup.
#[test]
fn clear_outdated_history_stats_uses_the_go_retention_duration() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(78))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET GLOBAL tidb_enable_historical_stats = ON");
    rows(&mut session, "CREATE TABLE history_gc (a INT)");
    rows(&mut session, "ANALYZE TABLE history_gc");
    let table_id = session
        .historical_stats_worker
        .get_one_historical_stats_table();
    let handle = ClusterHistoricalStatsHandle {
        transactions: Arc::clone(&stack.factory.transactions),
        catalog: Arc::clone(&stack.factory.catalog),
        global_vars: stack.factory.global_vars.clone(),
    };
    session
        .historical_stats_worker
        .dump_historical_stats(
            table_id,
            &handle,
            &tidb_domain::historical_stats::NoopHistoricalStatsMetrics,
        )
        .expect("historical statistics dump succeeds");
    for table in ["stats_meta_history", "stats_history"] {
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!("SELECT count(*) FROM mysql.{table} WHERE table_id = {table_id}"),
            )),
            [["1"]]
        );
    }

    rows(
        &mut session,
        "SET GLOBAL tidb_historical_stats_duration = '1s'",
    );
    std::thread::sleep(std::time::Duration::from_secs(2));
    stack
        .factory
        .clear_outdated_history_stats()
        .expect("historical statistics GC succeeds");
    for table in ["stats_meta_history", "stats_history"] {
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!("SELECT count(*) FROM mysql.{table} WHERE table_id = {table_id}"),
            )),
            [["0"]]
        );
    }
}

/// Pinned storage `TestGCStats`: stale index and column rows are removed one
/// item transaction at a time, while a dropped table needs one pass to clear
/// payload and a later pass to remove `stats_meta`.
#[test]
fn stats_gc_matches_go_item_and_dropped_table_phases() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(79))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");
    rows(
        &mut session,
        "CREATE TABLE stats_gc_t (a INT, b INT, INDEX idx(a, b), INDEX idx_a(a))",
    );
    rows(
        &mut session,
        "INSERT INTO stats_gc_t VALUES (1,1),(2,2),(3,3)",
    );
    rows(&mut session, "ANALYZE TABLE stats_gc_t WITH 0 TOPN");

    rows(&mut session, "ALTER TABLE stats_gc_t DROP INDEX idx");
    stack
        .factory
        .gc_stats(Duration::ZERO, Duration::ZERO)
        .expect("index statistics GC succeeds");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM mysql.stats_histograms",
        )),
        vec![vec!["3".to_owned()]]
    );

    rows(&mut session, "ALTER TABLE stats_gc_t DROP INDEX idx_a");
    rows(&mut session, "ALTER TABLE stats_gc_t DROP COLUMN a");
    stack
        .factory
        .gc_stats(Duration::ZERO, Duration::ZERO)
        .expect("column statistics GC succeeds");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM mysql.stats_histograms",
        )),
        vec![vec!["1".to_owned()]]
    );

    rows(&mut session, "DROP TABLE stats_gc_t");
    // Embedded DDL and GC can share one physical millisecond. Go's TSO has a
    // positive logical component while GC's wall-clock TSO has zero, so wait
    // until the strict `version < gcVer` window can include the drop event.
    std::thread::sleep(Duration::from_millis(2));
    stack
        .factory
        .gc_stats(Duration::ZERO, Duration::ZERO)
        .expect("first dropped-table GC phase succeeds");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM mysql.stats_histograms",
        )),
        vec![vec!["0".to_owned()]]
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT count(*) FROM mysql.stats_meta")),
        vec![vec!["1".to_owned()]]
    );

    std::thread::sleep(Duration::from_millis(2));
    stack
        .factory
        .gc_stats(Duration::ZERO, Duration::ZERO)
        .expect("second dropped-table GC phase succeeds");
    assert_eq!(
        displayed(rows(&mut session, "SELECT count(*) FROM mysql.stats_meta")),
        vec![vec!["0".to_owned()]]
    );
}

/// Pinned storage `TestGCPartition`, including its retained logical
/// meta-only row after both physical partitions finish two-phase GC.
#[test]
fn stats_gc_matches_go_partition_phases() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(80))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");
    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'static'",
    );
    rows(
        &mut session,
        "SET GLOBAL tidb_partition_prune_mode = 'static'",
    );
    rows(
        &mut session,
        "CREATE TABLE stats_gc_partition (a BIGINT, b BIGINT, INDEX idx(a, b)) \
         PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (3), \
         PARTITION p1 VALUES LESS THAN (6))",
    );
    rows(
        &mut session,
        "INSERT INTO stats_gc_partition VALUES (1,2),(2,3),(3,4),(4,5),(5,6)",
    );
    rows(&mut session, "ANALYZE TABLE stats_gc_partition WITH 0 TOPN");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM mysql.stats_histograms",
        )),
        vec![vec!["6".to_owned()]]
    );

    rows(
        &mut session,
        "ALTER TABLE stats_gc_partition DROP INDEX idx",
    );
    stack
        .factory
        .gc_stats(Duration::ZERO, Duration::ZERO)
        .expect("partition index statistics GC succeeds");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM mysql.stats_histograms",
        )),
        vec![vec!["4".to_owned()]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM mysql.stats_buckets",
        )),
        vec![vec!["10".to_owned()]]
    );

    rows(&mut session, "ALTER TABLE stats_gc_partition DROP COLUMN b");
    stack
        .factory
        .gc_stats(Duration::ZERO, Duration::ZERO)
        .expect("partition column statistics GC succeeds");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM mysql.stats_histograms",
        )),
        vec![vec!["2".to_owned()]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM mysql.stats_buckets",
        )),
        vec![vec!["5".to_owned()]]
    );

    rows(&mut session, "DROP TABLE stats_gc_partition");
    std::thread::sleep(Duration::from_millis(2));
    stack
        .factory
        .gc_stats(Duration::ZERO, Duration::ZERO)
        .expect("first partition-table GC phase succeeds");
    assert_eq!(
        displayed(rows(&mut session, "SELECT count(*) FROM mysql.stats_meta")),
        vec![vec!["3".to_owned()]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM mysql.stats_histograms",
        )),
        vec![vec!["0".to_owned()]]
    );

    std::thread::sleep(Duration::from_millis(2));
    stack
        .factory
        .gc_stats(Duration::ZERO, Duration::ZERO)
        .expect("second partition-table GC phase succeeds");
    assert_eq!(
        displayed(rows(&mut session, "SELECT count(*) FROM mysql.stats_meta")),
        vec![vec!["1".to_owned()]]
    );
}

/// Pinned storage `TestGCColumnStatsUsage`: a dropped column removes only its
/// usage row, then dropped-table GC removes every remaining usage row.
#[test]
fn stats_gc_matches_go_column_usage_cleanup() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(81))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE TABLE stats_gc_usage (a INT, b INT, c INT)",
    );
    rows(
        &mut session,
        "INSERT INTO stats_gc_usage VALUES (1,1,1),(2,2,2),(3,3,3)",
    );
    rows(&mut session, "ANALYZE TABLE stats_gc_usage");
    let table_id = stack
        .factory
        .catalog
        .load()
        .find_table("test", "stats_gc_usage")
        .expect("table is in the live catalog")
        .1
        .id;
    rows(
        &mut session,
        &format!(
            "INSERT INTO mysql.column_stats_usage(table_id,column_id) VALUES \
             ({table_id},1),({table_id},2),({table_id},3)"
        ),
    );

    rows(&mut session, "ALTER TABLE stats_gc_usage DROP COLUMN a");
    stack
        .factory
        .gc_stats(Duration::ZERO, Duration::ZERO)
        .expect("dropped-column usage GC succeeds");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM mysql.column_stats_usage",
        )),
        vec![vec!["2".to_owned()]]
    );

    rows(&mut session, "DROP TABLE stats_gc_usage");
    std::thread::sleep(Duration::from_millis(2));
    stack
        .factory
        .gc_stats(Duration::ZERO, Duration::ZERO)
        .expect("dropped-table usage GC succeeds");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM mysql.column_stats_usage",
        )),
        vec![vec!["0".to_owned()]]
    );
}

/// Pinned storage `TestExtremCaseOfGC`: an existing table with an empty
/// histogram set must keep its `stats_meta` row.
#[test]
fn stats_gc_keeps_meta_for_existing_table_without_histograms() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(82))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE TABLE stats_gc_empty_hist (a INT, b INT)",
    );
    rows(
        &mut session,
        "INSERT INTO stats_gc_empty_hist VALUES (1,2),(3,4)",
    );
    rows(&mut session, "ANALYZE TABLE stats_gc_empty_hist");
    let table_id = stack
        .factory
        .catalog
        .load()
        .find_table("test", "stats_gc_empty_hist")
        .expect("table is in the live catalog")
        .1
        .id;
    rows(
        &mut session,
        &format!("DELETE FROM mysql.stats_histograms WHERE table_id = {table_id}"),
    );
    stack
        .factory
        .gc_stats(Duration::ZERO, Duration::ZERO)
        .expect("empty-histogram GC succeeds");
    assert_eq!(
        displayed(rows(&mut session, "SELECT count(*) FROM mysql.stats_meta")),
        vec![vec!["1".to_owned()]]
    );
}

/// Pinned `TestDumpHistoricalStatsFallback`: after ANALYZE ran with history
/// disabled, historical dump uses the latest statistics and names that table
/// in the fallback list once the feature is enabled.
#[test]
fn historical_stats_reader_falls_back_to_latest_stats_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(77))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");
    rows(
        &mut session,
        "CREATE TABLE history_fallback (a INT, INDEX idx(a)) \
         PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (6))",
    );
    rows(
        &mut session,
        "SET GLOBAL tidb_enable_historical_stats = OFF",
    );
    rows(&mut session, "ANALYZE TABLE history_fallback");
    let catalog = stack.factory.catalog.load();
    let (_, table) = catalog
        .find_table("test", "history_fallback")
        .expect("analyzed table remains published");
    let table = table.clone();
    drop(catalog);
    assert_eq!(
        stack
            .factory
            .dump_historical_stats_by_snapshot("test", &table, u64::MAX)
            .expect_err("the Go feature gate rejects historical reads"),
        "tidb_enable_historical_stats should be enabled"
    );
    rows(&mut session, "SET GLOBAL tidb_enable_historical_stats = ON");
    let (latest, fallbacks) = stack
        .factory
        .dump_historical_stats_by_snapshot("test", &table, u64::MAX)
        .expect("latest statistics fallback succeeds");
    assert_eq!(
        fallbacks,
        ["test.history_fallback p0", "test.history_fallback global"]
    );
    let latest = latest.expect("latest statistics exist");
    assert!(!latest.is_historical_stats);
    let partitions = latest.partitions.expect("partition statistics exist");
    assert!(
        !partitions["p0"]
            .as_ref()
            .expect("partition fallback exists")
            .is_historical_stats
    );
    assert!(
        !partitions[tidb_stats::TIDB_GLOBAL_STATS]
            .as_ref()
            .expect("global fallback exists")
            .is_historical_stats
    );
}

/// Pinned `TestDumpHistoricalStatsByTable`: static ANALYZE queues only the
/// physical partition, while dynamic ANALYZE queues that partition and the
/// logical table whose dump contains global statistics.
#[test]
fn partition_analyze_queues_the_same_historical_stats_ids_as_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(76))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");
    rows(&mut session, "SET GLOBAL tidb_enable_historical_stats = ON");
    rows(
        &mut session,
        "CREATE TABLE history_partition (a INT, b INT, INDEX idx(b)) \
         PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (6))",
    );
    let catalog = stack.factory.catalog.load();
    let (_, table) = catalog
        .find_table("test", "history_partition")
        .expect("partitioned table is published");
    let logical_id = table.id;
    let partition_id = table
        .get_partition_info()
        .expect("partition metadata")
        .read()
        .definitions
        .snapshot()[0]
        .id;
    let table = table.clone();
    drop(catalog);

    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'static'",
    );
    rows(&mut session, "ANALYZE TABLE history_partition");
    assert_eq!(
        session
            .historical_stats_worker
            .get_one_historical_stats_table(),
        partition_id
    );
    assert_eq!(
        session
            .historical_stats_worker
            .get_one_historical_stats_table(),
        tidb_domain::historical_stats::NO_HISTORICAL_STATS_TABLE
    );

    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'dynamic'",
    );
    rows(&mut session, "ANALYZE TABLE history_partition");
    let mut queued = vec![
        session
            .historical_stats_worker
            .get_one_historical_stats_table(),
        session
            .historical_stats_worker
            .get_one_historical_stats_table(),
    ];
    queued.sort_unstable();
    let mut expected = vec![partition_id, logical_id];
    expected.sort_unstable();
    assert_eq!(queued, expected);
    assert_eq!(
        session
            .historical_stats_worker
            .get_one_historical_stats_table(),
        tidb_domain::historical_stats::NO_HISTORICAL_STATS_TABLE
    );

    let handle = ClusterHistoricalStatsHandle {
        transactions: Arc::clone(&stack.factory.transactions),
        catalog: Arc::clone(&stack.factory.catalog),
        global_vars: stack.factory.global_vars.clone(),
    };
    for table_id in queued {
        session
            .historical_stats_worker
            .dump_historical_stats(
                table_id,
                &handle,
                &tidb_domain::historical_stats::NoopHistoricalStatsMetrics,
            )
            .expect("partition/global historical dump succeeds");
    }
    for table_id in [partition_id, logical_id] {
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!("SELECT count(*) FROM mysql.stats_history WHERE table_id = {table_id}"),
            )),
            [["1"]]
        );
    }
    let (historical, _) = stack
        .factory
        .dump_historical_stats_by_snapshot("test", &table, u64::MAX)
        .expect("partition historical statistics reload succeeds");
    let partitions = historical
        .expect("partition historical statistics exist")
        .partitions
        .expect("partition map exists");
    assert!(partitions["p0"].is_some());
    assert!(partitions[tidb_stats::TIDB_GLOBAL_STATS].is_some());
}

/// The probe-33 regression: a derived table whose inner SELECT plans as a
/// partial-aggregate push (root HashAgg over `TableReader(data:HashAgg)`)
/// must still answer the aggregate, not the bare scan rows. Go returns
/// `((1,30),(2,120))`; the broken handoff returned five raw rows.
#[test]
fn a_derived_aggregate_over_the_coprocessor_answers_its_output() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(7))
        .expect("session opens");
    rows(&mut session, "CREATE TABLE test.rep (g int, v int)");
    rows(
        &mut session,
        "INSERT INTO test.rep VALUES (1, 10), (1, 20), (2, 40), (2, 80), (1, 0)",
    );

    let inner = displayed(rows(
        &mut session,
        "SELECT g, sum(v) AS t FROM test.rep GROUP BY g ORDER BY g",
    ));
    assert_eq!(
        inner,
        [["1", "30"], ["2", "120"]],
        "the inner aggregate alone must already be right"
    );

    let derived = displayed(rows(
        &mut session,
        "SELECT * FROM (SELECT g, sum(v) AS t FROM test.rep GROUP BY g) s ORDER BY g",
    ));
    assert_eq!(
        derived,
        [["1", "30"], ["2", "120"]],
        "the derived consumer must see the aggregate, not the scan rows"
    );

    // The COUNT(*) shape panicked the worker thread on the live node
    // (chunk column index out of bounds); here a panic fails the test.
    let counted = displayed(rows(
        &mut session,
        "SELECT * FROM (SELECT g, count(*) AS c FROM test.rep GROUP BY g) s ORDER BY g",
    ));
    assert_eq!(counted, [["1", "3"], ["2", "2"]]);

    // The desc keep-order Limit rides the REVERSED region walk -- the
    // shape whose first live draft returned NOTHING because the reverse
    // scan's caller-swaps-the-bounds contract was missed.
    rows(
        &mut session,
        "CREATE TABLE test.walk (id bigint primary key, v int)",
    );
    rows(
        &mut session,
        "INSERT INTO test.walk VALUES (1, 10), (2, 20), (3, 30), (5, 50), (100, 1)",
    );
    let descending = displayed(rows(
        &mut session,
        "SELECT id FROM test.walk WHERE id > 1 ORDER BY id DESC LIMIT 2",
    ));
    assert_eq!(
        descending,
        [["100"], ["5"]],
        "the desc keep-order Limit must answer the LARGEST ids over the region walk"
    );

    // The covering-index COUNT rides an [IndexScan, Aggregation] DAG:
    // the region decodes the indexed values out of the KEY and counts
    // them, Go's PhysicalIndexReader carrying the partial stage.
    rows(&mut session, "CREATE INDEX walk_v ON test.walk (v)");
    let counted_over_index = displayed(rows(
        &mut session,
        "SELECT count(v) FROM test.walk WHERE v > 5",
    ));
    assert_eq!(counted_over_index, [["4"]]);

    // A full covering SUM uses the unordered Global aggregate contract. Its
    // input is indexed in the pruned scan schema, so lowering must translate
    // that offset back through the table schema before reading index keys.
    let summed_over_index = displayed(rows(&mut session, "SELECT sum(v) FROM test.walk"));
    assert_eq!(summed_over_index, [["111"]]);
}

/// Go `setDataForServersInfo` (`infoschema_reader.go:2730`) over
/// `GetAllServerInfo`: one row per server, in Go's eight-column order.
/// With no etcd client the syncer answers THIS node alone -- Go's
/// `etcdCli == nil` path -- which is what a single-node deployment shows.
#[test]
fn tidb_servers_info_reports_this_node() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(11))
        .expect("session opens");

    let rows = displayed(rows(
        &mut session,
        "SELECT DDL_ID, IP, PORT, STATUS_PORT, LEASE, VERSION, GIT_HASH, LABELS \
         FROM information_schema.tidb_servers_info",
    ));
    assert_eq!(rows.len(), 1, "a single node reports itself alone");
    let row = &rows[0];

    // Go's DDL_ID is `uuid.New().String()`; the shape is what a peer's
    // stale-entry match and this reader both see.
    assert_eq!(row[0].len(), 36, "DDL_ID is a uuid: {}", row[0]);
    assert_eq!(row[0].matches('-').count(), 4, "{}", row[0]);
    // The port the node was configured with, as an integer column.
    assert_eq!(row[2], "0", "the fixture binds an ephemeral port");
    assert_eq!(row[3], "10080", "the default status port");
    // The lease travels as text, and the version pair is the build's.
    assert!(row[4].ends_with("ms"), "LEASE is text: {}", row[4]);
    assert!(!row[5].is_empty(), "VERSION is reported");
    // No labels are configured, which renders as the empty string rather
    // than a stray separator (Go `BuildStringFromLabels`).
    assert_eq!(row[7], "");
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

/// Go `dataForTiDBClusterInfo` (`infoschema_reader.go:1842`) over
/// `GetClusterServerInfo`: one row per node, describing where it is and how
/// long it has been up.
///
/// Go chains five retrievers there and only the first has a source here, so
/// this reports the TiDB rows alone -- see `Session::cluster_info_table_rows`
/// for the four it cannot see and why inventing them would be worse.
#[test]
fn cluster_info_reports_this_node() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(19))
        .expect("session opens");

    let reported = displayed(rows(
        &mut session,
        "SELECT TYPE, INSTANCE, STATUS_ADDRESS, VERSION, GIT_HASH, UPTIME, SERVER_ID \
         FROM information_schema.cluster_info",
    ));
    assert_eq!(reported.len(), 1, "a single node reports itself alone");
    let row = &reported[0];
    assert_eq!(row[0], "tidb");
    // Both addresses are host:port, and they are the node's own two ports.
    assert!(row[1].contains(':'), "INSTANCE is host:port: {}", row[1]);
    assert!(
        row[2].contains(':'),
        "STATUS_ADDRESS is host:port: {}",
        row[2]
    );
    assert_ne!(row[1], row[2], "the SQL and status ports differ");
    assert!(!row[3].is_empty(), "VERSION is reported");
    assert!(!row[4].is_empty(), "GIT_HASH is reported");
    // Go prints `time.Since(startTime).String()`, so the unit is spelled out.
    assert!(
        row[5].ends_with('s'),
        "UPTIME is a Go duration string: {}",
        row[5]
    );

    // The instance is the same node TIDB_SERVERS_INFO describes, which is the
    // point of the two tables agreeing.
    let servers = displayed(rows(
        &mut session,
        "SELECT IP, PORT FROM information_schema.tidb_servers_info",
    ));
    assert_eq!(row[1], format!("{}:{}", servers[0][0], servers[0][1]));
}

/// A `DATETIME(n)`/`TIMESTAMP(n) DEFAULT CURRENT_TIMESTAMP(n)` column must
/// survive the round trip through this node's own catalog loader.
///
/// Go stores the marker WORD alone and re-derives the fsp from the column's
/// decimal wherever the default is printed. The loader used to rebuild the
/// written spelling as a bare word and then apply Go's admission-time
/// "written fsp must equal the column's" check to it, which no bare word can
/// satisfy for a column with an fsp. It therefore REFUSED a table its own
/// DDL had just published -- the worst shape a DDL can take, since CREATE
/// then reports 1050 while every read reports 1146.
#[test]
fn a_fractional_clock_default_survives_the_catalog_loader() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(23))
        .expect("session opens");

    rows(
        &mut session,
        "CREATE TABLE test.dt (o datetime(3) DEFAULT CURRENT_TIMESTAMP(3), v int)",
    );
    rows(
        &mut session,
        "CREATE TABLE test.ts (o timestamp(6) DEFAULT CURRENT_TIMESTAMP(6))",
    );

    // The table is READABLE, which is what the refusal used to break.
    let shown = displayed(rows(&mut session, "SHOW CREATE TABLE test.dt"));
    assert!(
        shown[0][1].contains("`o` datetime(3) DEFAULT CURRENT_TIMESTAMP(3)"),
        "{}",
        shown[0][1]
    );
    let shown = displayed(rows(&mut session, "SHOW CREATE TABLE test.ts"));
    assert!(
        shown[0][1].contains("`o` timestamp(6) DEFAULT CURRENT_TIMESTAMP(6)"),
        "{}",
        shown[0][1]
    );

    // And the marker still evaluates per row rather than storing the word.
    rows(&mut session, "INSERT INTO test.dt (v) VALUES (1)");
    assert_eq!(
        displayed(rows(&mut session, "SELECT v, o IS NOT NULL FROM test.dt")),
        [["1", "1"]]
    );
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

/// The REST of Go `fieldTypeFromPBColumn`: flag is not the only field the
/// decode needs.
///
/// `elems` decides what an ENUM/SET ordinal means, and `decimal` the scale a
/// DECIMAL and the fsp a TIME/DATETIME read back with. The scan path rebuilt
/// none of them, so this pins each against the point-get path, which builds
/// its types from the catalog and therefore never lost them. A disagreement
/// here is the same class of bug as the unsigned one: one table, two paths,
/// two answers.
#[test]
fn the_scan_path_decodes_elems_and_scale_like_the_point_get_path() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(37))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.tt (id int primary key, en enum('alpha','beta','gamma'), \
         st set('p','q','r'), dc decimal(12,4), yr year, tm time(3), dt datetime(6))",
    );
    rows(
        &mut session,
        "INSERT INTO test.tt VALUES (1, 'gamma', 'q,r', 12345.6789, 2024, \
         '12:34:56.789', '2024-03-04 05:06:07.891011')",
    );

    let columns = "en, st, dc, yr, tm, dt";
    // `id = 1` is a point get; `id > 0 AND id < 2` is a scan of the same row.
    let point_get = displayed(rows(
        &mut session,
        &format!("SELECT {columns} FROM test.tt WHERE id = 1"),
    ));
    let scanned = displayed(rows(
        &mut session,
        &format!("SELECT {columns} FROM test.tt WHERE id > 0 AND id < 2"),
    ));
    assert_eq!(point_get, scanned, "the two paths must read one row alike");
    assert_eq!(
        scanned,
        [[
            "gamma".to_owned(),
            "q,r".to_owned(),
            "12345.6789".to_owned(),
            "2024".to_owned(),
            "12:34:56.789".to_owned(),
            "2024-03-04 05:06:07.891011".to_owned(),
        ]],
        "and both must read what was stored"
    );
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

/// THE DIFFERENTIAL, as a test: a predicate pushed into the coprocessor and
/// the same predicate evaluated locally must select the same rows.
///
/// `WHERE p` is answered by the region's filter; `sum(CASE WHEN p ...)` is
/// answered by the local evaluator over the same rows. Any disagreement is a
/// silent wrong answer -- the region either invented a row or dropped one --
/// and no single-path test can see it, because each evaluator is correct
/// against its own inputs. This is how the UNSIGNED decode bug was found,
/// after every existing test passed straight through it.
///
/// The fixture is deliberately made of boundary values: agreement on
/// ordinary data proves nothing.
#[test]
fn a_pushed_down_predicate_selects_what_local_evaluation_selects() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(43))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.diff (id int primary key, i bigint, u bigint unsigned, \
         s varchar(20) COLLATE utf8mb4_general_ci, b varchar(20) COLLATE utf8mb4_bin, \
         dc decimal(12,3), d datetime)",
    );
    rows(
        &mut session,
        "INSERT INTO test.diff VALUES \
         (1, -9223372036854775808, 18446744073709551615, 'Hello', 'Hello', -999999.999, \
          '1000-01-01 00:00:00'), \
         (2, 0, 0, '', '', 0.000, '2024-06-15 12:30:45'), \
         (3, NULL, NULL, NULL, NULL, NULL, NULL), \
         (4, 9223372036854775807, 9223372036854775808, 'HELLO', 'HELLO', 999999.999, \
          '9999-12-31 23:59:59'), \
         (5, -1, 1, 'world', 'world', -0.001, '2000-02-29 00:00:00')",
    );

    const PREDICATES: &[&str] = &[
        // Signed and unsigned integers at their extremes.
        "i > 0",
        "i < 0",
        "i = -9223372036854775808",
        "u > 2",
        "u >= 9223372036854775808",
        "u = 18446744073709551615",
        // Three-valued logic.
        "i IS NULL",
        "i IS NOT NULL AND u > 0",
        "NOT (i > 0)",
        "i = 0 OR u = 0",
        "i IN (0, -1)",
        "i NOT IN (0)",
        // Collation-sensitive comparison, both sides of the pair.
        "s = 'hello'",
        "b = 'hello'",
        "s > 'HELLO'",
        // Other families, and cross-type coercion.
        "dc > 0",
        "dc = -0.001",
        "d > '2024-01-01'",
        "i = '0'",
        "i BETWEEN '-1' AND '1'",
    ];

    for predicate in PREDICATES {
        let pushed = displayed(rows(
            &mut session,
            &format!("SELECT count(*) FROM test.diff WHERE {predicate}"),
        ));
        let local = displayed(rows(
            &mut session,
            &format!(
                "SELECT coalesce(sum(CASE WHEN ({predicate}) THEN 1 ELSE 0 END), 0) \
                 FROM test.diff"
            ),
        ));
        assert_eq!(
            pushed, local,
            "`{predicate}`: the region and the local evaluator disagree"
        );
    }
}

/// Statistics may change the PLAN; they must never change the ANSWER.
///
/// `ANALYZE` replaces pseudo estimates with real ones and the optimizer then
/// picks differently. What must hold either way is that every query returns
/// the same rows before and after: a cost decision that alters results is a
/// wrong answer no estimate can justify.
///
/// The plan TEXT changing shape across `ANALYZE` is not evidence of lost
/// pushdown -- see `crate::explain`'s first named divergence, where every row
/// prints task `root` whether or not the wire pushed anything, and
/// `analyze_does_not_stop_a_pushed_shape_reaching_the_region` for the receipt
/// that actually answers it.
#[test]
fn analyze_changes_the_plan_and_never_the_answer() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(47))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.st (id int primary key, a int, u bigint unsigned, \
         s varchar(10), KEY ka(a), KEY ku(u))",
    );
    rows(
        &mut session,
        "INSERT INTO test.st VALUES (1,1,18446744073709551615,'x'), (2,1,0,'y'), \
         (3,2,9223372036854775808,'z'), (4,3,1,'w'), (5,3,NULL,NULL)",
    );

    const QUERIES: &[&str] = &[
        "SELECT id FROM test.st WHERE a = 1 ORDER BY id",
        "SELECT id FROM test.st WHERE a >= 2 ORDER BY id",
        "SELECT count(*) FROM test.st WHERE u > 2",
        "SELECT id FROM test.st WHERE u = 18446744073709551615",
        "SELECT a, count(*) FROM test.st GROUP BY a ORDER BY a",
        "SELECT id FROM test.st WHERE s IS NULL",
        "SELECT id FROM test.st ORDER BY u DESC LIMIT 2",
        "SELECT max(u), min(u) FROM test.st",
    ];

    let before: Vec<_> = QUERIES
        .iter()
        .map(|query| displayed(rows(&mut session, query)))
        .collect();

    rows(&mut session, "ANALYZE TABLE test.st");

    for (query, expected) in QUERIES.iter().zip(before) {
        assert_eq!(
            displayed(rows(&mut session, query)),
            expected,
            "`{query}` answered differently once statistics existed"
        );
    }
}

/// A grouped aggregate keeps the same answer once statistics exist.
#[test]
fn analyze_does_not_change_a_grouped_aggregate_answer() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(53))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.pd (id int primary key, g int, v int)",
    );
    rows(
        &mut session,
        "INSERT INTO test.pd VALUES (1,1,10),(2,1,20),(3,2,40),(4,2,80),(5,1,0)",
    );

    let query = "SELECT g, sum(v) FROM test.pd GROUP BY g ORDER BY g";
    let expected = displayed(rows(&mut session, query));

    rows(&mut session, "ANALYZE TABLE test.pd");

    assert_eq!(
        displayed(rows(&mut session, query)),
        expected,
        "the answer changed once statistics existed"
    );
}

/// Pinned `globalstats.TestShowGlobalStatsWithAsyncMergeGlobal` and
/// `TestShowGlobalStatsWithoutAsyncMergeGlobal`: static pruning publishes
/// only physical-partition statistics, while dynamic pruning also publishes
/// the logical table's global column and index statistics. Every SHOW surface
/// must traverse the same set.
#[test]
fn partition_analyze_show_surfaces_match_global_stats_visibility() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(57))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");

    let mut check = |table: &str,
                     prune_mode: &str,
                     meta_count: usize,
                     global_meta_count: usize,
                     bucket_count: usize,
                     global_bucket_count: usize,
                     histogram_count: usize,
                     global_histogram_count: usize,
                     healthy_count: usize,
                     global_healthy_count: usize| {
        rows(
            &mut session,
            &format!("SET SESSION tidb_partition_prune_mode = '{prune_mode}'"),
        );
        rows(
            &mut session,
            &format!("CREATE TABLE {table} (a int, KEY(a)) PARTITION BY HASH(a) PARTITIONS 2"),
        );
        rows(
            &mut session,
            &format!("INSERT INTO {table} VALUES (1), (2), (3), (4)"),
        );
        rows(
            &mut session,
            &format!("ANALYZE TABLE {table} WITH 0 TOPN, 1 BUCKETS"),
        );

        let show_count =
            |session: &mut _, statement: &str| displayed(rows(session, statement)).len();
        assert_eq!(
            show_count(
                &mut session,
                &format!("SHOW STATS_META WHERE table_name = '{table}'"),
            ),
            meta_count
        );
        assert_eq!(
            show_count(
                &mut session,
                &format!(
                    "SHOW STATS_META WHERE table_name = '{table}' AND partition_name = 'global'"
                ),
            ),
            global_meta_count
        );
        assert_eq!(
            show_count(
                &mut session,
                &format!("SHOW STATS_BUCKETS WHERE table_name = '{table}'"),
            ),
            bucket_count
        );
        assert_eq!(
            show_count(
                &mut session,
                &format!(
                    "SHOW STATS_BUCKETS WHERE table_name = '{table}' AND partition_name = 'global'"
                ),
            ),
            global_bucket_count
        );
        assert_eq!(
            show_count(
                &mut session,
                &format!("SHOW STATS_HISTOGRAMS WHERE table_name = '{table}'"),
            ),
            histogram_count
        );
        assert_eq!(
            show_count(
                &mut session,
                &format!(
                    "SHOW STATS_HISTOGRAMS WHERE table_name = '{table}' AND partition_name = 'global'"
                ),
            ),
            global_histogram_count
        );
        assert_eq!(
            show_count(
                &mut session,
                &format!("SHOW STATS_HEALTHY WHERE table_name = '{table}'"),
            ),
            healthy_count
        );
        assert_eq!(
            show_count(
                &mut session,
                &format!(
                    "SHOW STATS_HEALTHY WHERE table_name = '{table}' AND partition_name = 'global'"
                ),
            ),
            global_healthy_count
        );
    };

    check("gs_static", "static", 2, 0, 4, 0, 4, 0, 2, 0);
    check("gs_dynamic", "dynamic", 3, 1, 6, 2, 6, 2, 3, 1);
}

/// Pinned `globalstats.TestGlobalStatsHealthy`: global and physical metadata
/// receive the same flushed deltas, while health is calculated against each
/// object's own analyzed row count.
#[test]
fn partition_global_stats_health_matches_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(102))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");
    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'dynamic'",
    );
    rows(
        &mut session,
        "CREATE TABLE global_health (a INT, KEY(a)) PARTITION BY RANGE (a) (\
         PARTITION p0 VALUES LESS THAN (10),\
         PARTITION p1 VALUES LESS THAN (20))",
    );

    let check_meta = |session: &mut _, expected: [(&str, &str); 3]| {
        let actual = displayed(rows(
            session,
            "SHOW STATS_META WHERE table_name = 'global_health'",
        ));
        assert_eq!(actual.len(), 3, "{actual:?}");
        assert_eq!(
            actual
                .iter()
                .map(|row| (row[4].as_str(), row[5].as_str()))
                .collect::<Vec<_>>(),
            expected
        );
    };
    let check_healthy = |session: &mut _, expected: [&str; 3]| {
        let actual = displayed(rows(
            session,
            "SHOW STATS_HEALTHY WHERE table_name = 'global_health'",
        ));
        assert_eq!(actual.len(), 3, "{actual:?}");
        assert_eq!(
            actual.iter().map(|row| row[3].as_str()).collect::<Vec<_>>(),
            expected
        );
    };
    let flush_and_update = |session: &mut _| {
        let reloads = stack._stats_reloader.stats().reloads;
        rows(session, "FLUSH STATS_DELTA *.*");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while stack._stats_reloader.stats().reloads == reloads {
            assert!(
                std::time::Instant::now() < deadline,
                "statistics update did not run after FLUSH STATS_DELTA"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    };

    rows(&mut session, "ANALYZE TABLE global_health");
    check_meta(&mut session, [("0", "0"), ("0", "0"), ("0", "0")]);
    check_healthy(&mut session, ["100", "100", "100"]);

    rows(&mut session, "INSERT INTO global_health VALUES (1),(2)");
    flush_and_update(&mut session);
    check_meta(&mut session, [("2", "2"), ("2", "2"), ("0", "0")]);
    check_healthy(&mut session, ["0", "0", "100"]);

    rows(
        &mut session,
        "INSERT INTO global_health VALUES (11),(12),(13),(14)",
    );
    flush_and_update(&mut session);
    check_meta(&mut session, [("6", "6"), ("2", "2"), ("4", "4")]);
    check_healthy(&mut session, ["0", "0", "0"]);

    rows(&mut session, "ANALYZE TABLE global_health");
    check_meta(&mut session, [("0", "6"), ("0", "2"), ("0", "4")]);
    check_healthy(&mut session, ["100", "100", "100"]);

    rows(
        &mut session,
        "INSERT INTO global_health VALUES (4),(5),(15),(16)",
    );
    flush_and_update(&mut session);
    check_meta(&mut session, [("4", "10"), ("2", "4"), ("2", "6")]);
    check_healthy(&mut session, ["33", "0", "50"]);
}

/// Pinned `globalstats.TestBuildGlobalLevelStats`: static ANALYZE publishes
/// physical rows only, dynamic ANALYZE adds the logical global row, and
/// predicate-column demand controls the complete histogram inventory.
#[test]
fn build_global_level_stats_matches_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(103))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");
    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'static'",
    );
    rows(
        &mut session,
        "CREATE TABLE global_level (a INT, b INT, c INT) PARTITION BY HASH(a) PARTITIONS 3",
    );
    rows(&mut session, "CREATE TABLE global_level_plain (a INT)");
    rows(
        &mut session,
        "INSERT INTO global_level VALUES (1,1,1),(3,12,3),(4,20,4),(2,7,2),(5,21,5)",
    );
    rows(
        &mut session,
        "INSERT INTO global_level_plain VALUES (1),(3),(4),(2),(5)",
    );
    rows(
        &mut session,
        "CREATE INDEX idx_global_level_ab ON global_level(a,b)",
    );
    rows(
        &mut session,
        "CREATE INDEX idx_global_level_b ON global_level(b)",
    );
    rows(&mut session, "SELECT * FROM global_level WHERE c = 0");
    rows(&mut session, "SELECT * FROM global_level_plain WHERE a = 0");
    stack
        .factory
        .dump_col_stats_usage_to_kv("default")
        .expect("predicate-column usage dump");

    let count_rows = |session: &mut _, table: &str| {
        let mut counts = displayed(rows(
            session,
            &format!("SHOW STATS_META WHERE table_name = '{table}'"),
        ))
        .into_iter()
        .map(|row| row[5].clone())
        .collect::<Vec<_>>();
        counts.sort();
        counts
    };
    let histogram_count = |session: &mut _, table: &str| {
        rows(
            session,
            &format!("SHOW STATS_HISTOGRAMS WHERE table_name = '{table}'"),
        )
        .len()
    };

    rows(
        &mut session,
        "ANALYZE TABLE global_level, global_level_plain",
    );
    assert_eq!(count_rows(&mut session, "global_level"), ["1", "2", "2"]);
    assert_eq!(histogram_count(&mut session, "global_level"), 15);
    assert_eq!(count_rows(&mut session, "global_level_plain"), ["5"]);
    assert_eq!(histogram_count(&mut session, "global_level_plain"), 1);

    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'dynamic'",
    );
    rows(
        &mut session,
        "ANALYZE TABLE global_level, global_level_plain",
    );
    assert_eq!(
        count_rows(&mut session, "global_level"),
        ["1", "2", "2", "5"]
    );
    assert_eq!(histogram_count(&mut session, "global_level"), 20);
    assert_eq!(count_rows(&mut session, "global_level_plain"), ["5"]);
    assert_eq!(histogram_count(&mut session, "global_level_plain"), 1);

    rows(
        &mut session,
        "ANALYZE TABLE global_level INDEX idx_global_level_ab, idx_global_level_b",
    );
    assert_eq!(
        count_rows(&mut session, "global_level"),
        ["1", "2", "2", "5"]
    );
    assert_eq!(histogram_count(&mut session, "global_level"), 20);
}

#[test]
fn global_topn_merge_matches_issue_24349() {
    assert_global_topn_merge_matches_issue_24349(None, 104);
}

#[test]
fn concurrent_global_topn_merge_matches_issue_24349() {
    assert_global_topn_merge_matches_issue_24349(Some(2), 105);
}

/// Pinned `globalstats.TestIssues24349` and its concurrency-two twin: global
/// TopN candidates include values recovered from sibling partition histograms
/// before the remaining histogram buckets are merged.
fn assert_global_topn_merge_matches_issue_24349(
    merge_concurrency: Option<u64>,
    connection_id: u64,
) {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(connection_id))
        .expect("session opens");
    rows(&mut session, "USE test");
    if let Some(merge_concurrency) = merge_concurrency {
        rows(
            &mut session,
            &format!("SET GLOBAL tidb_merge_partition_stats_concurrency = {merge_concurrency}"),
        );
    }
    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'dynamic'",
    );
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");
    rows(
        &mut session,
        "CREATE TABLE global_topn_merge (a INT, b INT) PARTITION BY HASH(a) PARTITIONS 3",
    );
    rows(
        &mut session,
        "INSERT INTO global_topn_merge VALUES \
         (0,3),(0,3),(0,3),(0,2),(1,1),(1,2),(1,2),(1,2),(1,3),(1,4),(2,1),(2,1)",
    );
    rows(
        &mut session,
        "SELECT * FROM global_topn_merge WHERE a = 0 AND b = 3",
    );
    stack
        .factory
        .dump_col_stats_usage_to_kv("default")
        .expect("predicate-column usage dump");
    rows(
        &mut session,
        "ANALYZE TABLE global_topn_merge WITH 1 TOPN, 3 BUCKETS",
    );

    let mut global_topn = displayed(rows(
        &mut session,
        "SHOW STATS_TOPN WHERE table_name = 'global_topn_merge' AND partition_name = 'global'",
    ));
    global_topn.sort();
    assert_eq!(
        global_topn,
        [
            ["test", "global_topn_merge", "global", "a", "0", "1", "6"],
            ["test", "global_topn_merge", "global", "b", "0", "2", "4"],
        ]
    );

    rows(
        &mut session,
        "EXPLAIN SELECT * FROM global_topn_merge WHERE a > 0 AND b > 0",
    );
    let mut all_topn = displayed(rows(
        &mut session,
        "SHOW STATS_TOPN WHERE table_name = 'global_topn_merge'",
    ));
    all_topn.sort();
    assert_eq!(
        all_topn,
        [
            ["test", "global_topn_merge", "global", "a", "0", "1", "6"],
            ["test", "global_topn_merge", "global", "b", "0", "2", "4"],
            ["test", "global_topn_merge", "p0", "a", "0", "0", "4"],
            ["test", "global_topn_merge", "p0", "b", "0", "3", "3"],
            ["test", "global_topn_merge", "p1", "a", "0", "1", "6"],
            ["test", "global_topn_merge", "p1", "b", "0", "2", "3"],
            ["test", "global_topn_merge", "p2", "a", "0", "2", "2"],
            ["test", "global_topn_merge", "p2", "b", "0", "1", "2"],
        ]
    );

    let mut buckets = displayed(rows(
        &mut session,
        "SHOW STATS_BUCKETS WHERE table_name = 'global_topn_merge'",
    ));
    buckets.sort();
    assert_eq!(
        buckets,
        [
            [
                "test",
                "global_topn_merge",
                "global",
                "a",
                "0",
                "0",
                "4",
                "4",
                "0",
                "0",
                "0"
            ],
            [
                "test",
                "global_topn_merge",
                "global",
                "a",
                "0",
                "1",
                "6",
                "2",
                "2",
                "2",
                "0"
            ],
            [
                "test",
                "global_topn_merge",
                "global",
                "b",
                "0",
                "0",
                "8",
                "1",
                "1",
                "4",
                "0"
            ],
            [
                "test",
                "global_topn_merge",
                "p0",
                "b",
                "0",
                "0",
                "1",
                "1",
                "2",
                "2",
                "0"
            ],
            [
                "test",
                "global_topn_merge",
                "p1",
                "b",
                "0",
                "0",
                "2",
                "1",
                "1",
                "3",
                "0"
            ],
            [
                "test",
                "global_topn_merge",
                "p1",
                "b",
                "0",
                "1",
                "3",
                "1",
                "4",
                "4",
                "0"
            ],
        ]
    );
}

/// Pinned `globalstats.TestMergeGlobalStatsForCMSketch`: a global equality
/// estimate uses the merged sketch while partition pruning retains p0.
#[test]
fn merged_global_cmsketch_drives_equality_estimate() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(106))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");
    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'dynamic'",
    );
    rows(
        &mut session,
        "CREATE TABLE global_cms (a INT) PARTITION BY RANGE (a) (\
         PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))",
    );
    rows(
        &mut session,
        "INSERT INTO global_cms VALUES \
         (1),(2),(3),(4),(5),(6),(6),(NULL),(11),(12),(13),(14),(15),(16),(17),(18),(19),(19)",
    );
    rows(&mut session, "ANALYZE TABLE global_cms");

    assert_eq!(
        displayed(rows(
            &mut session,
            "EXPLAIN FORMAT = 'brief' SELECT * FROM global_cms WHERE a = 1",
        )),
        [
            [
                "TableReader",
                "1.00",
                "root",
                "partition:p0",
                "data:Selection"
            ],
            [
                "└─Selection",
                "1.00",
                "cop[tikv]",
                "",
                "eq(test.global_cms.a, 1)"
            ],
            [
                "  └─TableFullScan",
                "18.00",
                "cop[tikv]",
                "table:global_cms",
                "keep order:false"
            ],
        ]
    );
}

/// Pinned `globalstats.TestGlobalStatsData3`: composite-index global TopN,
/// buckets, and NDV preserve the source behavior for every covered key type.
#[test]
fn composite_index_global_stats_match_go_types() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(107))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");
    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'dynamic'",
    );

    let cases = [
        (
            "tintint",
            "INT",
            "(1,1),(1,2),(2,1),(2,2),(2,3),(2,3),(3,1),(3,1),(3,1),\
             (11,1),(12,1),(12,2),(13,1),(13,1),(13,2),(13,2),(13,2)",
            ["1", "2", "3"],
        ),
        (
            "tintstr",
            "VARCHAR(32)",
            "(1,'1'),(1,'2'),(2,'1'),(2,'2'),(2,'3'),(2,'3'),(3,'1'),(3,'1'),(3,'1'),\
             (11,'1'),(12,'1'),(12,'2'),(13,'1'),(13,'1'),(13,'2'),(13,'2'),(13,'2')",
            ["1", "2", "3"],
        ),
        (
            "tintdouble",
            "DOUBLE",
            "(1,1),(1,2),(2,1),(2,2),(2,3),(2,3),(3,1),(3,1),(3,1),\
             (11,1),(12,1),(12,2),(13,1),(13,1),(13,2),(13,2),(13,2)",
            ["1", "2", "3"],
        ),
        (
            "tdoubledecimal",
            "DECIMAL(30,2)",
            "(1,1),(1,2),(2,1),(2,2),(2,3),(2,3),(3,1),(3,1),(3,1),\
             (11,1),(12,1),(12,2),(13,1),(13,1),(13,2),(13,2),(13,2)",
            ["1.00", "2.00", "3.00"],
        ),
        (
            "tstrdt",
            "DATETIME",
            "(1,'2000-01-01'),(1,'2000-01-02'),(2,'2000-01-01'),\
             (2,'2000-01-02'),(2,'2000-01-03'),(2,'2000-01-03'),\
             (3,'2000-01-01'),(3,'2000-01-01'),(3,'2000-01-01'),\
             (11,'2000-01-01'),(12,'2000-01-01'),(12,'2000-01-02'),\
             (13,'2000-01-01'),(13,'2000-01-01'),(13,'2000-01-02'),\
             (13,'2000-01-02'),(13,'2000-01-02')",
            [
                "2000-01-01 00:00:00",
                "2000-01-02 00:00:00",
                "2000-01-03 00:00:00",
            ],
        ),
    ];

    for (table, b_type, values, [one, two, three]) in cases {
        rows(
            &mut session,
            &format!(
                "CREATE TABLE {table} (a INT, b {b_type}, KEY(a,b)) \
                 PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (10), \
                 PARTITION p1 VALUES LESS THAN (20))"
            ),
        );
        rows(
            &mut session,
            &format!("INSERT INTO {table} VALUES {values}"),
        );
        rows(
            &mut session,
            &format!("ANALYZE TABLE {table} WITH 2 TOPN, 2 BUCKETS"),
        );

        let meta = displayed(rows(
            &mut session,
            &format!("SHOW STATS_META WHERE table_name = '{table}'"),
        ));
        assert_eq!(
            meta.iter().map(|row| row[5].as_str()).collect::<Vec<_>>(),
            ["17", "9", "8"],
            "{table} metadata"
        );

        let topn = displayed(rows(
            &mut session,
            &format!("SHOW STATS_TOPN WHERE table_name = '{table}' AND is_index = 1"),
        ))
        .into_iter()
        .map(|row| row.join(" "))
        .collect::<Vec<_>>();
        assert_eq!(
            topn,
            [
                format!("test {table} global a 1 (3, {one}) 3"),
                format!("test {table} global a 1 (13, {two}) 3"),
                format!("test {table} p0 a 1 (2, {three}) 2"),
                format!("test {table} p0 a 1 (3, {one}) 3"),
                format!("test {table} p1 a 1 (13, {one}) 2"),
                format!("test {table} p1 a 1 (13, {two}) 3"),
            ],
            "{table} TopN"
        );

        let buckets = displayed(rows(
            &mut session,
            &format!("SHOW STATS_BUCKETS WHERE table_name = '{table}' AND is_index = 1"),
        ))
        .into_iter()
        .map(|row| row.join(" "))
        .collect::<Vec<_>>();
        assert_eq!(
            buckets,
            [
                format!("test {table} global a 1 0 6 2 (1, {one}) (2, {three}) 0"),
                format!("test {table} global a 1 1 11 2 (11, {one}) (13, {one}) 0"),
                format!("test {table} p0 a 1 0 3 1 (1, {one}) (2, {one}) 0"),
                format!("test {table} p0 a 1 1 4 1 (2, {two}) (2, {two}) 0"),
                format!("test {table} p1 a 1 0 2 1 (11, {one}) (12, {one}) 0"),
                format!("test {table} p1 a 1 1 3 1 (12, {two}) (12, {two}) 0"),
            ],
            "{table} buckets"
        );

        let histograms = displayed(rows(
            &mut session,
            &format!("SHOW STATS_HISTOGRAMS WHERE table_name = '{table}' AND is_index = 1"),
        ));
        assert_eq!(
            histograms
                .iter()
                .map(|row| row[6].as_str())
                .collect::<Vec<_>>(),
            ["11", "6", "5"],
            "{table} NDV"
        );
    }
}

#[test]
fn scalar_column_and_index_global_stats_match_go() {
    assert_scalar_column_and_index_global_stats_match_go(None, 108);
}

#[test]
fn concurrent_scalar_column_and_index_global_stats_match_go() {
    assert_scalar_column_and_index_global_stats_match_go(Some(2), 109);
}

/// Pinned `globalstats.TestGlobalStatsData2` and its concurrency-two twin.
fn assert_scalar_column_and_index_global_stats_match_go(
    merge_concurrency: Option<u64>,
    connection_id: u64,
) {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(connection_id))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");
    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'dynamic'",
    );
    if let Some(merge_concurrency) = merge_concurrency {
        rows(
            &mut session,
            &format!("SET GLOBAL tidb_merge_partition_stats_concurrency = {merge_concurrency}"),
        );
    }

    let cases = [
        (
            "tint",
            "CREATE TABLE tint (c INT, KEY(c)) PARTITION BY RANGE (c) (\
             PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))",
            "(1),(2),(3),(4),(4),(5),(5),(5),(NULL),\
             (11),(12),(13),(14),(15),(16),(16),(16),(16),(17),(17)",
            ["1", "2", "3", "4", "5", "11", "13", "14", "15", "16", "17"],
        ),
        (
            "tdouble",
            "CREATE TABLE tdouble (a INT, c DOUBLE, KEY(c)) PARTITION BY RANGE (a) (\
             PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))",
            "(1,1),(2,2),(3,3),(4,4),(4,4),(5,5),(5,5),(5,5),(NULL,NULL),\
             (11,11),(12,12),(13,13),(14,14),(15,15),(16,16),(16,16),(16,16),(16,16),(17,17),(17,17)",
            ["1", "2", "3", "4", "5", "11", "13", "14", "15", "16", "17"],
        ),
        (
            "tdecimal",
            "CREATE TABLE tdecimal (a INT, c DECIMAL(10,2), KEY(c)) PARTITION BY RANGE (a) (\
             PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))",
            "(1,1),(2,2),(3,3),(4,4),(4,4),(5,5),(5,5),(5,5),(NULL,NULL),\
             (11,11),(12,12),(13,13),(14,14),(15,15),(16,16),(16,16),(16,16),(16,16),(17,17),(17,17)",
            ["1.00", "2.00", "3.00", "4.00", "5.00", "11.00", "13.00", "14.00", "15.00", "16.00", "17.00"],
        ),
        (
            "tdatetime",
            "CREATE TABLE tdatetime (a INT, c DATETIME, KEY(c)) PARTITION BY RANGE (a) (\
             PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))",
            "(1,'2000-01-01'),(2,'2000-01-02'),(3,'2000-01-03'),\
             (4,'2000-01-04'),(4,'2000-01-04'),(5,'2000-01-05'),\
             (5,'2000-01-05'),(5,'2000-01-05'),(NULL,NULL),\
             (11,'2000-01-11'),(12,'2000-01-12'),(13,'2000-01-13'),\
             (14,'2000-01-14'),(15,'2000-01-15'),(16,'2000-01-16'),\
             (16,'2000-01-16'),(16,'2000-01-16'),(16,'2000-01-16'),\
             (17,'2000-01-17'),(17,'2000-01-17')",
            [
                "2000-01-01 00:00:00", "2000-01-02 00:00:00", "2000-01-03 00:00:00",
                "2000-01-04 00:00:00", "2000-01-05 00:00:00", "2000-01-11 00:00:00",
                "2000-01-13 00:00:00", "2000-01-14 00:00:00", "2000-01-15 00:00:00",
                "2000-01-16 00:00:00", "2000-01-17 00:00:00",
            ],
        ),
        (
            "tstring",
            "CREATE TABLE tstring (a INT, c VARCHAR(32), KEY(c)) PARTITION BY RANGE (a) (\
             PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))",
            "(1,'a1'),(2,'a2'),(3,'a3'),(4,'a4'),(4,'a4'),(5,'a5'),(5,'a5'),(5,'a5'),(NULL,NULL),\
             (11,'b11'),(12,'b12'),(13,'b13'),(14,'b14'),(15,'b15'),\
             (16,'b16'),(16,'b16'),(16,'b16'),(16,'b16'),(17,'b17'),(17,'b17')",
            ["a1", "a2", "a3", "a4", "a5", "b11", "b13", "b14", "b15", "b16", "b17"],
        ),
    ];

    for (
        table,
        create,
        values,
        [one, two, three, four, five, eleven, thirteen, fourteen, fifteen, sixteen, seventeen],
    ) in cases
    {
        rows(&mut session, create);
        rows(
            &mut session,
            &format!("INSERT INTO {table} VALUES {values}"),
        );
        rows(
            &mut session,
            &format!("ANALYZE TABLE {table} WITH 2 TOPN, 2 BUCKETS"),
        );

        let meta = displayed(rows(
            &mut session,
            &format!("SHOW STATS_META WHERE table_name = '{table}'"),
        ));
        assert_eq!(
            meta.iter().map(|row| row[5].as_str()).collect::<Vec<_>>(),
            ["20", "9", "11"]
        );

        for is_index in [0, 1] {
            let topn = displayed(rows(
                &mut session,
                &format!("SHOW STATS_TOPN WHERE table_name = '{table}' AND column_name = 'c' AND is_index = {is_index}"),
            )).into_iter().map(|row| row.join(" ")).collect::<Vec<_>>();
            assert_eq!(
                topn,
                [
                    format!("test {table} global c {is_index} {five} 3"),
                    format!("test {table} global c {is_index} {sixteen} 4"),
                    format!("test {table} p0 c {is_index} {four} 2"),
                    format!("test {table} p0 c {is_index} {five} 3"),
                    format!("test {table} p1 c {is_index} {sixteen} 4"),
                    format!("test {table} p1 c {is_index} {seventeen} 2"),
                ]
            );

            let buckets = displayed(rows(
                &mut session,
                &format!("SHOW STATS_BUCKETS WHERE table_name = '{table}' AND column_name = 'c' AND is_index = {is_index}"),
            )).into_iter().map(|row| row.join(" ")).collect::<Vec<_>>();
            assert_eq!(
                buckets,
                [
                    format!("test {table} global c {is_index} 0 5 2 {one} {four} 0"),
                    format!("test {table} global c {is_index} 1 12 2 {eleven} {seventeen} 0"),
                    format!("test {table} p0 c {is_index} 0 2 1 {one} {two} 0"),
                    format!("test {table} p0 c {is_index} 1 3 1 {three} {three} 0"),
                    format!("test {table} p1 c {is_index} 0 3 1 {eleven} {thirteen} 0"),
                    format!("test {table} p1 c {is_index} 1 5 1 {fourteen} {fifteen} 0"),
                ]
            );

            let histograms = displayed(rows(
                &mut session,
                &format!("SHOW STATS_HISTOGRAMS WHERE table_name = '{table}' AND column_name = 'c' AND is_index = {is_index}"),
            ));
            assert_eq!(
                histograms
                    .iter()
                    .map(|row| row[6].as_str())
                    .collect::<Vec<_>>(),
                ["12", "5", "7"]
            );
            assert_eq!(
                histograms
                    .iter()
                    .map(|row| row[7].as_str())
                    .collect::<Vec<_>>(),
                ["1", "1", "0"]
            );
        }
    }
}

/// Pinned `globalstats.TestGlobalIndexStatistics`: all three ANALYZE forms
/// populate a partitioned table's global unique-index statistics and the
/// optimizer uses the global index across every partition.
#[test]
fn global_index_statistics_match_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
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

#[test]
fn global_statistics_and_session_bindings_match_go() {
    assert_global_statistics_and_session_bindings_match_go(1, 111);
}

#[test]
fn concurrent_global_statistics_and_session_bindings_match_go() {
    assert_global_statistics_and_session_bindings_match_go(2, 112);
}

/// Pinned `globalstats.TestGlobalStatsAndSQLBinding` and its concurrency-two
/// twin: global statistics choose the index for hash, range, and list
/// partitions, while a matching session binding's `IGNORE INDEX` moves the
/// same statements to table scans until the binding is dropped.
fn assert_global_statistics_and_session_bindings_match_go(
    merge_concurrency: u64,
    connection_id: u64,
) {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(connection_id))
        .expect("session opens");
    rows(
        &mut session,
        &format!("SET GLOBAL tidb_merge_partition_stats_concurrency = {merge_concurrency}"),
    );
    rows(&mut session, "CREATE DATABASE test_global_stats");
    rows(&mut session, "USE test_global_stats");
    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'dynamic'",
    );
    rows(&mut session, "SET GLOBAL tidb_enable_auto_analyze = OFF");
    rows(
        &mut session,
        "SET SESSION tidb_enable_non_prepared_plan_cache = 0",
    );
    rows(
        &mut session,
        "CREATE TABLE thash(a INT, b INT, KEY(a)) PARTITION BY HASH(a) PARTITIONS 4",
    );
    rows(
        &mut session,
        "CREATE TABLE trange(a INT, b INT, KEY(a)) PARTITION BY RANGE(a) (\
         PARTITION p0 VALUES LESS THAN (200), PARTITION p1 VALUES LESS THAN (400), \
         PARTITION p2 VALUES LESS THAN (600), PARTITION p3 VALUES LESS THAN (800), \
         PARTITION p4 VALUES LESS THAN (1001))",
    );
    rows(
        &mut session,
        "CREATE TABLE tlist(a INT, b INT, KEY(a)) PARTITION BY LIST(a) (\
         PARTITION p0 VALUES IN (0,1,2,3,4,5,6,7,8,9), \
         PARTITION p1 VALUES IN (10,11,12,13,14,15,16,17,18,19), \
         PARTITION p2 VALUES IN (20,21,22,23,24,25,26,27,28,29), \
         PARTITION p3 VALUES IN (30,31,32,33,34,35,36,37,38,39), \
         PARTITION p4 VALUES IN (40,41,42,43,44,45,46,47,48,49,50))",
    );

    let mut range_values = Vec::with_capacity(1000);
    let mut list_values = Vec::with_capacity(1000);
    for i in 0..1000 {
        if i < 10 {
            range_values.push(format!("({i},{i})"));
            list_values.push("(0,0)".to_owned());
        } else {
            range_values.push(format!(
                "({},{})",
                100 + (i * 37) % 900,
                100 + (i * 53) % 900
            ));
            list_values.push(format!("({},{})", 1 + (i * 17) % 50, 1 + (i * 29) % 50));
        }
    }
    let range_values = range_values.join(",");
    rows(
        &mut session,
        &format!("INSERT INTO thash VALUES {range_values}"),
    );
    rows(
        &mut session,
        &format!("INSERT INTO trange VALUES {range_values}"),
    );
    rows(
        &mut session,
        &format!("INSERT INTO tlist VALUES {}", list_values.join(",")),
    );
    for table in ["thash", "trange", "tlist"] {
        rows(&mut session, &format!("ANALYZE TABLE {table}"));
    }
    rows(
        &mut session,
        "SET SESSION tidb_opt_table_full_scan_cost_factor = 100",
    );

    let queries = [
        ("thash", "a < 100"),
        ("trange", "a < 100"),
        ("tlist", "a < 1"),
    ];
    for (table, predicate) in queries {
        let plan = displayed(rows(
            &mut session,
            &format!("EXPLAIN SELECT * FROM {table} WHERE {predicate}"),
        ));
        assert!(
            plan.iter().any(|row| row[0].contains("IndexRangeScan")),
            "{table} must use its index before binding: {plan:?}"
        );
    }

    for table in ["thash", "trange", "tlist"] {
        rows(
            &mut session,
            &format!(
                "CREATE SESSION BINDING FOR SELECT * FROM {table} WHERE a < 100 \
                 USING SELECT * FROM {table} IGNORE INDEX(a) WHERE a < 100"
            ),
        );
    }
    for (table, predicate) in queries {
        let plan = displayed(rows(
            &mut session,
            &format!("EXPLAIN SELECT * FROM {table} WHERE {predicate}"),
        ));
        assert!(
            plan.iter().any(|row| row[0].contains("TableFullScan")),
            "{table} must honor the binding's ignored index: {plan:?}"
        );
    }

    for table in ["thash", "trange", "tlist"] {
        rows(
            &mut session,
            &format!("DROP SESSION BINDING FOR SELECT * FROM {table} WHERE a < 100"),
        );
    }
    for (table, predicate) in queries {
        let plan = displayed(rows(
            &mut session,
            &format!("EXPLAIN SELECT * FROM {table} WHERE {predicate}"),
        ));
        assert!(
            plan.iter().any(|row| row[0].contains("IndexRangeScan")),
            "{table} must return to its index after dropping the binding: {plan:?}"
        );
    }
    rows(&mut session, "SET GLOBAL tidb_enable_auto_analyze = ON");
    rows(
        &mut session,
        "SET SESSION tidb_opt_table_full_scan_cost_factor = 1",
    );
}

/// Pinned `globalstats.TestGlobalStatsData`: partition and global histograms
/// retain Go's exact cumulative bucket counts, repeats, bounds, and zeroed
/// merged bucket NDV for both the column and its index.
#[test]
fn partition_global_stats_bucket_data_matches_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(58))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");
    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'dynamic'",
    );
    rows(
        &mut session,
        "CREATE TABLE global_data (a int, KEY(a)) PARTITION BY RANGE (a) \
         (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))",
    );
    rows(
        &mut session,
        "INSERT INTO global_data VALUES \
         (1),(2),(3),(4),(5),(6),(6),(NULL),\
         (11),(12),(13),(14),(15),(16),(17),(18),(19),(19)",
    );
    rows(
        &mut session,
        "ANALYZE TABLE global_data WITH 0 TOPN, 2 BUCKETS",
    );

    let bucket_rows = |session: &mut _, is_index| {
        displayed(rows(
            session,
            &format!(
                "SHOW STATS_BUCKETS WHERE table_name = 'global_data' AND is_index = {is_index}"
            ),
        ))
        .into_iter()
        .map(|row| row.join(" "))
        .collect::<Vec<_>>()
    };
    assert_eq!(
        bucket_rows(&mut session, 0),
        [
            "test global_data global a 0 0 7 2 1 6 0",
            "test global_data global a 0 1 17 2 11 19 0",
            "test global_data p0 a 0 0 4 1 1 4 0",
            "test global_data p0 a 0 1 7 2 5 6 0",
            "test global_data p1 a 0 0 6 1 11 16 0",
            "test global_data p1 a 0 1 10 2 17 19 0",
        ]
    );
    assert_eq!(
        bucket_rows(&mut session, 1),
        [
            "test global_data global a 1 0 7 2 1 6 0",
            "test global_data global a 1 1 17 2 11 19 0",
            "test global_data p0 a 1 0 4 1 1 4 0",
            "test global_data p0 a 1 1 7 2 5 6 0",
            "test global_data p1 a 1 0 6 1 11 16 0",
            "test global_data p1 a 1 1 10 2 17 19 0",
        ]
    );
}

/// Pinned `globalstats.TestGlobalStatsNDV`: the global index NDV is the FM
/// sketch union across every physical partition, including empty partitions,
/// and repeated values do not increase it.
#[test]
fn partition_global_stats_ndv_matches_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(60))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");
    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'dynamic'",
    );
    rows(
        &mut session,
        "CREATE TABLE global_ndv (a int, KEY(a)) PARTITION BY RANGE (a) (\
         PARTITION p0 VALUES LESS THAN (10),\
         PARTITION p1 VALUES LESS THAN (20),\
         PARTITION p2 VALUES LESS THAN (30),\
         PARTITION p3 VALUES LESS THAN (40))",
    );

    let check_ndv = |session: &mut _, expected: &[&str]| {
        rows(session, "ANALYZE TABLE global_ndv");
        let actual = displayed(rows(
            session,
            "SHOW STATS_HISTOGRAMS WHERE table_name = 'global_ndv'",
        ))
        .into_iter()
        .filter(|row| row[4] == "1")
        .collect::<Vec<_>>();
        assert_eq!(actual.len(), expected.len());
        assert_eq!(
            actual.iter().map(|row| row[6].as_str()).collect::<Vec<_>>(),
            expected
        );
    };

    check_ndv(&mut session, &["0", "0", "0", "0", "0"]);
    rows(&mut session, "INSERT INTO global_ndv VALUES (1),(2),(3)");
    check_ndv(&mut session, &["3", "3", "0", "0", "0"]);
    rows(
        &mut session,
        "INSERT INTO global_ndv VALUES (11),(12),(13),(21),(22),(23)",
    );
    check_ndv(&mut session, &["9", "3", "3", "3", "0"]);
    rows(
        &mut session,
        "INSERT INTO global_ndv VALUES (31),(32),(33),(34)",
    );
    check_ndv(&mut session, &["13", "3", "3", "3", "4"]);
    rows(
        &mut session,
        "INSERT INTO global_ndv VALUES (31),(33),(34),(1),(2),(3)",
    );
    check_ndv(&mut session, &["13", "3", "3", "3", "4"]);
}

/// Pinned `globalstats.TestGlobalStatsIndexNDV`: FM-sketch union uses the
/// index-key encoding consistently for every datum family accepted by the
/// source test, so equal values in different partitions remain one NDV.
#[test]
fn partition_global_index_ndv_matches_go_for_all_source_types() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(61))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");
    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'dynamic'",
    );

    let check_ndv = |session: &mut _, table: &str, expected: &[&str]| {
        rows(session, &format!("ANALYZE TABLE {table}"));
        let actual = displayed(rows(
            session,
            &format!("SHOW STATS_HISTOGRAMS WHERE is_index = 1 AND table_name = '{table}'"),
        ));
        assert_eq!(actual.len(), expected.len());
        assert_eq!(
            actual.iter().map(|row| row[6].as_str()).collect::<Vec<_>>(),
            expected
        );
    };

    let run_case = |session: &mut _, table: &str, sql_type: &str, values: [&str; 8]| {
        rows(
            session,
            &format!(
                "CREATE TABLE {table} (a INT, b {sql_type}, KEY(b)) \
                 PARTITION BY RANGE (a) (\
                 PARTITION p0 VALUES LESS THAN (10),\
                 PARTITION p1 VALUES LESS THAN (20))"
            ),
        );
        rows(
            session,
            &format!(
                "INSERT INTO {table} VALUES (1,{}),(1,{}),(1,{})",
                values[0], values[1], values[2]
            ),
        );
        check_ndv(session, table, &["3", "3", "0"]);
        rows(
            session,
            &format!(
                "INSERT INTO {table} VALUES (11,{}),(11,{}),(11,{})",
                values[0], values[1], values[2]
            ),
        );
        check_ndv(session, table, &["3", "3", "3"]);
        rows(
            session,
            &format!(
                "INSERT INTO {table} VALUES (11,{}),(11,{}),(11,{})",
                values[3], values[4], values[5]
            ),
        );
        check_ndv(session, table, &["6", "3", "6"]);
        rows(
            session,
            &format!(
                "INSERT INTO {table} VALUES (1,{}),(1,{}),(1,{}),(1,{}),(1,{})",
                values[3], values[4], values[5], values[6], values[7]
            ),
        );
        check_ndv(session, table, &["8", "8", "6"]);
    };

    run_case(
        &mut session,
        "global_index_ndv_int",
        "INT",
        ["1", "2", "3", "4", "5", "6", "7", "8"],
    );
    run_case(
        &mut session,
        "global_index_ndv_double",
        "DOUBLE",
        ["1.1", "2.2", "3.3", "4.4", "5.5", "6.6", "7.7", "8.8"],
    );
    run_case(
        &mut session,
        "global_index_ndv_decimal",
        "DECIMAL(30,15)",
        ["1.1", "2.2", "3.3", "4.4", "5.5", "6.6", "7.7", "8.8"],
    );
    run_case(
        &mut session,
        "global_index_ndv_string",
        "VARCHAR(30)",
        [
            "'111'", "'222'", "'333'", "'444'", "'555'", "'666'", "'777'", "'888'",
        ],
    );
    run_case(
        &mut session,
        "global_index_ndv_datetime",
        "DATETIME",
        [
            "'2001-01-01'",
            "'2002-01-01'",
            "'2003-01-01'",
            "'2004-01-01'",
            "'2005-01-01'",
            "'2006-01-01'",
            "'2007-01-01'",
            "'2008-01-01'",
        ],
    );
}

/// Pinned `globalstats.TestGlobalStatsVersion`: after global stats exist,
/// analyzing a newly added partition refreshes the global row while retaining
/// modifications in another partition until that partition is analyzed.
#[test]
fn partition_scoped_analyze_refreshes_global_count_and_modify_count() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(62))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");
    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'dynamic'",
    );
    rows(
        &mut session,
        "CREATE TABLE global_stats_version (a INT) PARTITION BY RANGE (a) (\
         PARTITION p0 VALUES LESS THAN (10),\
         PARTITION p1 VALUES LESS THAN (20))",
    );
    rows(
        &mut session,
        "INSERT INTO global_stats_version VALUES (1),(5),(NULL),(11),(15)",
    );
    rows(&mut session, "FLUSH STATS_DELTA *.*");
    rows(&mut session, "ANALYZE TABLE global_stats_version");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SHOW STATS_META WHERE table_name = 'global_stats_version'",
        ))
        .len(),
        3
    );

    let global_meta = |session: &mut _| {
        displayed(rows(
            session,
            "SHOW STATS_META WHERE table_name = 'global_stats_version' \
             AND partition_name = 'global'",
        ))
        .into_iter()
        .next()
        .expect("global stats meta row")
    };

    rows(
        &mut session,
        "ALTER TABLE global_stats_version ADD PARTITION \
         (PARTITION p2 VALUES LESS THAN (30))",
    );
    rows(
        &mut session,
        "INSERT INTO global_stats_version VALUES (13),(14),(22),(23)",
    );
    rows(&mut session, "FLUSH STATS_DELTA *.*");
    rows(
        &mut session,
        "ANALYZE TABLE global_stats_version PARTITION p2",
    );
    rows(&mut session, "FLUSH STATS_DELTA *.*");
    let meta = global_meta(&mut session);
    assert_eq!((meta[4].as_str(), meta[5].as_str()), ("2", "9"));

    rows(
        &mut session,
        "ANALYZE TABLE global_stats_version PARTITION p1",
    );
    let meta = global_meta(&mut session);
    assert_eq!((meta[4].as_str(), meta[5].as_str()), ("0", "9"));

    rows(
        &mut session,
        "ALTER TABLE global_stats_version DROP PARTITION p2",
    );
    assert_eq!(global_meta(&mut session)[5], "7");
    rows(&mut session, "FLUSH STATS_DELTA *.*");
    rows(&mut session, "ANALYZE TABLE global_stats_version");
    let meta = global_meta(&mut session);
    assert_eq!(meta[5], "7");
}

#[test]
fn flush_stats_delta_missing_targets_match_go_warnings() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(63))
        .expect("session opens");
    rows(&mut session, "USE test");

    rows(
        &mut session,
        "FLUSH STATS_DELTA missing_database.*, test.missing_table",
    );
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        vec![
            vec![
                "Warning".to_owned(),
                "1049".to_owned(),
                "Unknown database 'missing_database'".to_owned(),
            ],
            vec![
                "Warning".to_owned(),
                "1146".to_owned(),
                "Table 'test.missing_table' doesn't exist".to_owned(),
            ],
        ]
    );
}

/// Pinned `globalstats.TestDDLPartition4GlobalStats`: truncating partitions
/// replaces their physical IDs, subtracts their persisted counts from the
/// logical global row, and installs replacement partition metadata.
#[test]
fn truncate_partitions_refreshes_global_stats_meta_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(64))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");
    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'dynamic'",
    );
    rows(
        &mut session,
        "CREATE TABLE global_stats_truncate (a INT) PARTITION BY RANGE (a) (\
         PARTITION p0 VALUES LESS THAN (10),\
         PARTITION p1 VALUES LESS THAN (20),\
         PARTITION p2 VALUES LESS THAN (30),\
         PARTITION p3 VALUES LESS THAN (40),\
         PARTITION p4 VALUES LESS THAN (50),\
         PARTITION p5 VALUES LESS THAN (60))",
    );
    rows(
        &mut session,
        "INSERT INTO global_stats_truncate VALUES \
         (1),(2),(3),(4),(5),(11),(21),(31),(41),(51),\
         (12),(22),(32),(42),(52)",
    );
    rows(&mut session, "FLUSH STATS_DELTA *.*");
    rows(&mut session, "ANALYZE TABLE global_stats_truncate");
    let global_count = |session: &mut _| {
        displayed(rows(
            session,
            "SHOW STATS_META WHERE table_name = 'global_stats_truncate' \
             AND partition_name = 'global'",
        ))[0][5]
            .clone()
    };
    assert_eq!(
        displayed(rows(
            &mut session,
            "SHOW STATS_META WHERE table_name = 'global_stats_truncate'",
        ))
        .len(),
        7
    );
    assert_eq!(global_count(&mut session), "15");

    rows(
        &mut session,
        "ALTER TABLE global_stats_truncate TRUNCATE PARTITION p2, p4",
    );
    rows(&mut session, "FLUSH STATS_DELTA *.*");
    assert_eq!(global_count(&mut session), "11");

    rows(&mut session, "ANALYZE TABLE global_stats_truncate");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SHOW STATS_META WHERE table_name = 'global_stats_truncate'",
        ))
        .len(),
        7
    );
    assert_eq!(global_count(&mut session), "11");
}

/// Pinned `globalstats.TestGlobalStats`: dynamic partition pruning consumes
/// logical-table statistics, static pruning consumes each physical
/// partition's statistics under `PartitionUnion`, and switching to dynamic
/// before a global row exists uses pseudo statistics until the next analyze.
#[test]
fn global_stats_drive_partition_plans_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(crate::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(65))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");

    let plan_of = |session: &mut _, sql: &str| {
        displayed(rows(session, sql))
            .into_iter()
            .map(|row| row.join(" "))
            .collect::<Vec<_>>()
            .join("\n")
    };

    rows(
        &mut session,
        "CREATE TABLE global_stats_plan (a INT, KEY(a)) PARTITION BY RANGE (a) (\
         PARTITION p0 VALUES LESS THAN (10),\
         PARTITION p1 VALUES LESS THAN (20),\
         PARTITION p2 VALUES LESS THAN (30))",
    );
    rows(
        &mut session,
        "INSERT INTO global_stats_plan VALUES (1),(5),(NULL),(11),(15),(21),(25)",
    );
    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'dynamic'",
    );
    rows(&mut session, "ANALYZE TABLE global_stats_plan");

    let dynamic = plan_of(
        &mut session,
        "EXPLAIN FORMAT = 'brief' SELECT a FROM global_stats_plan WHERE a > 5",
    );
    assert!(
        dynamic.contains("IndexRangeScan") && dynamic.contains("4.00"),
        "{dynamic}"
    );
    assert!(dynamic.contains("partition:all"), "{dynamic}");
    assert!(
        !dynamic.contains("PartitionUnion") && !dynamic.contains("stats:pseudo"),
        "{dynamic}"
    );

    let explicit = plan_of(
        &mut session,
        "EXPLAIN FORMAT = 'brief' SELECT * FROM global_stats_plan PARTITION(p1) WHERE a > 15",
    );
    assert!(
        explicit.contains("IndexRangeScan") && explicit.contains("2.00"),
        "{explicit}"
    );
    assert!(explicit.contains("partition:p1"), "{explicit}");
    assert!(!explicit.contains("stats:pseudo"), "{explicit}");

    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'static'",
    );
    let static_plan = plan_of(
        &mut session,
        "EXPLAIN FORMAT = 'brief' SELECT a FROM global_stats_plan WHERE a > 5",
    );
    assert!(
        static_plan.contains("PartitionUnion") && static_plan.contains("5.00"),
        "{static_plan}"
    );
    for partition in ["p0", "p1", "p2"] {
        assert!(
            static_plan.contains(&format!("partition:{partition}")),
            "{static_plan}"
        );
    }

    rows(&mut session, "DROP TABLE global_stats_plan");
    rows(
        &mut session,
        "CREATE TABLE global_stats_plan (a INT, b INT, KEY(a)) \
         PARTITION BY HASH(a) PARTITIONS 2",
    );
    rows(
        &mut session,
        "INSERT INTO global_stats_plan VALUES (1,1),(3,3),(4,4),(2,2),(5,5)",
    );
    rows(&mut session, "ANALYZE TABLE global_stats_plan");
    let static_meta = displayed(rows(
        &mut session,
        "SHOW STATS_META WHERE table_name = 'global_stats_plan'",
    ));
    assert_eq!(static_meta.len(), 2);
    assert_eq!(
        static_meta
            .iter()
            .map(|row| row[5].as_str())
            .collect::<Vec<_>>(),
        ["2", "3"]
    );

    rows(
        &mut session,
        "SET SESSION tidb_partition_prune_mode = 'dynamic'",
    );
    let pseudo = plan_of(
        &mut session,
        "EXPLAIN FORMAT = 'brief' SELECT a FROM global_stats_plan WHERE a > 3",
    );
    assert!(
        pseudo.contains("IndexRangeScan") && pseudo.contains("1.67"),
        "{pseudo}"
    );
    assert!(
        pseudo.contains("partition:all") && pseudo.contains("stats:pseudo"),
        "{pseudo}"
    );
    assert!(!pseudo.contains("PartitionUnion"), "{pseudo}");

    rows(&mut session, "ANALYZE TABLE global_stats_plan");
    let dynamic_meta = displayed(rows(
        &mut session,
        "SHOW STATS_META WHERE table_name = 'global_stats_plan'",
    ));
    assert_eq!(dynamic_meta.len(), 3);
    assert_eq!(
        dynamic_meta
            .iter()
            .map(|row| row[5].as_str())
            .collect::<Vec<_>>(),
        ["5", "2", "3"]
    );
    let analyzed = plan_of(
        &mut session,
        "EXPLAIN FORMAT = 'brief' SELECT a FROM global_stats_plan WHERE a > 3",
    );
    assert!(
        analyzed.contains("IndexRangeScan") && analyzed.contains("2.00"),
        "{analyzed}"
    );
    assert!(analyzed.contains("partition:all"), "{analyzed}");
    assert!(
        !analyzed.contains("PartitionUnion") && !analyzed.contains("stats:pseudo"),
        "{analyzed}"
    );

    rows(&mut session, "DROP TABLE global_stats_plan");
    rows(
        &mut session,
        "CREATE TABLE global_stats_plan (a INT, b INT, c INT) \
         PARTITION BY HASH(a) PARTITIONS 2",
    );
    rows(
        &mut session,
        "CREATE INDEX idx_ab ON global_stats_plan(a, b)",
    );
    rows(
        &mut session,
        "INSERT INTO global_stats_plan VALUES \
         (1,1,1),(5,5,5),(11,11,11),(15,15,15),(21,21,21),(25,25,25)",
    );
    rows(&mut session, "ANALYZE TABLE global_stats_plan");

    let index_scan = plan_of(
        &mut session,
        "EXPLAIN FORMAT = 'brief' SELECT b FROM global_stats_plan WHERE a > 5 AND b > 10",
    );
    assert!(
        index_scan.contains("IndexRangeScan") && index_scan.contains("2.67"),
        "{index_scan}"
    );
    assert!(
        index_scan.contains("partition:all") && !index_scan.contains("stats:pseudo"),
        "{index_scan}"
    );

    let index_lookup = plan_of(
        &mut session,
        "EXPLAIN FORMAT = 'brief' SELECT * FROM global_stats_plan USE INDEX(idx_ab) WHERE a > 1",
    );
    assert!(
        index_lookup.contains("IndexLookUp") && index_lookup.contains("5.00"),
        "{index_lookup}"
    );
    assert!(
        index_lookup.contains("partition:all") && !index_lookup.contains("stats:pseudo"),
        "{index_lookup}"
    );

    let table_scan = plan_of(
        &mut session,
        "EXPLAIN FORMAT = 'brief' SELECT * FROM global_stats_plan",
    );
    assert!(
        table_scan.contains("TableFullScan") && table_scan.contains("6.00"),
        "{table_scan}"
    );
    assert!(
        table_scan.contains("partition:all") && !table_scan.contains("stats:pseudo"),
        "{table_scan}"
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
        // Divergence 7: the ranges are a superset, so the filter stays above.
        assert!(plan.contains("Selection"), "{plan}");
    }

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
/// This is the path `--store unistore --cluster-session` actually serves, and
/// it had NO coverage: the mock-seam modules in this directory serve
/// `cop_scans: None`, and the pipeline session's own `CREATE VIEW` wire test
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

/// A HASH-partitioned table created HERE writes its rows to the right
/// physical tables and reads every one of them back.
///
/// This is the end-to-end claim the metadata round trip does NOT make. The
/// loader proves the stored bounds fold back; this proves a row written
/// under one partition's physical table id is found again by a read that has
/// to visit all of them. The four ids straddle both partitions under
/// `HASH(id) PARTITIONS 2`, so a read that reached only one physical table
/// would come back with half the rows rather than with an error.
#[test]
fn a_hash_partitioned_table_writes_and_reads_every_partition() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(63))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.hp (id int primary key, v int) PARTITION BY HASH (id) PARTITIONS 2",
    );
    rows(
        &mut session,
        "INSERT INTO test.hp VALUES (1, 10), (2, 20), (3, 30), (4, 40)",
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT id, v FROM test.hp ORDER BY id")),
        [["1", "10"], ["2", "20"], ["3", "30"], ["4", "40"]],
        "every partition's rows come back IN ORDER: the per-partition scans \
         are each ordered, and merging them is what makes the whole answer so"
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT id FROM test.hp ORDER BY id DESC"
        )),
        [["4"], ["3"], ["2"], ["1"]],
        "the descending merge walks both partitions backwards together"
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT count(*) FROM test.hp")),
        [["4"]],
        "the aggregate reaches every physical table too"
    );
}

/// A RANGE-partitioned table prunes on read without losing rows.
///
/// The unpruned read is the control: if pruning dropped a partition it
/// should not have, only the narrowed query would be wrong, and only a
/// comparison against the full scan shows it.
///
/// This does NOT test cross-partition ordering, and cannot: a clustered
/// primary key must cover the partition columns, so a RANGE table keyed on
/// its own primary key stores the partitions in handle order and
/// concatenating them is already sorted. `HASH` is what separates the two,
/// because hashing scatters the handle across partitions.
#[test]
fn a_range_partitioned_table_prunes_without_losing_rows() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(64))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.rp (id int primary key, v int) PARTITION BY RANGE (id) \
         (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (MAXVALUE))",
    );
    rows(
        &mut session,
        "INSERT INTO test.rp VALUES (5, 50), (15, 150), (25, 250)",
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT id FROM test.rp ORDER BY id")),
        [["5"], ["15"], ["25"]],
        "the unpruned read sees both partitions"
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT id FROM test.rp WHERE id < 10")),
        [["5"]],
        "the pruned read keeps the row that is actually below the bound"
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT id FROM test.rp WHERE id >= 10 ORDER BY id"
        )),
        [["15"], ["25"]],
        "and the other side keeps the rows above it"
    );
}

/// A KEY-partitioned table answers an ordered read in order.
///
/// KEY hashes the partition columns exactly as HASH does, so the handles
/// scatter across partitions and the merge is what puts them back together.
/// This is the sibling of
/// [`a_hash_partitioned_table_writes_and_reads_every_partition`] over the
/// other method that stores rows out of handle order.
#[test]
fn a_key_partitioned_table_answers_an_ordered_read_in_order() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(65))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.kp (id int primary key, v int) PARTITION BY KEY (id) PARTITIONS 2",
    );
    rows(
        &mut session,
        "INSERT INTO test.kp VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)",
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT id FROM test.kp ORDER BY id")),
        [["1"], ["2"], ["3"], ["4"], ["5"]],
        "the merge orders across every KEY partition"
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

/// Two explicit transactions racing an `UPDATE` of the same row both commit,
/// and the row carries BOTH increments -- Go's default `tidb_txn_mode =
/// 'pessimistic'` semantics, sysbench `oltp_read_write`'s exact shape.
///
/// The pessimistic wiring is what each half proves. The loser's `UPDATE`
/// blocks on the winner's row lock instead of proceeding on a stale
/// snapshot; when the lock releases, fair locking grants it WITH a conflict
/// and the statement is re-executed reading at the advanced `for_update_ts`
/// (Go `handlePessimisticDML` -> `UpdateForUpdateTS`), so its `v + 1`
/// computes from the winner's committed value. Before the wiring, `BEGIN`
/// held an optimistic transaction end to end: neither `UPDATE` blocked, both
/// computed from the same snapshot, and whichever `COMMIT` ran second failed
/// with 9007 -- this test then fails on the error, and would fail on
/// `v == 2` even if both were let through.
#[test]
fn racing_pessimistic_updates_both_commit_with_serial_effect() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut first = factory
        .open_session(session_context(80))
        .expect("session opens");
    rows(
        &mut first,
        "CREATE TABLE test.race_rw (id int primary key, v int)",
    );
    rows(&mut first, "INSERT INTO test.race_rw VALUES (1, 0)");

    assert_eq!(
        first.control_transaction("BEGIN").expect("begin"),
        Some(true)
    );
    rows(&mut first, "UPDATE test.race_rw SET v = v + 1 WHERE id = 1");

    let second = std::thread::scope(|scope| {
        let contender = scope.spawn(|| {
            let mut second = factory
                .open_session(session_context(81))
                .expect("session opens");
            assert_eq!(
                second.control_transaction("BEGIN").expect("begin"),
                Some(true)
            );
            // Blocks on the first transaction's pessimistic row lock until
            // that transaction commits.
            rows(
                &mut second,
                "UPDATE test.race_rw SET v = v + 1 WHERE id = 1",
            );
            second.control_transaction("COMMIT").expect("commit");
        });
        // Give the contender time to reach the lock wait, so the interesting
        // interleaving -- blocked UPDATE, then the winner's COMMIT -- is the
        // one exercised. The assertions hold under any interleaving.
        std::thread::sleep(std::time::Duration::from_millis(200));
        first.control_transaction("COMMIT").expect("commit");
        contender.join()
    });
    second.expect("the contending transaction commits after waiting the lock out");

    assert_eq!(
        displayed(rows(&mut first, "SELECT v FROM test.race_rw WHERE id = 1")),
        [["2"]],
        "both increments landed: the loser re-read the winner's commit"
    );
}

/// A retried pessimistic statement re-reads through the COPROCESSOR at its
/// advanced `for_update_ts`, not at `start_ts`.
///
/// The retry exists so the statement recomputes from the winner's committed
/// row: Go re-runs it after `UpdateForUpdateTS`, and every read of the
/// re-executed statement -- point get and range scan alike -- uses the new
/// timestamp. This tier moved only the point path: `SessionSnapshot::start_ts`
/// answered the transaction's `start_ts` while ignoring the statement's
/// `read_ts`, and that value is what stamps `request.snapshot_ts` for a
/// pushdown (`tidb-executor/src/cluster_storage.rs`). A range-shaped DML
/// therefore recomputed from the SAME stale row it just lost the lock race
/// on, and its `v + 1` overwrote the winner: a silent lost update, and a
/// mixed-timestamp read inside one statement.
///
/// The predicate is on a NON-key column so the plan is a coprocessor scan
/// rather than a handle range; the sibling test above covers the point-get
/// shape, which already reads at the advanced timestamp.
#[test]
fn a_retried_range_dml_recomputes_at_the_advanced_for_update_ts() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut first = factory
        .open_session(session_context(86))
        .expect("session opens");
    rows(
        &mut first,
        "CREATE TABLE test.retry_range (k int primary key, v int)",
    );
    rows(&mut first, "INSERT INTO test.retry_range VALUES (7, 10)");

    assert_eq!(
        first.control_transaction("BEGIN").expect("begin"),
        Some(true)
    );
    // Take the row lock so the contender below must wait and then retry.
    rows(
        &mut first,
        "UPDATE test.retry_range SET v = v + 1 WHERE v > 0",
    );

    let second = std::thread::scope(|scope| {
        let contender = scope.spawn(|| {
            let mut second = factory
                .open_session(session_context(87))
                .expect("session opens");
            assert_eq!(
                second.control_transaction("BEGIN").expect("begin"),
                Some(true)
            );
            // Blocks on the first transaction's lock, then retries at an
            // advanced `for_update_ts`.
            rows(
                &mut second,
                "UPDATE test.retry_range SET v = v + 1 WHERE v > 0",
            );
            second.control_transaction("COMMIT").expect("commit");
        });
        std::thread::sleep(std::time::Duration::from_millis(200));
        first.control_transaction("COMMIT").expect("commit");
        contender.join()
    });
    second.expect("the contending transaction commits after waiting the lock out");

    assert_eq!(
        displayed(rows(
            &mut first,
            "SELECT v FROM test.retry_range WHERE k = 7"
        )),
        [["12"]],
        "both increments landed: the retry re-read the winner's commit \
         through the coprocessor, not through the stale start_ts snapshot"
    );
}

/// A NON-locking read must not be answered from the pessimistic lock cache.
///
/// Go gates that cache on `e.lock`: `PointGetExecutor.get`
/// (`pkg/executor/point_get.go:671-684`) consults
/// `TxnCtx.GetKeyInPessimisticLockCache` only inside `if e.lock`, so a plain
/// `SELECT` falls through to the snapshot and reads at the transaction's own
/// `start_ts`. The cached row is the one the LOCK saw, at a `for_update_ts`
/// at or after `start_ts`; answering a plain read from it publishes a newer
/// row into a repeatable read.
///
/// The window is exactly the one pessimistic locking is built to tolerate: a
/// writer that commits after `BEGIN`, which the lock survives by advancing
/// its own `for_update_ts` rather than failing.
#[test]
fn a_plain_read_is_not_answered_from_the_pessimistic_lock_cache() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut reader = factory
        .open_session(session_context(84))
        .expect("session opens");
    rows(
        &mut reader,
        "CREATE TABLE test.lock_cache (id int primary key, v int)",
    );
    rows(&mut reader, "INSERT INTO test.lock_cache VALUES (1, 10)");

    assert_eq!(
        reader.control_transaction("BEGIN").expect("begin"),
        Some(true)
    );
    assert_eq!(
        displayed(rows(
            &mut reader,
            "SELECT v FROM test.lock_cache WHERE id = 1"
        )),
        [["10"]],
        "the transaction's snapshot is the row as of BEGIN"
    );

    let mut writer = factory
        .open_session(session_context(85))
        .expect("session opens");
    rows(
        &mut writer,
        "UPDATE test.lock_cache SET v = 99 WHERE id = 1",
    );

    // The LOCKING read may see the newer row -- it takes its own
    // `for_update_ts`, which is Go's behaviour too. This is what fills the
    // lock cache.
    let _ = reader.execute("SELECT v FROM test.lock_cache WHERE id = 1 FOR UPDATE");

    assert_eq!(
        displayed(rows(
            &mut reader,
            "SELECT v FROM test.lock_cache WHERE id = 1"
        )),
        [["10"]],
        "the plain read that follows still reads at start_ts: the lock cache \
         belongs to locking reads only (`point_get.go:677`)"
    );
    reader.control_transaction("ROLLBACK").expect("rollback");
}

/// `BEGIN OPTIMISTIC` keeps Go's optimistic contract: neither `UPDATE`
/// blocks or locks, and the transaction that commits second fails at
/// `COMMIT` with the 9007 write conflict -- which is also the receipt that
/// [`racing_pessimistic_updates_both_commit_with_serial_effect`] tests the
/// WIRING and not some property both modes share: this test IS the
/// pre-wiring behavior, kept reachable by the keyword exactly as Go keeps
/// it.
#[test]
fn racing_optimistic_updates_still_conflict_at_commit() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut first = factory
        .open_session(session_context(84))
        .expect("session opens");
    rows(
        &mut first,
        "CREATE TABLE test.race_opt (id int primary key, v int)",
    );
    rows(&mut first, "INSERT INTO test.race_opt VALUES (1, 0)");

    first
        .control_transaction("BEGIN OPTIMISTIC")
        .expect("begin");
    rows(
        &mut first,
        "UPDATE test.race_opt SET v = v + 1 WHERE id = 1",
    );

    // The contender's whole transaction runs and commits while the first is
    // still open: optimistically nothing blocks it.
    let mut second = factory
        .open_session(session_context(85))
        .expect("session opens");
    second
        .control_transaction("BEGIN OPTIMISTIC")
        .expect("begin");
    rows(
        &mut second,
        "UPDATE test.race_opt SET v = v + 1 WHERE id = 1",
    );
    second.control_transaction("COMMIT").expect("commit");

    // The first transaction's prewrite now finds the newer commit: 9007.
    let refused = first
        .control_transaction("COMMIT")
        .expect_err("an optimistic loser reports the conflict at COMMIT");
    assert_eq!(
        refused.code, 9007,
        "the loser's error keeps Go's write-conflict identity: {}",
        refused.message
    );
    assert_eq!(
        displayed(rows(
            &mut second,
            "SELECT v FROM test.race_opt WHERE id = 1"
        )),
        [["1"]],
        "only the winner's increment landed"
    );
}

/// `BEGIN` inside an open transaction implicitly COMMITS it -- Go's
/// documented `BEGIN` semantics -- and the staged writes are PUBLISHED, not
/// discarded. sysbench relies on this shape: an ignorable statement error
/// (1213) makes it abandon the transaction and simply issue the next
/// `BEGIN`. Before the fix the wrapper discarded the abandoned buffer, and
/// -- when a statistics republish had refreshed the catalog first -- the
/// implicit commit failed with a phantom 9007 "Write conflict" at `BEGIN`.
#[test]
fn begin_inside_a_transaction_implicitly_commits_it() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut session = factory
        .open_session(session_context(88))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.implicit_commit (id int primary key, v int)",
    );
    rows(
        &mut session,
        "INSERT INTO test.implicit_commit VALUES (1, 0)",
    );

    session.control_transaction("BEGIN").expect("begin");
    rows(
        &mut session,
        "UPDATE test.implicit_commit SET v = 7 WHERE id = 1",
    );
    // No COMMIT: the next BEGIN carries it implicitly.
    session
        .control_transaction("BEGIN")
        .expect("BEGIN with an open transaction implicitly commits, never conflicts");
    session.control_transaction("COMMIT").expect("commit");

    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT v FROM test.implicit_commit WHERE id = 1"
        )),
        [["7"]],
        "the abandoned transaction's write was committed, not discarded"
    );
}

/// Go's pessimistic point write folds its row read INTO its lock:
/// `PointGetExecutor.getAndLock` (`pkg/executor/point_get.go:549`) locks with
/// `InitReturnValues(1)` (line 614) and reads the row from the answer, cached
/// in `TxnCtx.SetPessimisticLockCache`. This session's EXECUTE path does the
/// fold: the classified row is locked WITH its value BEFORE the snapshot is
/// bound, so the statement's one read is answered from the lock response. The
/// assertions here are behavioral, not counters: the pre-lock must REALLY
/// hold the row (a contender blocks mid-transaction), the second update of
/// the same row must compute from its own staged write, and COMMIT persists.
#[test]
fn a_prepared_point_update_locks_its_row_before_reading_it() {
    use tidb_protocol::PreparedValue;

    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut first = factory
        .open_session(session_context(90))
        .expect("session opens");
    rows(
        &mut first,
        "CREATE TABLE test.fold (id int primary key, v int)",
    );
    rows(&mut first, "INSERT INTO test.fold VALUES (1, 10)");

    assert_eq!(
        first.control_transaction("BEGIN").expect("begin"),
        Some(true)
    );
    let statement = first
        .prepare_general("UPDATE test.fold SET v = v + 5 WHERE id = ?")
        .expect("prepare");
    let affected = match first
        .execute_general(&statement, &[PreparedValue::SignedLongLong(1)])
        .expect("execute")
    {
        crate::sql_node::GeneralExecuteOutcome::Write(outcome) => outcome.affected_rows,
        crate::sql_node::GeneralExecuteOutcome::Rows(_) => {
            panic!("an UPDATE answers OK, not with a result set")
        }
    };
    assert_eq!(affected, 1);

    // The lock was taken BEFORE the statement read anything, so a contender
    // on the same row must block until this transaction commits -- exactly
    // the interleaving
    // [`racing_pessimistic_updates_both_commit_with_serial_effect`] pins for
    // the post-run lock step, now exercised by the PRE-lock.
    let second = std::thread::scope(|scope| {
        let contender = scope.spawn(|| {
            let mut second = factory
                .open_session(session_context(91))
                .expect("session opens");
            assert_eq!(
                second.control_transaction("BEGIN").expect("begin"),
                Some(true)
            );
            rows(&mut second, "UPDATE test.fold SET v = v + 100 WHERE id = 1");
            second.control_transaction("COMMIT").expect("commit");
        });
        std::thread::sleep(std::time::Duration::from_millis(200));
        first.control_transaction("COMMIT").expect("commit");
        contender.join()
    });
    second.expect("the contending transaction commits after waiting the prelock out");

    let mut after = factory
        .open_session(session_context(92))
        .expect("session opens");
    assert_eq!(
        displayed(rows(&mut after, "SELECT v FROM test.fold WHERE id = 1")),
        [["115"]],
        "+5 landed from the lock-carrying read, then +100 from the winner that re-read it"
    );
}

/// The same fold over the TEXT protocol: a client-side prepared driver
/// (Connector/J without `useServerPrepStmts`) sends the point `UPDATE` as
/// plain COM_QUERY, so the write-path classification must fire there too.
/// Behavioral assertion as above -- the row is locked BEFORE the statement
/// reads it, so a contender blocks mid-transaction.
#[test]
fn a_text_point_update_locks_its_row_before_reading_it() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut first = factory
        .open_session(session_context(96))
        .expect("session opens");
    rows(
        &mut first,
        "CREATE TABLE test.foldtxt (id int primary key, v int)",
    );
    rows(&mut first, "INSERT INTO test.foldtxt VALUES (1, 10)");

    assert_eq!(
        first.control_transaction("BEGIN").expect("begin"),
        Some(true)
    );
    let affected = {
        let out = rows(&mut first, "UPDATE test.foldtxt SET v = v + 5 WHERE id = 1");
        out.len()
    };
    let _ = affected;

    // A contender on the same row must block until this transaction commits:
    // the lock predates the statement's own read.
    let second = std::thread::scope(|scope| {
        let contender = scope.spawn(|| {
            let mut second = factory
                .open_session(session_context(97))
                .expect("session opens");
            assert_eq!(
                second.control_transaction("BEGIN").expect("begin"),
                Some(true)
            );
            rows(
                &mut second,
                "UPDATE test.foldtxt SET v = v + 100 WHERE id = 1",
            );
            second.control_transaction("COMMIT").expect("commit");
        });
        std::thread::sleep(std::time::Duration::from_millis(200));
        first.control_transaction("COMMIT").expect("commit");
        contender.join()
    });
    second.expect("the contending transaction commits after waiting the prelock out");

    let mut after = factory
        .open_session(session_context(98))
        .expect("session opens");
    assert_eq!(
        displayed(rows(&mut after, "SELECT v FROM test.foldtxt WHERE id = 1")),
        [["115"]],
        "+5 landed from the lock-carrying read, then +100 from the winner that re-read it"
    );
}

/// Go's `SELECT ... FOR UPDATE` on one clustered handle-pinned row folds its
/// read INTO its lock (`TryFastPlan` -> `PointGetPlan(Lock=true)` ->
/// `getAndLock`). The text path classifies the same shape: the locking read's
/// row answers from the PessimisticLock response, and a contender on the row
/// blocks for the transaction's lifetime. The read-your-own-lock image is
/// also what a later statement in the same transaction must compute from.
#[test]
fn a_text_select_for_update_folds_its_read_into_its_lock() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut first = factory
        .open_session(session_context(99))
        .expect("session opens");
    rows(
        &mut first,
        "CREATE TABLE test.foldsel (id int primary key, v int)",
    );
    rows(&mut first, "INSERT INTO test.foldsel VALUES (1, 10)");

    assert_eq!(
        first.control_transaction("BEGIN").expect("begin"),
        Some(true)
    );
    assert_eq!(
        displayed(rows(
            &mut first,
            "SELECT v FROM test.foldsel WHERE id = 1 FOR UPDATE"
        )),
        [["10"]],
        "the locking read answers its row"
    );

    // The row is already locked by the statement itself: a contender's update
    // waits for this transaction to end.
    let second = std::thread::scope(|scope| {
        let contender = scope.spawn(|| {
            let mut second = factory
                .open_session(session_context(100))
                .expect("session opens");
            assert_eq!(
                second.control_transaction("BEGIN").expect("begin"),
                Some(true)
            );
            rows(
                &mut second,
                "UPDATE test.foldsel SET v = v + 100 WHERE id = 1",
            );
            second.control_transaction("COMMIT").expect("commit");
        });
        std::thread::sleep(std::time::Duration::from_millis(200));
        rows(&mut first, "UPDATE test.foldsel SET v = v + 5 WHERE id = 1");
        first.control_transaction("COMMIT").expect("commit");
        contender.join()
    });
    second.expect("the contending transaction commits after the folding reader commits");

    let mut after = factory
        .open_session(session_context(101))
        .expect("session opens");
    assert_eq!(
        displayed(rows(&mut after, "SELECT v FROM test.foldsel WHERE id = 1")),
        [["115"]],
        "+5 from the folded reader, then +100 from the winner that re-read it"
    );
}

/// The prelock joins the failed-statement release list: an EXECUTE that fails
/// AFTER its row was locked (a strict-mode cast error during the assignment)
/// must give the lock back AND drop its cached row image, so a contender can
/// take the row immediately (`OnPessimisticStmtEnd(isSuccessful=false)`).
#[test]
fn a_failed_prelocked_update_releases_its_row() {
    use tidb_protocol::PreparedValue;

    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut first = factory
        .open_session(session_context(93))
        .expect("session opens");
    rows(
        &mut first,
        "CREATE TABLE test.foldfail (id int primary key, v int)",
    );
    rows(&mut first, "INSERT INTO test.foldfail VALUES (1, 10)");

    assert_eq!(
        first.control_transaction("BEGIN").expect("begin"),
        Some(true)
    );
    let statement = first
        .prepare_general("UPDATE test.foldfail SET v = 'not-a-number' WHERE id = ?")
        .expect("prepare");
    // The execute outcome borrows the session for its row source, so the
    // error check happens inside a scope that ends before the rollback.
    let failed = {
        let outcome = first.execute_general(&statement, &[PreparedValue::SignedLongLong(1)]);
        outcome.is_err()
    };
    if !failed {
        // A non-strict node coerces instead of failing; then there is no
        // post-lock failure to exercise and the release behavior above is
        // covered by the transaction-end path instead.
        first.control_transaction("ROLLBACK").expect("rollback");
        return;
    }
    // The statement failed after its prelock; the transaction stays open.
    assert_eq!(
        first.control_transaction("ROLLBACK").expect("rollback"),
        Some(false)
    );

    // A contender takes the released row immediately: had the prelock leaked,
    // this would block for the lock-wait timeout instead of completing.
    let mut second = factory
        .open_session(session_context(94))
        .expect("session opens");
    assert_eq!(
        second.control_transaction("BEGIN").expect("begin"),
        Some(true)
    );
    rows(&mut second, "UPDATE test.foldfail SET v = 99 WHERE id = 1");
    second.control_transaction("COMMIT").expect("commit");

    let mut after = factory
        .open_session(session_context(95))
        .expect("session opens");
    assert_eq!(
        displayed(rows(&mut after, "SELECT v FROM test.foldfail WHERE id = 1")),
        [["99"]],
        "the failed statement left neither its lock nor its staged write behind"
    );
}
