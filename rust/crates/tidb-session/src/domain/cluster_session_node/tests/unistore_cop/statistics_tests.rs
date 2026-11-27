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

#[test]
fn stats_ddl_wait_does_not_require_an_unready_other_subscriber() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(125))
        .expect("session");
    rows(&mut session, "CREATE TABLE test.stats_wait (a INT)");
    let table_id = stack
        .factory
        .catalog
        .get_latest()
        .table_info_by_name("test", "stats_wait")
        .unwrap()
        .1
        .read().id;
    // Go has one notifier per domain: the domain's own (the statistics owner
    // campaign's) would finish the event without the unready subscriber.
    stop_domain_ddl_notifier(&stack.factory);
    let notifier = tidb_domain_core::domain::ddl_notifier::build_notifier_for_test(&stack.factory, Duration::ZERO);
    // Deterministically model the uninitialized priority queue's retry result
    // without modifying process-global auto-analyze settings in parallel tests.
    notifier.register_handler(
        tidb_ddl_notifier::TEST_HANDLER_ID,
        Arc::new(|_, _| Err(tidb_ddl_notifier::NotifierError::NotReadyRetryLater)),
    );
    drain_stats_ddl_events_with_notifier(&mut session, notifier);
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT modify_count, count FROM mysql.stats_meta WHERE table_id = {table_id}"
            )
        )),
        [["0", "0"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM mysql.tidb_ddl_notifier"
        )),
        [["1"]],
        "the unfinished subscriber's event must remain durable"
    );
}

/// Pinned notifier `processEventForHandler` plus statistics
/// `HandleDDLEvent`: the internal session opens a real pessimistic transaction
/// before the subscriber stages mutations, commits them with the processed
/// flag, and then removes the fully processed event.
#[test]
fn stats_notifier_uses_a_real_internal_transaction_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let factory = stack.factory;
    // Go has one notifier per domain: the domain's own would finish the
    // event below without the statistics subscriber under test.
    stop_domain_ddl_notifier(&factory);
    let mut client = factory
        .open_session(session_context(85))
        .expect("client session opens");
    rows(&mut client, "USE test");
    rows(
        &mut client,
        "CREATE TABLE stats_notifier_transaction (a INT)",
    );
    rows(
        &mut client,
        "INSERT INTO stats_notifier_transaction VALUES (1),(2)",
    );
    rows(&mut client, "ANALYZE TABLE stats_notifier_transaction");
    let table_id = factory
        .catalog
        .get_latest()
        .table_info_by_name("test", "stats_notifier_transaction")
        .expect("table is published")
        .1
        .read().id;
    let version_before = displayed(rows(
        &mut client,
        &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {table_id}"),
    ))[0][0]
        .parse::<u64>()
        .expect("stats version is unsigned");
    rows(&mut client, "DELETE FROM mysql.tidb_ddl_notifier");
    // The event row is deleted only once every registered handler has
    // processed it. With no auto-analyze worker running (stats lease zero),
    // the priority queue is never initialized, and Go's
    // `AnalysisPriorityQueue.HandleDDLEvent` answers `ErrNotReadyRetryLater`
    // while `RunAutoAnalyze` is on, keeping the row. With auto-analyze off it
    // ignores the event, which leaves only the statistics subscriber under
    // test.
    rows(&mut client, "SET GLOBAL tidb_enable_auto_analyze = OFF");

    let pool: Arc<dyn SessionPool> = Arc::new(ClusterNotifierSessionPool::new(
        factory.advanced_sys_session_pool(),
    ));
    let mut publisher = pool.get().expect("publisher session");
    pub_scheme_change_to_store(
        publisher.as_mut(),
        9_001,
        -1,
        SchemaChangeEvent::flashback_cluster(),
        &ClusterNotifierTableStore,
    )
    .expect("flashback event publishes");
    pool.put(publisher);

    let notifier = tidb_domain_core::domain::ddl_notifier::build_notifier_for_test(&factory, Duration::ZERO);
    tidb_owner::Listener::on_become_owner(notifier.as_ref());
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let version_after = loop {
        let pending = displayed(rows(
            &mut client,
            "SELECT count(*) FROM mysql.tidb_ddl_notifier",
        ));
        let version = displayed(rows(
            &mut client,
            &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {table_id}"),
        ))[0][0]
            .parse::<u64>()
            .expect("stats version is unsigned");
        if pending == [["0"]] && version != version_before {
            break version;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "stats notifier did not commit the subscriber mutation and clean up the event: \
             pending {pending:?}, version {version} (before {version_before})"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    tidb_owner::Listener::on_retire_owner(notifier.as_ref());
    assert!(version_after > version_before);
}

/// Pinned Go `TestStatsCacheShouldNotCacheTemporaryTable`: GLOBAL temporary
/// metadata is published by an ordinary DDL job, its rows remain
/// connection-local and are deleted at commit, and only explicit `ANALYZE`
/// caches statistics for it: zero rows, its column loaded, not pseudo, under
/// the testkit's zero stats lease (Go probe); the plan keeps Go's pseudo
/// policy for an empty table.
#[test]
fn global_temporary_analyze_uses_session_rows_and_statistics() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(139))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE GLOBAL TEMPORARY TABLE global_stats (a INT) ON COMMIT DELETE ROWS",
    );
    // Statistics reloads can concurrently add system-table entries. Resolve
    // this table from metadata rather than guessing from new cache keys.
    let temporary_id = stack
        .factory
        .catalog
        .get_latest()
        .table_info_by_name("test", "global_stats")
        .expect("GLOBAL temporary DDL publishes its table metadata")
        .1
        .read().id;
    assert!(stack
        .factory
        .stats()
        .load()
        .get(&temporary_id)
        .and_then(tidb_stats_handle_syncload::stats_watch::TableStatsState::loaded)
        .is_none());
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
        .and_then(tidb_stats_handle_syncload::stats_watch::TableStatsState::loaded)
        .expect("explicit ANALYZE publishes a real GLOBAL temporary cache object");
    assert_eq!(analyzed.hist_coll.column_count(), 1);
    assert!(!analyzed.hist_coll.pseudo);
    assert_eq!(analyzed.hist_coll.realtime_count, 0);

    let after = displayed(rows(&mut session, "EXPLAIN SELECT * FROM global_stats"));
    assert!(
        after
            .iter()
            .flatten()
            .any(|value| value.contains("stats:pseudo")),
        "an analyzed empty table retains Go's query-time pseudo policy: {after:?}"
    );
}

/// Go hands the stats handle `Domain.NextConnID` / `ReleaseConnID` as the
/// auto-analyze process-ID generator, so the process ID names this node's
/// server ID and `KILL` reaches it (`executeKillStmt` routes by
/// `gcid.ServerID`). The production auto-analyze must draw its ID from the
/// node's connection-ID allocator, not reuse an internal session's own
/// identifier, which `KILL` cannot route.
#[test]
fn production_auto_analyze_is_killable_by_its_process_id() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let factory = stack.factory;
    let mut client = factory
        .open_session(session_context(86))
        .expect("client session opens");
    rows(&mut client, "USE test");
    rows(
        &mut client,
        "CREATE TABLE auto_kill_live (a INT, INDEX idx_a(a))",
    );
    rows(&mut client, "INSERT INTO auto_kill_live VALUES (1),(2),(3)");

    let process_id = Arc::new(AtomicU64::new(0));
    let pool = factory.advanced_sys_session_pool();
    let analyze: Arc<dyn tidb_executor::cluster_analyze_seam::ClusterAnalyze> = Arc::new(KillingSystemAnalyze {
        inner: crate::domain::cluster_session_node::node_services(&factory).analyze(),
        processes: factory.processes(),
        factory: Arc::downgrade(&factory),
        process_id: Arc::clone(&process_id),
    });
    // Each pooled session holds the services it was created with, and the
    // pool already holds the sessions the node's loops used (the privilege
    // loop's among them): the seam goes onto every session the auto-analyze
    // can be handed, held at once so the pool hands out distinct ones.
    fn install_on_pooled(
        pool: &tidb_domain_core::sys_session::DomainSysSessionPool,
        factory: &Arc<tidb_domain_core::domain::Domain>,
        analyze: &Arc<dyn tidb_executor::cluster_analyze_seam::ClusterAnalyze>,
        depth: usize,
    ) {
        if depth == 0 {
            return;
        }
        tidb_stats_handle_util::call_with_sctx(
            pool,
            |context| {
                super::super::super::cluster_stats_session(context).with_session(|session| {
                    session.set_analyzer(factory, Arc::clone(analyze));
                    Ok(())
                })?;
                install_on_pooled(pool, factory, analyze, depth - 1);
                Ok(())
            },
            &[],
        )
        .expect("the pooled system session takes the seam");
    }
    install_on_pooled(pool.as_ref(), &factory, &analyze, 4);

    let source = super::super::super::ClusterPriorityQueueSource {
        factory: Arc::downgrade(&factory),
        stats_lease: Duration::ZERO,
        session_pool: Arc::clone(&pool),
    };
    let analyzed = tidb_stats_handle_autoanalyze_priorityqueue::AnalysisJobContext::auto_analyze(
        &source,
        2,
        false,
        "analyze table %n.%n",
        &["test".to_owned(), "auto_kill_live".to_owned()],
    );

    let process_id = process_id.load(Ordering::SeqCst);
    let (gcid, truncated) = tidb_globalconn::parse_conn_id(process_id)
        .expect("an allocator-minted process ID parses");
    assert!(!truncated);
    assert_eq!(
        gcid.server_id,
        tidb_globalconn::SERVER_ID_FOR_STANDALONE,
        "process ID {process_id} names this node's server ID"
    );
    assert!(
        !analyzed,
        "KILL QUERY {process_id} must interrupt the auto-analyze"
    );
}

#[test]
fn internal_process_batch_auto_analyze_kill_query() {
    run_tracked_auto_analyze(Some(true));
}

#[test]
fn internal_process_batch_auto_analyze_kill_connection() {
    run_tracked_auto_analyze(Some(false));
}

#[test]
fn auto_analyze_exec_uses_live_tracking_and_current_session_like_go() {
    run_tracked_auto_analyze(None);
}

/// Pinned `autoanalyze/exec.TestKillInWindows` and
/// `Domain.CheckAutoAnalyzeWindows`: an out-of-window domain check kills the
/// process ID installed by `RunAnalyzeStmt`; the inclusive configured minute
/// does not.
#[test]
fn auto_analyze_window_check_kills_only_outside_the_window_like_go() {
    const PROCESS_ID: u64 = 9_002;
    let (stack, _users) = cop_backed_stack();
    let factory = stack.factory;
    let mut client = factory
        .open_session(session_context(87))
        .expect("client session opens");
    rows(
        &mut client,
        "SET GLOBAL tidb_auto_analyze_start_time = '00:00 +0000'",
    );
    rows(
        &mut client,
        "SET GLOBAL tidb_auto_analyze_end_time = '00:00 +0000'",
    );
    let cancelled = Arc::new(AtomicBool::new(false));
    let target: Arc<dyn tidb_sessmgr::ProcessKillTarget> =
        Arc::new(AutoAnalyzeKillTarget {
            cancelled: Arc::clone(&cancelled),
        });
    let _guard = factory.processes.register(
        PROCESS_ID,
        "root".to_owned(),
        "127.0.0.1:0".to_owned(),
        "test".to_owned(),
        Some(target),
    );
    tidb_stats_handle_util::GLOBAL_AUTO_ANALYZE_PROCESS_LIST.tracker(PROCESS_ID);

    let inside = chrono::DateTime::parse_from_rfc3339("2026-08-31T00:00:30Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    factory.check_auto_analyze_windows_at(inside);
    assert!(!cancelled.load(Ordering::SeqCst));
    let outside = chrono::DateTime::parse_from_rfc3339("2026-08-31T12:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    factory.check_auto_analyze_windows_at(outside);
    assert!(cancelled.load(Ordering::SeqCst));
    tidb_stats_handle_util::GLOBAL_AUTO_ANALYZE_PROCESS_LIST.untracker(PROCESS_ID);
}

#[test]
fn auto_analyze_priority_queue_uses_shared_stats_ddl_and_ordinary_analyze_path() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let factory = stack.factory;
    factory
        .campaign_stats_owner(tidb_domain_core::node_config::StatsLease::Zero)
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
    // Go's priority queue tests `HandleNextDDLEventWithTxn` after a DDL.
    drain_stats_ddl_events(&factory, &mut session);
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

    let catalog = factory.catalog.get_latest();
    let analyze_id = catalog
        .table_info_by_name("test", "queue_analyze")
        .expect("analyze table exists")
        .1
        .read().id;
    let dropped_id = catalog
        .table_info_by_name("test", "queue_drop")
        .expect("drop table exists")
        .1
        .read().id;
    let locked_id = catalog
        .table_info_by_name("test", "queue_locked")
        .expect("locked table exists")
        .1
        .read().id;
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
    // Go priorityqueue.TestDropTable handles the DROP event before observing
    // the heap. Here the active notifier owns that transaction asynchronously.
    let queue_bit = 1_u64 << tidb_ddl_notifier::PRIORITY_QUEUE_HANDLER_ID.0;
    let pending_queue_sql = format!(
        "SELECT count(*) FROM mysql.tidb_ddl_notifier WHERE processed_by_flag & {queue_bit} = 0"
    );
    let queue_deadline = std::time::Instant::now() + Duration::from_secs(5);
    while displayed(rows(&mut session, &pending_queue_sql)) != [["0"]] {
        assert!(
            std::time::Instant::now() < queue_deadline,
            "priority queue subscriber did not commit the DROP event"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let snapshot = queue.snapshot().unwrap();
    assert_eq!(snapshot.current_jobs.len(), 1);
    assert_eq!(snapshot.current_jobs[0].table_id, analyze_id);
    assert_ne!(snapshot.current_jobs[0].table_id, dropped_id);

    factory.handle_auto_analyze_tick(true, Duration::ZERO);
    let refresher = refresher
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    refresher.wait_auto_analyze_finished_for_test();
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
        .wait_auto_analyze_finished_for_test();
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let factory = stack.factory;
    factory
        .campaign_stats_owner(tidb_domain_core::node_config::StatsLease::Zero)
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
    // Go's priority queue tests `HandleNextDDLEventWithTxn` after a DDL.
    drain_stats_ddl_events(&factory, &mut session);
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
        .wait_auto_analyze_finished_for_test();
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

/// Go `analyzeColumnsPushDown` samples `FullIntRange` over an integer primary
/// key (an unsigned one split across the int64 boundary into one list, the
/// unsigned half first), `FullNotNullRange` over a clustered key and the
/// signed `FullIntRange` without a handle column -- never the record prefix
/// and its `PrefixNext`, which TiKV's table scan answers as a point get of
/// that prefix: no rows, so `stats_meta.count` 0. Unistore scans any range,
/// so the request is checked at the store's RPC client, as Go's
/// `UnistoreRPCClientSendHook` does.
#[test]
fn analyze_samples_the_go_handle_ranges() {
    use tikv_client::proto::coprocessor;
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(78))
        .expect("session opens");
    rows(&mut session, "USE test");
    for statement in [
        "CREATE TABLE range_int (a INT PRIMARY KEY, b INT, KEY(b))",
        "CREATE TABLE range_unsigned (a BIGINT UNSIGNED PRIMARY KEY, b INT)",
        "CREATE TABLE range_clustered (a VARCHAR(10), b INT, PRIMARY KEY(a) CLUSTERED)",
        "CREATE TABLE range_rowid (a INT, b INT)",
        "INSERT INTO range_int VALUES (1, 1), (2, 2), (3, 3)",
        "INSERT INTO range_unsigned VALUES \
         (1, 1), (9223372036854775808, 2), (18446744073709551615, 3)",
        "INSERT INTO range_clustered VALUES ('a', 1), ('b', 2), ('c', 3)",
        "INSERT INTO range_rowid VALUES (1, 1), (NULL, 2), (3, 3)",
    ] {
        rows(&mut session, statement);
    }
    let sent: Arc<std::sync::Mutex<Vec<(Vec<u8>, Vec<u8>)>>> = Arc::default();
    let recorded = Arc::clone(&sent);
    stack.rpc.set_send_hook(Some(Arc::new(move |request| {
        let Some(request) = request.as_any().downcast_ref::<coprocessor::Request>() else {
            return;
        };
        if request.tp == tidb_unistore_cophandler::REQ_TYPE_ANALYZE {
            recorded.lock().unwrap().extend(
                request
                    .ranges
                    .iter()
                    .map(|range| (logical_rpc_key(&range.start).to_vec(), logical_rpc_key(&range.end).to_vec())),
            );
        }
    })));
    let min_int = vec![0; 8];
    let max_int_next = [[0xff; 8].as_slice(), &[0]].concat();
    let unsigned_boundary = [0x80, 0, 0, 0, 0, 0, 0, 0].to_vec();
    for (table, expected) in [
        ("range_int", vec![(min_int.clone(), max_int_next.clone())]),
        (
            "range_unsigned",
            vec![
                (min_int.clone(), unsigned_boundary.clone()),
                (unsigned_boundary.clone(), max_int_next.clone()),
            ],
        ),
        ("range_clustered", vec![(vec![0x01], vec![0xfb])]),
        ("range_rowid", vec![(min_int.clone(), max_int_next.clone())]),
    ] {
        let table_id: i64 = displayed(rows(
            &mut session,
            &format!(
                "SELECT TIDB_TABLE_ID FROM information_schema.tables \
                 WHERE table_schema = 'test' AND table_name = '{table}'"
            ),
        ))[0][0]
            .parse()
            .expect("table id");
        sent.lock().unwrap().clear();
        rows(&mut session, &format!("ANALYZE TABLE {table}"));
        let prefix = tidb_tablecodec::table_key::gen_table_record_prefix(table_id);
        let expected = expected
            .into_iter()
            .map(|(low, high)| {
                (
                    [prefix.as_slice(), &low].concat(),
                    [prefix.as_slice(), &high].concat(),
                )
            })
            .collect::<Vec<_>>();
        let analyzed = sent
            .lock()
            .unwrap()
            .iter()
            .filter(|(start, _)| start.starts_with(&prefix))
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(analyzed, expected, "{table}");
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!("SELECT count FROM mysql.stats_meta WHERE table_id = {table_id}"),
            )),
            [["3"]],
            "{table}"
        );
    }
    stack.rpc.set_send_hook(None);
}

/// Go `getAdjustedSampleRate` takes `RealtimeCount` from
/// `GetPhysicalTableStats` -- the statistics cache, or `PseudoTable` on a
/// miss -- not from the `mysql.stats_meta` row itself. With `stats-lease = 0`
/// nothing reloads the cache between statements, so a changed stored count
/// is not what the next ANALYZE's sample-rate note reports.
#[test]
fn analyze_sample_rate_reads_the_statistics_cache() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(79))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE TABLE rate_cache (a INT PRIMARY KEY, b INT)",
    );
    rows(
        &mut session,
        "INSERT INTO rate_cache VALUES (1, 1), (2, 2), (3, 3)",
    );
    let table_id: i64 = displayed(rows(
        &mut session,
        "SELECT TIDB_TABLE_ID FROM information_schema.tables \
         WHERE table_schema = 'test' AND table_name = 'rate_cache'",
    ))[0][0]
        .parse()
        .expect("table id");
    let note = |count: &str| {
        [
            "Note".to_owned(),
            "1105".to_owned(),
            format!(
                "Analyze use auto adjusted sample rate 1.000000 for table test.rate_cache, \
                 reason to use this rate is \"use min(1, 110000/{count}) as the sample-rate=1\""
            ),
        ]
    };
    rows(&mut session, "ANALYZE TABLE rate_cache");
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        [note("10000")],
        "a table the cache has not loaded is PseudoTable"
    );
    rows(
        &mut session,
        &format!("UPDATE mysql.stats_meta SET count = 1000 WHERE table_id = {table_id}"),
    );
    rows(&mut session, "ANALYZE TABLE rate_cache");
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        [note("3")],
        "the cache holds what the first ANALYZE saved"
    );
}

/// Go analyzes a temporary table as any other: the plan carries its
/// `TableInfo` (a LOCAL one from the session's `InfoSchema`), and the
/// requests go to TiKV, which holds none of its rows. The sample rate is
/// the pseudo table's, a zero-row `mysql.stats_meta` row is saved, and the
/// plan stays pseudo (Go probe, `zzprobe/tempanalyze`).
#[test]
fn analyze_of_a_temporary_table_reads_tikv_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(80))
        .expect("session opens");
    rows(&mut session, "USE test");
    let note = |table: &str| {
        [
            "Note".to_owned(),
            "1105".to_owned(),
            format!(
                "Analyze use auto adjusted sample rate 1.000000 for table test.{table}, \
                 reason to use this rate is \"use min(1, 110000/10000) as the sample-rate=1\""
            ),
        ]
    };
    let scan_row = |session: &mut Session, table: &str| -> Vec<String> {
        let plan = displayed(rows(session, &format!("EXPLAIN SELECT * FROM {table}")));
        let scan = plan.last().expect("a plan").clone();
        vec![scan[1].clone(), scan[3].clone(), scan[4].clone()]
    };
    let pseudo =
        |table: &str| vec!["10000.00".to_owned(), format!("table:{table}"), "keep order:false, stats:pseudo".to_owned()];

    rows(&mut session, "CREATE TEMPORARY TABLE lt (a INT)");
    rows(&mut session, "INSERT INTO lt VALUES (1), (2), (3)");
    assert_eq!(scan_row(&mut session, "lt"), pseudo("lt"));
    rows(&mut session, "ANALYZE TABLE lt");
    assert_eq!(displayed(rows(&mut session, "SHOW WARNINGS")), [note("lt")]);
    assert_eq!(scan_row(&mut session, "lt"), pseudo("lt"));
    assert_eq!(
        displayed(rows(&mut session, "SELECT COUNT(*), SUM(count), SUM(modify_count) FROM mysql.stats_meta")),
        [["1", "0", "0"]]
    );

    rows(&mut session, "CREATE GLOBAL TEMPORARY TABLE gt (a INT) ON COMMIT DELETE ROWS");
    rows(&mut session, "BEGIN");
    rows(&mut session, "INSERT INTO gt VALUES (1), (2), (3)");
    rows(&mut session, "ANALYZE TABLE gt");
    assert_eq!(displayed(rows(&mut session, "SHOW WARNINGS")), [note("gt")]);
    assert_eq!(scan_row(&mut session, "gt"), pseudo("gt"));
    rows(&mut session, "COMMIT");
    let meta = displayed(rows(&mut session, "SHOW STATS_META WHERE table_name = 'gt'"));
    assert_eq!(meta.len(), 1);
    assert_eq!((meta[0][0].as_str(), meta[0][1].as_str(), meta[0][4].as_str(), meta[0][5].as_str()), ("test", "gt", "0", "0"));
    assert_eq!(scan_row(&mut session, "gt"), pseudo("gt"));
    // Each zero-row ANALYZE stores its column's empty histogram.
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT is_index, hist_id, distinct_count, null_count, tot_col_size, stats_ver, flag, correlation, \
             last_analyze_pos IS NULL FROM mysql.stats_histograms ORDER BY table_id"
        )),
        [["0", "1", "0", "0", "0", "2", "0", "0", "1"], ["0", "1", "0", "0", "0", "2", "0", "0", "1"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT (SELECT COUNT(*) FROM mysql.stats_buckets), (SELECT COUNT(*) FROM mysql.stats_top_n), \
             (SELECT COUNT(*) FROM mysql.stats_fm_sketch)"
        )),
        [["0", "0", "0"]]
    );
}

/// Pinned storage `TestDeleteAnalyzeJobs` begins with the ordinary ANALYZE
/// lifecycle: the job is inserted pending, run, and retained finished for
/// `SHOW ANALYZE STATUS` until timestamp-based cleanup removes it.
#[test]
fn analyze_job_lifecycle_is_persisted_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
    let cutoff = tidb_datatype::current_time::utc_now_timestamp()
        .add_duration(MySqlDuration::from_nanoseconds(1_000_000_000, 0).unwrap())
        .unwrap();
    stack
        .factory
        .delete_analyze_jobs(cutoff)
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
    // A running job one minute in, three rows processed (Go
    // `InsertAnalyzeJob`, `StartAnalyzeJob`, `UpdateAnalyzeJobProgress`).
    rows(&mut session, "SET time_zone = '+00:00'");
    rows(
        &mut session,
        "INSERT INTO mysql.analyze_jobs (table_schema, table_name, partition_name, job_info, \
         processed_rows, start_time, state, instance, process_id) VALUES ('test', \
         'stats_show_analyze_job', '', 'analyze table all columns with 256 buckets, 100 topn, 1 samplerate', \
         3, UTC_TIMESTAMP() - INTERVAL 60 SECOND, 'running', '127.0.0.1:4000', 78)",
    );
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

/// Pinned `TestCleanupCorruptedAnalyzeJobsOnCurrentInstance` and
/// `TestCleanupCorruptedAnalyzeJobsOnDeadInstances`: the two restricted
/// transactions select different corruptions and preserve the timestamps Go
/// does not update in `BatchUpdateAnalyzeJobSQL`.
#[test]
fn corrupted_analyze_job_cleanup_matches_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(79))
        .expect("session opens");
    // Go's test names this instance from `infosync.GetServerInfo()`, as the
    // cleanup does.
    let local = stack
        .factory
        .server_info
        .as_ref()
        .expect("server info")
        .get_local_server_info();
    let local_instance = tidb_domain_serverinfo::serverinfo_syncer::join_host_port(
        &local.static_info.ip,
        local.static_info.port,
    );
    let local_instance = local_instance.as_str();
    let now = tidb_datatype::current_time::utc_now_timestamp();
    // Seven jobs, eleven minutes old unless noted: 1 pending; 2 running
    // with no process (started, finished, restarted); 3 and 4 running under
    // processes 3 and 4; 5 finished; 6 pending and new; 7 pending on another
    // instance.
    rows(&mut session, "SET time_zone = '+00:00'");
    let insert = |session: &mut _, values: String| {
        rows(
            session,
            &format!(
                "INSERT INTO mysql.analyze_jobs (id, update_time, table_schema, table_name, \
                 partition_name, job_info, start_time, end_time, state, instance, process_id) \
                 VALUES {values}"
            ),
        );
    };
    let old = "UTC_TIMESTAMP() - INTERVAL 660 SECOND";
    insert(
        &mut session,
        format!(
            "(1, {old}, 'test', 't', '', 'job', NULL, NULL, 'pending', '{local_instance}', 1), \
             (2, {old}, 'test', 't', '', 'job', {old}, {old}, 'running', '{local_instance}', NULL), \
             (3, {old}, 'test', 't', '', 'job', {old}, NULL, 'running', '{local_instance}', 3), \
             (4, {old}, 'test', 't', '', 'job', {old}, NULL, 'running', '{local_instance}', 4), \
             (5, {old}, 'test', 't', '', 'job', {old}, {old}, 'finished', '{local_instance}', NULL), \
             (6, UTC_TIMESTAMP(), 'test', 't', '', 'job', NULL, NULL, 'pending', '{local_instance}', 6), \
             (7, {old}, 'test', 't', '', 'job', NULL, NULL, 'pending', '10.0.0.1:4000', 7)"
        ),
    );

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
    // `update_time` is `ON UPDATE CURRENT_TIMESTAMP`: the rows the batch
    // UPDATE changed (jobs 1 and 4) move on, the others keep theirs (gorun:
    // a running job's `2000-01-01 00:00:00` becomes the update's time).
    let update_times_after = displayed(rows(
        &mut session,
        "SELECT id, update_time FROM mysql.analyze_jobs ORDER BY id",
    ));
    for (before, after) in update_times_before.iter().zip(&update_times_after) {
        if before[0] == "1" || before[0] == "4" {
            assert!(after[1] > before[1], "job {} kept {}", before[0], before[1]);
        } else {
            assert_eq!(after, before);
        }
    }

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
            tidb_stats_handle_autoanalyze::analyze_jobs::CORRUPTED_ANALYZE_JOB_FAILURE,
        ]]
    );
}

#[test]
fn analyze_job_cleanup_worker_uses_gos_positive_lease_gate_and_stops() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let factory = stack.factory;
    factory.start_analyze_jobs_cleanup_worker(tidb_domain_core::node_config::StatsLease::Zero);
    assert!(factory.analyze_jobs_cleanup_worker.get().is_none());
    factory.start_analyze_jobs_cleanup_worker(tidb_domain_core::node_config::StatsLease::Disabled);
    assert!(factory.analyze_jobs_cleanup_worker.get().is_none());
    factory.start_analyze_jobs_cleanup_worker(tidb_domain_core::node_config::StatsLease::Positive(
        Duration::from_secs(60),
    ));
    assert!(factory.analyze_jobs_cleanup_worker.get().is_some());
    drop(factory);
}

#[test]
fn auto_analyze_worker_uses_gos_positive_lease_gate_and_stops() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let factory = stack.factory;
    factory.start_auto_analyze_worker(tidb_domain_core::node_config::StatsLease::Zero);
    assert!(factory.auto_analyze_worker.get().is_none());
    factory.start_auto_analyze_worker(tidb_domain_core::node_config::StatsLease::Disabled);
    assert!(factory.auto_analyze_worker.get().is_none());
    factory.start_auto_analyze_worker(tidb_domain_core::node_config::StatsLease::Positive(
        Duration::from_secs(60),
    ));
    assert!(factory.auto_analyze_worker.get().is_some());
    drop(factory);
}

#[test]
fn auto_analyze_worker_observes_the_live_process_switch() {
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            tidb_vardef::RUN_AUTO_ANALYZE.store(self.0, Ordering::SeqCst);
        }
    }
    let _restore = Restore(tidb_vardef::RUN_AUTO_ANALYZE.swap(true, Ordering::SeqCst));
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let factory = &stack.factory;
    let owner = factory.stats_owner.as_ref().unwrap();
    owner.campaign_owner(&[]).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !owner.is_owner() {
        assert!(std::time::Instant::now() < deadline);
        std::thread::yield_now();
    }
    let queue = factory.auto_analyze_priority_queue(Duration::ZERO);
    factory.handle_auto_analyze_tick(true, Duration::ZERO);
    assert!(queue.is_initialized());
    factory.start_auto_analyze_worker(tidb_domain_core::node_config::StatsLease::Positive(
        Duration::from_millis(10),
    ));
    tidb_vardef::RUN_AUTO_ANALYZE.store(false, Ordering::SeqCst);
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while queue.is_initialized() {
        assert!(
            std::time::Instant::now() < deadline,
            "worker must observe Go's current process switch"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    owner.campaign_cancel();
}

/// Pinned `pkg/statistics/handle/handletest/handle_test.go::
/// TestStatsCacheShouldNotCacheSystemTable`. The SHOW scans may enumerate
/// every schema, but they must not publish system-table objects into the
/// canonical statistics cache.
#[test]
fn show_stats_does_not_cache_system_table_statistics() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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

/// Go master `TestPlanReplayer`: the legacy `WITH STATS AS OF TIMESTAMP`
/// syntax still parses, but its execution is refused because the historical
/// statistics feature has been removed.
#[test]
fn plan_replayer_dump_with_stats_as_of_timestamp_is_refused() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let mut session = stack.factory.open_session(session_context(78)).expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "CREATE TABLE t (a INT, b INT, INDEX ia(a))");
    let error = session
        .run("plan replayer dump with stats as of timestamp '2023-06-28 12:34:00' explain select * from t where a=10")
        .unwrap_err();
    assert_eq!(
        error.to_mysql_error().message,
        "WITH STATS AS OF TIMESTAMP is no longer supported because the historical statistics feature has been removed; remove the clause to dump the current statistics"
    );
}

#[test]
fn stats_gc_runs_periodically_only_after_ownership_is_acquired() {
    // Go `UpdateTableStatsLoop` campaigns the statistics owner at boot only
    // when `instance.tidb_enable_stats_owner` is on; this node starts off it,
    // so the campaign below is its first ownership.
    let was_enabled = tidb_config::config_tree::config::get_global_config()
        .instance
        .tidb_enable_stats_owner
        .load();
    let set_enabled = |enabled: bool| {
        tidb_config::config_tree::config::update_global(|config| {
            config.instance.tidb_enable_stats_owner = tidb_config::config_tree::AtomicBool::new(enabled);
        });
    };
    set_enabled(false);
    let (stack, _users) = cop_backed_stack_with_stats_lease(Some(
        tidb_domain_core::node_config::StatsLease::Positive(Duration::from_millis(1)),
    ));
    set_enabled(was_enabled);
    let factory = &stack.factory;
    let owner = factory.stats_owner.as_ref().unwrap();
    let mut session = factory.open_session(session_context(79)).unwrap();
    let gc_marker = "SELECT count(*) FROM mysql.tidb WHERE variable_name = 'tidb_stats_gc_last_ts'";
    std::thread::sleep(Duration::from_millis(220));
    assert_eq!(displayed(rows(&mut session, gc_marker)), [["0"]]);
    rows(
        &mut session,
        "CREATE TABLE test.periodic_gc (a INT, KEY idx(a))",
    );
    rows(&mut session, "INSERT INTO test.periodic_gc VALUES (1),(2)");
    rows(&mut session, "ANALYZE TABLE test.periodic_gc WITH 0 TOPN");
    rows(&mut session, "ALTER TABLE test.periodic_gc DROP INDEX idx");
    // Make only the stats version old enough for Go's 10 * max(lease) window.
    // The production worker must retain the actual schema lease, not use zero.
    rows(&mut session, "UPDATE mysql.stats_meta SET version = 1");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM mysql.stats_histograms WHERE is_index = 1"
        )),
        [["1"]]
    );
    owner.campaign_owner(&[]).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        if displayed(rows(&mut session, gc_marker)) == [["1"]] {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "positive statistics lease must run GC through the elected owner without a manual call"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM mysql.stats_histograms WHERE is_index = 1"
        )),
        [["0"]]
    );
    owner.campaign_cancel();
    // A later ownership term must keep the same worker and continue after
    // storage errors. An invalid persisted watermark is a real GC read error.
    std::thread::sleep(Duration::from_millis(220));
    rows(&mut session, "UPDATE mysql.tidb SET variable_value = 'invalid' WHERE variable_name = 'tidb_stats_gc_last_ts'");
    owner.campaign_owner(&[]).unwrap();
    std::thread::sleep(Duration::from_millis(220));
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT variable_value FROM mysql.tidb WHERE variable_name = 'tidb_stats_gc_last_ts'"
        )),
        [["invalid"]]
    );
    rows(
        &mut session,
        "UPDATE mysql.tidb SET variable_value = '0' WHERE variable_name = 'tidb_stats_gc_last_ts'",
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        if displayed(rows(
            &mut session,
            "SELECT variable_value FROM mysql.tidb WHERE variable_name = 'tidb_stats_gc_last_ts'",
        )) != [["0"]]
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "a failed GC pass must not retire the owner worker"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    owner.campaign_cancel();
}

#[test]
fn stats_maintenance_is_absent_for_zero_and_negative_leases() {
    for lease in [
        tidb_domain_core::node_config::StatsLease::Zero,
        tidb_domain_core::node_config::StatsLease::Disabled,
    ] {
        let (stack, _users) = cop_backed_stack_with_stats_lease(Some(lease));
        assert!(stack._stats_maintenance.is_none());
    }
}

#[test]
fn stats_gc_matches_go_item_and_dropped_table_phases() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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

/// Pinned storage `TestGCColumnStatsUsage`: a dropped column removes only its
/// usage row, then dropped-table GC removes every remaining usage row.
#[test]
fn stats_gc_matches_go_column_usage_cleanup() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
    // Go `SaveAnalyzeResultToStorage` stamps each analyzed column's
    // `last_analyzed_at`.
    let usage_count = |session: &mut Session| {
        displayed(rows(session, "SELECT count(*) FROM mysql.column_stats_usage"))
    };
    assert_eq!(usage_count(&mut session), vec![vec!["3".to_owned()]]);

    rows(&mut session, "ALTER TABLE stats_gc_usage DROP COLUMN a");
    assert_eq!(usage_count(&mut session), vec![vec!["3".to_owned()]]);
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        .get_latest()
        .table_info_by_name("test", "stats_gc_empty_hist")
        .expect("table is in the live catalog")
        .1
        .read().id;
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

/// Go `statsReadWriter.TableStatsToJSON` reads the analyzed statistics back
/// through `TableStatsFromStorage`: the column's TopN and buckets together
/// count every row, and the counters come from `mysql.stats_meta`.
#[test]
fn latest_stats_dump_reads_the_analyzed_histograms_as_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(78))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");
    rows(&mut session, "CREATE TABLE dump_latest (a INT, INDEX idx(a))");
    rows(&mut session, "INSERT INTO dump_latest VALUES (1),(1),(1),(2),(2),(3),(4)");
    rows(&mut session, "ANALYZE TABLE dump_latest WITH 2 TOPN");
    // A newer meta version than the cached table's: only Go's second read
    // (`StatsMetaByTableIDFromStorage`) sees it.
    rows(
        &mut session,
        "UPDATE mysql.stats_meta SET version = version + 1 WHERE table_id = \
         (SELECT tidb_table_id FROM information_schema.tables WHERE table_name = 'dump_latest')",
    );
    let catalog = stack.factory.catalog.get_latest();
    let (_, table) = catalog
        .table_info_by_name("test", "dump_latest")
        .expect("analyzed table remains published");
    let table = table.clone();
    let table = table.read();
    drop(catalog);
    let latest = stack
        .factory
        .dump_stats_to_json("test", &table, true)
        .expect("latest statistics dump succeeds")
        .expect("latest statistics exist");
    assert_eq!(latest.count, 7);
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT m.version FROM mysql.stats_meta m JOIN information_schema.tables t \
             ON m.table_id = t.tidb_table_id WHERE t.table_name = 'dump_latest'",
        )),
        [[latest.version.to_string()]]
    );
    let total = |item: &tidb_stats_util::JsonColumn| {
        let topn: u64 = item
            .cm_sketch
            .as_ref()
            .and_then(|sketch| sketch.top_n.as_ref())
            .map_or(0, |top_n| top_n.iter().flatten().map(|entry| entry.count).sum());
        let buckets = item
            .histogram
            .as_ref()
            .and_then(|histogram| histogram.buckets.as_ref())
            .and_then(|buckets| buckets.last().cloned().flatten())
            .map_or(0, |bucket| bucket.count);
        topn as i64 + buckets
    };
    let columns = latest.columns.as_ref().expect("column statistics exist");
    assert_eq!(total(columns["a"].as_ref().expect("column a")), 7);
    let indices = latest.indices.as_ref().expect("index statistics exist");
    assert_eq!(total(indices["idx"].as_ref().expect("index idx")), 7);
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

/// Pinned `globalstats.TestBuildGlobalLevelStats`: static ANALYZE publishes
/// physical rows only, dynamic ANALYZE adds the logical global row, and
/// predicate-column demand controls the complete histogram inventory.
#[test]
fn build_global_level_stats_matches_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        .dump_col_stats_usage_to_kv()
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

/// Pinned `globalstats.TestGlobalStatsData3`: composite-index global TopN,
/// buckets, and NDV preserve the source behavior for every covered key type.
#[test]
fn composite_index_global_stats_match_go_types() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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

#[test]
fn flush_stats_delta_missing_targets_match_go_warnings() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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

#[test]
fn analyze_sampling_memory_quota_rejects_then_session_recovers() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(991)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.sample_quota (id INT PRIMARY KEY, v VARCHAR(100))",
    );
    rows(
        &mut session,
        "INSERT INTO test.sample_quota VALUES (1,'one'),(2,'two'),(3,'three')",
    );
    // Go `tidb_mem_quota_analyze`'s `SetGlobal` / `GetGlobal` are
    // `GlobalAnalyzeMemoryTracker`'s limit; its `globalPanicOnExceed` turns
    // into `errAnalyzeOOM` (`getAnalyzePanicErr`).
    rows(&mut session, "SET GLOBAL tidb_mem_quota_analyze = 1");
    assert_eq!(tidb_memory::get_mem_quota_analyze(), 1);
    assert_eq!(displayed(rows(&mut session, "SELECT @@global.tidb_mem_quota_analyze")), [["1"]]);
    let error = session
        .node_conn()
        .execute_write("ANALYZE TABLE test.sample_quota WITH 1 SAMPLERATE")
        .expect_err("sample exceeds quota");
    assert_eq!(
        error.message,
        "analyze panic due to memory quota exceeds, please try with smaller samplerate(refer to 110000/count)"
    );
    rows(&mut session, "SET GLOBAL tidb_mem_quota_analyze = -1");
    assert_eq!(tidb_memory::get_mem_quota_analyze(), -1);
    rows(
        &mut session,
        "ANALYZE TABLE test.sample_quota WITH 1 SAMPLERATE",
    );
    assert_eq!(
        rows(&mut session, "SELECT count(*) FROM test.sample_quota").len(),
        1
    );
}

#[test]
fn analyze_preserves_generated_execution_error() {
    assert_analyze_preserves_generated_execution_error(None);
}

/// The same statements while the stats worker passes every 5ms, so a pass
/// always caches the table between its first `stats_meta` row (the failing
/// ANALYZE's delta flush) and the second ANALYZE's own refresh -- the order
/// a 3s worker reached only under load. Go's `SELECT b` cached a
/// `PseudoTable` holding `a` (`Handle.getStatsByPhysicalID`), which that pass
/// copies; without it the pass cached a table holding no column, and
/// ANALYZE's histograms never became visible.
#[test]
fn analyze_preserves_generated_execution_error_under_stats_worker_passes() {
    assert_analyze_preserves_generated_execution_error(Some(
        tidb_domain_core::node_config::StatsLease::Positive(std::time::Duration::from_millis(5)),
    ));
}

/// Go `SimpleExec.executeDropStats` soft-deletes every named physical id's
/// statistics (`DeleteTableStatsFromKV(ids, true)`): the buckets and TopN rows
/// go, the histogram rows are zeroed in place, and the meta row stays with a
/// fresh version. The production node answered "DROP STATS ... is not ported"
/// and left every row.
#[test]
fn drop_stats_soft_deletes_the_stored_statistics() {
    let (stack, _users) = cop_backed_stack();
    let factory = stack.factory;
    let mut session = factory
        .open_session(session_context(1038))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE TABLE ds (c1 VARCHAR(20), c2 VARCHAR(20))",
    );
    rows(
        &mut session,
        "INSERT INTO ds VALUES ('1','1'),('2','2'),('3','3'),('4','4'),('1','1'),('2','2'),('3','3'),('4','4')",
    );
    rows(&mut session, "ANALYZE TABLE ds ALL COLUMNS WITH 2 TOPN");
    let table_id = factory
        .catalog
        .get_latest()
        .table_info_by_name("test", "ds")
        .expect("table is published")
        .1
        .read().id;
    let count = |session: &mut Session, table: &str| {
        displayed(rows(
            session,
            &format!("SELECT COUNT(*) FROM mysql.{table} WHERE table_id = {table_id}"),
        ))[0][0]
            .clone()
    };
    assert_ne!(count(&mut session, "stats_buckets"), "0");
    assert_ne!(count(&mut session, "stats_top_n"), "0");
    let version_before = displayed(rows(
        &mut session,
        &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {table_id}"),
    ))[0][0]
        .parse::<u64>()
        .unwrap();

    session.node_conn().execute_write("DROP STATS ds").unwrap();

    assert_eq!(count(&mut session, "stats_buckets"), "0");
    assert_eq!(count(&mut session, "stats_top_n"), "0");
    assert_eq!(count(&mut session, "column_stats_usage"), "0");
    assert_ne!(count(&mut session, "stats_histograms"), "0");
    // The histogram rows stay, zeroed; the meta row keeps its counts and is
    // re-versioned.
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT COUNT(*) FROM mysql.stats_histograms WHERE table_id = {table_id} \
                 AND distinct_count <> 0"
            ),
        )),
        [["0"]]
    );
    let version_after = displayed(rows(
        &mut session,
        &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {table_id}"),
    ))[0][0]
        .parse::<u64>()
        .unwrap();
    assert!(version_after > version_before);
}

/// Go's must-analyze set follows only VIRTUAL generated columns back to their
/// base columns (`VirtualExpr`); an indexed STORED generated column is decoded
/// from storage, so its JSON base columns stay under
/// `tidb_analyze_skip_column_types` and get no histogram. Rust followed every
/// generated column, collected `j1`/`j2`, and so priced the table's scans on a
/// wider row than Go does (`TableFullScan` 271.59 for Go's 410.74).
#[test]
fn analyze_skips_json_bases_of_stored_generated_index_columns() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1100)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE sgb (j1 JSON, j2 JSON, a INT AS (JSON_EXTRACT(j1, '$.a')) STORED, \
             b VARCHAR(20) AS (JSON_KEYS(j2)) STORED, KEY idx_a (a), KEY idx_b (b))",
        )
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO sgb(j1, j2) VALUES ('{\"a\": 1}', '{\"1\": \"1\"}')")
        .unwrap();
    rows(&mut session, "ANALYZE TABLE sgb");
    let analyzed: Vec<String> = displayed(rows(
        &mut session,
        "SHOW STATS_HISTOGRAMS WHERE table_name = 'sgb' AND is_index = 0",
    ))
    .into_iter()
    .map(|row| row[3].clone())
    .collect();
    // Expected: Go 1.27 on this branch (`TestZZ` probe, `show stats_histograms`).
    assert_eq!(analyzed, ["a", "b"]);
}

/// Go `AnalyzeExec.Next` ends in `return statsHandle.Update(ctx, infoSchema,
/// tableAndPartitionIDs...)`: one update after every table is saved, whose
/// failure is the statement's error although the statistics are stored. The
/// port refreshed per table and only logged the failure.
#[test]
fn failed_stats_cache_update_fails_the_analyze_statement() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1133)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    rows(&mut session, "CREATE TABLE analyze_update_a (a INT)");
    rows(&mut session, "CREATE TABLE analyze_update_b (a INT)");
    rows(&mut session, "INSERT INTO analyze_update_a VALUES (1), (2)");
    rows(&mut session, "INSERT INTO analyze_update_b VALUES (3)");
    let updated = Arc::new(std::sync::Mutex::new(Vec::new()));
    let analyze = Arc::new(FailingStatsCacheUpdate {
        inner: crate::domain::cluster_session_node::node_services(session.node().domain()).analyze(),
        updated: Arc::clone(&updated),
    });
    session.node_conn().set_analyzer(&stack.factory, analyze);
    let error = session
        .node_conn()
        .execute("ANALYZE TABLE analyze_update_a, analyze_update_b")
        .err()
        .expect("the cache update failure is the statement's error");
    assert_eq!(error.message, "statistics cache update failed");
    let ids = displayed(rows(
        &mut session,
        "SELECT tidb_table_id FROM information_schema.tables \
         WHERE table_name IN ('analyze_update_a', 'analyze_update_b') ORDER BY table_name",
    ));
    assert_eq!(
        *updated.lock().unwrap(),
        ids.iter()
            .map(|row| row[0].parse::<i64>().unwrap())
            .collect::<Vec<_>>(),
        "one update over both tables, after both are analyzed"
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT count(DISTINCT table_id) FROM mysql.stats_histograms \
                 WHERE table_id IN ({}, {})",
                ids[0][0], ids[1][0]
            ),
        )),
        [["2"]],
        "both tables' statistics are stored"
    );
}

/// A pre-evaluated scalar subquery keeps its `ScalarQueryCol` reference even
/// when its value is NULL, and `tidb_opt_derive_topn` derives a TopN or Limit
/// below a window from a filter on `row_number()`. Go oracle texts.
#[test]
fn scalar_subquery_null_reference_and_derived_topn_follow_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1133)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE dq1 (a INT, b INT)")
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE dq2 (a INT, b INT)")
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE dw (a INT, b INT, c INT, PRIMARY KEY (b, a))")
        .unwrap();
    let plan = |session: &mut Session, sql: &str| {
        displayed(rows(session, &format!("explain format='brief' {sql}")))
            .into_iter()
            .map(|row| row.join(" "))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let text = plan(&mut session, "select dq1.a, (select min(dq2.a) from dq2) from dq1");
    assert!(text.contains("(<nil>)->Column"), "{text}");
    assert!(text.contains("ScalarQueryCol#"), "{text}");

    let window = "select * from (select row_number() over (order by a) as rn from dw) dt where rn <= 3";
    let off = plan(&mut session, window);
    assert!(!off.contains("TopN"), "{off}");
    session
        .node_conn()
        .execute_write("SET tidb_opt_derive_topn = 1")
        .unwrap();
    let on = plan(&mut session, window);
    assert!(on.contains("TopN") && on.contains("count:3"), "{on}");
    let no_order = plan(
        &mut session,
        "select * from (select row_number() over () as rn from dw) dt where rn <= 3",
    );
    assert!(no_order.contains("Limit") && no_order.contains("count:3"), "{no_order}");
}

/// `planner/core/indexmerge_path`: Go analyzes a multi-valued index as an
/// independent index task per physical table (its keys, not the row
/// samples), dumps each partition's FM sketch for the global merge, and the
/// column task's job lists only its own indexes. Values from the oracle.
#[test]
fn analyze_runs_multi_valued_indexes_as_independent_tasks() {
    if crate::domain::isolate_process_globals() {
        return;
    }
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(134)).unwrap();
    for sql in [
        "CREATE TABLE test.mvt (a INT, b INT, j JSON, INDEX ia(a), INDEX mv((CAST(j AS SIGNED ARRAY))))",
        "INSERT INTO test.mvt VALUES (1, 2, '[1,2,3]'), (2, 3, '[2,3]'), (3, 4, '[]')",
        "ANALYZE TABLE test.mvt",
        "CREATE TABLE test.mvp (a INT, j JSON, INDEX mv((CAST(j AS SIGNED ARRAY)))) \
         PARTITION BY HASH(a) PARTITIONS 2",
        "INSERT INTO test.mvp VALUES (1, '[1,2]'), (2, '[3]'), (3, '[4,5,6]')",
        "ANALYZE TABLE test.mvp",
    ] {
        rows(&mut session, sql);
    }
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT job_info FROM information_schema.analyze_status \
             WHERE table_name = 'mvt' ORDER BY job_info"
        )),
        [
            ["analyze index mv"],
            ["analyze table index ia, columns a, b with 256 buckets, 100 topn, 1 samplerate"],
        ]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT h.distinct_count FROM mysql.stats_histograms h \
             JOIN information_schema.tables t ON h.table_id = t.tidb_table_id \
             WHERE t.table_name = 'mvp' AND h.is_index = 1"
        )),
        [["3"]]
    );
}

/// Go `statsLockImpl.LockTables` runs Go's statements in a transaction of a
/// statistics session of its own (`util.CallWithSCtx(..., FlagWrapTxn)`):
/// the user's transaction stays open and the lock row outlives its
/// rollback. A second LOCK STATS reports Go's skipped-target warning.
#[test]
fn lock_stats_runs_in_its_own_session_transaction_as_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(200)).unwrap();
    let mut probe = stack.factory.open_session(session_context(201)).unwrap();
    rows(&mut session, "create table test.ls1 (a int)");
    let table_id = displayed(rows(
        &mut probe,
        "select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'ls1'",
    ))[0][0]
        .clone();
    rows(&mut session, "begin");
    rows(&mut session, "lock stats test.ls1");
    assert!(session.in_transaction());
    assert!(rows(&mut session, "show warnings").is_empty());
    rows(&mut session, "lock stats test.ls1");
    assert_eq!(
        displayed(rows(&mut session, "show warnings")),
        [["Warning", "1105", "skip locking locked table: test.ls1"]]
    );
    rows(&mut session, "rollback");
    assert_eq!(
        displayed(rows(
            &mut probe,
            &format!("select count(*) from mysql.stats_table_locked where table_id = {table_id}"),
        )),
        [["1"]]
    );
    rows(&mut session, "unlock stats test.ls1");
    assert_eq!(
        displayed(rows(&mut probe, "select count(*) from mysql.stats_table_locked")),
        [["0"]]
    );
}

/// Go `LockExec`: every target's INSERT and SELECT privilege is checked
/// before the handle's transaction; a refused LOCK STATS locks nothing.
#[test]
fn lock_stats_checks_privileges_before_its_transaction_as_go() {
    let (stack, users) = cop_backed_stack();
    let mut root = stack.factory.open_session(session_context(202)).unwrap();
    rows(&mut root, "create table test.ls2 (a int)");
    rows(&mut root, "create user 'ls_low'@'%' identified by 'abc'");
    rows(&mut root, "grant create temporary tables on test.* to 'ls_low'@'%'");
    let identity = users
        .authenticate_native(
            "ls_low",
            "127.0.0.1",
            &super::super::node_fixture::SALT,
            &super::super::node_fixture::scramble(b"abc", &super::super::node_fixture::SALT),
        )
        .expect("ls_low logs in");
    let mut low = stack
        .factory
        .open_session(super::super::node_fixture::session_context_with_identity(203, identity))
        .unwrap();
    let error = low
        .node_conn()
        .execute_write("lock stats test.ls2")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1142, "INSERT command denied to user 'ls_low'@'%' for table 'ls2'")
    );
    assert_eq!(
        displayed(rows(&mut root, "select count(*) from mysql.stats_table_locked")),
        [["0"]]
    );
}

/// Go `SaveAnalyzeResultToStorage` replaces only the histograms an analysis
/// produced: a later full ANALYZE keeps the rows of an index dropped in
/// between (statistics GC removes them).
#[test]
fn a_reanalyze_keeps_a_dropped_index_histogram_as_go() {
    let (stack, _users) = cop_backed_stack();
    let mut client = stack.factory.open_session(session_context(206)).unwrap();
    rows(&mut client, "use test");
    rows(&mut client, "create table rt(a int, b int, index ib(b))");
    rows(&mut client, "insert into rt values (1,1),(2,2),(3,3)");
    let histograms = "select is_index, hist_id from mysql.stats_histograms where table_id = (select tidb_table_id from information_schema.tables where table_schema='test' and table_name='rt') order by is_index, hist_id";
    rows(&mut client, "analyze table rt");
    assert_eq!(
        displayed(rows(&mut client, histograms)),
        [["0", "1"], ["0", "2"], ["1", "1"]]
    );
    rows(&mut client, "alter table rt drop index ib");
    rows(&mut client, "analyze table rt");
    assert_eq!(
        displayed(rows(&mut client, histograms)),
        [["0", "1"], ["0", "2"], ["1", "1"]]
    );
}

/// Go `SaveAnalyzeResultToStorage`'s `mysql.stats_meta` write on a
/// statistics session: rows changed after sampling stay counted, a newer
/// snapshot wins, and a multi-valued or global index result moves only the
/// versions of an existing row.
#[test]
fn analyze_results_save_stats_meta_as_go() {
    use tidb_stats_handle_storage::cluster_stats_load::ClusterTableStats;
    use tidb_stats_handle_storage::save::{save_analyze_result_to_storage, AnalyzeResults, STATS_VERSION_2};

    let (stack, _users) = cop_backed_stack();
    let mut client = stack.factory.open_session(session_context(207)).unwrap();
    let pool = stack.factory.advanced_sys_session_pool();
    let save = |table_id: i64, snapshot: u64, row_count: u64, base_count: i64, mv: bool, analyze_snapshot: bool| {
        let stats = ClusterTableStats {
            table_id,
            version: 0,
            snapshot,
            last_analyze_version: 0,
            last_stats_hist_version: 0,
            modify_count: 0,
            row_count,
            columns: Vec::new(),
            indexes: Vec::new(),
        };
        let mut stats_ver = 0;
        tidb_stats_handle_util::call_with_sctx(
            pool.as_ref(),
            |sctx| {
                stats_ver = save_analyze_result_to_storage(
                    sctx,
                    &AnalyzeResults {
                        stats: &stats,
                        is_partition: false,
                        base_count,
                        base_modify_count: 0,
                        stats_ver: STATS_VERSION_2,
                        for_mv_index_or_global_index: mv,
                    },
                    analyze_snapshot,
                )?;
                Ok(())
            },
            &[tidb_stats_handle_util::FLAG_WRAP_TXN],
        )
        .expect("the save commits");
        stats_ver
    };
    let meta = |client: &mut Session, table_id: i64| {
        displayed(rows(
            client,
            &format!("select version, snapshot, modify_count, count, last_stats_histograms_version from mysql.stats_meta where table_id = {table_id}"),
        ))
    };
    let row = |values: [&str; 5]| vec![values.map(str::to_owned).to_vec()];

    for (table_id, analyze_snapshot, count) in [(4242, true, "6"), (4243, false, "3")] {
        rows(
            &mut client,
            &format!("insert into mysql.stats_meta (version, table_id, modify_count, count, snapshot) values (90, {table_id}, 3, 6, 10)"),
        );
        let version = save(table_id, 100, 3, 3, false, analyze_snapshot);
        assert_ne!(version, 0);
        let version = version.to_string();
        assert_eq!(
            meta(&mut client, table_id),
            row([&version, "100", "3", count, &version])
        );
    }

    rows(
        &mut client,
        "insert into mysql.stats_meta (version, table_id, modify_count, count, snapshot) values (120, 4244, 4, 8, 110)",
    );
    assert_eq!(save(4244, 100, 3, 3, false, true), 0);
    assert_eq!(meta(&mut client, 4244), row(["120", "110", "4", "8", "NULL"]));

    assert_eq!(save(4244, 200, 9, 0, true, true), 0);
    let moved = meta(&mut client, 4244);
    assert_ne!(moved[0][0], "120");
    assert_eq!(moved[0][1..4], ["110", "4", "8"]);
    assert_eq!(moved[0][4], moved[0][0]);

    let version = save(4245, 200, 9, 0, true, true);
    assert_ne!(version, 0);
    let version = version.to_string();
    assert_eq!(meta(&mut client, 4245), row([&version, "0", "0", "0", &version]));
}

/// Go `storage.UpdateStatsMeta` through `DumpStatsDeltaToKV`: deltas
/// accumulate into `stats_meta`, a negative one clamps the count at zero,
/// and a locked table's delta is stashed in `stats_table_locked` instead.
#[test]
fn stats_delta_flush_updates_meta_as_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(209)).unwrap();
    rows(&mut session, "USE test");
    rows(&mut session, "CREATE TABLE delta_flush (a INT)");
    let table_id = displayed(rows(
        &mut session,
        "SELECT tidb_table_id FROM information_schema.tables WHERE table_schema = 'test' AND table_name = 'delta_flush'",
    ))[0][0]
        .clone();
    let meta = |session: &mut Session, table: &str| {
        displayed(rows(
            session,
            &format!("SELECT count, modify_count FROM mysql.{table} WHERE table_id = {table_id}"),
        ))
    };
    let flush = || {
        stack
            .factory
            .dump_stats_delta_to_kv(true, &[])
            .expect("the delta flush succeeds");
    };
    rows(&mut session, "INSERT INTO delta_flush VALUES (1),(2),(3)");
    flush();
    assert_eq!(meta(&mut session, "stats_meta"), [["3", "3"]]);
    rows(&mut session, "DELETE FROM delta_flush WHERE a = 1");
    flush();
    assert_eq!(meta(&mut session, "stats_meta"), [["2", "4"]]);
    // A negative delta beyond the stored count leaves zero, not a wrap.
    rows(&mut session, &format!("UPDATE mysql.stats_meta SET count = 1 WHERE table_id = {table_id}"));
    rows(&mut session, "DELETE FROM delta_flush");
    flush();
    assert_eq!(meta(&mut session, "stats_meta"), [["0", "6"]]);

    rows(&mut session, "LOCK STATS delta_flush");
    rows(&mut session, "INSERT INTO delta_flush VALUES (4),(5)");
    flush();
    assert_eq!(meta(&mut session, "stats_meta"), [["0", "6"]]);
    assert_eq!(meta(&mut session, "stats_table_locked"), [["2", "2"]]);
}

/// Go `TestDumpStatsDeltaMergeKeepsEarliestInitTime`'s slow-dump steps
/// (249288e17a4): a delta dump blocked on its `stats_meta` lock read commits
/// with its old start_ts as the version, after the stats cache already moved
/// past it; the dump then writes a newer version, so the cache still loads
/// the delta. The lock wait reaches the statement summary, since the lock
/// read's record set is closed.
#[test]
fn slow_stats_delta_dump_bumps_the_stats_meta_version() {
    use std::sync::mpsc;
    use std::time::Duration;
    let (stack, _users) = cop_backed_stack();
    let mut tk = stack.factory.open_session(session_context(209)).unwrap();
    rows(&mut tk, "set global tidb_stmt_summary_internal_query = 1");
    rows(&mut tk, "use test");
    rows(&mut tk, "create table t_lock (a int)");
    rows(&mut tk, "create table t_other (a int)");
    rows(&mut tk, "insert into t_lock values (1)");
    rows(&mut tk, "insert into t_other values (1)");
    rows(&mut tk, "analyze table t_lock");
    rows(&mut tk, "analyze table t_other");
    let lock_table_id = table_id_of(&mut tk, "t_lock");
    let other_table_id = table_id_of(&mut tk, "t_other");
    tk.update_stats_cache().unwrap();
    stack.factory.dump_stats_delta_to_kv(true, &[]).unwrap();
    let hist_version = |tk: &mut Session| {
        displayed(rows(
            tk,
            &format!("select last_stats_histograms_version from mysql.stats_meta where table_id = {lock_table_id}"),
        ))
    };
    let analyzed_hist_version = hist_version(&mut tk);
    // The stats cache looks back 5 * lease = 500ms, and the blocked dump
    // takes more than that.
    stack.factory.set_stats_lease(Duration::from_millis(100));

    rows(&mut tk, "insert into t_lock values (2),(3)");
    let mut tk_lock = stack.factory.open_session(session_context(210)).unwrap();
    rows(&mut tk_lock, "set @@tidb_txn_mode = 'pessimistic'");
    rows(&mut tk_lock, "begin");
    rows(
        &mut tk_lock,
        &format!("select * from mysql.stats_meta where table_id = {lock_table_id} for update"),
    );

    let (ready_tx, ready) = mpsc::channel::<()>();
    let ready_tx = std::sync::Mutex::new(Some(ready_tx));
    let _before_dump = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/statistics/handle/usage/beforeDumpStatsDeltaToKV",
        move |_| {
            if let Some(ready_tx) = ready_tx.lock().unwrap().take() {
                let _ = ready_tx.send(());
            }
        },
    );
    let (dump_tx, dump_done) = mpsc::channel();
    let dump_thread = {
        let factory = Arc::clone(&stack.factory);
        std::thread::spawn(move || {
            let _ = dump_tx.send(factory.dump_stats_delta_to_kv(true, &[lock_table_id]));
        })
    };
    ready
        .recv_timeout(Duration::from_secs(10))
        .expect("the first dump initializes its deltas");
    std::thread::sleep(Duration::from_millis(700));
    assert!(dump_done.try_recv().is_err(), "the lock blocks the first dump");

    // Before the first dump commits, the cache loads a t_other version newer
    // than its start_ts + 5 * lease.
    rows(&mut tk, "insert into t_other values (2)");
    stack.factory.dump_stats_delta_to_kv(true, &[other_table_id]).unwrap();
    tk.update_stats_cache().unwrap();

    rows(&mut tk_lock, "rollback");
    dump_done
        .recv_timeout(Duration::from_secs(10))
        .expect("the first dump finishes after the lock is released")
        .unwrap();
    dump_thread.join().unwrap();

    tk.update_stats_cache().unwrap();
    let lock_count: i64 = displayed(rows(
        &mut tk,
        &format!("select modify_count from mysql.stats_meta where table_id = {lock_table_id}"),
    ))[0][0]
        .parse()
        .unwrap();
    let cached = stack
        .factory
        .stats
        .load()
        .get(&lock_table_id)
        .and_then(tidb_stats_handle_syncload::stats_watch::TableStatsState::loaded)
        .expect("t_lock's statistics are cached")
        .hist_coll
        .modify_count;
    assert_eq!(cached, lock_count);
    // Go `UpdateStatsMetaVer` keeps last_stats_histograms_version, so the
    // cache does not reload the column and index stats.
    assert_eq!(hist_version(&mut tk), analyzed_hist_version);

    assert_eq!(
        displayed(rows(
            &mut tk,
            "select count(*) > 0 from information_schema.statements_summary \
             where digest_text like '%stats_meta%for update%' and max_latency >= 500000000",
        )),
        [["1"]]
    );
    rows(&mut tk, "set global tidb_stmt_summary_internal_query = 0");
}

/// Go `Handle.InitStatsLite` / `InitStats` (`bootstrap.go`) on the
/// statistics handle's sessions: `stats_meta` scopes the tables (a listed
/// subset, or every one), the lite load keeps every item's payload evicted,
/// and the full load fully loads indexes (histogram, TopN, buckets) while
/// columns stay metadata-only.
#[test]
fn init_stats_matches_go_table_scope_and_payload_shapes() {
    use tidb_stats_handle::bootstrap::InitialStatsLoad;
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(312)).unwrap();
    let mut ids = Vec::new();
    for table in ["boot_a", "boot_b"] {
        rows(&mut session, &format!("CREATE TABLE test.{table} (a INT, KEY idx_a (a))"));
        rows(
            &mut session,
            &format!(
                "INSERT INTO test.{table} VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)"
            ),
        );
        rows(&mut session, &format!("ANALYZE TABLE test.{table} WITH 0 TOPN"));
        ids.push(table_id_of(&mut session, table));
    }
    let a_column = 1;
    let index = 1;

    let one = init_stats_on(&stack, &ids[..1], InitialStatsLoad::Lite);
    assert_eq!(one.keys().copied().collect::<Vec<_>>(), ids[..1]);
    let lite = init_stats_on(&stack, &[], InitialStatsLoad::Lite);
    for id in &ids {
        let table = lite[id].loaded().expect("an analyzed table loads");
        assert!(table
            .hist_coll
            .get_column(a_column)
            .is_none_or(|column| !column.read().unwrap().stats_loaded_status.is_full_load()));
        assert!(table.hist_coll.get_index(index).is_none());
        let existence = table.existence_map.as_ref().unwrap().read().unwrap();
        assert!(existence.has_analyzed(a_column, false));
        assert!(existence.has_analyzed(index, true));
    }

    let full = init_stats_on(&stack, &[], InitialStatsLoad::IndexFull);
    for id in &ids {
        let table = full[id].loaded().expect("an analyzed table loads");
        let column = table.hist_coll.get_column(a_column).expect("column metadata loads");
        assert!(column.read().unwrap().stats_loaded_status.is_all_evicted());
        let index = table.hist_coll.get_index(index).expect("the index loads");
        let index = index.read().unwrap();
        assert!(index.stats_loaded_status.is_full_load());
        assert_eq!(index.histogram.buckets.len(), 10);
    }
}

/// Go `initStatsTopN4Chunk`: an index whose values all fit its TopN has no
/// buckets and is fully loaded at bootstrap; `isFullCache` stops the TopN
/// and bucket stages, leaving the index metadata-only.
#[test]
fn init_stats_loads_topn_only_indexes_and_stops_at_the_cache_quota_as_go() {
    use tidb_stats_handle::bootstrap::InitialStatsLoad;
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(313)).unwrap();
    rows(&mut session, "CREATE TABLE test.boot_topn (a INT, KEY idx_a (a))");
    rows(&mut session, "INSERT INTO test.boot_topn VALUES (1),(1),(1),(2),(2),(2)");
    rows(&mut session, "ANALYZE TABLE test.boot_topn");
    let id = table_id_of(&mut session, "boot_topn");
    let index = 1;

    let full = init_stats_on(&stack, &[], InitialStatsLoad::IndexFull);
    let table = full[&id].loaded().expect("the analyzed table loads");
    let loaded = table.hist_coll.get_index(index).expect("the index loads");
    let loaded = loaded.read().unwrap();
    assert!(loaded.stats_loaded_status.is_full_load());
    assert!(loaded.histogram.buckets.is_empty());
    assert_eq!(loaded.top_n.as_ref().map(tidb_stats::TopN::total_count), Some(6));
    drop(loaded);

    let quota = tidb_vardef::STATS_CACHE_MEM_QUOTA.swap(1, std::sync::atomic::Ordering::SeqCst);
    let limited = init_stats_on(&stack, &[], InitialStatsLoad::IndexFull);
    tidb_vardef::STATS_CACHE_MEM_QUOTA.store(quota, std::sync::atomic::Ordering::SeqCst);
    let table = limited[&id].loaded().expect("the analyzed table loads");
    let limited = table.hist_coll.get_index(index).expect("index metadata loads");
    let limited = limited.read().unwrap();
    assert!(!limited.stats_loaded_status.is_full_load());
    assert!(limited.top_n.is_none());
}

/// Go `storage.ReadColumnDistributionStats`: one column's metadata, TopN and
/// histogram read at one fresh snapshot on a statistics session, without the
/// statistics cache; a column with no histogram row reads as nothing.
#[test]
fn column_distribution_stats_read_on_a_statistics_session_as_go() {
    use tidb_stats_handle_storage::stats_read_writer::StatsSessionPool;
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(312)).unwrap();
    rows(&mut session, "CREATE TABLE test.distribution (id INT PRIMARY KEY, v INT, w INT)");
    rows(&mut session, "INSERT INTO test.distribution VALUES (1, 1, 1), (2, 2, 1), (3, 3, 1)");
    rows(&mut session, "ANALYZE TABLE test.distribution COLUMNS id, v");
    let catalog = stack.factory.catalog.get_latest();
    let (_, table) = catalog
        .table_info_by_name("test", "distribution")
        .expect("the table is in the catalog");
    let (table_id, v_info, w_info) = {
        let table = table.read();
        let column = |name: &str| {
            table
                .columns
                .iter_deref()
                .find(|column| column.read().name.original() == name)
                .expect("the column exists")
        };
        (table.id, column("v"), column("w"))
    };
    let pool = stack.factory.advanced_sys_session_pool();
    let stats_pool = tidb_domain_core::sys_session::StatsPool {
        session_pool: Some(&pool),
    };
    let read = |info: &tidb_stats::column::ColumnInfo| {
        let mut column = None;
        stats_pool
            .call_with_sctx(
                &mut |sctx| {
                    column = tidb_stats_handle_storage::read::read_column_distribution_stats(
                        sctx, table_id, info,
                    )?;
                    Ok(())
                },
                &[],
            )
            .expect("the read runs on a statistics session");
        column
    };
    let column = read(&v_info).expect("the analyzed column has a distribution");
    assert_eq!(column.physical_id, table_id);
    assert_eq!(column.stats_version, 2);
    assert!(column.stats_loaded_status.is_full_load());
    assert_eq!(column.top_n.as_ref().map(|top_n| top_n.num()), Some(3));
    assert_eq!(column.histogram.ndv, 3);
    assert_eq!(column.histogram.null_count, 0);
    assert!(read(&w_info).is_none(), "an unanalyzed column has no distribution");
}

/// `EXPLAIN FOR CONNECTION` renders the target's last plan with the runtime
/// stats its execution recorded (Go `buildExplainFor` hands the target
/// process's `RuntimeStatsColl` to `buildExplainPlan`) and the memory its
/// operators charged to the target statement's trackers (`p.SCtx()`), not
/// the explaining statement's. Captured from a Go tidb-server over the wire
/// (durations masked):
///
/// ```text
/// Update_3      estRows 0.00  actRows 0  time:D, open:D, close:D, loops:1
/// └─Point_Get_1 estRows 1.00  actRows 1  time:D, open:D, close:D, loops:2, Get:{num_rpc:1, total_time:D}
/// TableReader_7     actRows 1  ..., cop_task: {num: 1, ...}  272 Bytes
/// └─Selection_6     actRows 1  cop[tikv]
///   └─TableFullScan_5 actRows 3  cop[tikv]
/// ```
#[test]
fn explain_for_connection_renders_the_targets_runtime_stats_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1144)).unwrap();
    for sql in [
        "USE test",
        "create table t (id int primary key, uk int, b int, unique key(uk))",
        "insert into t values (1,10,0),(2,20,0),(3,30,0)",
    ] {
        session.node_conn().execute_write(sql).unwrap();
    }
    rows(&mut session, "set @b=5, @i=1");
    let id = displayed(rows(&mut session, "select connection_id()"))[0][0].clone();
    let mut target = stack.factory.open_session(session_context(1145)).unwrap();
    let explain_target = |session: &mut crate::Session| -> Vec<Vec<String>> {
        displayed(rows(session, &format!("explain for connection {id}")))
            .into_iter()
            .map(|row| row.iter().map(|cell| mask_durations(cell)).collect())
            .collect()
    };
    rows(&mut session, "prepare s1 from 'update t set b=? where id=?'");
    rows(&mut session, "execute s1 using @b, @i");
    rows(&mut session, "execute s1 using @b, @i");
    let plan = explain_target(&mut target);
    assert_eq!(plan.len(), 2, "{plan:?}");
    // An `EXECUTE`'s plan is Go's `Execute` plan, whose brief binary plan
    // `EXPLAIN FOR CONNECTION` decodes (`shouldDecodeBriefBinaryPlan`): the
    // write root's estRows is the encoded `0.00`, not the live `N/A`.
    assert_eq!(
        [&plan[0][0], &plan[0][1], &plan[0][2], &plan[0][5]],
        ["Update_3", "0.00", "0", "time:D, open:D, close:D, loops:1"]
    );
    assert_eq!(
        [&plan[1][0], &plan[1][2], &plan[1][5]],
        [
            "└─Point_Get_1",
            "1",
            "time:D, open:D, close:D, loops:2, Get:{num_rpc:1, total_time:D}"
        ]
    );
    // The connection explaining itself renders its previous statement's
    // plan, the same decoded `Execute` plan.
    let own = explain_target(&mut session);
    assert_eq!([&own[0][0], &own[0][1]], ["Update_3", "0.00"], "{own:?}");
    rows(&mut session, "execute s1 using @b, @i");
    rows(&mut session, "select * from t where b > 0");
    let plan = explain_target(&mut target);
    let shape: Vec<(&str, &str, &str)> = plan
        .iter()
        .map(|row| (row[0].as_str(), row[2].as_str(), row[3].as_str()))
        .collect();
    assert_eq!(
        shape,
        [
            ("TableReader_7", "1", "root"),
            ("└─Selection_6", "1", "cop[tikv]"),
            ("  └─TableFullScan_5", "3", "cop[tikv]"),
        ]
    );
    assert!(
        plan[0][5].starts_with("time:D, open:D, close:D, loops:2, cop_task: {num: 1, "),
        "{}",
        plan[0][5]
    );
    // A reader's memory is the bytes of its coprocessor responses, which
    // carry the store's execution times as varints (one byte longer once a
    // duration crosses a power of 128): Go's recorded 272 bytes, give or take
    // those bytes -- in Go too.
    let reader_bytes: i64 = plan[0][7]
        .strip_suffix(" Bytes")
        .and_then(|bytes| bytes.parse().ok())
        .unwrap_or_else(|| panic!("{}", plan[0][7]));
    assert!((reader_bytes - 272).abs() <= 24, "{} vs Go 272 Bytes", plan[0][7]);
}

/// An autocommit `EXPLAIN ANALYZE` write commits before its rows render, as
/// Go's `runStmt` commits an `IsExplainAnalyzeDML` statement before its
/// `ExplainExec` record set is read: its RU stats count the prewrite and the
/// commit. Captured from a Go tidb-server over the wire:
/// `explain analyze update t set b=b+1 where id=2` reports `RU:3.94`,
/// `insert into t values (9,90,9)` `RU:3.54` and `delete from t where id=9`
/// `RU:3.99`, every run. Fresh nextgen Go capture:
/// `zzprobe/o15_named_sql.TestNamedRequestUnits` with optimistic transactions.
#[test]
fn an_analyzed_autocommit_write_renders_after_its_commit_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1150)).unwrap();
    for sql in [
        "USE test",
        "SET tidb_txn_mode = optimistic",
        "create table t (id int primary key, uk int, b int, unique key(uk))",
        "insert into t values (1,10,0),(2,20,0),(3,30,0)",
    ] {
        session.node_conn().execute_write(sql).unwrap();
    }
    for _ in 0..2 {
        let plan = displayed(rows(&mut session, "explain analyze update t set b=b+1 where id=2"));
        assert_eq!(plan[0][0], "Update_3");
        assert!(plan[0][5].ends_with(", RU:3.94"), "{}", plan[0][5]);
    }
    assert_eq!(displayed(rows(&mut session, "select b from t where id=2")), [["2"]]);
    // The row and unique-index keys share a region, so the primary's commit
    // request carries both (client-go `batched.primaryBatch()`): one commit
    // RPC, as Go's `RU:3.54` / `RU:3.99` count.
    for k in 0..2 {
        let insert = format!("explain analyze insert into t values ({}, {}, 9)", 9 + k, 90 + k);
        let plan = displayed(rows(&mut session, &insert));
        assert!(plan[0][5].ends_with(", RU:3.54"), "{}", plan[0][5]);
        let plan = displayed(rows(&mut session, &format!("explain analyze delete from t where id={}", 9 + k)));
        assert!(plan[0][5].ends_with(", RU:3.99"), "{}", plan[0][5]);
    }
    // The statement finishes after its commit (Go `FinishExecuteStmt` at the
    // `ExplainExec` record set's close), so the summary counts the write's
    // request units: Go records 0.475 read and 3.4693 write.
    assert_eq!(
        displayed(rows(
            &mut session,
            "select round(avg_request_unit_read, 4), round(avg_request_unit_write, 4) \
             from information_schema.statements_summary \
             where digest_text = 'explain analyze update `t` set `b` = `b` + ? where `id` = ?'"
        )),
        [["0.475", "3.4693"]]
    );
}

/// A write's root carries its transaction's commit statistics: Go's
/// `FinishExecuteStmt` registers `RuntimeStatsWithCommit` on the plan's root
/// in the statement's `RuntimeStatsColl`, before the connection goes to sleep
/// and re-encodes the brief plan an `EXECUTE`'s target decodes. Captured from a
/// Go tidb-server over the wire (durations and region ids masked):
///
/// ```text
/// update t set b=5 where id=1
///   Update_3  time:D, open:D, close:D, loops:1, commit_txn: {prewrite:D, get_commit_ts:D, commit:D,
///             slowest_prewrite_rpc: {total: D, region_id: R, store: store1, },
///             commit_primary_rpc: {total: D, region_id: R, store: store1, }, region_num:1, write_keys:1, write_byte:33}
/// execute s4 using @lo  (update t set b=b+1 where b > ?)
///   Update_4  0.00 estRows, the same commit_txn
/// ```
#[test]
fn a_write_root_carries_its_commit_stats_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1147)).unwrap();
    for sql in [
        "USE test",
        "create table t (id int primary key, uk int, b int, unique key(uk))",
        "insert into t values (1,10,0),(2,20,0),(3,30,0)",
    ] {
        session.node_conn().execute_write(sql).unwrap();
    }
    rows(&mut session, "set @lo=0");
    let id = displayed(rows(&mut session, "select connection_id()"))[0][0].clone();
    let mut target = stack.factory.open_session(session_context(1148)).unwrap();
    let root = |target: &mut crate::Session| -> Vec<String> {
        let plan = displayed(rows(target, &format!("explain for connection {id}")));
        plan[0]
            .iter()
            .map(|cell| {
                let masked = mask_durations(cell);
                let mut out = String::new();
                let mut rest = masked.as_str();
                while let Some(at) = rest.find("region_id: ") {
                    out.push_str(&rest[..at + "region_id: ".len()]);
                    rest = rest[at + "region_id: ".len()..].trim_start_matches(|c: char| c.is_ascii_digit());
                    out.push('R');
                }
                out.push_str(rest);
                out
            })
            .collect()
    };
    let commit = "commit_txn: {prewrite:D, get_commit_ts:D, commit:D, \
                  slowest_prewrite_rpc: {total: D, region_id: R, store: store1, }, \
                  commit_primary_rpc: {total: D, region_id: R, store: store1, }, \
                  region_num:1, write_keys:1, write_byte:33}";
    rows(&mut session, "update t set b=5 where id=1");
    let plan = root(&mut target);
    assert_eq!(plan[0], "Update_3");
    assert_eq!(plan[5], format!("time:D, open:D, close:D, loops:1, {commit}"));
    rows(&mut session, "prepare s4 from 'update t set b=b+1 where b > ?'");
    for _ in 0..3 {
        rows(&mut session, "execute s4 using @lo");
    }
    let plan = root(&mut target);
    assert_eq!([&plan[0], &plan[1]], ["Update_4", "0.00"]);
    assert_eq!(plan[5], format!("time:D, open:D, close:D, loops:1, {commit}"));
}

/// The statement summary's plan and binary plan are rendered after the
/// statement ran (Go `getEncodedPlan`'s `EncodeFlatPlan` and `getBinaryPlan`'s
/// `BinaryPlanStrFromFlatPlan(flat, false)`), so each operator carries its
/// runtime info and the binary plan its ver2 cost and no brief fields.
/// Captured from a Go tidb-server over the wire (durations masked):
///
/// ```text
/// | TableReader_7       | 3333.33  | 332760.00  | 1 | root      |         | time:D, ..., cop_task: {num: 1, ... | data:Selection_6               | 256 Bytes | N/A |
/// | └─Selection_6       | 3333.33  | 4569000.00 | 1 | cop[tikv] |         | tikv_task:{time:D, loops:0}, ...    | gt(test.t.b, 0)                | N/A       | N/A |
/// |   └─TableFullScan_5 | 10000.00 | 4070000.00 | 3 | cop[tikv] | table:t | tikv_task:{time:D, loops:0}         | keep order:false, stats:pseudo | N/A       | N/A |
/// ```
#[test]
fn the_summary_plans_carry_the_statements_runtime_stats_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1146)).unwrap();
    for sql in [
        "USE test",
        "create table t (id int primary key, b int)",
        "insert into t values (1,0),(2,5),(3,0)",
    ] {
        session.node_conn().execute_write(sql).unwrap();
    }
    rows(&mut session, "select /* e21 */ * from t where b > 0");
    let decoded = |session: &mut crate::Session, column: &str, separator: char| -> Vec<Vec<String>> {
        let sql = format!(
            "select {column} from information_schema.statements_summary \
             where query_sample_text like 'select /* e21 */%'"
        );
        let text = displayed(rows(session, &sql))[0][0].clone();
        text.lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                line.trim()
                    .trim_matches('|')
                    .split(separator)
                    .map(|cell| mask_durations(cell.trim()))
                    .collect()
            })
            .collect()
    };
    let binary = decoded(&mut session, "tidb_decode_binary_plan(binary_plan)", '|');
    assert_eq!(
        binary[0],
        ["id", "estRows", "estCost", "actRows", "task", "access object", "execution info", "operator info", "memory", "disk"]
    );
    let cells = |row: &[String], picks: &[usize]| -> Vec<String> {
        picks.iter().map(|&i| row[i].clone()).collect()
    };
    let columns = [0, 1, 2, 3, 4, 5, 7, 8, 9];
    assert_eq!(
        cells(&binary[1], &columns),
        ["TableReader_7", "3333.33", "332760.00", "1", "root", "", "data:Selection_6", "256 Bytes", "N/A"]
    );
    assert!(
        binary[1][6].starts_with("time:D, open:D, close:D, loops:2, cop_task: {num: 1, "),
        "{}",
        binary[1][6]
    );
    assert_eq!(
        cells(&binary[2], &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
        [
            "└─Selection_6",
            "3333.33",
            "4569000.00",
            "1",
            "cop[tikv]",
            "",
            "tikv_task:{time:D, loops:0}, scan_detail: {total_process_keys: 3, rocksdb: {block: {}}}",
            "gt(test.t.b, 0)",
            "N/A",
            "N/A",
        ]
    );
    assert_eq!(
        cells(&binary[3], &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
        [
            "└─TableFullScan_5",
            "10000.00",
            "4070000.00",
            "3",
            "cop[tikv]",
            "table:t",
            "tikv_task:{time:D, loops:0}",
            "keep order:false, stats:pseudo",
            "N/A",
            "N/A",
        ]
    );
    let encoded = decoded(&mut session, "tidb_decode_plan(plan)", '\t');
    let shape: Vec<Vec<String>> = encoded
        .iter()
        .map(|row| cells(row, &[0, 1, 2, 3, 4, 6, 7]))
        .collect();
    assert_eq!(
        shape,
        [
            ["id", "task", "estRows", "operator info", "actRows", "memory", "disk"],
            ["TableReader_7", "root", "3333.33", "data:Selection_6", "1", "256 Bytes", "N/A"],
            ["└─Selection_6", "cop[tikv]", "3333.33", "gt(test.t.b, 0)", "1", "N/A", "N/A"],
            ["└─TableFullScan_5", "cop[tikv]", "10000", "table:t, keep order:false, stats:pseudo", "3", "N/A", "N/A"],
        ]
    );
    assert_eq!(encoded[3][5], "tikv_task:{time:D, loops:0}");
}

/// Go `TestInsertColStats2KVDoesNotDeadlockWithAnalyzeSave` (f01fb0ab6d4):
/// the add-column statistics write locks `mysql.stats_meta` before its
/// histogram row, the order ANALYZE's save takes, so a concurrent save
/// waits for it instead of deadlocking.
#[test]
fn insert_col_stats_2_kv_does_not_deadlock_with_analyze_save() {
    use std::sync::mpsc;
    use std::time::{Duration, Instant};
    let (stack, _users) = cop_backed_stack();
    let mut tk = stack.factory.open_session(session_context(209)).unwrap();
    rows(&mut tk, "create table test.t (a int)");
    rows(&mut tk, "insert into test.t values (1), (2)");
    rows(&mut tk, "analyze table test.t");
    let table_id = table_id_of(&mut tk, "t");
    assert_eq!(
        displayed(rows(&mut tk, &format!("select count from mysql.stats_meta where table_id = {table_id}"))),
        [["2"]]
    );

    const COL_ID: i64 = 100;
    let mut field_type = tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Varchar);
    field_type.set_flen(200);
    let col = tidb_model::ColumnInfo {
        id: COL_ID,
        name: tidb_ast::CiString::new("filler1"),
        field_type,
        state: tidb_model::SchemaState::PUBLIC,
        ..tidb_model::ColumnInfo::default()
    };

    let (entered_tx, entered) = mpsc::channel::<()>();
    let (release_tx, release) = mpsc::channel::<()>();
    let release = std::sync::Mutex::new(release);
    let entered_tx = std::sync::Mutex::new(entered_tx);
    let _pause = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/statistics/handle/storage/pauseBeforeInsertColStatsMetaUpdate",
        move |_| {
            let _ = entered_tx.lock().unwrap().send(());
            let _ = release.lock().unwrap().recv();
        },
    );

    let pool = stack.factory.advanced_sys_session_pool();
    let (col_stats_tx, col_stats) = mpsc::channel();
    let col_stats_thread = {
        let pool = Arc::clone(&pool);
        std::thread::spawn(move || {
            let result = tidb_stats_handle_util::call_with_sctx(
                pool.as_ref(),
                |sctx| {
                    tidb_stats_handle_storage::save::insert_col_stats_2_kv(sctx, "t", table_id, &[col])
                },
                &[tidb_stats_handle_util::FLAG_WRAP_TXN],
            );
            let _ = col_stats_tx.send(result.map_err(|error| error.to_string()));
        })
    };
    if entered.recv_timeout(Duration::from_secs(20)).is_err() {
        panic!(
            "InsertColStats2KV did not reach the stats_meta update: {:?}",
            col_stats.try_recv()
        );
    }

    let mut tk_a = stack.factory.open_session(session_context(210)).unwrap();
    rows(&mut tk_a, "set innodb_lock_wait_timeout = 15");
    let (analyze_tx, analyze_done) = mpsc::channel();
    let analyze_thread = std::thread::spawn(move || {
        // Go `runAnalyzeStatsSave`: SaveAnalyzeResultToStorage's lock order.
        let mut run = || -> Result<(), String> {
            let exec = |session: &mut Session, sql: &str| session.run(sql).map(drop).map_err(|error| format!("{sql}: {error:?}"));
            exec(&mut tk_a, "begin pessimistic")?;
            let body = |session: &mut Session| -> Result<(), String> {
                exec(
                    session,
                    &format!("select count from mysql.stats_meta where table_id in ({}, {table_id}) for update", -table_id),
                )?;
                exec(
                    session,
                    &format!(
                        "replace into mysql.stats_histograms (table_id, is_index, hist_id, distinct_count, version, null_count) values ({table_id}, 0, {COL_ID}, 1, 1, 0)"
                    ),
                )
            };
            if let Err(error) = body(&mut tk_a) {
                let _ = tk_a.run("rollback");
                return Err(error);
            }
            exec(&mut tk_a, "commit")
        };
        let _ = analyze_tx.send(run());
    });

    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let waiting = rows(&mut tk, "select state from information_schema.tidb_trx where state = 'LockWaiting'");
        if !waiting.is_empty() {
            break;
        }
        assert!(Instant::now() < deadline, "the analyze save never waited for the stats_meta lock");
        std::thread::sleep(Duration::from_millis(20));
    }
    release_tx.send(()).unwrap();

    let analyze = analyze_done
        .recv_timeout(Duration::from_secs(20))
        .expect("analyze stats save did not finish");
    assert_eq!(analyze, Ok(()));
    let col_stats_result = col_stats
        .recv_timeout(Duration::from_secs(20))
        .expect("InsertColStats2KV did not finish");
    assert_eq!(col_stats_result, Ok(()));
    analyze_thread.join().unwrap();
    col_stats_thread.join().unwrap();

    // The analyze save runs after the add-column stats transaction commits,
    // so its histogram replace is the row that remains.
    assert_eq!(
        displayed(rows(
            &mut tk,
            &format!("select distinct_count, null_count from mysql.stats_histograms where table_id = {table_id} and is_index = 0 and hist_id = {COL_ID}"),
        )),
        [["1", "0"]]
    );
}
