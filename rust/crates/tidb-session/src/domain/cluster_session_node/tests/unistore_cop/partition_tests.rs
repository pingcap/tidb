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

/// Pinned Go `checkExchangePartitionRecordValidation` and
/// `onExchangeTablePartition`: the default `WITH VALIDATION` reads the
/// standalone rows before publication, rejects 1737 atomically when one does
/// not route to the named partition, and otherwise makes the two old physical
/// record sets visible under their exchanged metadata IDs.
#[test]
fn exchange_partition_validates_and_swaps_real_rows_atomically() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(152))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE TABLE exchange_pt (id BIGINT PRIMARY KEY CLUSTERED) \
         PARTITION BY RANGE (id) (\
           PARTITION p0 VALUES LESS THAN (10),\
           PARTITION p1 VALUES LESS THAN (MAXVALUE))",
    );
    rows(
        &mut session,
        "CREATE TABLE exchange_nt (id BIGINT PRIMARY KEY CLUSTERED)",
    );
    rows(&mut session, "INSERT INTO exchange_pt VALUES (1)");
    rows(&mut session, "INSERT INTO exchange_nt VALUES (5)");
    rows(
        &mut session,
        "ALTER TABLE exchange_pt EXCHANGE PARTITION p0 WITH TABLE exchange_nt",
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT id FROM exchange_pt ORDER BY id")),
        [["5"]]
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT id FROM exchange_nt ORDER BY id")),
        [["1"]]
    );

    rows(
        &mut session,
        "CREATE TABLE exchange_bad_pt (id BIGINT PRIMARY KEY CLUSTERED) \
         PARTITION BY RANGE (id) (\
           PARTITION p0 VALUES LESS THAN (10),\
           PARTITION p1 VALUES LESS THAN (MAXVALUE))",
    );
    rows(
        &mut session,
        "CREATE TABLE exchange_bad_nt (id BIGINT PRIMARY KEY CLUSTERED)",
    );
    rows(&mut session, "INSERT INTO exchange_bad_pt VALUES (2)");
    rows(&mut session, "INSERT INTO exchange_bad_nt VALUES (15)");
    let error = match session
        .node_conn()
        .execute("ALTER TABLE exchange_bad_pt EXCHANGE PARTITION p0 WITH TABLE exchange_bad_nt")
    {
        Err(error) => error,
        Ok(_) => panic!("a row for p1 cannot be exchanged into p0"),
    };
    assert_eq!(error.code, 1737);
    assert_eq!(error.state, *b"HY000");
    assert_eq!(
        error.message,
        "Found a row that does not match the partition"
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT id FROM exchange_bad_pt ORDER BY id"
        )),
        [["2"]],
        "the failed DDL leaves partition metadata and physical rows unchanged"
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT id FROM exchange_bad_nt ORDER BY id"
        )),
        [["15"]]
    );
}

/// Pinned `TestExchangeAPartition`: the subscriber reads the exchanged
/// partition and standalone-table metadata and applies Go's count and modify
/// deltas to the logical/global row.
#[test]
fn exchange_partition_event_updates_global_statistics_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(83))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "SET GLOBAL tidb_partition_prune_mode = 'dynamic'",
    );
    rows(
        &mut session,
        "CREATE TABLE stats_exchange_pt (a INT PRIMARY KEY, b INT, INDEX idx(b)) \
         PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (6), \
         PARTITION p1 VALUES LESS THAN (11), PARTITION p2 VALUES LESS THAN (16), \
         PARTITION p3 VALUES LESS THAN (21))",
    );
    rows(
        &mut session,
        "INSERT INTO stats_exchange_pt VALUES (1,2),(2,2),(6,2),(11,2),(16,2)",
    );
    rows(&mut session, "ANALYZE TABLE stats_exchange_pt");
    rows(
        &mut session,
        "CREATE TABLE stats_exchange_nt (a INT PRIMARY KEY, b INT, INDEX idx(b))",
    );
    rows(
        &mut session,
        "INSERT INTO stats_exchange_nt VALUES (1,2),(2,2),(3,2),(4,2),(5,2)",
    );
    rows(&mut session, "ANALYZE TABLE stats_exchange_nt");

    let (partitioned, partition_id, standalone) = {
        let catalog = stack.factory.catalog.get_latest();
        let partitioned = catalog
            .table_info_by_name("test", "stats_exchange_pt")
            .expect("partitioned table is published")
            .1
            .read().clone_like_go();
        let partition_id = partitioned
            .partition
            .as_ref()
            .expect("partition metadata exists")
            .read()
            .definitions
            .snapshot()
            .into_iter()
            .find(|definition| definition.name.lowercase() == "p0")
            .expect("p0 exists")
            .id;
        let standalone = catalog
            .table_info_by_name("test", "stats_exchange_nt")
            .expect("standalone table is published")
            .1
            .read().clone_like_go();
        (partitioned, partition_id, standalone)
    };
    let logical_id = partitioned.id;
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT count, modify_count FROM mysql.stats_meta WHERE table_id = {logical_id}"
            ),
        )),
        [["5", "0"]]
    );
    handle_stats_schema_change(
        &mut session,
        &SchemaChangeEvent::exchange_partition(
            partitioned,
            partition_payload(&[partition_id]),
            standalone,
        ),
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT count, modify_count FROM mysql.stats_meta WHERE table_id = {logical_id}"
            ),
        )),
        [["8", "7"]]
    );
}

/// Pinned `lockstats.TestRemoveLockedTables`: the live cluster path wraps the
/// complete operation in Go's independent pessimistic transaction and merges
/// a partition lock delta into both physical and logical metadata.
#[test]
fn stats_lock_live_pessimistic_transaction_merges_partition_delta_like_go() {
    let (stack, _users) = cop_backed_stack();
    let factory = stack.factory;
    let mut session = factory
        .open_session(session_context(85))
        .expect("client session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE TABLE lock_live (a INT) PARTITION BY RANGE (a) (\
         PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN MAXVALUE)",
    );
    rows(
        &mut session,
        "INSERT INTO lock_live VALUES (1),(2),(11),(12)",
    );
    rows(&mut session, "ANALYZE TABLE lock_live");
    let catalog = factory.catalog.get_latest();
    let table = catalog
        .table_info_by_name("test", "lock_live")
        .expect("lock table is published")
        .1;
    let table = table.read();
    let table_id = table.id;
    let partition = table.get_partition_info().expect("partition metadata");
    let partition = partition.read();
    let partition_ids = partition
        .definitions
        .map_visible(|definition| definition.id);
    let p0_id = partition_ids[0];
    let p1_id = partition_ids[1];
    drop(partition);
    drop(catalog);

    rows(&mut session, "LOCK STATS lock_live");
    rows(
        &mut session,
        &format!(
            "UPDATE mysql.stats_table_locked SET count = -1, modify_count = 1 \
             WHERE table_id = {p0_id}"
        ),
    );
    rows(&mut session, "UNLOCK STATS lock_live");

    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT table_id, count, modify_count FROM mysql.stats_meta \
                 WHERE table_id IN ({table_id},{p0_id},{p1_id}) ORDER BY table_id"
            ),
        )),
        [
            [table_id.to_string(), "3".to_owned(), "1".to_owned()],
            [p0_id.to_string(), "1".to_owned(), "1".to_owned()],
            [p1_id.to_string(), "2".to_owned(), "0".to_owned()],
        ]
    );
}

/// Pinned planner `TestAutoAnalyzeForMissingPartition`: under dynamic pruning
/// and `tidb_skip_missing_partition_stats`, auto analyze fills the physical
/// partitions that ordinary partition ANALYZE intentionally left missing.
#[test]
fn auto_analyze_fills_missing_partition_statistics_like_go() {
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
        .wait_auto_analyze_finished_for_test();

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

/// Pinned `pkg/statistics/handle/ddl.TestTruncateAPartitionedTable`: whole
/// table truncation gives every partition a fresh physical ID, initializes
/// each replacement stats row, and advances every retired partition version.
#[test]
fn truncate_partitioned_table_statistics_match_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
    drain_stats_ddl_events(&stack.factory, &mut session);
    rows(
        &mut session,
        "INSERT INTO stats_truncate_partitioned VALUES (1,2),(2,2),(6,2)",
    );
    rows(&mut session, "ANALYZE TABLE stats_truncate_partitioned");
    let old_ids = partition_id_map(
        &stack.factory.catalog.get_latest(),
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
    drain_stats_ddl_events(&stack.factory, &mut session);
    let new_ids = partition_id_map(
        &stack.factory.catalog.get_latest(),
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
    drain_stats_ddl_events(&stack.factory, &mut session);
    rows(
        &mut session,
        "INSERT INTO stats_truncate_hash VALUES (1,2),(2,2),(6,2),(11,2),(16,2)",
    );
    rows(&mut session, "ANALYZE TABLE stats_truncate_hash");
    let (logical_id, partitions) =
        partition_id_map(&stack.factory.catalog.get_latest(), "test", "stats_truncate_hash")
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
    drain_stats_ddl_events(&stack.factory, &mut session);
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT count, modify_count FROM mysql.stats_meta WHERE table_id = {logical_id}"
            ),
        )),
        [["4", "1"]]
    );
    let new_p0 = partition_id_map(&stack.factory.catalog.get_latest(), "test", "stats_truncate_hash")
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        drain_stats_ddl_events(&stack.factory, &mut session);
        let (logical_id, original) =
            partition_id_map(&stack.factory.catalog.get_latest(), "test", &table_name)
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
        drain_stats_ddl_events(&stack.factory, &mut session);
        let new_id = partition_id_map(&stack.factory.catalog.get_latest(), "test", &table_name)
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
    drain_stats_ddl_events(&stack.factory, &mut session);
    rows(
        &mut session,
        "INSERT INTO stats_drop_partitions VALUES (1,2),(2,2),(6,2),(11,2),(16,2)",
    );
    rows(&mut session, "ANALYZE TABLE stats_drop_partitions");
    let (logical_id, partitions) = partition_id_map(
        &stack.factory.catalog.get_latest(),
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
    drain_stats_ddl_events(&stack.factory, &mut session);
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

/// Pinned `TestReorgPartitions`, `TestIncreasePartitionCountOfHashPartitionTable`,
/// and `TestDecreasePartitionCountOfHashPartitionTable`: the subscriber does
/// not alter global count during a reorganization, initializes every added
/// physical ID, and advances every dropped physical ID for delayed GC.
#[test]
fn reorganize_partition_event_updates_statistics_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(80))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "SET GLOBAL tidb_partition_prune_mode = 'dynamic'",
    );
    rows(
        &mut session,
        "CREATE TABLE stats_reorganize_event (a INT PRIMARY KEY, b INT, INDEX idx(b)) \
         PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (6), \
         PARTITION p1 VALUES LESS THAN (11))",
    );
    rows(
        &mut session,
        "INSERT INTO stats_reorganize_event VALUES (1,2),(2,2),(6,2)",
    );
    rows(&mut session, "ANALYZE TABLE stats_reorganize_event");

    let table = stack
        .factory
        .catalog
        .get_latest()
        .table_info_by_name("test", "stats_reorganize_event")
        .expect("partitioned table is published")
        .1
        .read().clone_like_go();
    let logical_id = table.id;
    let dropped_id = table
        .partition
        .as_ref()
        .expect("partition metadata exists")
        .read()
        .definitions
        .snapshot()[0]
        .id;
    let dropped_version = displayed(rows(
        &mut session,
        &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {dropped_id}"),
    ))[0][0]
        .parse::<u64>()
        .expect("partition version is unsigned");
    let logical_before = displayed(rows(
        &mut session,
        &format!("SELECT count, modify_count FROM mysql.stats_meta WHERE table_id = {logical_id}"),
    ));
    let added_id = 9_100_000_001_i64;
    let event = SchemaChangeEvent::reorganize_partitions(
        table,
        partition_payload(&[added_id]),
        partition_payload(&[dropped_id]),
    );
    handle_stats_schema_change(&mut session, &event);

    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT modify_count, count FROM mysql.stats_meta WHERE table_id = {added_id}"
            ),
        )),
        [["0", "0"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!("SELECT count(*) FROM mysql.stats_histograms WHERE table_id = {added_id}"),
        )),
        [["3"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT count, modify_count FROM mysql.stats_meta WHERE table_id = {logical_id}"
            ),
        )),
        logical_before
    );
    let retired_version = displayed(rows(
        &mut session,
        &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {dropped_id}"),
    ))[0][0]
        .parse::<u64>()
        .expect("retired partition version is unsigned");
    assert!(retired_version > dropped_version);
}

/// Pinned `TestAddPartitioning` and `TestRemovePartitioning`: newly added
/// partition placeholders are written before the six global-statistics table
/// IDs move, and removal moves those same tables before retiring partitions.
#[test]
fn add_and_remove_partitioning_events_move_statistics_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(81))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "CREATE TABLE stats_partitioning_event \
         (a INT PRIMARY KEY, b INT, INDEX idx(b))",
    );
    rows(
        &mut session,
        "INSERT INTO stats_partitioning_event VALUES (1,2),(2,2),(6,2),(11,2),(16,2)",
    );
    rows(&mut session, "ANALYZE TABLE stats_partitioning_event");
    let source = stack
        .factory
        .catalog
        .get_latest()
        .table_info_by_name("test", "stats_partitioning_event")
        .expect("table is published")
        .1
        .read().clone_like_go();
    let old_table_id = source.id;
    let global_table_id = 9_200_000_001_i64;
    let partition_ids = [9_200_000_002_i64, 9_200_000_003_i64];
    let mut partitioned = source.clone_like_go();
    partitioned.id = global_table_id;
    let added = partition_payload(&partition_ids);
    handle_stats_schema_change(
        &mut session,
        &SchemaChangeEvent::new_add_partitioning_event(old_table_id, partitioned, added.clone_like_go()),
    );

    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT count, modify_count FROM mysql.stats_meta WHERE table_id = {global_table_id}"
            ),
        )),
        [["5", "0"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!("SELECT count(*) FROM mysql.stats_meta WHERE table_id = {old_table_id}"),
        )),
        [["0"]]
    );
    let initial_partition_versions = partition_ids.map(|partition_id| {
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!(
                    "SELECT count(*) FROM mysql.stats_histograms WHERE table_id = {partition_id}"
                ),
            )),
            [["3"]]
        );
        displayed(rows(
            &mut session,
            &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {partition_id}"),
        ))[0][0]
            .parse::<u64>()
            .expect("partition version is unsigned")
    });

    let single_table_id = 9_200_000_004_i64;
    let mut single = source;
    single.id = single_table_id;
    handle_stats_schema_change(
        &mut session,
        &SchemaChangeEvent::remove_partitioning(global_table_id, single, added),
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT count, modify_count FROM mysql.stats_meta WHERE table_id = {single_table_id}"
            ),
        )),
        [["5", "0"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!("SELECT count(*) FROM mysql.stats_meta WHERE table_id = {global_table_id}"),
        )),
        [["0"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            &format!(
                "SELECT count(*) FROM mysql.stats_histograms WHERE table_id = {single_table_id}"
            ),
        )),
        [["3"]]
    );
    for (partition_id, old_version) in partition_ids.into_iter().zip(initial_partition_versions) {
        let retired_version = displayed(rows(
            &mut session,
            &format!("SELECT version FROM mysql.stats_meta WHERE table_id = {partition_id}"),
        ))[0][0]
            .parse::<u64>()
            .expect("retired partition version is unsigned");
        assert!(retired_version > old_version);
    }
}

/// Go creates one pending job for every physical partition task before
/// dispatch, then creates independent global-merge jobs after all partition
/// results have been saved. Global jobs never accumulate processed rows.
#[test]
fn partition_analyze_jobs_match_go_task_shapes() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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

/// Go's global-stats merge (`AsyncMergePartitionStats2GlobalStats`; Go
/// master removed the blocking one, so `tidb_enable_async_merge_global_stats
/// = OFF` changes nothing): a re-analyzed partition merges with the others'
/// stored rows. Expected rows from a Go node.
#[test]
fn global_stats_merge_reads_partitions_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let mut session = stack
        .factory
        .open_session(session_context(60))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "SET SESSION tidb_analyze_version = 2");
    rows(&mut session, "SET SESSION tidb_partition_prune_mode = 'dynamic'");
    rows(&mut session, "SET SESSION tidb_enable_async_merge_global_stats = OFF");
    rows(
        &mut session,
        "CREATE TABLE gb (a int, b int, KEY(a)) PARTITION BY RANGE (a) (\
         PARTITION p0 VALUES LESS THAN (10),\
         PARTITION p1 VALUES LESS THAN (20),\
         PARTITION p2 VALUES LESS THAN (30))",
    );
    rows(
        &mut session,
        "INSERT INTO gb VALUES (1,1),(2,1),(3,2),(11,2),(12,3),(21,3),(22,4),(23,4)",
    );
    let global_meta = "SELECT m.count, m.modify_count FROM mysql.stats_meta m \
         JOIN information_schema.tables t ON m.table_id = t.tidb_table_id \
         WHERE t.table_name = 'gb'";
    let global_histograms = "SELECT h.is_index, h.hist_id, h.distinct_count, h.null_count \
         FROM mysql.stats_histograms h JOIN information_schema.tables t \
         ON h.table_id = t.tidb_table_id WHERE t.table_name = 'gb' \
         ORDER BY h.is_index, h.hist_id";

    rows(&mut session, "ANALYZE TABLE gb");
    assert_eq!(displayed(rows(&mut session, global_meta)), [["8", "0"]]);
    assert_eq!(
        displayed(rows(&mut session, global_histograms)),
        [["0", "1", "8", "0"], ["0", "2", "4", "0"], ["1", "1", "8", "0"]]
    );

    rows(&mut session, "INSERT INTO gb VALUES (4,5),(5,6),(1,7)");
    rows(&mut session, "ANALYZE TABLE gb PARTITION p0");
    // The merged count is the partitions' stored counts (6 + 2 + 3) with no
    // modifications, read before any stats delta of the INSERT is flushed.
    assert_eq!(displayed(rows(&mut session, global_meta)), [["11", "0"]]);
    assert_eq!(
        displayed(rows(&mut session, global_histograms)),
        [["0", "1", "10", "0"], ["0", "2", "7", "0"], ["1", "1", "10", "0"]]
    );
}

/// Pinned `pkg/statistics/handle/handletest/handle_test.go::
/// TestSkipMissingPartitionStats`. Dynamic global merge keeps the logical
/// row count, accounts for the unanalyzed partition as modifications, and
/// publishes every merged column/index when missing partition statistics are
/// explicitly skipped.
#[test]
fn global_statistics_skip_missing_partition_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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

/// Go `DumpStatsToJSONBySnapshot` at snapshot 0: a plain table dumps its
/// own statistics; a partitioned one dumps `global`, plus its partitions
/// under static pruning or `dumpPartitionStats`.
#[test]
fn dump_stats_to_json_follows_gos_partition_rules() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let mut session = stack.factory.open_session(session_context(76)).expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "CREATE TABLE plain_dump (a INT, INDEX ia(a))");
    rows(&mut session, "INSERT INTO plain_dump VALUES (1), (2), (3)");
    rows(&mut session, "CREATE TABLE part_dump (a INT) PARTITION BY HASH(a) PARTITIONS 2");
    rows(&mut session, "INSERT INTO part_dump VALUES (1), (2), (3)");
    rows(&mut session, "ANALYZE TABLE plain_dump, part_dump");
    let info = |name: &str| {
        let catalog = stack.factory.catalog.get_latest();
        let (_, table) = catalog.table_info_by_name("test", name).expect("table is published");
        let table = table.read().clone();
        table
    };
    let plain = stack
        .factory
        .dump_stats_to_json("test", &info("plain_dump"), true)
        .expect("dump succeeds")
        .expect("analyzed table has statistics");
    assert!(plain.columns.as_ref().is_some_and(|columns| columns.contains_key("a")), "{plain:?}");
    assert!(plain.indices.as_ref().is_some_and(|indices| indices.contains_key("ia")), "{plain:?}");
    let keys = |dump_partition_stats: bool| {
        let json = stack
            .factory
            .dump_stats_to_json("test", &info("part_dump"), dump_partition_stats)
            .expect("dump succeeds")
            .expect("a partitioned table always answers its JSON table");
        assert_eq!(json.table_name, "part_dump");
        json.partitions.unwrap_or_default().into_keys().collect::<Vec<_>>()
    };
    // `tidb_partition_prune_mode` is dynamic by default.
    assert_eq!(keys(false), ["global"]);
    assert_eq!(keys(true), ["global", "p0", "p1"]);
    rows(&mut session, "SET GLOBAL tidb_partition_prune_mode = 'static'");
    assert_eq!(keys(false), ["global", "p0", "p1"]);
}

/// Pinned storage `TestGCPartition`, including its retained logical
/// meta-only row after both physical partitions finish two-phase GC.
#[test]
fn stats_gc_matches_go_partition_phases() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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

/// Pinned `globalstats.TestShowGlobalStatsWithAsyncMergeGlobal` and
/// `TestShowGlobalStatsWithoutAsyncMergeGlobal`: static pruning publishes
/// only physical-partition statistics, while dynamic pruning also publishes
/// the logical table's global column and index statistics. Every SHOW surface
/// must traverse the same set.
#[test]
fn partition_analyze_show_surfaces_match_global_stats_visibility() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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

/// Pinned `globalstats.TestGlobalStatsData`: partition and global histograms
/// retain Go's exact cumulative bucket counts, repeats, bounds, and zeroed
/// merged bucket NDV for both the column and its index.
#[test]
fn partition_global_stats_bucket_data_matches_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
    drain_stats_ddl_events(&stack.factory, &mut session);
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
    drain_stats_ddl_events(&stack.factory, &mut session);
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
    // Go DDL tests handle the subscriber event before inspecting stats;
    // SHOW reads the cache, so also perform the equivalent of h.Update.
    drain_stats_ddl_events(&stack.factory, &mut session);
    let reloads = stack._stats_reloader.stats().reloads;
    rows(&mut session, "FLUSH STATS_DELTA *.*");
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while stack._stats_reloader.stats().reloads == reloads {
        assert!(
            std::time::Instant::now() < deadline,
            "statistics cache did not refresh after dropping p2"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(global_meta(&mut session)[5], "7");
    rows(&mut session, "ANALYZE TABLE global_stats_version");
    let meta = global_meta(&mut session);
    assert_eq!(meta[5], "7");
}

/// Pinned `globalstats.TestDDLPartition4GlobalStats`: truncating partitions
/// replaces their physical IDs, subtracts their persisted counts from the
/// logical global row, and installs replacement partition metadata.
#[test]
fn truncate_partitions_refreshes_global_stats_meta_like_go() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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
    drain_stats_ddl_events(&stack.factory, &mut session);
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
    drain_stats_ddl_events(&stack.factory, &mut session);
    // Go calls h.Update after handling the DDL event before reading SHOW's
    // cached statistics. Trigger and await the corresponding cache refresh.
    let reloads = stack._stats_reloader.stats().reloads;
    rows(&mut session, "FLUSH STATS_DELTA *.*");
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while stack._stats_reloader.stats().reloads == reloads {
        assert!(
            std::time::Instant::now() < deadline,
            "statistics cache did not refresh"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
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
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
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

/// Go `onReorganizePartition` on the production node: REORGANIZE PARTITION,
/// `PARTITION BY` on an existing table and REMOVE PARTITIONING move every row
/// to the physical table the new definitions route it to, rebuild the
/// indexes, and give the table its `NewTableID` where Go does. This node
/// refused all three.
#[test]
fn partition_reorganizations_move_rows_as_go_does() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1002)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.r (a INT, b INT, KEY ib (b)) PARTITION BY RANGE (a) \
         (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))",
    );
    rows(
        &mut session,
        "INSERT INTO test.r VALUES (1, 1), (5, 5), (15, 15)",
    );
    session
        .node_conn()
        .execute_write(
            "ALTER TABLE test.r REORGANIZE PARTITION p0 INTO \
             (PARTITION pa VALUES LESS THAN (4), PARTITION pb VALUES LESS THAN (10))",
        )
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        [[
            "Warning",
            "1105",
            "The statistics of related partitions will be outdated after reorganizing \
             partitions. Please use 'ANALYZE TABLE' statement if you want to update it now"
        ]]
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT a FROM test.r PARTITION (pb)")),
        [["5"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT a FROM test.r USE INDEX (ib) WHERE b > 0 ORDER BY a"
        )),
        [["1"], ["5"], ["15"]]
    );
    rows(&mut session, "ADMIN CHECK TABLE test.r");

    rows(
        &mut session,
        "CREATE TABLE test.h (a INT PRIMARY KEY, b INT, KEY ib (b))",
    );
    rows(
        &mut session,
        "INSERT INTO test.h VALUES (1, 1), (2, 2), (3, 3), (4, 4)",
    );
    session
        .node_conn()
        .execute_write("ALTER TABLE test.h PARTITION BY HASH (a) PARTITIONS 2")
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT a FROM test.h PARTITION (p1) ORDER BY a"
        )),
        [["1"], ["3"]]
    );
    rows(&mut session, "ADMIN CHECK TABLE test.h");
    session
        .node_conn()
        .execute_write("ALTER TABLE test.h REMOVE PARTITIONING")
        .unwrap();
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.h"));
    assert!(!created[0][1].contains("PARTITION"), "{}", created[0][1]);
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT a FROM test.h USE INDEX (ib) WHERE b > 0 ORDER BY a"
        )),
        [["1"], ["2"], ["3"], ["4"]]
    );
    rows(&mut session, "ADMIN CHECK TABLE test.h");
}

/// Go `hashPartitionManagement`: on a HASH table ADD PARTITION and COALESCE
/// PARTITION reorganize every partition, so each row is re-hashed into the
/// new count; and INTERVAL maintenance (LAST / FIRST PARTITION LESS THAN) is
/// an ADD / DROP PARTITION job. This node appended a definition without
/// moving a row, and refused the interval statements.
#[test]
fn hash_partition_management_rehashes_rows_as_go_does() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1003)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.h (a INT, b INT, KEY ib (b)) PARTITION BY HASH (a) PARTITIONS 2",
    );
    rows(
        &mut session,
        "INSERT INTO test.h VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6)",
    );
    session
        .node_conn()
        .execute_write("ALTER TABLE test.h ADD PARTITION PARTITIONS 1")
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT a FROM test.h PARTITION (p2) ORDER BY a"
        )),
        [["2"], ["5"]]
    );
    rows(&mut session, "ADMIN CHECK TABLE test.h");
    session
        .node_conn()
        .execute_write("ALTER TABLE test.h COALESCE PARTITION 2")
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM test.h PARTITION (p0)"
        )),
        [["6"]]
    );
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.h"));
    assert!(
        created[0][1].ends_with("PARTITION BY HASH (`a`) PARTITIONS 1"),
        "{}",
        created[0][1]
    );
    rows(&mut session, "ADMIN CHECK TABLE test.h");

    rows(
        &mut session,
        "CREATE TABLE test.iv (a INT) PARTITION BY RANGE (a) INTERVAL (10) \
         FIRST PARTITION LESS THAN (10) LAST PARTITION LESS THAN (30)",
    );
    session
        .node_conn()
        .execute_write("ALTER TABLE test.iv LAST PARTITION LESS THAN (50)")
        .unwrap();
    session
        .node_conn()
        .execute_write("ALTER TABLE test.iv FIRST PARTITION LESS THAN (20)")
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT partition_name FROM information_schema.partitions \
             WHERE table_schema = 'test' AND table_name = 'iv' ORDER BY partition_ordinal_position"
        )),
        [["P_LT_20"], ["P_LT_30"], ["P_LT_40"], ["P_LT_50"]]
    );
}

/// Go `AlterTablePartitionPlacement` / `onAlterTablePartitionPlacement` and
/// the `AlterTableOption` loop's one charset job: on the production node a
/// partition takes (and clears) a placement policy, and several CHARSET /
/// COLLATE options settle through `GetCharsetAndCollateInTableOption`.
#[test]
fn partition_placement_and_multi_option_charset_on_the_production_node() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1019)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE PLACEMENT POLICY pp1 FOLLOWERS=1")
        .unwrap();
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE tp (a INT) PARTITION BY RANGE (a) \
             (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    let show_create = |session: &mut Session, table: &str| {
        displayed(rows(session, &format!("SHOW CREATE TABLE {table}")))[0][1].clone()
    };
    session
        .node_conn()
        .execute_write("ALTER TABLE tp PARTITION p0 PLACEMENT POLICY=pp1")
        .unwrap();
    assert!(show_create(&mut session, "tp").contains(
        "PARTITION `p0` VALUES LESS THAN (10) /*T![placement] PLACEMENT POLICY=`pp1` */"
    ));
    session
        .node_conn()
        .execute_write("ALTER TABLE tp PARTITION p0 PLACEMENT POLICY=DEFAULT")
        .unwrap();
    assert!(!show_create(&mut session, "tp").contains("PLACEMENT POLICY"));
    session
        .node_conn()
        .execute_write("CREATE TABLE c (a VARCHAR(10))")
        .unwrap();
    let error = session
        .node_conn()
        .execute_write("ALTER TABLE c PARTITION p0 PLACEMENT POLICY=pp1")
        .unwrap_err();
    assert_eq!(error.code, 1505);
    let error = session
        .node_conn()
        .execute_write("ALTER TABLE tp PARTITION nope PLACEMENT POLICY=pp1")
        .unwrap_err();
    assert_eq!(error.code, 1735);

    session
        .node_conn()
        .execute_write("ALTER TABLE c CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci")
        .unwrap();
    assert!(show_create(&mut session, "c")
        .contains("DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"));
    let error = session
        .node_conn()
        .execute_write("ALTER TABLE c CHARSET latin1 CHARSET utf8mb4")
        .unwrap_err();
    assert_eq!(error.code, 1302);
}

/// Go `ddl.Start` -> `detectAndUpdateJobVersion` turns on the global index V1
/// key format (a single node at once), so a non-unique global index carries
/// the partition ID in its key and rows of different partitions sharing a
/// `_tidb_rowid` after EXCHANGE PARTITION keep separate entries (issue
/// 65289). The node never ran the detection and wrote the legacy key.
#[test]
fn global_index_keys_carry_the_partition_after_exchange() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1028)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE t (a INT, b INT, PRIMARY KEY (a) NONCLUSTERED)")
        .unwrap();
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE tp (a INT, b INT, PRIMARY KEY (a) NONCLUSTERED) PARTITION BY RANGE (a) \
             (PARTITION p0 VALUES LESS THAN (5), PARTITION p1 VALUES LESS THAN (11), \
             PARTITION p2 VALUES LESS THAN (20))",
        )
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO tp (a, b) VALUES (2, 2), (4, 4), (6, 6)")
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO t (a, b) VALUES (12, 2), (14, 4), (16, 6)")
        .unwrap();
    session
        .node_conn()
        .execute_write("ALTER TABLE tp EXCHANGE PARTITION p2 WITH TABLE t")
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE INDEX idx_b ON tp (b) GLOBAL")
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT COUNT(*) FROM tp USE INDEX (idx_b)"
        )),
        [["6"]]
    );
}

/// Go resolves DROP STATS' tables before it writes anything.
#[test]
fn drop_stats_refuses_an_unknown_table_and_a_partition_clause_on_a_plain_one() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1039)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE dp (a INT)")
        .unwrap();
    let error = session
        .node_conn()
        .execute_write("DROP STATS missing")
        .unwrap_err();
    assert_eq!(error.code, 1146);
    let error = session
        .node_conn()
        .execute_write("DROP STATS dp PARTITION p0")
        .unwrap_err();
    assert_eq!(error.code, 1505);
}

/// Go assigns a `CREATE TABLE ... LIKE` copy its own physical table ids -- the
/// table's, then one per partition -- like any other new table. The node kept
/// the source's partition ids, so the two tables shared physical tables and a
/// row inserted into one was counted by the other.
#[test]
fn create_table_like_a_partitioned_table_gets_its_own_partition_ids() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1041)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn().execute_write(
            "CREATE TABLE lk1 (c_int INT, PRIMARY KEY (c_int)) PARTITION BY HASH (c_int) PARTITIONS 2",
        )
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE lk2 LIKE lk1")
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO lk2 VALUES (1)")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT COUNT(*) FROM lk1")),
        [["0"]]
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT COUNT(*) FROM lk2")),
        [["1"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT COUNT(DISTINCT tidb_partition_id) FROM information_schema.partitions \
             WHERE table_schema = 'test' AND table_name IN ('lk1', 'lk2')"
        )),
        [["4"]]
    );
}

/// Go plans `Point_Get ... partition:p0` for a unique key on a partitioned
/// table and routes the key by the partition expression; UPDATE and DELETE
/// reach the same row, and a key no partition holds answers empty.
#[test]
fn partitioned_unique_key_point_get_routes_by_partition() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1049)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE ppg (a INT, b INT, UNIQUE KEY (a)) PARTITION BY LIST (a) \
             (PARTITION p0 VALUES IN (0,4), PARTITION p1 VALUES IN (1,3))",
        )
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO ppg VALUES (4, 40), (1, 10), (3, 30)")
        .unwrap();
    let plan = displayed(rows(
        &mut session,
        "EXPLAIN FORMAT = 'brief' SELECT * FROM ppg WHERE a = 4",
    ));
    assert_eq!(plan.len(), 1, "{plan:?}");
    assert!(
        plan[0][0].starts_with("Point_Get") && plan[0][3].contains("partition:p0"),
        "{plan:?}"
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM ppg WHERE a = 4")),
        [["4", "40"]]
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM ppg WHERE a = 7")),
        Vec::<Vec<String>>::new()
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT * FROM ppg PARTITION (p1) WHERE a = 4"
        )),
        Vec::<Vec<String>>::new()
    );
    session
        .node_conn()
        .execute_write("UPDATE ppg SET b = 41 WHERE a = 4")
        .unwrap();
    session
        .node_conn()
        .execute_write("DELETE FROM ppg WHERE a = 3")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM ppg ORDER BY a")),
        [["1", "10"], ["4", "41"]]
    );
}

/// Go's hash pruner has no empty-condition shortcut: a full range over a
/// BIT(1) column still reaches the issue-22619 branch, so the third of three
/// hash partitions (which a one-bit value can never reach) is not scanned.
#[test]
fn full_scan_of_a_bit_hash_table_skips_the_unreachable_partition() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1108)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE tbh (a BIT(1) DEFAULT NULL, b INT) PARTITION BY HASH(a) PARTITIONS 3",
        )
        .unwrap();
    let plan = displayed(rows(&mut session, "EXPLAIN SELECT * FROM tbh"));
    let reader = plan
        .iter()
        .find(|row| row[0].contains("TableReader"))
        .expect("a table reader");
    assert_eq!(reader[3], "partition:p0,p1", "{plan:?}");
}

/// Plan ids and a dirty partitioned table's shape follow Go: the skyline's
/// preferRange filter runs before any candidate draws a plan id, `Delete`
/// takes its id before the pruning projection, `INSERT ... SELECT` draws the
/// target's column ids first, and a table the transaction wrote in is read
/// through a `UnionScan` carrying the partition id. Expected rows: Go 1.27 on
/// this branch (`TestZZ` probes `US9`, `US16`, `US17`).
#[test]
fn plan_ids_and_dirty_partition_shapes_match_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1120)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    for ddl in [
        "CREATE TABLE tpi (a INT PRIMARY KEY, b INT, c INT, KEY(c))",
        "CREATE TABLE tpd (a INT PRIMARY KEY, b INT, c INT, UNIQUE KEY(b), KEY(c))",
        "CREATE TABLE tpp (a INT PRIMARY KEY, b INT) PARTITION BY HASH(a) PARTITIONS 3",
        "INSERT INTO tpd VALUES (1,1,1),(2,2,2),(3,3,3)",
        "INSERT INTO tpp VALUES (1,1),(2,2),(3,3)",
    ] {
        session.node_conn().execute_write(ddl).unwrap();
    }
    let plan = |session: &mut Session, sql: &str| -> Vec<String> {
        displayed(rows(session, &format!("EXPLAIN FORMAT = 'verbose' {sql}")))
            .into_iter()
            .filter(|row| row.len() > 5)
            .map(|row| format!("{} {} {} {}", row[0], row[1], row[2], row[5]))
            .collect()
    };
    assert_eq!(
        plan(&mut session, "SELECT * FROM tpi WHERE c = 1")[0],
        "IndexLookUp_7 10.00 19521.54 "
    );
    assert_eq!(
        plan(&mut session, "DELETE FROM tpd WHERE c = 1")[..2],
        ["Delete_3 N/A N/A N/A", "└─IndexLookUp_8 10.00 19521.54 "]
    );
    assert_eq!(
        plan(
            &mut session,
            "INSERT INTO tpd SELECT a + 10, b + 10, c FROM tpd"
        )[1],
        "└─Projection_5 10000.00 630395.30 plus(test.tpd.a, 10)->Column#8, \
         plus(test.tpd.b, 10)->Column#9, test.tpd.c"
    );
    session.node_conn().control_transaction("BEGIN").unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO tpp VALUES (4,4)")
        .unwrap();
    assert_eq!(
        plan(&mut session, "SELECT * FROM tpp WHERE a > 1")[..3],
        [
            "Projection_5 3333.33 93418.22 test.tpp.a, test.tpp.b",
            "└─UnionScan_7 3333.33 92752.88 gt(test.tpp.a, 1)",
            "  └─TableReader_9 3333.33 92752.88 data:TableRangeScan_8",
        ]
    );
    assert_eq!(
        plan(&mut session, "SELECT COUNT(*) FROM tpp")[..3],
        [
            "HashAgg_8 1.00 588672.28 funcs:count(1)->Column#5",
            "└─UnionScan_11 10000.00 387557.30 ",
            "  └─TableReader_13 10000.00 387557.30 data:TableFullScan_12",
        ]
    );
    session.node_conn().control_transaction("ROLLBACK").unwrap();
}

/// Static partition pruning spends the plan ids Go does: one for the
/// partition expression of a HASH or plain RANGE table, one for every
/// partition copy (`NewBaseLogicalPlan` before `SetID`), and the union's MPP
/// candidate. Expected rows: Go 1.27 on this branch (`TestZZ` probe `PP1`).
#[test]
fn static_partition_pruning_spends_the_plan_ids_go_does() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1125)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    for ddl in [
        "SET @@tidb_partition_prune_mode = 'static'",
        "CREATE TABLE tsp (a BIT(1) DEFAULT NULL, b INT DEFAULT NULL) PARTITION BY HASH(a) PARTITIONS 3",
        "CREATE TABLE tsr (a INT, b INT, KEY(b)) PARTITION BY RANGE(a) (PARTITION p0 VALUES LESS THAN (10), \
         PARTITION p1 VALUES LESS THAN (20), PARTITION p2 VALUES LESS THAN (MAXVALUE))",
    ] {
        session.node_conn().execute_write(ddl).unwrap();
    }
    let plan = |session: &mut Session, sql: &str| -> Vec<String> {
        displayed(rows(session, &format!("EXPLAIN FORMAT = 'verbose' {sql}")))
            .into_iter()
            .filter(|row| row.len() > 5 && row[3] == "root")
            .map(|row| format!("{} {} {} {}", row[0], row[1], row[2], row[5]))
            .collect()
    };
    assert_eq!(
        plan(&mut session, "SELECT * FROM tsp"),
        [
            "PartitionUnion_8 20000.00 142694.60 ",
            "├─TableReader_11 10000.00 356736.49 data:TableFullScan_10",
            "└─TableReader_13 10000.00 356736.49 data:TableFullScan_12",
        ]
    );
    assert_eq!(
        plan(&mut session, "SELECT * FROM tsr WHERE a = 5"),
        ["TableReader_9 10.00 336428.45 data:Selection_8"]
    );
}

/// Go's JSON builtin classes wrap their arguments in implicit casts
/// (`newBaseBuiltinFuncWithTp`), and `isnull` tests a JSON argument as its
/// string form. A cast whose result is JSON is never folded, so the constant
/// operands stay `cast(1, json BINARY)` in the plan. Each operator info below
/// is the Go oracle's text (database `pl17`/`pl18` renamed to `test`).
/// Go `columnDefToCol` has no default arm: COLUMN_FORMAT, STORAGE,
/// SECONDARY_ENGINE_ATTRIBUTE and an inline REFERENCES are parsed and never
/// read, by CREATE TABLE and ADD COLUMN alike; `buildTablePartitionInfo`
/// warns for PARTITION BY SYSTEM_TIME and builds a normal table (both
/// captured from a Go tidb-server).
#[test]
fn ignored_column_options_and_system_time_partitioning_follow_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1131)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    for ddl in [
        "CREATE TABLE c1 (a INT COLUMN_FORMAT FIXED, b INT STORAGE DISK, \
         c INT SECONDARY_ENGINE_ATTRIBUTE = 'x')",
        "CREATE TABLE c3 (id INT PRIMARY KEY, a INT)",
        "ALTER TABLE c3 ADD COLUMN b INT COLUMN_FORMAT DYNAMIC",
        "ALTER TABLE c3 ADD COLUMN c INT STORAGE MEMORY",
        "ALTER TABLE c3 ADD COLUMN d INT SECONDARY_ENGINE_ATTRIBUTE = 'y'",
        "ALTER TABLE c3 ADD COLUMN e INT REFERENCES c1(a)",
    ] {
        session
            .node_conn()
            .execute_write(ddl)
            .unwrap_or_else(|error| panic!("{ddl}: {error:?}"));
    }
    assert_eq!(
        displayed(rows(&mut session, "SHOW CREATE TABLE c1"))[0][1],
        "CREATE TABLE `c1` (\n  `a` int DEFAULT NULL,\n  `b` int DEFAULT NULL,\n  \
         `c` int DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    );
    assert_eq!(
        displayed(rows(&mut session, "SHOW CREATE TABLE c3"))[0][1],
        "CREATE TABLE `c3` (\n  `id` int NOT NULL,\n  `a` int DEFAULT NULL,\n  \
         `b` int DEFAULT NULL,\n  `c` int DEFAULT NULL,\n  `d` int DEFAULT NULL,\n  \
         `e` int DEFAULT NULL,\n  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */\n\
         ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    );
    for ddl in [
        "CREATE TABLE sy (a INT) PARTITION BY SYSTEM_TIME (PARTITION p0 HISTORY, PARTITION pn CURRENT)",
        "CREATE TABLE sy2 (a INT) PARTITION BY SYSTEM_TIME INTERVAL 1 DAY \
         (PARTITION p0 HISTORY, PARTITION pn CURRENT)",
    ] {
        session
            .node_conn()
            .execute_write(ddl)
            .unwrap_or_else(|error| panic!("{ddl}: {error:?}"));
        assert_eq!(
            displayed(rows(&mut session, "SHOW WARNINGS")),
            [[
                "Warning".to_owned(),
                "8200".to_owned(),
                "Unsupported partition type SYSTEM_TIME, treat as normal table".to_owned(),
            ]],
            "{ddl}"
        );
    }
    assert_eq!(
        displayed(rows(&mut session, "SHOW CREATE TABLE sy"))[0][1],
        "CREATE TABLE `sy` (\n  `a` int DEFAULT NULL\n) ENGINE=InnoDB DEFAULT \
         CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    );
}

/// Go matches partition names by their Unicode lower-case form in LOCK
/// STATS and ANALYZE, and refuses a PARTITION clause on a table that has
/// none with 1505 (captured from Go on this branch).
#[test]
fn stats_partition_names_and_the_nonpartitioned_refusal_follow_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1904)).unwrap();
    rows(&mut session, "CREATE DATABASE stats_parts");
    rows(&mut session, "USE stats_parts");
    rows(&mut session, "CREATE TABLE t (id INT PRIMARY KEY)");
    rows(
        &mut session,
        "CREATE TABLE pt (a INT) PARTITION BY RANGE (a) \
         (PARTITION pÄ VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))",
    );
    session
        .node_conn()
        .execute_write("LOCK STATS pt PARTITION PÄ")
        .unwrap();
    session
        .node_conn()
        .execute_write("UNLOCK STATS pt PARTITION pä")
        .unwrap();
    session
        .node_conn()
        .execute_write("ANALYZE TABLE pt PARTITION PÄ")
        .unwrap();
    let error = session
        .node_conn()
        .execute_write("ANALYZE TABLE t PARTITION p0")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (
            1505,
            "Partition management on a not partitioned table is not possible"
        )
    );
}

/// ADD, DROP and TRUNCATE PARTITION run as Go's persisted
/// `ActionAddTablePartition` (none -> replica-only -> public),
/// `ActionDropTablePartition` and `ActionTruncateTablePartition` (public ->
/// write-only -> delete-only -> delete-reorganization -> none) jobs, a schema
/// version per step; IF [NOT] EXISTS queue nothing and leave Go's note, and
/// FIRST / LAST PARTITION rewrite the job's query (captured from Go on this
/// branch).
#[test]
fn partition_changes_run_as_persisted_jobs_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1952)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    let version = |session: &mut _| -> i64 {
        displayed(rows(session, "admin show ddl"))[0][0]
            .parse()
            .unwrap()
    };
    // (job id, db|table|type|state|status, query) of the latest job.
    let job = |session: &mut _| -> (String, String, String) {
        let row = displayed(rows(session, "admin show ddl jobs 1")).remove(0);
        let query = displayed(rows(
            session,
            &format!("admin show ddl job queries {}", row[0]),
        ))
        .remove(0)
        .remove(0);
        (
            row[0].clone(),
            [1, 2, 3, 4, 11].map(|column| row[column].clone()).join("|"),
            query,
        )
    };
    let step = |session: &mut Session, sql: &str| -> (i64, String, String) {
        let before = version(session);
        rows(session, sql);
        let (_, kind, query) = job(session);
        (version(session) - before, kind, query)
    };
    let refused = |session: &mut Session, sql: &str| -> (u16, String) {
        let error = session.node_conn().execute_write(sql).unwrap_err();
        (error.code, error.message)
    };
    rows(
        &mut session,
        "create table r (a int, b int, unique key ua(a)) partition by range (a) \
         (partition p0 values less than (10), partition p1 values less than (20))",
    );
    rows(&mut session, "insert into r values (1,1),(11,11)");
    let sql = "alter table r add partition (partition p2 values less than (30), partition p3 values less than (40))";
    assert_eq!(
        step(&mut session, sql),
        (
            2,
            "test|r|add partition|public|synced".to_owned(),
            sql.to_owned()
        )
    );
    let (last, _, _) = job(&mut session);
    rows(
        &mut session,
        "alter table r add partition if not exists (partition p2 values less than (50))",
    );
    assert_eq!(
        displayed(rows(&mut session, "show warnings")),
        [["Note", "1517", "Duplicate partition name p2"]]
    );
    assert_eq!(job(&mut session).0, last, "IF NOT EXISTS queues nothing");
    assert_eq!(
        refused(
            &mut session,
            "alter table r add partition (partition p2 values less than (50))"
        ),
        (1517, "Duplicate partition name p2".to_owned())
    );
    assert_eq!(
        refused(
            &mut session,
            "alter table r add partition (partition p9 values less than (35))"
        ),
        (
            1493,
            "VALUES LESS THAN value must be strictly increasing for each partition".to_owned()
        )
    );
    let sql = "alter table r drop partition p3";
    assert_eq!(
        step(&mut session, sql),
        (
            4,
            "test|r|drop partition|none|synced".to_owned(),
            sql.to_owned()
        )
    );
    let (last, _, _) = job(&mut session);
    rows(&mut session, "alter table r drop partition if exists p3");
    assert_eq!(
        displayed(rows(&mut session, "show warnings")),
        [["Note", "1507", "Error in list of partitions to DROP"]]
    );
    assert_eq!(job(&mut session).0, last, "IF EXISTS queues nothing");
    for sql in [
        "alter table r truncate partition p0, p1",
        "alter table r truncate partition all",
        "alter table r truncate partition p0, p0",
    ] {
        assert_eq!(
            step(&mut session, sql),
            (
                4,
                "test|r|truncate partition|none|synced".to_owned(),
                sql.to_owned()
            )
        );
    }
    assert!(rows(&mut session, "select * from r").is_empty());
    assert_eq!(
        refused(&mut session, "alter table r truncate partition nosuch"),
        (1735, "Unknown partition 'nosuch' in table 'r'".to_owned())
    );
    rows(&mut session, "insert into r values (1,1),(11,11),(21,21)");
    assert_eq!(
        step(&mut session, "alter table r drop partition p0, p1").0,
        4
    );
    assert_eq!(
        displayed(rows(&mut session, "select * from r")),
        [["21", "21"]]
    );
    assert_eq!(
        refused(&mut session, "alter table r drop partition p2"),
        (
            1508,
            "Cannot remove all partitions, use DROP TABLE instead".to_owned()
        )
    );

    // A HASH table's ADD is a reorganization; DROP is refused and TRUNCATE
    // is the same job as on RANGE.
    rows(
        &mut session,
        "create table h (a int) partition by hash (a) partitions 2",
    );
    rows(&mut session, "insert into h values (1),(2),(3)");
    rows(&mut session, "alter table h add partition (partition p9)");
    assert_eq!(
        displayed(rows(&mut session, "select a from h order by a")),
        [["1"], ["2"], ["3"]]
    );
    assert_eq!(
        refused(&mut session, "alter table h drop partition p0"),
        (
            1512,
            "DROP PARTITION can only be used on RANGE/LIST partitions".to_owned()
        )
    );
    let sql = "alter table h truncate partition p0";
    assert_eq!(
        step(&mut session, sql),
        (
            4,
            "test|h|truncate partition|none|synced".to_owned(),
            sql.to_owned()
        )
    );

    rows(
        &mut session,
        "create table l (a int) partition by list (a) (partition p0 values in (1,2), partition pd default)",
    );
    assert_eq!(
        refused(&mut session, "alter table l add partition (partition p1 values in (3))"),
        (
            8200,
            "Unsupported ADD List partition, already contains DEFAULT partition. Please use REORGANIZE PARTITION instead".to_owned()
        )
    );
    rows(
        &mut session,
        "create table l2 (a int) partition by list (a) (partition p0 values in (1,2))",
    );
    let sql =
        "alter table l2 add partition (partition p1 values in (3), partition p2 values in (4))";
    assert_eq!(
        step(&mut session, sql),
        (
            2,
            "test|l2|add partition|public|synced".to_owned(),
            sql.to_owned()
        )
    );
    assert_eq!(
        refused(
            &mut session,
            "alter table l2 add partition (partition p3 values in (4))"
        ),
        (
            1495,
            "Multiple definition of same constant in list partitioning".to_owned()
        )
    );

    rows(
        &mut session,
        "create table i (a int) partition by range (a) interval (10) \
         first partition less than (10) last partition less than (40)",
    );
    assert_eq!(
        step(&mut session, "alter table i last partition less than (60)"),
        (
            2,
            "test|i|add partition|public|synced".to_owned(),
            "alter table i ADD PARTITION (PARTITION `P_LT_50` VALUES LESS THAN (50),\n PARTITION `P_LT_60` VALUES LESS THAN (60))".to_owned()
        )
    );
    assert_eq!(
        step(&mut session, "alter table i first partition less than (30)"),
        (
            4,
            "test|i|drop partition|none|synced".to_owned(),
            "alter table i DROP PARTITION `P_LT_10`, `P_LT_20`".to_owned()
        )
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "select partition_name from information_schema.partitions \
             where table_schema = 'test' and table_name = 'i' order by partition_ordinal_position",
        )),
        [["P_LT_30"], ["P_LT_40"], ["P_LT_50"], ["P_LT_60"]]
    );

    rows(&mut session, "create table nt (a int)");
    for sql in [
        "alter table nt add partition (partition p0 values less than (10))",
        "alter table nt drop partition p0",
        "alter table nt truncate partition p0",
    ] {
        assert_eq!(
            refused(&mut session, sql),
            (
                1505,
                "Partition management on a not partitioned table is not possible".to_owned()
            ),
            "{sql}"
        );
    }

    // ALTER TABLE ... ENGINE_ATTRIBUTE / STORAGE_CLASS: Go's one-step
    // `ActionModifyEngineAttribute` job.
    rows(
        &mut session,
        "create table e (a int) partition by range (a) (partition p0 values less than (10))",
    );
    for sql in [
        "alter table e engine_attribute = '{\"storage_class\": \"STANDARD\"}'",
        "alter table e storage_class = 'IA'",
    ] {
        assert_eq!(
            step(&mut session, sql),
            (
                1,
                "test|e|modify engine attribute|public|synced".to_owned(),
                sql.to_owned()
            )
        );
    }
    assert_eq!(
        displayed(rows(&mut session, "show create table e"))[0][1],
        "CREATE TABLE `e` (\n  `a` int DEFAULT NULL\n) ENGINE=InnoDB STORAGE_CLASS='IA' DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\nPARTITION BY RANGE (`a`)\n(PARTITION `p0` VALUES LESS THAN (10))"
    );
}

/// REORGANIZE PARTITION, ADD PARTITION on a HASH table, COALESCE
/// PARTITION, ALTER TABLE ... PARTITION BY and REMOVE PARTITIONING run as
/// Go's persisted reorganization jobs: six schema versions (none ->
/// delete-only -> write-only -> write-reorganization -> delete-reorganization
/// -> public -> none), the rows copied and every index rebuilt, the job
/// counting one row per copy and per index entry (captured from Go on this
/// branch).
#[test]
fn partition_reorganizations_run_as_persisted_jobs_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1955)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    let version = |session: &mut Session| -> i64 {
        displayed(rows(session, "admin show ddl"))[0][0]
            .parse()
            .unwrap()
    };
    let job = |session: &mut Session| -> String {
        let row = displayed(rows(session, "admin show ddl jobs 1")).remove(0);
        [1, 2, 3, 4, 7, 11]
            .map(|column| row[column].clone())
            .join("|")
    };
    // (versions, warnings) of one statement.
    let step = |session: &mut Session, sql: &str| -> (i64, Vec<Vec<String>>) {
        let before = version(session);
        rows(session, sql);
        let warnings = displayed(rows(session, "show warnings"));
        (version(session) - before, warnings)
    };
    let related = vec![vec![
        "Warning".to_owned(),
        "1105".to_owned(),
        "The statistics of related partitions will be outdated after reorganizing partitions. Please use 'ANALYZE TABLE' statement if you want to update it now".to_owned(),
    ]];
    let new = vec![vec![
        "Warning".to_owned(),
        "1105".to_owned(),
        "The statistics of new partitions will be outdated after reorganizing partitions. Please use 'ANALYZE TABLE' statement if you want to update it now".to_owned(),
    ]];
    rows(
        &mut session,
        "create table r (a int, b int, unique key ub(b) global, key ka(a)) partition by range (a) \
         (partition p0 values less than (10), partition p1 values less than (20), \
         partition p2 values less than (30))",
    );
    rows(
        &mut session,
        "insert into r values (1,1),(5,5),(11,11),(15,15),(25,25)",
    );
    assert_eq!(
        step(
            &mut session,
            "alter table r reorganize partition p0, p1 into \
             (partition p01 values less than (15), partition p11 values less than (20))",
        ),
        (6, related.clone())
    );
    assert_eq!(
        job(&mut session),
        "test|r|alter table reorganize partition|none|13|synced"
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "select a, b from r partition (p01) order by a"
        )),
        [["1", "1"], ["5", "5"], ["11", "11"]]
    );
    assert_eq!(
        displayed(rows(&mut session, "select a, b from r partition (p11)")),
        [["15", "15"]]
    );
    rows(&mut session, "admin check table r");
    assert_eq!(
        displayed(rows(
            &mut session,
            "select * from r use index(ub) where b = 15"
        )),
        [["15", "15"]]
    );

    rows(
        &mut session,
        "create table h (a int, b int) partition by hash (a) partitions 2",
    );
    rows(&mut session, "insert into h values (1,1),(2,2),(3,3),(4,4)");
    assert_eq!(
        step(&mut session, "alter table h add partition partitions 1"),
        (6, related.clone())
    );
    assert_eq!(
        job(&mut session),
        "test|h|alter table reorganize partition|none|4|synced"
    );
    assert_eq!(
        displayed(rows(&mut session, "select a from h partition (p2)")),
        [["2"]]
    );
    assert_eq!(
        step(&mut session, "alter table h coalesce partition 2"),
        (6, related)
    );
    assert_eq!(
        displayed(rows(&mut session, "select count(*) from h")),
        [["4"]]
    );
    rows(&mut session, "admin check table h");

    rows(&mut session, "create table n (a int primary key, b int)");
    rows(&mut session, "insert into n values (1,1),(12,12)");
    assert_eq!(
        step(
            &mut session,
            "alter table n partition by range (a) \
             (partition p0 values less than (10), partition p1 values less than (20))",
        ),
        (6, new.clone())
    );
    assert_eq!(
        job(&mut session),
        "test|n|alter table partition by|none|2|synced"
    );
    assert_eq!(
        displayed(rows(&mut session, "select a from n partition (p1)")),
        [["12"]]
    );
    rows(&mut session, "admin check table n");
    assert_eq!(
        step(&mut session, "alter table n remove partitioning"),
        (6, Vec::<Vec<String>>::new())
    );
    assert_eq!(
        job(&mut session),
        "test|n|alter table remove partitioning|none|2|synced"
    );
    assert_eq!(
        displayed(rows(&mut session, "select a from n order by a")),
        [["1"], ["12"]]
    );
    rows(&mut session, "admin check table n");

    assert_eq!(
        step(
            &mut session,
            "alter table r partition by hash(a) partitions 2"
        ),
        (6, new)
    );
    assert_eq!(
        job(&mut session),
        "test|r|alter table partition by|none|15|synced"
    );
    rows(&mut session, "admin check table r");
    assert_eq!(
        displayed(rows(&mut session, "select count(*) from r")),
        [["5"]]
    );
    let error = session
        .node_conn()
        .execute_write(
            "alter table r reorganize partition p9 into (partition p10 values less than (5))",
        )
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (8200, "Unsupported reorganize partition")
    );
}

/// EXCHANGE PARTITION runs as Go's persisted `ActionExchangeTablePartition`
/// job: the standalone table is marked (`ExchangePartitionInfo`) under one
/// version, then the ids swap under the next; a row that fails the
/// validation rolls the job back after the marking version (captured from
/// Go on this branch).
#[test]
fn exchange_partition_runs_as_a_persisted_job_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1954)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    let version = |session: &mut Session| -> i64 {
        displayed(rows(session, "admin show ddl"))[0][0]
            .parse()
            .unwrap()
    };
    let job = |session: &mut Session| -> String {
        let row = displayed(rows(session, "admin show ddl jobs 1")).remove(0);
        [1, 2, 3, 4, 11].map(|column| row[column].clone()).join("|")
    };
    rows(
        &mut session,
        "create table pt (a int, b int) partition by range (a) \
         (partition p0 values less than (10), partition p1 values less than (20))",
    );
    rows(&mut session, "create table nt (a int, b int)");
    rows(&mut session, "insert into pt values (1,1),(11,11)");
    rows(&mut session, "insert into nt values (2,2),(3,3)");
    let before = version(&mut session);
    rows(
        &mut session,
        "alter table pt exchange partition p0 with table nt",
    );
    assert_eq!(
        displayed(rows(&mut session, "show warnings")),
        [[
            "Warning",
            "1105",
            "after the exchange, please analyze related table of the exchange to update statistics"
        ]]
    );
    assert_eq!(version(&mut session) - before, 2);
    assert_eq!(job(&mut session), "test|pt|exchange partition|none|synced");
    assert_eq!(
        displayed(rows(&mut session, "select * from pt order by a")),
        [["2", "2"], ["3", "3"], ["11", "11"]]
    );
    assert_eq!(
        displayed(rows(&mut session, "select * from nt")),
        [["1", "1"]]
    );

    rows(&mut session, "insert into nt values (15,15)");
    let before = version(&mut session);
    let error = session
        .node_conn()
        .execute_write("alter table pt exchange partition p0 with table nt")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1737, "Found a row that does not match the partition")
    );
    assert_eq!(version(&mut session) - before, 2);
    assert_eq!(
        job(&mut session),
        "test|nt|exchange partition|public|rollback done"
    );
    // The rollback lifted the restriction: nt takes any row again.
    rows(&mut session, "insert into nt values (16,16)");
    rows(&mut session, "delete from nt where a = 16");

    let before = version(&mut session);
    rows(
        &mut session,
        "alter table pt exchange partition p0 with table nt without validation",
    );
    assert_eq!(version(&mut session) - before, 2);
    assert_eq!(
        displayed(rows(
            &mut session,
            "select * from pt partition (p0) order by a"
        )),
        [["1", "1"], ["15", "15"]]
    );
    assert_eq!(
        displayed(rows(&mut session, "select * from nt order by a")),
        [["2", "2"], ["3", "3"]]
    );
    let error = session
        .node_conn()
        .execute_write("alter table pt exchange partition p9 with table nt")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1735, "Unknown partition 'p9' in table 'pt'")
    );
}

/// DROP and TRUNCATE PARTITION clean the old partitions' rows out of the
/// global indexes in their delete-reorganization step (Go
/// `cleanGlobalIndexEntriesFromDroppedPartitions`): their unique values are
/// free again, `ADMIN CHECK TABLE` passes, and the job counts one entry per
/// row and global index (captured from Go on this branch).
#[test]
fn dropped_partitions_leave_the_global_indexes_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1953)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    let job = |session: &mut Session| -> String {
        let row = displayed(rows(session, "admin show ddl jobs 1")).remove(0);
        [1, 2, 3, 4, 7, 11]
            .map(|column| row[column].clone())
            .join("|")
    };
    rows(
        &mut session,
        "create table g (a int, b int, c int, unique key ub(b) global, key kc(c) global) \
         partition by range (a) (partition p0 values less than (10), \
         partition p1 values less than (20), partition p2 values less than (30))",
    );
    rows(
        &mut session,
        "insert into g values (1,1,1),(2,2,2),(11,11,11),(21,21,21)",
    );
    rows(&mut session, "alter table g drop partition p0");
    assert_eq!(job(&mut session), "test|g|drop partition|none|4|synced");
    rows(&mut session, "insert into g values (25,1,1)");
    rows(&mut session, "admin check table g");
    assert_eq!(
        displayed(rows(
            &mut session,
            "select * from g use index(ub) where b = 1"
        )),
        [["25", "1", "1"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "select a from g use index(kc) where c in (1,2) order by a"
        )),
        [["25"]]
    );
    rows(&mut session, "alter table g truncate partition p1");
    assert_eq!(job(&mut session), "test|g|truncate partition|none|2|synced");
    rows(&mut session, "insert into g values (12,11,11)");
    rows(&mut session, "admin check table g");
    assert_eq!(
        displayed(rows(
            &mut session,
            "select a from g use index(kc) order by a"
        )),
        [["12"], ["21"], ["25"]]
    );
}

/// Go master 328460a9af0 (`executor/update` integration case, recorded
/// results): after EXCHANGE PARTITION two partitions of a non-clustered table
/// hold the same `_tidb_rowid`, so a multi-table UPDATE tracks each matched row
/// by its partition and handle; a clustered table keeps its handle.
#[test]
fn multi_table_update_tracks_rows_by_partition_after_exchange() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = stack.factory.open_session(session_context(941)).unwrap();
    let mut update = |tk: &mut Session, sql: &str| {
        let affected = tk
            .node_conn()
            .execute_write(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
            .expect("an UPDATE reports its rows")
            .affected_rows;
        assert_eq!(affected, 2, "{sql}");
    };
    let rows_of = |tk: &mut Session, sql: &str| displayed(rows(tk, sql));
    rows(&mut tk, "use test");
    for (create_tp, create_tx, insert_tp, insert_tx) in [
        (
            "create table tp (a int, b int, c int, key (b), key (c)) partition by range (a) (partition p0 values less than (10), partition p1 values less than (20))",
            "create table tx (a int, b int, c int, key (b), key (c))",
            "insert into tp values (1, 1, 1)",
            "insert into tx values (11, 11, 11)",
        ),
        (
            "create table tp (a int, b int, c int, s int as (b + c) stored, v int as (b * 10 + c) virtual, key (s), key (v)) partition by range (a) (partition p0 values less than (10), partition p1 values less than (20))",
            "create table tx (a int, b int, c int, s int as (b + c) stored, v int as (b * 10 + c) virtual, key (s), key (v))",
            "insert into tp (a, b, c) values (1, 1, 1)",
            "insert into tx (a, b, c) values (11, 11, 11)",
        ),
    ] {
        rows(&mut tk, "drop table if exists tp, tx");
        for sql in [create_tp, create_tx, insert_tp, insert_tx, "alter table tp exchange partition p1 with table tx"] {
            rows(&mut tk, sql);
        }
        let generated = create_tp.contains(" s int ");
        update(&mut tk, "update tp t1, tp t2 set t1.b = 100, t2.c = 200 where t1.a = 1 and t2.a = 11");
        let expected: &[&[&str]] = if generated {
            &[&["1", "100", "1", "101", "1001", "1"], &["11", "11", "200", "211", "310", "1"]]
        } else {
            &[&["1", "100", "1", "1"], &["11", "11", "200", "1"]]
        };
        assert_eq!(rows_of(&mut tk, "select *, _tidb_rowid from tp order by a"), expected);
        update(&mut tk, "update tp t1, tp t2 set t1.b = 300, t2.b = 400 where t1.a = 1 and t2.a = 11");
        let expected: &[&[&str]] = if generated {
            &[&["1", "300", "1", "301", "3001", "1"], &["11", "400", "200", "600", "4200", "1"]]
        } else {
            &[&["1", "300", "1", "1"], &["11", "400", "200", "1"]]
        };
        assert_eq!(rows_of(&mut tk, "select *, _tidb_rowid from tp order by a"), expected);
        if !generated {
            update(&mut tk, "update tp t1, tp t2 set t1.c = t2.b, t2.c = t1.b where t1.a = 1 and t2.a = 11");
            assert_eq!(
                rows_of(&mut tk, "select *, _tidb_rowid from tp order by a"),
                [["1", "300", "400", "1"], ["11", "400", "300", "1"]]
            );
            update(&mut tk, "update tp t1, tp t2 set t1.b = 1, t2.c = 2 where t1.a = 1 and t2.a = 1");
            assert_eq!(
                rows_of(&mut tk, "select *, _tidb_rowid from tp order by a"),
                [["1", "1", "2", "1"], ["11", "400", "300", "1"]]
            );
        }
        rows(&mut tk, "admin check table tp");
    }
    rows(&mut tk, "drop table if exists tp, tx");
    rows(&mut tk, "create table tp (a int, b int, c int, primary key (a) clustered) partition by range (a) (partition p0 values less than (10), partition p1 values less than (20))");
    rows(&mut tk, "create table tx (a int, b int, c int, primary key (a) clustered)");
    rows(&mut tk, "insert into tp values (1, 1, 1)");
    rows(&mut tk, "insert into tx values (11, 11, 11)");
    rows(&mut tk, "alter table tp exchange partition p1 with table tx");
    update(&mut tk, "update tp t1, tp t2 set t1.b = 100, t2.c = 200 where t1.a = 1 and t2.a = 11");
    assert_eq!(rows_of(&mut tk, "select * from tp order by a"), [["1", "100", "1"], ["11", "11", "200"]]);
}
