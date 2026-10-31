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


    use crate::*;
    
    use std::sync::Arc;
    use tidb_infoschema::*;
    use tidb_stats_handle::StatsHandle;
    use tidb_stats_handle_syncload::sync_load::StatisticsLoadWorkers;
    use crate::driver::CatalogStatisticsLoad;
    use tidb_stats_handle::catalog_statistics::CatalogStatistics;
    
    

    static STATS_LOAD_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[derive(Default)]
    struct RecordingLoader {
        requests: std::sync::Mutex<Vec<tidb_model::StatsLoadItem>>,
        delay: std::time::Duration,
        failure: Option<String>,
    }

    impl tidb_stats_handle_syncload::sync_load::StatisticsItemLoader for RecordingLoader {
        fn load_items(
            &self,
            items: &[tidb_model::StatsLoadItem],
            _resource_group: &str,
        ) -> Result<Vec<(i64, Arc<tidb_stats::table_statistics::TableStatistics>)>, String> {
            *self
                .requests
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = items.to_vec();
            if !self.delay.is_zero() {
                std::thread::sleep(self.delay);
            }
            match &self.failure {
                Some(error) => Err(error.clone()),
                None => Ok(Vec::new()),
            }
        }
    }

    fn analyzed_lite_catalog() -> (Catalog, Arc<StatsHandle>, i64, i64) {
        let handle = Arc::new(StatsHandle::default());
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        crate::run_create_table_on("CREATE TABLE t(a INT, b INT, KEY ia(a))", &mut catalog)
            .expect("fixture DDL");
        let TableEntry::Kv(table) = catalog.get_in("test", "t").expect("fixture table") else {
            panic!("fixture is not a KV table")
        };
        let table_id = table.table_id();
        let column_id = table.columns()[0].id;
        let statistics = tidb_stats::table_statistics::TableStatistics {
            pseudo: false,
            row_count: 10,
            column_load_status: [(column_id, tidb_stats::StatsLoadedStatus::all_evicted())]
                .into_iter()
                .collect(),
            column_stats_existence: [(column_id, true)].into_iter().collect(),
            ..tidb_stats::table_statistics::TableStatistics::default()
        };
        handle.set_table_statistics(table_id, Arc::new(statistics));
        (catalog, handle, table_id, column_id)
    }

    fn index_pruning_catalog(
        partitioned: bool,
    ) -> (Catalog, Arc<StatsHandle>, i64, Vec<i64>, Vec<(String, i64)>) {
        let handle = Arc::new(StatsHandle::default());
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        let partition_clause = if partitioned {
            " PARTITION BY HASH(a) PARTITIONS 4"
        } else {
            ""
        };
        crate::run_create_table_on(
            &format!(
                "CREATE TABLE t(\
                    a INT, b INT, c INT, d INT, e INT, f INT, g INT, h INT,\
                    i INT, j INT, k INT, l INT, m INT,\
                    KEY ia(a), KEY iab(a,b), KEY iac(a,c), KEY iad(a,d),\
                    KEY iae(a,e), KEY iaf(a,f), KEY iag(a,g), KEY iah(a,h),\
                    KEY iai(a,i), KEY iaj(a,j), KEY iak(a,k), KEY ial(a,l),\
                    KEY iam(a,m), KEY ib(b), KEY ibc(b,c), KEY ibd(b,d),\
                    KEY ibe(b,e), KEY ic(c), KEY icd(c,d), KEY ice(c,e),\
                    KEY icf(c,f), KEY id(d), KEY ide(d,e), KEY idf(d,f),\
                    KEY ie(e), KEY ief(e,f), KEY if_idx(f)\
                ){partition_clause}"
            ),
            &mut catalog,
        )
        .expect("index-pruning fixture DDL");
        let TableEntry::Kv(table) = catalog.get_in("test", "t").expect("fixture table") else {
            panic!("fixture is not a KV table")
        };
        let table_id = table.table_id();
        let partition_ids = table
            .partition()
            .map(|partition| {
                partition
                    .definitions
                    .iter()
                    .map(|definition| definition.id)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let column_ids = table
            .columns()
            .iter()
            .map(|column| column.id)
            .collect::<Vec<_>>();
        let indexes = table
            .indexes()
            .iter()
            .map(|index| (index.name.clone(), index.id))
            .collect::<Vec<_>>();
        let statistics = tidb_stats::table_statistics::TableStatistics {
            pseudo: false,
            row_count: 4,
            column_load_status: column_ids
                .iter()
                .copied()
                .map(|id| (id, tidb_stats::StatsLoadedStatus::all_evicted()))
                .collect(),
            index_load_status: indexes
                .iter()
                .map(|(_, id)| (*id, tidb_stats::StatsLoadedStatus::all_evicted()))
                .collect(),
            column_stats_existence: column_ids.iter().copied().map(|id| (id, true)).collect(),
            index_stats_existence: indexes.iter().map(|(_, id)| (*id, true)).collect(),
            ..tidb_stats::table_statistics::TableStatistics::default()
        };
        for physical_id in std::iter::once(table_id).chain(partition_ids.iter().copied()) {
            handle.set_table_statistics(physical_id, Arc::new(statistics.clone()));
        }
        (catalog, handle, table_id, partition_ids, indexes)
    }

    fn clear_async_statistics_items() {
        for item in tidb_stats_asyncload::ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS.all_items() {
            tidb_stats_asyncload::ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS.delete(item.table_item_id);
        }
    }

    fn requested_index_ids(table_id: i64) -> std::collections::HashSet<i64> {
        tidb_stats_asyncload::ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS
            .all_items()
            .into_iter()
            .filter_map(|item| {
                (item.table_item_id.table_id == table_id && item.table_item_id.is_index)
                    .then_some(item.table_item_id.id)
            })
            .collect()
    }

    fn assert_only_indexes_starting_with_a_are_requested(
        requested: &std::collections::HashSet<i64>,
        indexes: &[(String, i64)],
        capped: bool,
    ) {
        let starting_with_a = indexes
            .iter()
            .filter_map(|(name, id)| name.starts_with("ia").then_some(*id))
            .collect::<std::collections::HashSet<_>>();
        let unrelated = indexes
            .iter()
            .filter_map(|(name, id)| (!name.starts_with("ia")).then_some(*id))
            .collect::<std::collections::HashSet<_>>();
        assert!(!requested.is_empty());
        assert!(requested.is_disjoint(&unrelated));
        assert!(requested.is_subset(&starting_with_a));
        if capped {
            assert!(requested.len() < starting_with_a.len());
            assert!(requested.len() <= 10);
        }
    }

    #[test]
    fn determinate_load_requests_one_analyzed_column_per_visited_table() {
        let _guard = STATS_LOAD_TEST_LOCK.lock().unwrap();
        let (mut catalog, handle, table_id, column_id) = analyzed_lite_catalog();
        let loader = Arc::new(RecordingLoader::default());
        handle.set_item_loader(loader.clone(), StatisticsLoadWorkers::new());
        let usage = tidb_stats::table_statistics::ColumnStatsUsage {
            visited_logical_table_ids: [table_id].into_iter().collect(),
            ..Default::default()
        };

        let context = crate::StmtContext::for_query().with_stats_handle(Arc::clone(&handle)).with_stats_load_policy(100, true, 0);
        catalog
            .request_statistics_load(&usage, &context)
            .expect("start synchronous load");
        catalog
            .wait_statistics_load(&context)
            .expect("finish synchronous load");
        let requests = loader
            .requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(requests.len(), 1);
        assert!(requests.iter().any(|request| {
            request.table_item_id.table_id == table_id
                && request.table_item_id.id == column_id
                && !request.table_item_id.is_index
                && request.full_load
        }));
    }

    #[test]
    fn a_singleflight_transport_timeout_activates_statement_failure_before_wait_timer() {
        let handle = Arc::new(StatsHandle::default());
        let _guard = STATS_LOAD_TEST_LOCK.lock().unwrap();
        // Go master TestSyncWaitStatsLoadWithFailedResultBeforeTimer: prepare
        // the result before waiting instead of relying on two racing timers.
        for pseudo in [false, true] {
            let catalog = tidb_infoschema_perfschema::mock_catalog();
            let requested = tidb_model::StatsLoadItem {
                table_item_id: tidb_model::TableItemID {
                    table_id: 9_800_001,
                    id: 1,
                    is_index: false,
                    is_sync_load_failed: false,
                },
                full_load: true,
            };
            let (sender, receiver) = std::sync::mpsc::sync_channel(1);
            sender
                .send(tidb_stats_handle_syncload::sync_load::SyncLoadOutcome::TransportError(
                    "sync load took too long to return".to_owned(),
                ))
                .unwrap();
            let context = crate::StmtContext::for_query().with_stats_handle(Arc::clone(&handle)).with_stats_load_policy(100, pseudo, 1000);
            context.install_pending_statistics_load(
                vec![receiver],
                vec![requested],
                std::time::Duration::from_secs(1),
            );
            let consumed = tidb_stats_handle_metrics::SYNC_LOAD_TOTAL.get();
            let timeouts = tidb_stats_handle_metrics::SYNC_LOAD_TIMEOUT_TOTAL.get();
            let result = catalog.wait_statistics_load(&context);
            assert!(tidb_stats_handle_metrics::SYNC_LOAD_TOTAL.get() >= consumed + 1.0);
            assert!(tidb_stats_handle_metrics::SYNC_LOAD_TIMEOUT_TOTAL.get() >= timeouts + 1.0);
            assert!(
                context.sync_stats_failed(),
                "an undelivered item must fail the statement load"
            );
            if pseudo {
                result.expect("pseudo timeout falls back");
                assert!(context.skip_plan_cache());
                assert_eq!(context.take_warnings().len(), 1);
            } else {
                assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains("some requested items are not loaded in time"));
                assert!(!context.skip_plan_cache());
            }
            let mut failed = requested.table_item_id;
            failed.is_sync_load_failed = true;
            assert!(tidb_stats_asyncload::ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS
                .all_items()
                .iter()
                .any(|item| item.table_item_id == failed));
            tidb_stats_asyncload::ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS.delete(failed);
            assert!(context.take_pending_statistics_load().is_none());
        }
    }

    #[test]
    fn delivered_worker_errors_complete_the_wait_and_record_latency() {
        let handle = Arc::new(StatsHandle::default());
        let _guard = STATS_LOAD_TEST_LOCK.lock().unwrap();
        use tidb_stats_handle_metrics::{SYNC_LOAD_HISTOGRAM, SYNC_LOAD_TOTAL};
        let catalog = tidb_infoschema_perfschema::mock_catalog();
        let requested = tidb_model::StatsLoadItem {
            table_item_id: tidb_model::TableItemID {
                table_id: 9_800_002,
                id: 1,
                is_index: false,
                is_sync_load_failed: false,
            },
            full_load: true,
        };
        let context = crate::StmtContext::for_query().with_stats_handle(Arc::clone(&handle)).with_stats_load_policy(100, false, 1000);
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        sender
            .send(tidb_stats_handle_syncload::sync_load::SyncLoadOutcome::Item {
                item: requested.table_item_id,
                error: Some("storage read failed after retry".to_owned()),
            })
            .unwrap();
        context.install_pending_statistics_load(
            vec![receiver],
            vec![requested],
            std::time::Duration::from_secs(1),
        );
        let count = SYNC_LOAD_TOTAL.get();
        let observed = SYNC_LOAD_HISTOGRAM.get_sample_count();
        let elapsed = SYNC_LOAD_HISTOGRAM.get_sample_sum();
        std::thread::sleep(std::time::Duration::from_millis(10));
        catalog
            .wait_statistics_load(&context)
            .expect("worker errors are delivered results");
        assert!(!context.sync_stats_failed());
        let mut failed = requested.table_item_id;
        failed.is_sync_load_failed = true;
        assert!(!tidb_stats_asyncload::ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS
            .all_items()
            .iter()
            .any(|item| item.table_item_id == failed));
        assert!(SYNC_LOAD_TOTAL.get() >= count + 1.0);
        assert!(SYNC_LOAD_HISTOGRAM.get_sample_count() > observed);
        assert!(
            SYNC_LOAD_HISTOGRAM.get_sample_sum() >= elapsed + 10.0,
            "latency includes time between submission and waiting"
        );
    }

    #[test]
    fn statistics_wait_timeout_and_closed_channel_record_consumption() {
        let handle = Arc::new(StatsHandle::default());
        let _guard = STATS_LOAD_TEST_LOCK.lock().unwrap();
        use tidb_stats_handle_metrics::{SYNC_LOAD_TIMEOUT_TOTAL, SYNC_LOAD_TOTAL};
        for closed in [false, true] {
            let catalog = tidb_infoschema_perfschema::mock_catalog();
            let requested = tidb_model::StatsLoadItem {
                table_item_id: tidb_model::TableItemID {
                    table_id: 9_800_003 + i64::from(closed),
                    id: 1,
                    is_index: false,
                    is_sync_load_failed: false,
                },
                full_load: true,
            };
            let context = crate::StmtContext::for_query().with_stats_handle(Arc::clone(&handle)).with_stats_load_policy(100, false, 1);
            let (sender, receiver) = std::sync::mpsc::sync_channel(1);
            let sender = if closed {
                drop(sender);
                None
            } else {
                Some(sender)
            };
            context.install_pending_statistics_load(
                vec![receiver],
                vec![requested],
                std::time::Duration::from_millis(1),
            );
            let count = SYNC_LOAD_TOTAL.get();
            let timeouts = SYNC_LOAD_TIMEOUT_TOTAL.get();
            let error = catalog.wait_statistics_load(&context).unwrap_err();
            assert!(error.to_string().contains(if closed {
                "channel closed unexpectedly"
            } else {
                "sync load stats timeout"
            }));
            assert!(context.sync_stats_failed());
            assert!(SYNC_LOAD_TOTAL.get() >= count + 1.0);
            if !closed {
                assert!(SYNC_LOAD_TIMEOUT_TOTAL.get() >= timeouts + 1.0);
            }
            let mut failed = requested.table_item_id;
            failed.is_sync_load_failed = true;
            tidb_stats_asyncload::ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS.delete(failed);
            drop(sender);
        }
    }

    #[test]
    fn histograms_in_flight_cleans_completed_statistics_items() {
        let _guard = STATS_LOAD_TEST_LOCK.lock().unwrap();
        let (mut catalog, handle, table_id, column_id) = analyzed_lite_catalog();
        handle.set_item_loader(
            Arc::new(RecordingLoader {
                delay: std::time::Duration::from_millis(40),
                ..RecordingLoader::default()
            }),
            StatisticsLoadWorkers::new(),
        );
        let usage = tidb_stats::table_statistics::ColumnStatsUsage {
            predicate_columns: [(
                tidb_model::TableItemID {
                    table_id,
                    id: column_id,
                    is_index: false,
                    is_sync_load_failed: false,
                },
                true,
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        let context = crate::StmtContext::for_query().with_stats_handle(Arc::clone(&handle)).with_stats_load_policy(0, true, 0);

        catalog
            .request_statistics_load(&usage, &context)
            .expect("start asynchronous load");
        assert_eq!(catalog.clean_needed_statistics_items(&handle), 1);
        catalog
            .load_needed_histograms(&handle, "")
            .expect("domain tick drains asynchronous demand");
        assert_eq!(catalog.clean_needed_statistics_items(&handle), 0);
    }

    #[test]
    fn asynchronous_column_metadata_request_uses_full_load_eligibility() {
        let _guard = STATS_LOAD_TEST_LOCK.lock().unwrap();
        clear_async_statistics_items();
        let (mut catalog, handle, table_id, column_id) = analyzed_lite_catalog();
        let loader = Arc::new(RecordingLoader::default());
        handle.set_item_loader(loader.clone(), StatisticsLoadWorkers::new());
        let requested = tidb_model::TableItemID {
            table_id,
            id: column_id,
            is_index: false,
            is_sync_load_failed: false,
        };
        tidb_stats_asyncload::ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS.insert(requested, false);

        catalog
            .load_needed_histograms(&handle, "")
            .expect("metadata-only asynchronous load");
        assert_eq!(
            loader
                .requests
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_slice(),
            &[tidb_model::StatsLoadItem {
                table_item_id: requested,
                full_load: false,
            }]
        );
    }

    #[test]
    fn asynchronous_index_request_is_always_full_load() {
        let _guard = STATS_LOAD_TEST_LOCK.lock().unwrap();
        clear_async_statistics_items();
        let (mut catalog, handle, table_id, _) = analyzed_lite_catalog();
        let index_id = catalog
            .kv_table_by_id(table_id)
            .and_then(|table| table.indexes().first())
            .expect("fixture index ia")
            .id;
        let mut statistics = catalog
            .table_statistics_in(&handle, table_id)
            .expect("fixture statistics")
            .as_ref()
            .clone();
        statistics
            .index_load_status
            .insert(index_id, tidb_stats::StatsLoadedStatus::all_evicted());
        statistics.index_stats_existence.insert(index_id, true);
        handle.set_table_statistics(table_id, Arc::new(statistics));
        let loader = Arc::new(RecordingLoader::default());
        handle.set_item_loader(loader.clone(), StatisticsLoadWorkers::new());
        let requested = tidb_model::TableItemID {
            table_id,
            id: index_id,
            is_index: true,
            is_sync_load_failed: false,
        };
        tidb_stats_asyncload::ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS.insert(requested, false);

        catalog
            .load_needed_histograms(&handle, "")
            .expect("asynchronous index load");
        assert_eq!(
            loader
                .requests
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_slice(),
            &[tidb_model::StatsLoadItem {
                table_item_id: requested,
                full_load: true,
            }]
        );
    }

    #[test]
    fn failed_async_load_removes_the_item_and_returns_the_storage_error() {
        let _guard = STATS_LOAD_TEST_LOCK.lock().unwrap();
        let (mut catalog, handle, table_id, column_id) = analyzed_lite_catalog();
        let loader = Arc::new(RecordingLoader {
            failure: Some("corrupted histogram bound".to_owned()),
            ..RecordingLoader::default()
        });
        handle.set_item_loader(loader.clone(), StatisticsLoadWorkers::new());
        let requested = tidb_model::TableItemID {
            table_id,
            id: column_id,
            is_index: false,
            is_sync_load_failed: false,
        };
        let usage = tidb_stats::table_statistics::ColumnStatsUsage {
            predicate_columns: [(requested, true)].into_iter().collect(),
            ..Default::default()
        };
        let context = crate::StmtContext::for_query().with_stats_handle(Arc::clone(&handle)).with_stats_load_policy(0, true, 0);

        catalog
            .request_statistics_load(&usage, &context)
            .expect("start asynchronous load");
        assert!(catalog.load_needed_histograms(&handle, "").is_err());
        assert!(!tidb_stats_asyncload::ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS
            .all_items()
            .iter()
            .any(|item| item.table_item_id == requested));
        assert_eq!(
            loader
                .requests
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_slice(),
            &[tidb_model::StatsLoadItem {
                table_item_id: requested,
                full_load: true,
            }]
        );
    }

    #[test]
    fn load_column_statistics_after_table_drop() {
        let handle = Arc::new(StatsHandle::default());
        let _guard = STATS_LOAD_TEST_LOCK.lock().unwrap();
        clear_async_statistics_items();
        let (_, handle, table_id, column_id) = analyzed_lite_catalog();
        let dropped = tidb_model::TableItemID {
            table_id,
            id: column_id,
            is_index: false,
            is_sync_load_failed: false,
        };
        tidb_stats_asyncload::ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS.insert(dropped, true);
        // The table is dropped after queueing the column: the next schema
        // image no longer has it.
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        let loader = Arc::new(RecordingLoader::default());
        handle.set_item_loader(loader, StatisticsLoadWorkers::new());

        catalog
            .load_needed_histograms(&handle, "")
            .expect("a dropped table is a successful skip");
        assert!(!tidb_stats_asyncload::ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS
            .all_items()
            .iter()
            .any(|item| item.table_item_id == dropped));
    }

    /// A queued column the table no longer has (what `DROP COLUMN` leaves)
    /// is a successful skip.
    #[test]
    fn load_statistics_for_a_column_the_table_does_not_have() {
        let _guard = STATS_LOAD_TEST_LOCK.lock().unwrap();
        clear_async_statistics_items();
        let (mut catalog, handle, table_id, _) = analyzed_lite_catalog();
        let column_id = catalog
            .kv_table_by_id(table_id)
            .and_then(|table| table.columns().iter().map(|column| column.id).max())
            .expect("fixture columns")
            + 1;
        let loader = Arc::new(RecordingLoader::default());
        handle.set_item_loader(loader, StatisticsLoadWorkers::new());
        let dropped = tidb_model::TableItemID {
            table_id,
            id: column_id,
            is_index: false,
            is_sync_load_failed: false,
        };
        tidb_stats_asyncload::ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS.insert(dropped, true);
        catalog
            .load_needed_histograms(&handle, "")
            .expect("a dropped column is a successful skip");
        assert!(!tidb_stats_asyncload::ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS
            .all_items()
            .iter()
            .any(|item| item.table_item_id == dropped));
    }

    #[test]
    fn load_index_statistics_after_table_drop() {
        let _guard = STATS_LOAD_TEST_LOCK.lock().unwrap();
        clear_async_statistics_items();
        let (catalog, handle, table_id, _) = analyzed_lite_catalog();
        let index_id = catalog
            .kv_table_by_id(table_id)
            .and_then(|table| table.indexes().first())
            .expect("fixture index ia")
            .id;
        let dropped = tidb_model::TableItemID {
            table_id,
            id: index_id,
            is_index: true,
            is_sync_load_failed: false,
        };
        tidb_stats_asyncload::ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS.insert(dropped, true);
        // The table is dropped after queueing the index: the next schema
        // image no longer has it.
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        let loader = Arc::new(RecordingLoader::default());
        handle.set_item_loader(loader, StatisticsLoadWorkers::new());

        catalog
            .load_needed_histograms(&handle, "")
            .expect("a dropped table is a successful skip");
        assert!(!tidb_stats_asyncload::ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS
            .all_items()
            .iter()
            .any(|item| item.table_item_id == dropped));
    }

    /// A queued index the table no longer has (what `DROP INDEX` leaves) is
    /// a successful skip.
    #[test]
    fn load_statistics_for_an_index_the_table_does_not_have() {
        let _guard = STATS_LOAD_TEST_LOCK.lock().unwrap();
        clear_async_statistics_items();
        let (mut catalog, handle, table_id, _) = analyzed_lite_catalog();
        let index_id = catalog
            .kv_table_by_id(table_id)
            .and_then(|table| table.indexes().iter().map(|index| index.id).max())
            .expect("fixture index ia")
            + 1;
        let loader = Arc::new(RecordingLoader::default());
        handle.set_item_loader(loader, StatisticsLoadWorkers::new());
        let dropped = tidb_model::TableItemID {
            table_id,
            id: index_id,
            is_index: true,
            is_sync_load_failed: false,
        };
        tidb_stats_asyncload::ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS.insert(dropped, true);
        catalog
            .load_needed_histograms(&handle, "")
            .expect("a dropped index is a successful skip");
        assert!(!tidb_stats_asyncload::ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS
            .all_items()
            .iter()
            .any(|item| item.table_item_id == dropped));
    }

    /// Go `collectSyncIndices` and `markAtLeastOneFullStatsLoadForEachTable`
    /// read `tblStats.Pseudo` off the handle's cached table, not
    /// `GetStatsTable`'s copy: an analyzed table whose `stats_meta` count is
    /// zero is pseudo only for planning and still asks for its loads.
    #[test]
    fn a_zero_count_analyzed_table_still_requests_statistics_loads() {
        let _guard = STATS_LOAD_TEST_LOCK.lock().unwrap();
        let (mut catalog, handle, table_id, column_id) = analyzed_lite_catalog();
        let index_id = catalog
            .kv_table_by_id(table_id)
            .and_then(|table| table.indexes().first())
            .expect("fixture index")
            .id;
        let statistics = tidb_stats::table_statistics::TableStatistics {
            // `GetStatsTable` rule 2 makes this table's planner copy pseudo,
            // while the cached `HistColl.Pseudo` stays false.
            pseudo: true,
            cache_pseudo: false,
            row_count: 0,
            modify_count: 3,
            column_load_status: [(column_id, tidb_stats::StatsLoadedStatus::all_evicted())]
                .into_iter()
                .collect(),
            index_load_status: [(index_id, tidb_stats::StatsLoadedStatus::all_evicted())]
                .into_iter()
                .collect(),
            column_stats_existence: [(column_id, true)].into_iter().collect(),
            index_stats_existence: [(index_id, true)].into_iter().collect(),
            ..tidb_stats::table_statistics::TableStatistics::default()
        };
        handle.set_table_statistics(table_id, Arc::new(statistics));
        let column = tidb_model::TableItemID {
            table_id,
            id: column_id,
            is_index: false,
            is_sync_load_failed: false,
        };
        let index = tidb_model::TableItemID {
            table_id,
            id: index_id,
            is_index: true,
            is_sync_load_failed: false,
        };

        let predicate = tidb_stats::table_statistics::ColumnStatsUsage {
            predicate_columns: [(column, true)].into_iter().collect(),
            ..Default::default()
        };
        let items = catalog.statistics_load_items(&handle, &predicate, true);
        assert!(
            items
                .iter()
                .any(|item| item.table_item_id == index && item.full_load),
            "the predicate column's index is requested: {items:?}"
        );

        let visited = tidb_stats::table_statistics::ColumnStatsUsage {
            visited_logical_table_ids: [table_id].into_iter().collect(),
            ..Default::default()
        };
        let items = catalog.statistics_load_items(&handle, &visited, true);
        assert!(
            items
                .iter()
                .any(|item| item.table_item_id == column && item.full_load),
            "a visited table without full statistics loads one column: {items:?}"
        );
    }

    /// Go `NewPlanCacheKey`'s `statsVerHash` sums
    /// `getLatestVersionFromStatsTable`: the largest column/index
    /// `LastUpdateVersion`, or zero for a table `GetStatsTable` rule 2 makes
    /// pseudo. A stats-delta flush moves only the `stats_meta` version, and Go
    /// keeps the cached plan across it (captured live: a Go tidb-server keeps
    /// `@@last_plan_from_cache = 1` for a prepared SELECT after a flush that a
    /// Rust server re-planned).
    #[test]
    fn plan_cache_stats_hash_follows_histogram_versions_not_stats_meta() {
        let handle = Arc::new(StatsHandle::default());
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        crate::run_create_table_on("CREATE TABLE t(a INT, KEY ia(a))", &mut catalog)
            .expect("fixture DDL");
        let TableEntry::Kv(table) = catalog.get_in("test", "t").expect("fixture table") else {
            panic!("fixture is not a KV table")
        };
        let (table_id, column_id, index_id) =
            (table.table_id(), table.columns()[0].id, table.indexes()[0].id);
        let statistics = |meta_version: u64, row_count: i64, column_version: u64| {
            let histogram = |id, last_update_version| tidb_stats::Histogram {
                id,
                last_update_version,
                ..tidb_stats::Histogram::default()
            };
            let columns = [(
                column_id,
                tidb_stats::column::Column {
                    histogram: histogram(column_id, column_version),
                    top_n: None,
                    cmsketch: None,
                    stats_version: 2,
                    ..Default::default()
                },
            )]
            .into_iter()
            .collect();
            let indexes = [(
                index_id,
                tidb_stats::index::Index {
                    histogram: histogram(index_id, 7),
                    top_n: None,
                    cmsketch: None,
                    stats_version: 2,
                    info: Some(tidb_stats::index::IndexInfo {
                        columns: vec![String::new(); 1],
                        unique: false,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )]
            .into_iter()
            .collect();
            Arc::new(
                tidb_stats::table_statistics::TableStatistics::new(row_count, 0, columns, indexes)
                    .with_stat_versions(meta_version, 7),
            )
        };
        let environment = crate::PreparedPlanCacheEnvironment::default().with_stats_handle(Arc::clone(&handle));
        let keys = [CatalogTableKey::new("test", "t")];

        handle.set_table_statistics(table_id, statistics(100, 10, 9));
        assert_eq!(environment.stats_version_hash(&catalog, &keys), 9);
        handle.set_table_statistics(table_id, statistics(200, 11, 9));
        assert_eq!(
            environment.stats_version_hash(&catalog, &keys),
            9,
            "a stats-delta flush keeps the key"
        );
        handle.set_table_statistics(table_id, statistics(300, 11, 12));
        assert_eq!(environment.stats_version_hash(&catalog, &keys), 12);
        handle.set_table_statistics(table_id, statistics(400, 0, 12));
        assert_eq!(
            environment.stats_version_hash(&catalog, &keys),
            0,
            "a zero-count table hashes as a pseudo table"
        );
    }

    #[test]
    fn pruned_indexes_are_not_requested_for_statistics() {
        let _guard = STATS_LOAD_TEST_LOCK.lock().unwrap();
        let (mut catalog, handle, table_id, column_id) = analyzed_lite_catalog();
        let index_id = catalog
            .kv_table_by_id(table_id)
            .and_then(|table| table.indexes().first())
            .expect("fixture index")
            .id;
        let statistics = tidb_stats::table_statistics::TableStatistics {
            pseudo: false,
            row_count: 10,
            column_load_status: [(column_id, tidb_stats::StatsLoadedStatus::all_evicted())]
                .into_iter()
                .collect(),
            index_load_status: [(index_id, tidb_stats::StatsLoadedStatus::all_evicted())]
                .into_iter()
                .collect(),
            column_stats_existence: [(column_id, true)].into_iter().collect(),
            index_stats_existence: [(index_id, true)].into_iter().collect(),
            ..tidb_stats::table_statistics::TableStatistics::default()
        };
        handle.set_table_statistics(table_id, Arc::new(statistics));
        let column = tidb_model::TableItemID {
            table_id,
            id: column_id,
            is_index: false,
            is_sync_load_failed: false,
        };
        let usage = tidb_stats::table_statistics::ColumnStatsUsage {
            predicate_columns: [(column, true)].into_iter().collect(),
            kept_index_ids: [(table_id, std::collections::HashSet::new())]
                .into_iter()
                .collect(),
            ..Default::default()
        };

        let items = catalog.statistics_load_items(&handle, &usage, true);
        assert!(items.iter().any(|item| item.table_item_id == column));
        assert!(!items.iter().any(|item| item.table_item_id.is_index));
    }

    /// Go `pkg/statistics/handle/handletest/handle_test.go`:
    /// `TestPrunedIndexesNoAsyncStatsLoad` and its dynamic/static partition
    /// variants. Drive the ordinary planner rule and inspect the same global
    /// asynchronous demand boundary that Go drains with
    /// `LoadNeededHistograms`.
    #[test]
    fn pruned_indexes_do_not_enter_async_statistics_demand() {
        let _guard = STATS_LOAD_TEST_LOCK.lock().unwrap();
        clear_async_statistics_items();

        for (partitioned, static_prune) in [(false, false), (true, false), (true, true)] {
            let (catalog, handle, table_id, partition_ids, indexes) = index_pruning_catalog(partitioned);
            let context = crate::StmtContext::for_query().with_stats_handle(Arc::clone(&handle))
                .with_stats_load_policy(0, true, 0)
                .with_opt_index_prune_threshold(1)
                .with_static_partition_prune(static_prune);

            crate::run_select_on("SELECT * FROM t WHERE a > 1", &catalog, &context)
                .expect("ordinary planning and execution");

            if static_prune {
                assert!(!partition_ids.is_empty());
                for partition_id in partition_ids {
                    assert_only_indexes_starting_with_a_are_requested(
                        &requested_index_ids(partition_id),
                        &indexes,
                        false,
                    );
                }
            } else {
                assert_only_indexes_starting_with_a_are_requested(
                    &requested_index_ids(table_id),
                    &indexes,
                    true,
                );
            }
            clear_async_statistics_items();
        }
    }

    #[test]
    fn direct_virtual_column_dependencies_request_expression_index_stats_only() {
        let handle = Arc::new(StatsHandle::default());
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        crate::run_create_table_on(
            "CREATE TABLE t(a INT, v INT AS (a + 1), KEY iv(v))",
            &mut catalog,
        )
        .expect("fixture DDL");
        let TableEntry::Kv(table) = catalog.get_in("test", "t").expect("fixture table") else {
            panic!("fixture is not a KV table")
        };
        let table_id = table.table_id();
        let a = table
            .columns()
            .iter()
            .find(|column| column.name == "a")
            .unwrap()
            .id;
        let v = table
            .columns()
            .iter()
            .find(|column| column.name == "v")
            .unwrap()
            .id;
        let index_id = table.indexes().first().expect("expression index").id;
        handle.set_table_statistics(
            table_id,
            Arc::new(tidb_stats::table_statistics::TableStatistics {
                pseudo: false,
                row_count: 10,
                column_load_status: [
                    (a, tidb_stats::StatsLoadedStatus::all_evicted()),
                    (v, tidb_stats::StatsLoadedStatus::all_evicted()),
                ]
                .into_iter()
                .collect(),
                index_load_status: [(index_id, tidb_stats::StatsLoadedStatus::all_evicted())]
                    .into_iter()
                    .collect(),
                column_stats_existence: [(a, true), (v, true)].into_iter().collect(),
                index_stats_existence: [(index_id, true)].into_iter().collect(),
                ..tidb_stats::table_statistics::TableStatistics::default()
            }),
        );
        let requested = tidb_model::TableItemID {
            table_id,
            id: a,
            is_index: false,
            is_sync_load_failed: false,
        };
        let usage = tidb_stats::table_statistics::ColumnStatsUsage {
            predicate_columns: [(requested, true)].into_iter().collect(),
            ..Default::default()
        };

        let items = catalog.statistics_load_items(&handle, &usage, true);
        assert!(items.iter().any(|item| item.table_item_id == requested));
        assert!(items.iter().any(|item| {
            item.table_item_id.table_id == table_id
                && item.table_item_id.id == index_id
                && item.table_item_id.is_index
                && item.full_load
        }));
        assert!(!items
            .iter()
            .any(|item| { item.table_item_id.id == v && !item.table_item_id.is_index }));
    }

    #[test]
    fn ordinary_planning_runs_both_statistics_rule_points() {
        let _guard = STATS_LOAD_TEST_LOCK.lock().unwrap();
        let (mut catalog, handle, table_id, column_id) = analyzed_lite_catalog();
        let loader = Arc::new(RecordingLoader::default());
        handle.set_item_loader(loader.clone(), StatisticsLoadWorkers::new());
        let usage_list = tidb_stats_handle_usage::SessionStatsList::new();
        let usage = usage_list.new_session_stats_item();
        let context = crate::StmtContext::for_query().with_stats_handle(Arc::clone(&handle))
            .with_stats_load_policy(100, true, 0)
            .with_plan_replayer_capture(true)
            .with_column_stats_usage(crate::stmt_ctx_build::col_stats_usage_updater(&Some(Arc::clone(&usage))));

        crate::run_select_on("SELECT * FROM t WHERE a > 1", &catalog, &context)
            .expect("planned query");

        assert!(context.operator_num() > 0);
        assert_eq!(
            context
                .table_runtime_statistics()
                .get(&table_id)
                .and_then(Option::as_deref)
                .map(|statistics| statistics.row_count),
            Some(10)
        );
        usage_list.sweep_session_stats_list();
        assert!(usage_list
            .session_stats_usage()
            .get_usage_and_reset()
            .contains_key(&tidb_model::TableItemID {
                table_id,
                id: column_id,
                is_index: false,
                is_sync_load_failed: false,
            }));
        assert!(loader
            .requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .any(|item| {
                item.table_item_id.table_id == table_id
                    && item.table_item_id.id == column_id
                    && !item.table_item_id.is_index
                    && item.full_load
            }));
    }

    fn stats_usage_fixture() -> Catalog {
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        crate::run_create_table_on(
            "CREATE TABLE t(a INT NOT NULL PRIMARY KEY, b INT NOT NULL, c INT NOT NULL)",
            &mut catalog,
        )
        .expect("fixture t");
        crate::run_create_table_on(
            "CREATE TABLE t2(a INT UNSIGNED NOT NULL PRIMARY KEY, b INT NOT NULL, c INT UNSIGNED)",
            &mut catalog,
        )
        .expect("fixture t2");
        crate::run_create_table_on("CREATE TABLE t3(a INT, b INT, c INT)", &mut catalog)
            .expect("fixture t3");
        crate::run_create_table_on(
            "CREATE TABLE pt1(a INT NOT NULL, b INT NOT NULL, c INT NOT NULL, ptn INT) \
             PARTITION BY RANGE (ptn) (PARTITION p1 VALUES LESS THAN (16), \
             PARTITION p2 VALUES LESS THAN (32))",
            &mut catalog,
        )
        .expect("fixture pt1");
        catalog
    }

    fn column_item(
        catalog: &Catalog,
        table_name: &str,
        column_name: &str,
    ) -> tidb_model::TableItemID {
        let TableEntry::Kv(table) = catalog.get_in("test", table_name).expect("fixture table")
        else {
            panic!("fixture is not a KV table")
        };
        let column = table
            .columns()
            .iter()
            .find(|column| column.name == column_name)
            .expect("fixture column");
        tidb_model::TableItemID {
            table_id: table.table_id(),
            id: column.id,
            is_index: false,
            is_sync_load_failed: false,
        }
    }

    #[test]
    fn original_collect_predicate_columns_cases_match_before_and_after_optimization() {
        let handle = Arc::new(StatsHandle::default());
        let catalog = stats_usage_fixture();
        let cases: &[(&str, &[(&str, &str)])] = &[
            ("SELECT * FROM t WHERE a > 2", &[("t", "a")]),
            (
                "SELECT * FROM t WHERE b IN (2, 5) OR c = 5",
                &[("t", "b"), ("t", "c")],
            ),
            (
                "SELECT * FROM (SELECT a + b AS ab, c FROM t) AS tmp WHERE ab > 4",
                &[("t", "a"), ("t", "b")],
            ),
            (
                "SELECT b, COUNT(*) FROM t GROUP BY b",
                &[("t", "b")],
            ),
            (
                "SELECT b, SUM(a) FROM t GROUP BY b HAVING SUM(a) > 3",
                &[("t", "a"), ("t", "b")],
            ),
            ("SELECT COUNT(*), SUM(a), SUM(c) FROM t", &[]),
            (
                "(SELECT a, c FROM t) UNION (SELECT a, b FROM t2)",
                &[("t", "a"), ("t", "c"), ("t2", "a"), ("t2", "b")],
            ),
            (
                "SELECT AVG(b) OVER(PARTITION BY a) FROM t",
                &[("t", "a")],
            ),
            (
                "SELECT * FROM (SELECT AVG(b) OVER(PARTITION BY a) AS w FROM t) AS tmp WHERE w > 4",
                &[("t", "a"), ("t", "b")],
            ),
            (
                "SELECT ROW_NUMBER() OVER(PARTITION BY a ORDER BY c) FROM t",
                &[("t", "a")],
            ),
            (
                "SELECT * FROM t, t2 WHERE t.a = t2.a",
                &[("t", "a"), ("t2", "a")],
            ),
            (
                "SELECT * FROM t AS x JOIN t2 AS y ON x.c + y.b > 2",
                &[("t", "c"), ("t2", "b")],
            ),
            (
                "SELECT * FROM t AS x JOIN t2 AS y ON x.a = y.a AND x.c < 3 AND y.b > 2",
                &[("t", "a"), ("t", "c"), ("t2", "a"), ("t2", "b")],
            ),
            (
                "SELECT x.c, y.b, SUM(x.b), SUM(y.a) FROM t AS x JOIN t2 AS y ON x.a < y.a GROUP BY x.c, y.b ORDER BY x.c",
                &[("t", "a"), ("t", "c"), ("t2", "a"), ("t2", "b")],
            ),
            (
                "SELECT * FROM t2 WHERE t2.b > ALL(SELECT b FROM t WHERE t.c > 2)",
                &[("t", "b"), ("t", "c"), ("t2", "b")],
            ),
            (
                "SELECT * FROM t2 WHERE t2.b > ANY(SELECT b FROM t WHERE t.c > 2)",
                &[("t", "b"), ("t", "c"), ("t2", "b")],
            ),
            (
                "SELECT * FROM t2 WHERE t2.b > (SELECT SUM(b) FROM t WHERE t.c > t2.a)",
                &[("t", "b"), ("t", "c"), ("t2", "a"), ("t2", "b")],
            ),
            (
                "SELECT * FROM t2 WHERE t2.b > (SELECT COUNT(*) FROM t WHERE t.a > t2.a)",
                &[("t", "a"), ("t2", "a"), ("t2", "b")],
            ),
            (
                "SELECT * FROM t2 WHERE EXISTS (SELECT * FROM t WHERE t.a > t2.b)",
                &[("t", "a"), ("t2", "b")],
            ),
            (
                "SELECT * FROM t2 WHERE NOT EXISTS (SELECT * FROM t WHERE t.a > t2.b)",
                &[("t", "a"), ("t2", "b")],
            ),
            (
                "SELECT * FROM t2 WHERE t2.a IN (SELECT b FROM t)",
                &[("t", "b"), ("t2", "a")],
            ),
            (
                "SELECT * FROM t2 WHERE t2.a NOT IN (SELECT b FROM t)",
                &[("t", "b"), ("t2", "a")],
            ),
            (
                "SELECT * FROM t ORDER BY c",
                &[("t", "c")],
            ),
            (
                "SELECT * FROM t ORDER BY a + b LIMIT 10",
                &[("t", "a"), ("t", "b")],
            ),
            (
                "SELECT * FROM ((SELECT a, c FROM t) UNION ALL (SELECT a, b FROM t2)) AS tmp WHERE tmp.c > 2",
                &[("t", "c"), ("t2", "b")],
            ),
            (
                "WITH cte(x, y) AS (SELECT a + 1, b FROM t WHERE b > 1) SELECT * FROM cte WHERE x > 3",
                &[("t", "a"), ("t", "b")],
            ),
            (
                "WITH RECURSIVE cte(x, y) AS (SELECT c, 1 FROM t UNION ALL SELECT x + 1, y FROM cte WHERE x < 5) SELECT * FROM cte",
                &[("t", "c")],
            ),
            (
                "WITH RECURSIVE cte(x, y) AS (SELECT 1, c FROM t UNION ALL SELECT x + 1, y FROM cte WHERE x < 5) SELECT * FROM cte WHERE y > 1",
                &[("t", "c")],
            ),
            (
                "WITH RECURSIVE cte(x, y) AS (SELECT a, b FROM t UNION SELECT x + 1, y FROM cte WHERE x < 5) SELECT * FROM cte",
                &[("t", "a"), ("t", "b")],
            ),
        ];

        for (sql, expected) in cases {
            let tidb_ast::Stmt::Query(query) = tidb_parser::parse(sql).expect("original SQL")
            else {
                panic!("original case is not a query")
            };
            let context = crate::StmtContext::for_query().with_stats_handle(Arc::clone(&handle));
            let (before, after) =
                super::planner_bridge::statistics_usage_before_and_after_logical_optimization(
                    &query, &catalog, "test", &context,
                )
                .unwrap_or_else(|error| panic!("{sql}: {error}"));
            let expected = expected
                .iter()
                .map(|(table, column)| column_item(&catalog, table, column))
                .collect::<std::collections::HashSet<_>>();
            assert_eq!(
                before
                    .predicate_columns
                    .keys()
                    .copied()
                    .collect::<std::collections::HashSet<_>>(),
                expected,
                "before optimization: {sql}"
            );
            assert_eq!(
                after
                    .predicate_columns
                    .keys()
                    .copied()
                    .collect::<std::collections::HashSet<_>>(),
                expected,
                "after optimization: {sql}"
            );
        }
    }

    #[test]
    fn original_collect_histogram_needed_columns_cases_match() {
        let handle = Arc::new(StatsHandle::default());
        let catalog = stats_usage_fixture();
        let cases: &[(&str, &[(&str, &str, bool)])] = &[
            ("SELECT * FROM t WHERE a > 2", &[("t", "a", true)]),
            (
                "SELECT * FROM t WHERE b IN (2, 5) OR c = 5",
                &[("t", "b", true), ("t", "c", true)],
            ),
            (
                "SELECT * FROM t WHERE a + b > 1",
                &[("t", "a", true), ("t", "b", true)],
            ),
            ("SELECT * FROM t3 WHERE _tidb_rowid > 1", &[]),
            (
                "SELECT b, COUNT(a) FROM t WHERE b > 1 GROUP BY b HAVING COUNT(a) > 2",
                &[("t", "a", false), ("t", "b", true)],
            ),
            (
                "SELECT * FROM t AS x JOIN t2 AS y ON x.b + y.b > 2 AND x.c > 1 AND y.a < 1",
                &[
                    ("t", "b", false),
                    ("t", "c", true),
                    ("t2", "a", true),
                    ("t2", "b", false),
                ],
            ),
            (
                "SELECT * FROM t2 WHERE t2.b > ALL(SELECT b FROM t WHERE t.c > 2)",
                &[("t", "b", false), ("t", "c", true), ("t2", "b", false)],
            ),
            (
                "SELECT * FROM t2 WHERE t2.b > ANY(SELECT b FROM t WHERE t.c > 2)",
                &[("t", "b", false), ("t", "c", true), ("t2", "b", false)],
            ),
            (
                "SELECT * FROM t2 WHERE t2.b IN (SELECT b FROM t WHERE t.c > 2)",
                &[("t", "b", false), ("t", "c", true), ("t2", "b", false)],
            ),
        ];

        for (sql, expected) in cases {
            let tidb_ast::Stmt::Query(query) = tidb_parser::parse(sql).expect("original SQL")
            else {
                panic!("original case is not a query")
            };
            let context = crate::StmtContext::for_query().with_stats_handle(Arc::clone(&handle));
            let (_, usage) =
                super::planner_bridge::statistics_usage_before_and_after_logical_optimization(
                    &query, &catalog, "test", &context,
                )
                .unwrap_or_else(|error| panic!("{sql}: {error}"));
            let expected = expected
                .iter()
                .map(|(table, column, full)| (column_item(&catalog, table, column), *full))
                .collect::<std::collections::HashMap<_, _>>();
            assert_eq!(usage.predicate_columns, expected, "{sql}");
        }

        let sql = "SELECT * FROM pt1 WHERE ptn < 20 AND b > 1";
        let tidb_ast::Stmt::Query(query) = tidb_parser::parse(sql).expect("partition SQL") else {
            panic!("partition case is not a query")
        };
        let expected = [("pt1", "b"), ("pt1", "ptn")]
            .into_iter()
            .map(|(table, column)| (column_item(&catalog, table, column), true))
            .collect::<std::collections::HashMap<_, _>>();

        let dynamic = crate::StmtContext::for_query().with_stats_handle(Arc::clone(&handle));
        let (_, dynamic_usage) =
            super::planner_bridge::statistics_usage_before_and_after_logical_optimization(
                &query, &catalog, "test", &dynamic,
            )
            .expect("dynamic partition planning");
        assert_eq!(dynamic_usage.predicate_columns, expected);
        assert!(dynamic_usage.table_partition_ids.is_empty());

        let static_prune = crate::StmtContext::for_query().with_stats_handle(Arc::clone(&handle)).with_static_partition_prune(true);
        let (_, static_usage) =
            super::planner_bridge::statistics_usage_before_and_after_logical_optimization(
                &query,
                &catalog,
                "test",
                &static_prune,
            )
            .expect("static partition planning");
        assert_eq!(static_usage.predicate_columns, expected);
        let TableEntry::Kv(table) = catalog.get_in("test", "pt1").expect("fixture pt1") else {
            panic!("fixture pt1 is not a KV table")
        };
        let partition_ids = table
            .partition()
            .expect("partition metadata")
            .definitions
            .iter()
            .map(|definition| definition.id)
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(
            static_usage
                .table_partition_ids
                .get(&table.table_id())
                .expect("static expansion")
                .iter()
                .copied()
                .collect::<std::collections::HashSet<_>>(),
            partition_ids
        );
    }
