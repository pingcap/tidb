# TiDB executor Test Case Map (pkg/executor)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## pkg/executor

### Tests
- `pkg/executor/adapter_test.go` - executor: Tests format SQL.
- `pkg/executor/analyze_test.go` - executor: Tests analyze index extract top n.
- `pkg/executor/analyze_utils_test.go` - executor: Tests get analyze panic err.
- `pkg/executor/batch_point_get_test.go` - executor: Tests batch point get lock exist key.
- `pkg/executor/benchmark_test.go` - executor: Tests shuffle stream agg rows.
- `pkg/executor/brie_test.go` - executor: Tests glue get version.
- `pkg/executor/brie_utils_test.go` - executor: Tests split batch create table with table ID.
- `pkg/executor/checksum_test.go` - executor: Tests checksum.
- `pkg/executor/chunk_size_control_test.go` - executor: Tests limit and table scan.
- `pkg/executor/cluster_table_test.go` - executor: Tests cluster table slow query.
- `pkg/executor/compact_table_test.go` - executor: Tests compact table too busy.
- `pkg/executor/copr_cache_test.go` - executor: Tests integration cop cache.
- `pkg/executor/delete_test.go` - executor: Tests delete lock key.
- `pkg/executor/detach_integration_test.go` - executor: Tests detach all contexts.
- `pkg/executor/detach_test.go` - executor: Tests detach executor.
- `pkg/executor/distribute_table_test.go` - executor: Tests show distribution jobs.
- `pkg/executor/distsql_test.go` - executor: Tests coprocessor client send.
- `pkg/executor/executor_failpoint_test.go` - executor: Tests TiDB last txn info commit mode.
- `pkg/executor/executor_pkg_test.go` - executor: Tests build KV ranges for index join without CWC.
- `pkg/executor/executor_required_rows_test.go` - executor: Tests limit required rows.
- `pkg/executor/explain_test.go` - executor: Tests explain analyze memory.
- `pkg/executor/explain_unit_test.go` - executor: Tests explain analyze invoke next and close.
- `pkg/executor/explainfor_test.go` - executor: Tests explain for.
- `pkg/executor/grant_test.go` - executor: Tests grant global.
- `pkg/executor/historical_stats_test.go` - executor: Tests record history stats after analyze.
- `pkg/executor/hot_regions_history_table_test.go` - executor: Tests TiDB hot regions history.
- `pkg/executor/import_into_test.go` - executor: Tests security enhanced mode.
- `pkg/executor/infoschema_cluster_table_test.go` - executor: Tests skip empty IP nodes for TiDB-type coprocessor.
- `pkg/executor/infoschema_reader_bench_test.go` - executor: Tests infoschema tables.
- `pkg/executor/infoschema_reader_internal_test.go` - executor: Tests set data from check constraints.
- `pkg/executor/infoschema_reader_test.go` - executor: Tests TiFlash system table with TiFlash V620.
- `pkg/executor/insert_test.go` - executor: Tests insert on duplicate key with binlog.
- `pkg/executor/inspection_result_internal_test.go` - executor: Tests convert readable size to byte size.
- `pkg/executor/inspection_result_test.go` - executor: Tests inspection result.
- `pkg/executor/inspection_summary_test.go` - executor: Tests valid inspection summary rules.
- `pkg/executor/join_pkg_test.go` - executor: Tests hash join V2 under apply.
- `pkg/executor/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/executor/memtable_reader_test.go` - executor: Tests metric table data.
- `pkg/executor/metrics_reader_test.go` - executor: Tests stmt label.
- `pkg/executor/parallel_apply_test.go` - executor: Tests parallel apply plan.
- `pkg/executor/partition_table_test.go` - executor: Tests point get with range and list partition table.
- `pkg/executor/pkg_test.go` - executor: Tests nested loop apply.
- `pkg/executor/point_get_test.go` - executor: Tests select check visibility.
- `pkg/executor/prepared_test.go` - executor: Tests prepared null param.
- `pkg/executor/recover_test.go` - executor: Tests recover table.
- `pkg/executor/resource_tag_test.go` - executor: Tests resource group tag.
- `pkg/executor/revoke_test.go` - executor: Tests revoke global.
- `pkg/executor/sample_test.go` - executor: Tests table sample basic.
- `pkg/executor/select_into_test.go` - executor: Tests select into file exists.
- `pkg/executor/select_test.go` - executor: Tests reset context of stmt.
- `pkg/executor/set_test.go` - executor: Tests set var.
- `pkg/executor/show_affinity_test.go` - executor: Tests show affinity.
- `pkg/executor/show_ddl_jobs_test.go` - executor: Tests show comments from job.
- `pkg/executor/show_placement_labels_test.go` - executor: Tests show placement labels builder.
- `pkg/executor/show_placement_test.go` - executor: Tests show placement.
- `pkg/executor/show_stats_test.go` - executor: Tests show stats meta.
- `pkg/executor/show_test.go` - executor: Tests fill one import job info.
- `pkg/executor/shuffle_test.go` - executor: Tests partition range splitter.
- `pkg/executor/simple_test.go` - executor: Tests refresh table stats.
- `pkg/executor/slow_query_sql_test.go` - executor: Tests slow query without slow log.
- `pkg/executor/slow_query_test.go` - executor: Tests parse slow log panic.
- `pkg/executor/split_test.go` - executor: Tests split index.
- `pkg/executor/stmtsummary_test.go` - executor: Tests stmt summary retriever V2 table statement summary.
- `pkg/executor/table_readers_required_rows_test.go` - executor: Tests table reader required rows.
- `pkg/executor/temporary_table_test.go` - executor: Tests normal global temporary table no network.
- `pkg/executor/tikv_regions_peers_table_test.go` - executor: Tests tikv region peers.
- `pkg/executor/trace_test.go` - executor: Tests trace exec.
- `pkg/executor/traffic_test.go` - executor: Tests traffic form.
- `pkg/executor/union_scan_test.go` - executor: Tests union scan for mem buffer reader.
- `pkg/executor/update_test.go` - executor: Tests pessimistic update PK lazy check.
- `pkg/executor/utils_test.go` - executor: Tests batch retriever helper.
- `pkg/executor/window_test.go` - executor: Tests window functions.
- `pkg/executor/write_concurrent_test.go` - executor: Tests batch insert with on duplicate.

### Testdata
- `pkg/executor/testdata/prepare_suite_in.json`
- `pkg/executor/testdata/prepare_suite_out.json`
- `pkg/executor/testdata/slow_query_suite_in.json`
- `pkg/executor/testdata/slow_query_suite_out.json`
- `pkg/executor/testdata/tiflash_v620_dt_segments.json`
- `pkg/executor/testdata/tiflash_v620_dt_tables.json`
- `pkg/executor/testdata/tiflash_v630_dt_segments.json`
- `pkg/executor/testdata/tiflash_v640_dt_tables.json`

## pkg/executor/aggfuncs

### Tests
- `pkg/executor/aggfuncs/aggfunc_test.go` - executor/aggfuncs: Tests agg approx count distinct push down.
- `pkg/executor/aggfuncs/export_test.go` - executor/aggfuncs: Tests export.
- `pkg/executor/aggfuncs/func_avg_test.go` - executor/aggfuncs: Tests merge partial result for avg.
- `pkg/executor/aggfuncs/func_bitfuncs_test.go` - executor/aggfuncs: Tests merge partial result for bit funcs.
- `pkg/executor/aggfuncs/func_count_test.go` - executor/aggfuncs: Tests merge partial result for count.
- `pkg/executor/aggfuncs/func_cume_dist_test.go` - executor/aggfuncs: Tests memory cume dist.
- `pkg/executor/aggfuncs/func_first_row_test.go` - executor/aggfuncs: Tests merge partial result for first row.
- `pkg/executor/aggfuncs/func_group_concat_test.go` - executor/aggfuncs: Tests merge partial result for group concat.
- `pkg/executor/aggfuncs/func_json_arrayagg_test.go` - executor/aggfuncs: Tests merge partial result4 JSON arrayagg.
- `pkg/executor/aggfuncs/func_json_objectagg_test.go` - executor/aggfuncs: Tests merge partial result4 JSON objectagg.
- `pkg/executor/aggfuncs/func_lead_lag_test.go` - executor/aggfuncs: Tests lead/lag.
- `pkg/executor/aggfuncs/func_max_min_test.go` - executor/aggfuncs: Tests merge partial result for max/min.
- `pkg/executor/aggfuncs/func_ntile_test.go` - executor/aggfuncs: Tests memory ntile.
- `pkg/executor/aggfuncs/func_percent_rank_test.go` - executor/aggfuncs: Tests memory percent rank.
- `pkg/executor/aggfuncs/func_percentile_test.go` - executor/aggfuncs: Tests percentile.
- `pkg/executor/aggfuncs/func_rank_test.go` - executor/aggfuncs: Tests memory rank.
- `pkg/executor/aggfuncs/func_stddevpop_test.go` - executor/aggfuncs: Tests merge partial result for stddevpop.
- `pkg/executor/aggfuncs/func_stddevsamp_test.go` - executor/aggfuncs: Tests merge partial result for stddevsamp.
- `pkg/executor/aggfuncs/func_sum_test.go` - executor/aggfuncs: Tests merge partial result for sum.
- `pkg/executor/aggfuncs/func_value_test.go` - executor/aggfuncs: Tests memory value.
- `pkg/executor/aggfuncs/func_varpop_test.go` - executor/aggfuncs: Tests merge partial result for varpop.
- `pkg/executor/aggfuncs/func_varsamp_test.go` - executor/aggfuncs: Tests merge partial result for varsamp.
- `pkg/executor/aggfuncs/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/executor/aggfuncs/row_number_test.go` - executor/aggfuncs: Tests memory row number.
- `pkg/executor/aggfuncs/spill_helper_test.go` - executor/aggfuncs: Tests partial result for count.
- `pkg/executor/aggfuncs/window_func_test.go` - executor/aggfuncs: Tests window functions.

## pkg/executor/aggregate

### Tests
- `pkg/executor/aggregate/agg_spill_test.go` - executor/aggregate: Tests get correct result.

## pkg/executor/importer

### Tests
- `pkg/executor/importer/chunk_process_testkit_test.go` - executor/importer: Tests file chunk process.
- `pkg/executor/importer/import_test.go` - executor/importer: Tests init default options.
- `pkg/executor/importer/importer_testkit_test.go` - executor/importer: Tests verify checksum.
- `pkg/executor/importer/job_test.go` - executor/importer: Tests job happy path.
- `pkg/executor/importer/kv_encode_test.go` - executor/importer: Tests KV encoder for dup resolve.
- `pkg/executor/importer/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/executor/importer/precheck_test.go` - executor/importer: Tests check requirements.
- `pkg/executor/importer/sampler_test.go` - executor/importer: Tests sample index size ratio.
- `pkg/executor/importer/table_import_test.go` - executor/importer: Tests prepare sort dir.
- `pkg/executor/importer/table_import_testkit_test.go` - executor/importer: Tests import from select cleanup.

## pkg/executor/internal/applycache

### Tests
- `pkg/executor/internal/applycache/apply_cache_test.go` - executor/internal/applycache: Tests apply cache.
- `pkg/executor/internal/applycache/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/executor/internal/calibrateresource

### Tests
- `pkg/executor/internal/calibrateresource/calibrate_resource_test.go` - executor/internal/calibrateresource: Tests calibrate resource.
- `pkg/executor/internal/calibrateresource/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/executor/internal/exec

### Tests
- `pkg/executor/internal/exec/indexusage_test.go` - executor/internal/exec: Tests index usage reporter.

## pkg/executor/internal/mpp

### Tests
- `pkg/executor/internal/mpp/local_mpp_coordinator_test.go` - executor/internal/mpp: Tests need report execution summary.

## pkg/executor/internal/pdhelper

### Tests
- `pkg/executor/internal/pdhelper/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/executor/internal/pdhelper/pd_test.go` - executor/internal/pdhelper: Tests TTL cache.

## pkg/executor/internal/querywatch

### Tests
- `pkg/executor/internal/querywatch/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/executor/internal/querywatch/query_watch_test.go` - executor/internal/querywatch: Tests query watch.

## pkg/executor/internal/vecgroupchecker

### Tests
- `pkg/executor/internal/vecgroupchecker/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/executor/internal/vecgroupchecker/vec_group_checker_test.go` - executor/internal/vecgroupchecker: Tests vec group checker data race.

## pkg/executor/join

### Tests
- `pkg/executor/join/anti_semi_join_probe_test.go` - executor/join: Tests anti-semi join basic.
- `pkg/executor/join/bench_test.go` - executor/join: Tests hash table build.
- `pkg/executor/join/concurrent_map_test.go` - executor/join: Tests concurrent map.
- `pkg/executor/join/hash_table_v1_test.go` - executor/join: Tests hash row container.
- `pkg/executor/join/hash_table_v2_test.go` - executor/join: Tests hash table size.
- `pkg/executor/join/inner_join_probe_test.go` - executor/join: Tests inner join probe basic.
- `pkg/executor/join/inner_join_spill_test.go` - executor/join: Tests inner join spill basic.
- `pkg/executor/join/join_row_table_test.go` - executor/join: Tests heap object can move.
- `pkg/executor/join/join_stats_test.go` - executor/join: Tests hash join runtime stats.
- `pkg/executor/join/join_table_meta_test.go` - executor/join: Tests join table meta key mode.
- `pkg/executor/join/joiner_test.go` - executor/join: Tests required rows.
- `pkg/executor/join/left_outer_anti_semi_join_probe_test.go` - executor/join: Tests left-outer anti-semi join probe basic.
- `pkg/executor/join/left_outer_join_probe_test.go` - executor/join: Tests left-outer join probe basic.
- `pkg/executor/join/left_outer_semi_join_probe_test.go` - executor/join: Tests left-outer semi-join probe basic.
- `pkg/executor/join/outer_join_spill_test.go` - executor/join: Tests outer join spill basic.
- `pkg/executor/join/right_outer_join_probe_test.go` - executor/join: Tests right-outer join probe basic.
- `pkg/executor/join/row_table_builder_test.go` - executor/join: Tests row table builder key.
- `pkg/executor/join/semi_join_probe_test.go` - executor/join: Tests semi-join basic.
- `pkg/executor/join/tagged_ptr_test.go` - executor/join: Tests tagged bits.

## pkg/executor/join/test/indexjoin

### Tests
- `pkg/executor/join/test/indexjoin/index_lookup_join_test.go` - executor/join/test/indexjoin: Tests index lookup join hang.
- `pkg/executor/join/test/indexjoin/index_lookup_merge_join_test.go` - executor/join/test/indexjoin: Tests issue18068.

## pkg/executor/join/test/mergejoin

### Tests
- `pkg/executor/join/test/mergejoin/merge_join_test.go` - executor/join/test/mergejoin: Tests shuffle merge join in disk.

## pkg/executor/lockstats

### Tests
- `pkg/executor/lockstats/lock_stats_executor_test.go` - executor/lockstats: Tests populate partition ID and names.

## pkg/executor/mppcoordmanager

### Tests
- `pkg/executor/mppcoordmanager/mpp_coordinator_manager_test.go` - executor/mppcoordmanager: Tests detect and delete.

## pkg/executor/sortexec

### Tests
- `pkg/executor/sortexec/benchmark_test.go` - executor/sortexec: Tests sort exec.
- `pkg/executor/sortexec/parallel_sort_spill_test.go` - executor/sortexec: Tests parallel sort spill to disk.
- `pkg/executor/sortexec/parallel_sort_test.go` - executor/sortexec: Tests parallel sort.
- `pkg/executor/sortexec/sort_spill_test.go` - executor/sortexec: Tests non-parallel sort spill to disk.
- `pkg/executor/sortexec/sort_test.go` - executor/sortexec: Tests sort on disk.
- `pkg/executor/sortexec/sortexec_pkg_test.go` - executor/sortexec: Tests interrupted during sort.
- `pkg/executor/sortexec/topn_spill_test.go` - executor/sortexec: Tests generate top-N results when spill only once.

## pkg/executor/staticrecordset

### Tests
- `pkg/executor/staticrecordset/integration_test.go` - executor/staticrecordset: Tests static record set.

## pkg/executor/test/admintest

### Tests
- `pkg/executor/test/admintest/admin_test.go` - executor/test/admintest: Tests admin recover index.
- `pkg/executor/test/admintest/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/executor/test/aggregate

### Tests
- `pkg/executor/test/aggregate/aggregate_test.go` - executor/test/aggregate: Tests hash agg runtime stat.
- `pkg/executor/test/aggregate/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/executor/test/analyzetest

### Tests
- `pkg/executor/test/analyzetest/analyze_bench_test.go` - executor/test/analyzetest: Tests analyze partition.
- `pkg/executor/test/analyzetest/analyze_test.go` - executor/test/analyzetest: Tests analyze partition.
- `pkg/executor/test/analyzetest/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/executor/test/analyzetest/memorycontrol

### Tests
- `pkg/executor/test/analyzetest/memorycontrol/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/executor/test/analyzetest/memorycontrol/memory_control_test.go` - executor/test/analyzetest/memorycontrol: Tests global memory control for analyze.

## pkg/executor/test/analyzetest/panictest

### Tests
- `pkg/executor/test/analyzetest/panictest/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/executor/test/analyzetest/panictest/panic_test.go` - executor/test/analyzetest/panictest: Tests panic in handle result error with single goroutine.

## pkg/executor/test/autoidtest

### Tests
- `pkg/executor/test/autoidtest/autoid_test.go` - executor/test/autoidtest: Tests filter different allocators.
- `pkg/executor/test/autoidtest/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/executor/test/cte

### Tests
- `pkg/executor/test/cte/cte_test.go` - executor/test/cte: Tests CTE issue49096.
- `pkg/executor/test/cte/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/executor/test/ddl

### Tests
- `pkg/executor/test/ddl/ddl_test.go` - executor/test/ddl: Tests in txn exec DDL fail.
- `pkg/executor/test/ddl/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/executor/test/distsqltest

### Tests
- `pkg/executor/test/distsqltest/distsql_test.go` - executor/test/distsqltest: Tests DistSQL partition table concurrency.
- `pkg/executor/test/distsqltest/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/executor/test/executor

### Tests
- `pkg/executor/test/executor/executor_test.go` - executor/test/executor: Tests timezone push down.
- `pkg/executor/test/executor/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/executor/test/fktest

### Tests
- `pkg/executor/test/fktest/foreign_key_test.go` - executor/test/fktest: Tests foreign key on insert child table.
- `pkg/executor/test/fktest/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/executor/test/indexmergereadtest

### Tests
- `pkg/executor/test/indexmergereadtest/index_merge_reader_test.go` - executor/test/indexmergereadtest: Tests index merge pick and exec task panic.
- `pkg/executor/test/indexmergereadtest/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/executor/test/infoschema

### Tests
- `pkg/executor/test/infoschema/infoschema_test.go` - executor/test/infoschema: Tests inspection tables.
- `pkg/executor/test/infoschema/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/executor/test/issuetest

### Tests
- `pkg/executor/test/issuetest/executor_issue_test.go` - executor/test/issuetest: Tests issue24210.
- `pkg/executor/test/issuetest/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/executor/test/jointest

### Tests
- `pkg/executor/test/jointest/join_test.go` - executor/test/jointest: Tests join v2.
- `pkg/executor/test/jointest/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/executor/test/jointest/hashjoin

### Tests
- `pkg/executor/test/jointest/hashjoin/hash_join_test.go` - executor/test/jointest/hashjoin: Tests index nested loop hash join.
- `pkg/executor/test/jointest/hashjoin/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/executor/test/loaddatatest

### Tests
- `pkg/executor/test/loaddatatest/load_data_test.go` - executor/test/loaddatatest: Tests load data init param.
- `pkg/executor/test/loaddatatest/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/executor/test/loadremotetest

### Tests
- `pkg/executor/test/loadremotetest/error_test.go` - executor/test/loadremotetest: Tests error.
- `pkg/executor/test/loadremotetest/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/executor/test/loadremotetest/multi_file_test.go` - executor/test/loadremotetest: Tests multi-file.
- `pkg/executor/test/loadremotetest/one_csv_test.go` - executor/test/loadremotetest: Tests single CSV.
- `pkg/executor/test/loadremotetest/util_test.go` - executor/test/loadremotetest: Tests load remote.

## pkg/executor/test/memtest

### Tests
- `pkg/executor/test/memtest/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/executor/test/memtest/mem_test.go` - executor/test/memtest: Tests insert update tracker on cleanup.

## pkg/executor/test/oomtest

### Tests
- `pkg/executor/test/oomtest/oom_test.go` - executor/test/oomtest: Tests OOM behavior.

## pkg/executor/test/passwordtest

### Tests
- `pkg/executor/test/passwordtest/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/executor/test/passwordtest/password_management_test.go` - executor/test/passwordtest: Tests validate password.

## pkg/executor/test/plancache

### Tests
- `pkg/executor/test/plancache/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/executor/test/plancache/plan_cache_test.go` - executor/test/plancache: Tests point get prepared plan.

## pkg/executor/test/planreplayer

### Tests
- `pkg/executor/test/planreplayer/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/executor/test/planreplayer/plan_replayer_test.go` - executor/test/planreplayer: Tests plan replayer.

## pkg/executor/test/seqtest

### Tests
- `pkg/executor/test/seqtest/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/executor/test/seqtest/prepared_test.go` - executor/test/seqtest: Tests prepared.
- `pkg/executor/test/seqtest/seq_executor_test.go` - executor/test/seqtest: Tests early close.

## pkg/executor/test/showtest

### Tests
- `pkg/executor/test/showtest/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/executor/test/showtest/show_test.go` - executor/test/showtest: Tests show create table placement.

## pkg/executor/test/simpletest

### Tests
- `pkg/executor/test/simpletest/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/executor/test/simpletest/simple_test.go` - executor/test/simpletest: Tests extended stats privileges.

## pkg/executor/test/splittest

### Tests
- `pkg/executor/test/splittest/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/executor/test/splittest/split_table_test.go` - executor/test/splittest: Tests cluster index show table region.

## pkg/executor/test/tiflashtest

### Tests
- `pkg/executor/test/tiflashtest/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/executor/test/tiflashtest/tiflash_test.go` - executor/test/tiflashtest: Tests unsupported charset table.

## pkg/executor/test/txn

### Tests
- `pkg/executor/test/txn/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/executor/test/txn/txn_test.go` - executor/test/txn: Tests invalid read temporary table.

## pkg/executor/test/unstabletest

### Tests
- `pkg/executor/test/unstabletest/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/executor/test/unstabletest/memory_test.go` - executor/test/unstabletest: Tests global memory control.

## pkg/executor/test/writetest

### Tests
- `pkg/executor/test/writetest/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/executor/test/writetest/write_test.go` - executor/test/writetest: Tests insert ignore.
