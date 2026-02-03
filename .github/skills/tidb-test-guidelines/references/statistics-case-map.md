# TiDB statistics Test Case Map (pkg/statistics)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## pkg/statistics

### Tests
- `pkg/statistics/bench_daily_test.go` - statistics: Tests bench daily.
- `pkg/statistics/builder_test.go` - statistics: Tests build hist and top n.
- `pkg/statistics/cmsketch_test.go` - statistics: Tests CMSketch.
- `pkg/statistics/fmsketch_test.go` - statistics: Tests fmsketch.
- `pkg/statistics/histogram_bench_test.go` - statistics: Tests merge partition hist to global hist.
- `pkg/statistics/histogram_test.go` - statistics: Tests truncate histogram.
- `pkg/statistics/integration_test.go` - statistics: Tests change ver to 2 behavior.
- `pkg/statistics/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/statistics/sample_test.go` - statistics: Tests weighted sampling.
- `pkg/statistics/scalar_test.go` - statistics: Tests calc fraction.
- `pkg/statistics/statistics_test.go` - statistics: Tests merge histogram.
- `pkg/statistics/table_test.go` - statistics: Tests clone col and idx existence map.

### Testdata
- `pkg/statistics/testdata/integration_suite_in.json`
- `pkg/statistics/testdata/integration_suite_out.json`

## pkg/statistics/asyncload

### Tests
- `pkg/statistics/asyncload/async_load_test.go` - statistics/asyncload: Tests load column statistics after table drop.

## pkg/statistics/handle

### Tests
- `pkg/statistics/handle/bootstrap_test.go` - statistics/handle: Tests gen init stats histograms SQL all records.
- `pkg/statistics/handle/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/statistics/handle/autoanalyze

### Tests
- `pkg/statistics/handle/autoanalyze/autoanalyze_test.go` - statistics/handle/autoanalyze: Tests enable auto analyze priority queue.

## pkg/statistics/handle/autoanalyze/exec

### Tests
- `pkg/statistics/handle/autoanalyze/exec/exec_test.go` - statistics/handle/autoanalyze: Tests exec auto analyzes.

## pkg/statistics/handle/autoanalyze/priorityqueue

### Tests
- `pkg/statistics/handle/autoanalyze/priorityqueue/analysis_job_factory_test.go` - statistics/handle/autoanalyze: Tests calculate change percentage.
- `pkg/statistics/handle/autoanalyze/priorityqueue/calculator_test.go` - statistics/handle/autoanalyze: Tests calculate weight.
- `pkg/statistics/handle/autoanalyze/priorityqueue/dynamic_partitioned_table_analysis_job_test.go` - statistics/handle/autoanalyze: Tests analyze dynamic partitioned table.
- `pkg/statistics/handle/autoanalyze/priorityqueue/heap_test.go` - statistics/handle/autoanalyze: Tests heap add or update.
- `pkg/statistics/handle/autoanalyze/priorityqueue/interval_test.go` - statistics/handle/autoanalyze: Tests get average analysis duration.
- `pkg/statistics/handle/autoanalyze/priorityqueue/job_test.go` - statistics/handle/autoanalyze: Tests stringer.
- `pkg/statistics/handle/autoanalyze/priorityqueue/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/statistics/handle/autoanalyze/priorityqueue/non_partitioned_table_analysis_job_test.go` - statistics/handle/autoanalyze: Tests gen SQL for non-partitioned table.
- `pkg/statistics/handle/autoanalyze/priorityqueue/queue_ddl_handler_test.go` - statistics/handle/autoanalyze: Tests handle DDL events with running jobs.
- `pkg/statistics/handle/autoanalyze/priorityqueue/queue_test.go` - statistics/handle/autoanalyze: Tests call API before initialize.
- `pkg/statistics/handle/autoanalyze/priorityqueue/static_partitioned_table_analysis_job_test.go` - statistics/handle/autoanalyze: Tests gen SQL for analyze static partitioned table.

## pkg/statistics/handle/autoanalyze/priorityqueue/calculatoranalysis

### Tests
- `pkg/statistics/handle/autoanalyze/priorityqueue/calculatoranalysis/calculator_analysis_test.go` - statistics/handle/autoanalyze: Tests priority calculator with generated data.
- `pkg/statistics/handle/autoanalyze/priorityqueue/calculatoranalysis/main_test.go` - Configures default goleak settings and registers testdata.

### Testdata
- `pkg/statistics/handle/autoanalyze/priorityqueue/calculatoranalysis/testdata/calculated_priorities.golden.csv`

## pkg/statistics/handle/autoanalyze/refresher

### Tests
- `pkg/statistics/handle/autoanalyze/refresher/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/statistics/handle/autoanalyze/refresher/refresher_test.go` - statistics/handle/autoanalyze: Tests turn off and on auto analyze.
- `pkg/statistics/handle/autoanalyze/refresher/worker_test.go` - statistics/handle/autoanalyze: Tests worker.

## pkg/statistics/handle/cache

### Tests
- `pkg/statistics/handle/cache/bench_test.go` - statistics/handle/cache: Tests stats cache LFU copy and update.
- `pkg/statistics/handle/cache/statscache_test.go` - statistics/handle/cache: Tests cache of batch update.

## pkg/statistics/handle/cache/internal/lfu

### Tests
- `pkg/statistics/handle/cache/internal/lfu/lfu_cache_test.go` - statistics/handle/cache: Tests LFU put/get/del.

## pkg/statistics/handle/ddl

### Tests
- `pkg/statistics/handle/ddl/ddl_test.go` - statistics/handle/ddl: Tests DDL after load.

## pkg/statistics/handle/globalstats

### Tests
- `pkg/statistics/handle/globalstats/global_stats_internal_test.go` - statistics/handle/globalstats: Tests global stats internal.
- `pkg/statistics/handle/globalstats/global_stats_test.go` - statistics/handle/globalstats: Tests show global stats with async merge global.
- `pkg/statistics/handle/globalstats/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/statistics/handle/globalstats/topn_bench_test.go` - statistics/handle/globalstats: Tests merge part top N to global top N with hists.
- `pkg/statistics/handle/globalstats/topn_test.go` - statistics/handle/globalstats: Tests merge part top N to global top N without hists.

## pkg/statistics/handle/handletest

### Tests
- `pkg/statistics/handle/handletest/handle_test.go` - statistics/handle/handletest: Tests empty table.
- `pkg/statistics/handle/handletest/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/statistics/handle/handletest/analyze

### Tests
- `pkg/statistics/handle/handletest/analyze/analyze_test.go` - statistics/handle/handletest: Tests analyze virtual col.
- `pkg/statistics/handle/handletest/analyze/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/statistics/handle/handletest/initstats

### Tests
- `pkg/statistics/handle/handletest/initstats/init_stats_test.go` - statistics/handle/handletest: Tests lite init stats with table IDs.
- `pkg/statistics/handle/handletest/initstats/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/statistics/handle/handletest/lockstats

### Tests
- `pkg/statistics/handle/handletest/lockstats/lock_partition_stats_test.go` - statistics/handle/handletest: Tests lock and unlock partition stats.
- `pkg/statistics/handle/handletest/lockstats/lock_table_stats_test.go` - statistics/handle/handletest: Tests lock and unlock table stats.
- `pkg/statistics/handle/handletest/lockstats/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/statistics/handle/handletest/statstest

### Tests
- `pkg/statistics/handle/handletest/statstest/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/statistics/handle/handletest/statstest/stats_test.go` - statistics/handle/handletest: Tests stats cache process.

## pkg/statistics/handle/lockstats

### Tests
- `pkg/statistics/handle/lockstats/lock_stats_test.go` - statistics/handle/lockstats: Tests generate skipped tables message.
- `pkg/statistics/handle/lockstats/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/statistics/handle/lockstats/query_lock_test.go` - statistics/handle/lockstats: Tests get tables locked statuses.
- `pkg/statistics/handle/lockstats/unlock_stats_test.go` - statistics/handle/lockstats: Tests get stats delta from table locked.

## pkg/statistics/handle/storage

### Tests
- `pkg/statistics/handle/storage/dump_test.go` - statistics/handle/storage: Tests conversion.
- `pkg/statistics/handle/storage/gc_test.go` - statistics/handle/storage: Tests GC stats.
- `pkg/statistics/handle/storage/read_test.go` - statistics/handle/storage: Tests load stats.
- `pkg/statistics/handle/storage/stats_read_writer_test.go` - statistics/handle/storage: Tests update stats meta version for GC.

## pkg/statistics/handle/syncload

### Tests
- `pkg/statistics/handle/syncload/stats_syncload_test.go` - statistics/handle/syncload: Tests concurrent load hist.

## pkg/statistics/handle/updatetest

### Tests
- `pkg/statistics/handle/updatetest/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/statistics/handle/updatetest/update_test.go` - statistics/handle/updatetest: Tests single session insert.

## pkg/statistics/handle/usage

### Tests
- `pkg/statistics/handle/usage/index_usage_integration_test.go` - statistics/handle/usage: Tests GC index usage.
- `pkg/statistics/handle/usage/predicate_column_test.go` - statistics/handle/usage: Tests cleanup predicate columns.
- `pkg/statistics/handle/usage/session_stats_collect_test.go` - statistics/handle/usage: Tests predicate usage first touch creates row.

## pkg/statistics/handle/usage/collector

### Tests
- `pkg/statistics/handle/usage/collector/collector_test.go` - statistics/handle/usage: Tests session send delta.

## pkg/statistics/handle/usage/indexusage

### Tests
- `pkg/statistics/handle/usage/indexusage/collector_test.go` - statistics/handle/usage: Tests get bucket.

## pkg/statistics/handle/util

### Tests
- `pkg/statistics/handle/util/util_test.go` - statistics/handle/util: Tests is special global index.
