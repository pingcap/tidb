# TiDB util Test Case Map (pkg/util)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## pkg/util

### Tests
- `pkg/util/errors_test.go` - util: Tests origin error.
- `pkg/util/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/util/misc_test.go` - util: Tests run with retry.
- `pkg/util/prefix_helper_test.go` - util: Tests prefix.
- `pkg/util/security_test.go` - util: Tests invalid TLS.
- `pkg/util/session_pool_test.go` - util: Tests session pool.
- `pkg/util/split_test.go` - util: Tests longest common prefix len.
- `pkg/util/urls_test.go` - util: Tests parse host port addr.
- `pkg/util/util_test.go` - util: Tests log format.
- `pkg/util/wait_group_wrapper_test.go` - util: Tests wait group wrapper run.

## pkg/util/admin

### Tests
- `pkg/util/admin/admin_integration_test.go` - util/admin: Tests admin check table corrupted.
- `pkg/util/admin/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/util/arena

### Tests
- `pkg/util/arena/arena_test.go` - util/arena: Tests simple arena allocator.
- `pkg/util/arena/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/util/backoff

### Tests
- `pkg/util/backoff/backoff_test.go` - util/backoff: Tests exponential.

## pkg/util/benchdaily

### Tests
- `pkg/util/benchdaily/bench_daily_test.go` - util/benchdaily: Tests bench daily.
- `pkg/util/benchdaily/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/util/bitmap

### Tests
- `pkg/util/bitmap/concurrent_test.go` - util/bitmap: Tests concurrent bitmap set.
- `pkg/util/bitmap/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/util/cdcutil

### Tests
- `pkg/util/cdcutil/cdc_test.go` - util/cdcutil: Tests CDC check with embed etcd.
- `pkg/util/cdcutil/export_for_test.go` - util/cdcutil: Tests export for.

## pkg/util/cgmon

### Tests
- `pkg/util/cgmon/cgmon_test.go` - util/cgmon: Tests upload default value without cgroup.

## pkg/util/cgroup

### Tests
- `pkg/util/cgroup/cgroup_cpu_test.go` - util/cgroup: Tests get cgroup CPU.
- `pkg/util/cgroup/cgroup_mock_test.go` - util/cgroup: Tests cgroups get memory usage.

## pkg/util/checksum

### Tests
- `pkg/util/checksum/checksum_test.go` - util/checksum: Tests checksum read at.
- `pkg/util/checksum/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/util/chunk

### Tests
- `pkg/util/chunk/alloc_test.go` - util/chunk: Tests allocator.
- `pkg/util/chunk/chunk_in_disk_test.go` - util/chunk: Tests data in disk by chunks.
- `pkg/util/chunk/chunk_test.go` - util/chunk: Tests append row.
- `pkg/util/chunk/chunk_util_test.go` - util/chunk: Tests copy selected join rows.
- `pkg/util/chunk/codec_test.go` - util/chunk: Tests codec.
- `pkg/util/chunk/column_test.go` - util/chunk: Tests column copy.
- `pkg/util/chunk/iterator_test.go` - util/chunk: Tests iterator on sel.
- `pkg/util/chunk/list_test.go` - util/chunk: Tests list.
- `pkg/util/chunk/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/util/chunk/mutrow_test.go` - util/chunk: Tests mut row.
- `pkg/util/chunk/pool_test.go` - util/chunk: Tests new pool.
- `pkg/util/chunk/row_container_test.go` - util/chunk: Tests new row container.
- `pkg/util/chunk/row_in_disk_test.go` - util/chunk: Tests data in disk by rows.

## pkg/util/codec

### Tests
- `pkg/util/codec/bench_test.go` - util/codec: Tests decode with size.
- `pkg/util/codec/bytes_test.go` - util/codec: Tests fast slow fast reverse.
- `pkg/util/codec/codec_test.go` - util/codec: Tests codec key.
- `pkg/util/codec/collation_test.go` - util/codec: Tests hash group key collation.
- `pkg/util/codec/decimal_test.go` - util/codec: Tests decimal codec.
- `pkg/util/codec/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/util/collate

### Tests
- `pkg/util/collate/collate_bench_test.go` - util/collate: Tests utf8mb4 bin compare short.
- `pkg/util/collate/collate_test.go` - util/collate: Tests UTF8 collator compare.
- `pkg/util/collate/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/util/collate/ucadata

### Tests
- `pkg/util/collate/ucadata/unicode_0900_ai_ci_data_test.go` - util/collate/ucadata: Tests hangul jamo has only one weight.
- `pkg/util/collate/ucadata/unicode_ci_data_original_test.go` - util/collate/ucadata: Tests unicode ci data original.
- `pkg/util/collate/ucadata/unicode_ci_data_test.go` - util/collate/ucadata: Tests unicode0400 is the same.

## pkg/util/column-mapping

### Tests
- `pkg/util/column-mapping/column_test.go` - util/column-mapping: Tests rule.

## pkg/util/context

### Tests
- `pkg/util/context/warn_test.go` - util/context: Tests SQL warn.

## pkg/util/cpu

### Tests
- `pkg/util/cpu/cpu_test.go` - util/cpu: Tests CPU value.
- `pkg/util/cpu/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/util/cpuprofile

### Tests
- `pkg/util/cpuprofile/cpuprofile_test.go` - util/cpuprofile: Tests main.

## pkg/util/cteutil

### Tests
- `pkg/util/cteutil/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/util/cteutil/storage_test.go` - util/cteutil: Tests storage basic.

## pkg/util/dbterror

### Tests
- `pkg/util/dbterror/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/util/dbterror/terror_test.go` - util/dbterror: Tests error redact.

## pkg/util/dbterror/plannererrors

### Tests
- `pkg/util/dbterror/plannererrors/errors_test.go` - util/dbterror/plannererrors: Tests error.

## pkg/util/dbutil

### Tests
- `pkg/util/dbutil/common_test.go` - util/dbutil: Tests replace placeholder.
- `pkg/util/dbutil/index_test.go` - util/dbutil: Tests index.
- `pkg/util/dbutil/retry_test.go` - util/dbutil: Tests is retryable error.
- `pkg/util/dbutil/table_test.go` - util/dbutil: Tests table.
- `pkg/util/dbutil/variable_test.go` - util/dbutil: Tests show grants.

## pkg/util/ddl-checker

### Tests
- `pkg/util/ddl-checker/executable_checker_test.go` - util/ddl-checker: Tests parse.

## pkg/util/deadlockhistory

### Tests
- `pkg/util/deadlockhistory/deadlock_history_test.go` - util/deadlockhistory: Tests deadlock history collection.
- `pkg/util/deadlockhistory/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/util/deeptest

### Tests
- `pkg/util/deeptest/statictesthelper_test.go` - util/deeptest: Tests assert recursively not equal.

## pkg/util/disjointset

### Tests
- `pkg/util/disjointset/int_set_test.go` - util/disjointset: Tests int disjoint set.
- `pkg/util/disjointset/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/util/disjointset/set_test.go` - util/disjointset: Tests disjoint set.

## pkg/util/disk

### Tests
- `pkg/util/disk/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/util/disk/tempDir_test.go` - util/disk: Tests remove dir.

## pkg/util/disttask

### Tests
- `pkg/util/disttask/idservice_test.go` - util/disttask: Tests gen server ID.

## pkg/util/encrypt

### Tests
- `pkg/util/encrypt/aes_layer_test.go` - util/encrypt: Tests read at.
- `pkg/util/encrypt/aes_test.go` - util/encrypt: Tests pad.
- `pkg/util/encrypt/crypt_test.go` - util/encrypt: Tests SQL decode.
- `pkg/util/encrypt/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/util/engine

### Tests
- `pkg/util/engine/engine_test.go` - util/engine: Tests is ti flash h t t p resp.

## pkg/util/etcd

### Tests
- `pkg/util/etcd/etcd_test.go` - util/etcd: Tests set etcd cli by namespace.

## pkg/util/execdetails

### Tests
- `pkg/util/execdetails/execdetails_test.go` - util/execdetails: Tests string.
- `pkg/util/execdetails/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/util/expensivequery

### Tests
- `pkg/util/expensivequery/expensivequery_test.go` - util/expensivequery: Tests main.

## pkg/util/extsort

### Tests
- `pkg/util/extsort/disk_sorter_test.go` - util/extsort: Tests disk sorter common.
- `pkg/util/extsort/external_sorter_test.go` - util/extsort: Tests external sorter.

## pkg/util/fastrand

### Tests
- `pkg/util/fastrand/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/util/fastrand/random_test.go` - util/fastrand: Tests rand.

## pkg/util/filter

### Tests
- `pkg/util/filter/filter_test.go` - util/filter: Tests filter on schema.
- `pkg/util/filter/schema_test.go` - util/filter: Tests is system schema.

## pkg/util/format

### Tests
- `pkg/util/format/format_test.go` - util/format: Tests format.
- `pkg/util/format/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/util/gctuner

### Tests
- `pkg/util/gctuner/finalizer_test.go` - util/gctuner: Tests finalizer.
- `pkg/util/gctuner/mem_test.go` - util/gctuner: Tests mem.
- `pkg/util/gctuner/memory_limit_tuner_test.go` - util/gctuner: Tests global memory tuner.
- `pkg/util/gctuner/tuner_test.go` - util/gctuner: Tests tuner.

## pkg/util/generatedexpr

### Tests
- `pkg/util/generatedexpr/gen_expr_test.go` - util/generatedexpr: Tests parse expression.
- `pkg/util/generatedexpr/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/util/generic

### Tests
- `pkg/util/generic/bounded_min_heap_test.go` - util/generic: Tests bounded min heap basic.
- `pkg/util/generic/sync_map_test.go` - util/generic: Tests sync map.

## pkg/util/globalconn

### Tests
- `pkg/util/globalconn/globalconn_test.go` - util/globalconn: Tests to conn ID.
- `pkg/util/globalconn/pool_test.go` - util/globalconn: Tests auto inc pool.

## pkg/util/hack

### Tests
- `pkg/util/hack/hack_test.go` - util/hack: Tests string.
- `pkg/util/hack/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/util/hack/map_abi_test.go` - util/hack: Tests swiss table.

## pkg/util/intest

### Tests
- `pkg/util/intest/assert_test.go` - util/intest: Tests assert.

## pkg/util/intset

### Tests
- `pkg/util/intset/fast_int_set_bench_test.go` - util/intset: Tests map int set difference.
- `pkg/util/intset/fast_int_set_test.go` - util/intset: Tests fast int set basic.

## pkg/util/keydecoder

### Tests
- `pkg/util/keydecoder/keydecoder_test.go` - util/keydecoder: Tests decode key.
- `pkg/util/keydecoder/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/util/kvcache

### Tests
- `pkg/util/kvcache/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/util/kvcache/simple_lru_test.go` - util/kvcache: Tests put.

## pkg/util/logutil

### Tests
- `pkg/util/logutil/hex_test.go` - util/logutil: Tests hex.
- `pkg/util/logutil/log_test.go` - util/logutil: Tests fields from trace info.
- `pkg/util/logutil/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/util/mathutil

### Tests
- `pkg/util/mathutil/exponential_average_test.go` - util/mathutil: Tests exponential.
- `pkg/util/mathutil/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/util/mathutil/math_test.go` - util/mathutil: Tests str len of uint64 fast.
- `pkg/util/mathutil/rand_test.go` - util/mathutil: Tests rand with time.

## pkg/util/memory

### Tests
- `pkg/util/memory/arbitrator_test.go` - util/memory: Tests wrap list.
- `pkg/util/memory/bench_test.go` - util/memory: Tests mem total.
- `pkg/util/memory/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/util/memory/pool_test.go` - util/memory: Tests pool allocations.
- `pkg/util/memory/tracker_test.go` - util/memory: Tests set label.

## pkg/util/memoryusagealarm

### Tests
- `pkg/util/memoryusagealarm/memoryusagealarm_test.go` - util/memoryusagealarm: Tests if need do record.

## pkg/util/mock

### Tests
- `pkg/util/mock/iter_test.go` - util/mock: Tests slice iter.
- `pkg/util/mock/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/util/mock/mock_test.go` - util/mock: Tests context.

## pkg/util/mvmap

### Tests
- `pkg/util/mvmap/bench_test.go` - util/mvmap: Tests MV map put.
- `pkg/util/mvmap/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/util/mvmap/mvmap_test.go` - util/mvmap: Tests MV map.

## pkg/util/naming

### Tests
- `pkg/util/naming/naming_test.go` - util/naming: Tests scope.

## pkg/util/paging

### Tests
- `pkg/util/paging/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/util/paging/paging_test.go` - util/paging: Tests grow paging size.

## pkg/util/parser

### Tests
- `pkg/util/parser/ast_test.go` - util/parser: Tests simple cases.
- `pkg/util/parser/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/util/parser/parser_test.go` - util/parser: Tests space.

## pkg/util/partialjson

### Tests
- `pkg/util/partialjson/extract_test.go` - util/partialjson: Tests iter.

## pkg/util/password-validation

### Tests
- `pkg/util/password-validation/password_validation_test.go` - util/password-validation: Tests validate dictionary password.

## pkg/util/plancodec

### Tests
- `pkg/util/plancodec/codec_test.go` - util/plancodec: Tests encode task type.
- `pkg/util/plancodec/id_test.go` - util/plancodec: Tests plan ID changed.
- `pkg/util/plancodec/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/util/prefetch

### Tests
- `pkg/util/prefetch/reader_test.go` - util/prefetch: Tests basic.

## pkg/util/printer

### Tests
- `pkg/util/printer/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/util/printer/printer_test.go` - util/printer: Tests print result.

## pkg/util/profile

### Tests
- `pkg/util/profile/flamegraph_test.go` - util/profile: Tests profile to datum.
- `pkg/util/profile/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/util/profile/profile_test.go` - util/profile: Tests profiles.

### Testdata
- `pkg/util/profile/testdata/test.pprof`

## pkg/util/promutil

### Tests
- `pkg/util/promutil/registry_test.go` - util/promutil: Tests noop registry.

## pkg/util/queue

### Tests
- `pkg/util/queue/queue_test.go` - util/queue: Tests queue.

## pkg/util/ranger

### Tests
- `pkg/util/ranger/bench_test.go` - util/ranger: Tests detach cond and build range for index.
- `pkg/util/ranger/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/util/ranger/ranger_test.go` - util/ranger: Tests table range.
- `pkg/util/ranger/types_test.go` - util/ranger: Tests range.

## pkg/util/ranger/context

### Tests
- `pkg/util/ranger/context/context_test.go` - util/ranger/context: Tests context detach.

## pkg/util/redact

### Tests
- `pkg/util/redact/redact_test.go` - util/redact: Tests redact.

## pkg/util/regexpr-router

### Tests
- `pkg/util/regexpr-router/regexpr_router_test.go` - util/regexpr-router: Tests create router.

## pkg/util/replayer

### Tests
- `pkg/util/replayer/replayer_test.go` - util/replayer: Tests plan replayer path without write prem.

## pkg/util/resourcegrouptag

### Tests
- `pkg/util/resourcegrouptag/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/util/resourcegrouptag/resource_group_tag_test.go` - util/resourcegrouptag: Tests resource group tag encoding PB.

## pkg/util/rowDecoder

### Tests
- `pkg/util/rowDecoder/decoder_test.go` - util/rowDecoder: Tests row decoder.
- `pkg/util/rowDecoder/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/util/rowcodec

### Tests
- `pkg/util/rowcodec/bench_test.go` - util/rowcodec: Tests checksum.
- `pkg/util/rowcodec/common_test.go` - util/rowcodec: Tests remove keyspace prefix.
- `pkg/util/rowcodec/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/util/rowcodec/rowcodec_test.go` - util/rowcodec: Tests encode large small reuse bug.

## pkg/util/schemacmp

### Tests
- `pkg/util/schemacmp/lattice_test.go` - util/schemacmp: Tests compatibilities.
- `pkg/util/schemacmp/table_test.go` - util/schemacmp: Tests join schemas.
- `pkg/util/schemacmp/type_test.go` - util/schemacmp: Tests type unwrap.

## pkg/util/selection

### Tests
- `pkg/util/selection/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/util/selection/selection_test.go` - util/selection: Tests selection.

## pkg/util/sem

### Tests
- `pkg/util/sem/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/util/sem/sem_test.go` - util/sem: Tests invisible schema.

## pkg/util/sem/compat

### Tests
- `pkg/util/sem/compat/compat_test.go` - util/sem/compat: Tests invisible schema.
- `pkg/util/sem/compat/sem_integration_test.go` - util/sem/compat: Tests restricted SQL.

## pkg/util/sem/v2

### Tests
- `pkg/util/sem/v2/config_test.go` - util/sem/v2: Tests parse config with different format.
- `pkg/util/sem/v2/sem_test.go` - util/sem/v2: Tests s e m methods.
- `pkg/util/sem/v2/sql_rule_test.go` - util/sem/v2: Tests SQL rules.

## pkg/util/servermemorylimit

### Tests
- `pkg/util/servermemorylimit/servermemorylimit_test.go` - util/servermemorylimit: Tests memory usage ops history.

## pkg/util/set

### Tests
- `pkg/util/set/float64_set_test.go` - util/set: Tests float64 set.
- `pkg/util/set/int_set_test.go` - util/set: Tests int set.
- `pkg/util/set/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/util/set/set_test.go` - util/set: Tests set basic.
- `pkg/util/set/set_with_memory_usage_test.go` - util/set: Tests float64 set memory usage.
- `pkg/util/set/string_set_test.go` - util/set: Tests string set.

## pkg/util/slice

### Tests
- `pkg/util/slice/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/util/slice/slice_test.go` - util/slice: Tests slice.

## pkg/util/sqlescape

### Tests
- `pkg/util/sqlescape/utils_test.go` - util/sqlescape: Tests reserve buffer.

## pkg/util/sqlexec

### Tests
- `pkg/util/sqlexec/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/util/stmtsummary

### Tests
- `pkg/util/stmtsummary/evicted_test.go` - util/stmtsummary: Tests map to evicted count datum.
- `pkg/util/stmtsummary/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/util/stmtsummary/statement_summary_test.go` - util/stmtsummary: Tests set up.

## pkg/util/stmtsummary/v2

### Tests
- `pkg/util/stmtsummary/v2/column_test.go` - util/stmtsummary/v2: Tests column.
- `pkg/util/stmtsummary/v2/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/util/stmtsummary/v2/reader_test.go` - util/stmtsummary/v2: Tests time range overlap.
- `pkg/util/stmtsummary/v2/record_test.go` - util/stmtsummary/v2: Tests stmt record.
- `pkg/util/stmtsummary/v2/stmtsummary_benchmark_test.go` - util/stmtsummary/v2: Tests stmt summary add single workload.
- `pkg/util/stmtsummary/v2/stmtsummary_test.go` - util/stmtsummary/v2: Tests stmt window.

## pkg/util/stmtsummary/v2/tests

### Tests
- `pkg/util/stmtsummary/v2/tests/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/util/stmtsummary/v2/tests/table_test.go` - util/stmtsummary/v2/tests: Tests stmt summary index advisor.

## pkg/util/stringutil

### Tests
- `pkg/util/stringutil/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/util/stringutil/string_util_test.go` - util/stringutil: Tests unquote.

## pkg/util/sys/linux

### Tests
- `pkg/util/sys/linux/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/util/sys/linux/sys_test.go` - util/sys/linux: Tests get o s version.

## pkg/util/sys/storage

### Tests
- `pkg/util/sys/storage/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/util/sys/storage/sys_test.go` - util/sys/storage: Tests get target directory capacity.

## pkg/util/systimemon

### Tests
- `pkg/util/systimemon/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/util/systimemon/systime_mon_test.go` - util/systimemon: Tests systime monitor.

## pkg/util/table-filter

### Tests
- `pkg/util/table-filter/column_filter_test.go` - util/table-filter: Tests match columns.
- `pkg/util/table-filter/compat_test.go` - util/table-filter: Tests schema filter.
- `pkg/util/table-filter/table_filter_test.go` - util/table-filter: Tests match tables.

## pkg/util/table-router

### Tests
- `pkg/util/table-router/router_test.go` - util/table-router: Tests route.

## pkg/util/table-rule-selector

### Tests
- `pkg/util/table-rule-selector/selector_test.go` - util/table-rule-selector: Tests selector.

## pkg/util/texttree

### Tests
- `pkg/util/texttree/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/util/texttree/texttree_test.go` - util/texttree: Tests pretty identifier.

## pkg/util/timeutil

### Tests
- `pkg/util/timeutil/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/util/timeutil/time_test.go` - util/timeutil: Tests sleep.
- `pkg/util/timeutil/time_zone_test.go` - util/timeutil: Tests get t z name from file name.

## pkg/util/tls

### Tests
- `pkg/util/tls/tls_test.go` - util/tls: Tests version name.

## pkg/util/topsql

### Tests
- `pkg/util/topsql/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/util/topsql/topsql_test.go` - util/topsql: Tests TopSQL CPU profile.

## pkg/util/topsql/collector

### Tests
- `pkg/util/topsql/collector/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/util/topsql/reporter

### Tests
- `pkg/util/topsql/reporter/datamodel_test.go` - util/topsql/reporter: Tests ts item to proto.
- `pkg/util/topsql/reporter/datasink_test.go` - util/topsql/reporter: Tests default data sink registerer.
- `pkg/util/topsql/reporter/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/util/topsql/reporter/pubsub_test.go` - util/topsql/reporter: Tests pub sub data sink.
- `pkg/util/topsql/reporter/reporter_test.go` - util/topsql/reporter: Tests collect and send batch.
- `pkg/util/topsql/reporter/single_target_test.go` - util/topsql/reporter: Tests single target data sink.

## pkg/util/topsql/stmtstats

### Tests
- `pkg/util/topsql/stmtstats/aggregator_test.go` - util/topsql/stmtstats: Tests setup close aggregator.
- `pkg/util/topsql/stmtstats/kv_exec_count_test.go` - util/topsql/stmtstats: Tests kv exec counter.
- `pkg/util/topsql/stmtstats/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/util/topsql/stmtstats/stmtstats_test.go` - util/topsql/stmtstats: Tests kv statement stats item merge.

## pkg/util/traceevent

### Tests
- `pkg/util/traceevent/adapter_test.go` - util/traceevent: Tests trace control extractor.
- `pkg/util/traceevent/flightrecorder_test.go` - util/traceevent: Tests flight recorder config.
- `pkg/util/traceevent/traceevent_test.go` - util/traceevent: Tests suite.

## pkg/util/traceevent/test

### Tests
- `pkg/util/traceevent/test/integration_test.go` - util/traceevent/test: Tests prev trace ID persistence.

## pkg/util/tracing

### Tests
- `pkg/util/tracing/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/util/tracing/noop_bench_test.go` - util/tracing: Tests noop log KV.
- `pkg/util/tracing/util_test.go` - util/tracing: Tests span from context.

## pkg/util/vitess

### Tests
- `pkg/util/vitess/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/util/vitess/vitess_hash_test.go` - util/vitess: Tests vitess hash.

## pkg/util/watcher

### Tests
- `pkg/util/watcher/watcher_test.go` - util/watcher: Tests watcher.

## pkg/util/workloadrepo

### Tests
- `pkg/util/workloadrepo/worker_test.go` - util/workloadrepo: Tests race to create tables worker.

## pkg/util/zeropool

### Tests
- `pkg/util/zeropool/pool_test.go` - util/zeropool: Tests pool.
