# Parallel DISTINCT aggregate spill parity receipt

Comparison source: Go `origin/master` at `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`.
The behavior authority is `ea373a0d8398d36d2666d68e3bfadbb86053848d`
(`executor: Support disk spilling for parallel Distinct Aggregate`). No Go
source was edited.

## Inventory

Before editing, the complete direct Go owner surface was enumerated and read:
95 tracked artifacts and 30,399 lines across the three packages touched by the
commit. This includes every production file, test, fixture/helper, generated
or platform variant, and Bazel build artifact:

```text
pkg/executor/aggfuncs/
BUILD.bazel OWNERS aggfunc_test.go aggfuncs.go builder.go export_test.go
func_avg.go func_avg_test.go func_bitfuncs.go func_bitfuncs_test.go
func_count.go func_count_distinct.go func_count_test.go func_cume_dist.go
func_cume_dist_test.go func_distinct_agg_test.go func_first_row.go
func_first_row_test.go func_group_concat.go func_group_concat_test.go
func_json_arrayagg.go func_json_arrayagg_test.go func_json_objectagg.go
func_json_objectagg_test.go func_lead_lag.go func_lead_lag_test.go
func_max_min.go func_max_min_test.go func_ntile.go func_ntile_test.go
func_percent_rank.go func_percent_rank_test.go func_percentile.go
func_percentile_test.go func_rank.go func_rank_test.go func_stddevpop.go
func_stddevpop_test.go func_stddevsamp.go func_stddevsamp_test.go
func_sum.go func_sum_int.go func_sum_test.go func_value.go func_value_test.go
func_varpop.go func_varpop_test.go func_varsamp.go func_varsamp_test.go
main_test.go row_number.go row_number_test.go spill_deserialize_helper.go
spill_helper_test.go spill_serialize_helper.go window_func_test.go

pkg/executor/aggregate/
BUILD.bazel OWNERS agg_hash_base_worker.go agg_hash_executor.go
agg_hash_final_worker.go agg_hash_partial_worker.go agg_spill.go
agg_spill_test.go agg_stream_executor.go agg_util.go

pkg/util/chunk/
BUILD.bazel alloc.go alloc_test.go chunk.go chunk_in_disk.go
chunk_in_disk_test.go chunk_test.go chunk_util.go chunk_util_test.go codec.go
codec_test.go column.go column_test.go compare.go iterator.go iterator_test.go
list.go list_test.go main_test.go mutrow.go mutrow_test.go pool.go pool_test.go
row.go row_container.go row_container_reader.go row_container_test.go
row_in_disk.go row_in_disk_test.go
```

The changed Go production functions were read function-by-function: every
`SerializePartialResult`/`DeserializePartialResult` implementation in
`pkg/executor/aggfuncs`, `HashAggExec.initForParallelExec`, the partial-worker
spill preparation/write/flush path, `CheckChunkSpill`, and `chunk.Chunk`'s
memory accounting. The aggregate spill, helper, unit, integration, and
fixture tests plus build files were checked for the complete call and format
surface.

## Behavior and Rust owner

Go's parallel hash aggregation now permits DISTINCT functions to enter the
partial/final spill pipeline. Each distinct aggregate serializes its worker
local value set (including decimal, temporal, percentile, JSON, and vector
variants); final workers deserialize and merge the set, rather than adding
already-folded worker scalars that may contain the same value twice.

Rust already retained the original DISTINCT inputs in `AggState` so worker
merges could deduplicate them, but its parallel spill gate still rejected all
DISTINCT functions and its spill record only carried the folded `Partial`
scalar. The direct Rust owners are `tidb-executor::hash_agg::parallel` and
`tidb-executor::agg_spill`.

## Change and regression

* Removed the Rust-only `!function.distinct` spill gate.
* Extended the spill record with each DISTINCT input's collation key, datum,
  extra arguments, and aggregate-local sort key; restored records rebuild the
  value set before final-worker merge.
* Kept the existing scalar partial encoding for non-DISTINCT functions and
  preserved the existing merge path, which replays only values newly admitted
  to the destination set.
* Added `parallel_hashagg_distinct_spill_preserves_value_sets`, comparing a
  pressured parallel `COUNT(DISTINCT ...)` + `SUM(DISTINCT ...)` execution with
  an unspilled run and asserting that spill is actually triggered.

## Ready validation

```text
cargo test --offline --locked -p tidb-executor --lib \
  hash_agg::parallel::tests::parallel_hashagg_distinct_spill_preserves_value_sets \
  -- --nocapture
# passed: 1 test

cargo fmt --all -- --check
# passed
git diff --check
# passed
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
TMPDIR=/tmp/tidb-codex make lint
# passed; unrelated pre-existing warnings only
```

The broader Go aggregate spill suite exercises the live Go executor and is
not run from the Rust workspace; this receipt covers the dependency-closed
Rust parallel spill owner and its focused regression.
