# `pkg/planner/core/operator/physicalop` — storage-engine helper batch

Comparison source: Go `origin/master` at commit
`1c1a334d2be1dce64888b6e1f054462c566b0734` (2026-09-02), including the
alternative-round changes introduced by `a74cc59699`.

## Complete package inventory

The package boundary contains 57 tracked artifacts: `BUILD.bazel` plus 56 Go
files, totaling 17,900 lines in the working snapshot. The complete file list
was read before editing, including production, tests, generated output, and
build metadata:

```text
BUILD.bazel
base_physical_agg.go
base_physical_join.go
base_physical_plan.go
enforce.go
foreign_key.go
fragment.go
fragment_test.go
nominal_sort.go
physical_apply.go
physical_batch_point_get.go
physical_common_plans.go
physical_cte.go
physical_cte_table.go
physical_exchange_receiver.go
physical_exchange_sender.go
physical_expand.go
physical_hash_agg.go
physical_hash_join.go
physical_index_hash_join.go
physical_index_join.go
physical_index_merge_join.go
physical_index_reader.go
physical_index_scan.go
physical_indexlookup.go
physical_indexlookup_reader.go
physical_indexmerge_reader.go
physical_limit.go
physical_lock.go
physical_max_one_row.go
physical_mem_table.go
physical_merge_join.go
physical_plan_misc.go
physical_projection.go
physical_schema_producer.go
physical_selection.go
physical_sequence.go
physical_show.go
physical_shuffle.go
physical_sort.go
physical_stream_agg.go
physical_table_dual.go
physical_table_reader.go
physical_table_sample.go
physical_table_scan.go
physical_topn.go
physical_union_all.go
physical_union_scan.go
physical_utils.go
physical_utils_test.go
physical_window.go
plan_clone_generated.go
single_scan_index_join.go
storage_engine_usage.go
task.go
task_base.go
tiflash_predicate_push_down.go
```

There is no package `doc.go`, `OWNERS`, fixture/testdata directory, fuzz
corpus, platform-specific source, or generator input beyond the checked-in
`plan_clone_generated.go` output. The inventory contains 800 production
functions and six Go test functions. Existing failpoint hooks in `fragment.go`
are covered by the failpoint-aware package gate.

## Go behavior restored

`StorageEngineUsage` now walks physical operators while stopping at reader
boundaries, counts TiKV/TiFlash table readers, treats point/index readers as
TiKV, traverses both CTE seed and recursive plans, and leaves TiDB-side reads
unclassified. `HasSingleScanIndexJoin` recognizes plain index joins (including
embedded hash/merge variants), follows unary wrappers on the inner side, and
protects only TiKV handle or covering-index probes; double-read index lookup and
index-merge readers are excluded. Focused regressions cover nil, homogeneous,
mixed, CTE/wrapper, reader-boundary, and inner-side cases. BUILD metadata lists
both restored production files.

## Rust owner and boundary

`tidb-planner::storage_engine_usage` owns the same tree predicates over the
closed `PhysicalPlan` enum and has source-derived unit tests. The live
alternative-round optimizer integration (session isolation-engine mutation,
cost comparison, and round-driver cleanup) remains an explicit boundary for a
later dependency-closed planner batch; this commit does not claim the whole
planner package is transcreated.

## Validation (Ready profile)

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
TMPDIR=/tmp/tidb-codex \
./tools/check/failpoint-go-test.sh pkg/planner/core/operator/physicalop \
  -run 'Test(StorageEngineUsage|HasSingleScanIndexJoin)$' -count=1 -vet=off
# PASS; package 1.047s; failpoints disabled to refcount 0

cargo +nightly-2026-08-22 fmt --all -- --check
```

The Rust planner test target was attempted with
`cargo +nightly-2026-08-22 test --offline --locked -p tidb-planner --lib
storage_engine_usage -- --test-threads=1` but is blocked before compilation by
the local `openssl-sys` dependency because `pkg-config` and OpenSSL headers are
not installed. `make lint` passes with the pinned Go runtime. `make
bazel_prepare` is required because Go files and a top-level test body changed,
but is blocked locally by `make: bazel: No such file or directory`. `git diff
--check` passes.

Regression evidence: before the Go helper files and tests were restored, the
focused test symbols and helper APIs were absent; the focused failpoint-aware
run above passes after the change. The Rust source tests likewise compile only
with the new owner module present.

Risks are limited to engine classification and index-join shape detection;
the helpers are read-only tree walks. The optimizer round integration, full
Bazel shards, and Rust planner compilation remain unverified locally.
