# `pkg/statistics/handle/cache` parity receipt

Pinned source: Go `master` at this repository's Go tree tip (the Go files
themselves; earlier rounds pinned snapshot
`c6054025ed4c32ab3672a2a24ea46892714d21ec`, which has since moved).

This receipt covers the complete root Go package. The `internal`, `metrics`,
and `internal/*` directories are separate Go packages with their own atomic
receipts.

## Atomic inventory (2026-09-06 refresh)

The pinned package has six artifacts and 1,263 lines. Every production,
test, benchmark, and build file was re-read before editing. There are no
package docs, fixtures, generated files, platform variants, or external
support artifacts.

| Artifact | Lines | Git blob | SHA-256 | Rust disposition |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 56 | `47524ad467f49882c580937d8fc9a1552c5b0df095cb652f3d476e363f3e8d89` (content hash) | — | Cargo crate and benchmark graph own the library/test dependencies. |
| `bench_test.go` | 186 | `aaf4e64fd433a408d7393ead26cb78d27acce80e` | `95599555482310f57c61548d242c54970bda9e9621d45b201fc899d711c5b641` | `benches/statscache.rs` retains all six LFU/map workload shapes. |
| `statscache.go` | 410 | `fd98ba1eb884e0b68b5daaa01ae8358a8d0d1c05` | `24512ba05feacebf4cbbce4902fe4348b28dabd3c2ad5eb12ae984ce565331d9` | `StatsCacheImpl` owns ordered refresh, batching, reuse/reload/delete, cancellation, health metrics, and lifecycle. |
| `statscache_test.go` | 202 | `602fe311ace06d4ecbfb62bd64ded6191a2ef2cf` | `56b0338289c8fb7ad273414e193ae78f31d15e33ea4850875945b6293e25647a` | Batch-boundary and exact health-bucket regressions map to owner tests. |
| `statscacheinner.go` | 197 | `68b6a08e8e21ce683d55b9ddd49777661f9ffd93` | `da23f0b514dc65114edcc3dadeb9f8a4c3007e2aa7bf046aa06a57b0f9ed80ed` | `StatsCache` owns LFU/map selection, counters, retry, COW/in-place update, version, capacity, eviction, wait, and close. |
| `stats_table_row_cache.go` | 212 | `682b9ba91df93b62c49e4e5c379758aafd0d2e23` | `741d6526e96b02605824ea218e5e0cc636b814bb401d80a5fb4cbd83383a9fa3` | `StatsTableRowCache` restored (see below): batch refresh, both-reads-or-nothing, warn-and-serve-stale. |

## Production behavior

`rust/crates/tidb-stats-handle-cache/src/lib.rs` owns the five-file root
cache contract minus the size cache, which lives in its own module:
`src/stats_table_row_cache.rs`.

### `StatsTableRowCache` — restored to parity (2026-09-06)

An earlier round of this receipt recorded that pinned Go deleted
`stats_table_row_cache.go` in favor of statement-local
`pkg/statistics/handle/storage/table_size_stats.go`. That was true at the
`c6054025` snapshot (the deletion is upstream #69955), but the Go tree this
repository pins does NOT carry #69955: `stats_table_row_cache.go` is present
at the tip, and `pkg/executor/infoschema_reader.go` consumes the
**process-wide** `cache.TableRowStatsCache` — `UpdateByID(sctx, tableIDs...)`
at `infoschema_reader.go:671`, then `EstimateDataLength`/`GetTableRows`/
`GetDataAndIndexLength` from the same cache (`:731`, `:1344-1393`). The
statement-local substitution therefore dropped real Go behavior:

1. **Refresh contract** (`UpdateByID`): row counts are read from
   `mysql.stats_meta where table_id in (...)`, column lengths from
   `mysql.stats_histograms where is_index = 0 and table_id in (...)`, and BOTH
   maps are copied into the cache (`maps.Copy` under one write lock) only when
   both reads succeed — a failed read returns before any copy.
2. **Reader contract** (`updateStatsCacheIfNeed` + row builders): a failed
   refresh only logs a warning (`logutil.BgLogger().Warn("cannot update stats
   cache for tables")`); the rows are then built from the cache's PREVIOUS
   values. First-statement failure reads as zeros; later failures serve the
   last good values.
3. **Upsert semantics**: `maps.Copy` overwrites only the keys the fresh reads
   carry, so tables absent from one batch keep their cached values.

`src/stats_table_row_cache.rs` ports this contract:
`get_table_rows`/`get_col_length` (zero defaults), `update` (both maps under
one lock, upsert-only), and `update_by_id(source, ids)` over a
`StatsTableRowSizeSource` abstraction of the two restricted reads — both
succeed or nothing is copied. The cache-side
`EstimateDataLength`/`GetDataAndIndexLength` (Go duplicates them between this
file and `storage/table_size_stats.go`) map to `snapshot()` feeding the single
Rust implementation of those pure estimators,
`tidb-exec::cluster_stats_load::TableSizeStats`.

`ClusterTableStorageStatsProvider`
(`tidb-server::cluster_session_node`) now owns the cache and follows the Go
flow: `update_by_id` over every visible table and partition ID, warn on
failure (`information_schema_stats_cache_update_failed`), then build the
returned `TableStorageStatistics` from the cache snapshot. Go's process-global
`TableRowStatsCache` maps to this provider-owned instance; because every
statement refreshes all visible IDs before reading, the only observable
difference the cache model fixes is the failure path, which now retains prior
values instead of zeroing.

Two documented narrows are kept: (a) when plan pruning retains only
`TABLE_ROWS`, the histogram read is skipped (the #69818 optimization Go
applies in `GetTableSizeStats`; the reader-side cache update in this Go line
still reads lengths — client-visible values are identical because length
values only feed columns the plan did not retain); (b) the dispatch zero
fallback now applies only to seam providers that surface their own errors —
the production provider never does.

### Root cache

The root cache preserves Go's LFU in-place versus map copy-on-write selection,
runtime quota backing, hit/miss/update/delete accounting, retrying `Put`,
lifecycle maximum version, targeted watermark suppression, dynamic capacity,
eviction/wait/close, ordered metadata refresh, ten-item independent
update/delete batches, cancellation, metadata-only reuse, storage reload,
DDL disappearance, per-table read-error continuation, and health gauges.

The statement-local size path remains implemented in
`tidb-exec::cluster_stats_load::TableSizeStats` and the server/session
information-schema boundary. Each statement reads `stats_meta`, reads
non-index `stats_histograms` only when a retained projection requests
`AVG_ROW_LENGTH`, `DATA_LENGTH`, or `INDEX_LENGTH`, computes fixed/variable
width and partition/global-index estimates, and overlays the result on a
scratch catalog. A failed restricted read at the provider produces Go's
cached-stale values (zeros only on the first failure), matching the reader
contract above.

## Test and benchmark mapping

- The two original Go tests map to Rust's
  `source_batch_update_flushes_each_side_at_its_limit` and
  `source_healthy_metrics_use_exact_buckets`.
- The size-cache contract maps to the new crate tests
  (`update_by_id_copies_both_maps_only_when_both_reads_succeed`,
  `update_upserts_and_retains_entries_absent_from_the_batch`,
  `absent_ids_read_as_zero`, `snapshot_reflects_every_cached_entry`) and the
  store-backed regression
  `tidb-exec::the_table_row_size_cache_serves_previous_values_after_a_failed_refresh`,
  which fails the real encoded `stats_meta` scan after a successful refresh
  and pins the retained (stale) values.
- Root owner tests additionally cover backend update modes, refresh reuse,
  deletion/error handling, cancellation, targeted watermark behavior, and the
  failpoint-gated cache miss.
- `tidb-exec::analyze_commit_size_source` covers the durable row-count and
  non-index column-length reads and the focused
  `table_size_stats_are_statement_local_and_clamp_negative_sizes` regression.
- `tidb-server::cluster_session::loaded_column_ndv_reaches_grouped_cluster_plans`
  proves TABLES projection pruning and histogram-read skipping for
  TABLE_ROWS-only scans. A follow-up round re-pinned its PARTITIONS section:
  current Go's `updateStatsCacheIfNeed` (`infoschema_reader.go:646-661`)
  self-prunes on the retained columns, so a TABLE_NAME-only PARTITIONS
  projection runs no refresh at all — the port's plan-level pruning matches,
  and the test's old "Go does not column-prune PARTITIONS" expectation (reads
  == 3) described the pre-merge reader and was updated to expect no third
  read.
- The benchmark target retains
  `BenchmarkStatsCacheLFUCopyAndUpdate`,
  `BenchmarkStatsCacheMapCacheCopyAndUpdate`, `BenchmarkLFUCachePutGet`,
  `BenchmarkMapCachePutGet`, `BenchmarkLFUCacheGet`, and `BenchmarkMapCacheGet`.

## Validation

Ready checks for the 2026-09-06 `StatsTableRowCache` round (all from
`rust/` with `OPENSSL_DIR`/`DYLD_FALLBACK_LIBRARY_PATH` set to the pinned
runtime):

    cargo +nightly-2026-08-22 test --offline --locked -p tidb-stats-handle-cache
    # 11 passed; 0 failed (unit + healthy-metrics + refresh suites)
    cargo +nightly-2026-08-22 test --offline --locked -p tidb-exec --test all the_table_row_size_cache
    # 1 passed; 0 failed (store-backed stale-serving regression)
    cargo +nightly-2026-08-22 test --offline --locked -p tidb-exec
    # 817 passed, 8 failed — all 8 failures reproduce on the pristine tip
    # (verified by stashing this batch); none touch this package.
    cargo +nightly-2026-08-22 clippy --offline --locked --no-deps --all-targets -p tidb-stats-handle-cache
    # zero findings in this crate
    cargo +nightly-2026-08-22 clippy --offline --locked --no-deps -p tidb-server
    # no findings in the files this batch touched
    cargo +nightly-2026-08-22 fmt --all -- --check
    # clean except four pre-existing sibling in-flight files
    # (access_cost.rs, planner_bridge.rs, rule_aggregation_push_down.rs,
    # inject_extra_projection.rs) untouched by this batch
    cargo +nightly-2026-08-22 check --offline --locked -p tidb-server

The Go/Bazel gate is not required: this batch changes only Rust sources.
Go behavior was verified by reading the Go tree at the tip, not by running
the Go suite (the pinned Go package has no server-dependent test that runs
without a cluster).

Ready validation for this batch is required before its commit is reported:

- pinned Go package tests through `tools/check/failpoint-go-test.sh`;
- Rust owner/consumer tests and checks above;
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`;
- pinned `make lint`;
- `git diff --check`.

No Go or Bazel source was changed in this batch, so `make bazel_prepare` is
not required. This is one atomic root-package receipt, not a repository-wide
parity claim.

## 2026-09-06 return-contract alignment follow-up

The complete five-artifact Go root package above remains unchanged between
the receipt pin and current `origin/master`
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723`. Per the requested Rust-only
scope, no Go source was edited and no Go test was executed. The complete Rust
owner was re-read before editing: the 35-line `Cargo.toml`, the pre-change
815-line `src/lib.rs`, and the 130-line `benches/statscache.rs`. The crate has
no build script, generated or platform-specific sources, fixtures, examples,
or external test target; the benchmark retains all six source workload shapes.

Ten direct Go-shaped cache returns had Rust-only `#[must_use]` enforcement:
`StatsCacheImpl::{next_check_version_with_offset, mem_consumed,
max_table_stats_version, values, len}` and
`StatsCache::{len, values, cost, version, copy_and_update}`. The annotations
were removed without changing cache behavior. The focused
`go_cache_returns_may_be_ignored_like_go` regression, under
`#[deny(unused_must_use)]`, failed before the fix with exactly ten diagnostics
(`/tmp/tidb-stats-cache-prefix.log`) and passes afterward. The two annotations
on `StatsCacheImpl::get` and `StatsCache::get` remain deliberately: their
`Option<Arc<Table>>` results are inherently must-use in Rust even without a
function annotation, so deleting only the redundant annotations would not
change the caller contract or establish Go parity.

Ready validation for this Rust-only follow-up:

- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-stats-handle-cache --lib go_cache_returns_may_be_ignored_like_go -- --test-threads=1` (1 passed);
- `cargo +nightly-2026-08-22 nextest run --manifest-path rust/Cargo.toml --offline --locked -p tidb-stats-handle-cache --lib --test-threads=1` (8 passed);
- `cargo +nightly-2026-08-22 nextest run --manifest-path rust/Cargo.toml --offline --locked -p tidb-stats-handle-cache --lib --features failpoints --test-threads=1` (9 passed, including the failpoint-gated miss regression);
- `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-stats-handle-cache --all-targets --quiet`;
- `rustfmt +nightly-2026-08-22 --check --edition 2021 rust/crates/tidb-stats-handle-cache/src/lib.rs rust/crates/tidb-stats-handle-cache/benches/statscache.rs`;
- `make lint`;
- `git diff --check`.

No runtime, cache-algorithm, compatibility, or performance behavior changed;
the regression is compile-time return-contract evidence. No Go/Bazel/module or
import graph changed, so `make bazel_prepare` remains unnecessary.
