# `pkg/executor/internal/pdhelper` — complete Go-master parity receipt

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete inventory

The package contains four tracked artifacts and 288 lines. Every production
source, test harness, test, and Bazel target was read line by line before the
Rust edit. There are no generated sources, platform variants, benchmarks,
fuzz targets, or fixture files.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 38 | `7a6e95233d771f5b4be0dd23bc4bcd2828d10a52` | `ca2b8d2ae760ee7028e74f981f41928a7089416e02a0174328dfaa0046c71888` | internal library and two-shard flaky test target |
| `main_test.go` | 53 | `4725130c65edda4e9bd3b36ecf570e60984c4f42` | `17ba4b34eeb1c67230cd72f647bf24a658cd04100574bddb305996832ad18683` | common setup, TiKV failpoints, and goleak harness |
| `pd.go` | 128 | `a1e43ba00f4017e1b206a576dea1e057f77ba4bf` | `0994428a763c56cba33d28b562a853171f8d809b75b01be0035bbbf40224b91d` | process-global PD approximate-count cache and storage/SQL fallback |
| `pd_test.go` | 69 | `f35c9c9e5a93fb76d60701c68dbcea4bef1a8118` | `6b258ea36561f961fe09e35fb45c717316fd9538820539c4f07fbd5d4540ffb8` | TTL, capacity, LRU, hit/miss, and expiry test |

`PDHelper` owns a capacity-1,048,576, 30-second TTL cache keyed by the direct
`tableID_db_table_partition` join. On a miss it asks PD for record-region
statistics, uses `storage_keys` when more than two regions are present, and
otherwise executes an internal `COUNT(*)` (including a partition clause) with
the statistics foreground priority. Storage/SQL failures cache `(0,false)`;
cache hits return `(value,true)`. `Start`/`Stop` run the cache cleanup worker,
and the test harness enables TiKV failpoints and checks for goroutine leaks.

## Rust ownership and parity fix

Rust's dependency-closed production owner is `tidb-exec::pd_approximate_count`
plus the cluster-session provider that shares one cache between SHOW and
ANALYZE. It preserves source key identity, PD-region versus exact-count
fallback, failure caching, cache-hit `has_pd`, resource-group propagation,
capacity/LRU/TTL, and the joined expiry worker.

The audit found three public cache helpers with no Go API and no production
caller: `get_or_load`, `get_or_load_table`, `len`, and `is_empty` (the latter
two are observers). They were Rust-only test conveniences. Removed them and
rewrote the source-derived cache tests to call the production `get` and
`insert` methods directly; the cache behavior remains covered by the same
capacity, LRU, TTL, and cleanup assertions. No Go-visible behavior was changed
and no speculative API was added.

## Validation and risk

Profile: **Ready** for this code batch. Rust source and test code changed; no
Go source, imports, Bazel metadata, or module files changed, so
`make bazel_prepare` is not required.

```text
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-exec \
  --test all pd_approximate_count --offline --locked
# passed: 3 source-derived tests
```

The Rust test command completed with existing workspace warnings. The Ready
`make lint` gate passed. Changed-file `rustfmt --check` passed; the workspace
`cargo fmt --all -- --check` reports pre-existing formatting drift in unrelated
`tidb-planner` worktree files. Not verified here:
full downstream Go suites beyond the package test, Bazel execution, and full
workspace tests. Existing unrelated privilege/session and planner worktree
changes remain outside this receipt.
