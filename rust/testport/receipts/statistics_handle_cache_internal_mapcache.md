# `pkg/statistics/handle/cache/internal/mapcache` → `tidb-stats-handle-cache-internal-mapcache`

Pinned source: `c6054025ed4c32ab3672a2a24ea46892714d21ec` (Go `master` at the
audit boundary).

## Atomic inventory

| Artifact | Lines | Git blob | SHA-256 | Rust owner |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 12 | `38c800e866f91ee3ceb92ad73acb1a8d9e7a7089` | `cdab4f2f2e78df456c66e90066966a18650e93cb8d0aac100297e130e95dbdd4` | workspace target and crate manifest |
| `map_cache.go` | 139 | `9970497512d339273e71d3745be74ad766725438` | `5fe0a2b388e30528ec0b62cc9774f7116da2106e3c42e978a276a41810324d63` | `src/lib.rs` |

This is the complete Go package: two production/build artifacts and 151
lines. It has no package documentation, tests, fixtures, benchmarks, generated
inputs/outputs, or platform-specific variants. The current checkout is
byte-identical to the pinned Go master source.

## Behavior mapping

- `MapCache` stores shared actual `tidb_stats::Table` values keyed by signed
  table ID; `get`, `keys`, `values`, and `len` preserve the source map
  semantics and unspecified iteration order.
- `put` derives each cost from `Table::memory_usage().total_mem_usage`, updates
  replacement deltas, and preserves Go's wrapping signed `int64` arithmetic.
- `del` removes only present entries and subtracts their recorded cost.
- `copy` creates an independent map and aggregate counter while retaining the
  same shared table pointers and per-item key/cost values.
- `set_capacity`, `close`, `trigger_evict`, and `wait_for_async_updates` are
  exact source no-ops. The Rust `RwLock` is the minimum synchronization needed
  by the shared `StatsCacheInner: Send + Sync` contract; it adds no cache
  operation or alternate eviction policy.

The former generic `tidb_stats::MapCache<V>` and caller-supplied cost surface
are absent. No source-vs-owner production gap or Rust-only cache behavior was
found in this re-audit. The owner retains two focused native tests for source
put/replace/delete/copy semantics and the required shared-read safety.

## Validation

Ready profile: this source package has no Go tests, so the package gate
compiles and lints the complete Rust owner, runs its focused owner tests, and
checks both current and detached Go package probes.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/statistics/handle/cache/internal/mapcache -count=1` (current checkout)
- same pinned Go command in `/tmp/tidb-go-latest-c605` (detached Go master)
- `env OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-stats-handle-cache-internal-mapcache`
- `env OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... cargo +nightly-2026-08-22 clippy --manifest-path rust/Cargo.toml --offline --locked -p tidb-stats-handle-cache-internal-mapcache --no-deps -- -D warnings`
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`
- pinned `make lint`
- `git diff --check`

No Go or Bazel source changed; `make bazel_prepare` is not required for this
documentation-only receipt refresh.
