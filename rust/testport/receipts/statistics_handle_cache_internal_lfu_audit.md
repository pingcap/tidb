# `pkg/statistics/handle/cache/internal/lfu` parity audit

Pinned source: `c6054025ed4c32ab3672a2a24ea46892714d21ec` (Go `master` at the
audit boundary).

This is an audit receipt, not a package-completion claim. The package depends
on `github.com/dgraph-io/ristretto`; repository policy requires that external
package to be consumed as a complete implementation or transcreated as its own
complete pinned package. No complete Rust Ristretto owner exists in this
workspace or the local dependency cache.

## Atomic inventory

| Artifact | Lines | Git blob | SHA-256 |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 37 | `c8e67ed8c85bb67450941c7730836852d05efb01` | `e6b4ed348fe8d34d5b760229931068637f874248127673c491737eb34f6eda02` |
| `key_set.go` | 74 | `5e3e6f1deef23f84af1b52b225b96b83139f22f3` | `f4aa32c896ba0445340b1e43932dbe456d5f0172d9954ac7c5ed231f30454cbd` |
| `key_set_shard.go` | 69 | `396842a6ef2465784ac0a4c37cf6495720a584eb` | `357036a84ea064c7e9ee079eb3eddfd153961f7d2593a583560a4e3ff57dac54` |
| `lfu_cache.go` | 286 | `20ee62bf973c888d228550025f40c4b1520dcb97` | `5f4618cfff8c3b73261fad9d206171dbb62976f3823cfb2b17840b3c81b153fd` |
| `lfu_cache_test.go` | 316 | `e77571fb936d033ab73072d213cc9637ac016413` | `e5af4feb31baf59faf5ae99a0ef8c67d165c49bddfc655323bb31c742cc914f9` |

There are no generated, platform-specific, fixture, or benchmark artifacts.
The BUILD test target enables the race detector, is flaky, has ten shards, and
contains all ten tests in `lfu_cache_test.go`. The current checkout is
byte-identical to this Go-master pin.

## Removed false surfaces

The former `tidb-stats` modules `stats_key_set`, `stats_key_set_shards`, and
`memory_cost` were not the Go package:

- key sets stored caller-provided costs instead of shared statistics tables,
  so `Remove` could not derive `TotalTrackingMemUsage` from the value;
- the shard wrapper publicly exposed its internal shard count and extra
  `Default`/`is_empty` behavior, and changed negative-key behavior from Go's
  invalid negative array index to Euclidean routing;
- memory adjustment accepted a caller-provided optional memory total and
  exposed private Go constants/functions as public policy;
- their eight tests were absent from the pinned Go package and did not execute
  asynchronous admission, primary-before-secondary lookup, rejection,
  eviction, table-copy/drop behavior, close suppression, metrics, or
  concurrency.

Those modules, tests, and exports were removed. The stale function-batch
`b044.md` receipt was also removed; its LFU entries referred to ignored test
functions that no longer existed, and package completion cannot be claimed by
function batches.

The re-audit found two concrete source-vs-owner gaps and corrected them:

- `KeySetShard` now computes the signed Go remainder before indexing, so a
  negative table ID panics instead of silently routing to a Rust-only shard.
- The test-mode constructor now applies Go's five-million-byte default when a
  zero quota is requested, avoiding a host-sized TinyLFU sketch.

Focused regressions failed before each change and pass after. The Rust owner
also retains eight native tests covering source put/get/delete, rejection and
metadata retention, replacement cost, copy sharing, capacity reduction, and
concurrent access.

## Remaining package behavior

A complete owner must preserve all three production files together, including:

- 256 table-valued, independently locked fallback shards;
- Ristretto's TinyLFU counters, buffered asynchronous admission, resident-key
  update behavior, sampled rejection/eviction, callbacks, metrics, `Wait`,
  dynamic `MaxCost`, clear, and close behavior;
- primary-cache-first reads and their documented stale-read window;
- tracking-memory accounting across put, reject, evict, exit, table
  `CopyAs(AllDataWritable)`, `DropEvicted`, fake negative-key eviction
  triggers, and closed callback suppression;
- the exact 20%-of-host-memory and test-mode capacity paths;
- all ten source tests, including the race-enabled concurrency fixtures and
  eventual asynchronous memory-control checks.

The existing synchronous insertion-order cache in `tidb-session` explicitly
documents that it is not Ristretto-equivalent and was not reused here.

## Validation

Ready profile: this batch changes the Rust LFU owner and adds focused
regressions, so the source-tagged Go suite, race gate, owner tests, clippy,
formatting, repository lint, and diff hygiene were run.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test -tags=intest,deadlock ./pkg/statistics/handle/cache/internal/lfu -count=1` (current checkout)
- same tagged command in `/tmp/tidb-go-latest-c605` (detached Go master)
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test -race -tags=intest,deadlock ./pkg/statistics/handle/cache/internal/lfu -count=1` (detached Go master; BUILD's race setting)
- `env OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-stats-handle-cache-internal-lfu`
- `env OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... cargo +nightly-2026-08-22 clippy --manifest-path rust/Cargo.toml --offline --locked -p tidb-stats-handle-cache-internal-lfu --no-deps -- -D warnings`
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`
- pinned `make lint`
- `git diff --check`

`make bazel_prepare` is not required: no Go/Bazel source, import section, test
target, or module dependency changed. Full package completion remains blocked
by the required complete owner for external `github.com/dgraph-io/ristretto`;
the Rust `stretto` implementation is retained as executable seed/integration
evidence, not claimed as a drop-in Ristretto proof.
