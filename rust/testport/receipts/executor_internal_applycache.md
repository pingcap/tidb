# `pkg/executor/internal/applycache` — complete Go-master parity receipt

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete inventory

The package contains four tracked artifacts and 338 lines. Every production
file, source test, common `TestMain`, and Bazel target was read line by line.
There is no generated source, fixture, benchmark, fuzz target, or platform
variant.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 40 | `44685a118308295de4279032c13b1afffa96e757` | `ab4310ccc168a2d54ebf37e9d5ef3a6e1513be843ac1f7c53a5f9a32a9249c8c` | internal library and two-shard race/flaky test target |
| `apply_cache.go` | 107 | `e3740b760153657663b4d622ba65155c43f13839` | `605885bcafe0b8f68d1ea04b0aeb72a1d59733bcfc454f508dcad165dc5caba6` | memory-budgeted synchronized LRU cache |
| `apply_cache_test.go` | 138 | `569c07758e29ca28350eed85646e0d15164e6553` | `74d586a380f2b4b3e6472cf3f96505c7931a101d76fd76506f625478d894ba62` | eviction and concurrent Get/Set tests |
| `main_test.go` | 53 | `8fad2e6da729200fd8d93e0ad83a383be68ee39c` | `ba388d6cbb032a6e516b09089d4d945ba8dc404b399c8707423a374ea643ab56` | common setup, failpoint, and goleak harness |

`apply_cache.go` defines `ApplyCache`, the private `applyCacheKey` hash
adapter, `applyCacheKVMem`, `NewApplyCache`, synchronized `Get`/`Set`, and
`GetMemTracker`. Its source policy charges key bytes plus retained list
memory, rejects over-quota values, and evicts oldest entries until the new
pair fits. The two source tests cover exact admission/eviction and concurrent
alternating keys; `TestMain` configures common TiDB test state and goleak.

## Rust ownership and parity fix

`tidb-executor::apply_cache` is the dependency-closed Rust owner and is wired
into the live `ApplyExec` path. Its cache admission, LRU ordering, key/value
charge, and synchronization remain unchanged. The Rust port previously made
`ApplyCache`, `set_shared`, `memory_consumed`, `len`, `is_empty`, and helper
functions public and carried a separate `tests/apply_cache_source.rs` suite.
Those were Rust-only public/test surfaces: Go's package is internal and
exposes no length/empty/charge observer. The fix narrows the module and cache
helpers to `pub(crate)`, removes the uncalled `len`/`is_empty` observers and
the supplemental external test file, and retains source-derived unit tests in
the crate plus live ApplyExec cache-reuse coverage.

No Go behavior changed; no new Rust API or alternative policy was introduced.

## Validation and risk

Profile: **Ready** for this Rust visibility/API cleanup. No Go or Bazel source
changed, so `make bazel_prepare` is not required.

```text
OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-executor --lib tests_executor_internal_source --offline --locked
# passed; 6 source-derived executor-internal tests, including two apply-cache tests

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/executor/internal/applycache -count=1
# passed; 2 Go source tests

make lint
# passed
```

The pre-change Rust external apply-cache suite passed five tests; after the
cleanup the crate-internal source suite passed and the removed API cannot be
used by external crates. Existing Rust compiler warnings and the unrelated
dirty `tidb-txnkv` files remain. Not verified: Bazel execution and full
workspace tests.
