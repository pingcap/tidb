# `pkg/executor/internal/vecgroupchecker` — complete Go-master parity receipt

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete inventory

The package contains four tracked artifacts and 939 lines. Every production
file, source test, common `TestMain`, and Bazel target was read line by line.
There is no generated source, fixture, benchmark, fuzz target, or platform
variant. The package has no failpoint calls; its `TestMain` enables the shared
TikV failpoint hooks as part of the common test harness.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 40 | `4cc2db005b45f4a1c60b53bb74c6fcaf6cee84aa` | `3167139245f1ab3f4eb5867b3a4db66cc49da8b8cb1d5245c127a541db3ee531` | internal library and four-shard flaky test target |
| `main_test.go` | 53 | `0220b483f64d6a5398eb219fb4380b5b45458a69` | `2c78f211de0fac54b059512b8fd8afd30edae9cb772d5390248083133d31c874` | common setup, failpoint enablement, and goleak harness |
| `vec_group_checker.go` | 566 | `242eb21009a28ffd4d7c23a03be411fb01d8bc98` | `25e9f350932453ed382742d805b50e056021e221aa5f2df2fbbf63b31f720bfe` | vectorized adjacent-key grouping state machine |
| `vec_group_checker_test.go` | 280 | `2f72ede3b5b7330560c042f9dffc9a6653110093` | `ac1cce0c1ae1eb2a8be2fc69cc6b9a20332dd4a898c453b26326c339c0299795` | data-race ownership, group-count, collation/padding, and reset tests |

`vec_group_checker.go` defines `VecGroupChecker`, boundary-key encoding for
all supported evaluation types, vectorized comparison across adjacent rows,
cross-chunk continuation, range iteration, exhaustion, and reset. The tests
cover retained copies of variable-length/decimal/JSON values, chunk-boundary
group counts, binary and Unicode collations, trailing-space padding, and
issue 53867 reset behavior. `BUILD.bazel` limits the library to executor
subpackages and wires the four-shard test target; `main_test.go` configures
common TiDB state and goleak.

## Rust ownership and parity fix

`tidb-executor::vec_group_checker` is the dependency-closed Rust owner and is
used by `shuffle` plus its source-derived tests. Its grouping behavior and
crate-internal source coverage remain unchanged. The Rust port previously
exported the module, `VecGroupChecker`, and all six operational methods as a
public API even though Go's package is under `pkg/executor/internal` and no
other Rust crate calls it. That was Rust-only visibility, not Go behavior.

The fix narrows the module, type, constructor, split/range/exhaustion/reset,
and group-count methods to `pub(crate)`. No external Rust test or fixture is
removed because all existing coverage is in-crate and exercises the same
internal contract. No Go behavior or new API was introduced.

## Validation and risk

Profile: **Ready** for this Rust visibility cleanup. No Go or Bazel source
changed, so `make bazel_prepare` is not required.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/executor/internal/vecgroupchecker -count=1
# passed; all Go source tests

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-executor --lib vec_group_checker --offline --locked
# passed; 12 Rust checker and source-derived tests

make lint
# passed
```

The pre-change Rust checker suite passed the same 12 tests; the change only
removes an uncalled external API surface. Existing Rust compiler warnings and
the unrelated dirty `tidb-txnkv` files remain. Not verified: Bazel execution
and full workspace tests.
