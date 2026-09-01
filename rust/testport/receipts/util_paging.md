# `pkg/util/paging` — complete Go-master parity receipt

Comparison source: Go `origin/master` at
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01). The package is
unchanged from the earlier pinned implementation; this receipt records the
rolling authority and complete artifact hashes.

## Complete inventory

The package has exactly four Go-master artifacts and 162 lines, all read in
full: `BUILD.bazel`, `main_test.go`, `paging.go`, and `paging_test.go`. There is
no package `doc.go`, README, fixture, generated or platform variant, benchmark,
fuzz target, or ownership file.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 24 | `c5a5a4d9ba5b2bb6b4ad1ea25ac0d0384f5f5723` | `68fa50fbf34b44a6159ef302b384240533fdafcad405bdf5fc25fa7f44761f59` | library/test targets |
| `main_test.go` | 33 | `fd5aef62c58a3b1f675b901255ac66db3aa815fa` | `4bd3d5038bcb4d89c9890154cd7c07ece0617560c04f6c65fd525bf5a9f1fabf` | TestMain/goleak setup |
| `paging.go` | 69 | `3825fcfe50cd13e94eea3f9442f2d67a8f0ec587` | `fb4456f6225fc4db3e67e91972c8d4de6f2d6d3daec3a64f88c437ba7b04b6fd` | paging constants and formulas |
| `paging_test.go` | 36 | `7600bda2976dbfd864a35748f35cdbfb12d0c80f` | `65c7989b9ff6b89a826f12eed63ef20414c62b2457d98717916a1cc7ade3e5a4` | two source tests |

Production behavior consists of four constants, two private policy constants,
the paging-growth cap calculation, and the piecewise seek-count calculation.
The source suite has exactly `TestGrowPagingSize` and `TestCalculateSeekCnt`;
`TestMain` only installs the repository's common Go test environment and leak
check.

## Rust ownership and audit result

`rust/crates/tidb-util/src/paging.rs` owns the complete production package and
the two source test translations. The formulas preserve Go `uint64` wrapping
where Rust debug arithmetic would otherwise panic.

The audit removed three supplemental boundary/overflow tests and the duplicate
copies of both Go tests from `tidb-distsql`. It also removed Rust-only
`must_use` diagnostics from the two functions. The independent DistSQL paging
configuration-default test remains with its consumer package and is not part
of this package claim.

## Validation and risk

Profile: **Ready** for this documentation-only authority refresh. No Go
source, imports, Bazel metadata, or module files changed, so `make
bazel_prepare` is not required. No source behavior changed and no new
regression test is added; both source-derived tests remain focused regression
carriers.

```text
git diff --exit-code 0bc44483e3e41a8ea917d4382dc202369468d200..origin/master \
  -- pkg/util/paging
# passed: current package is unchanged from the previous authority pin

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/paging -count=1
# passed (current worktree and exact detached Go-master worktree; two tests)

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib paging::tests --offline --locked -- --test-threads=1
# passed: two source-derived tests

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-distsql --test all paging_source::paging_config_defaults_consume_the_policy_authority --offline --locked -- --exact --test-threads=1
# passed: one consumer-owned default-authority test

cd rust && cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed
```

No Go or Bazel file changed, so `make bazel_prepare` is not required. Full
workspace tests and Bazel execution remain outside this leaf receipt.

## Risk

- Correctness: production calculations are unchanged; the exact source tests
  remain authoritative.
- Compatibility: only compiler diagnostics and redundant test coverage are
  removed.
- Performance: unchanged.
