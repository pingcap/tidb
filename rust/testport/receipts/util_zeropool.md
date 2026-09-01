# `pkg/util/zeropool` — complete Go-master parity receipt

Comparison source: Go `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02). The package is
unchanged from the earlier pinned implementation; this receipt records the
rolling authority and complete artifact hashes.

## Complete inventory

The package has exactly three Go-master artifacts and 281 lines, all read in
full: `BUILD.bazel`, `pool.go`, and `pool_test.go`. There is no package
`doc.go`, README, fixture, generated or platform variant, or ownership file.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 20 | `016c0c6f674abc3f5a4f00df3a2297d2c402bec7` | `d2cf6c94236cbde3984132bd461b7899eacbf9fedd0f4cf0016a9d02f0763a24` | library/test targets |
| `pool.go` | 83 | `6b03b743e5e03922b114a228197dd121ec007e93` | `6f55a4b32126c0e2ad1d5883fc5a3efbfbc02a1f2e64c9afb869660e6d5e763a` | generic zero-allocation pool |
| `pool_test.go` | 178 | `94f367dd82cc1593a61b317cae2c8b3748f6e1e6` | `afb7a2aeb37c5b4664f696643dc702671a92081ffe62cf1adfe347524db2e493` | four-subtest suite and four benchmarks |

Production behavior includes the valid generic zero value, optional factory,
concurrent `Get` and `Put`, move-out without retaining the returned value, and
the no-copy-after-use contract. The source has exactly one test, `TestPool`,
with four subtests, plus four benchmarks.

## Rust ownership and audit result

`rust/crates/tidb-util/src/zeropool/mod.rs` owns production and the single
source test. Rust moves `T` directly through a mutex-protected native value
pool, so Go's secondary pointer pool and interface-boxing workaround are not
needed. `Default` represents Go's valid zero value, and the absence of
`Clone`/`Copy` preserves its no-copy contract. Mutex poison is recovered
because Go mutexes do not introduce poison failures.

`rust/crates/tidb-util/benches/zeropool.rs` contains the four source benchmark
translations. The `BenchmarkSyncPoolValue` translation type-erases each value
and allocates a fresh box on every `Put`, preserving the allocation behavior
that benchmark exists to contrast; the old concrete `Vec` pool silently
removed that source behavior. The audit removed four supplemental Rust tests
with no Go equivalent; the remaining test is exactly `TestPool` and retains
all four Go subtest behaviors.

## Validation and risk

Profile: **Ready** for this documentation-only authority refresh. No Go
source, imports, Bazel metadata, or module files changed, so `make
bazel_prepare` is not required. No source behavior changed and no new
regression test is added; the existing source-derived `TestPool` suite remains
the focused regression carrier.

```text
git diff --exit-code c6054025ed4c32ab3672a2a24ea46892714d21ec -- \
  pkg/util/zeropool
# passed: current package is unchanged from the previous authority pin

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/zeropool -count=1
# passed (current worktree and exact detached Go-master worktree)

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib zeropool::tests::TestPool --offline --locked -- --exact --test-threads=1
# passed: one source-owned test with four sub-behaviors

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml -p tidb-util --all-targets --offline --locked
# passed: owner plus all four benchmark translations

cd rust && cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
make lint
# passed in a clean detached Go-master checkout
```

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: production and source-owned test behavior are unchanged; the
  comparative value-pool benchmark now measures the source workload.
- Compatibility: no production API or source-owned test changed; the earlier
  audit removed only internal supplemental tests.
- Performance: production is unchanged. Only the intentionally allocating
  comparison benchmark becomes slower and representative of Go.
