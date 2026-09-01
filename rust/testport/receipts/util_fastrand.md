# `pkg/util/fastrand` — complete Go-master parity receipt

Comparison source: Go `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02). The Go package is
unchanged from the earlier pinned implementation; this receipt records the
rolling authority and complete artifact hashes.
Runtime boundary: Go `go1.25.10`, as declared by the pinned repository.

## Complete inventory

The package has exactly five Go-master artifacts and 227 lines, all read in
full: `BUILD.bazel`, `main_test.go`, `random.go`, `random_test.go`, and
`runtime.go`. There is no package `doc.go`, fixture, generated input/output,
platform file, README, or ownership file.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 27 | `d06067c2be322c94e19ecec0a48c755b4a37cd91` | `3d5370de520dbd14458ee17d6897607fd1579c40fbb42a82a00e3de5ae988c34` | library/test targets |
| `main_test.go` | 33 | `11ae979799f15ae8eefa936b4a86c3e0080cebc8` | `122e8c79a77352c22c0cb8d40827be464b22807aa76076e49f4b0c7962ac673e` | TestMain/goleak setup |
| `random.go` | 66 | `83b49174e3fe6ed550b13f8ca37557d8384627d0` | `2307711f830f2e695b7e4e7213b26b1378edc7a3e6ac015600a3653bce02aca3` | wyrand and bounded helpers |
| `random_test.go` | 77 | `de32d1d1cf8fc188f3cefc508a89c3d86f62077c` | `dfa6f969609483ca3781676cf0ca0ff1bc2cf979def01b2e5b0342158d085bba` | source test and four benchmarks |
| `runtime.go` | 24 | `6386a44f2fa717d018720be0c83ec018aba57198` | `d04e196efe03078c22d1b2ce252afc77b972c353d5909424c57a75ea05e8cfd1` | runtime cheaprand link |

Production behavior comprises the private 64-bit `wyrand`, ASCII buffer
generation excluding NUL and `$`, multiply-high `Uint32N`, power-of-two-aware
`Uint64N`, and `Uint32` linked to `runtime.cheaprand`. The package has one unit
test and four parallel benchmarks. `TestMain` only installs common Go test
state and goleak exclusions; it contains no package behavior.

Because `runtime.go` links outside TiDB, the official Go 1.25.10
`src/runtime/rand.go` implementation of `cheaprand` was also read before the
Rust runtime boundary was changed. It uses 64-bit `wyrand` on native
64-bit-multiply targets and the source xorshift64+ step on 32-bit targets.

## Rust ownership and audit result

`rust/crates/tidb-util/src/fastrand/` is the sole owner. `random.rs` preserves
the package's exact wrapping arithmetic and reduction formulas. `runtime.rs`
uses thread-local state as the native per-runtime-thread equivalent of Go's
per-M `cheaprand` state and keeps initialization infallible.

The audit retained the existing correct 64-bit runtime algorithm and added
the missing 32-bit xorshift branch, including native-endian state-word order.
The source has no deterministic runtime-seed API, so Rust does not expose one.

The inline suite retains the exact `TestRand` translation and now includes one
focused regression (`TestReturnValuesMayBeIgnoredLikeGo`) that rejects
Rust-only `must_use` diagnostics on the public helpers. Four supplemental
deterministic-vector, alphabet, zero-bound, and thread-local tests absent from
Go remain removed. `benches/fastrand.rs` retains executable translations of
all four source benchmarks and no additional cases. Existing statistics,
password-salt, trace-event, selection, memory, server, and statement-context
consumers continue to use the canonical package owner.

## Validation

Profile: **Ready** for this focused parity fix. Rust source changed, so the
owner tests, benchmark compilation, formatting, diff checks, and pinned
detached `make lint` gate were run. Go source/Bazel metadata did not change;
`make bazel_prepare` is not required.

```text
git diff --exit-code c6054025ed4c32ab3672a2a24ea46892714d21ec..origin/master \
  -- pkg/util/fastrand
# passed: Go package unchanged at the current authority pin

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/fastrand -count=1
# passed (current worktree and exact detached Go-master worktree; one test)

# Before the fix, the focused regression failed with four unused_must_use
# errors; after removing the Rust-only annotations it passes.
OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib fastrand::random::tests --offline --locked -- --test-threads=1
# passed: TestRand and TestReturnValuesMayBeIgnoredLikeGo (2 tests)

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml -p tidb-util --all-targets --offline --locked
# passed: owner and benchmark targets (workspace warnings only)

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --bench fastrand --offline --locked --no-run
# passed: benchmark executable compiled

cd rust && cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
make lint
# passed in a clean detached worktree with the fastrand source fix applied
```

Only `aarch64-apple-darwin` is installed locally, so the 32-bit branch was
reviewed against Go 1.25.10 source but not cross-compiled locally. No Go or
Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: the source test passes on both sides and the two runtime
  algorithms follow the declared Go toolchain. The 32-bit branch lacks a local
  cross-compilation gate because no 32-bit target is installed.
- Compatibility: removes only package-local supplemental tests; all public
  function signatures and in-tree consumers are unchanged.
- Performance: 64-bit production code is unchanged. The added 32-bit branch
  is the same fixed arithmetic step as Go and introduces no lock or policy.
