# `pkg/util/engine` — complete Go-master parity receipt

Comparison source: Go `origin/master` at
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01). The Go package is
unchanged from the earlier pinned implementation; this receipt records the
rolling authority, complete artifact hashes, and the Rust-only API fix.

## Complete inventory

The package has exactly three Go-master artifacts and 253 lines, all read in
full: `BUILD.bazel`, `engine.go`, and `engine_test.go`. There is no package
`doc.go`, fixture, generated input/output, platform file, README, ownership
file, benchmark, fuzz target, example, or nested package.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 24 | `c21b6957317d3a7a7186ecfccd5b3ccecc2b2e08` | `a670e693b3f8dcca4647a7f9b936b346087bcaa403b9179c42a63912bbb0ac6b` | library and short flaky test targets |
| `engine.go` | 60 | `eaf4ccfcf5b3503fd2a16058b513fef607bba01b` | `61c770e29e7843e2f1d78083bc7fde8452139c58fea0fd3b4a760d925baeb563` | three store-label classifiers |
| `engine_test.go` | 169 | `af9c27499e9829b9c8f77c5d08084d00fdeefe9a` | `e6ea0990b701de22d442a5f6bde2adc4de96d74af8142c34e8ccf020d682e570` | two five-case source matrices |

The production surface is `IsTiFlash` over protobuf stores,
`IsTiFlashHTTPResp` over PD HTTP stores, and `IsTiFlashWriteHTTPResp` over PD
HTTP stores. All three scan labels in source order and use exact,
case-sensitive comparisons. The source tests cover the five label cases for
each HTTP classifier: classic TiFlash, NextGen write, NextGen compute,
non-TiFlash, and no labels. The Go test target's `flaky` scheduling annotation
has no Cargo semantic analogue.

## Rust ownership and audit result

`rust/crates/tidb-pd-client/src/engine.rs` owns the complete behavior. Its
protobuf and normalized `PdStore` boundaries preserve the source label key,
value, and order. `engine=tiflash` and `engine=tiflash_compute` classify as
TiFlash; only `engine=tiflash` classifies as a write node. No engine-role
inference is added.

The audit removed Rust-only `#[must_use]` diagnostics from all three public
boolean classifiers. The focused `TestReturnValuesMayBeIgnoredLikeGo`
regression applies `#[deny(unused_must_use)]` and discards each return value;
it failed before the fix with three lint errors and passes afterward. The two
source-shaped five-case matrices remain unchanged.

## Validation

Profile: **Ready** for this focused parity fix. Rust source and its focused
test changed, so owner tests, package checking, formatting, and diff checks
were run. No Go source, Go test, Bazel metadata, or Go module file changed;
`make bazel_prepare` is not required.

```text
git diff --exit-code 5e8a1a229a7591ddac49a0cd3b795587c2595ab9..origin/master \
  -- pkg/util/engine
# passed: current package is unchanged from the previous authority pin

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/engine -count=1
# passed (current worktree and exact detached Go-master worktree; two tests)

# Before the fix, the focused regression failed with three unused_must_use
# errors; after removing the Rust-only annotations it passes.
OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-pd-client \
  --test engine_source --offline --locked -- --test-threads=1
# passed: two source matrices and TestReturnValuesMayBeIgnoredLikeGo (3 tests)

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml -p tidb-pd-client \
  --offline --locked
# passed: package library and all declared targets (workspace warnings only)

cd rust && cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed
```

The package has no live endpoint or platform-specific behavior to verify
locally. Full workspace tests and Bazel execution remain outside this leaf
receipt.

## Risk

- Correctness: all three source classifiers and both source matrices remain
  covered; the regression proves ignored boolean returns compile like Go.
- Compatibility: removing `#[must_use]` changes diagnostics only; public
  signatures, label matching, and write/compute role boundaries are unchanged.
- Performance: classification remains a single linear label scan with early
  return and no new allocation.
