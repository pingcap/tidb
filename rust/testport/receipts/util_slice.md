# `pkg/util/slice` — complete Go-master parity receipt

Comparison source: Go `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02). The package is
unchanged from the earlier pinned implementation; this receipt records the
rolling authority and complete artifact hashes.

## Complete inventory

The package has exactly four tracked artifacts and 149 lines, all read in
full: `BUILD.bazel`, `main_test.go`, `slice.go`, and `slice_test.go`. They
define one public Bazel library/test target, the `TestMain` goleak harness,
three production functions (`AllOf`, `Int64sToStrings`, and `DeepClone`), and
the four-row table-driven `TestSlice`. There is no package `doc.go`, README,
fixture, benchmark, fuzz target, generated source, platform variant, or
nested package.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 24 | `946ff90ddc408c38582da55f1b734dba34a74043` | `764e4887dbbd32b2088a5334b2c1290fa901fe0d581153a950d1b009ad51e7de` | library/test targets |
| `main_test.go` | 33 | `23ba88c2d4a34098c0fd7c97d78de4f3e6701e0a` | `53d6fcd083db19b908006557cee85cc90e842b72d52579b2b85a68b86772179c` | TestMain/goleak setup |
| `slice.go` | 51 | `624eec4d6c28bbad3d35932cca17d1b44b992609` | `7cd1bb4402d53901a203f8f4075859a4149f5c2fd440d23336747163c833970b` | production helpers |
| `slice_test.go` | 41 | `9a447baca394a11adf8f592c068e033009c9d147` | `23b14a7d7fd19fece9346a19a6c9967488c3281a78e9cf606bf2cb85a872470d` | source unit test |

## Rust ownership and audit result

`rust/crates/tidb-util/src/slice.rs` is the sole owner. All three production
functions were already present: `all_of` preserves empty truth and
short-circuiting, `int64s_to_strings` uses signed base-ten formatting, and
`deep_clone` keeps Go nil distinct from a present empty slice while invoking
the element clone operation.

The audit removed four supplementary Rust tests absent from the pinned package
and retained the single table-driven `TestSlice` translation with all four
source rows. The only production consumer, statistics bootstrap SQL, continues
to use the package conversion function.

The strict-surface re-audit also removed Rust-only `must_use` diagnostics from
`Int64sToStrings` and `DeepClone`; Go exposes both as ordinary functions.

## Validation and risk

Profile: **Ready** for this documentation-only authority refresh. No Go
source, imports, Bazel metadata, or module files changed, so `make
bazel_prepare` is not required. No new regression test is added because this
batch changes no behavior; the existing source-derived `TestSlice` remains
retained by the Rust owner.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/slice -count=1
# passed (current worktree and exact detached Go-master worktree)

cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml \
  -p tidb-util --lib slice::tests:: --offline --locked
# passed: source-derived TestSlice

cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml \
  -p tidb-stats --lib --offline --locked
# passed: only production consumer

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

Not verified here: full workspace tests, Bazel execution, or unrelated
statistics packages. Existing non-slice worktree changes remain outside this
receipt.

## Risk

- Correctness: unchanged; production behavior was already aligned.
- Compatibility: only non-source test code is removed.
- Performance: unchanged.
