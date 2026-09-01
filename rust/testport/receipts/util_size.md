# `pkg/util/size` — complete Go-master parity receipt

Comparison source: Go `origin/master` at
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01). The package is
unchanged from the earlier pinned audit; this receipt refreshes the rolling
master authority and corrects the artifact inventory hashes.

## Complete inventory

The package has exactly two tracked artifacts and 86 lines, both read in full:
`BUILD.bazel` and `size.go`. They define five binary size units and fifteen
commonly used Go ABI sizes for memory tracing. There is no package `doc.go`,
test, benchmark, fixture, generated/platform variant, README, or ownership
file.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 8 | `7fb2f6f6d09741536545e5b683777cb2a5ef81c4` | `284f23a6ab10e49c683bbad3a0e202d6a819bfa52dcf7365fb3e26319902897c9` | public Go utility library target |
| `size.go` | 78 | `2a534d04f80f950c187f804fbe7f5576f5bce23a` | `12f3b382a01df93e5dd0a0d022f9fe965679d7cdc501c9c764e52074829b5375` | Go ABI and binary-size constants |

## Rust ownership and audit result

`rust/crates/tidb-util/src/size/mod.rs` is the sole owner. Its five unit
constants and fifteen ABI constants were already complete. Architecture-width
values derive from the target word size; Go slice, string, interface,
function, and map values retain Go header sizes rather than substituting Rust
container layouts. The audit removed the supplementary Rust constant-table
test because the pinned Go package has no test artifact.

## Validation

Profile: **Ready** for this documentation-only authority refresh. No Go
source, imports, Bazel metadata, or module files changed, so `make
bazel_prepare` is not required. No source behavior changed and no regression
test is added because the package has no source tests.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/size -count=1
# passed (current worktree and exact detached Go-master worktree; no test files)

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml -p tidb-util --lib --offline --locked
# passed (workspace warnings only)

cd rust && cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed
```

Not verified here: full workspace tests, Bazel execution, or external memory
tracing consumers. Existing unrelated worktree changes remain outside this
receipt.

## Risk

- Correctness: unchanged; all production constants were already aligned.
- Compatibility: only a Rust-only test is removed.
- Performance: unchanged; all values remain compile-time constants.
