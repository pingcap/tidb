# `pkg/util/globalconn` — complete package transcreation

Go source: `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02). The five package
files are byte-identical to the exact detached Go worktree at that revision.

## Complete inventory

The package has exactly five artifacts (1,391 textual lines), all read in
full:

- `BUILD.bazel` (32 lines) — library and test targets;
- `globalconn.go` (355 lines) — GCID packing/parsing, simple/global
  allocators, reserved IDs, and the linker-injected global-kill width values;
- `globalconn_test.go` (229 lines) — the nine source unit tests;
- `pool.go` (275 lines) — auto-increment and lock-free circular pools;
- `pool_test.go` (500 lines) — pool tests, reference queue, concurrency cases,
  and the two benchmark families.

There is no `doc.go`, README, fixture, `testdata`, generated input/output,
platform variant, fuzz target, example, `go:generate`, `go:embed`, or nested
package. The global-kill integration build changes the 32-bit server/local
widths only through linker values; no additional Go source artifact exists.

## Rust ownership and parity result

`rust/crates/tidb-util/src/globalconn/mod.rs` and `pool.rs` are the production
owner. The prior audit already restored Go's wrapping ring-mask arithmetic,
source-shaped pool layout, global-kill build configuration, and exact allocator
and pool behavior; the owner keeps the nine source tests and two benchmark
families. The ordinary `tidb-server` connection tracker remains the live
consumer.

This authority refresh found two Rust-only `#[must_use]` diagnostics on
`Gcid::to_conn_id` and `SimpleAllocator::new`. Go permits both return values to
be discarded, so the annotations were removed. The focused
`return_values_may_be_ignored_like_go` regression is denied against
`unused_must_use`: before the fix it failed with two compiler errors, and after
the fix it passes with the ten inline Rust tests (nine source-shaped tests plus
the parity regression).

## Validation

Profile: **Ready** for this focused parity fix within the continuing
package-by-package audit, not a repository-wide readiness claim.

- `git -c maintenance.auto=false -c gc.auto=0 fetch origin master --prune` —
  passed; `origin/master` is `c6054025ed4c32ab3672a2a24ea46892714d21ec`.
- `cmp -s /tmp/tidb-go-latest-c605/pkg/util/globalconn/<file> pkg/util/globalconn/<file>`
  for all five artifacts — passed; every file matches the latest Go worktree.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/globalconn -count=1` — passed in the current and exact detached latest-master worktrees.
- Pre-fix focused Rust compile with the new deny-lint regression — failed with
  the expected two `unused_must_use` errors.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib CARGO_INCREMENTAL=0 cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib globalconn::tests --offline --locked -- --test-threads=1` — passed; ten tests.
- `cd rust && OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 fmt --all -- --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed.
- `git diff --check` — passed.

No Go, Bazel, module, or Cargo manifest file changed, so `make bazel_prepare`
was not required.

## Risk

- Correctness: the source Go package and latest detached worktree pass; the
  Rust owner passes all ten inline tests, including the fail-before/fail-after
  diagnostic regression.
- Compatibility: return annotations were the only production change; callers
  may now ignore the values exactly as Go callers do. No packed-ID arithmetic,
  allocation, or pool sequencing changed.
- Performance: unchanged; this removes compile-time diagnostics only.
