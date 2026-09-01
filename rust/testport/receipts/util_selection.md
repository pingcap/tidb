# `pkg/util/selection` — complete package transcreation

Go source: `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02), unchanged from
extraction pin `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly four artifacts (433 textual lines), all read in full: `selection.go`,
`selection_test.go`, `main_test.go`, and `BUILD.bazel`. They define the
`sort.Interface` boundary, introselect and its median-of-medians fallback, the
test-only quickselect comparison, four unit tests, one benchmark with 21
size/algorithm cases, and the common test harness. There is no package doc,
README, fixture, generated/platform variant, or ownership file. The checkout
is byte-identical to the pin.

## Rust ownership and audit result

`rust/crates/tidb-util/src/selection.rs` is the production owner. The audit
restored Go's signed index result (`-1` for empty input), removed Rust's
unsupported `Option` and saturating rank-zero policy, retained exactly the
four Go unit tests, and restored the source quickselect comparison behind the
existing test-export boundary. The Rust-only public `Selectable::is_empty`
convenience was removed; the interface now has exactly Go's `Len`, `Less`, and
`Swap` operations. `rust/crates/tidb-util/benches/selection.rs`
contains all seven source sizes and all three source algorithms for each size.

The sole production consumer, HashAgg approximate percentile, now consumes
the Go-shaped signed selection index after its existing nonempty rank guard.

## Validation

Profile: WIP; this is one completed package within the continuing repository
audit, not a repository-wide readiness claim.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909..c6054025ed4c32ab3672a2a24ea46892714d21ec -- pkg/util/selection` — passed; no Go package drift.
- `git diff --exit-code c6054025ed4c32ab3672a2a24ea46892714d21ec..HEAD -- pkg/util/selection` — passed; no current-branch Go package drift.
- `git ls-tree -r -l c6054025ed4c32ab3672a2a24ea46892714d21ec pkg/util/selection` — passed; exactly the four artifacts listed above.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/selection -count=1` — passed in current and exact detached latest-master (`/tmp/tidb-go-latest-c605`) worktrees.
- `cargo test -p tidb-util --locked selection::tests::` — passed (4 tests).
- `cargo check -p tidb-util --bench selection --features testexport --locked`
  — passed.
- `cargo check -p tidb-executor --lib --locked` — passed.
- `cargo test -p tidb-util --locked` — passed (663 active, 3 existing ignored).
- `cargo test -p tidb-executor --lib --locked approx_percentile` — passed.
- `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: reduced; empty selection and result typing now match Go, and
  the production percentile caller is covered.
- Compatibility: the Rust-only `Option<usize>` API is intentionally replaced
  by Go's signed integer contract; the only production caller was migrated.
- Performance: the selection algorithm and depth-six fallback are unchanged.
