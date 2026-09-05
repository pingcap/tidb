# Rust session `SET_VAR` warning ownership receipt

Status: bounded Rust-only alignment batch; this receipt covers effective
`IsHintUpdatableVerified` lookup for duplicated `SET_VAR` hints.

Comparison source: Go `origin/master` at
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723` (2026-09-06).

## Complete package inventory

Before editing, every tracked root artifact in the Go session package was
enumerated and read from the fetched tree: 92 artifacts under `pkg/session`
(25 production Go files, 45 tests, and fixture, generated, platform, build,
and metadata files). The related Go expression package was rechecked in full:
208 artifacts (117 production Go files, 78 tests, generated/build inputs, and
package metadata). The planner package was also rechecked in full: 568
artifacts (196 production Go files, 166 tests, plus generated sources,
fixtures, platform variants, `BUILD.bazel`, and ownership/build metadata).
No Go, generated, fixture, platform, or Bazel file changed.

The Rust owners were inventoried before editing: `tidb-session` has 222
tracked files and `tidb-exec` has 176, including every production source,
inline and standalone test, generated test harness input, fixture, platform
variant, Cargo/build artifact, and package metadata. The changed Rust files
are `tidb-exec/src/hint_updatable_vars.rs`,
`tidb-exec/tests/hint_updatable_vars_source.rs`, and
`tidb-session/src/tests_session_var_hooks.rs`.

## Alignment

Go's `setVarHintChecker` (`pkg/planner/optimize.go:985-999`) emits warning
3637 only when the resolved `SysVar` has
`IsHintUpdatableVerified == false`. The ordinary source registry in
`pkg/sessionctx/variable/setvar_affect.go` contains 128 names, but
`group_concat_max_len` is additionally marked directly on its SysVar at
`pkg/sessionctx/variable/sysvar.go:2142-2147`.

Rust porting had copied the 128-name source registry only. Its checker thus
appended a 3637 warning for each `group_concat_max_len` hint before the hint
parser appended Go's single 3126 duplicate-conflict warning. A duplicate
hint consequently reported three warnings and `@@warning_count = 3`, while
Go reports only the conflict and count 1.

The Rust registry now keeps the exact 128-name source list and adds a
separate direct-SysVar declaration list for `group_concat_max_len`; the
effective lookup checks both. This preserves source provenance while matching
Go's runtime flag. Focused regressions cover both the effective registry
membership and the session-visible `SHOW WARNINGS`/`@@warning_count` result.

## Validation

Focused validation:

- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-exec --test all hint_updatable_vars_source -- --nocapture`
- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_session_var_hooks:: -- --nocapture --test-threads=1`

All focused tests passed (two registry tests and 23 session hook tests).

Ready validation for this batch:

- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`
- `git diff --check`
- `cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --all-targets`
- `GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex-tmp make lint`

## Risks and boundaries

Only the effective SET_VAR warning predicate changes. The parser still keeps
the first value and emits one 3126 conflict for duplicate names; validation,
overlay restoration, and all other warning classes are unchanged. No Go
source, generated output, fixture, platform variant, or build artifact was
modified.
