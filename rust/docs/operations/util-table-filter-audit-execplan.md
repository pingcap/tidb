# `pkg/util/table-filter` parity audit ExecPlan

This living ExecPlan records the complete Go-package audit and the
`ColumnFilterRules` API restoration. The repository-wide rolling audit
continues after this package.

## Purpose / Big Picture

Keep the Go table/column filter API aligned with the Rust `tidb-util`
`table_filter` owner. Current Go `master` exports the parsed column-rule list
and a concrete parser while retaining the interface-returning compatibility
entry point; callers need both forms without duplicate filtering policy.

## Progress

- [x] (2026-09-02) Read and inventoried all ten package artifacts at
      `origin/master` `c6054025ed4c32ab3672a2a24ea46892714d21ec`: five
      production files, four test files, `README.md`, and `BUILD.bazel`
      (2,180 lines). No package docs, generated/platform
      variants, fixtures, benchmarks, examples, or additional harnesses exist.
- [x] (2026-09-02) Compared every artifact with the hparser branch; the only
      Go delta is exporting `ColumnFilterRules`, adding
      `ParseColumnFilterRules`, and delegating `ParseColumnFilter`.
- [x] (2026-09-02) Restored the source-shaped Go API and added a focused
      regression covering concrete-rule matching plus compatibility behavior.
- [x] (2026-09-02) Demonstrated the new regression failed before the fix in a
      detached worktree (`ParseColumnFilterRules undefined`), then passed in
      focused and full package tests.
- [ ] Push this batch to `origin/hparser-integration`, verify local/remote
      SHAs, and fetch the newest target branch before the next package.

## Surprises & Discoveries

- The Rust owner had already exposed the concrete rule-list API and its
  source-derived test. The hparser Go branch lagged current `master`, so this
  fix belongs in Go and does not require speculative Rust changes.
- The package has no production failpoints; ordinary targeted Go test commands
  are sufficient. A new top-level test requires the Bazel preparation gate.

## Decision Log

- Decision: retain `ParseColumnFilter` as a delegating interface-returning
  compatibility function and expose `ParseColumnFilterRules` as the concrete
  return type, exactly as current Go `master` does. Rationale: callers that
  only need `ColumnFilter` remain source-compatible while callers needing the
  concrete rules type can use the exported API. Date: 2026-09-02, Codex.

## Validation

Run from the repository root:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex go test ./pkg/util/table-filter -run '^TestParseColumnFilterRules$' -count=1
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex go test ./pkg/util/table-filter -count=1
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint
    git diff --check
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make bazel_prepare

Expected results are passing focused/full Go tests, passing lint, and clean
diff hygiene. `make bazel_prepare` is required because a new top-level Go test
was added; it is blocked locally because the `bazel` executable is unavailable.

## Risks

- Correctness: low; parsing and matching logic is unchanged, and both API forms
  exercise the same reversed-rule list.
- Compatibility: additive exported Go type/function; the old interface API
  retains its exact signature and behavior.
- Performance: one delegating call; no matching or allocation policy changes.

## Outcomes & Retrospective

The Go table-filter package now exposes the same concrete parser API as current
`master`, while existing interface consumers remain source-compatible. Rust
regexp semantics and owner tests are unchanged and remain covered by the
existing receipt.
