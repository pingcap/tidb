# `pkg/dumpformat/sqlfile` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Objective

Inventory every Go-master production, test, and build artifact in
`pkg/dumpformat/sqlfile`, compare the SQL dump writer's observable contract
with Rust owners, and keep the package-atomic boundary explicit.

## Progress

- [x] (2026-09-01) Fetch and pin Go `origin/master` at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`.
- [x] (2026-09-01) Read all four artifacts (318 lines): BUILD metadata, SQL
  value escaping, statement writer, and all four focused tests. Confirm no
  fixtures, generated/platform variants, fuzz/benchmark inputs, or generator
  sources.
- [x] (2026-09-01) Compare the writer with Rust `sqlescape`, parser syntax,
  and repository callers; no dependency-closed dump writer owner exists.
- [x] (2026-09-01) Run the exact detached Go-master package suite and diff
  hygiene check.
- [ ] Fetch immediately before staging, create one meaningful receipt batch,
  push it to `origin/hparser-integration`, and verify remote SHAs.
- [ ] Continue the rolling audit with the next unrecorded dump-format package.

## Scope and decision

This package's contract spans SQL literal escaping, row-kind dispatch,
statement-size splitting, separators, and logical byte accounting. The Rust
SQL argument escaper is a lower-level unrelated owner and cannot safely stand
in for the dump writer. Keep the boundary explicit; any future parity change
must move a real dump-format consumer and the four source regressions together.

## Validation gate

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/dumpformat/sqlfile -count=1
git diff --check
```

The detached Go suite passes. No Rust or Bazel source was changed, so broader
workspace Ready gates are not applicable to this documentation-only boundary.

## Outcome

The complete inventory and explicit owner decision are recorded in
`rust/testport/receipts/dumpformat_sqlfile.md`; the rolling audit continues
with the remaining dump-format writer/parser packages.
