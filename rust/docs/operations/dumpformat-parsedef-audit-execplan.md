# `pkg/dumpformat/parsedef` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Objective

Inventory the complete Go-master `pkg/dumpformat/parsedef` package, map its
row/logging contract to Rust owners, and avoid creating an uncalled Rust-only
facade when the importer and zap dependencies are not dependency-closed.

## Progress

- [x] (2026-09-01) Fetch and pin Go `origin/master` at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`.
- [x] (2026-09-01) Read both package artifacts (50 lines): BUILD metadata and
  the complete `Row`/`MarshalLogArray` implementation. Confirm no tests,
  fixtures, generated/platform variants, fuzz/benchmark inputs, or generator
  sources exist.
- [x] (2026-09-01) Compare the row carrier with Rust result rows and logging
  owners; no dependency-closed equivalent exists.
- [x] (2026-09-01) Run the pinned Go compile/test command and diff hygiene
  check.
- [ ] Fetch immediately before staging, create one meaningful receipt batch,
  push it to `origin/hparser-integration`, and verify remote SHAs.
- [ ] Continue the rolling audit with the next unrecorded dump-format package.

## Scope and decision

This package is a two-artifact public support library. `Row` is shared by
Lightning parser consumers and its only behavior is zapcore array encoding.
Rust's execution row and logging implementations do not expose those Go
consumer contracts. Keep the boundary explicit until a package-atomic
dump-format/importer owner is available; do not add a detached compatibility
type.

## Validation gate

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/dumpformat/parsedef -count=1
git diff --check
```

The Go command passes with no package-local tests. No Rust or Bazel source was
changed, and the broader Ready workspace gates are not applicable to this
documentation-only boundary.

## Outcome

The inventory and explicit owner decision are recorded in
`rust/testport/receipts/dumpformat_parsedef.md`; the rolling audit continues
with `pkg/dumpformat/testutils` and `pkg/dumpformat/parquetfile`.
