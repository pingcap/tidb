# `pkg/parser/opcode` parity audit ExecPlan

## Objective

Inventory the complete operator package, remove the Rust-only stale opcode,
align Go source with Go master, and verify the shared Rust AST owner.

## Completed

- Read all three pinned artifacts (310 lines, four production declarations,
  and one test).
- Removed `Binary` and its metadata row from Go and Rust authorities.
- Added a focused opcode-count/source-table regression and captured the
  expected pre-fix failure in both Go and Rust.
- Confirmed remaining `BINARY` identifiers are distinct charset/cast/
  weight-string/expression concepts.

## Validation gate

- [x] Before-fix Go and Rust regressions fail on the stale count.
- [x] Focused Go and Rust opcode suites pass after the fix.
- [x] Ready Rust formatting, repository lint, and diff checks pass.
- [ ] `make bazel_prepare` — attempted, blocked because `bazel` is unavailable.
- [ ] Push the batch to `origin/hparser-integration`, verify remote SHAs, and
      pull the explicit branch ref.

## Remaining boundary

Any future operator additions/removals must update the Go table, Rust `Op`,
expression adapters, and source-derived table tests as one package unit.
