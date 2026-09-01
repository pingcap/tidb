# `pkg/parser/generate_keyword` parity audit ExecPlan

## Objective

Compare the complete Go-master keyword-generator package with the native Rust
generator and close any source-derived catalog or parser-input behavior gaps.

## Completed

- Read all four Go-master artifacts (211 lines), two production functions, one
  test entry point, and the Bazel targets.
- Added focused regressions for Go's CRLF handling and ASCII regexp boundaries;
  the CRLF test failed before the implementation change.
- Added the four Go-master unreserved keywords missing from Rust, updated the
  compile-time catalog count from 685 to 689, and added a focused presence/
  section test that failed before the catalog update.
- Verified the generated Rust catalog against Go master's `parser.y` with the
  native generator's `--check` mode and retained the native output mode as a
  tooling-only extension.

## Validation gate

- [x] Focused Rust catalog and generator tests pass.
- [x] Exact Go-master generator package test passes in a detached worktree.
- [x] Go-master `parser.y` catalog check passes.
- [x] Ready Rust formatting, repository lint, and diff checks pass.
- [ ] Push the batch, verify remote SHAs, and pull `origin/hparser-integration`.

## Next boundary

Audit `pkg/parser/goyacc` as the remaining standalone parser generator package;
root parser grammar/output migration remains an atomic dependency boundary.

