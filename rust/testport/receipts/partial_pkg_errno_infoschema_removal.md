# Partial `pkg/errno/infoschema.go` port removal

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Ownership audit

Pinned Go `pkg/errno` has eight artifacts: `errcode.go`, `errname.go`,
`infoschema.go`, `errname_test.go`, `infoschema_test.go`, `main_test.go`,
`logredaction.md`, and `BUILD.bazel`. The removed
`rust/crates/tidb-util/src/errno_summary.rs` represented only
`infoschema.go` and `infoschema_test.go`, not the complete Go package. Both
relevant pinned Go files, the package build inventory, and the complete Rust
file were read before deletion.

Repository-wide search found no production or test consumer of the Rust API.
The partial module also added an isolated `Stats` constructor and four tests
with no Go equivalent, in addition to the one source-owned copy-safety test.
Keeping that public slice cannot satisfy the rule that a complete Go package
is the minimum transcreation unit.

The audit deleted the partial implementation, all five tests, and its public
module export. This is not a claim that `pkg/errno` is transcreated; a future
implementation requires one atomic inventory and integration decision for
the complete error-code/name/statistics package and its documentation.

## Validation

Profile: WIP; this is a parity cleanup within the continuing package audit,
not a repository-wide readiness claim.

- `cargo fmt --all --check` — passed.
- `cargo check -p tidb-util --locked` — passed.
- `cargo test -q -p tidb-util --locked -- --test-threads=1` — passed: 605
  unit tests passed, 3 were ignored, and every integration and doc test
  passed.
- `git diff --check` — passed.
- `rg -n "errno_summary|Stats::new\\(\\).*increment_error" rust/crates --glob '!target/**'` — returned no references.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: no runtime path used the module; the owner crate compiles
  without it.
- Compatibility: the public but repository-unused partial API is
  intentionally removed.
- Performance: no runtime impact because there was no consumer.
