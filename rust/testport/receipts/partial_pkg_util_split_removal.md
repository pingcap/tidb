# Partial `pkg/util/split.go` port removal

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Ownership audit

The removed `rust/crates/tidb-util/src/split.rs` represented only
`pkg/util/split.go` and `pkg/util/split_test.go`. Both pinned Go files and the
complete Rust file were read before deletion. These files belong to Go's
monolithic `pkg/util` package; they do not form a standalone `pkg/util/split`
package. Repository-wide search found no production or test consumer of the
Rust API.

The Rust file nevertheless described itself as a complete transcreation and
exported the partial surface publicly. It contained the two Go-owned tests
plus five supplemental tests with no Go equivalent. Keeping this unused
partial implementation cannot satisfy the repository rule that one complete
Go package is the minimum transcreation and completion unit.

The audit deleted the 242-line partial implementation, all seven tests, and
its public module export. This is not a claim that `pkg/util` is transcreated;
that package requires one complete inventory and integration decision as a
whole.

## Validation

Profile: WIP; this is a parity cleanup within the continuing package audit,
not a repository-wide readiness claim.

- `cargo fmt --all --check` — passed.
- `cargo check -p tidb-util --locked` — passed.
- `cargo test -p tidb-util --locked -- --test-threads=1` — passed: 623 unit
  tests passed, 3 were ignored, and all integration and doc tests passed.
- `git diff --check` — passed.
- `rg -n "tidb_util::split|pub mod split|get_values_list\\(" crates --glob '!target/**'` — returned no references.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: no runtime path used the module; the owner crate compiles and
  its complete serial test suite passes without it.
- Compatibility: the public but repository-unused partial API is
  intentionally removed.
- Performance: no runtime impact because there was no consumer.
