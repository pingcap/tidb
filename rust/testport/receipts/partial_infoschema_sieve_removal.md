# Partial `pkg/infoschema/sieve.go` port removal

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Ownership audit

The removed `rust/crates/tidb-util/src/sieve.rs` explicitly represented only
`pkg/infoschema/sieve.go` and `sieve_test.go`, not the complete
`pkg/infoschema` package. Both pinned Go files and the complete Rust file were
read before deletion. Repository-wide search found no production or test
consumer of the Rust API; the only outside reference was explanatory text in
the separate binding-cache module.

The partial port also documented behavior differences from Go: custom
`GoSized` layout declarations in place of `internal.Sizeof`, removal of the
context/cancel lifecycle, omission of the `skipGet` failpoint, a custom
generational slab in place of `container/list`, recovery from Go's stale hand
after purge, and omission of the real metrics hook. Keeping those narrowings
as a public `tidb-util` feature cannot satisfy the repository's rule that one
complete Go package is the minimum transcreation unit.

The audit deleted the partial production implementation, its six tests, and
its public module export. The stale binding-cache documentation reference was
removed. This is not a claim that `pkg/infoschema` is transcreated; that
package requires one complete inventory and implementation decision as a
whole.

## Validation

Profile: WIP; this is a parity cleanup within the continuing package audit,
not a repository-wide readiness claim.

- `cargo check -p tidb-util -p tidb-session --locked` — passed.
- `cargo fmt --all --check` and `git diff --check` — passed.
- `cargo test -p tidb-util --locked` — the removed SIEVE tests no longer
  exist; 630 tests passed, 3 helpers were ignored, and the unrelated
  `logutil::tests::zap_logger_with_keys` test failed after capturing a
  concurrent SEM-v2 global log line.
- `cargo test -p tidb-util logutil::tests::zap_logger_with_keys --lib --locked` — passed in isolation.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: no runtime path used the module; the owner crates compile
  without it.
- Compatibility: the public but repository-unused partial SIEVE API is
  intentionally removed.
- Performance: no runtime impact because there was no consumer.
