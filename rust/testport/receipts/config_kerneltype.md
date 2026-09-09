# `pkg/config/kerneltype` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly six artifacts, all read in full:

- `doc.go` — Classic and NextGen architecture and binary compatibility
  contract;
- `classic.go` — Classic build-tag implementation of `IsNextGen` and
  `IsClassic`;
- `nextgen.go` — NextGen build-tag implementation of the same predicates;
- `type.go` — kernel names and PD compatibility matching;
- `type_test.go` — `TestKernelType` and `TestIsMatch`;
- `BUILD.bazel` — one library target containing both build variants and one
  two-shard test target.

There is no ownership file, generated source, fixture, benchmark, or additional
test harness in this package. The checkout is byte-identical to the pin.

## Rust ownership and audit result

`rust/crates/tidb-config/src/kerneltype.rs` owns the complete package behavior.
The Cargo `nextgen` feature is the compile-time counterpart of Go's `nextgen`
build tag; default and feature builds select the same boolean predicates,
kernel names, empty-old-PD handling, exact matching, and unknown-value
rejection. Its two owner tests cover every assertion from the two Go tests in
both build selections.

The deleted `tests/kerneltype_source.rs` duplicated those assertions and added
a second test carrier without a Go counterpart. The module documentation now
preserves the source package's architectural and binary-mixing contract.

## Validation

Profile: WIP; this is one completed package within the continuing repository
audit, not a repository-wide readiness claim.

- `go test ./pkg/config/kerneltype` — passed in the default Classic build.
- `go test -tags nextgen ./pkg/config/kerneltype` — passed in the NextGen
  build.
- `cargo test -p tidb-config --locked kerneltype` — passed in the default
  Classic build.
- `cargo test -p tidb-config --features nextgen --locked kerneltype` — passed
  in the NextGen build.
- `cargo fmt --all -- --check` and `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: unchanged; both build selections already matched Go.
- Compatibility: no production API changed; only a duplicate external test was
  removed.
- Performance: unchanged; all predicates remain compile-time constants or
  constant-string comparisons.
