# `pkg/types` current-master explain-format delta

## Inventory

The complete Go `pkg/types` tree at `origin/master` contains 61 tracked
artifacts: 56 root-package files and five `parser_driver` files. The root
inventory is:

```text
BUILD.bazel benchmark_test.go binary_literal.go binary_literal_test.go
compare.go compare_test.go const_test.go context.go context_test.go convert.go
convert_test.go core_time.go core_time_test.go datum.go datum_eval.go
datum_test.go enum.go enum_test.go errors.go errors_test.go etc.go etc_test.go
eval_type.go explain_format.go export_test.go field_name.go field_type.go
field_type_builder.go field_type_test.go format_test.go fsp.go fsp_test.go
helper.go helper_test.go json_binary.go json_binary_functions.go
json_binary_functions_test.go json_binary_test.go json_constants.go
json_path_expr.go json_path_expr_test.go main_test.go mydecimal.go
mydecimal_benchmark_test.go mydecimal_test.go overflow.go overflow_test.go
set.go set_test.go string.go time.go time_test.go truncate.go vector.go
vector_functions.go vector_test.go
```

The nested `pkg/types/parser_driver` inventory is:

```text
BUILD.bazel accept_in_place_test.go main_test.go value_expr.go value_expr_test.go
```

There are no generated, platform-specific, fixture, or external data files in
this package tree. The existing b034–b037 receipts cover the complete source
test inventory (206 `Test*` functions) and the earlier vector overflow fix.
This audit rechecked the current `origin/master` delta in
`explain_format.go`, `vector.go`, and the parser-driver files before editing.

## Current-master behavior and owner decision

Go master adds the public `ExplainFormatRU = "ru"` literal and appends it to
the validator's `ExplainFormats` slice. Rust's `tidb-datatype::explain_format`
is the dependency-closed owner, so it now exports the constant, grows the
ordered validator array from 13 to 14 entries, and has a focused order/value
regression.

Go master also changes `PeekBytesAsVectorFloat32` to use checked `uint64`
size arithmetic. The Rust vector owner already has that checked arithmetic and
the source-derived overflow regression; no duplicate change was made.

The new parser-driver `AcceptInPlace` methods are API-shaped visitor hooks on
Go driver nodes. Rust represents these expressions as `tidb-ast::Expr` enum
variants and has no dependency-closed driver-node owner or caller that could
accept an invented parallel API. They remain an explicit boundary rather than
a Rust-only carrier.

## Validation

Profile: Ready for this bounded production change.

- `cargo +nightly-2026-08-22 test --offline --locked -p tidb-datatype --lib explain_format -- --test-threads=1` — passed (the focused owner test; existing workspace warnings only).
- `rustfmt +nightly-2026-08-22 --edition 2021 --check crates/tidb-datatype/src/explain_format.rs crates/tidb-datatype/src/lib.rs` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed.
- `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risks and not verified

- Correctness: the RU literal and ordered-list position now match Go; no
  behavior was invented for parser-driver traversal.
- Compatibility: this adds one exported Rust constant and one validator entry;
  consumers that enumerate the array will observe the new Go-compatible value.
- Performance: the validator list remains a static array; no hot-path policy
  changed.
- The full Go `pkg/types` test suite and parser-driver API tests were not run;
  existing workspace host-toolchain blockers and the lack of a Rust driver
  owner keep those surfaces outside this focused validation.
