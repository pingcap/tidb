# `pkg/types` numeric-to-SET conversion parity receipt

Status: completed Rust-only alignment for the Go `convertToMysqlSet` owner.
The Go authority is `origin/master` at `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`;
the part-3 `pkg/ddl/db_integration_test.go` inventory and neighboring
divergences remain recorded in `receipts/b102.md`.

Go's `pkg/types/datum.go:1781-1795` leaves the zero `SET` beside a failed
`convertToUint` and wraps the failure as `ErrTruncated`. For `INSERT INTO
sett VALUES(-1)`, the failed negative-to-unsigned conversion therefore emits
`WarnDataTruncated` (1265), even though the non-strict value beside the error
is the empty SET. Rust previously discarded the conversion event when the
saturated value was zero, treating it as the valid zero SET and accepting the
row.

The Rust `tidb-datatype` conversion now treats any numeric conversion event or
failure as a failed SET parse, returning the empty `MysqlSet` with a
`ScalarConversionEvent::Truncated`; a genuinely converted numeric zero still
has no event and remains valid. String/bytes/enum/set parsing and the vector
hard-invalid conversion are unchanged.

Focused regressions:

- `tidb_datatype::datum_convert::go_tests::out_of_range_enum_and_set_keep_the_empty_value`
  includes the negative numeric SET case and verifies the zero value beside
  the truncation event.
- `tidb_executor::all::db_integration_ddl_types_source::issue_19229_enum_set_bad_values_truncate_1265`
  now asserts exact `DataTruncatedAtRow` behavior for `SET(-1)` alongside the
  existing ENUM and string cases.

No Go, generated, platform, Bazel, or module files changed. The adjacent
`ADD COLUMN IF NOT EXISTS` and generated-column ordering differences remain
explicitly documented in `receipts/b102.md`.
