# `pkg/types` vs `tidb-datatype`: semantic divergence audit

Status: IN PROGRESS (skeleton committed first; findings appended as they land).

Scope: file-by-file semantic comparison of Go `pkg/types` against
`rust/crates/tidb-datatype`. Every finding must carry a Go file:line, a Rust
file:line, and a concrete distinguishing input.

**Execution constraint on this machine**: nothing can be run. `syspolicyd` is
wedged; freshly created executables hang at `_dyld_start`. `cargo check` and
`cargo clippy` work; `cargo test`, `gorun`, `goeval` and Go test binaries do
not. Every claim below is derived by reading source on both sides. No finding
here has been confirmed by execution.

## File map

| Go | Rust |
| --- | --- |
| `datum.go` | `datum/mod.rs`, `datum/compare.rs`, `datum/convert.rs`, `datum/stringify.rs` |
| `compare.go` | `compare.rs` |
| `convert.go` | `convert.rs`, `datum_convert.rs` |
| `mydecimal.go` | `mydecimal.rs`, `decimal.rs` |
| `time.go`, `core_time.go`, `fsp.go` | `mysql_time.rs`, `core_time.rs`, `time_parse.rs`, `duration.rs`, `fsp.rs`, `packed_time.rs`, `str_to_date.rs` |
| `field_type.go`, `field_type_builder.go` | `field_type/mod.rs`, `field_type/aggregate.rs`, `field_type/builder.rs` |
| `set.go`, `enum.go` | `enum_set.rs` |
| `binary_literal.go` | `binary_literal.rs` |
| `overflow.go` | `overflow.rs` |
| `etc.go`, `helper.go` | `etc.rs`, `numeric_helper.rs` |
| `json_*.go` | `binary_json.rs`, `binary_json_ops.rs`, `json_path.rs` |
| `vector.go`, `vector_functions.go` | `vector.rs` |
| `context.go` | `conversion_context/mod.rs`, `conversion_context/flags.rs` |

## Ranked divergences

(To be filled.)

## Verified-equal inventory

(To be filled.)
