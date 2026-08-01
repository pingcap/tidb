# JSON surface: `pkg/types/json_*.go` vs `tidb-datatype` binary JSON

Scope: TiDB's binary JSON format and its operations, audited against the Go
sources that define them.

Go sources read:

- `pkg/types/json_constants.go` (type codes, layout constants, precedence table)
- `pkg/types/json_binary.go` (grammar comment, encode/decode, text form)
- `pkg/types/json_binary_functions.go` (comparison, path ops, modify, merge)
- `pkg/types/json_path_expr.go` (path parsing)

Rust sources read:

- `rust/crates/tidb-datatype/src/binary_json.rs`
- `rust/crates/tidb-datatype/src/binary_json_ops.rs`
- `rust/crates/tidb-datatype/src/json_path.rs`
- `rust/crates/tidb-codec/src/json.rs`

**Nothing in this document was executed.** This machine cannot run freshly
built binaries (`syspolicyd` wedged; every new executable hangs at
`_dyld_start`). Every finding is derived by reading both sides. `cargo check`
and `cargo clippy` are the only gates that ran.

Status: the JSON surface is **substantially implemented**, not a stub —
~5000 Rust lines against ~3300 Go lines of production code. The findings below
are real divergences inside a working implementation.

---

## Ranked findings

(filled in below)
