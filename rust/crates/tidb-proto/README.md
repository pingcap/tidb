# TiDB Rust protocol sources

`proto/` contains checked-in protobuf inputs compiled by `build.rs`. They are
dependency-closed projections used to keep Rust builds reproducible and limit
generated APIs to the protocol surface TiDB Rust consumes. `protoc` validates
wire-schema syntax and references; it does not check that a projection still
matches Go TiDB's pinned upstream source.

Go TiDB pins TiPB in the repository root `go.mod`. The full
`tipb.ScalarFuncSig` enum is copied from that pin's `proto/expression.proto`
into `proto/select.proto`. Keep it complete: runtime code can start using an
upstream signature after any expression change, and a partial local enum can
silently block pushdown. After changing the TiPB pin, refresh it with:

```sh
python3 rust/scripts/sync-tipb-scalar-func-sig.py --write
python3 rust/scripts/sync-tipb-scalar-func-sig.py
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-proto --test tipb_selection_expression_source
```

The no-argument script is the drift check. It compares the checked-in enum to
the selected Go module source and fails when an upstream addition, removal, or
renumbering has not been synchronized. The Rust source test also anchors
`RegexpLikeSig` to its TiPB wire number. For other projected messages and
fields, keep the upstream module/version and original proto file in the source
comment, and preserve field numbers, scalar types, and cardinality exactly;
do not treat successful Rust code generation as evidence of upstream parity.
