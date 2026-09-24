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
renumbering has not been synchronized. `make lint` runs this guard through
`rust_proto_check`; the Rust source test also anchors `RegexpLikeSig` to its
TiPB wire number.

`check-tipb-proto-projection.py` also compiles the pinned upstream schemas and
the checked-in TiPB projections (`select.proto`, `analyze.proto`, and
`resourcetag.proto`) to protobuf descriptors. It checks every locally
projected message field's name, number, type, cardinality, and oneof membership,
and every projected enum value. Enums must match the full upstream value set
unless the checker explicitly records them as partial projections (`ExecType`
and `ExprType`). This keeps intentionally omitted message fields and enum
values as reviewed projection choices while preventing local declarations
from silently drifting from the pinned wire contract.

The `RegexpLikeSig` omission came from treating the checked-in, dependency-
closed `select.proto` projection as if `protoc` validated upstream parity.
`protoc` only validated that this local schema compiled. The projection's
hand-maintained `ScalarFuncSig` subset predated Go's regexp-like pushdown use,
and its source comment did not pin the TiPB version or identify
`expression.proto` as the enum owner. Rust therefore had no generated enum
variant even though Go TiPB assigned wire value 4313. The repair keeps the
upstream enum as the source of truth and makes drift a lint failure, instead
of relying on reviewers to remember to copy new values.

Other messages remain dependency-closed projections rather than generated
copies of whole upstream files. When editing one, record its exact upstream
module version and proto path in the source comment; check every projected
field's number, scalar/message type, repeatedness, oneof membership, and
presence semantics against that source. Successful local code generation is
only a syntax/build check, not an upstream parity check.
