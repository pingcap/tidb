# Masking-policy restrict-operations width divergence

This is a divergence-driven unit, not a lockdown of the 5,398-line
`pkg/parser/ast/ddl.go`. The surface was previously unowned and was discovered
while closing the concrete `pkg/meta/model/masking_policy.go` dependency of the
`pkg/meta/meta.go` lockdown.

## Go authority

- Owner: `pkg/parser/ast/ddl.go`
- Pinned SHA-256:
  `9964cbd22f136969bb2f67563190e521f1f57c26ec18ee0f5af384a19ad7fec1`
- Pinned size: 143,820 bytes, 5,398 lines
- Exact declaration: `type MaskingPolicyRestrictOps uint64` at line 1,790

The prior Rust type used `u8`. That was not merely a storage optimization: the
type is embedded in `model.MaskingPolicyInfo` and Go `encoding/json` preserves
the complete unsigned 64-bit value. A direct Go probe marshalled and
unmarshalled the boundary values `0`, `1<<7`, `1<<8`, `1<<31`, `1<<63`, and
`math.MaxUint64`; every value round-tripped as its decimal JSON number. The
previous Rust representation could not represent four of those six values.

## Port and boundary gate

`tidb_ast::MaskingPolicyRestrictOps` now stores `u64`, serializes transparently
as the Go number, and exposes `from_bits`/`bits` so downstream metadata can
preserve unknown future bits. Known SQL names still restore in Go declaration
order. An unknown nonzero bit remains nonempty and restores as
`RESTRICT ON ()`, matching Go's interaction between the nonzero check and its
known-name list.

The boundary test is:

    masking::source_width_tests::restrict_ops_preserve_the_full_go_uint64_domain

It covers the same six direct-Go values, JSON round trips, and the unknown-bit
restore boundary. The mutation probe must narrow or truncate the backing value
and must be killed by this test before this unit is returned.

