# Encoding parity record

This record covers the complete direct-file inventories of these Go packages at
commit `e2788410d8d696605e8cb002585877a063ccc909`:

| Go package | Rust implementation | Current findings |
| --- | --- | --- |
| `pkg/util/codec` | `tidb-codec` plus its `tidb-datatype` decimal/collation dependencies | 0 |
| root `pkg/tablecodec` | `tidb-tablecodec` plus shared key/row codecs | 0 |

`pkg/tablecodec/rowindexcodec` is a separate Go package and has its own package
entry in `rust/testport/TESTPORT_EXECPLAN.md`.

## Pinned source inventories

`pkg/util/codec`:

- `BUILD.bazel`
- `bench_test.go`
- `bytes.go`
- `bytes_test.go`
- `codec.go`
- `codec_test.go`
- `collation_test.go`
- `decimal.go`
- `decimal_test.go`
- `float.go`
- `main_test.go`
- `number.go`

Root `pkg/tablecodec`:

- `BUILD.bazel`
- `OWNERS`
- `bench_test.go`
- `main_test.go`
- `tablecodec.go`
- `tablecodec_test.go`

## Verified behavior

The package audit includes exact source behavior for:

- comparable and compact bytes, signed/unsigned integers, floats, decimals,
  JSON, vectors, sentinels, `CutOne`, and typed `DecodeRange`;
- process-wide collation mode, fixed encoder collation mode, group/row/column
  hash keys, equality keys, and serialized join keys;
- UTC timestamp conversion guards and malformed temporal behavior;
- decimal declared shapes, signed invalid shapes and partial header writes,
  scale clamping, value-size estimation, negative zero decoded from physical
  bytes, low-81-digit overflow retention, and malformed-input panic points;
- table/index/record key layout, memcomparable `_bin` prefixes, common-handle
  padding, restored index values, partition handles, nullable handles, prefix
  truncation, and row unflattening.

Resolved findings were removed instead of retained as supported deviations.
The package receipts contain the exact validation commands and results. This
record does not claim parity for other Go packages that share either Rust crate.
