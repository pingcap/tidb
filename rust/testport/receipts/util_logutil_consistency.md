# `pkg/util/logutil/consistency` — current Go-master boundary receipt

Go source: `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). This is a separate
Go package under the top-level `pkg/util/logutil` directory and is recorded as
its own package unit.

## Complete inventory

Both package artifacts were read in full before deciding ownership:

| Artifact | Lines | Blob | SHA-256 | Inventory |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 23 | `3e0c7594177494f8ae08b59ec9ffefc936ed3ec0` | `88df90aecf53f2183a09ac7f92c911b82765834b701dea306b936ebd99788a85` | public library target and helper/tablecodec/TiKV/protobuf/redaction dependencies |
| `reporter.go` | 313 | `8fe201c938333564bc9d340e281aae998afd45a6` | `fb142692a267397bbb5be6f15cff4624a743dfdbcf2bb3181e69d0f54668b9f0` | MVCC fetch/region lookup, row/index MVCC decoding, `RecordData`, and lookup/admin inconsistency reports |

The package has 336 Go lines, no tests, fixtures, generated or platform
variants, benchmark, fuzz target, or nested package. Its exported behavior is
the error/reporting contract for `ADMIN CHECK` and index-lookup inconsistencies:
`GetMVCCByKeyResp`, `GetMvccByKey`, `DecodeRowMvccData`,
`DecodeIndexMvccData`, the three `Reporter` methods, and `RecordData.String`.

## Rust ownership and decision

Rust has lower-level pieces in `tidb-executor::admin_check` for checking stored
row/index relationships and rendering `RecordData`-like error details, but no
dependency-closed owner for this package's helper-storage MVCC RPC calls,
region lookup, row/index value decoders, redaction, and zap reporter. The
existing checker is a consumer-specific execution path, not a drop-in
replacement for the Go reporter package.

No Rust-only behavior was found that can be removed safely, and no missing Go
behavior can be implemented without first moving the complete helper storage,
MVCC protobuf, tablecodec, metadata, and reporting stack. This package remains
explicitly unclaimed rather than receiving a speculative logger/reporting API.

## Validation

Profile: Ready package re-audit; no source changed. The parent Go command
`go test -tags=intest,deadlock -count=1 ./pkg/util/logutil/...` passed and
reported this package as `[no test files]`. No failpoint lifecycle applies.

`make bazel_prepare` is not required because no Go/Bazel artifact changed.

## Risks and unverified scope

- Correctness: MVCC decoding and error text depend on TiKV wire responses,
  tablecodec versions, and redaction policy.
- Compatibility: the Rust admin checker intentionally covers statement
  execution, not the Go reporter's RPC/reporting API.
- Performance: no production path changed.
- Not verified locally: live TiKV helper RPCs, corruption/error responses,
  and every executor caller that consumes the Go reporter.
