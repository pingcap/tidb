# `pkg/table/tblctx` parity receipt

Status: Audited; no dependency-closed Rust implementation was added. This
receipt covers the complete Go package and its current boundary; it is not a
repository-wide parity claim.

Comparison source: Go `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec`.
Rust comparison branch: `origin/hparser-integration` at the current audit
tip.

## Complete Go inventory

All four tracked artifacts in `pkg/table/tblctx` were read in full before
editing: 654 lines total. There is no package `doc.go`, fixture or `testdata`
directory, generated source or input, platform/build-tag variant, benchmark,
fuzz target, README, or ownership artifact.

| artifact | lines | role |
| --- | ---: | --- |
| `BUILD.bazel` | 51 | Go library/test target and codec/table dependencies |
| `buffers.go` | 186 | reusable row/check buffers and row/binlog encoding |
| `buffers_test.go` | 267 | encoding, reservation, buffer reuse, and capacity tests |
| `table.go` | 150 | mutation, allocator, statistics, cache, temporary-table, and exchange interfaces |

## Branch delta and behavior finding

The current branch threads an explicit `codec.Encoder` through
`WriteMemBufferEncoded`, `EncodeBinlogRowData`, and `tablecodec.EncodeRow` /
`EncodeOldRow`; the corresponding codec/tablecodec callsites and BUILD
dependencies are present in the branch. This is an API adaptation for the
existing Rust codec owner, not a separate Rust-only result: the encoder's
value path still uses raw string bytes (`comparable=false`), and the source
tests retain byte-for-byte row/binlog assertions. No missing Go behavior or
safe local fix was identified in this package, so the branch delta remains.

## Rust ownership and boundary decision

The former standalone Rust `tblctx` seed was deleted as an unwired partial
carrier. The `tidb-tablecodec` owner supplies row/value encoding, while the
executor `b151` source inventory records the five `tblctx` tests as
`go-parity-carrier`; it does not implement the live Go mutation context.

Faithful ownership requires session write buffers, rowcodec/tablecodec,
transaction mem-buffer flags, error-context handling, table DML, and the
temporary/exchange/statistics interfaces. Adding a local Rust buffer/context
would duplicate an uncalled API and create Rust-only behavior. Go remains the
authoritative implementation until those consumers are integrated.

## Validation

Profile: **Ready** for this no-code boundary audit.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test -tags=intest ./pkg/table/tblctx -count=1` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — repository Ready gate (run after this docs batch is staged).
- `git diff --check` — passed after this batch is staged.

No Go/Bazel artifact changed in this audit batch, so `make bazel_prepare` was
not applicable. No Rust source changed; the existing codec/tablecodec owner
checks are covered by their published receipts.

## Risks and unverified scope

- Correctness risk is unchanged: buffer reuse and row encoding remain in Go,
  with the current explicit encoder path covered by source tests.
- Compatibility risk is limited to the documented API adaptation; no public
  behavior was changed here.
- Performance is unchanged by this audit.
- Not verified locally: Rust live DML/session integration, transaction
  mem-buffer flags across all table paths, non-host platforms, and broad
  integration suites.

The rolling repository audit continues with the next unclaimed package.
