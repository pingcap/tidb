# `pkg/util/keydecoder` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The pinned package contains `BUILD.bazel`, production `keydecoder.go`, source
test `keydecoder_test.go`, and package harness `main_test.go`. It has one
top-level source test and no `doc.go`, fixture, generated source, benchmark,
fuzz target, example, platform variant, or build-tagged production variant.
The checkout package is byte-identical to the pin.

`main_test.go` supplies Go's common test setup and goroutine leak checker;
Rust's aggregate test does not start Go runtime workers and needs no package
setup analogue. `BUILD.bazel` maps to the existing `tidb-executor` production
owner, `tidb-exec` catalog adapter, and generated aggregate test target.

## Rust ownership and integration

`tidb-executor::keydecoder` owns the wire decoder, handle type, decoded JSON
shape, partial-result error, and the narrow information-schema catalog view.
`tidb-exec::keydecoder` adapts the persisted cluster catalog, while the
ordinary executor catalog supplies the in-memory adapter used by sessions.
Logical-table lookup precedes physical-partition lookup, schema churn may
return the table name/ID without decoding the payload, missing tables retain
the physical ID, malformed record payloads return the populated partial value
with an error, and malformed index payloads return the populated partial value
without an error, matching Go.

The decoder retains integer/common handle spelling, partition metadata,
index names and values, and Go's JSON names and omission rules. A successfully
decoded existing index now stores `Some(Vec::new())` for an empty value stream,
matching Go's non-nil empty slice; JSON still omits that empty slice under
`omitempty`.

The source package's consumers are ordinary information-schema readers.
Rust's `INFORMATION_SCHEMA.DEADLOCKS` and `DATA_LOCK_WAITS` now both call this
decoder and serialize `DecodedKey`. The latter previously passed a raw lock
key through the unrelated `TIDB_DECODE_KEY` hexadecimal builtin decoder; that
non-Go path was removed. Invalid keys produce SQL NULL in those readers after
the decoder error, as Go does.

The Rust test surface is now one `test_decode_key`, covering exactly the
source test's integer handle, common handle, index, record partition, index
partition, wholly invalid key, shallow-invalid table key, and missing-table
record cases. Four supplemental test identities and their additional malformed
payload/schema-churn assertions were removed.

## Validation

Profile: WIP; this is a complete package checkpoint inside the continuing
package-by-package parity audit, not repository-wide readiness.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/keydecoder` — passed.
- `GOTOOLCHAIN=go1.25.10 go test ./pkg/util/keydecoder -count=1` — passed.
- `cargo check -p tidb-executor -p tidb-exec -p tidb-session` — passed.
- `cargo test -p tidb-exec --test all keydecoder_source` — passed; one test.
- `cargo test -p tidb-session --lib data_lock_waits_reads_the_storage_provider_with_go_privilege_and_encoding` — passed.
- `cargo fmt -p tidb-executor -p tidb-exec -p tidb-session` — passed.
- `git diff --check` — passed.

The first Go test attempt was sandbox-blocked from the host Go build cache; the
same command passed with that cache access allowed. No Go source, Go test,
Bazel metadata, or Go module file changed, so `make bazel_prepare` is not
required.

## Risk

- Correctness: improved; empty index values retain Go's slice state and both
  lock-diagnostic tables use the package decoder intended for their KEY_INFO
  column.
- Compatibility: decoded fields, JSON omission, error/partial-result behavior,
  schema churn, and missing metadata follow the pinned Go package.
- Performance: decoding remains one shallow key classification, one catalog
  lookup, and one payload decode per lock key; `DATA_LOCK_WAITS` borrows the
  catalog once for the complete row batch.
- Not verified locally: a live TiKV lock wait against a concurrently changing
  schema. The source test and the ordinary session/provider regression cover
  all pinned package cases and the production consumer boundary.
