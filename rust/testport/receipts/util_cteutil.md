# `pkg/util/cteutil` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The pinned package contains production `storage.go`, source tests
`storage_test.go`, package harness `main_test.go`, and `BUILD.bazel`. It has six
top-level source tests and no `doc.go`, fixture, generated source, benchmark,
fuzz target, example, platform variant, or build-tagged production variant.
The checkout package is byte-identical to the pin.

`main_test.go` supplies Go's common test setup and goroutine leak checker; the
Rust storage test starts no background runtime and needs no harness analogue.
The package-local Go test maps to the standalone `cteutil_source` Cargo target.

## Rust ownership and integration

`tidb-executor::cte_storage::CteStorage` owns the package behavior. Construction
now leaves storage closed; `open_and_ref` creates the row container on the
first reference, `deref_and_close` closes it at zero, and close/reopen reset the
done, error, and iteration state with Go's ordering and error behavior. Add and
read operations validate the open state, spill accounting stays attached to
the statement trackers, and `swap_data` exchanges only schema, chunk size, and
row-container data while retaining reference and producer state.

The ordinary physical-plan builder explicitly opens the shared result and
recursive-input storages, and the recursive producer opens and closes its
iteration-output storage. These are the Rust owners corresponding to Go's
`executorBuilder.buildCTE` and CTE executor lifecycle. The former catalog-backed
`CteTable`, its `TableEntry::Cte` variant, and its DML, DDL, SHOW, and source-table
branches were removed: no pinned Go catalog relation exists, and the live Rust
physical CTE path did not construct that variant. Unused row-matrix convenience
methods were also removed in favor of Go's chunk-oriented storage boundary.

The standalone Rust test has exactly the six Go identities and cases:
`TestStorageBasic`, `TestOpenAndClose`, `TestAddAndGetChunk`, `TestSpillToDisk`,
`TestReopen`, and `TestSwapData`.

## Validation

Profile: WIP; this is a complete package checkpoint inside the continuing
package-by-package parity audit, not repository-wide readiness.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/cteutil` — passed.
- `GOTOOLCHAIN=go1.25.10 go test ./pkg/util/cteutil -count=1` — passed; six tests.
- `cargo check -p tidb-executor -p tidb-session` — passed.
- `cargo test -p tidb-executor --test cteutil_source` — passed; six tests.
- `cargo test -p tidb-session tests_recursive_cte::counter_iterates_over_the_previous_rounds_delta` — passed; ordinary recursive CTE execution.
- `cargo test -p tidb-session tests_recursive_cte::cte_storage_obeys_the_session_spill_policy_and_quota` — passed; statement spill and cleanup integration.
- `cargo fmt -p tidb-executor -p tidb-session` — passed.
- `git diff --check` — passed.

No Go source, Go test, Bazel metadata, or Go module file changed, so
`make bazel_prepare` is not required.

## Risk

- Correctness: explicit lifecycle state is newly enforced at every physical
  builder boundary; the source identities and live recursive consumers pass.
- Compatibility: construction, double-close errors, reference counting,
  reopen, swap, chunk access, and spill behavior follow the pinned package.
- Performance: the physical executor retains chunk-oriented storage and
  statement spill accounting; removing the unused catalog/materialization path
  removes dead allocation surfaces.
- Not verified locally: concurrent readers/producers. Rust's physical executor
  is single-threaded at this boundary, whereas Go exposes an external mutex for
  callers that share `StorageRC`; there is no representable concurrent Rust
  call site in this package owner.
