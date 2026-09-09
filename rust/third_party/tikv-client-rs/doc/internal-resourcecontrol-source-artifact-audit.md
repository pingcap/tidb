# `internal/resourcecontrol` source-artifact audit

This is the atomic completion receipt for client-go package `internal/resourcecontrol`, pinned at commit `52c1e76cec993571493c81de442bcbef90cdc106`. The Rust implementation is in `tikv-client` and is validated with `nightly-2026-08-22` in both legacy and `nextgen` modes.

## Complete source inventory

`git ls-tree -r --name-only 52c1e76cec993571493c81de442bcbef90cdc106 internal/resourcecontrol` contains exactly two files:

| Source artifact | SHA-256 | Rust owner |
| --- | --- | --- |
| `internal/resourcecontrol/resource_control.go` (326 lines) | `59cef72b939bd1f5bb809f868a5d428b0464f0ba2eaea510439be6f23ed2e281` | `src/resource_control.rs`, typed request/stream boundaries in `src/store/request.rs`, physical dispatch in `src/request/plan.rs` |
| `internal/resourcecontrol/resource_control_test.go` (185 lines) | `8a96f6d910e0d7918c5413aeaa35bb7fc446b02998df0358964f08a949076c54` | Source-derived tests in `src/resource_control.rs`, plus route and physical-dispatch integration tests in `src/store/mod.rs` and `src/transaction/transaction.rs` |

There is no `doc.go`, build-tag/platform variant, generated source/input owned by this package, fixture, package build file, goleak harness, benchmark, example, or metadata artifact. Imported kvproto and PD controller types are dependencies rather than package-generated artifacts; Rust consumes its existing generated protobuf messages and exposes a native controller trait without claiming the external PD controller implementation.

## Production mapping

| client-go production surface | Rust mapping and integration decision |
| --- | --- |
| `RequestInfo` fields and `NewRequestInfo` | Public `ResourceControlRequestInfo` stores native `Option<u64>` write bytes (`None` is source `-1` read; `Some(0)` is a zero-byte write), store/replica IDs, request size, access location, predicted bytes, Cop identity, and bypass. The public constructor initializes the same four precomputed inputs; public getters reproduce every controller interface method. |
| Access-location conversion | Route construction classifies local/cross-zone stores; selection preserves those two values and maps unknown/native extension values to source `AccessUnknown`. |
| Analyze and internal-request bypass | The source analyze type `104` bypasses only under `nextgen`, only when the request source contains `stats`, and for Cop, CopStream, or BatchCop wire requests. `internal_others` bypass remains feature-independent. Typed stream wrappers expose their owned Cop type without inventing a dynamic request union. |
| `MakeRequestInfo` read/write classification | The exact `tikvrpc.IsTxnWriteRequest || IsRawWriteRequest` command matrix is implemented on the typed request boundary. Only Prewrite and Commit derive nonzero bytes; other classified writes remain zero. Prewrite counts mutation keys/values, primary lock, and secondaries; Commit counts keys. Missing peers yield store ID zero. |
| Request size | Uses the source's deliberately narrow `tikvrpc.Request.GetSize` command matrix, not every protobuf's encoded size. This preserves zero for RawDelete and other omitted commands while retaining encoded sizes for Get/BatchGet/Scan/Cop and the listed transaction commands. |
| Route-owned request inputs | `Dispatch` supplies selected replica count and normalized access location after sharding/routing. `PlanBuilder::predicted_read_bytes` supplies the optional hint only to reads. Ordinary Cop and CopStream set `IsCop`; BatchCop deliberately does not, matching `isCopRequest`, while still participating in analyze bypass. |
| `ResponseInfo` switch | Unary Cop, CopStream first response, Get, BatchGet, and Scan have separate source-shaped branches; unsupported responses return all-zero information. Cop counts top-level data/details plus every nested batch task. CopStream counts only its embedded first response's data/details and keeps response size zero because source `tikvrpc.Response.GetSize` omits the wrapper. Scan uses complete encoded response size. |
| Scan-detail bytes | Legacy builds use processed-version bytes. `nextgen` uses `max(total, processed)`, preserving compatibility when an older TiKV reports processed greater than total. Data length is used when Cop details are absent. |
| KV CPU | Exact precedence is V2 nanoseconds, V2 legacy milliseconds, legacy response milliseconds, then zero. Nested batch task CPU is accumulated independently. |
| Response interface | Public getters expose read bytes, KV CPU, response size, and source's always-true `Succeed`. `Default` represents a nil or unsupported response and supports the source txn-file adapter boundary. |
| Controller/dispatch integration | The native async `ResourceGroupController` is selected only for a non-empty group, enabled/installed controller, non-background request, and non-bypass request. Admission runs before user RPC interceptors, updates RU details, installs penalty and only-unset priority, and prevents transport on failure. A successful physical response is settled once and updates RU details; transport failure is not settled. Shard/retry clones retain controller, group, route, predicted-byte, and RU state. Txn-file bulk admission uses discounted MemDB bytes and voter count; a post-commit settlement failure preserves the committed result, emits the source `TxnFileErrorAccounting` metric, and logs the error. |

Client-go carries route fields in one dynamic `tikvrpc.Request`; client-rust owns them in `Dispatch` because its protobuf requests are statically typed. Combining those fields immediately before controller selection is the native equivalent and ensures every physical shard/retry is charged for its actual selected route. The PD controller's token algorithms remain supplied by the application/PD dependency and are not transcreated into this internal accounting package.

## Test/support mapping

All five source tests are independently named and executable in Rust:

- `TestMakeRequestInfo` → `source_go_internal_resourcecontrol_resource_control_test_TestMakeRequestInfo`: the exact BatchGet, Prewrite, Commit, bypass, store-ID, nil-peer, and write-byte assertions are preserved.
- `TestMakeRequestInfoPredictedReadBytes` → `source_go_internal_resourcecontrol_resource_control_test_TestMakeRequestInfoPredictedReadBytes`: native route selection carries the source 256-KiB hint and the zero default only for reads.
- `TestMakeRequestInfoIsCop` → `source_go_internal_resourcecontrol_resource_control_test_TestMakeRequestInfoIsCop`: Cop and CopStream are true; Get, BatchGet, and Scan are false. A separate production-derived test proves BatchCop's source-false paging identity while retaining analyze bypass.
- `TestResponseInfoReadBytes` → `source_go_internal_resourcecontrol_resource_control_test_TestResponseInfoReadBytes`: legacy uses processed bytes, NextGen uses total bytes, and the NextGen compatibility row selects processed bytes when it is larger.
- `TestResponseInfoBatchedTasks` → `source_go_internal_resourcecontrol_resource_control_test_TestResponseInfoBatchedTasks`: the exact top-level plus three nested-task byte/CPU table is reproduced.

Additional production-derived coverage checks the full transaction/raw write command matrix, the narrow request-size matrix, all access-location outcomes, internal/analyze/background selection, CopStream dispatch downcasting and first-response accounting, transactional Get/BatchGet/Scan response accounting, legacy CPU fallback, public constructors/getters, controller ordering, penalty/priority mutation, RU accumulation, global enable/install policy, and no settlement after transport failure.

The re-audit also added a direct consumer regression for client-go's txn-file accounting-failure side effect. Before the fix, a failed post-commit controller settlement left `TiKVTxnFileErrorCounter{type="accounting"}` unchanged (`0` rather than `1`). Rust now increments the counter exactly once while retaining the already committed transaction, matching `txn_file.go`.

Two additional red/green boundary regressions exercise arithmetic that the five source tests do not reach. Batched Cop scan-byte accumulation previously panicked in a debug Rust build when `u64::MAX` was followed by one byte, while Go's `uint64` wraps to zero. Batched KV CPU similarly retained `2^64` nanoseconds in Rust instead of wrapping the low 64-bit `time.Duration` wire arithmetic to zero. Scan-byte addition, V2/legacy CPU addition, and legacy millisecond conversion now use explicit wrapping arithmetic; ordinary representable durations are unchanged.

## Consumer inventory

Every pinned source consumer was inspected:

- `internal/client/client_interceptor.go`, `client_async.go`, and `client.go` map to the physical `Dispatch` admission/settlement boundary, streaming wrappers, RU-v2 bypass, and the already complete `internal/client` transport package.
- `internal/client/client_interceptor_test.go` bypass/admission/error/response behavior maps to the package and transaction dispatch tests listed above.
- `txnkv/transaction/txn_file.go` consumes precomputed `RequestInfo` and empty `ResponseInfo`. Rust exposes both native forms, preserves discounted byte/voter/leader inputs, ignores a post-commit settlement error, and now emits the source accounting-error metric. The txn-file protocol itself remains owned by the separate completed `txnkv/transaction` receipt.

No other pinned Go production or test file imports this package. Downstream transaction/txn-file completion is not implied by this receipt.

## Completion gates

Final validation on `nightly-2026-08-22` used the exact package code:

- The exact `source_go_internal_resourcecontrol_resource_control_test_` filter passed all five direct source-test identities with both `--no-default-features` and `--all-features`.
- The complete `resource_control::test::` module passed 15 tests with `--no-default-features` and 15 with `--all-features`.
- The txn-file accounting-error consumer regression passed in both feature configurations.
- `make unit-test` passed 1,401 tests with two configured skips in the no-default workspace matrix and 1,365 tests with six configured skips in the all-feature library matrix.
- `make check` passed generation verification, all-workspace/all-target/all-feature checking, rustfmt, and Clippy with `-D warnings`.
- `make doc` passed private-item rustdoc with `-D warnings` and all 51 doctests.
- `cargo +nightly-2026-08-22 fmt --all -- --check` and `git diff --check`: passed.

The pinned Go 1.25.12 package passed all four source configurations:

- `go test ./internal/resourcecontrol -count=1`.
- `go test -tags nextgen ./internal/resourcecontrol -count=1`.
- `go test -race ./internal/resourcecontrol -count=1`.
- `go test -race -tags nextgen ./internal/resourcecontrol -count=1`.

The race runs emitted only the known macOS malformed `LC_DYSYMTAB` linker warning. Mechanical reconciliation finds exactly two artifacts/511 lines, five Go declarations, five exact independently named Rust ports (each defined once), no benchmark/example/support harness, and five direct Go importer files. No live cluster is required: package-owned outputs are deterministic request/response accounting and dispatch ordering over typed mock transport; high-level RU behavior remains covered by the completed integration matrix.
