# Pinned client-go txnkv/txnsnapshot package inventory

The whole pinned package is an open dependency acceptance unit. TiDB master
0b505ecc58b659655345b7bb85a619db02f94300 selects client-go/v2
v2.0.8-0.20260921040125-5f38569c8cc0. Individual fixes are seed evidence,
not a transcreated-package claim. Related plan:
rust/docs/operations/store-copr-audit-execplan.md.

| Artifact | Lines | SHA-256 | Rust owner / disposition |
| --- | ---: | --- | --- |
| `client_helper.go` | 168 | `1c8fe55bca147827b9f1f0ae9c2f07480029657cfcb2a3350c925b288c4b04e6` | tidb-txnkv lock resolver and snapshot request contexts; partial hint/wrapper/cache/metrics integration |
| `scan.go` | 360 | `624db2aaeff610817fcea812a2bb9a9f35b0fa383d93dea4702210f783bbb698` | tidb-txnkv transaction/coordinator/snapshot_read.rs; forward scan only, original scanner lifecycle/reverse/options remain open |
| `snapshot.go` | 1552 | `2ea9ee7c2ac01c024aeec357388d4b2475323c3a179687c34c798e99085ab7d8` | tidb-txnkv transaction/coordinator/snapshot_read.rs, snapshot_batch_get.rs and region_batches.rs; cache/Get/BatchGet seed, full options/statistics/replica/tier behavior open |
| `snapshot_async.go` | 321 | `bc29fa3f439714bcc4f73316787ff37dc761514df7fa869607fe52135abec022` | tidb-txnkv transaction/command_client.rs and snapshot_batch_get.rs; completion-order admission, scoped retry workers and cancellation/join integrated; configuration/statistics/options gates open |
| `snapshot_async_test.go` | 151 | `58761e072d346c24d79e439aa7976a6fb61100278aec49c3f502122ef9835b58` | snapshot_lock_wait_source cancellation/join regression holds both status and retry RPCs; original Go test also passes; live transport reconciliation remains open |
| `snapshot_test.go` | 281 | `4b0ca6f81413776cebe4bf77572f1532c12e5139f338c1e9de02cd3de25dfd92` | snapshot source tests and point statistics owners; complete original test reconciliation open |
| `test_probe.go` | 74 | `d130a33d8d18dc3cf4d2b7e85f5578dc67df787ce0a4aa4d48935f379d01fc17` | Go support artifact; Rust scripted client seams, complete correspondence open |

All seven package artifacts are listed, including production-compiled test
support. There is no doc.go, nested fixture, generated input/output,
platform-specific file or package-local build file. Build-tag/import and module
requirements still belong to the complete acceptance gate. Module go.mod,
go.sum and LICENSE hashes are recorded in the sibling
client-go-txnlock-package-inventory.md; TiDB go.mod/go.sum and DEPS.bzl select
the consuming build. The package has no standalone main_test.go.

Get and BatchGet pass the exact sent request hints into read lock resolution.
BatchGet resolves per physical response with a worker-owned backoffer. Scanner
pair errors use snapshot.get; response-level errors use ResolveLocks with
ForRead false and do not stamp resolved/committed hints onto Scan RPCs. Full
scanner iteration, asynchronous configuration/statistics, cache/options,
replica routing, metrics, original tests and platform/build gates remain open.

The MaxTS first-lock shortcut is now represented in both live point-read paths,
including Scan pair rereads. Each Get records its first transaction, skips a
later unhinted transaction only at MaxTS, and resolves repeated hinted locks.
The scripted Rust regression covers ordinary timestamps, both entry paths,
exact sent contexts, status-RPC ordering and reset between Gets. The selected
original TestKV lock-hint tests in the pinned module's tikv package also pass;
they are supplementary oracle evidence, not acceptance of that separate package.
See the ExecPlan for exact commands and remaining worker gates. All seven
artifact hashes and line counts were rechecked at the new master; the pin and
module inputs are unchanged from 64e8c4c05e.

The live BatchGet owner now starts recovery as each initial RPC completes,
without waiting for an earlier slow response or another batch's TTL/status
wait. Successful initial replies create no recovery thread. Split retries
fork the charged backoff state, preserve each physical request's hint context,
and run their children independently. Scoped workers are joined on success,
sibling failure, cancellation and deadline; failed operations do not populate
the snapshot cache. Get and BatchGet retain/update their resolving-lock token
through the full retry lifetime. The complete original async runtime statistics,
EnableAsyncBatchGet option wiring, remaining snapshot options and all other
whole-package gates are still open. See the worker milestone in the ExecPlan.
