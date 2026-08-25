# client-go to client-rust parity ledger

Source of truth: `tikv/client-go@52c1e76cec993571493c81de442bcbef90cdc106`.

Rust baseline: `tikv/client-rust@0ebb6b91a50684aeb07383a500033955e3bcfeb3`.

Statuses are `unassessed`, `seed`, `in-progress`, `blocked`, `complete`, or `not-applicable`. Only `complete` is a package parity claim. A receipt must account for production sources, tests/support, fixtures, generated/build artifacts, Rust integration, and validation.

| Go package | Initial Rust owner | Status | Receipt / dependency note |
| --- | --- | --- | --- |
| `config` | `src/config.rs`, `src/config/client_go.rs`, client constructors | complete | Receipt below; all defaults, nested sections, validation, global snapshot/restore, path parsing, TLS material behavior, RU-v2 accounting, and both `nextgen` build selections are covered. |
| `config/retry` | `src/retry.rs`, `src/async_util.rs`, `src/kv/variables.rs`, `src/request/plan.rs`, `src/stats.rs` | complete | Complete package receipt below and full artifact/symbol/test/consumer mapping in [`config-retry-source-artifact-audit.md`](config-retry-source-artifact-audit.md). Both production files, the original test file, goleak harness, all 17 retry classes, every state/error/cancellation/fork branch, native context/error decisions, metrics/runtime-stat boundaries, and validation gates are accounted for. Each unfinished caller loop remains explicitly owned by its own package row. |
| `error` | `src/error.rs`, `src/common/errors.rs`, `src/stats.rs` | complete | Receipt below; every singleton and typed wrapper, predicates, constructors, extraction precedence/failpoint/redaction, protobuf text/debug JSON, logging, and write-conflict metric side effect are covered. |
| `internal/apicodec` | `src/{common/errors,request/keyspace,request/mod,request/plan_builder,store/request,raw/client,raw/requests,transaction/{lock,requests,transaction}}.rs`, directly required proto/generated inputs | complete | Complete package receipt below and full seven-artifact/schema/symbol/test/consumer mapping in [`internal-apicodec-source-artifact-audit.md`](internal-apicodec-source-artifact-audit.md). V1/V2 byte, region, request, response, context, error, bucket, MPP/Compact, API V3 rejection, typed decode-error, and exact unsupported-command behavior are covered. Each high-level routing/raw/transaction consumer retains its own package status. |
| `internal/client` | `src/store`, `src/pd`, transport | complete | Complete package receipt below and full per-artifact/test mapping in [`internal-client-source-artifact-audit.md`](internal-client-source-artifact-audit.md). All ten production files, the mockserver support dependency, six ordinary test files, goleak harness, metadata-only `OWNERS`, typed command matrix, native API-codec boundary, and validation gates are accounted for. Proxy selection remains correctly owned by the separate `internal/locate` row. |
| `internal/client/mockserver` | `src/store/mockserver.rs`, generated Tonic server bindings | complete | Receipt below. The source's narrow test-server RPC and lifecycle surface is transcreated as test-only Rust support, independently reusable by the parent `internal/client` transport tests. |
| `internal/kvrpc` | `src/request/shard.rs`, `src/raw/requests.rs` | complete | Receipt below; size/count boundaries, aligned payloads, region association, and typed result mapping are covered. |
| `internal/latch` | `src/transaction/latch.rs`, transaction client/commit | complete | Receipt below; hashing, sizing, ordered acquisition, stale detection, wakeup/recycle/close behavior, async cancellation safety, configuration, and optimistic-commit integration are covered. |
| `internal/locate` | `src/region.rs`, `src/region_cache.rs`, `src/pd`, `src/request`, `src/store`, `src/traffic.rs` | complete | Complete package receipt below and full 17-artifact/production-symbol/147-test/dependency/consumer mapping in [`internal-locate-source-artifact-audit.md`](internal-locate-source-artifact-audit.md). Region coding and indexes, TTL/GC/refresh, store discovery/liveness/health, replica and proxy selection, sender retry/error precedence, runtime diagnostics, network accounting, configurable PD region-meta circuit breaking, native async ownership, and validation gates are accounted for. Raw, snapshot, transaction, root `tikv`, and other consumers retain independent statuses. |
| `internal/logutil` | Rust `log` call sites | seed | Complete three-file inventory audited (`log.go`, `tracing.go`, `hex.go`) with no source tests or build artifacts. Existing `trace` hooks cover the trace dependency, but `hex.go` generically reflects every Go protobuf byte field and redacts it recursively. Generated Prost messages do not retain equivalent runtime descriptors, so completion requires a deliberate descriptor/reflection strategy; a lossy Debug-format substitute is not source parity. |
| `internal/mockstore/cluster` | `src/mock/cluster.rs` | complete | Receipt below; all interface methods, protobuf shapes, nullable results, signed counts, and ownership boundaries are covered. |
| `internal/mockstore/deadlock` | `src/mock/deadlock.rs` | complete | Receipt below; synchronized detection, source-exact cycle hash, deduplication, cleanup, expiry, and all original tests are covered. |
| `internal/mockstore/mocktikv` | Rust test support | unassessed | Large mock protocol surface. |
| `internal/resourcecontrol` | `src/resource_control.rs`, `src/request/{plan,plan_builder}.rs`, `src/store/{mod,request}.rs`, transaction request contexts / resource-manager proto | complete | Complete package receipt below and full artifact/symbol/consumer mapping in [`internal-resourcecontrol-source-artifact-audit.md`](internal-resourcecontrol-source-artifact-audit.md). Both production/test files, both legacy/NextGen variants, exact request/response accounting matrices, stream paths, bypass, routing inputs, controller ordering, RU updates, public native interfaces, and validation gates are covered. The external PD controller algorithm and downstream txn-file protocol retain separate ownership. |
| `internal/unionstore` | `src/transaction/unionstore.rs` plus native ART/RBT/arena adapters | complete | Receipt below; all eight production files and six source test/support/benchmark artifacts are accounted for. Public transaction consumption remains on the separate `txnkv/transaction` row. |
| `internal/unionstore/arena` | `src/transaction/arena.rs` | complete | Receipt below; block allocation, addresses, checkpoints, hooks, value-log history/revert/inspection, and all original tests are covered. |
| `internal/unionstore/art` | `src/transaction/art.rs` | complete | Receipt below; all nine source/test artifacts are covered by a safe ordered-map/value-log mapping, with the parent unionstore integration retained on its own row. |
| `internal/unionstore/rbt` | `src/transaction/rbt.rs` | complete | Receipt below; all five source/test artifacts are covered by a safe ordered-map/value-log mapping, with the parent unionstore integration retained on its own row. |
| `TiDB/pkg/store/mockstore/unistore` | `unistore` workspace crate | seed | User-directed reusable standalone test substrate. The initial committed-version MVCC core is independent of `tikv-client` and is consumed by a client pipelined-MemDB integration test; complete TiDB UniStore protocol/RPC packages remain a separate, large source inventory and cannot yet be claimed. |
| `kv` | `src/kv`, root exports | complete | Receipt below; all five production files, three test/support files, key flags, lock/get metadata, variables, atomics, and read/location modes are covered. |
| `metrics` | `src/stats.rs`, Prometheus instrumentation | seed | The error package increments a native Prometheus transaction write-conflict counter. Range-task batch loading now also registers and emits the source `region_cache_operations_total{type="scan_regions",result}`, `load_region_cache_seconds{type="scan_regions"}`, and `stale_region_from_pd` metrics. Exact client-go namespace/subsystem/const-label registration and all remaining metric families/update sites still require the complete metrics receipt. |
| `oracle` | `src/oracle.rs`, `src/timestamp.rs` | complete | Receipt below; complete interface/future/validator surface, exact timestamp helpers, global scope, noop behavior, and typed source errors are covered. |
| `oracle/oracles` | `src/oracle/oracles.rs`, PD timestamp adapter | complete | Receipt below; local/mock/PD timestamp allocation, cache, refresh/adaptation, stale reads, validation singleflight/retry/cancellation, external/min timestamps, metrics, source test hooks, and native task-leak coverage are complete. |
| `rawkv` | `src/raw` | in-progress | Full source/integration inventory audited: `rawkv.go`, `rawkv_test.go`, `test_prob.go`, and `integration_tests/raw/{api_mock_test.go,api_test.go,util_test.go,tikv-v1ttl.toml,tikv-v2.toml}`. Initial end-to-end RawChecksum support, source-compatible positional RawBatchGet values, 512-key and 16 KiB batch boundaries, end-inclusive reverse-scan region traversal and empty unbounded-upper behavior, arbitrary column-family pass-through/reset, chainable in-place `SetColumnFamily(string)` and reversible `SetAtomicForCAS(bool)` semantics (alongside Rust clone builders), retained PD cluster ID, shared PD accessor and consuming close lifecycle, V1/V1TTL/V2 constructor selection (V1TTL correctly emits V1 request context), source-exact server execution-duration contexts, BatchPutWithTTL validation/defaulting and legacy first-TTL wire compatibility, source-exact atomic BatchDelete and flagless DeleteRange dispatch, no-dispatch empty bounded DeleteRange behavior, unbounded multi-region DeleteRange sharding, point read/CAS server-error propagation, and source 10,240 scan-limit guards are present. Scoped `raw_ttl` live validation was attempted on 2026-08-23, but no `PD_ADDRS` was configured and the default `127.0.0.1:2379` refused the connection before setup; live coverage remains required in an available TiKV/PD environment. Completion still requires every raw API/options/error path and source test/support artifact. |
| `testutils` | Rust test support | seed | The sole 61-line file is an alias/factory façade over `internal/mockstore/cluster` and `internal/mockstore/mocktikv`; completion must follow the concrete mocktikv implementation rather than invent empty aliases. |
| `tikv` | `src/{pd,region_cache,request,store}`, `src/transaction/client.rs`, `src/async_util.rs` | seed | Atomic inventory: production `backoff.go` (101), `client.go` (73), `compatible_txn_safe_point_loader.go` (139), `gc.go` (423), `interface.go` (91), `kv.go` (1,093), `logutil.go` (32), `pool.go` (45), `region.go` (281), `safepoint.go` (243), `split_region.go` (417), `unionstore_export.go` (68), plus production test hooks `failpoint_export.go` (23), `test_probe.go` (267), and `test_util.go` (153); tests `kv_test.go` (417) and goleak `main_test.go` (29). There are no package-local non-Go fixtures, generated inputs, or build variants. Rust already has compatible client construction, PD/region cache ownership, pool/run-loop support, GC/safepoint RPC primitives, unsafe range deletion, and SplitRegion transport, but not the source `KVStore` lifecycle, compatible transaction-safe-point loader, full GC worker/service-safe-point flow, split/scatter orchestration, public region/store interfaces, hooks, or original tests. Completion is downstream of `internal/client`, `internal/locate`, `tikvrpc`, and `txnkv/transaction`; this is an assessed seed, not a package claim. |
| `tikvrpc` | `src/store/{command,endpoint,errors,request}.rs`, `src/lib.rs` exports, request plans/routing, generated protocol bindings | complete | Complete package receipt below and full six-artifact/protocol/symbol/test/consumer mapping in [`tikvrpc-source-artifact-audit.md`](tikvrpc-source-artifact-audit.md). Every command, route, context, batch, unary/debug/stream, size/detail/error, endpoint, origin, start-TS, tagger, timeout/close, original-test, generated-list, and 74-consumer boundary is accounted for. Typed payloads and owned snapshots replace Go's unchecked dynamic wrapper and revision race without weakening behavior; unfinished high-level consumers retain their own rows. |
| `tikvrpc/interceptor` | `src/interceptor.rs`, transaction/snapshot dispatch plans | complete | Receipt below; native async wrappers preserve ordered onion execution, duplicate-name replacement, chain flattening, transaction/snapshot set/add APIs, and physical-RPC dispatch integration. |
| `trace` | `src/trace.rs` | complete | Receipt below; flags, categories, independently replaceable global hooks, typed contexts/fields, trace IDs, defaults, resets, and all original tests are covered. |
| `txnkv` | `src/transaction/{client,mod,snapshot,lock,priority}.rs`, `src/lib.rs` exports | seed | Complete root-package inventory: production `client.go` (138), `lock_export.go` (41), `snapshot_export.go` (61), `transaction_export.go` (58), and `util_export.go` (27); compile-only `client_test.go` (25); no package-local generated/build/fixture artifact. Rust exposes the native transaction client, current timestamp, keyspace-configured construction, lock resolver, snapshot, priority, and transaction APIs, but does not yet own client-go's single `tikv.KVStore` wrapper, API-version/safe-point-prefix construction topology, or close-time txn-file idle-connection cleanup. The dependent transaction, snapshot, lock, and high-level `tikv` package receipts remain required before this root package can be complete. |
| `txnkv/rangetask` | `src/transaction/range_task.rs`, `src/transaction/client.rs`, `src/transaction/requests.rs`, `src/stats.rs` | complete | Complete package receipt below and full artifact/symbol/consumer mapping in [`txnkv-rangetask-source-artifact-audit.md`](txnkv-rangetask-source-artifact-audit.md). Both production files and the complete external integration-test matrix are accounted for; the reusable public runner, stateful DeleteRange task, idiomatic client entrypoints, retries, metrics, logging, cancellation, and validation gates are covered. Downstream GC, split/scatter, and pipelined transaction algorithms retain their own package rows. |
| `txnkv/transaction` | `src/transaction/{transaction,txn_file,client,requests,lowering,buffer}.rs`, request/error/PD integration | complete | Complete package receipt below and full 16-artifact/11,766-line production/support/test/consumer mapping in [`txnkv-transaction-source-artifact-audit.md`](txnkv-transaction-source-artifact-audit.md). Normal, pessimistic, shared, aggressive, async/1PC, pipelined, transaction-file, binlog, schema/filter/callback, mutation-assertion, retry/cleanup, and all 33 original package-test declarations are accounted for. High-level `txnkv`, lock, snapshot, root `tikv`, and live differential gates retain independent statuses. |
| `txnkv/txnlock` | `src/transaction/lock.rs`, `src/transaction/{client,requests,transaction}.rs`, `src/request/plan.rs`, `src/stats.rs` | complete | Atomic receipt: [`txnkv-txnlock-source-artifact-audit.md`](txnkv-txnlock-source-artifact-audit.md). All two production and four test/support artifacts (2,144 lines), five ordinary tests plus `TestMain`, production symbols, direct consumers, lifecycle owners, and validation gates are assigned. The shared client resolver now owns a cancel-and-join bounded pool for read cleanup, secondary checks, and exact per-region async-commit recovery; physical-shard metrics, pipelined observer lifetime, determined FIFO cache, read hints, lite/pessimistic/GC paths, NextGen async requests, resource context, and owner shutdown match the pin. Snapshot retry-class consumption remains explicitly owned by the separate `txnkv/txnsnapshot` receipt. |
| `txnkv/txnsnapshot` | `src/transaction/{snapshot,sync_snapshot,snapshot_stats,transaction}.rs`, read request plans | seed | Atomic source inventory: production `client_helper.go` (168), `scan.go` (360), `snapshot.go` (1,414), and `snapshot_async.go` (301); test/support `test_probe.go` (74). There are no colocated Go test files, fixtures, generated inputs/outputs, package build files, or build-tag variants. Rust exposes async and sync snapshots with source-shaped Get/BatchGet/BufferBatchGet, snapshot cache/options, read validation, cumulative region/lock backoff, runtime collection/formatting, and eager stateful forward/reverse scanners. Scanner response-level locks discard incomplete pairs and retry, while pair-level locks retain clean pairs and point-read only the locked key. Collector output follows client-go's RPC/backoff/time/resolve-lock/scan-detail formatting; Rust also exposes generated read-index/read-pool timings, but the pinned protobuf lacks client-go's separate read-pool-task-details message. Completion still requires exact one-region-at-a-time scanner routing/backoff ownership, async callback and test-probe equivalents, remaining source option/result paths, and package-level differential validation. |
| `txnkv/txnutil` | Rust priority type plus transaction request contexts | complete | Receipt below; normal/low/high wire values, defaults, mutable async/sync transaction and snapshot APIs, read/write propagation, retry/shard preservation, and normal heartbeat behavior are covered. |
| `util` | `src/util`, cross-cutting helpers | unassessed | Split Rust files as needed but retain one Go package claim. |
| `util/async` | `src/async_util.rs` | complete | Receipt below; callback and run-loop behavior plus every original test scenario are covered. |
| `util/codec` | `src/kv/codec.rs`, root `codec` re-export | complete | Receipt below; all byte and number codec operations, ordering, append/leftover, boundary, and malformed-input behavior are covered. |
| `util/collectors` | metrics collectors | blocked | Atomic inventory is `channelz.go` and `channelz_test.go`. The source synchronously queries grpc-go's externally served `grpc.channelz.v1.Channelz` API during each Prometheus scrape, then walks and filters its channel/subchannel/socket graph. Rust has Prometheus support but no channelz protobuf bindings; more importantly, Tonic exposes async RPCs whereas `prometheus::core::Collector::collect` is synchronous. A complete native mapping therefore requires an explicit, user-approved scrape-runtime/refresh contract (or a transport/runtime change) before generated protocol inputs and collector behavior can be introduced. Do not add a cached asynchronous façade and claim source parity. |
| `util/intest` | `src/intest.rs`, Cargo `internal-tests` feature | complete | Receipt below; both build variants and mutable runtime override behavior are implemented and validated. |
| `util/israce` | `src/israce.rs`, Cargo `race-tests` feature | complete | Receipt below; both race-enabled and non-race build variants are implemented and validated. |
| `util/redact` | `src/redact.rs` | complete | Receipt below; helper behavior is complete. Consumer call-site integration remains required by each owning package. |

## Non-package artifacts still required

The final repository claim must additionally account for root build and policy files, `go.mod`/`go.sum` dependencies, `.github` CI, `examples`, `integration_tests` including its separate Go module, generated protobuf inputs and outputs, configuration fixtures, and client-rust's `proto-build`, `proto`, `tests`, examples, Cargo features, and toolchain files. The eight directly key-bearing `internal/apicodec` protobuf inputs now match pinned kvproto and include the namespace/lookup/V3 identity expansion, but unrelated generated-root inputs still drift and remain an explicit final artifact gate. These artifacts are not package rows, but no full parity claim is possible without their final receipt.

## Complete package receipt: `internal/apicodec`

Source pin: `client-go@52c1e76cec993571493c81de442bcbef90cdc106`; required protocol pin: `kvproto@059694ae4472276644613acccefa24cbc89d959f`.

The complete four-production-file/three-test-file source inventory, SHA-256 identities, eight directly required protocol inputs, generated-output decision, per-symbol mapping, complete V1/V2 command matrices, native typed-request ownership, all original tests, and source-consumer audit are recorded in [`internal-apicodec-source-artifact-audit.md`](internal-apicodec-source-artifact-audit.md). The package has no other source, build/platform variant, fixture, benchmark, example, metadata, or package-generated artifact.

Rust now preserves V1 raw identity and transactional memcomparable region keys; V2 uint24 prefixes and maximum-ID carry; point/range/reverse/region/bucket transforms; exact API version and numeric keyspace oneofs including V1's all-ones null ID; canonical names; Compact/MPP metadata; API V3 identity rejection; every transactional/raw/Cop/TiFlash/other request and response branch; nested region/key/lock/MVCC transforms; source sibling/edge suppression; and CopStream's exact unsupported-decode error. The completion audit exposed and fixed an untyped malformed-region-key path: V1/V2 failures now carry `ApiCodecDecode`, and public `is_decode_error` traverses native wrapper chains like client-go's `IsDecodeError`. BucketVersionNotMatch keys deliberately remain physical in region errors because neither source response switch decodes them.

The required `keyspacepb`, `kvrpcpb`, and `mpp` schemas were updated from the pinned kvproto checkout and regenerated. Namespace/LookupKeyspace, V3 metadata/context/Compact/MPP identities, and pinned execution-detail fields are compile-tested. Other unrelated kvproto inputs remain on the final generated-artifact gate; this package receipt does not overclaim the whole generated root.

Validation on `nightly-2026-08-22`:

    cargo +nightly-2026-08-22 test -p tikv-client request::keyspace::tests --lib
    # 27 passed; 0 failed

    cargo +nightly-2026-08-22 test -p tikv-client request::keyspace::tests --lib --all-features
    # 27 passed; 0 failed

    cargo +nightly-2026-08-22 test -p tikv-client --lib --quiet
    # 534 passed; 0 failed

    cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet
    # 534 passed; 0 failed

    cargo +nightly-2026-08-22 check -p tikv-client --all-targets --all-features
    # passed with the existing warning backlog

    cargo +nightly-2026-08-22 doc -p tikv-client --all-features --no-deps
    # passed with two pre-existing unrelated rustdoc warnings

    cargo +nightly-2026-08-22 fmt --all -- --check
    git diff --check
    # passed

    for proto in apipb coprocessor errorpb keyspacepb kvrpcpb metapb mpp tikvpb; do
        cmp "proto/$proto.proto" "/private/tmp/kvproto-client-go-pin/proto/$proto.proto"
    done
    # all eight inputs byte-identical

The pinned Go tests were inspected but not re-executed because this host has no Go toolchain. Deterministic byte/protobuf behavior needs no live cluster; final API-v1/API-v2 cross-client validation remains mandatory on the incomplete high-level `tikv`, `rawkv`, snapshot, and transaction rows.

## Complete package receipt: `internal/locate`

Source pin: `client-go@52c1e76cec993571493c81de442bcbef90cdc106`; protocol pin: `kvproto@059694ae4472276644613acccefa24cbc89d959f`; PD dependency pin: `afa43111d1494d620c225e51461097097661d127`.

The complete nine-production-file/eight-test-support-file/20,132-line inventory, SHA-256 identities, per-surface integration decisions, all 147 original declarations, ten direct protocol inputs, pinned PD circuit-breaker behavior, and all 28 importing Go files are recorded in [`internal-locate-source-artifact-audit.md`](internal-locate-source-artifact-audit.md). There is no package-local doc, build/platform variant, fixture, generated source, benchmark, example, metadata, or build file.

Rust now preserves region coding and keyspace boundaries; exact region/version/range/bucket behavior; three coherent indexes; process-wide TTL/jitter; preload, full refresh, bounded GC, and four reload flags; cache-only/PD/range/batch location and grouping; stable store discovery/re-resolution/tombstones; TiFlash filters/compute discovery; liveness, health, slow/load/flow state; logical/physical/proxy routes; replica selector scores, attempts, error flags, time budgets, busy/stale/flashback/learner/NextGen paths; sender cancellation, retry/backoff, token, timeout, connection, epoch, bucket, and region-error precedence; runtime diagnostics; and request/response network accounting. Native futures consolidate Go's sync/callback façades, `BTreeMap` replaces the custom B-tree wrapper, and immutable PD peer metadata is filtered at selection rather than destructively edited. The configurable PD region-meta breaker implements the pinned closed/open/half-open state machine and exact overload code set.

Validation on `nightly-2026-08-22`:

    cargo +nightly-2026-08-22 test -p tikv-client --lib source_
    # 292 passed; 0 failed

    cargo +nightly-2026-08-22 test -p tikv-client --lib --quiet
    # 548 passed; 0 failed

    cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet
    # 548 passed; 0 failed

    cargo +nightly-2026-08-22 check -p tikv-client --all-targets --all-features
    # passed with the existing warning backlog

    cargo +nightly-2026-08-22 doc -p tikv-client --all-features --no-deps
    # passed with two pre-existing unrelated rustdoc warnings

    cargo +nightly-2026-08-22 fmt --all -- --check
    git diff --check
    # passed

    cargo +nightly-2026-08-22 run -p tikv-client-proto-build
    # checked-in bindings regenerated

    for proto in apipb coprocessor disaggregated errorpb keyspacepb kvrpcpb metapb mpp pdpb tikvpb; do
        cmp "proto/$proto.proto" "/private/tmp/kvproto-client-go-pin/proto/$proto.proto"
    done
    # all ten direct inputs byte-identical

    # mechanical audit: 147 source declarations, 147 recorded names,
    # no missing or extra test declaration

The pinned Go tests were inspected but not re-executed because this host has no Go toolchain. Deterministic routing, cache, selector, error, coding, and lifecycle behavior plus completed loopback Tonic transport establish this package boundary without a live cluster. Raw, snapshot, transaction, root `tikv`, and final cross-client cluster workflows retain independent gates.

## Complete package receipt: `tikvrpc`

Source pin: `client-go@52c1e76cec993571493c81de442bcbef90cdc106`; required protocol pin: `kvproto@059694ae4472276644613acccefa24cbc89d959f`.

The complete six-file/2,624-line inventory, source and protocol SHA-256 identities, generated-command decision, per-symbol mapping, every original test, and all 74 importing Go files are recorded in [`tikvrpc-source-artifact-audit.md`](tikvrpc-source-artifact-audit.md). There is no other package-local source, build/platform variant, fixture, benchmark, example, metadata, or build file; `tikvrpc/interceptor` remains a separately completed child package.

Rust now preserves every continued-`iota` command value, name, alias, and classification; all 54 ordinary/debug physical routes; exact 42-command generated context registry plus CopStream and MPP/Empty special cases; process-wide default origin; owned context replacement and relocation-safe batch snapshots; all batch request/response oneofs including Empty; narrow size/detail/error matrices; all 37 concrete region-error response types; 22 start-timestamp branches; typed response-address and request-tagger carriers; four endpoint types and engine labels; and unary, BatchCommands, Cop/BatchCop/MPP stream timeout/first-response/close behavior. Static payload types replace Go's unchecked `interface{}` assertions, and per-receive Tokio deadlines replace the grpc-go lease-scanner task. The CopStream bypass branch consumes its one-time RPC count while leaving server details and RU totals untouched.

Validation on `nightly-2026-08-22`:

    cargo +nightly-2026-08-22 test -p tikv-client store::request::tests --lib
    # 14 passed; 0 failed

    cargo +nightly-2026-08-22 test -p tikv-client store::command::tests --lib
    # 8 passed; 0 failed

    cargo +nightly-2026-08-22 test -p tikv-client store::endpoint::tests --lib
    # 2 passed; 0 failed

    cargo +nightly-2026-08-22 test -p tikv-client store::errors::test::source_gen_region_error_response_matrix_is_complete --lib
    # 1 passed; 0 failed

    cargo +nightly-2026-08-22 test -p tikv-client --lib --quiet
    # 541 passed; 0 failed

    cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet
    # 541 passed; 0 failed

    cargo +nightly-2026-08-22 check -p tikv-client --all-targets --all-features
    # passed with the existing warning backlog

    cargo +nightly-2026-08-22 doc -p tikv-client --all-features --no-deps
    # passed with two pre-existing unrelated rustdoc warnings

    cargo +nightly-2026-08-22 fmt --all -- --check
    git diff --check
    # passed

    for proto in coprocessor debugpb errorpb kvrpcpb metapb mpp tikvpb; do
        cmp "proto/$proto.proto" "/private/tmp/kvproto-client-go-pin/proto/$proto.proto"
    done
    # all seven direct inputs byte-identical

The pinned Go tests were inspected but not re-executed because this host has no Go toolchain. No live cluster is needed for the deterministic command/context/codec behavior or for stream behavior already exercised by the completed loopback transport receipt. Locate behavior is now covered by its own complete receipt; raw, snapshot, transaction, and high-level `tikv` behavior remain on their own non-complete rows and the final differential cluster gate.

## Complete package receipt: `config/retry`

Source pin: `client-go@52c1e76cec993571493c81de442bcbef90cdc106`.

The complete two-production-file, one-test-file, and goleak-harness inventory, SHA-256 identities, per-symbol mapping, native context/error decisions, exact original-test mapping, and source-consumer audit are recorded in [`config-retry-source-artifact-audit.md`](config-retry-source-artifact-audit.md). The package has no other fixture, generated/build input, platform/build-tag variant, documentation artifact, benchmark, example, or metadata file.

Rust now preserves all 17 source retry classes; four jitter algorithms; variable-derived bases and weighted budgets; ordinary and separately excluded cumulative sleep; one-sleep caps; failpoint skipping; cancellation and kill precedence; last-three versus lifetime diagnostics; source-selected terminal errors; clone/fork/descendant-merge/type ancestry; reset; exact metric labels; runtime-stat inputs; and fake/real region-error classification. A cancelled pending sleep records zero without advancing the exponential state, correcting the prior seed. Cluster-ID mismatch is still immediate and non-retryable, but maps Go's process-terminating `Fatal` to a typed Rust error so an embedded client library cannot terminate its host process.

Validation on `nightly-2026-08-22`:

    cargo +nightly-2026-08-22 test -p tikv-client retry::tests --lib
    # 20 passed; 0 failed

    cargo +nightly-2026-08-22 test -p tikv-client retry::tests --lib --all-features
    # 20 passed; 0 failed

    cargo +nightly-2026-08-22 test -p tikv-client --lib --quiet
    # 530 passed; 0 failed

    cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet
    # 530 passed; 0 failed

    cargo +nightly-2026-08-22 check -p tikv-client --all-targets --all-features
    # passed with the existing warning backlog

    cargo +nightly-2026-08-22 doc -p tikv-client --all-features --no-deps
    # passed with two pre-existing unrelated rustdoc warnings

    cargo +nightly-2026-08-22 fmt --all -- --check
    git diff --check
    # passed

The pinned Go tests were inspected but not re-executed because this host has no `go` binary. No live cluster is required for the package's deterministic state machine. Correct class/budget/topology selection remains part of every owning consumer package and the final TiKV/PD differential matrix; this receipt does not promote those incomplete rows.

## Complete package receipt: `internal/resourcecontrol`

Source pin: `client-go@52c1e76cec993571493c81de442bcbef90cdc106`.

The complete production/test inventory, SHA-256 identities, per-symbol mapping, native typed-request decision, exact five-test mapping, and source-consumer audit are recorded in [`internal-resourcecontrol-source-artifact-audit.md`](internal-resourcecontrol-source-artifact-audit.md). The package has no additional support, fixture, generated/build, platform/build-tag, documentation, benchmark, harness, or metadata artifact.

Rust now preserves the source request write matrix and byte totals, deliberately narrow request-size matrix, peer/replica/access routing inputs, predicted-read and Cop identities, internal/analyze/background bypass, legacy/NextGen scan bytes, Cop/BatchCop/CopStream response distinctions, CPU precedence, response sizing, public controller information interfaces, physical admission/settlement ordering, penalty/priority mutation, and RU-detail updates. The external PD controller implementation remains injected, and downstream txn-file behavior remains on `txnkv/transaction`; neither is overclaimed here.

Validation on `nightly-2026-08-22`:

    cargo +nightly-2026-08-22 test -p tikv-client resource_control --lib
    # 14 passed; 0 failed

    cargo +nightly-2026-08-22 test -p tikv-client resource_control --lib --all-features
    # 14 passed; 0 failed

    cargo +nightly-2026-08-22 test -p tikv-client --lib --quiet
    # 523 passed; 0 failed

    cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet
    # 523 passed; 0 failed

    cargo +nightly-2026-08-22 check -p tikv-client --all-targets --all-features
    # passed with the existing warning backlog

    cargo +nightly-2026-08-22 doc -p tikv-client --all-features --no-deps
    # passed with two pre-existing unrelated rustdoc warnings

    cargo +nightly-2026-08-22 fmt --all -- --check
    git diff --check
    # passed

The pinned Go tests were inspected but not re-executed because this host has no `go` binary. No live cluster is required for this deterministic accounting/interceptor package; final cross-client TiKV/PD validation still covers consumer-level RU behavior.

## Complete package receipt: `txnkv/rangetask`

Source pin: `client-go@52c1e76cec993571493c81de442bcbef90cdc106`.

The complete two-production-file inventory, SHA-256 identities, source-symbol mapping, native-language decisions, external integration-test mapping, and source-consumer audit are recorded in [`txnkv-rangetask-source-artifact-audit.md`](txnkv-rangetask-source-artifact-audit.md). There is no package-local test/support, fixture, generated/build input, platform/build-tag variant, documentation artifact, or package metadata file. The dedicated 265-line repository integration test is included in the receipt rather than omitted because it lives outside the package directory.

Rust exposes a public generic runner, task stats/handler, the locate backoffer factory, and a stateful DeleteRange task. The runner preserves source batching, clipping, queue/worker capacity, cancellation, worker-order errors, dynamic metric/log identifiers, redacted progress logging, completed resets, cumulative failure state, and enqueue observations. Transactional DeleteRange preserves bounds, destructive/notify-only modes, per-regional-request retry ownership, successful-region counting, terminal response errors, and partial progress after failure. The direct `Client` methods remain the idiomatic stateless façade. Downstream GC, split/scatter, and pipelined-transaction algorithms retain separate package rows and are not implied complete.

Validation on `nightly-2026-08-22`:

    cargo +nightly-2026-08-22 test -p tikv-client range_task --lib
    # 8 passed; 0 failed

    cargo +nightly-2026-08-22 test -p tikv-client delete_range --lib
    # 8 passed; 0 failed

    cargo +nightly-2026-08-22 test -p tikv-client --lib --quiet
    # 520 passed; 0 failed

    cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet
    # 520 passed; 0 failed

    cargo +nightly-2026-08-22 check -p tikv-client --all-targets --all-features
    # passed with the existing warning backlog

    cargo +nightly-2026-08-22 doc -p tikv-client --all-features --no-deps
    # passed with two pre-existing unrelated rustdoc warnings

    cargo +nightly-2026-08-22 fmt --all -- --check
    git diff --check
    # passed

The source's complete mock-region integration matrix is deterministic and is transcreated as a library test. The source package has no live DeleteRange fixture or test; live destructive differential validation remains a final high-level `tikv` gate rather than an unaccounted package artifact.
The pinned Go test was inspected but not re-executed because this host has no `go` binary (`go version` returns command not found).

## Complete package receipt: `internal/client`

Source pin: `client-go@52c1e76cec993571493c81de442bcbef90cdc106`.

The complete inventory, production mapping, native-language decisions, and one-by-one original test/support receipt are recorded in [`internal-client-source-artifact-audit.md`](internal-client-source-artifact-audit.md). The transport owns address-keyed/versioned pool lifecycle, fixed dial and receive defaults, round robin, direct/forwarded unary and streaming RPCs, BatchCommands collection/publication/recovery/diagnostics, native-future cancellation, ResolveLock collapse, resource-control integration, RU-v2 accounting, execution-detail/OpenTracing hooks, health feedback, exact source metric labels/names, panic supervisors, and close/idle behavior. The source `CallRPC` matrix is compiled as 53 typed Rust request routes plus Debug; unary API coding is owned by the typed plan/keyspace boundary and stream-specific coding remains in this transport.

Validation on `nightly-2026-08-22`:

    cargo +nightly-2026-08-22 fmt --all -- --check
    git diff --check

    cargo +nightly-2026-08-22 test --lib --quiet
    # 474 passed; 0 failed

    cargo +nightly-2026-08-22 test --all-features --lib --quiet
    # 474 passed; 0 failed

Strict all-feature Clippy was attempted but remains blocked by the repository's pre-existing generated-code and unrelated lint backlog; the new structured connection helper carries the same local `too_many_arguments` allowance as adjacent constructors. The original Go tests were not run because this host has no Go toolchain. Real loopback Tonic tests cover package-owned network behavior; a live TiKV/PD cluster is not required for this transport package. `internal/locate` proxy/replica selection and `internal/apicodec` each have their own later complete receipt and were not implied complete by this transport receipt.

## Complete package receipt: `internal/client/mockserver`

Source pin: `client-go@52c1e76cec993571493c81de442bcbef90cdc106`.

The complete source inventory is the production/test-support file `internal/client/mockserver/mock_tikv_service.go`. It has no colocated Go tests, `doc.go`, build or platform variant, fixture, generated input/output, or package-specific build artifact. Its consumers are the `internal/client` test files, especially the unary-forwarding, async, BatchCommands, health-feedback, and restart tests; those consumers remain on the parent `internal/client` receipt and are not implied complete by this helper receipt.

Rust maps the package to test-only `src/store/mockserver.rs`. `MockServer` owns loopback bind/start/stop/restart lifecycle and source-style ephemeral startup; its generated-Tonic router exposes exactly the source-supported `KvGet`, `KvPrewrite`, `CoprocessorStream`, and bidirectional `BatchCommands` routes. Its accepted TCP connections are cancellation-aware, so `Stop` wakes and aborts active HTTP/2 streams instead of Tonic's default graceful-drain wait, matching grpc-go `Server.Stop`. Other generated TiKV RPC routes receive Tonic's `Unimplemented` status, equivalent to client-go's embedded generated unimplemented server. The replaceable metadata checker is applied at the four source route boundaries. The replaceable BatchCommands handler takes over the complete response; absent a handler, it emits one empty response per ID and the source health feedback `{store_id: 1, slow_score: 1, monotonically increasing feedback_seq_no}`. Source's grpc-go `ConnectionTimeout(time.Minute)` is a server-preface implementation setting with no Tonic-equivalent public hook; it does not change any supported mock RPC contract. Generated server bindings are supplied by the owned `proto-build` input, which is the required integration artifact.

Native regression coverage invokes the generated gRPC client against a real loopback Tonic listener: it proves every supported RPC route, metadata acceptance/rejection, default response/feedback, dynamic BatchCommands replacement, source-style lifecycle state, duplicate-start rejection, stop/restart at the same address, and prompt force-stop of an active bidirectional stream. The source support package has no separate Go test target; its consumer assertions are retained for the parent `internal/client` package claim.

Validation on `nightly-2026-08-22`:

    cargo +nightly-2026-08-22 check -p tikv-client --all-features
    cargo +nightly-2026-08-22 test -p tikv-client store::mockserver::tests --all-features
    # 6 passed; 0 failed

    cargo +nightly-2026-08-22 fmt --all -- --check
    git diff --check

No real TiKV/PD cluster is required: this package owns only deterministic in-process transport test support. The parent client package retains the actual reconnection, async-dispatch, queue, failure, metrics, and health-feedback consumer validation.

## Complete package receipt: `tikvrpc/interceptor`

Source pin: `client-go@52c1e76cec993571493c81de442bcbef90cdc106`.

The complete inventory is production `tikvrpc/interceptor/interceptor.go`, behavioral tests `interceptor_test.go`, and goleak harness `main_test.go`. There is no `doc.go`, platform/build-tag variant, generated input/output, fixture, or package-specific build file. `integration_tests/interceptor_test.go` and `internal/client/client_interceptor.go` were audited as consumers: client-go binds an interceptor to a transaction/snapshot context and invokes it around each synchronous physical transport request.

Rust maps this to `src/interceptor.rs`, `src/request/plan.rs`, `src/request/plan_builder.rs`, `src/request/shard.rs`, `src/store/mod.rs`, `src/pd/client.rs`, and transaction/snapshot APIs. `RpcInterceptor` receives the resolved store address, immutable request, and one-shot async continuation, so it can run code before/after, replace a response, or suppress dispatch. `RpcInterceptorChain` flattens linked chains, replaces duplicate names, and builds source-equivalent onion order. `Transaction`, `Snapshot`, `SyncTransaction`, and `SyncSnapshot` expose replacement and additive configuration; plan metadata keeps the chain through shards, retries, 2PC, heartbeat, rollback, and lock resolution. The Go context helper has no literal Rust equivalent; per-transaction plan state is the native scope with the same source consumer behavior.

The native unit tests prove duplicate replacement and onion entry/exit ordering, and a mock transaction commit proves one interceptor sees exactly Prewrite and Commit, matching client-go's integration assertion. There are no spawned interceptor tasks, so Go's goleak harness maps to Rust's ordinary awaited-future ownership. Real-network routing supplies a true address through PD; deterministic mock targets are intentionally empty.

Validation on `nightly-2026-08-22`:

    cargo +nightly-2026-08-22 test -p tikv-client interceptor --lib --all-features --quiet
    # 2 passed; 0 failed

    cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet
    # 225 passed; 0 failed

No real cluster is required for this package-owned decorator behavior. The broader connection lifecycle, batching, resource-control integration, and transport fault behavior remain separate `internal/client` / `tikvrpc` claims.

## Complete package receipt: `txnkv/txnutil`

Source pin: `client-go@52c1e76cec993571493c81de442bcbef90cdc106`.

The complete production inventory is `txnkv/txnutil/priority.go` (34 lines). The package has no colocated Go test, platform variant, generated source/input, fixture, or package-specific build file. Integration evidence was audited at `txnkv/util_export.go`, `txnkv/transaction/txn.go`, `txnkv/txnsnapshot/snapshot.go`, and the read/write request-context construction sites under `txnkv/transaction` and `txnkv/txnsnapshot`. Client-go transaction heartbeats were separately audited in `txnkv/transaction/2pc.go` and intentionally remain normal priority.

Rust implementation and integration files are:

- `src/transaction/priority.rs`: idiomatic public enum and exact protobuf conversion.
- `src/transaction/transaction.rs`: normal defaults, builder, mutable setter, and propagation through read, scan, pessimistic lock/rollback, prewrite, commit, and rollback paths.
- `src/transaction/snapshot.rs`, `src/transaction/sync_transaction.rs`, and `src/transaction/sync_snapshot.rs`: mutable user-facing parity for async and sync APIs.
- `src/store/request.rs` and `src/request/plan_builder.rs`: uniform protobuf context setting before a request is cloned for shards/retries. The trait method has a default to preserve compatibility for custom request implementations.
- `src/raw/requests.rs`: wrapper delegation so the request abstraction remains behaviorally transparent; no raw client path selects non-normal transaction priority.
- `src/transaction/mod.rs` and `src/lib.rs`: public export.

Validation on Rust 1.93.0:

    cargo test priority
    # 4 passed; 0 failed

    cargo test --lib
    # 76 passed; 0 failed

    cargo clippy --all-targets --all-features -- -D warnings
    # passed

    cargo fmt --all -- --check
    # passed

    git diff --check
    # passed

No real-cluster test was required for this package: the user-visible contract is the `kvrpcpb.Context.priority` value sent by planned requests, and the tests capture that exact generated protobuf request after normal planning. The test also proves the client-go nuance that a transaction heartbeat remains `Normal` after the transaction changes to `High`.

## Complete package receipt: `util/codec`

Source pin: `client-go@52c1e76cec993571493c81de442bcbef90cdc106`.

The complete production inventory is `util/codec/bytes.go` (195 lines) and `util/codec/number.go` (305 lines). The package has no `doc.go`, colocated Go test, platform/build-tag variant, generated source/input, fixture, or package-specific build file. Its repository consumers and support evidence were audited at `integration_tests/util_test.go`, `internal/apicodec/mem_codec.go`, `internal/mockstore/mocktikv/mvcc.go`, and `internal/mockstore/mocktikv/mvcc_leveldb.go`. Those consumer packages retain their own ledger statuses; this receipt claims only the complete codec contract they exercise.

Rust implementation and integration files are:

- `src/kv/codec.rs`: ascending and descending memory-comparable byte encoding; reusable-buffer decoding with leftovers; the existing in-place decoder; signed-to-comparable mapping; fixed-width signed/unsigned ascending and descending codecs; ordinary signed/unsigned varints; comparable signed/unsigned varints; and exact insufficient/overflow/invalid-input branches.
- `src/lib.rs`: public `tikv_client::codec` module re-export.
- Existing consumers `src/kv/key.rs` and `src/pd/client.rs` continue to use the same codec implementation.

Tests cover the client-go examples for empty, partial, exact-eight-byte, and multi-group byte strings in both orders; append and leftover behavior; output-buffer reuse; invalid markers and padding; fixed-width extrema and ordering; ordinary varint extrema, truncation, and overflow; comparable-varint length boundaries, lexicographic ordering, malformed sign encodings, and the pinned source's unusual non-consuming leftover for a one-byte signed comparable value.

Validation on Rust 1.93.0:

    cargo test codec
    # 5 passed; 0 failed

    cargo test --lib
    # 80 passed; 0 failed

    cargo test --doc
    # 49 passed; 0 failed

    cargo clippy --all-targets --all-features -- -D warnings
    # passed

    cargo fmt --all -- --check
    # passed

    git diff --check
    # passed

No real-cluster test is applicable because this package is a deterministic byte transformation with no I/O or cluster state. The pinned Go package has no test target to run, and the host has no Go toolchain; validation therefore uses a complete source-branch audit, source-derived boundary vectors, the pre-existing TiKV-compatible byte vectors, malformed inputs, and crate integration tests.

## Complete package receipt: `util/israce`

Source pin: `client-go@52c1e76cec993571493c81de442bcbef90cdc106`.

The complete production/build inventory is `util/israce/israce.go`, selected by Go's `race` build tag and defining `RaceEnabled = true`, and `util/israce/norace.go`, selected by `!race` and defining `RaceEnabled = false` (20 lines each). There is no `doc.go`, colocated test, generated input/output, fixture, or package-specific build file. The only source-of-truth consumer is `internal/locate/replica_selector_test.go`, which skips a race-sensitive test when the constant is true; the similarly named checks in client-go integration tests import TiDB's separate israce package and are not part of this package's consumer inventory.

Rust implementation and build integration files are:

- `src/israce.rs`: hidden public `RACE_ENABLED`, selected at compile time.
- `Cargo.toml`: explicit `race-tests` feature representing the race-instrumented test build.
- `src/lib.rs`: hidden public module export so integration tests can observe the build state.

Stable Rust has no automatic ThreadSanitizer compile cfg corresponding to Go's automatic `race` tag. Rust sanitizer test commands and CI jobs must therefore enable `--features race-tests`; this is the explicit native build mapping, not an omitted branch.

Validation on Rust 1.93.0:

    cargo test race_enabled_matches_the_build_feature
    # 1 passed; RACE_ENABLED is false

    cargo test race_enabled_matches_the_build_feature --features race-tests
    # 1 passed; RACE_ENABLED is true

    cargo test --lib
    # 81 passed; 0 failed

    cargo clippy --all-targets --all-features -- -D warnings
    # passed, including race-tests

    cargo fmt --all -- --check
    # passed

    git diff --check
    # passed

No real-cluster validation applies; the full package contract is a compile-time boolean used exclusively to configure tests.

## Complete package receipt: `util/intest`

Source pin: `client-go@52c1e76cec993571493c81de442bcbef90cdc106`.

The complete production/build inventory is `util/intest/in_unittest.go`, selected by the `intest` build tag and initializing mutable `InTest = true`, and `util/intest/not_in_unittest.go`, selected by `!intest` and initializing it to false (20 lines each). There is no `doc.go`, colocated test, generated input/output, fixture, or package-specific build file. Consumers were inventoried in `tikv/kv.go`, `txnkv/transaction/txn.go`, `txnkv/txnlock/lock_resolver.go`, `txnkv/txnsnapshot/snapshot.go`, `internal/apicodec/codec_v2.go`, and their integration/test support. Those packages retain separate ledger statuses and will account for each gated behavior in their own receipts.

Rust implementation and build integration files are:

- `src/intest.rs`: `in_test()` and `set_in_test()` over an `AtomicBool`, initialized at compile time and mutable at runtime like the Go variable without unsafe concurrent access.
- `Cargo.toml`: explicit `internal-tests` feature representing the Go `intest` build tag.
- `src/lib.rs`: hidden public module export so unit and integration consumers can observe or override the state.

Validation on Rust 1.93.0:

    cargo test in_test_matches_the_build_feature_and_remains_mutable
    # 1 passed; initial state false; mutation and reset passed

    cargo test in_test_matches_the_build_feature_and_remains_mutable --features internal-tests
    # 1 passed; initial state true; mutation and reset passed

    cargo test --lib
    # 82 passed; 0 failed

    cargo clippy --all-targets --all-features -- -D warnings
    # passed, including internal-tests and race-tests

    cargo fmt --all -- --check
    # passed

    git diff --check
    # passed

No real-cluster validation applies; the complete package contract is build-time initialization plus a mutable test-state flag. Consumer-specific assertions remain attached to their owning package receipts rather than being claimed here.

## Complete package receipt: `util/redact`

Source pin: `client-go@52c1e76cec993571493c81de442bcbef90cdc106`.

The complete production inventory is `util/redact/redact.go` (141 lines). It has no `doc.go`, colocated test, platform/build-tag variant, generated input/output, fixture, or package-specific build file. All repository call sites were inventoried across `error`, `tikv`, `internal/apicodec`, `internal/locate`, `internal/logutil`, `internal/mockstore/mocktikv`, `internal/unionstore`, `txnkv/rangetask`, `txnkv/transaction`, `txnkv/txnlock`, and `txnkv/txnsnapshot`. Those consumers retain separate ledger statuses and must adopt the Rust helper before their own package can be complete.

Rust implementation and integration files are:

- `src/redact.rs`: atomic process-wide mode, uppercase hexadecimal key/string and byte output, and conditional protobuf `KeyError` mutation.
- `src/lib.rs`: public `tikv_client::redact` module.

The redactor accounts for every source branch: lock primary/key/all secondaries; write-conflict key/primary; already-exists key; deadlock lock key/deadlock key/non-empty wait-chain keys; commit-ts-expired key; transaction-not-found primary; assertion-failed key; and primary-mismatch lock info. It preserves the source distinction that empty optional key fields remain empty while every secondary entry, including an empty one, becomes `?`.

Go's exported `String` helper is an unsafe zero-copy conversion used only on freshly hex-encoded ASCII inside `Key`; its private `encodeToString` and `toUpperASCIIInplace` helpers have no external repository consumer. Rust folds all three into safe uppercase formatting because arbitrary bytes cannot safely inhabit a Rust `String`. For the only reachable source path, output is byte-for-byte equivalent.

Validation on Rust 1.93.0:

    cargo test redact
    # 3 passed; 0 failed

    cargo test --lib
    # 85 passed; 0 failed

    cargo test --doc
    # 49 passed; 0 failed

    cargo clippy --all-targets --all-features -- -D warnings
    # passed

    cargo fmt --all -- --check
    # passed

    git diff --check
    # passed

The redaction tests are serialized because the source contract is process-global. No real-cluster validation applies; this package is deterministic formatting and protobuf mutation. Repository-wide assurance that no log/error path leaks keys belongs to the consumer package and final differential gates, not this helper receipt.

## Complete package receipt: `util/async`

Source pin: `client-go@52c1e76cec993571493c81de442bcbef90cdc106`.

The complete inventory is production files `util/async/core.go` (83 lines) and `util/async/runloop.go` (158 lines), plus original tests `util/async/core_test.go` (90 lines) and `util/async/runloop_test.go` (164 lines). There is no `doc.go`, build/platform variant, generated input/output, fixture, or package-specific build file. Consumers were inventoried in `internal/client`, `internal/locate`, `internal/mockstore/mocktikv`, `tikv` test support, `txnkv/transaction` test support, and `txnkv/txnsnapshot`. Those packages retain separate integration receipts because Rust's normal transport paths primarily use native futures rather than callbacks.

Rust implementation and integration files are:

- `src/async_util.rs`: `Pool` and `Executor` traits; owned tasks; cloneable exactly-once generic callbacks; reverse-order injected actions; immediate and scheduled fulfillment; run-loop state and queue; optional custom pool; cancellation token; and typed execution errors.
- `src/lib.rs`: hidden public `async_util` module, using a Rust-safe name because `async` is a language keyword.

The native API uses `Option<E>` for Go's nullable error and a `Cancellation` token for `context.Context` cancellation. `RunLoop::execute` returns both the task count and a result so cancellation and concurrent-execution errors preserve Go's `n, err` contract. Unexecuted tasks return to the front of the queue in source order, tasks appended during execution run in a later batch within the same call, an initially empty loop waits, and only one caller may execute a loop.

All original scenarios are represented: reverse injection order; invoke/invoke, schedule/schedule, invoke/schedule, and schedule/invoke exactly-once combinations; default asynchronous spawning and custom pool dispatch; initial wait/wakeup; nested append; delayed work left for a second execution; cancellation while running and waiting; and concurrent execution rejection.

Validation on Rust 1.93.0:

    cargo test async_util
    # 9 passed; 0 failed

    cargo test --lib
    # 94 passed; 0 failed

    cargo test --doc
    # 49 passed; 0 failed

    cargo clippy --all-targets --all-features -- -D warnings
    # passed

    cargo fmt --all -- --check
    # passed

    git diff --check
    # passed

No real-cluster test applies to this in-process scheduling package. Consumer packages must still account for whether their Rust-native future paths make callback adaptation unnecessary or integrate this bridge explicitly.

## Complete package receipt: `internal/kvrpc`

Source pin: `client-go@52c1e76cec993571493c81de442bcbef90cdc106`.

The complete production inventory is `internal/kvrpc/batch.go` (82 lines). It has no `doc.go`, colocated test, build/platform variant, generated input/output, fixture, or package-specific build file. Source consumers are `rawkv/rawkv.go` for size-limited batch put and count-limited batch get/delete, and `tikv/split_region.go` for count-limited split keys. The Rust client has no split-region public API yet, so that consumer remains on the `tikv` ledger row; the complete batching primitive it will use is present here.

Rust implementation and integration files are:

- `src/request/shard.rs`: source-exact size and key-count batching plus boundary tests.
- `src/request/mod.rs`: crate-internal export of key-count batching.
- `src/raw/requests.rs`: raw batch put keeps key/value/TTL tuples aligned; raw batch get and delete now use the count rule and limit 512 rather than byte-size batching; dispatched batch-get request sizes are captured by integration-style mock transport.

Go `Batch` is represented natively as a typed request shard paired with `RegionWithLeader`; key/value/TTL tuple ownership prevents payload misalignment. Go `BatchResult` is represented by the existing typed `Result<Response>` plan stream and merge stages. These are architectural type mappings, not missing data or error branches.

The source's exact thresholds are preserved even where comments imply otherwise. Size batching checks the accumulated size before adding the next item, so a batch may exceed the limit by one item. Key batching checks `count > limit`, so a nominal 512 limit emits 513 keys in the first full batch. Zero limits and the non-empty negative key-limit panic edge are covered. The previous Rust `size + item_size >= limit` behavior was intentionally removed because it split too early.

Validation on Rust 1.93.0:

    cargo test batches
    # 4 passed; 0 failed

    cargo test raw_batch_get_uses_client_go_key_count_boundary
    # 1 passed; dispatched request sizes 513 and 1

    cargo test raw::requests::test::test_raw_batch_put
    # 1 passed; region batches retain aligned key/value/TTL tuples

    cargo test --lib
    # 97 passed; 0 failed

    cargo test --doc
    # 49 passed; 0 failed

    cargo clippy --all-targets --all-features -- -D warnings
    # passed

    cargo fmt --all -- --check
    # passed

    git diff --check
    # passed

No real-cluster validation is required for the package primitive because tests inspect exact batch membership and dispatched generated protobuf requests after region grouping. End-to-end raw and future split-region cluster behavior remains part of those consumer package gates.

## Complete package receipt: `internal/mockstore/cluster`

Source pin: `client-go@52c1e76cec993571493c81de442bcbef90cdc106`.

The complete production inventory is `internal/mockstore/cluster/cluster.go` (65 lines). It defines only the `Cluster` interface and has no `doc.go`, colocated Go test, build/platform variant, generated input/output, fixture, or package-specific build file. Direct consumers are the compile-time implementation assertion in `internal/mockstore/mocktikv/cluster.go` and the public test-support alias in `testutils/mockstore.go`; interface-typed integration fixtures use that alias in the raw, 2PC, async-commit, assertion, delete-range, pipelined-memory-buffer, range-task, and split suites. Concrete cluster state, algorithms, and bootstrap helpers belong to the separate `internal/mockstore/mocktikv` and `testutils` ledger rows and are not claimed here.

Rust implementation and integration files are:

- `src/mock/cluster.rs`: internal test-support `Cluster` trait with every source method: ID allocation; region/leader/bucket/down-peer lookup; store enumeration; scheduled transaction delay; encoded and raw splitting; evenly split key ranges; and store addition/removal.
- `src/mock.rs`: includes the interface in the existing test-only mock subsystem, preserving client-rust's established ownership boundary.

Go pointer results map to `Option` for region, leader, buckets, and raw-split region, while Go slices map to owned or borrowed Rust slices/vectors according to call ownership. The Go `int` split count remains `isize`, including negative values, rather than being narrowed to an unsigned Rust type. Variadic store labels map to an owned vector. The trait takes shared references because concrete simulators may use interior synchronization, matching the source implementation's concurrent test usage. It remains object-safe so fixtures can hold a dynamic cluster control surface like a Go interface.

Validation on Rust 1.93.0:

    cargo test cluster_interface_is_object_safe_and_complete
    # 1 passed; 0 failed

    cargo test --lib
    # 98 passed; 0 failed

    cargo test --doc
    # 49 passed; 0 failed

    cargo clippy --all-targets --all-features -- -D warnings
    # passed

    cargo fmt --all -- --check
    # passed

    git diff --check
    # passed

No real-cluster validation applies to an interface-only test-support package. Its conformance test invokes all nine methods through `dyn Cluster`, proving the complete method surface and object safety. Observable simulator behavior will require mock-store tests before the `internal/mockstore/mocktikv` row can become complete.

## Complete package receipt: `kv`

Source pin: `client-go@52c1e76cec993571493c81de442bcbef90cdc106`.

The complete production inventory is `kv/key.go` (88 lines), `kv/keyflags.go` (279 lines), `kv/kv.go` (264 lines), `kv/store_vars.go` (102 lines), and `kv/variables.go` (92 lines). Original test/support inventory is `kv/key_test.go` (54 lines), `kv/kv_test.go` (91 lines), and `kv/main_test.go` (25 lines). There is no `doc.go`, build/platform variant, generated input/output, fixture, or package-specific build file. Repository consumers were inventoried across retry configuration, `tikvrpc`, region routing, snapshots, transaction locking and buffering, mock stores, metrics, and integration tests; their behavioral adoption remains on those owning ledger rows.

Rust implementation and integration files are:

- `src/kv/key.rs`: next-key, prefix-next, three-way comparison, half-open `KeyRange`, and the existing owned byte-key contract.
- `src/kv/key_flags.rs`: all fourteen source bits, persistent-mask behavior, every query method, all twenty-two ordered flag operations, and the source's fixed power-of-two public operation values.
- `src/kv/types.rs`: pessimistic-lock returned values and context, wait sentinels/defaulting, synchronized result collection and iteration, resource tag/deadlock callbacks, typed observability boundary, value entries and native size accounting, get/batch-get options, and async getter interfaces.
- `src/kv/store_vars.rs`: mutable process-wide store and commit-batch atomics; leader/follower/mixed/learner/prefer-leader modes; exact names, follower classification, byte values, and unknown-value round trips; and access-location modes.
- `src/kv/variables.rs`: shared kill signal, optional higher-priority kill handler, transaction-file controls, source defaults, and default shared variables.
- `src/kv/mod.rs` and `src/lib.rs`: public package and idiomatic root exports.

Rust uses `Vec<u8>` map keys rather than Go's byte-preserving `string` conversion, `Arc<AtomicU32>` rather than raw shared pointers, and one mutex around the returned-value map rather than a separately exposed map and lock. Go's zero `time.Time` maps to `None`; constructors store `Some(SystemTime)`. The `util.LockKeysDetails` field is represented by an object-safe `LockStatistics` marker with `Any` access so this package can carry typed statistics while their structure and merge behavior remain owned by the `util` ledger row. `ErrDeadlock`'s protobuf and retry flag are represented together by `DeadlockError`; final error classification remains on the `error` row. Rust futures provide request cancellation, so getter traits do not duplicate Go's `context.Context` argument.

All original assertions are represented: all-`0xFF` prefix-next keys become empty; get and batch-get options default off and enable commit timestamps; empty/non-empty value-entry cases match; and the leak-check support file is non-applicable because this package starts no task or thread and the complete Rust unit suite terminates normally. Added source-branch tests cover next-key and comparison, every flag implication/inverse, persistent flags, lock-context defaults and result filtering, replica/access byte values and names, process atomics, and variable defaults/shared kill state.

Validation on Rust 1.93.0:

    cargo test 'kv::'
    # 12 passed; 0 failed (7 kv-package parity tests plus 5 already-receipted codec tests)

    cargo test --lib
    # 105 passed; 0 failed

    cargo test --doc
    # 49 passed; 0 failed

    cargo clippy --all-targets --all-features -- -D warnings
    # passed

    cargo fmt --all -- --check
    # passed

    git diff --check
    # passed

No real-cluster validation is required for this foundational type/state package: its own observable contracts are deterministic values, transitions, defaults, formatting, atomics, and synchronized collection behavior. Replica routing, lock protocol requests, statistics merging, kill handling during retries, and snapshot commit timestamps remain mandatory integration gates for their respective consumer packages. The host has no Go toolchain, so original Go tests could not be executed locally; their complete source assertions were transcreated and augmented in Rust.

## Complete package receipt: `internal/unionstore/arena`

Source pin: `client-go@52c1e76cec993571493c81de442bcbef90cdc106`.

The complete inventory is production file `internal/unionstore/arena/arena.go` (429 lines) and original test file `internal/unionstore/arena/arena_test.go` (79 lines). There is no `doc.go`, build/platform variant, generated input/output, external fixture, or package-specific build file. Consumers are the ART, red-black-tree, and union-store memory buffers; those packages retain their own ledger rows and must integrate this primitive before claiming completion.

Rust implementation and integration files are:

- `src/transaction/arena.rs`: block arena, little-endian address/header encoding, compact key handles, checkpoints, truncation/reset, memory hooks, value-log append/read/history/snapshot selection, rollback traversal, current-version inspection, and generic node/database traits.
- `src/transaction/mod.rs`: hidden public native module ownership for future transaction-buffer implementations.
- `src/kv/key_flags.rs`: the completed source-compatible metadata type consumed by value-log inspection.

The Rust API uses stable block indexes and offsets rather than raw pointers, borrowed slices for arena reads, `Arc` callbacks for memory changes, and traits with borrowed node data for value-log callbacks. It preserves source details that are easy to miss: eight-byte aligned node allocation; an allocation limit of 128 MiB including value-log headers; growth to the first power of two strictly greater than the requested size then capping at 128 MiB; null classification when either address half is `u32::MAX`; truncation capacity recomputed from used lengths rather than retained buffer allocations; no hook during enlargement/truncation but a hook after append crosses a block and after reset; zero-length tombstones; end-of-record value addresses; old-value chain selection; rollback without implicit truncation; and inspection that skips superseded versions.

Both original tests are transcreated with their exact 80 MiB, 127 MiB, 4 KiB, 3,000-byte, and over-limit cases. Additional tests cover address round trips and compact handles, little-endian header traversal, alignment, capacity/truncation, empty checkpoints, tombstones, snapshot history, current-version filtering, rollback callbacks, hook counts, reset, and checkpoint ordering.

Validation on Rust 1.93.0:

    cargo test transaction::arena
    # 4 passed; 0 failed

    cargo test --lib
    # 109 passed; 0 failed

    cargo test --doc
    # 49 passed; 0 failed

    cargo clippy --all-targets --all-features -- -D warnings
    # passed

    cargo fmt --all -- --check
    # passed

    git diff --check
    # passed

No real-cluster validation applies because this package is deterministic in-process memory storage with no I/O. The host has no Go toolchain, so the original Go tests could not be executed locally; their exact cases and panic boundary run in Rust. Integration with each concrete union-store index remains on `internal/unionstore/art`, `internal/unionstore/rbt`, and `internal/unionstore`.

## Complete package receipt: `internal/mockstore/deadlock`

Source pin: `client-go@52c1e76cec993571493c81de442bcbef90cdc106`.

The complete inventory is production file `internal/mockstore/deadlock/deadlock.go` (151 lines), behavioral test `internal/mockstore/deadlock/deadlock_test.go` (81 lines), and leak-check harness `internal/mockstore/deadlock/main_test.go` (25 lines). There is no `doc.go`, build/platform variant, generated input/output, external fixture, or package-specific build file. Its only non-package consumer is `internal/mockstore/mocktikv/mvcc_leveldb.go`; wiring the detector into a concrete mock MVCC store remains on that package's ledger row.

Rust implementation and integration files are `src/mock/deadlock.rs`, containing the mutex-protected wait-for graph and tests, and `src/mock.rs`, registering it in existing test support. A native `Result<(), DeadlockError>` represents Go's nullable error. Detection traverses existing outgoing edges while holding the same graph lock, returns the key hash from the existing edge that reaches the source transaction, and registers only accepted edges. Exact transaction/key-hash duplicates collapse while different hashes remain distinct; rejected cycle-closing edges are never inserted; cleanup removes outbound edges only; single-edge cleanup removes the first exact pair and deletes an empty list; and expiry removes map keys strictly below the threshold.

The original complete scenario is transcreated, including the indirect cycle returning `deadlock(200)`, cycle break, same/different-hash behavior, both cleanup forms, and strict expiry boundaries. Added tests cover direct cycles, the pinned source's unusual acceptance of a first self-edge followed by rejection using its existing hash, absent cleanup, and synchronized concurrent duplicate registration. The concurrency test joins every spawned thread, providing the native equivalent of the source leak harness; production code starts no background work.

Validation on Rust 1.93.0:

    cargo test mock::deadlock
    # 3 passed; 0 failed

    cargo test --lib
    # 112 passed; 0 failed

    cargo test --doc
    # 49 passed; 0 failed

    cargo clippy --all-targets --all-features -- -D warnings
    # passed

    cargo fmt --all -- --check
    # passed

    git diff --check
    # passed

No real-cluster validation applies to this deterministic mock-only graph. The host has no Go toolchain, so the original Go test could not run locally; all of its assertions execute in Rust. End-to-end deadlock response behavior remains on `internal/mockstore/mocktikv` and transaction-lock consumer rows.

## Complete package receipt: `trace`

Source pin: `client-go@52c1e76cec993571493c81de442bcbef90cdc106`.

The complete inventory is production files `trace/flags.go` (48 lines) and `trace/trace.go` (150 lines), plus original tests `trace/flags_test.go` (82 lines) and `trace/trace_test.go` (144 lines). There is no `doc.go`, build/platform variant, generated input/output, external fixture, package-specific build file, or leak harness. Consumers were inventoried in region request/cache handling, transaction prewrite/commit, and lock resolution. Those packages remain responsible for attaching trace IDs/control flags to generated request contexts and emitting their complete event sets.

Rust implementation is the public `src/trace.rs` module registered by `src/lib.rs`. It provides exact control bits 0 through 3; idempotent `has`/`with` and bitwise combination; the four source category discriminants; type-preserving structured fields; immutable type-keyed context derivation; nested trace-ID override; and three independently replaceable process-wide handlers for events, category enablement, and control extraction. Handler locks are released before callbacks run, allowing callbacks to reconfigure tracing without deadlock while preserving the source's atomic replacement semantics.

Rust `TraceContext` is the native counterpart of Go `context.Context`: marker types key arbitrary `Send + Sync` values, derivation leaves parents unchanged, and trace IDs are safely owned. `TraceField` stores an arbitrary typed payload behind `Any` rather than narrowing zap fields to strings. `None` handler registration maps to Go's nil registration. Defaults exactly follow implementation and tests: event is no-op, all client categories are disabled, and TiKV request-category control is enabled. This also preserves the pinned source's implementation despite a stale comment claiming the nil extractor returns zero.

All original scenarios are covered: exact non-overlapping bit values; empty, fluent, combined, and idempotent flag operations; default/custom/context-sensitive/reset extractors; immediate-logging convenience behavior; event invocation and nil reset; independent category checks and reset; absent, attached, and nested trace IDs. Global-state tests are serialized.

Validation on Rust 1.93.0:

    cargo test 'trace::tests'
    # 4 passed; 0 failed

    cargo test --lib
    # 116 passed; 0 failed

    cargo test --doc
    # 49 passed; 0 failed

    cargo clippy --all-targets --all-features -- -D warnings
    # passed

    cargo fmt --all -- --check
    # passed

    git diff --check
    # passed

No real-cluster validation applies to this callback/context package. Its generated-request and event-emission effects require consumer integration and remain on `internal/locate`, `txnkv/transaction`, and `txnkv/txnlock`. The host has no Go toolchain, so original Go tests could not run locally; their complete assertions execute in Rust.

## Complete package receipt: `oracle`

Source pin: `client-go@52c1e76cec993571493c81de442bcbef90cdc106`.

The complete package inventory is the single production file `oracle/oracle.go` (157 lines). There is no `doc.go`, colocated Go test or support file, build/platform variant, generated input/output, external fixture, package-specific build file, or leak harness. All repository importers were inventoried: concrete PD, local, and mock implementations live in the separate `oracle/oracles` package, while routing, raw KV, transaction, lock, snapshot, store, RPC, examples, and integration tests consume this package's interfaces, constants, helpers, validator, or errors. Their integration behavior remains on their owning ledger rows.

Rust implementation is the public `src/oracle.rs` module registered by `src/lib.rs`. It defines the complete object-safe `Oracle`, `ReadTimestampValidator`, and `TimestampFuture` contracts; transaction-scope options and the exact `"global"` default-scope constant; dynamically typed implementation errors; the always-successful validator; and distinct future-read and latest-stale-read error types with source-exact fields and messages. Go context cancellation maps to cancellation by dropping the Rust async operation. `std::time::Duration` is the native refresh-interval type; unlike Go's signed duration, it excludes negative intervals at the type boundary, which is compatible with the concrete source implementations rejecting every non-positive interval.

Timestamp helpers preserve the source's 18-bit logical layout, wrapping signed composition, unsigned physical/logical extraction, zero-logical time conversion, and millisecond truncation toward zero on both sides of the Unix epoch. The lower-limit helper shifts the instant before truncation, preserving the otherwise subtle sub-millisecond behavior. Existing `src/timestamp.rs` remains the protobuf `Timestamp`/transaction-version adapter; allocation, low-resolution caching, staleness, expiration, external timestamps, and read validation algorithms remain correctly owned by `oracle/oracles`.

Source-derived tests cover ordinary and wrapping composition, extraction maxima, pre- and post-epoch millisecond truncation, logical-bit clearing, timestamp-to-time conversion, positive and negative lower-limit shifts, noop validation, exact error fields/text, and object safety of both dynamic interfaces.

Validation on Rust 1.93.0:

    cargo test 'oracle::tests'
    # 6 passed; 0 failed

    cargo test --lib
    # 122 passed; 0 failed

    cargo test --doc
    # 49 passed; 0 failed

    cargo clippy --all-targets --all-features -- -D warnings
    # passed

    cargo fmt --all -- --check
    # passed

    git diff --check
    # passed

No real-cluster test applies to this interface-and-deterministic-helper package. Cluster timestamp allocation and validation behavior will be tested with the concrete `oracle/oracles` receipt. The host has no Go toolchain and the source package has no Go test target; Rust validation therefore uses source-derived boundary cases and crate-wide integration gates.

## Complete package receipt: `internal/latch`

Source pin: `client-go@52c1e76cec993571493c81de442bcbef90cdc106`.

The complete inventory is production files `internal/latch/latch.go` (325 lines) and `internal/latch/scheduler.go` (141 lines), behavioral tests `internal/latch/latch_test.go` (163 lines) and `internal/latch/scheduler_test.go` (104 lines), and the leak-check harness `internal/latch/main_test.go` (25 lines). There is no `doc.go`, build/platform variant, generated input/output, external fixture, or package-specific build file. Production consumers were audited at `tikv/kv.go`, `txnkv/client.go`, `txnkv/transaction/2pc.go`, and `txnkv/transaction/txn.go`: the store owns one optional scheduler, configuration enables it with a capacity, and only non-pessimistic commit paths acquire mutation-key latches and publish a successful commit timestamp.

The core Rust implementation is internal module `src/transaction/latch.rs`, registered by `src/transaction/mod.rs`. It preserves source Murmur3-x86-32 slot hashing, power-of-two capacity rounding, bytewise key ordering, per-key ownership and maximum commit timestamps, partial multi-key acquisition, first-matching FIFO wakeups, stale detection both on first acquisition and after wakeup, reverse release, five-entry opportunistic recycling, two-minute expiration, one-minute/50,000-unlock global recycling thresholds, and idempotent close behavior. Owned key buffers naturally provide the source's release-time anti-retention copy guarantee.

The native scheduler uses a mutex-serialized state machine and one-shot async notifications instead of a blocking Go wait group plus background unlock goroutine. It retains unlock and wakeup ordering while avoiding blocking Tokio workers. RAII guards release on every success/error path, and a dropped waiting future removes itself from wait queues, releases any partially acquired keys, and wakes successors; this is the Rust cancellation counterpart absent from the non-context-aware Go `Lock` call.

Integration files are `src/config.rs` and `src/lib.rs` for the disabled-by-default `TxnLocalLatches { enabled: false, capacity: 0 }` option and public builder; `src/transaction/client.rs` for zero-capacity rejection, one shared scheduler across client clones, and propagation to transactions; `src/transaction/transaction.rs` for optimistic-only acquisition before prewrite, source-exact stale rejection, successful commit timestamp publication, and guard lifetime; and `src/common/errors.rs` for `write conflict in latch,startTS: <ts>`. These are required integration slices, not completion claims for the broader `config`, `error`, or `txnkv/transaction` packages. Client-go's local-latch wait histogram remains on the `metrics` and transaction consumer rows.

Both original behavioral scenarios are transcreated: wakeup becomes stale after an earlier commit, first acquisition detects a retained maximum commit timestamp, and all idle entries recycle at the exact expiration boundary. The source's 10-worker/999-transaction stress test is represented by 10 workers and 1,000 transactions with the same unique-key precondition. Additional tests verify known Murmur3 vectors, capacity boundaries, cancellation after partial acquisition, close idempotence, source config defaults/validation, and a consumer-level optimistic commit that fails before any RPC and releases the stale guard for a newer transaction.

Validation on Rust 1.93.0:

    cargo test latch
    # 8 passed; 0 failed

    cargo test --lib
    # 130 passed; 0 failed

    cargo test --doc
    # 49 passed; 0 failed

    cargo clippy --all-targets --all-features -- -D warnings
    # passed

    cargo fmt --all -- --check
    # passed

    git diff --check
    # passed

No real-cluster test is required for the latch package itself: its complete behavior is local scheduling before the first TiKV request, and the consumer regression proves stale rejection occurs without dispatch. End-to-end latency metrics and concurrent cluster commit outcomes remain on their owning `metrics` and `txnkv/transaction` rows. The host has no Go toolchain, so the original Go tests and goleak harness could not run locally; all source assertions run in Rust, and the Rust design has no persistent scheduler task to leak.

## Complete package receipt: `error`

Source pin: `client-go@52c1e76cec993571493c81de442bcbef90cdc106`.

The complete inventory is production file `error/error.go` (429 lines) and behavioral test `error/error_test.go` (68 lines). There is no `doc.go`, build/platform variant, generated input/output, external fixture, package-specific build file, or leak harness. All five production call sites were audited in transaction heartbeat, normal/file commit, and transaction-lock response handling. Those owning packages remain responsible for choosing when to convert a raw key error; generic Rust response extraction correctly retains raw lock/key protobufs needed by lock resolution instead of prematurely applying `ExtractKeyErr` everywhere.

Rust implementation is public module `src/error.rs`, registered by `src/lib.rs`. It provides all 26 source singleton categories with exact text; mismatch-cluster text; query-signal, deadlock, PD, key-exists, write-conflict, local-latch conflict, retryable, transaction/key/entry-size, PD-timeout, legacy GC, transaction-aborted-by-GC, token-limit, assertion, and both LockOnlyIfExists typed errors; error-chain predicates; write-conflict constructors; optional-error logging; and a boxed, downcastable native counterpart of Go's dynamic `error` return. Protobuf-backed wrappers preserve owned generated messages and render compact protobuf-style text rather than Rust debug syntax. GC errors retain idiomatic `SystemTime` fields and format deterministic Go-style UTC timestamps.

`extract_key_error` preserves source precedence: failpoint override, redaction, conflict, retryable text, assertion failure, abort, commit-TS-too-large, transaction-not-found, then unexpected-key-error fallback. Except for the source failpoint mutation and redaction, protobuf fields remain attached to the caller as in Go. Conflict construction increments the Prometheus counter added in `src/stats.rs`; exact global namespace/subsystem/const-label configuration remains correctly assigned to the broader `metrics` package. `src/common/errors.rs` transparently wraps the public local-latch type so high-level transaction callers retain concrete error identity.

The original debug-info test is transcreated byte-for-byte. Because prost messages lack gogo protobuf JSON support, the native serializer explicitly preserves protobuf JSON field names/order, default omission, repeated messages, numeric enums, standard base64 bytes, and every current `MvccLock`, `MvccWrite`, and `MvccValue` field. Redaction clones debug data and replaces only the source-selected key/value fields with `?`, leaving the response untouched.

Additional source-branch tests cover every singleton string, structured error messages, protobuf text rendering, all extraction branches and precedence, retained protobuf payloads, failpoint conflict override, typed downcasts/predicates, metric increments, source-exact local-latch integration, pre/post-epoch time formatting, and unredacted/redacted debug JSON.

Validation on Rust 1.93.0:

    cargo test 'error::tests'
    # 4 passed; 0 failed

    cargo test --lib
    # 134 passed; 0 failed

    cargo test --doc
    # 49 passed; 0 failed

    cargo clippy --all-targets --all-features -- -D warnings
    # passed

    cargo fmt --all -- --check
    # passed

    git diff --check
    # passed

No real-cluster test applies to this deterministic taxonomy/transformation package. Protocol consumers and retry classification remain explicit integration gates on `txnkv/transaction`, `txnkv/txnlock`, `tikvrpc`, and `config/retry`. The host has no Go toolchain; the sole original Go test's exact assertions and all production branches execute in Rust.

## Complete package receipt: `config`

Source pin: `client-go@52c1e76cec993571493c81de442bcbef90cdc106`.

The complete production inventory is `config/client.go` (300 lines), `config/config.go` (229 lines), build-selected `config/nextgen_off.go` and `config/nextgen_on.go` (20 lines each), `config/ruv2.go` (66 lines), and `config/security.go` (106 lines). Behavioral/support inventory is `config/config_test.go` (172 lines), `config/ruv2_test.go` (81 lines), `config/security_test.go` (120 lines), and the goleak harness `config/main_test.go` (27 lines). There is no `doc.go`, generated source/input, package-specific build file, or source fixture. The native TLS validation test creates an ephemeral self-signed certificate/key pair at runtime, avoiding a checked-in private key while retaining successful client-material coverage.

Rust implementation is the public `src/config.rs` module plus `src/config/client_go.rs`. The established native `Config` remains non-exhaustive and retains its Rust transport timeout, decoding-size, path, and keyspace builders while adding every source section and field. `Default` implements all source constructors: top-level, PD, TiKV, async-commit, coprocessor-cache, RU-v2, pessimistic-transaction, security, and disabled local-latch values. Validation preserves source order, boundaries, and text for PD timeout, local latches, connection count, compression, keepalive, and transaction-file chunk/concurrency limits. Go's signed duration domain maps to non-negative `std::time::Duration`; invalid negative source configuration is excluded by the native type boundary. Existing kebab-case serde compatibility is retained, while source-hidden local-latch and coprocessor admission fields remain excluded.

Global configuration uses an atomically lock-protected `Arc<Config>` snapshot. `get_global_config`, `store_global_config`, and `update_global` preserve independent copy/update and exact-pointer restore behavior; the scope helper applies the failpoint override and global fallback. URI parsing preserves case-insensitive `tikv`, comma-separated authorities, user-info exclusion, first query value, form decoding, fragments, and both source error classes. The two Go build files map to Cargo's explicit `nextgen` feature and `NEXT_GEN` constant, validated in both feature states.

`Security::to_tls_config` validates a CA pool, supports CA-only operation, pre-validates a present certificate/key pair including mismatch, and returns the existing native `SecurityManager`. `src/common/security.rs` now supports optional client identity and reloads configured files for each connection, preserving client-go's callback reload behavior; `src/pd/client.rs` consumes either the source-style security section or the pre-existing Rust builder fields. The source's client-side TLS use has no server-certificate callback counterpart in client-rust, so Go's duplicate server-side `GetCertificate` assignment is non-applicable rather than emulated.

RU-v2 weights and `update_tikv_ru_v2_from_exec_details_v2` preserve nil/absent early returns, wrapping RPC-count patching, all seven executor-input counters, raw-counter accumulation/drain, and scaled TiKV RU calculation. `src/util/ru.rs` supplies the concurrent native accumulator needed by this function; that is an explicit integration slice and does not complete the broader `util` package. Generated protobufs were already present and unchanged. All source importers were inventoried across transport, routing, resource control, raw/tikv clients, transaction locking/file/commit, snapshots, examples, and integration tests. Their use of configuration values remains an integration gate on each owning ledger row; this package receipt claims the complete configuration definitions and algorithms, not those consumers.

The six original behavioral tests are transcreated as six focused Rust tests, with additional assertions for every default and validation branch, malformed/duplicate/encoded paths, global restore identity, failpoint fallback, CA-only and invalid TLS material, both build selections, and exact raw/scaled RU values. Rust's global lock and TLS manager create no persistent task, so the Go goleak harness has no native background worker to monitor.

Validation on Rust 1.93.0:

    cargo test config::client_go::tests --all-features --quiet
    # 6 passed; 0 failed

    cargo test config::client_go::tests --no-default-features --quiet
    # 6 passed; 0 failed

    cargo test --lib --all-features --quiet
    # 140 passed; 0 failed

    cargo test --doc --all-features --quiet
    # 49 passed; 0 failed

    cargo clippy --all-targets --all-features -- -D warnings
    # passed

    cargo fmt --all -- --check
    # passed

    git diff --check
    # passed

No real-cluster test applies to the package-owned deterministic defaults, validation, parsing, build selection, or RU arithmetic. Actual TLS handshakes, forwarding, batching, and region refresh are covered by the completed `internal/client`, `internal/locate`, and `tikvrpc` receipts; transaction-file operation and its high-level RU collection remain on transaction consumer rows. The host has no Go toolchain, so the original Go tests and goleak harness could not run locally; every original assertion executes in Rust.

## Complete package receipt: `oracle/oracles`

Source pin: `client-go@52c1e76cec993571493c81de442bcbef90cdc106`.

The complete production inventory is `oracle/oracles/local.go`, `local_external_timestamp.go`, `mock.go`, and `pd.go`. Behavioral/support inventory is `local_test.go`, `pd_test.go`, test-hook file `export_test.go`, and the goleak harness `main_test.go`. There is no `doc.go`, build-tag/platform variant, generated source/input, fixture, or package-specific build artifact. The source package is consumed by the high-level `tikv` client and integration tests; those actual client-construction and cluster paths remain on their owning package rows.

Rust implementation is `src/oracle/oracles.rs`, exposed through `src/oracle.rs`. It provides local and mock oracles with exact physical/logical allocation, expiration, stale-time and externally-monotonic timestamp behavior. `PdOracle` initializes the global scope, maintains monotonically increasing per-scope cache entries, serves stale timestamps, and implements low-resolution refresh, manual configuration, adaptive shrinking/recovery, scope-aware validation singleflight, one-retry protection for timestamps from another PD client, and native dropped-future cancellation safety. `PdClientTimestampSource` connects this behavior to the existing PD client; `src/pd/client.rs`, `src/pd/retry.rs`, and `src/pd/cluster.rs` add the source-required GetMinTS and external-timestamp RPCs. Existing generated PD bindings are sufficient and unchanged.

`src/stats.rs` now records the three source-owned observability side effects: timestamp-future wait duration, validation fetches from PD, and the active low-resolution update interval. The full registration/inventory of client-go's broader `metrics` package remains a separate ledger claim. Go contexts map to Rust future cancellation: canceling a waiter drops only that wait while the shared PD request continues for the other waiters. The test-only empty-PD constructors/hooks map to an injectable `PdTimestampSource`, explicit cache seeding hook, and deferred refresh-loop start. The Rust task test verifies the refresh task finishes after `close`, the native equivalent of the Go goleak assertion.

Focused tests cover every original local and PD scenario: 100,000 distinct local timestamps; expiration/until-expired boundaries; stale timestamp errors; cache monotonicity under 100 concurrent setters; refresh scheduling; adaptive state transitions and manual updates; invalid/latest/future read timestamps; stale-read-only adaptation; shared validation requests; a canceled shared waiter; and the different-client, older-singleflight retry path. No unistore fixture is required: all package-owned PD behavior is represented by deterministic timestamp sources, while real PD RPC interoperability belongs to the transport/client integration packages.

Validation on Rust 1.93.0:

    cargo test oracle::oracles::tests --all-features --quiet
    # 13 passed; 0 failed

    cargo test --lib --all-features --quiet
    # 153 passed; 0 failed

    cargo test --doc --all-features --quiet
    # 49 passed; 0 failed

    cargo clippy --all-targets --all-features -- -D warnings
    # passed

    cargo fmt --all -- --check
    # passed

    git diff --check
    # passed

The host has no Go toolchain, so the original Go test target could not run locally. No real-cluster test is required for the package-owned cache/allocation/validation state machine; concrete PD transport compatibility, high-level oracle wiring, and broader metrics registration remain explicit validation work on `internal/client`, `tikv`, and `metrics`.

## Complete package receipt: `internal/unionstore/rbt`

Source pin: `client-go@52c1e76cec993571493c81de442bcbef90cdc106`.

The complete inventory is `internal/unionstore/rbt/rbt.go`, `rbt_arena.go`, `rbt_iterator.go`, and `rbt_snapshot.go`, plus the source-derived test file `rbt_test.go`. There is no `doc.go`, build-tag/platform variant, generated input/output, fixture, package-specific build artifact, or leak harness. The parent `internal/unionstore` package and its consumers remain separate atomic claims: this receipt supplies its RBT index only, not buffer composition or ART selection.

Rust implementation is the crate-private `src/transaction/rbt.rs`, registered from `src/transaction/mod.rs`. A safe `BTreeMap` maps the source's arena-backed red-black tree while retaining ordered keys, source-size accounting, staging, value-log history, checkpoints, snapshots, persistent-key-flag behavior, bounds, handles, transaction limits, dirty state, cache counters, and memory-footprint hooks. `DiscardValues` invalidates value reads without changing logical size/entry visibility; handle value reads also fail thereafter. Equal-sized values written after the active stage checkpoint overwrite that stage's logical value-log entry, so checkpoint rollback and value-history selection have the same source behavior.

Arena layout, capacity, raw addresses, and Go's mutable live iterator are deliberately native mappings: `BTreeMap` removes unsafe arena representation; `memory_footprint` reports native payload allocation rather than Go arena capacity; and iterators own a stable copied traversal view rather than becoming invalid after a write. The native `update_flags(key, ops)` operation replaces Go's mutation through a borrow-bound iterator. These mappings preserve all parent-facing ordered/value/flag semantics without exposing unsafe or lifetime-invalid Rust APIs.

The seven Rust tests transcreate every original Go test assertion, including 10,000-key staging cleanup, forward and reverse traversal, empty-buffer seeks, rollback-vs-persistent key flags, flags-only iteration and flag updates that create missing keys. They additionally cover snapshot visibility at the root-stage checkpoint, value-history overwrite rules, checkpoint revert, stage inspection, bounds, reverse bounds, handles, tombstones, entry/buffer limits, memory hooks, cache statistics, discard behavior, and the native flag-update mapping. No unistore fixture is required because this package is deterministic in-process storage.

Validation on `nightly-2026-08-22`:

    rustc --version
    # rustc 1.100.0-nightly (c656540d6 2026-08-21)

    cargo fmt --all --check
    # passed

    cargo test transaction::rbt::tests --all-features --quiet
    # 7 passed; 0 failed

    cargo test --doc --all-features --quiet
    # 49 passed; 0 failed

    cargo clippy --lib --all-features -- -D warnings -A clippy::redundant_field_names -A clippy::chunks_exact_to_as_chunks
    # passed; the two exclusions are pre-existing non-RBT findings in request/transaction/store code

    git diff --check
    # passed

Subsequent nightly crate-wide validation passes all 167 library tests. The host has no Go toolchain, so the original Go tests could not run locally; their complete in-process coverage is transcreated above.

## Complete package receipt: `internal/unionstore/art`

Source pin: `client-go@52c1e76cec993571493c81de442bcbef90cdc106`.

The complete inventory is `internal/unionstore/art/art.go`, `art_arena.go`, `art_node.go`, `art_iterator.go`, and `art_snapshot.go`, plus behavioral/support artifacts `art_test.go`, `art_node_test.go`, `art_iterator_test.go`, and `art_snapshot_test.go`. There is no `doc.go`, build-tag/platform variant, generated source/input, fixture, package-specific build artifact, or leak harness. The parent `internal/unionstore` package and all transaction-buffer selection/consumption remain separate atomic claims.

Rust implementation is the crate-private `src/transaction/art.rs`, registered from `src/transaction/mod.rs`. A safe `BTreeMap` maps the observable ART contract: ordered byte keys, logical size, value-log history and equal-size in-place overwrites, nested staging/checkpoint/revert, persistent-vs-rollbackable flags, durable key handles, limits, dirty state, cache statistics, discard behavior, memory hooks, snapshots, and iterator bounds. Logical size is incrementally maintained, preserving the source's constant-time buffer-limit checks for the 100,000-key workload; the observable live length is derived from non-deleted entries, preventing stale count state after nested rollback.

The ART node classes (4/16/48/256), prefix compression, bitmap child indexes, raw arena address reuse, and allocator free lists are a deliberate non-applicable representation mapping: Rust's ordered map supplies the same key ordering and prefix behavior without unsafe manual storage. Node/arena test artifacts are covered by capacity-boundary key sequences, all-byte key order, empty/prefix/long-common-prefix searches, handle lifetime, and 100,000 decimal-key retrieval rather than reproducing internal allocation topology. Source's snapshot allocator reference count maps to owned immutable snapshot data; cloned snapshot iterators therefore remain valid and shareable after writes, with no unsafe-node reuse to defer.

Ordinary ART iterators retain the source's write-sequence invalidation contract: every successful buffer-changing write, release, cleanup, checkpoint revert, and reset invalidates pre-existing iterators. Snapshot iterators deliberately retain a stable immutable view. The native `update_flags(key, ops)` method maps source mutation through a live iterator: it requires an existing live key and preserves the source's non-rollbackable flag behavior without holding an invalid Rust mutable map borrow. Empty ART bounds are normalized as unbounded, matching the source's `len(bound) == 0` convention.

Seven source-derived Rust tests cover all source behavioral/test categories: all 256 byte keys and 4/16/48/256 capacity boundaries with forward/reverse bounds and finished-iterator errors; short/long common prefixes; flags, flags-only keys, durable handles, discard panics, and flag mutation; persistent cleanup and non-persistent rollback clearing; checkpoints/history; iterator invalidation; stable concurrent snapshot iterators; limits, hooks, cache counters, stage inspection, reset; and the original 100,000-key decimal workload. No unistore fixture is needed because the package is deterministic in-process storage.

Validation on `nightly-2026-08-22`:

    cargo fmt --all --check
    # passed

    cargo test transaction::art::tests --all-features --quiet
    # 7 passed; 0 failed

    cargo test transaction::art::tests::hundred_thousand_decimal_keys_and_long_common_prefixes_are_retrievable --all-features --quiet
    # 1 passed; 0 failed; finished in 0.77s

    cargo test --lib --all-features --quiet
    # 167 passed; 0 failed

    cargo test --doc --all-features --quiet
    # 49 passed; 0 failed

    cargo clippy --lib --all-features -- -D warnings -A clippy::redundant_field_names -A clippy::chunks_exact_to_as_chunks
    # passed; exclusions are pre-existing non-ART style findings

    git diff --check
    # passed

The host has no Go toolchain, so original Go tests could not run locally. No real-cluster validation applies to this deterministic index implementation; union-store composition and real transaction behavior remain on their owning package rows.

## Complete package receipt: `internal/unionstore`

Source pin: `client-go@52c1e76cec993571493c81de442bcbef90cdc106`.

The complete production inventory is `memdb.go`, `memdb_art.go`, `memdb_rbt.go`, `membuffer_snapshot.go`, `mock.go`, `pipelined_memdb.go`, `union_iter.go`, and `union_store.go`. Test/support inventory is `main_test.go`, `memdb_test.go`, `memdb_norace_test.go`, `pipelined_memdb_test.go`, `union_store_test.go`, and `memdb_bench_test.go`; `OWNERS` has no runtime behavior. There is no `doc.go`, build-tag/platform variant, generated source/input, fixture, or package build artifact. The child `arena`, `art`, and `rbt` source packages have their own complete receipts; this receipt composes their public parent-facing behavior only.

Rust implementation is crate-private `src/transaction/unionstore.rs`, registered by `src/transaction/mod.rs`, with the pipelined worker's three source metrics recorded through `src/stats.rs`. It supplies ART and optional RBT MemBuffer adapters, source-value `ValueEntry`/`ReturnCommitTs` behavior, persistent flags, stages, checkpoints, history queries, snapshots, batched scans, union merge iteration, mock snapshot timestamps, and the full pipelined mutable/flushing/remote generation machine. A typed `PipelinedError::KeyExists` obtains its value from the immutable flushing buffer just as `handleAlreadyExistErr` does. The pipelined worker also observes generation length, size, and duration using the source histogram boundaries.

Rust ownership replaces Go's wrapper `RWMutex`, `RLock`/`RUnlock`, and `setSkipMutex`: mutable operations require `&mut self`, while owned immutable snapshots and atomic stage-0 sequence validation permit safe reads without unsafe iterator/allocator access. `ForEachInSnapshotRange` retains the source's direct immutable traversal behavior; getters and batched snapshots retain its invalidation check. Explicit iterator `Close` is a no-op because Rust views release resources through ownership/`Drop`. `PipelinedMemDb::get_memdb` preserves the source panic; a missing flush callback is unrepresentable because construction requires a closure. Safe native containers report payload memory footprint rather than Go arena capacity.

The 17 Rust tests account for all source test categories: deterministic set/get/delete, bounds, forward/reverse union merging, flags, stages, checkpoints, reset/history, snapshots and batched snapshot edge cases, hooks/cache metrics/limits, source mock commit-timestamp options, all unsupported pipelined errors and panics, flush thresholds/skipping/blocking/generations/cache/read precedence/typed duplicate-key failure, and worker scheduling. `memdb_norace_test.go` is transcreated by the 4,000-operation mixed staging oracle, 51,712-write nested-stage workload, and 50,000-operation ART-vs-RBT source-scale differential. Benchmarks are non-functional evidence and map to those deterministic scale regressions rather than unstable microbenchmark numbers. `main_test.go`'s Go-specific goleak wrapper is non-applicable to Rust's test harness; every native test that creates a blocking worker waits for it through `flush_wait`.

The crate-private core intentionally is not yet substituted for the existing public `transaction::buffer` path. In client-go that consumer belongs to the separate `txnkv/transaction` package; its integration, public async API shape, real TiKV differential coverage, and transaction semantics remain exclusively on that ledger row rather than creating a partial cross-package claim here.

Validation on `nightly-2026-08-22`:

    cargo fmt --check
    # passed

    cargo test -p tikv-client transaction::unionstore::tests --all-features --quiet
    # 17 passed; 0 failed

    cargo test -p unistore --quiet
    # 2 passed; 0 failed

    cargo clippy -p tikv-client --all-features --lib --tests -- -D warnings -A clippy::redundant-field-names -A clippy::chunks-exact-to-as-chunks
    # passed; exclusions are pre-existing findings outside unionstore

    git diff --check
    # passed

No real cluster is required for this internal deterministic buffer package. Original Go tests were not run in this validation pass; their source paths and behavior are fully audited and transcreated above. The independent `unistore` crate is seed-only TiDB test substrate work and is not part of this package-complete claim.

## Complete package receipt: `txnkv/transaction`

Source pin: `client-go@52c1e76cec993571493c81de442bcbef90cdc106`.

The atomic inventory is ten production files, two always-built support files, and four test files, totaling 16 artifacts and 11,766 lines. Exact paths, line counts, SHA-256 identities, per-file production decisions, all 33 original test declarations, support/probe mapping, generated-input decision, and every direct consumer are recorded in [`txnkv-transaction-source-artifact-audit.md`](txnkv-transaction-source-artifact-audit.md). There is no package-local `doc.go`, build/platform variant, generated source/input, fixture, benchmark/example, or build artifact.

Rust now implements the complete package state machine: local/snapshot batch reads; optimistic and pessimistic writes; exclusive/shared/aggressive locks; source wait/deadlock/return-value semantics; typed mutation assertions and lazy constraints; 2PC prewrite/commit/cleanup; async commit and 1PC eligibility/fallback; lock resolution and no-resolve write conflicts; binlog, schema, filter, callback, resource, request-source, disk-full, memory, timestamp-wait, and heartbeat behavior; proactive region splitting; pipelined generations/throttling/resolve/broadcast; and the full transaction-file serialization, upload, TLS/HTTP pool, split, retry, action, ambiguity, cleanup, and idle-close lifecycle. Public commit/rollback attempts are terminal, including failure; detached cleanup retains client/start-timestamp state and lifecycle hooks.

The completed `internal/unionstore`, `internal/latch`, `internal/locate`, `internal/client`, `internal/apicodec`, `tikvrpc`, `config/retry`, error, config, and KV packages supply this package's prerequisites. The reusable `unistore` crate remains optional test substrate and is already exercised by the unionstore dependency. Source ART `RemoveFromBuffer` is intentionally unsupported and panics in both implementations; production transaction code never calls it.

Native ownership replaces exposed unionstore/committer pointers, Go contexts/goroutines/wait groups, callback-style async probes, and dynamic mutation containers with encapsulated buffers, typed futures/tasks, lifecycle hooks, deterministic mock dispatch, and owned mutation vectors. These are integration decisions rather than missing behavior. Incomplete `txnkv`, `txnkv/txnlock`, `txnkv/txnsnapshot`, root `tikv`, and integration rows are not promoted by this receipt.

Final validation on `nightly-2026-08-22`: the complete transaction module suite passes 198 tests; default and all-feature library suites each pass 585 tests; doctests pass 50 tests; all-target/all-feature checking, Clippy, rustdoc, rustfmt, and `git diff --check` pass with only the repository's existing warnings; and mechanical comparison finds 33 pinned source test declarations and 33 recorded names with no missing or extra entry. The host has no Go toolchain, so pinned Go tests cannot run locally. The four original package test files require no live cluster; cross-client real-TiKV differential validation remains a final high-level repository gate.

## In-progress amendment: `internal/client` BatchCommands

The `internal/client` row above is amended as of 2026-08-23: streams sharing one selected pool slot now share a reconnection gate held through the source-mapped `BO_TIKV_RPC` retry loop. This maps client-go's connection epoch and prevents direct and forwarded streams from racing independent transport recovery. The remaining batch lifecycle work is configuration/policy-driven async collection, timing/load metrics, and typed request-path integration; the package remains in progress.

The `internal/client` row is further amended: a `BatchCommandSubmission` retains the source-equivalent pre-selection cancellation marker for its full Rust-future lifetime. Dropping an awaiting dispatch future now makes the entry cancelled before `build_with_limit`, so it is settled/skipped without spending a request ID; after publication, the response registry remains the sole retirement owner. Validation: `cargo +nightly-2026-08-22 fmt --all -- --check`, `cargo +nightly-2026-08-22 test -p tikv-client --lib store::batch` (18 passed), and `cargo +nightly-2026-08-22 test -p tikv-client --lib store::client` (5 passed). This is incremental in-progress evidence, not an `internal/client` receipt.

The `internal/client` row is further amended: positive `BatchCommandsResponse.transport_layer_load` values are now recorded by each stream receive supervisor and read by the next worker collection pass for Go-compatible overload batching. The focused stream-failure regression verifies a response load of 42 reaches the shared atomic state. HealthFeedback event-listener dispatch, request-stage metrics, and remaining client lifecycle work are still incomplete; no package receipt is claimed.

The `internal/client` row is further amended: `KvRpcClient` now owns a replaceable shared `ClientEventListener`; BatchCommands receive loops invoke it for attached HealthFeedback before response demultiplexing. This preserves source replacement and ordering semantics, but is still incremental evidence rather than a package receipt.

The `internal/client` connection-pool audit is now explicit: Rust already reuses address-keyed clients through `PdRpcClient.kv_client_cache`, so it does have the basic target registry. Client-go's versioned `RPCClient.connPools` lifecycle (`Close`, `CloseAddr`, `CloseAddrVer`, idle recycling) remains absent: Rust invalidation simply removes an entry and has no source-compatible version/close contract. Future work must extend the existing cache boundary before these source components can be claimed.

The versioned-close prerequisite is explicit: Rust has no `ErrConn` equivalent carrying target address and connection version, while source retry calls `CloseAddrVer` from that typed error. Generic gRPC errors must remain generic; a typed carrier plus retry extraction is required before a cache-version implementation can be claimed.

The `internal/client` row is further amended: Rust now carries `Error::Connection { address, version, source }` from `KvRpcClient` dispatch and uses it to run `PdRpcClient`'s source-shaped compare-close operation. Per-address generations persist across removals; stale versions cannot evict a newer cached client, and lifecycle serialization gives concurrent lookups one pool creation. The focused pinned-nightly regressions cover typed transport identity, stale-generation non-eviction, and singleflight creation. Client-wide close/idle recycling, monitor state/metrics, callback-executor APIs, and plan-level forwarded-host selection remain incomplete, so this is not a package receipt.

The `internal/client` row is further amended: explicit cache-pool retirement now shuts down its shared `BatchCommandsWorker`. It drains queued work, fails published response slots, and rejects post-close submissions with `batch client closed`, while ordinary stream recovery still retires only its forwarding host. The focused close regression passes; this is incremental lifecycle evidence, not a package receipt.

The `internal/client` row is further amended: `PdRpcClient::close` now owns all-cache shutdown. It rejects later connections, drains all address entries, and invokes each underlying pool close once; the focused regression verifies idempotence and post-close rejection. Idle recycle/monitor semantics and the remaining async/forwarding APIs are still incomplete, so no package receipt is claimed.

The `internal/client` row is further amended: the BatchCommands worker observes the source three-minute idle timeout, retires its shared pool, and rejects the next submission. `KvRpcClient` connection identity is shared across pre-existing worker clones and the later cache-generation assignment, preventing a construction-order lifecycle split. Focused idle and shared-identity regressions pass. Tonic connectivity-state monitoring remains unavailable, and callback/forwarding work remains incomplete.

The `internal/client` row is further amended: source BatchCommands request-stage timing is integrated through owned entry telemetry and pending-response retirement. It records batch/send/receive/done stages with source outcomes and peer store-ID labels, stamps `client_send_time_ns`, and preserves source response-before-send normalization. Rust's Prometheus API has no summary vector, so this maps the source summary to a histogram vector with the same metric name and labels. The focused boundary regression, real Tonic timestamp assertion, and full library suite pass; tail/unavailability metrics, diagnostics, callbacks, and forwarding remain incomplete.

The row is further amended on 2026-08-24 with three formerly open production artifacts. `client_collapse.go` now runs through one process-wide group inside `KvRpcClient`: only full-region ResolveLock requests collapse, the key is exactly region ID/start version/async mode (not commit version or txn-file), the shared physical RPC uses `ReadTimeoutShort` (30 seconds), each caller keeps its own timeout, and a timed-out caller does not cancel the physical request for another waiter. Source-derived transport coverage proves two independent clients share one identical request while async, lite-key, and batch-transaction-info requests remain separate. `client.go`'s CoprocessorStream, BatchCoprocessor, and EstablishMPPConnection branches now have typed native wrappers that attach forwarding metadata, eagerly receive the first stream item before returning, retain a per-`Recv` timeout, and cancel by close/drop. Finally, `conn_monitor.go` is mapped without inventing a hidden Tonic API: the same five-state gauge is updated at the pool's owned transitions instead of grpc-go's one-second `GetState` poll, and removal clears every state label. Focused tests prove Idle/Ready/TransientFailure states, close-time clearing, and first-response streaming. Both default and all-feature pinned-nightly library suites pass 458 tests. Exact execution-detail OpenTracing construction and complete `tikvrpc` dynamic stream/API-codec integration remain the two package gates; no complete receipt is claimed yet.

The `internal/client` execution-detail tracing gate is now implemented through a native task-scoped sink, the Rust async equivalent of client-go's context opt-in. `ExecutionDetailSpan` reproduces the source `spanInfo` names, declared-duration formatter, zero-duration calculation, child-before-parent finish order, legacy millisecond conversion, RocksDB-read suppression for writes, and asynchronous PersistLog offset semantics. Every generated response type that exposes `ExecDetailsV2`, plus the first CoprocessorStream response, participates. Unary and BatchCommands physical dispatches record from their source-shaped start boundary; `Dispatch` captures the sink so sharded/retry tasks can re-enter the caller's scope. Disabled tracing checks the task-local scope before response inspection or allocation. The complete source-derived tree/timeline table and a real BatchCommands integration regression pass; both default and all-feature pinned-nightly library suites pass 461 tests. Dynamic stream-command/API-codec integration remains open, so the package status stays `in-progress`.

The same in-progress receipt now covers the remaining source transport defaults and BatchCommands failure supervisors. TiKV pool creation uses client-go's fixed five-second dial deadline independently of Rust's per-request timeout, and the default receive limit is `math.MaxInt64 - 1` rather than Tonic's former 4 MiB default. A receive-loop panic retires the affected host's pending requests, increments `tikv_client_go_panic_total{type="batch-recv-loop"}`, and re-enters stream recreation; a send-loop panic increments the paired `batch-send-loop` series and restarts collection. A selected non-batch connection already in transient failure increments the source address/store counter before its next send. Under the source default unlimited concurrency setting, a stream locked for recreation now fails new work immediately with `no available connections`; an explicitly finite limit retains the source wait-until-deadline behavior.

All `conn_batch.go` send-side observations are now emitted at their source boundaries: pending and sent batch sizes, adaptive best size, head-arrival interval, extra fetched requests, overload waits, wait-head/wait-more/send durations, 20-ms send tails, connection-establish waits, recycle duration, and no-available-connection count. Existing receive-side and request-stage metric names were corrected to the full `tikv_client_go_*` namespace. Go summaries configure no quantiles, so Rust again preserves count/sum and labels with histograms while exposing additional buckets. Focused BatchCommands tests pass 28 tests; both default and all-feature pinned-nightly library suites pass 473 tests. The package remains `in-progress` until the complete per-test/support-artifact mapping and the shared typed-command/API-codec integration decision are recorded and reviewed.

## In-progress amendment: `rawkv` retry ownership

Client-go `rawkv/rawkv.go` creates `retry.NewBackofferWithVars(ctx, 20000, nil)` for each normal request and scan page. Its BatchGet/BatchDelete/BatchPut fan-out forks a parent, forks every region batch, cancels siblings on first error, waits for every child, then merges the last completed child's accounting into the operation backoffer. Rust now maps that topology through an owned `RegionRetryState` in `RetryableMultiRegion`: normal Rust callers keep their legacy `Backoff`, but RawKV constructs a fresh 20,000-ms `RetryBackoffer` for every multi-region operation. The state is forked once for the batch parent and once per shard, first terminal failure cancels its sibling retry waits, and the final child is merged only after it finishes. Scan uses a fresh source backoffer for each region page, as Go's `sendReq` does. The unsupported Rust-only `RawClient::with_backoff` attempt-count override is explicitly deprecated rather than silently treated as source behavior.

Focused pinned-nightly tests cover a RawGet region-miss retry, deterministic per-shard fork/final-child merge accounting, and cancellation of a sibling already waiting in its cumulative retry. `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet` passes 302 tests, `cargo +nightly-2026-08-22 fmt --all -- --check` and `git diff --check` pass. This was not a `rawkv` completion receipt: at that checkpoint parity still depended on the then-incomplete `internal/locate.RegionRequestSender`; locate is now complete, while the remaining raw package inventory and integration gates are still open.

The in-progress `internal/client` BatchCommands row is further amended: receive supervisors now emit client-go's stream receive/process duration metrics and tail telemetry, labelled by cache target, pool slot, and direct/forwarded path. The source 20-ms recv tail and 10-ms TiKV-send tail thresholds are preserved; stream recovery records client unavailability only after its reopened stream succeeds. Rust represents source SummaryVec metrics with histogram vectors while retaining names, labels, and the source tail histogram buckets. A delayed-stream regression proves both tail observations; the pinned-nightly library suite passes 303 tests. Stream tracked/retired/completed/outdated counters, canceled-entry tail latency, slow/hang diagnostics, callback APIs, and plan-level forwarding selection remain unfinished, so no package receipt is claimed.

The same in-progress row now also owns client-go's per-stream tracked/retired/completed/outdated counters. The metric context is stored with each published pending slot, not reconstructed from a later error: this covers normal response completion, cancelled caller receivers, ambiguous send failure, host-local stream failure, and explicit pool close without cross-charging direct/forwarded siblings. The existing receive summary supplies only the source `outdated` count because absent slots have no stored owner. Counter regressions assert both publication/failed-send and completed/stream-failure accounting; `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet` still passes 303 tests. Canceled-entry tails, slow/hang diagnostics, callback APIs, and plan-level forwarding remain incomplete, so no package receipt is claimed.

The in-progress metrics row also now owns `batch_stream_canceled_entry_tail_latency_seconds`. When a caller has dropped its response receiver after publication, the pending slot records its response-receive-minus-batch-selection duration when TiKV later returns that response; ordinary stream failures do not fabricate this observation. Labels and the source 1-second-to-128-second buckets are preserved. The dropped-receiver regression passes, and the full pinned-nightly library suite now passes 304 tests. Slow/hang diagnostics, callback APIs, and plan-level forwarding remain unfinished, so no package receipt is claimed.

The async API audit is complete as a native mapping: client-go's `SendRequestAsync` callback/executor represents an operation that Rust exposes as the `KvClient::dispatch` future itself. Its drop behavior already owns the source cancellation point before batch selection; the pending registry owns retirement after publication. No detached callback surface is added because it would lose Rust's cancellation ownership. The same amendment adds client-go's periodic BatchCommands diagnostics: 30-second slow and 90-second hanging pending entries are summarized with an oldest ID/wait and source-equivalent unconfirmed counts based on each stream's monotonic received-ID watermark. A five-entry regression reproduces client-go's expected `(slow, slow_unconfirmed, hanging, hanging_unconfirmed) = (5, 3, 4, 2)`. Dynamic forwarding selection and Tonic-unavailable grpc-go connection monitor gauges remain incomplete, so no package receipt is claimed.

The diagnostics lifecycle now also inspects outstanding published entries immediately before an idle or closed BatchCommands worker exits, matching `conn_batch.go`'s final inspection path. The source routing audit is explicit: `ForwardedHost` is derived by `internal/locate` only after a proxy-replica selection, with the physical request sent to the proxy and metadata naming the final target. Rust therefore retains the tested forwarding transport primitive but does not expose an arbitrary plan-level host until that complete routing package owns selection, retry, and proxy invalidation. The focused `store::batch` all-feature suite passes 25 tests; this remains incremental `internal/client` evidence, not a package receipt.

The `config/retry` row is further amended: pre-cancelled and noop Rust backoffs now display the triggering reason verbatim, matching the source return value. Cancellation during an already-started backoff sleep records a zero-duration retry and returns success for that call; only the following call observes the cancelled context. This preserves client-go's retry-class accounting and prevents a native cancellation wrapper from changing caller-visible error text. The scoped retry suite passes 19 tests; all remaining consumer loops must still adopt their source-owned cumulative budgets before a package receipt.

The retry implementation now retains two additional `config/retry` contracts: source `BackOffWeight` only scales a positive budget when it remains within signed `MaxInt32`, and `RetryBackoffer` renders Go's exact total/type diagnostic string. BatchCommands therefore uses the source long-lived `MaxInt32` reconnect budget rather than a Rust-doubled value. Focused retry (21) and BatchCommands (25) suites pass; consumer-wide budget ownership remains the completion gate.

The in-progress `internal/locate` row now has isolated source evidence for `accessmode.go` and `slow_score.go`: Rust preserves enum names, fixed ten-sample window replacement and gradients, initial timeout/score handling, thirty-second slow marking, and compare-and-swap periodic score/time-cost updates. The source B-tree cannot be truthfully separated from the cached-region TTL/version/invalidation model, and proxy forwarding must remain in the complete selector/request sender integration. Validation: `cargo +nightly-2026-08-22 fmt --all --check` and `cargo +nightly-2026-08-22 test -p tikv-client --lib locate --all-features --quiet` (4 passed). No `internal/locate` package receipt is claimed.

The `internal/locate` row additionally maps the ordered-cache stale-insertion boundary from `regionIndexMu.insertRegionToCache`: `RegionCache::add_region` reports acceptance, refuses a delayed lower version or configuration epoch for an existing region ID, preserves the latest-version map on removal, and treats an empty end key as positive infinity while eliminating intersecting cached ranges. The regression proves both stale rejection and max-end replacement. Validation: `cargo +nightly-2026-08-22 test -p tikv-client --lib region_cache --all-features --quiet` (7 passed), `cargo +nightly-2026-08-22 fmt --all --check`, and `git diff --check`. TTL, garbage collection, source store/replica state, proxy selection, and the sender state machine remain incomplete; no package receipt is claimed.

The same in-progress `internal/locate` row now maps region-cache TTL semantics on all existing Rust lookup routes. Each cached region has Go's default 600-second TTL plus `[0,60)` second jitter; expiration is strict (`now > ttl`), and live non-forced entries renew once their TTL reaches the source renewal window. A deterministic regression sets the boundary directly, proves renewal, then proves expiry causes PD read-through. Validation: `cargo +nightly-2026-08-22 test -p tikv-client --lib region_cache --all-features --quiet` (8 passed), `cargo +nightly-2026-08-22 fmt --all --check`, and `git diff --check`. No background GC or store-health `needExpireAfterTTL` input exists yet, and routing/selector/sender behavior remains incomplete; no package receipt is claimed.

The `internal/locate` row now also has standalone `StoreHealthStatus` evidence from `store_cache.go`: client latency, TiKV-supplied scores, source timing gates, stale-score decay, and combined slow-state transitions are represented in `src/locate.rs`. The active feedback-RPC callback/liveness decision is intentionally absent, as the current Rust store cache retains only PD protobuf metadata and cannot own source `Store` lifecycle. Validation: `cargo +nightly-2026-08-22 test -p tikv-client --lib locate --all-features --quiet` (6 passed), `cargo +nightly-2026-08-22 fmt --all --check`, and `git diff --check`. This is seed evidence only; no `internal/locate` receipt is claimed.

The same row now attaches each cached Rust store to a shared `StoreHealthStatus`, matching source `Store.healthStatus` ownership. `RegionCache::record_health_feedback` applies a stream feedback score only to the cached matching store and deliberately ignores an unknown store, while `store_health` supplies the future selector's detail view. Validation: `cargo +nightly-2026-08-22 test -p tikv-client --lib region_cache --all-features --quiet` (9 passed), `cargo +nightly-2026-08-22 fmt --all --check`, and `git diff --check`. Event-listener registration, periodic ticking/active refresh, liveness, and replica-selection consumers remain incomplete; no package receipt is claimed.

The `internal/locate` health-feedback path is now integrated with the source-compatible `internal/client` listener boundary: `PdRpcClient` registers its shared `RegionCache` listener on every concrete production `KvRpcClient`, so BatchCommands feedback is synchronously routed to the matching cached store. The new generic `KvClient` hook is no-op for custom transports; this preserves test/client extensibility without fabricating health behavior. The cache listener regression plus `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet` (316 passed), formatting, and diff checks pass. Background store-health ticks, active feedback requests, store liveness/epochs, selection, proxy routing, and sender lifecycle remain incomplete; no package receipt is claimed.

Cached store health can now be periodically advanced by `RegionCache::tick_store_health`: it snapshots `StoreHealthStatus` handles without holding the store map during updates and invokes the exact source slow-score tick/decay logic. The focused cache regression proves a reported score decays from 40 to 35 after 15 seconds. Validation: `cargo +nightly-2026-08-22 test -p tikv-client --lib region_cache --all-features --quiet` (9 passed), formatting, and diff checks. The real background scheduler cadence, active feedback request, liveness state, and selector use remain incomplete; no package receipt is claimed.

The in-progress cached-store model now retains source liveness values and preserves both liveness and health feedback when PD metadata is refreshed. Source zero-value behavior maps new entries to reachable. The direct regression verifies unreachable state and slow score survive an address metadata update. Validation: `cargo +nightly-2026-08-22 test -p tikv-client --lib region_cache --all-features --quiet` (9 passed), formatting, and diff checks. Active health checks, re-resolve/tombstone epoch handling, replica selection, proxy routing, and request sending remain incomplete; no package receipt is claimed.

The same `internal/locate` row now has source `ReplicaSelectMixedStrategy` scoring evidence in `src/locate.rs`. It models healthy/label/leader/learner/attempt facts as immutable candidate snapshots, returns the exact highest score plus all ties for the future source-random choice, and preserves label precedence over learner-only preference. Validation: `cargo +nightly-2026-08-22 test -p tikv-client --lib locate --all-features --quiet` (8 passed), formatting, and diff checks. It is explicitly not a usable selector: it lacks resolved peer snapshots, source attempt/error flags, random tie choice, request replica-read mutation, forwarding proxy state, and sender integration; no receipt is claimed.

The `internal/locate` row now derives mixed-selector candidate snapshots from actual `RegionCache` state. For each region peer, it retains source-relevant leader/learner identity, requested-label match, cached slow status, liveness, and caller-provided attempt count. The cache test proves the join across two stores including an unreachable learner. Validation: `cargo +nightly-2026-08-22 test -p tikv-client --lib region_cache --all-features --quiet` (10 passed), formatting, and diff checks. Source down-peer/witness filtering, random ties, error flags, request mutation, proxy forwarding, and sender lifecycle remain incomplete; no package receipt is claimed.

The isolated mixed-selector state now also randomizes exact score ties, matching client-go's post-score selection behavior. A unique winner regression proves this returns the scored target without introducing nondeterminism into source fixtures. Validation: `cargo +nightly-2026-08-22 test -p tikv-client --lib locate --all-features --quiet` (8 passed), formatting, and diff checks. Request-path consumption and all source error/proxy state remain incomplete; no receipt is claimed.

The `internal/locate` row now has a source-shaped logical/physical route carrier. `RegionStore.target_peer` supplies request context while `target` remains the physical connection address and `forwarded_host` is set only for proxy forwarding. The default constructor retains leader/direct behavior. The request, raw, transaction, and single-region paths all consume `request_region()` so the later selector can use a follower without mutating cached leader metadata. Validation: `cargo +nightly-2026-08-22 test -p tikv-client --lib store::tests --all-features --quiet` (18 passed) and `cargo +nightly-2026-08-22 test -p tikv-client --lib request:: --all-features --quiet` (41 passed), plus formatting and diff checks. Missing: source selector state machine, labels/stores filtering, stale-read transitions, retry/error effects, store re-resolution, active liveness checks, and a route builder that chooses a non-leader/proxy; no receipt is claimed.

The cache now converts a mixed-selector winner to the original region peer via `RegionCache::select_mixed_replica`. It first ensures peer stores are resolved, uses the existing immutable candidate snapshots and source tie randomization, and returns `None` only when no candidate is eligible. The focused test demonstrates a reachable follower selection in `ReplicaReadType::Follower`. Validation: `cargo +nightly-2026-08-22 test -p tikv-client --lib region_cache --all-features --quiet` (10 passed), formatting, and diff checks. This remains incremental: no source attempt/error state machine, peer-store route construction, request replica-read flags, or proxy forwarding selector is claimed.

`PdRpcClient::map_region_to_route` now consumes a logical target peer and optional physical proxy peer to construct the corresponding `RegionStore`: its client/target are the physical store, its request-context peer is logical, and forwarding metadata is present exactly when the proxy argument exists. The existing leader map path delegates to this direct route builder. Validation: `cargo +nightly-2026-08-22 test -p tikv-client --lib pd::client::test --all-features --quiet` (9 passed), `cargo +nightly-2026-08-22 test -p tikv-client --lib region_cache --all-features --quiet` (10 passed), formatting, and diff checks. The builder is not selected by a complete source replica selector yet; retry/error flags, stale-read request mutation, snapshot configuration, and proxy candidate choice remain incomplete, so no receipt is claimed.

Replica-read intent now survives Rust's existing sharded-plan topology. `ReplicaReadConfig` defaults to leader, is stored in `Dispatch`, copied by shard clones, and is forwarded by retry-plan wrappers into the new `PdClient::map_region_to_store_with_replica` seam. `PdRpcClient` applies the cache-backed mixed selector for non-leader modes, whereas custom clients safely preserve their former direct map default. Context-bearing RPCs set `Context.replica_read` exactly for a selected non-leader; context-free store requests retain no-op behavior. Validation: `cargo +nightly-2026-08-22 test -p tikv-client --lib store::request::tests --all-features --quiet` (4 passed), `cargo +nightly-2026-08-22 test -p tikv-client --lib kv::store_vars::tests --all-features --quiet` (2 passed), `cargo +nightly-2026-08-22 test -p tikv-client --lib request:: --all-features --quiet` (42 passed), and the full `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet` suite (322 passed), plus formatting and diff checks. `Snapshot::set_replica_read`, source selector attempt/error state, store filtering, stale-read mutation, and proxy selection are still incomplete; no receipt is claimed.

Snapshot now exposes `set_replica_read(ReplicaReadType)`, matching the source `KVSnapshot.SetReplicaRead` shape, plus an explicit `set_replica_read_config` for stable selector options. Its backing transaction defaults to leader and injects the selected config into every common read-plan helper; ordinary write/heartbeat construction remains explicitly leader-only. Validation: `cargo +nightly-2026-08-22 test -p tikv-client --lib transaction::transaction::tests --all-features --quiet` (12 passed), `cargo +nightly-2026-08-22 test -p tikv-client --lib request:: --all-features --quiet` (42 passed), and the full `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet` suite (323 passed), formatting, and diff checks. Source stale-read behavior, request-specific selection adjustment/seed, label/store-set APIs, retry/error state, and proxy candidate choice remain incomplete; no receipt is claimed.

Added stale-read first-attempt plumbing only. A stale config maps through mixed selection and carries `RegionStore.stale_read`; context-bearing request types set `stale_read=true` and suppress normal `replica_read`, matching `EnableStaleWithMixedReplicaRead`. Snapshot exposes this as `set_stale_read`. Validation: `cargo +nightly-2026-08-22 test -p tikv-client --lib store::request::tests --all-features --quiet` (4 passed), `cargo +nightly-2026-08-22 test -p tikv-client --lib store::tests --all-features --quiet` (18 passed), `cargo +nightly-2026-08-22 test -p tikv-client --lib region_cache --all-features --quiet` (10 passed), and the full `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet` suite (323 passed), formatting, and diff checks. Source lock-triggered stale disabling, second-attempt leader fallback, error-state retry selection, read seed/adjuster, and proxy choice remain incomplete; no receipt is claimed.

`Config.enable_forwarding` now reaches normal leader routing. For a cached leader in `unreachable` state, `RegionCache::proxy_for_unreachable_leader` chooses a reachable non-leader and `PdRpcClient` routes physically to that proxy while preserving the leader peer/address as the logical forwarding target. `unknown` liveness remains non-forwarding. Validation: `cargo +nightly-2026-08-22 test -p tikv-client --lib region_cache --all-features --quiet` (10 passed), `cargo +nightly-2026-08-22 test -p tikv-client --lib pd::client::test --all-features --quiet` (9 passed), and the full `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet` suite (323 passed), formatting, and diff checks. Missing source behavior includes proxy cache/rotation and attempt state, unknown-liveness fallback/probing, stale and error-driven transitions, active health checks, and all package completion artifacts; no receipt is claimed.

Async `Snapshot` and `SyncSnapshot` now expose source-shaped `set_match_store_labels`, replacing only the configured label constraints and retaining the selected read/stale modes. Existing cache scoring consumes those labels jointly with store-ID preference. Validation: `cargo +nightly-2026-08-22 test -p tikv-client --lib transaction::transaction::tests --all-features --quiet` (12 passed) and the full `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet` suite (323 passed), plus formatting and diff checks; the attempted `transaction::sync_snapshot` filter selected no tests and is not treated as evidence. Read-replica scope, seed/adjuster/load-threshold settings, stale/error transition state, and complete selector/send behavior remain incomplete; no receipt is claimed.

Prefer-leader routing now carries a crate-private handle to the logical target's `StoreHealthStatus` into `Dispatch`. After each physical dispatch, elapsed time is recorded only when the original config is `ReplicaReadType::PreferLeader`; proxy transport therefore still attributes latency to the leader, as source `rpcCtx.Store` does. Validation: `cargo +nightly-2026-08-22 test -p tikv-client --lib request:: --all-features --quiet` (42 passed), `cargo +nightly-2026-08-22 test -p tikv-client --lib locate --all-features --quiet` (8 passed), and the full `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet` suite (323 passed), formatting, and diff checks. Active feedback request/decay integration, liveness health probe, selector retry/error state, and complete package artifacts remain incomplete; no receipt is claimed.

The in-progress `internal/locate`/request-sender path now retains client-go's region-error retry classification rather than collapsing all recoverable cases into `regionMiss` or returning `ServerIsBusy` terminally. `RegionErrorRetry` distinguishes immediate retries from source retry classes: a busy logical store is marked slow and uses `BoTiKVServerBusy`; disk full, recovery in progress, witness, stale command, max timestamp, region initialization, and scheduling retain their source classes. Flashback, undetermined-result, and raft-entry-too-large errors are terminal. The generic retryable multi-region path and RawKV scan consume this action; the legacy lock loop retains its existing attempt-count scheduler, so full consumer-wide retry ownership is not claimed. Validation: `cargo +nightly-2026-08-22 fmt --check`, focused `request::plan::test::region_error_actions_preserve_client_go_retry_classes` (1 passed), `request::` (43 passed), and the full `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet` suite (324 passed), plus `git diff --check`. Selector-local candidate attempts, data-not-ready flags, busy-leader probing, stale second-attempt fallback, proxy rotation, and active liveness checks remain incomplete; no package receipt is claimed.

Selector-local state is now retained through Rust retry-plan reshards. `Dispatch` owns an opaque `ReplicaSelectorState`; every selected peer increments its attempt count, and `DataIsNotReady` marks only that peer. The cache feeds those facts into mixed selection, where a data-not-ready follower is eligible for exactly one additional attempt as in client-go. A stale read with exactly one recorded attempt maps an untried leader as a normal leader read on the second selector attempt. Validation: `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features locate --quiet` (9 passed), `region_cache` (10 passed), and `request::` (43 passed). Not-leader flags and leader updates are not yet retained as selector state; busy-server suspect-leader probing, request seed/adjuster/load threshold, proxy rotation, and complete sender lifecycle remain incomplete; no package receipt is claimed.

The selector state now includes client-go's zero-wait busy-leader probe. On the second `ServerIsBusy` with `estimated_wait_ms == 0` from the same direct leader under ordinary leader-read mode, the next route chooses a reachable follower and forces leader-read context (`replica_read=false`) so TiKV can provide a `NotLeader` hint. The probe is one logical request state and resets for a new cached leader; source leader-only/stale/non-leader modes and nonzero estimates do not activate it. The state regression and store-route context regression are included in the current suite. Store load estimates/busy thresholds, per-replica server-busy flags, not-leader flags, request read seed/adjuster, proxy rotation, and full sender lifecycle remain incomplete; no package receipt is claimed.

Concrete `NotLeader` hints now transition the internal selector to source leader mode when the hinted peer differs from the failed target and has not already been attempted. The following route resolves that store and requires reachable liveness before it uses `map_leader_route`, including the existing forwarding decision, rather than continuing a mixed/follower configuration. Same-peer and exhausted-hinted-peer cases preserve the former selection path. Validation: `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet` (327 passed). Full not-leader/epoch flags, load thresholds, proxy rotation, and remaining sender lifecycle are incomplete; no package receipt is claimed.

Cached Rust stores now retain the source `storeLoadStats` signal. A nonzero `ServerIsBusy.estimated_wait_ms` records an optimistic queue delay for the logical target store; the cache returns the remaining delay after elapsed monotonic time and ignores unknown stores. `PdRpcClient` owns the update through a defaultable `PdClient` hook. Validation: `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features region_cache --quiet` (11 passed) and focused request region-error action test (1 passed). No configurable `BusyThresholdMs` context, server-busy candidate flag, idle-replica routing, or all-busy leader fallback exists yet; no package receipt is claimed.

The first source `BusyThresholdMs` selection path is now present. A nonzero `ReplicaReadConfig.busy_threshold_ms` is installed in TiKV `Context` through `RegionStore`; when the cached leader's decayed wait exceeds it, the cache selects an unattempted reachable follower whose own estimate is at or below it, retaining source label scoring. Nonzero busy replies persist a selector-local busy-peer flag; if no idle candidate remains, the returned leader route sets and persists a zero threshold for later retries. Validation: source request-context tests (4 passed), `kv::store_vars` (2 passed), and `region_cache` (11 passed), all under pinned nightly. Request-kind gating and complete selector lifecycle remain incomplete; no package receipt is claimed.

`FlashbackInProgress` now follows client-go's replica-read exception: a route with `replica_read=true` transitions its selector state to a threshold-free leader retry without backoff. Other flashback routes remain terminal, as does `FlashbackNotPrepared`. Validation: `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet` (330 passed). Full flashback request orchestration, request-kind gating for busy selection, and complete sender lifecycle remain incomplete; no package receipt is claimed.

Stale-read retries now retain client-go's leader-attempt gate. Once the leader has been attempted and is not selector-busy, a subsequent mixed-selected follower clears `stale_read` and uses normal replica-read context; otherwise stale context remains. Validation: `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features locate --quiet` (14 passed) and full library suite (331 passed). First-attempt label-mismatch conversion, read seed/adjuster, and complete sender lifecycle remain incomplete; no package receipt is claimed.

`Snapshot` and `SyncSnapshot` now expose client-go's load-based replica-read threshold setter. The shared transaction configuration converts a positive `Duration` to TiKV's `u32` millisecond field and disables it for zero or overflow values; the existing request path carries the resulting threshold through selector and protobuf context. The pinned source's `ReplicaReadSeed` is request-wrapper state but has no selector consumer, so no artificial seed policy was added. Validation: `cargo +nightly-2026-08-22 test -p tikv-client --lib transaction::transaction::tests::snapshot_replica_read_configuration_defaults_to_leader_and_is_replaceable --all-features --quiet` (1 passed), formatting, and diff checks. Per-request replica-read adjuster, read-replica scope propagation, and complete sender lifecycle remain incomplete; no package receipt is claimed.

The request sender now preserves client-go's `onRegionNotFound` immediate retry exception. If a response reports `RegionNotFound` while the cached leader remains untried, Rust invalidates the shared cache and marks only the current selector to force that leader after refresh; otherwise it takes the ordinary `BoRegionMiss` path. The state is distinct from flashback because it does not disable `BusyThresholdMs`. Validation: `cargo +nightly-2026-08-22 test -p tikv-client --lib locate --all-features --quiet` (15 passed), `cargo +nightly-2026-08-22 test -p tikv-client --lib request:: --all-features --quiet` (43 passed), formatting, and diff checks. Full sender/cache invalidation lifecycle, active liveness, and package-wide source test inventory remain incomplete; no package receipt is claimed.

Snapshot replica-read adjustment now has a native callback contract matching client-go: `ReplicaReadAdjuster` receives each Get/BatchGet's unresolved key count and returns a read type with an optional single selector option. Labels append, store IDs replace the match-store preference, and leader-only/prefer-leader set their corresponding selector flags; adjustment is per request over a cloned stable config and runs only for follower-mode reads. The Go scan path does not use an adjuster, so Rust scans retain only stable replica settings. Validation: focused `transaction::transaction::tests::snapshot_replica_read_configuration_defaults_to_leader_and_is_replaceable` and `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet` (332 passed), formatting, and diff checks. Read-replica scope transport, active liveness, and complete sender lifecycle remain incomplete; no package receipt is claimed.

Load-based replica routing now retains client-go's command-class gate across `KvRequest`, retry sharding, and `PdClient`: only Get, BatchGet, Scan, and coprocessor request paths authorize busy-threshold follower selection; transactional writes and RawKV commands remain direct. A coprocessor request with nonempty source-style task assignments is additionally excluded from recording a busy replica for redirection. Validation: `request::test::source_load_based_replica_routing_uses_only_read_commands` plus `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet` (333 passed), formatting, and diff checks. Complete coprocessor busy-response propagation, scope transport, active liveness, and full sender lifecycle remain incomplete; no package receipt is claimed.

Stale reads now follow client-go's lock fallback at the retry-plan boundary. When `ResolveLock` observes a lock, it changes its cloned retry plan—not the shared original—to a direct leader read with `stale_read=false` and a zero busy threshold before resolving and reissuing the read. Validation: `request::shard::test::source_lock_on_stale_read_retries_a_threshold_free_leader` and `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet` (334 passed), formatting, and diff checks. Full stale snapshot/scan retry state, active liveness, and complete sender lifecycle remain incomplete; no package receipt is claimed.

Successful forced follower leader-read probes now heal Rust's cached leader as `replicaSelector.onSendSuccess` does. `RegionStore` exposes a promotion candidate only for that force-leader route and a peer distinct from the cached leader; the retry path updates the cache best-effort after the user response is known successful. Validation: `store::tests` (18 passed), `request::` (45 passed), and full pinned-nightly library suite (334 passed), formatting, and diff checks. Source transport-failure liveness probing, proxy cache rotation, active health checks, and complete sender lifecycle remain incomplete; no package receipt is claimed.

`EpochNotMatch.CurrentRegions` now refreshes Rust's region cache rather than merely invalidating it. The sender derives each replacement's provisional leader from the responding store, preserves an exact-version cached entry, otherwise invalidates the old version, and delegates insertion to the existing epoch/overlap-safe cache index. Regressions cover replacement installation/leader seeding and exact-version retention. Validation: `cargo +nightly-2026-08-22 test -p tikv-client source_epoch_not_match_ --lib --all-features --quiet` (2 passed) and `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet` (336 passed), plus formatting and diff checks. Bucket inheritance, TiFlash's electable-store branch, and the complete `internal/locate` receipt remain unclaimed.

`BucketVersionNotMatch` now has the source cache boundary: `RegionWithLeader` carries optional `metapb.Buckets`, sharded context-bearing requests set `Context.buckets_version`, and a mismatch updates only missing/older cache metadata under the cached region ID. `EpochNotMatch` replacement regions inherit the existing bucket hint. The PD path decodes bucket keys for public regions and re-encodes decoded error keys before cache insertion. The sender intentionally propagates this error after updating the cache, because source callers must reschedule bucket-aware work themselves. Validation: focused cache, sender, dispatch-context, and PD-codec regressions plus `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet` (340 passed), formatting, and diff checks. `need_buckets` PD queries, bucket-level range splitting, background version refresh, TiFlash, and the complete `internal/locate` receipt remain unclaimed.

Rust all-store refresh now follows `RegionCache.GetAllStores`: TiKV and TiFlash metadata are cached and surfaced through `PdClient::all_stores`, while tombstone and TiFlash-compute entries are excluded. Validation: `region_cache` (13 passed) and `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet` (341 passed), formatting, and diff checks. TiFlash endpoint selection/routing, peer rotation, and TiFlash-compute reload/invalidation are still absent; no `internal/locate` receipt is claimed.

`RegionWithLeader` now has client-go `KeyLocation` bucket lookup semantics: precise lookup identifies only represented bucket ranges, while region-contained holes in stale metadata fall back to the leading/trailing range; a key outside the region receives no bucket. Precise buckets are clamped to region bounds, including empty-infinity and invalid-clamp fallbacks. The source distinction is intentional: edge-hole fallback preserves the advertised endpoint and is not clamped. Validation: `cargo +nightly-2026-08-22 test -p tikv-client source_bucket_ --lib --all-features --quiet` (5 passed) and the full library suite (344 passed), plus formatting and diff checks. These helpers are not yet consumed to split bucket-aware requests; `need_buckets` PD queries, async update deduplication, and the complete `internal/locate` receipt remain unclaimed.

TiFlash now has its own source-shaped cache selection boundary. A live cached region cycles TiFlash peers with client-go's pre-increment load-balance index, retains the current peer for non-load-balanced work, applies label filtering, and returns structured missing-cache, expired-cache, no-TiFlash-peer, or all-filtered outcomes. `PdRpcClient` resolves the selected peer through the normal connection cache into a `RegionStore`; TiKV replica selection continues to exclude TiFlash peers. Validation: focused `source_tiflash_selection` (1 passed) and `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet` (345 passed), formatting, and diff checks. Source store failure epochs/re-resolution, empty-address and detailed unavailable reasons, pending-peer preference, TiFlash send-failure peer switching, TiFlash-compute lifecycle, and a BatchCop/MPP request-plan consumer remain incomplete; no package receipt is claimed.

PD `GetRegion*` responses now retain `pending_peers` on `RegionWithLeader`. The cache exposes source `GetAllValidTiFlashStores` ordering: the caller's current store remains first, eligible alternative TiFlash stores follow, and a second list excludes pending peers for BatchCop work to prefer caught-up replicas. Validation: focused `source_tiflash_selection` (1 passed), `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet` (345 passed), formatting, and diff checks. The helper has no BatchCop/MPP consumer yet, and source failed-store epochs/re-resolution remain incomplete.

PD `down_peers` are now retained with cached region metadata and excluded before both TiKV replica and TiFlash-only peer selection. The TiFlash all-valid helper likewise never advertises a reported-down alternative. Validation: focused `source_tiflash_selection` (1 passed) and full library validation (345 passed), plus formatting and diff checks. Source witness filtering, store failure epochs/re-resolution, active liveness, and the full `internal/locate` receipt remain incomplete.

Non-leader witness peers are now excluded before TiKV or TiFlash selection, while a witness leader remains routable as client-go's `newRegion` permits. The TiFlash all-valid list also excludes witnesses. Validation: focused `source_tiflash_selection` (1 passed) and full library validation (345 passed), plus formatting and diff checks. Store failure epochs/re-resolution, address/liveness refresh, and the full `internal/locate` receipt remain incomplete.

PD key and region-ID lookups now have explicit `need_buckets` variants. The cache reuses a valid bucket-bearing region and otherwise performs one source-style opt-in PD refresh, retaining the returned bucket metadata; ordinary lookups preserve `need_buckets=false`. Custom retry clients keep their existing result through trait defaults. Validation: focused `source_bucket_aware_pd_lookup` (1 passed) and `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet` (346 passed), plus formatting and diff checks. Bucket-aware range splitting, background update de-duplication, and the complete `internal/locate` receipt remain incomplete.

`RegionCache::update_buckets_if_needed` now maps source asynchronous PD bucket refresh: it refreshes only when the advertised version is newer, coalesces same-region refreshes, and removes the in-flight marker after the request. Validation: focused `source_background_bucket_refresh` (1 passed) and `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet` (347 passed), plus formatting and diff checks. Bucket-aware range splitting and the full `internal/locate` receipt remain incomplete.

After retaining TiFlash stores in discovery, the ordinary replica selector now excludes them from its TiKV access-mode candidate set. The regression includes a region with leader/follower TiKV peers plus a TiFlash peer and confirms only TiKV candidates reach mixed selection. Validation: focused candidate test, `region_cache` (13 passed), and full pinned-nightly library suite (341 passed), formatting, and diff checks. TiFlash endpoint routing and all remaining source cache/sender lifecycle behavior are still incomplete; no receipt is claimed.

Resource-control response accounting now also covers transactional Get, BatchGet, and Scan responses. Get/BatchGet consume V2 scan/time details; Scan uses the encoded response size for both read-byte and response-size accounting, matching `MakeResponseInfo`; Cop responses fall back to legacy `ExecDetails.TimeDetail` when V2 time details are unavailable. Focused pinned-nightly legacy and `nextgen` tests each pass (4 tests); the full `--lib --all-features` suite passes 351 tests, with formatting and diff checks clean. The dynamic request wrapper, bypass/access policy, PD controller interception, and transport metrics remain incomplete; this is still seed evidence only.

`internal/locate` sender seed: source `StoreNotMatch` and `MismatchPeerId` no longer fall through Rust's generic region-miss backoff. They invalidate cache state and end the current send attempt with the received region error, as client-go's `retry == false` path does. Validation: focused `source_store_identity_errors` (1 passed), `request::` (50 passed), and full pinned-nightly `--lib --all-features` (352 passed), plus formatting and diff checks. Store fail epochs/re-resolution, connection closing, active liveness, and the complete atomic package inventory remain incomplete.

`internal/locate` cache seed: a `NotLeader` hinted peer is accepted only if its peer ID and store ID occur in the cached region, matching `replicaSelector.updateLeader`; otherwise the region cache entry is invalidated for PD reload. Validation: focused `region_cache::test::cache_is_used` (1 passed) and full pinned-nightly `--lib --all-features` (352 passed), plus formatting and diff checks. Source selector state, store fail epochs/re-resolution, connection health, metrics, and remaining package/test artifacts remain incomplete.

`internal/locate` sender seed: `RecoveryInProgress` and `IsWitness` now consume their source backoff class but stop the current loop, and `KeyNotInRegion` invalidates then returns immediately. `RegionErrorRetry::TerminalAfterBackoff` prevents the plan recursion from incorrectly hiding those region errors. Validation: focused `source_terminal_region_errors` (1 passed), `request::` (51 passed), and full pinned-nightly `--lib --all-features` (353 passed), plus formatting and diff checks. Complete selector/store lifecycle, metrics, and source test inventory remain incomplete.

`internal/locate` sender seed: an unclassified TiKV region error follows client-go's actual selector path—immediately advance replica selection, with neither cache invalidation nor backoff. An earlier mapping incorrectly used the source non-selector fallback; the focused `source_terminal_region_errors_do_not_retry_the_send_loop` regression now covers the corrected outcome. The Rust lock retry fixture continues to use source-retryable `StaleCommand` rather than an empty error. Formatting, diff checks, and the full pinned-nightly library suite (366 passed) pass. The atomic source package remains incomplete.

`internal/locate` sender seed: retryable region errors now preserve their existing `RegionWithLeader` and re-enter only the sender/replica-selection path. This removes Rust's prior accidental re-sharding after both immediate source outcomes (for example, unknown errors and short configurable-read timeouts) and retryable source backoffs such as `ServerIsBusy`. Validation: the bounded `source_region_error_selector_retries_reuse_the_existing_shard` regression observes one shard and two RPC attempts for both an unknown response and a server-busy response; formatting, diff checks, and the full pinned-nightly library suite (366 passed) pass. The atomic source package remains incomplete.

`internal/locate` sender seed: concrete selector transport failures now retain their `RegionWithLeader` across `BoTiKVRPC`, matching `replicaSelector.onSendFailure`; custom/default PD clients retain legacy cache invalidation and re-sharding. Validation: `test_grpc_error_invalidates_store_cache` drives a failed RPC then success through both paths and observes one region lookup for preservation versus two for invalidation; formatting, diff checks, and the full pinned-nightly library suite (366 passed) pass. The atomic source package remains incomplete.

`internal/locate` sender seed: transport failures now select client-go's `BoTiFlashRPC` class for physical TiFlash and TiFlash-compute destinations, retaining `BoTiKVRPC` for TiKV or an unavailable route. Validation: focused `source_transport_failure_uses_tiflash_retry_class_only_for_tiflash_endpoints` (1 passed), formatting, diff checks, and the full pinned-nightly library suite (367 passed) pass. The atomic source package receipt remains incomplete.

`internal/locate` sender seed: non-selector `FlashbackInProgress` and `FlashbackNotPrepared` responses now preserve client-go's direct field-specific terminal errors rather than exposing a generic region-error wrapper; replica-read forced-leader fallback stays ahead of this terminal mapping. Validation: focused `region_error_actions_preserve_client_go_retry_classes` (1 passed), formatting, diff checks, and the full pinned-nightly library suite (367 passed) pass. The atomic source package receipt remains incomplete.

`internal/locate` sender seed: `RaftEntryTooLarge` now crosses the sender as a direct terminal error rather than a retryable Rust `RegionError`, preserving client-go's boundary and preventing RawKV's outer region-error retry from resubmitting the write. `UndeterminedResult` remains Rust's caller-visible `RegionError` equivalent; RawKV consumes it through its source-matched outer `BoRegionMiss` retry, while ordinary plans expose it to their caller. Validation: focused `region_error_actions_preserve_client_go_retry_classes` (1 passed), formatting, diff checks, and the full pinned-nightly library suite (367 passed) pass. The atomic source package receipt remains incomplete.

`internal/locate` sender seed: a region-error message containing client-go's `invalid max_ts update` sentinel now terminates directly instead of taking Rust's unknown-error replica retry. Validation: focused `region_error_actions_preserve_client_go_retry_classes` (1 passed), formatting, diff checks, and the full pinned-nightly library suite (367 passed) pass. The atomic source package receipt remains incomplete.

`internal/locate` sender seed: `ServerIsBusy` now selects `BoTiFlashServerBusy` for TiFlash and TiFlash-compute physical endpoints, retaining `BoTiKVServerBusy` for TiKV, as client-go does. Validation: focused `source_server_busy_uses_tiflash_retry_class_only_for_tiflash_endpoints` (1 passed), formatting, diff checks, and the full pinned-nightly library suite (368 passed) pass. The atomic source package receipt remains incomplete.

`internal/locate` sender seed: `StoreNotMatch` now also unconditionally retires the logical request-store address after cache invalidation, matching client-go's `CloseAddr(ctx.Addr)` even when PD metadata has already expired. A forwarding proxy's physical transport stays open, and `MismatchPeerId` remains a cache-only stop condition. Validation: focused `source_store_identity_errors_stop_the_current_send_loop` (1 passed), formatting, diff checks, and the full pinned-nightly library suite (368 passed) pass. The atomic source package receipt remains incomplete.

`internal/locate` sender seed: every sender resend now marks the generated TiKV `Context.is_retry_request` flag, while first sends remain unmarked, matching `RegionRequestSender.next`. The implementation covers context-bearing generated requests, coprocessor and RawCoprocessor delegation, and all existing sharding wrappers. Validation: focused `source_region_error_selector_retries_reuse_the_existing_shard` (1 passed, including `[false, true]` wire-flag observation), formatting, diff checks, and the full pinned-nightly library suite (368 passed) pass. The atomic source package receipt remains incomplete.

`internal/locate` sender seed: `EpochNotMatch` now has explicit source outcomes. Empty or normally-installed replacements stop the send loop for caller resplitting; only a cached epoch ahead of TiKV backs off with `BoRegionMiss` and retries. Validation: focused `source_epoch_not_match` (3 passed), `request::` (52 passed), and full pinned-nightly `--lib --all-features` (354 passed), plus formatting and diff checks. The atomic source package remains incomplete.

RawKV/request integration seed: terminal sender region errors are retried only by RawKV's source-owned cumulative `RetryBackoffer` path, which charges `BoRegionMiss`, re-shards, and resends; ordinary request plans keep them caller-visible. Raw scan uses the same outer behavior after its manual sender call. Validation: `raw_get_retries_a_region_miss` (1 passed), `raw_scan_retries_a_terminal_region_error` (1 passed), `raw::client::tests` (20 passed), `first_shard_error_cancels_a_sibling_cumulative_backoff` (1 passed), and full pinned-nightly `--lib --all-features` (355 passed), plus formatting and diff checks. RawKV and `internal/locate` remain incomplete atomic claims.

`internal/locate` selector seed: `ServerIsBusy` now fast-retries source follower/mixed/prefer-leader routes, load-threshold redirects, and forced leader probes; ordinary healthy leader zero-wait busy replies retain `BoTiKVServerBusy`. Validation: focused `source_server_busy_fast_retry` (1 passed), `request::` (53 passed), and full pinned-nightly `--lib --all-features` (356 passed), plus formatting and diff checks. The remaining selector/store lifecycle and complete source package receipt remain incomplete.

`internal/locate` selector seed: source `StaleCommand` replies now fast-retry through replica selection instead of unconditionally consuming `BoStaleCmd`; non-selector classification remains unchanged. The concurrent-backoff fixture uses `MaxTimestampNotSynced` so it still exercises cancellation under the source retry model. Validation: focused selector/cancellation tests (1 each), `request::` (53 passed), and full pinned-nightly `--lib --all-features` (356 passed), plus formatting and diff checks. The atomic package remains incomplete.

`internal/locate` selector seed: a hintless `NotLeader` now marks the failed peer for this selector only; after source scheduling backoff, a leader-read route probes an eligible follower with leader-read context instead of repeating that leader. Validation: focused `source_not_leader_without_hint` (1 passed) and full pinned-nightly `--lib --all-features` (357 passed), plus formatting and diff checks. Selector exhaustion, store epochs/re-resolution, active liveness, and full package/test inventory remain incomplete.

`internal/locate` sender seed: source `ServerIsBusy` responses whose reason contains `deadline is exceeded` now share the short configurable-read-timeout reselection path. Reads below 30 seconds retry selection immediately before busy bookkeeping/backoff; ordinary busy reasons and the 30-second-or-longer boundary retain `BoTiKVServerBusy` handling. Validation: focused `source_configurable_server_busy_timeout_requires_the_source_reason` (1 passed), formatting, diff checks, and the full pinned-nightly library suite (366 passed) pass. The atomic package receipt remains incomplete.

`internal/locate` sender seed: the source's third deadline encoding is now covered: an otherwise-unclassified region-error message containing `Deadline is exceeded` immediately reselects a short configurable read. The predicate explicitly defers to every typed branch client-go checks first, preventing a malformed mixed `ServerIsBusy` response from bypassing its normal handling. Validation: focused `source_configurable_region_error_timeout_requires_the_source_message` (1 passed), formatting, diff checks, and the full pinned-nightly library suite (366 passed) pass. The atomic package receipt remains incomplete.

`txnkv/txnsnapshot` seed: `Snapshot::set_sample_step` and `SyncSnapshot::set_sample_step` now retain client-go `KVSnapshot.SetSampleStep` and copy it into each physical `ScanRequest`, including scan retries. The dispatch-level `source_snapshot_sample_step_reaches_every_scan_request` regression observes the exact protobuf field. Request-context options, persistent key-only mode, scanner batch size, value caching, and the complete source package receipt remain incomplete.

`txnkv/txnsnapshot` seed: `SetNotFillCache`, `SetIsolationLevel`, and `SetTaskID` now have async and synchronous snapshot setters. Their values use the shared plan builder, so every physical Get, BatchGet, and Scan shard/retry carries `Context.not_fill_cache`, `Context.isolation_level`, and `Context.task_id`. The source-derived regression observes all three fields on each request type. Persistent key-only mode, scanner batch size, runtime stats, read-replica scope, caching, and the complete source package receipt remain incomplete.

`txnkv/txnsnapshot` seed: async and synchronous snapshots now expose `SetKeyOnly`. It forces the existing snapshot Scan API to set `ScanRequest.key_only` and prevents the scan buffer from caching returned values; explicit `scan_keys` remains key-only regardless of this setting. The wire regression observes the forced flag. Go's incremental scanner batch-size API, runtime stats, read-replica scope, caching, and the complete source package receipt remain incomplete.

`txnkv/txnsnapshot` seed: `SetPipelined` now records the supplied transaction timestamp in the snapshot read context's resolved-lock set, so normal Rust snapshot reads pass through locks flushed by that same transaction instead of trying to resolve them. The focused state regression covers the source invariant. Rust has no `BatchGetBufferTier` request path, so the source's pipelined buffer read mode remains an explicit gap; configurable read timeout, runtime stats, read-replica scope, caching, and the complete source package receipt also remain incomplete.

`txnkv/txnsnapshot` seed: `SetKVReadTimeout` and its getter now retain a nonzero snapshot deadline (zero clears it). Snapshot Get, BatchGet, and Scan carry the same duration into TiKV `Context.max_execution_duration_ms` and the cloned physical dispatch deadline. `KvRpcClient` applies that deadline to unary tonic requests and per-entry BatchCommands completion; dropping a timed-out batch submission retains Rust's existing cancellation/response-retirement contract. Focused planner and three-read wire regressions cover the carried deadline. Runtime stats, read-replica scope, incremental scanner batching, snapshot cache behavior, pipelined buffer-tier reads, and the complete source package receipt remain incomplete.

`txnkv/txnsnapshot` seed: `SetResourceGroupTag` now accepts an optional static tag on async and synchronous snapshots. `Some`, including an explicitly empty vector, writes `Context.resource_group_tag` on every Get, BatchGet, and Scan shard/retry; `None` preserves client-go's nil-tag state. The three-read context regression covers the wire value. `SetResourceGroupTagger` remains open because it requires a source-style mutable request-wrapper callback; runtime stats, read-replica scope, scanner batching, caching, pipelined buffer-tier reads, and the complete receipt remain incomplete.

`txnkv/txnsnapshot` seed: async and synchronous snapshots now expose source-named `SetIsStalenessReadOnly` aliases. They delegate to the existing stale-read state that already drives client-go-shaped mixed-replica selection and TiKV's stale-read context bit; the existing transaction configuration regression covers the shared state. Scope-aware read validation, resource tagger callbacks, runtime stats, scanner batching, caching, pipelined buffer-tier reads, and the complete receipt remain incomplete.

`txnkv/txnsnapshot` seed: `Snapshot::set_resource_group_tagger` and `SyncSnapshot::set_resource_group_tagger` now retain a resource-tag callback for Get, BatchGet, and Scan. The native callback receives the exact source operation kind and applies only when no static tag is configured, including when the static tag is explicitly empty; the focused regression observes all three callback tags and static-tag precedence. Scope-aware read validation, runtime stats, incremental scanner batching, snapshot caching, pipelined buffer-tier reads, and the complete receipt remain incomplete.

`txnkv/txnsnapshot` seed: `set_txn_scope`/`set_read_replica_scope` now retain client-go's shared scope for snapshot Get and BatchGet. `TransactionClient` owns a PD oracle validator and every physical dispatch, including shards and retries, validates the read timestamp before transport; the final client/transaction reference closes its refresh worker. The source scanner does not copy `readReplicaScope`, so Scan deliberately validates with the default empty oracle option (global scope). Focused regressions observe scopes, stale-read state, and no transport after validation failure. Runtime stats, incremental scanner batching, snapshot caching, pipelined buffer-tier reads, and the complete receipt remain incomplete.

`txnkv/txnsnapshot` seed: corrected `SetKVReadTimeout` default/retry behavior. Snapshot Get uses 30 seconds by default; BatchGet and Scan use 60 seconds. A nonzero configured timeout applies to the first Get/BatchGet physical send only, then `Dispatch::mark_retry_request` restores the operation default and matching `Context.max_execution_duration_ms`; Scan deliberately ignores the configurable override, as source scanner code does. Focused planner and transport regressions cover defaults, configured operation boundaries, and retry reset. Runtime stats, incremental scanner batching, snapshot caching, pipelined buffer-tier reads, and the complete receipt remain incomplete.

`txnkv/txnsnapshot` seed: `SetSnapshotTS` now also clears Rust's native snapshot read cache. The transaction buffer drops only `Cached` entries, retaining buffered mutations and locks; a focused two-timestamp Get regression proves a cached value from the old snapshot cannot leak into the new one. Runtime stats, incremental scanner batching, pipelined buffer-tier reads, and the complete receipt remain incomplete.

`txnkv/txnsnapshot` correction: pinned client-go `SetSnapshotTS` clears only `resolvedLocks`, not `committedLocks`. Rust now preserves committed read-through hints while removing timestamp-scoped resolved hints and cached values; the focused state regression observes both sets. Runtime stats, incremental scanner batching, pipelined buffer-tier reads, and the complete receipt remain incomplete.

`txnkv/txnsnapshot` seed: physical snapshot BatchGet requests now honor client-go's 5,120-key per-region limit. The source-derived 5,121-key regression observes one 5,120-key request and one 1-key request after region grouping. Runtime stats, incremental scanner batching, snapshot cache options, pipelined buffer-tier reads, async callback execution, and the complete receipt remain incomplete.

`txnkv/txnsnapshot` seed: native async and synchronous snapshots now expose client-go's pipelined buffer-tier read as `batch_get_from_buffer`. It rejects a non-pipelined snapshot before transport, then sends `BufferBatchGetRequest` with normal snapshot configuration after `set_pipelined`; source tagger callbacks distinguish this operation. The same 5,120-key regional cap applies. Runtime stats, incremental scanner batching, snapshot cache options, async callback execution, and the complete receipt remain incomplete.

`txnkv/txnsnapshot` correction: snapshot cache behavior now follows client-go's ownership boundary. Read-only snapshot Get/BatchGet cache values and misses; Scan never fills that cache; `MaxUint64` snapshots bypass it. `TimestampExt` now round-trips the full unsigned version space so that sentinel is usable. Focused regressions prove the three observable boundaries. Cache observability/options, runtime stats, incremental scanner batching, async callback execution, and the complete receipt remain incomplete.

`txnkv/txnsnapshot` seed: `Snapshot::set_scan_batch_size` and `SyncSnapshot::set_scan_batch_size` now preserve client-go's scanner RPC batching. Each native eager scan advances through 256-default (or configured) pair caps while retaining its caller-wide limit; forward uses `NextKey(last)` and reverse makes the last key exclusive. Focused forward/reverse transport regressions verify limits, boundaries, and ordering. Runtime stats, source iterator/callback ownership, and the complete receipt remain incomplete.

`txnkv/txnsnapshot` seed: `Snapshot::set_runtime_stats`/`SyncSnapshot::set_runtime_stats` now attach a shared `SnapshotRuntimeStats` collector at the physical Dispatch boundary. Get, BatchGet, BufferBatchGet, and Scan counts/durations therefore survive source-shaped region shards and retries, while normal user interceptors retain their order. The collector supports independent clone and merge snapshots; focused tests cover source command accounting. Backoff, TiKV execution-detail, resolve-lock, and read-pool detail remain incomplete.

`txnkv/txnsnapshot` seed: runtime stats now merge source `ExecDetailsV2` from Get, BatchGet, and BufferBatchGet: V2/legacy time details and all `ScanDetailV2` counters accumulate through clone and merge. Scan response detail is not claimed because the pinned Rust protobuf and source scanner expose none. Backoff, resolve-lock, read-pool detail, and exact source formatting remain incomplete.

`txnkv/txnsnapshot` seed: snapshot-read lock resolution now contributes source-shaped elapsed time to `SnapshotRuntimeStats`; only the resolver call is charged, whether it succeeds or fails, never sleep or write-side resolution. The locked Get retry regression observes this collector boundary. Backoff detail, unavailable read-pool protobuf detail, and exact source formatting remain incomplete.

`txnkv/txnsnapshot` seed: snapshot Get/BatchGet/BufferBatchGet/Scan region retries now own client-go's 20,000-ms input cumulative `RetryBackoffer`, including fork/cancellation/last-child accounting; source retry class names/sleeps feed runtime stats. The native no-resolve option still returns the underlying error without a retry. A server-busy regression observes client-go's equal-jitter first sleep. Read-lock `txnLockFast` still uses the older Rust `Backoff`, so complete snapshot backoff parity remains incomplete.

`txnkv/txnsnapshot` seed: live snapshot read locks now use client-go's cumulative 20,000-ms `RetryBackoffer` and `BackoffWithMaxSleepTxnLockFast` rather than Rust's legacy lock backoff. A positive resolver TTL caps that individual sleep; zero retries immediately, and runtime stats record `txnLockFast`. Mutation lock retries retain their native behavior. The one-millisecond live-lock regression passes. Source iterator/callback ownership and the complete receipt remain incomplete.

`txnkv/txnsnapshot` seed: async and synchronous snapshots now expose `set_variables`, retaining source `KVSnapshot.SetVars` as an `Arc<Variables>`. The exact shared variables configure both snapshot region and lock retry owners across Get, BatchGet, BufferBatchGet, and Scan; therefore custom backoff weighting, lock-fast delay, and the kill signal follow the source ownership boundary. Focused owner-budget and live-lock regressions pass. Iterator/callback ownership and the complete receipt remain incomplete.

`txnkv/txnsnapshot` seed: native `SnapshotIterator` and `SyncSnapshotIterator` now represent stateful source scanner ownership: eager first-batch construction, batch buffering, forward/reverse exclusive continuation, validity, and close. The async snapshot façade is generic over `PdClient` while preserving its production default, so scanner behavior is directly mock-testable. A focused forward regression proves prefetch, buffer consumption, `NextKey(last)` continuation, and terminal invalidation. Pair-level lock recovery and the complete scanner/package receipt remain incomplete.

`txnkv/txnsnapshot` seed: snapshot runtime scan detail now aggregates generated `ScanDetailV2` read-index propose/confirm and read-pool scheduling durations alongside the existing RocksDB/IA counters. The focused physical-read regression verifies all three nanosecond fields. The pinned protobuf lacks client-go's separate read-pool task-details message, so that aggregate remains unavailable.

`txnkv/txnsnapshot` seed: `SnapshotRuntimeStats` now renders client-go-compatible RPC, backoff, time-detail, resolve-lock, and scan-detail output, including `util.FormatDuration`'s precision rules and nested RocksDB/IA details. Source-derived formatter regressions cover rounding and every rendered section. The distinct read-pool task-details protobuf remains unavailable; pair-level scanner lock ownership and the package receipt remain open.

`txnkv/txnsnapshot` correction: response-level key errors now take precedence over pairs for Scan, BatchGet, and BufferBatchGet. This preserves client-go's incomplete-response contract: a top-level lock is resolved before retrying the original request, and pair errors are considered only on a response without that error. Focused precedence regressions pass. Per-pair lock recovery still reissues the whole native request rather than retaining successful pairs and point-reading just the locked key.

`txnkv/txnsnapshot` seed: scanner lock ownership now follows the pinned source distinction. A response-level lock resolves and retries the original incomplete ScanResponse; a pair-level error preserves every clean pair, recovers an empty pair key from `LockInfo`, and issues one snapshot Get for that key, skipping a missing/empty result. Continuation uses the raw response count and raw last key, so a missing locked tail neither ends early nor repeats that key. The response wrapper reuses existing sharding, retry, replica, interceptor, resource-control, runtime-stat, and read-validation plans. Async and blocking iterator regressions prove no whole-scan replay and no loss of clean pairs; the sync façade is now generic over `PdClient` for direct behavioral testing. Exact source one-region-at-a-time routing and shared per-`Next` backoff ownership remain open before a scanner receipt.

`internal/locate` region-cache seed: Rust now implements source multi-range batch location, including cached-prefix reuse, fresh-over-stale merging, contain-all PD batch scans, exact gap/limit checks, 128-region and 2,048-range bounds, leader filtering, and immediate ordered ScanRegions fallback for older PD servers returning Unimplemented. Validation: focused `source_batch` tests (8 passed) and both default/all-feature pinned-nightly library suites (486 passed), plus formatting and diff checks. The atomic package status remains `seed` until all remaining production and original test artifacts close together.

`internal/locate` region-cache seed: cached ordered scans now enforce source TTL and contiguous-coverage gates before returning reusable metadata. Rust also exposes the source range-helper behaviors for bounded single/multi-range refresh, complete half-open loading, inclusive-upper-key region-ID listing, cache-backed range location, and uncached direct PD lookup by region ID. Validation: focused region-cache source tests (21 passed) and both default/all-feature pinned-nightly library suites (488 passed), plus formatting and diff checks. The atomic package status remains `seed` pending all remaining production and original test artifacts.

`internal/locate` region-cache seed: the full original merger, range-splitting, and coverage-check case tables now run against Rust. Cache-miss key and inclusive-end lookups also preserve `findRegionByKey`'s one-time retry when newer intersecting metadata rejects the first PD result as stale. Validation: the focused stale-insert regression and both default/all-feature pinned-nightly library suites (489 passed), plus formatting and diff checks. The atomic package remains `seed` pending the rest of its production and original test inventory.

`internal/locate` region-cache seed: production now consumes `enable_preload` and `regions_refresh_interval`. Full refresh scans source-sized 10,000-region pages, atomically replaces all indexes while retaining in-flight by-ID waiters, and runs instead of GC when periodically configured; preload uses the source 20,000-ms retry budget, and close cancels/joins either task. Validation: focused full/periodic refresh coverage and both default/all-feature pinned-nightly library suites (490 passed), plus formatting and diff checks. The atomic package remains `seed` pending all remaining artifacts.

`internal/locate` store-cache seed: unhealthy-store PD re-resolution mutates the cached metadata without replacing its runtime state, preserving health/liveness, failure epoch, server-load estimate, and active users while the health-check route adopts the refreshed address and endpoint type. Direct by-ID region singleflight now also releases waiters after PD failure. Validation: focused regressions and both default/all-feature pinned-nightly library suites (492 passed), plus formatting and diff checks. Tombstone/resolve-state transitions and the remaining source inventory stay open; no atomic package receipt is claimed.

`internal/locate` store-cache in-progress slice: the full 1,222-line `store_cache.go` production inventory is now mapped into stable Rust cache entries and production consumers. PD store absence is nullable rather than a panic; one per-store resolution flight covers initial loads; `Unresolved`, `Resolved`, `NeedCheck`, and `Tombstone` transitions retain source foreground/background boundaries; re-resolution updates address/type/labels in place, preserves health/liveness/epoch/load/flow state, and advances the epoch for removed stores. Independent source-cadence schedules handle triggered/periodic resolution, health ticks with active two-second `GetHealthFeedback`, prefer-leader flow publication/reset, and full-store discovery; zero refresh intervals disable the configurable schedules while preserving ten-second discovery. Liveness probes are address-keyed singleflight, unhealthy TiKV stores own one recovery loop with periodic metadata refresh, and parent cancellation wakes every schedule promptly. Full discovery preserves terminal entries, leaves invalid empty-address stores unresolved, separately caches TiFlash-compute stores, and performs source two-interval metric-label cleanup. Resolved-store views, label filters, tombstone/down-peer exclusion, load decay, exact slow-score/feedback metrics, and unavailable-only compute-cache invalidation are covered by source-derived tests. Validation on `nightly-2026-08-22`: focused lifecycle and cancellation tests pass; `cargo +nightly-2026-08-22 test -p tikv-client --lib --quiet` and `cargo +nightly-2026-08-22 test -p tikv-client --all-features --lib --quiet` each pass 500 tests. `internal/locate` is now `in-progress`, not complete: the remaining eight production artifacts and their original tests must still close in the same atomic package receipt.

`internal/locate` request-sender diagnostics amendment: the ledger row's earlier generic “runtime metrics” remainder is superseded by this mapped slice. Rust now has source-shaped public region-request RPC/error statistics, bounded replica access details, exact region-error labels and NotLeader suffixes, logical-store Prometheus accounting, physical transport-error recording, shard/retry propagation, and automatic snapshot ownership/formatting/clone/merge integration. The source error-cardinality and access-overflow tables run unchanged in meaning, and focused retry/transport/snapshot tests exercise production call sites. Validation on `nightly-2026-08-22`: both default and all-feature library suites pass 504 tests; all-target/all-feature checking passes with the existing warning backlog. This is substantial in-progress evidence only: no `internal/locate` completion claim is made, and sender cancellation/state, replica/proxy selection, remaining cache/index behavior, and every still-unmapped original test remain required for the atomic receipt.

`internal/locate` selector-state amendment: client-go's `replica`, `baseReplicaSelector`, `canFastRetry`, pending-backoff, configurable-timeout, cancellation, and forwarding-proxy ownership now have production Rust consumers. Leader reads retain the source ten-attempt/fifty-cumulative-RPC-second budget instead of falling back after one send; configurable deadlines, DataIsNotReady, NotLeader, ServerIsBusy, and suspect-leader state remain distinct; a NotLeader hint gives an exhausted confirmed leader one final chance. Fast ServerIsBusy retry records a store-keyed delay, consumes it only when returning to that store, and charges the largest base delay when selection is exhausted. Logical targets and forwarding proxies both accumulate attempts and elapsed RPC time. Caller cancellation stops before routing or transport-failure invalidation, and gRPC Cancelled leaves cached region/store health unchanged. Forwarding walks each healthy proxy once, prefers a successfully cached proxy on later requests, and clears that preference when direct leader routing recovers. Source-derived regressions cover attempt/time/flag boundaries, pending-backoff replacement/consumption, healthy/exhausted/suspect fast-retry gates, cancellation classification, proxy caching/rotation/exhaustion, batched-Cop busy ownership, and deadline follower-read context. Validation on `nightly-2026-08-22`: default and all-feature library suites each pass 508 tests; the all-target/all-feature check passes with the existing warning backlog; formatting and diff checks pass. This remains in-progress evidence; no atomic package receipt is claimed.

`internal/locate` selector access-path amendment: the pinned mixed strategy's suspect-leader restoration now reaches production routing and request-local state. Once follower probes are unavailable or hintless, an eligible reachable leader has its temporary suspicion cleared and is retried without region invalidation; ordinary exhaustion, deadline, epoch, and liveness gates remain distinct. Unknown and unreachable non-forwarded leaders fall through to mixed follower probes. Forwarding now covers both unavailable liveness states, suppresses proxying after a hintless NotLeader, and on total proxy exhaustion advances the leader store epoch and marks reload-on-access before returning selector exhaustion. Stale reads no longer switch to replica-read after a configurable leader deadline. The `nextgen` build disables replica/stale read flags and busy-threshold routing at plan construction and again at the PD routing boundary while retaining explicit selector filters/options. Validation on `nightly-2026-08-22`: default and all-feature library suites each pass 514 tests; all-target/all-feature checking passes with only the existing warnings; formatting and diff checks pass. This is one substantial in-progress batch, not an atomic package completion claim.
