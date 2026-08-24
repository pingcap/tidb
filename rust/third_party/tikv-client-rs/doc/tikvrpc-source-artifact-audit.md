# `tikvrpc` source-artifact audit

This is the atomic completion receipt for client-go package `tikvrpc` at commit `52c1e76cec993571493c81de442bcbef90cdc106`. The child directory `tikvrpc/interceptor` is a separate Go package with its own completed receipt and is not folded into this inventory.

## Complete source inventory

The package contains exactly six files and 2,624 lines. There is no `doc.go`, build-tag or platform variant, fixture, benchmark, example, package build file, or metadata artifact.

| Source artifact | SHA-256 | Rust owner |
| --- | --- | --- |
| `tikvrpc/cmds_generated.go` (486 lines) | `7e5a83f61bd2a2e27645723bbc143e289a8754d88568bce6bb6aa3f7d9ee2081` | `src/store/request.rs`: typed `Request::attach_context` implementations and exhaustive generated-command regression |
| `tikvrpc/endpoint.go` (91 lines) | `d69ae8449ec32328b5deb7ae39d9c261e57072d3c49172bc0714542316b1a1b2` | `src/store/endpoint.rs` and region/store selection consumers |
| `tikvrpc/gen.sh` (122 lines) | `5c9d3910d6c70370a59d17225836ad541314e21c5ea9779c83534dba60cee764` | Source-enumerated 42-command context test; the Rust macro expands implementations at compile time instead of checking in a second generated Rust registry |
| `tikvrpc/main_test.go` (25 lines) | `f22834678863e27b9d1d865db8a438fff81d80591cf0b0c4c0e0c38806eeefb2` | Owned Tokio/Tonic stream, batch worker, mock-server, cancellation, close, and force-stop lifetimes |
| `tikvrpc/tikvrpc.go` (1,564 lines) | `5c3a95131991f15ea39bd4617d63c6dc3a38955b004282518cfd722406f870a5` | `src/store/{command,endpoint,errors,request}.rs`, `src/request/{plan,plan_builder,shard}.rs`, and routing/config owners |
| `tikvrpc/tikvrpc_test.go` (336 lines) | `68e89331dce81e7abf97a8ef8559637881a73aeeb789f5fba40fae2aecc745f7` | Source-derived command, context, batch snapshot, and CopStream RU tests |

## Required protocol inputs

Client-go pins kvproto commit `059694ae4472276644613acccefa24cbc89d959f`. Every direct protocol input needed by this package is byte-identical in client-rust and its Prost/Tonic bindings are generated from those inputs:

| Input | SHA-256 | Package surface |
| --- | --- | --- |
| `proto/coprocessor.proto` | `047bf9a5593908327fb0e9f87a9def843ddcc03c75236ab0a4f5b50fda5aa158` | Unary/streaming Cop and BatchCop requests/responses |
| `proto/debugpb.proto` | `305111885267bf596327caf714e516eef3823898c565ea591894c219543673b2` | Debug GetRegionProperties route |
| `proto/errorpb.proto` | `9a217e2ab8a8a77ab407a508ee3224a26c5f99a9911192b1acc52d3d9c93e1ea` | Synthetic and extracted region errors |
| `proto/kvrpcpb.proto` | `d107a80efae8c17afd39f9274c0688b1bdfdd9c6819481aec73a3fbe8e963a2c` | Context, command payloads, response details, origin, API and resource-control fields |
| `proto/metapb.proto` | `e1f5ea1f9f7701d087847a6a18385f3fa25f3355996fedc8435c55b8bca3a045` | Region, peer, store, and engine labels |
| `proto/mpp.proto` | `479798510cbd229b718bf5699e58bc6eea45d9d917d53b9313f6b4af8d81a166` | MPP dispatch/cancel/alive/connection routes |
| `proto/tikvpb.proto` | `4549bc2657d6ecb67407f4aa6f18a6cce41e9485cca975a6ee6dbbbd7efe2615` | TiKV service methods and BatchCommands oneofs |

The broader generated-protocol root remains a repository-level final gate; this receipt claims only the seven direct inputs above.

## Production symbol and behavior mapping

| client-go surface | Rust mapping and integration decision |
| --- | --- |
| `CmdType`, values, names, and `CmdGetKeyTTL` alias | `CommandType` preserves every continued-`iota` numeric value, source string including `CmdEmpty -> "Unknown"`, and native `GET_KEY_TTL` alias. |
| Debug, interruptible, Green-GC, transaction-write, and raw-write predicates | Source-exact `CommandType` predicates; request/resource-control callers use the same labels and typed command identity. |
| Dynamic `Request` payload and typed accessors | `Request` is an object-safe typed transport trait. Concrete protobuf values replace unchecked `interface{}` accessors, so mismatched type assertions are unconstructible. The 53-case `CallRPC` matrix plus Debug are compile-tested as 54 concrete implementations. |
| Dynamic route fields | Rust keeps payload metadata on concrete requests and route/retry metadata on `Dispatch`, `PlanBuilder`, `ReplicaReadConfig`, and `RegionStore`: context, replica mode/seed state, endpoint type, forwarding host, replica count, read type, input request source, access location, predicted bytes, and logical/physical targets survive shard and retry clones. Consumer package receipts still own their higher-level selection algorithms. |
| `SetDefaultRequestOrigin`, `GetDefaultRequestOrigin`, `NewRequest` context behavior | Public root `RequestOrigin`, `set_default_request_origin`, and `get_default_request_origin` exports control a sequentially consistent process-wide atomic that fills only `RequestOriginUnknown`. `attach_context`, unary wire cloning, Cop/BatchCop wire cloning, and batch conversion all stamp the default without overwriting an explicit origin. Rust constructs concrete typed values directly, so a dynamic constructor is unnecessary. |
| Replica/stale helpers | `ReplicaReadConfig`, selector state, `Dispatch::disable_stale_read_after_lock`, request context flags, and snapshot scope state preserve follower/mixed/leader/stale transitions. Rust-owned configuration replaces nullable seed pointers; no unchecked nil receiver is exposed. |
| `ToBatchCommandsRequest` and `FromBatchCommandsResponse` | `BatchCommandRequest`/`BatchCommandResponse` cover exactly the source oneofs, including `Empty`, health feedback, and broadcast status. Unsupported requests return `None`; an absent response command returns the exact `Unknown command response` error; generated alternatives impossible from a source request remain unreachable. |
| Request and response `GetSize` | Deliberately narrow source matrices use Prost encoded lengths. Unlisted protobufs return zero instead of acquiring generic-size behavior. |
| `Response`, `ResponseExt`, `GetRegionError`, `GetExecDetailsV2` | Typed `Box<dyn Any>` transport responses, `ResponseExt<T>`, `HasRegionError`, and execution-detail downcasts preserve source behavior. Compile-time response types replace Go's runtime invalid-response error; source-approved no-region-error stream/MPP/Empty types are separate typed values. |
| `AttachContext`, `SetContext`, `SetContextNoAttach`, generated patch registry | `Request::attach_context` replaces an owned context and returns the exact source acceptance result. The 42 generated commands, CopStream-to-Cop alias, MPP/Empty no-ops, and rejected store/diagnostic commands are exhaustive. Region/peer setters plus route-owned context are the native split of attached versus no-attach routing. Owned clones remove Go's revision-pointer race while preserving each published batch snapshot. |
| `GenRegionErrorResp` | `RegionErrorResponse::from_region_error` constructs all 37 distinct concrete source response types. CopStream uses the same concrete Cop response; Empty has no region error; invalid request/response pairings are eliminated by static types. `GetHealthFeedbackResponse` is included. |
| `CallRPC` and `CallDebugRPC` | Tonic dispatch covers every source unary, Debug, Empty, Cop/CopStream/BatchCop, and MPP route with timeout, forwarding, tracing, API coding, RU, and typed response ownership. The completed `internal/client` receipt owns pool and BatchCommands orchestration. |
| Stream response wrappers, `Lease`, timeout loop, `Recv`, and `Close` | Cop, BatchCop, and MPP wrappers eagerly receive the first item, apply a timeout to the first and every later receive, and cancel by dropping/closing the owned Tonic stream. Tokio's per-await deadline is the native equivalent of grpc-go's shared lease scanner and requires no polling task. Cop consumes its one-time RPC count even when RU charging is bypassed. |
| `ResourceGroupTagger` | Object-safe `ResourceGroupTagger` can amend any concrete `Request`; snapshot/transaction consumers retain their own completion status and public callback shape. |
| `GetStartTS` | `Request::start_timestamp` implements all 22 source branches, including CopStream, BatchCop, and MVCC-by-start-ts; every other request returns zero. |
| Endpoint names, TiFlash relation, labels, and store classification | `EndpointType` and constants in `src/store/endpoint.rs` preserve all four endpoint values and first matching engine-label behavior. |

## Original test mapping

| Source test | Rust evidence |
| --- | --- |
| `TestBatchResponse` | `batch_command_response_decoding_preserves_oneof_and_unknown_error` verifies a nil oneof returns `Unknown command response`. |
| `TestDefaultRequestOrigin` | `source_default_request_origin_fills_only_unknown_contexts` covers constructor-equivalent batch wiring, all four source regression request types, explicit-origin precedence, and atomic reset. |
| `TestAttachContextSetsRequestContext` | `source_attach_context_replaces_the_owned_request_snapshot` covers Get and LockWaitInfo replacement with region/API/keyspace metadata; `source_generated_attach_context_matrix_is_complete` covers the whole generated registry. |
| `TestTiDB51921` | `source_tidb_51921_batch_snapshots_encode_after_relocation` publishes every source-batchable owned snapshot, relocates the original, then concurrently Prost-encodes all 29 snapshots. Rust's borrow/ownership rules make the original mutable-pointer race unrepresentable. |
| `TestCopStreamResponseRecvBypass` | `source_cop_stream_ru_v2_counts_only_the_first_received_rpc` covers charged and bypass branches, one-time count consumption, unchanged server details, accumulation, and drain behavior. Real loopback eager-first-receive and close behavior is covered in `src/store/client.rs`. |
| `TestMain` | The package creates no permanent timeout scanner. Stream values, batch workers, connections, and mock servers all have explicit close/drop owners; completed `internal/client` loopback shutdown, cancellation, panic-recovery, and force-stop tests provide leak evidence. |

Additional production-derived tests cover all command values/names/classifications, exact endpoint labels, 42 generated context commands plus CopStream and no-op/rejected cases, 54 dispatch routes, 22 start timestamps, 37 synthetic region-error response types, request/response sizing, all batch oneofs, streaming API-codec behavior, and typed response/address/tagger carriers.

## Consumer audit

All 74 Go files importing `tikvrpc` were assigned; completion of this foundational package does not promote any consumer package.

| Consumer group | Exact source files and ownership |
| --- | --- |
| API codec | `internal/apicodec/{codec.go,codec_test.go,codec_v1.go,codec_v2.go,codec_v2_test.go}`; already complete and consumes typed command/context transforms. |
| Transport | `internal/client/{client.go,client_async.go,client_async_test.go,client_batch.go,client_collapse.go,client_fail_test.go,client_interceptor.go,client_interceptor_test.go,client_test.go,conn_pool.go}`; already complete and owns pools, batch streams, dispatch, close, and observability. |
| Internal request plumbing | `internal/kvrpc/batch.go`; retains its independent ledger status. `internal/mockstore/mocktikv/rpc.go` remains with the concrete mock-store package. |
| Locate/routing | `internal/locate/{metrics_collector.go,metrics_collector_test.go,region_cache.go,region_cache_test.go,region_request.go,region_request3_test.go,region_request_state_test.go,region_request_test.go,replica_selector.go,replica_selector_test.go,store_cache.go}`; now complete under its own receipt and owns selection/retry/ResponseExt integration. |
| Resource control | `internal/resourcecontrol/{resource_control.go,resource_control_test.go}`; already complete and consumes request/response size/classification/detail surfaces. |
| High-level raw/store | `rawkv/rawkv.go`; `tikv/{gc.go,interface.go,kv.go,kv_test.go,region.go,split_region.go,test_probe.go,test_util.go}`; their ledger rows remain non-complete. |
| Range and lock | `txnkv/rangetask/delete_range.go` is already complete; `txnkv/txnlock/lock_resolver.go` remains non-complete. |
| Transactions | `txnkv/transaction/{2pc.go,cleanup.go,commit.go,pessimistic.go,pipelined_flush.go,prewrite.go,test_probe.go,test_util.go,txn.go,txn_file.go,txn_file_test.go,txn_test.go}`; the package remains non-complete and owns tagger/transaction algorithms. |
| Snapshots | `txnkv/txnsnapshot/{client_helper.go,scan.go,snapshot.go,snapshot_async.go}`; the package remains non-complete and owns async callbacks, scanners, and snapshot state. |
| Interceptor child package | `tikvrpc/interceptor/{interceptor.go,interceptor_test.go}`; already complete under its separate package receipt. |
| Repository integration tests | `integration_tests/{2pc_test.go,async_commit_test.go,client_fp_test.go,health_feedback_test.go,interceptor_test.go,lock_test.go,pd_api_test.go,pipelined_memdb_test.go,prewrite_test.go,resource_group_test.go,resource_tag_test.go,snapshot_fail_test.go,snapshot_test.go,store_test.go,txn_file_test.go}`; retained by the final repository integration gate and their owning high-level packages. |

## Validation boundary

Package behavior is deterministic or already exercised through the completed loopback transport and locate packages. No live TiKV/PD cluster is required for this receipt. The pinned Go tests were inspected but cannot be re-executed on this host because no Go toolchain is installed. Final cross-client live-cluster behavior remains mandatory for the unfinished raw, snapshot, transaction, and high-level `tikv` packages.
