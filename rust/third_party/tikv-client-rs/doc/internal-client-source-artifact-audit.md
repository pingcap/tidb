# `internal/client` source-artifact audit

This audit pins client-go commit `52c1e76cec993571493c81de442bcbef90cdc106`. It is the atomic evidence behind the `internal/client` ledger row; it does not include behavior owned by `internal/locate`, even when that package selects a proxy before calling this transport.

## Inventory and production mapping

The complete production inventory is `client.go`, `client_async.go`, `client_batch.go`, `client_collapse.go`, `client_interceptor.go`, `conn_batch.go`, `conn_monitor.go`, `conn_pool.go`, `legacy_codec.go`, and `priority_queue.go`. `mockserver/mock_tikv_service.go` is the sole support artifact and has its own complete receipt. `OWNERS` is repository metadata. There is no `doc.go`, build-tag/platform variant, generated input/output, fixture, or package-local build file.

| Source artifact | Rust ownership and integration decision |
| --- | --- |
| `client.go` | `src/store/{client,request,command}.rs`, `src/pd/client.rs`, and `src/stats.rs`: address-keyed/versioned pools, close/compare-close, fixed five-second dial, `MaxRecvMsgSize`, round robin, unary/debug/three streaming paths, forwarding, RU-v2, trace details, per-store metrics, event listener, idle retirement, and typed connection errors. |
| `client_async.go` | Native Rust `Future` dispatch plus `BatchCommandSubmission`; dropping the future is cancellation and awaiting it is callback execution. No callback/run-loop façade is added. |
| `client_batch.go` | `src/store/{batch,command}.rs`: priority selection, request IDs, direct/forwarded grouping, publication-before-send, response demultiplexing, cancellation, diagnostics, health feedback, receive recovery, and stream metrics. |
| `client_collapse.go` | One process-wide ResolveLock singleflight in `src/store/client.rs`, keyed by `(region_id,start_version,is_async)` with source exclusions and timeout ownership. |
| `client_interceptor.go` | `src/resource_control.rs`, `src/request/{plan,plan_builder}.rs`, and the complete `tikvrpc/interceptor` receipt. Admission precedes the user interceptor; settlement follows only a successful physical response. |
| `conn_batch.go` | `BatchCommandsWorker` and `BatchCommandsDispatcher`: source collection policies, finite/default concurrency distinction, send-loop panic restart, idle lifecycle, all send-side metrics, and direct/forwarded stream publication. |
| `conn_monitor.go` | Event-driven five-state gauge in `src/store/client.rs`. Tonic exposes no grpc-go `GetState`; owned transitions replace polling and removal clears all labels. |
| `conn_pool.go` | `TikvConnect`, `SecurityManager`, `KvRpcClient`, and the PD cache owner: TLS, gzip, keepalive/windows, connection count, receive limit, fixed dial timeout, Debug client sharing, pool close, and BatchCommands setup. grpc-go's arbitrary `[]grpc.DialOption` and buffer-pool implementation are native-library extension points with no Tonic object equivalent; all source options with observable client behavior have structured Rust fields. |
| `legacy_codec.go` | Prost/Tonic owns immutable decode buffers and does not expose grpc-go's pooled `mem.BufferSlice`; no compatibility codec is needed or safe to emulate. |
| `priority_queue.go` | `src/store/priority_queue.rs`: max-priority heap, stable source comparison behavior, cancellation cleanup, and ownership-draining take/reset. |

The `tikvrpc.CallRPC` switch is integrated, not deferred: `source_call_rpc_command_matrix_has_typed_request_implementations` instantiates all 53 source command cases as concrete Rust `Request` implementations and the Debug request as the 54th route. Unary API-v2 encoding/decoding belongs to the plan/keyspace codec because Rust creates typed requests after routing; the three streaming wrappers retain their transport-local source matrix, including CopStream's deliberate unsupported-decode error. This records an ownership boundary rather than a partial implementation.

## Original test mapping

Every original test is listed below. Rust test names are exact where a one-to-one test exists; grouped names identify the deterministic native evidence replacing a Go runtime or memory-management mechanism.

### `client_async_test.go`

| Source test | Rust evidence |
| --- | --- |
| `TestSendRequestAsyncBasic` | Native future dispatch; `source_debug_and_empty_commands_use_their_distinct_paths`, `source_batch_dispatcher_construction_uses_client_configuration`, and the complete `util/async` native-mapping receipt. |
| `TestSendRequestAsyncAttachContext` | `source_context_bearing_unary_requests_retain_full_context_metadata` and real BatchCommands request transport tests. |
| `TestSendRequestAsyncUpdateTiKVRUV2` | `updates_and_drains_ru_v2_exactly`, `source_cop_stream_ru_v2_counts_only_the_first_received_rpc`, and physical unary/batch RU-v2 plan tests. |
| `TestSendRequestAsyncTimeout` | `source_dropped_submission_cancels_before_batch_selection`, `source_cancelled_response_records_its_stream_tail`, and request-stage timeout/cancellation boundary tests. |
| `TestSendRequestAsyncAndCloseClientOnHandle` | `source_explicit_close_retires_published_and_future_worker_entries`. |
| `TestSendRequestAsyncAndCloseClientBeforeSend` | `source_close_fails_only_entries_not_yet_published` and `source_client_close_retires_every_pool_once_and_prevents_reconnect`. |

### `client_batch_test.go`

| Source test | Rust evidence |
| --- | --- |
| `TestEncodedBatchCmd_SizeAndMarshalTo` | Prost `Message::encoded_len`/encoding plus `batch_command_encoding_retains_source_oneof_and_identity`. |
| `TestEncodeRequestCmd_Basic` | `batch_command_encoding_retains_source_oneof_and_identity` and `batch_command_bridge_accepts_only_source_batchable_requests`. |
| `TestEncodeRequestCmd_PoolReuse` | Prost owns each encoded message; no reusable caller-visible backing buffer exists. Repeated oneof conversion is covered by the same encoding tests. |
| `TestEncodeRequestCmd_AfterPoolReturn` | Non-applicable aliasing hazard: Rust ownership prevents access after a value is moved/dropped. |
| `TestReuseRequestData_Basic` | Non-applicable manual pool; Tonic owns transport buffers. Typed request reuse is clone-based and cannot alias returned storage. |
| `TestReuseRequestData_DoubleReturn` | Non-applicable by ownership: there is no public return-to-pool operation. |
| `TestEncodedMsgDataPool_ConcurrentSafety` | Non-applicable manual pool; concurrent sends transfer independently owned Prost messages. Batch publication concurrency is covered by dispatcher integration tests. |

### `client_fail_test.go`

| Source test | Rust evidence |
| --- | --- |
| `TestPanicInRecvLoop` | `source_receive_loop_recovers_panics_and_retires_pending_requests`; the supervisor increments `batch-recv-loop` and recreates the stream. |
| `TestRecvErrorInMultipleRecvLoops` | `source_dispatcher_recreates_failed_streams_and_preserves_metadata_per_host`; one reconnect gate serializes sibling direct/forwarded recovery. |

### `client_interceptor_test.go`

| Source test | Rust evidence |
| --- | --- |
| `TestInterceptedClient` | Complete `tikvrpc/interceptor` receipt and transaction physical-RPC interceptor integration. |
| `TestAppendChainedInterceptor` | `chain_is_onion_ordered_and_replaces_duplicate_names`. |
| `TestGetResourceControlInfoHonorsSelectionPolicy` | `source_resource_control_selection_uses_routed_replica_and_zone`. |
| `TestSendRequestDoesNotSettleAndKeepsRUDetailsOnTransportFailure` | `transaction_resource_control_does_not_settle_transport_failures`. |
| `TestSendRequestSettlesOnSuccess` | `transaction_resource_control_charges_and_settles_each_physical_rpc`. |
| `TestSendRequestAsyncDoesNotSettleAndKeepsRUDetailsOnTransportFailure` | Same physical future path as synchronous Rust callers; the transport-failure test above is the native mapping. |
| `TestBypassRUV2FollowsRequestInfoBypass` | `source_request_info_uses_typed_command_context_and_bypass_rules` and `source_ru_v2_skips_internal_bypass_requests`. |

### `client_test.go`

| Source test | Rust evidence |
| --- | --- |
| `TestStreamFirstRecvErrorClosesLease` | All three typed wrappers eagerly await their first item and drop the owned Tonic stream on error; `source_coprocessor_stream_reads_first_response_before_returning` exercises the real transport boundary. Per-`message` timeout and explicit close share the wrapper macro. |
| `TestConn` | `source_connection_pool_round_robin_increments_before_selecting`, `source_close_addr_ver_does_not_evict_a_newer_cached_client`, and `source_pool_creation_is_singleflight_per_client`. |
| `TestGetConnAfterClose` | `source_client_close_retires_every_pool_once_and_prevents_reconnect` and close-time connection-gauge clearing. |
| `TestCancelTimeoutRetErr` | Native future cancellation plus `source_dropped_submission_cancels_before_batch_selection` and timeout request-stage assertions. |
| `TestCompletedTiKVRUV2RPCCount` | Typed command classifications and RU-v2 physical-response tests, including write/read/bypass/non-TiKV boundaries. |
| `TestSendWhenReconnect` | Finite concurrency waits on stream readiness until the caller timeout; `source_dispatcher_recreates_failed_streams_and_preserves_metadata_per_host` verifies later recovery. |
| `TestCollapseResolveLock` | `source_resolve_lock_singleflight_key_and_exclusions`. |
| `TestForwardMetadataByUnaryCall` | `source_unary_forwarding_metadata_is_applied_only_when_requested`. |
| `TestForwardMetadataByBatchCommands` | `source_batch_stream_metadata_carries_forwarding_host_and_pool_index` and the real dispatcher metadata/recreation test. |
| `TestBatchCommandsBuilder` | `source_builder_groups_direct_and_forwarded_requests_with_monotonic_ids`, priority, cancellation, and reset tests. |
| `TestBatchRequestTerminalOutcome` | `source_receive_loop_routes_each_id_and_retains_terminal_outcomes`. |
| `TestVisitBatchRequestObservations` | `source_batch_request_stage_observations_preserve_terminal_boundaries`. |
| `TestFormatBatchRequestTimeoutReasonNormalizesObservedSentNS` | The same telemetry test covers response-before-send normalization and source nanosecond fields. |
| `TestWriteBatchCommandsEntryProgress` | Real dispatcher integration verifies every envelope has `client_send_time_ns`; receive progress tests verify response watermarks. |
| `TestInspectPendingBatchRequests` | `source_inspect_pending_batch_requests_separates_confirmed_entries`. |
| `TestTraceExecDetails` | `execution_detail_tree_and_historical_timeline_match_client_go` and `source_exec_details_trace_wraps_a_physical_batch_rpc`. |
| `TestBatchClientRecoverAfterServerRestart` | Deterministic close/reopen generation in `source_dispatcher_recreates_failed_streams_and_preserves_metadata_per_host`. |
| `TestLimitConcurrency` | `source_concurrency_limit_admits_only_available_normal_requests`, priority and reset tests. |
| `TestPrioritySentLimit` | `source_builder_prioritizes_and_allows_high_priority_to_exceed_limit`. |
| `TestBatchClientReceiveHealthFeedback` | `source_health_feedback_listener_is_replaced_and_runs_before_demux`. |
| `TestRandomRestartStoreAndForwarding` | Deterministic direct/forwarded failure isolation and recreation tests replace the nondeterministic stress loop. |
| `TestFastFailRequest` | `TikvConnect` applies the fixed source five-second Endpoint dial timeout; the connector default assertion prevents request-timeout coupling. |
| `TestErrConn` | `source_transport_error_carries_cached_connection_identity` and plan-level compare-close extraction tests. |
| `TestFastFailWhenNoAvailableConn` | `source_default_concurrency_fast_fails_an_unavailable_batch_stream`. |
| `TestConcurrentCloseConnPanic` | `source_client_close_retires_every_pool_once_and_prevents_reconnect` concurrently joins client close and versioned address close, then verifies exactly-once retirement. |
| `TestBatchPolicy` | `source_turbo_batch_policy_presets_and_custom_values_are_preserved`, collection-order and overload tests. |

### `priority_queue_test.go` and `main_test.go`

| Source test | Rust evidence |
| --- | --- |
| `TestPriority` | `source_priority_take_and_cancelled_cleanup_contract`. |
| `TestPriorityQueueTakeAllLeavesReferencesInBackingArray` | Rust's draining ownership leaves no interface references in a backing array; the same test verifies the queue is empty after take/cleanup. The Go GC-retention failure mode is unrepresentable without unsafe code. |
| `TestMain` | Go's goleak wrapper maps to owned `JoinHandle`/cancellation lifetimes. Explicit close, idle retirement, send/receive panic recovery, stream recreation, and mock-server force-stop tests prove every package-created task has a shutdown owner. |

## Validation boundary

The Rust tests use real loopback Tonic transports for unary, stream, BatchCommands, metadata, restart, and close behavior; deterministic state tests replace Go's random restart stress. A live TiKV/PD cluster is not required for this package-owned transport contract. Proxy selection and region retry remain on `internal/locate`; high-level transaction behavior remains on its owning package rows.
