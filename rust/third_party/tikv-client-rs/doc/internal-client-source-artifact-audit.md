# `internal/client` source-artifact audit

This independently reopened audit pins client-go commit `52c1e76cec993571493c81de442bcbef90cdc106`. It is the atomic evidence behind the `internal/client` ledger row; it does not include behavior owned by `internal/locate`, even when that package selects a proxy before calling this transport. The 2026-08-26 re-audit reruns the exact Go package and race suite and upgrades every ordinary Go test from grouped evidence to an independently executable Rust port.

## Inventory and production mapping

The immutable inventory below is from the pinned tree. It contains 10 production files (3,929 lines), seven test files (2,794 lines, including the goleak harness), one support file (196 lines), and one metadata file (5 lines): 19 artifacts and 6,924 lines total. `mockserver/mock_tikv_service.go` is the sole support artifact and has its own complete receipt. `OWNERS` is repository metadata. There is no `doc.go`, build-tag/platform variant, generated input/output, fixture, benchmark, example, or package-local build file. All 32 direct importer files are assigned to this transport receipt or their already-complete owning package receipts.

| Kind | Source artifact | Lines | SHA-256 |
| --- | --- | ---: | --- |
| production | `client.go` | 926 | `560218e9d0f88c68ff3b15436cb53759185af94b7be8fbfeb14d01967b98f0bb` |
| production | `client_async.go` | 188 | `9df6fe7a0c68108bf61f08c242ac7c735cdcfed3eec986dc6dcccddab9f67389` |
| production | `client_batch.go` | 1,497 | `1bd61047646f9d5d146e750642b0e0f8f3f9d4e89d765fb2c71be36d593debc5` |
| production | `client_collapse.go` | 166 | `343a880fde53e725eb6c609eafbf853d7d12c61c1915600ec649f5a1c3961139` |
| production | `client_interceptor.go` | 258 | `114690b7e027c68739352906bbb57ea21602c731249cea2eb64e16dcf3f47b5b` |
| production | `conn_batch.go` | 340 | `8b3a47a159cb34a82f8309c36058775dbb89d86fbf8ed986f77a821d78cb1458` |
| production | `conn_monitor.go` | 101 | `fe59a40c30a23eca1bf15402bc094f1476c2fe9cbb7205693598099750f08004` |
| production | `conn_pool.go` | 231 | `51f792832e9e3486c19e3095d7ff306ff4e10d2ad364f9ec9b772ff92440f8bd` |
| production | `legacy_codec.go` | 64 | `4d347d4a9ad0cb9e2451d30dc4bebb35543185874ac314dd19d9b55b9b08aa95` |
| production | `priority_queue.go` | 158 | `5b45c52f1940027a9f41ac8aca4d88079bdbe6413ff506b3704eeb2a28f42878` |
| test | `client_async_test.go` | 428 | `c8b0a6a5f7b51164eb9bc8f149f1a829b71d156d3ed6e1079d78d124c9330c95` |
| test | `client_batch_test.go` | 221 | `11dc90ed6808624c5b28d49113b9d0d222479072875d21fc3994532c3a72e271` |
| test | `client_fail_test.go` | 164 | `57487c87a69a5cf5762064b5c0fb77f0473b5352927a250374e11bd0241b9bd7` |
| test | `client_interceptor_test.go` | 306 | `da651c3f50ef7c1fd72c21bada33fda2d21cf0ef987e96c880a2d93e4d89c428` |
| test | `client_test.go` | 1,521 | `9c10eb19ba34d1d6d4f6f5fa7460729b15f3ee8b3f63f64c88d397b2b6226401` |
| test | `priority_queue_test.go` | 121 | `f194df1dccd85b31567890d59e5a1692d211dc1a19124f11b154d66c33ccef0a` |
| test harness | `main_test.go` | 33 | `428ec256f14e0d4da028b5e35d2078588f306d3d04a0bf66646bc0d96a28b2ac` |
| support | `mockserver/mock_tikv_service.go` | 196 | `04a517a353b368c25f4ffa23f45fd2167358287e7b8bf276df3902d4cf3c2585` |
| metadata | `OWNERS` | 5 | `c258cd9b48ab4d97180a984b7dba35e54d2785902cebabc09f9ad77d0a0a0bb6` |

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

## Complete direct-importer inventory

The pinned source has exactly 32 files importing `internal/client`:

- Package-local external tests: `internal/client/client_async_test.go`, `client_fail_test.go`, and `client_test.go`; their complete declaration mapping is below.
- Routing: `internal/locate/region_cache.go`, `region_request.go`, `region_request3_test.go`, `region_request_test.go`, `replica_selector.go`, and `replica_selector_test.go`; the complete locate receipt owns route/proxy choice while this package owns the invoked transport.
- Mock transport: `internal/mockstore/mocktikv/rpc.go`; the complete mocktikv receipt owns the in-process implementation of this interface.
- RawKV: `rawkv/rawkv.go` and `test_prob.go`; the complete RawKV receipt owns operation topology and batch probes.
- Root store: `tikv/client.go`, `kv.go`, `region.go`, and `split_region.go`; the independently re-audited root receipt owns construction, lifecycle, region helpers, and split/scatter orchestration.
- Range task: `txnkv/rangetask/delete_range.go`; the complete rangetask receipt owns task retries and progress.
- Transactions: `txnkv/transaction/2pc.go`, `cleanup.go`, `commit.go`, `pessimistic.go`, `pipelined_flush.go`, `prewrite.go`, `test_util.go`, `txn_file.go`, `txn_file_test.go`, and `txn_test.go`; the complete transaction receipt owns protocol sequencing while this package owns each physical dispatch.
- Lock resolution: `txnkv/txnlock/lock_resolver.go`; the complete txnlock receipt owns resolution algorithms and lifecycle.
- Snapshots: `txnkv/txnsnapshot/client_helper.go`, `scan.go`, `snapshot.go`, and `snapshot_async.go`; the complete snapshot receipt owns read topology and retry semantics.

No importer is used to inflate this transport claim or left without an owning package receipt.

## 2026-08-25 source re-audit

The complete production and original-test inventory was read again against the pin. The differential/source-derived gate exposed and corrected these transport behaviors:

- Debug and TiKV clients now share the configured maximum receive size and gzip receive/send policy.
- A malformed short BatchCommands envelope delivers its valid prefix before the source-equivalent receive-loop panic; the same stream resumes and the missing suffix remains pending.
- Recovery is elected once per pooled-connection epoch. Only the elected forwarding host retires pending entries, while sibling streams recreate without cross-host failure. The recreate gate is acquired before the stream becomes unavailable, preserving client-go's send/recreate exclusion and preventing finite-limit readiness deadlocks.
- Concurrency limits are connection-local and pool selection scans round-robin past recreating or saturated slots.
- The request deadline starts before queue admission, close interrupts a capacity wait, and all entries in one physical batch share its send/first-response timing state and source-formatted timeout diagnostics. Timeout cancellation is published before returning so a racing late response cannot be misclassified as delivered.
- Collected entries survive send-loop panic restart. The connection-owned idle deadline resets on head acceptance, remains active through collection/send work, and does not interrupt `fetchMorePendingRequests`.
- Custom batch-policy JSON preserves Go `int` values, null/zero-value behavior, case-insensitive and duplicate tagged keys, last-non-null assignment, and fallback rules.
- Response progress advances only after a complete envelope is processed; partial malformed-response metrics and exact terminal-outcome labels are retained.
- Pending-request inspection starts at client-go's randomized point inside its first 60-second interval instead of synchronizing every pool on one deadline.
- Dropping a worker now closes queued/published work and aborts its owned task, preventing the goleak-equivalent detached-worker lifetime.

The package deliberately uses native Tonic/Prost ownership for grpc-go dial-option, buffer-pool, codec, and connectivity polling mechanisms that do not exist as observable Rust APIs. These are explicit native substitutions, not deferred behavior.

## 2026-08-26 independent unit-test re-audit

The source declares 50 ordinary tests plus `TestMain`. Every ordinary declaration now has its own source-named Rust test; the detailed tables in the next section retain the underlying behavior and representation decisions each port executes.

| Source file | Source declaration | Independently executable Rust port |
| --- | --- | --- |
| `client_async_test.go` | `TestSendRequestAsyncBasic` | `source_test_send_request_async_basic` |
| `client_async_test.go` | `TestSendRequestAsyncAttachContext` | `source_test_send_request_async_attach_context` |
| `client_async_test.go` | `TestSendRequestAsyncUpdateTiKVRUV2` | `source_test_send_request_async_update_tikv_ruv2` |
| `client_async_test.go` | `TestSendRequestAsyncTimeout` | `source_test_send_request_async_timeout` |
| `client_async_test.go` | `TestSendRequestAsyncAndCloseClientOnHandle` | `source_test_send_request_async_and_close_client_on_handle` |
| `client_async_test.go` | `TestSendRequestAsyncAndCloseClientBeforeSend` | `source_test_send_request_async_and_close_client_before_send` |
| `client_batch_test.go` | `TestEncodedBatchCmd_SizeAndMarshalTo` | `source_test_encoded_batch_cmd_size_and_marshal_to` |
| `client_batch_test.go` | `TestEncodeRequestCmd_Basic` | `source_test_encode_request_cmd_basic` |
| `client_batch_test.go` | `TestEncodeRequestCmd_PoolReuse` | `source_test_encode_request_cmd_pool_reuse` |
| `client_batch_test.go` | `TestEncodeRequestCmd_AfterPoolReturn` | `source_test_encode_request_cmd_after_pool_return` |
| `client_batch_test.go` | `TestReuseRequestData_Basic` | `source_test_reuse_request_data_basic` |
| `client_batch_test.go` | `TestReuseRequestData_DoubleReturn` | `source_test_reuse_request_data_double_return` |
| `client_batch_test.go` | `TestEncodedMsgDataPool_ConcurrentSafety` | `source_test_encoded_msg_data_pool_concurrent_safety` |
| `client_fail_test.go` | `TestPanicInRecvLoop` | `source_test_panic_in_recv_loop` |
| `client_fail_test.go` | `TestRecvErrorInMultipleRecvLoops` | `source_test_recv_error_in_multiple_recv_loops` |
| `client_interceptor_test.go` | `TestInterceptedClient` | `source_test_intercepted_client` |
| `client_interceptor_test.go` | `TestAppendChainedInterceptor` | `source_test_append_chained_interceptor` |
| `client_interceptor_test.go` | `TestGetResourceControlInfoHonorsSelectionPolicy` | `source_test_get_resource_control_info_honors_selection_policy` |
| `client_interceptor_test.go` | `TestSendRequestDoesNotSettleAndKeepsRUDetailsOnTransportFailure` | `source_test_send_request_does_not_settle_and_keeps_ru_details_on_transport_failure` |
| `client_interceptor_test.go` | `TestSendRequestSettlesOnSuccess` | `source_test_send_request_settles_on_success` |
| `client_interceptor_test.go` | `TestSendRequestAsyncDoesNotSettleAndKeepsRUDetailsOnTransportFailure` | `source_test_send_request_async_does_not_settle_and_keeps_ru_details_on_transport_failure` |
| `client_interceptor_test.go` | `TestBypassRUV2FollowsRequestInfoBypass` | `source_test_bypass_ruv2_follows_request_info_bypass` |
| `client_test.go` | `TestStreamFirstRecvErrorClosesLease` | `source_test_stream_first_recv_error_closes_lease` |
| `client_test.go` | `TestConn` | `source_test_conn` |
| `client_test.go` | `TestGetConnAfterClose` | `source_test_get_conn_after_close` |
| `client_test.go` | `TestCancelTimeoutRetErr` | `source_test_cancel_timeout_ret_err` |
| `client_test.go` | `TestCompletedTiKVRUV2RPCCount` | `source_test_completed_tikv_ruv2_rpc_count` |
| `client_test.go` | `TestSendWhenReconnect` | `source_test_send_when_reconnect` |
| `client_test.go` | `TestCollapseResolveLock` | `source_test_collapse_resolve_lock` |
| `client_test.go` | `TestForwardMetadataByUnaryCall` | `source_test_forward_metadata_by_unary_call` |
| `client_test.go` | `TestForwardMetadataByBatchCommands` | `source_test_forward_metadata_by_batch_commands` |
| `client_test.go` | `TestBatchCommandsBuilder` | `source_test_batch_commands_builder` |
| `client_test.go` | `TestBatchRequestTerminalOutcome` | `source_test_batch_request_terminal_outcome` |
| `client_test.go` | `TestVisitBatchRequestObservations` | `source_test_visit_batch_request_observations` |
| `client_test.go` | `TestFormatBatchRequestTimeoutReasonNormalizesObservedSentNS` | `source_test_format_batch_request_timeout_reason_normalizes_observed_sent_ns` |
| `client_test.go` | `TestWriteBatchCommandsEntryProgress` | `source_test_write_batch_commands_entry_progress` |
| `client_test.go` | `TestInspectPendingBatchRequests` | `source_test_inspect_pending_batch_requests` |
| `client_test.go` | `TestTraceExecDetails` | `source_test_trace_exec_details` |
| `client_test.go` | `TestBatchClientRecoverAfterServerRestart` | `source_test_batch_client_recover_after_server_restart` |
| `client_test.go` | `TestLimitConcurrency` | `source_test_limit_concurrency` |
| `client_test.go` | `TestPrioritySentLimit` | `source_test_priority_sent_limit` |
| `client_test.go` | `TestBatchClientReceiveHealthFeedback` | `source_test_batch_client_receive_health_feedback` |
| `client_test.go` | `TestRandomRestartStoreAndForwarding` | `source_test_random_restart_store_and_forwarding` |
| `client_test.go` | `TestFastFailRequest` | `source_test_fast_fail_request` |
| `client_test.go` | `TestErrConn` | `source_test_err_conn` |
| `client_test.go` | `TestFastFailWhenNoAvailableConn` | `source_test_fast_fail_when_no_available_conn` |
| `client_test.go` | `TestConcurrentCloseConnPanic` | `source_test_concurrent_close_conn_panic` |
| `client_test.go` | `TestBatchPolicy` | `source_test_batch_policy` |
| `priority_queue_test.go` | `TestPriority` | `source_test_priority` |
| `priority_queue_test.go` | `TestPriorityQueueTakeAllLeavesReferencesInBackingArray` | `source_test_priority_queue_take_all_leaves_references_in_backing_array` |
| `main_test.go` | `TestMain` | Harness disposition: Rust-owned task handles, cancellation, close/restart, panic recovery, and mock-server force-stop gates replace goleak. |

The five grpc-go buffer-pool tests are executable ownership ports rather than skipped dispositions. They prove independent Prost allocations, retained clones after the original is dropped, repeatable typed conversion, single-owner return through `Option::take`, and concurrent message encoding. The priority-queue backing-array test uses drop tracking to prove `take(all)` transfers every owned element and leaves no hidden queue reference. Three metric-bearing aliases use distinct label sets so the original mapped test and the new source-named port can run together without cumulative-counter contamination.

## Original test mapping

Every original test is listed below with the deterministic native behavior executed by its independently named port. Grouped names are supplementary evidence, not substitutes for the one-to-one tests above.

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
| `TestPanicInRecvLoop` | `source_receive_loop_recovers_panics_on_the_same_stream` and `source_short_response_recovers_without_retiring_the_missing_suffix`; the receive loop increments `batch-recv-loop` and resumes `Recv` on that stream. |
| `TestRecvErrorInMultipleRecvLoops` | `source_connection_epoch_retires_only_the_recovery_leaders_host`, `source_epoch_retires_only_the_recovery_leader_when_reopen_cannot_finish`, and dispatcher recreation integration; one connection epoch serializes sibling direct/forwarded recovery without cross-host retirement. |

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
| `TestSendWhenReconnect` | Finite concurrency waits on stream readiness until the caller timeout; `source_connection_selection_skips_a_recreating_pool_slot` and dispatcher recreation integration verify source pool scanning and later recovery. |
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
| `TestLimitConcurrency` | `source_concurrency_limit_is_per_connection_and_scans_round_robin`, priority, unavailable-slot, and reset tests. |
| `TestPrioritySentLimit` | `source_builder_prioritizes_and_allows_high_priority_to_exceed_limit`. |
| `TestBatchClientReceiveHealthFeedback` | `source_health_feedback_listener_is_replaced_and_runs_before_demux`. |
| `TestRandomRestartStoreAndForwarding` | Deterministic direct/forwarded failure isolation and recreation tests replace the nondeterministic stress loop. |
| `TestFastFailRequest` | `TikvConnect` applies the fixed source five-second Endpoint dial timeout; the connector default assertion prevents request-timeout coupling. |
| `TestErrConn` | `source_transport_error_carries_cached_connection_identity` and plan-level compare-close extraction tests. |
| `TestFastFailWhenNoAvailableConn` | `source_default_concurrency_fast_fails_an_unavailable_batch_stream`. |
| `TestConcurrentCloseConnPanic` | `source_client_close_retires_every_pool_once_and_prevents_reconnect` concurrently joins client close and versioned address close, then verifies exactly-once retirement. |
| `TestBatchPolicy` | `source_turbo_batch_policy_presets_and_custom_values_are_preserved`, collection-order, overload, and `source_idle_deadline_does_not_interrupt_collection_after_a_head_arrives` tests. |

### `priority_queue_test.go` and `main_test.go`

| Source test | Rust evidence |
| --- | --- |
| `TestPriority` | `source_priority_take_and_cancelled_cleanup_contract`. |
| `TestPriorityQueueTakeAllLeavesReferencesInBackingArray` | Rust's draining ownership leaves no interface references in a backing array; the same test verifies the queue is empty after take/cleanup. The Go GC-retention failure mode is unrepresentable without unsafe code. |
| `TestMain` | Go's goleak wrapper maps to owned `JoinHandle`/cancellation lifetimes. Explicit close, idle retirement, send/receive panic recovery, stream recreation, and mock-server force-stop tests prove every package-created task has a shutdown owner. |

## Validation boundary

The Rust tests use real loopback Tonic transports for unary, stream, BatchCommands, metadata, restart, and close behavior; deterministic state tests replace Go's random restart stress. A live TiKV/PD cluster is not required for this package-owned transport contract. Proxy selection and region retry remain on `internal/locate`; high-level transaction behavior remains on its owning package rows.

Final validation uses Go 1.25.12 and `nightly-2026-08-22`:

    go test ./internal/client -count=1
    # passed in 34.541s

    go test -race ./internal/client -count=1
    # passed in 42.246s

    cargo +nightly-2026-08-22 test --quiet --lib --no-default-features source_
    # 844 passed; 0 failed

    cargo +nightly-2026-08-22 test --quiet --lib --all-features source_
    # 841 passed; 0 failed

    cargo +nightly-2026-08-22 test --quiet --workspace --no-default-features
    # main library: 1,091 passed; 0 failed; 1 ignored; all companion and doctest targets passed

    cargo +nightly-2026-08-22 test --quiet --lib --all-features
    # 1,088 passed; 0 failed; 1 ignored

    cargo +nightly-2026-08-22 check --workspace --all-targets --all-features
    cargo +nightly-2026-08-22 clippy --workspace --all-targets --all-features -- -D warnings
    RUSTDOCFLAGS=-Dwarnings cargo +nightly-2026-08-22 doc --workspace --all-features --no-deps --document-private-items
    cargo +nightly-2026-08-22 test --doc --all-features
    # passed; 51 doctests

    cargo +nightly-2026-08-22 fmt --all -- --check
    git diff --check

Mechanical source verification resolves the client-go checkout to `52c1e76cec993571493c81de442bcbef90cdc106`, finds exactly 19 `internal/client` artifacts, 51 distinct source `Test...` declarations, 32 direct importer files, and exactly 50 independently named ordinary Rust ports. `TestMain` is the sole harness disposition. The stronger ports found no production divergence; all production and consumer assignments from the 2026-08-25 re-audit remain valid.
