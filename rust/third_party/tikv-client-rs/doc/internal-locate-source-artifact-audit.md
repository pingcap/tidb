# `internal/locate` source-artifact audit

This is the atomic completion receipt for client-go package `internal/locate` at commit `52c1e76cec993571493c81de442bcbef90cdc106`. Client-go is the source of truth. The receipt covers every package artifact, every production surface, all 147 original test declarations, external protocol/dependency inputs, consumers, and the native Rust integration decisions.

## Complete source inventory

The package contains exactly 17 Go files and 20,132 lines: nine production files and eight test/support files. There is no `doc.go`, build-tag or platform variant, generated input/output, fixture, benchmark, example, package build file, or package-local metadata artifact.

| Source artifact | Lines | SHA-256 | Rust owner |
| --- | ---: | --- | --- |
| `internal/locate/accessmode.go` | 62 | `cf2db2d551d5437faee1b217395bb7ff64b105e61840d543901c5e1eb7c733a3` | `src/locate.rs`, `src/kv/store_vars.rs` |
| `internal/locate/metrics_collector.go` | 136 | `a0b1f4378a94a0da0dbb0b877fb5c5f5bb24349c5bed8a4aca96cc5413e2f5e5` | `src/traffic.rs`, physical dispatch in `src/request/plan.rs` |
| `internal/locate/pd_codec.go` | 219 | `47972f38028df65a182c0dc15e13ea123cfcf64e81f17e87a0d496dbcb030d41` | `src/pd/codec.rs`, `src/request/keyspace.rs`, `src/pd/client.rs` |
| `internal/locate/region_cache.go` | 3,472 | `c4f04a1ed08ddfb80ff7735e72820d7fee14359fdb9076e627a6c3f6b7a17755` | `src/region.rs`, `src/region_cache.rs`, `src/pd/{client,retry}.rs`, `src/store/mod.rs` |
| `internal/locate/region_request.go` | 2,449 | `30f61ab58725c11e63fdc3999bc300d4447d27d9d8790962c1a470e8bba36c13` | `src/region_request.rs`, `src/request/{plan,plan_builder,shard}.rs`, `src/store/{client,mod}.rs` |
| `internal/locate/replica_selector.go` | 744 | `72d5bae389afee9bd254a18a6e26c0c641b2e76ade9f682b16d13dae1b52c8e2` | `src/locate.rs`, `src/pd/client.rs`, `src/request/plan.rs` |
| `internal/locate/slow_score.go` | 192 | `c6ad9dc0e28cbca2f2d8fe1b9dce2f200eaefef4fda863c953aa0f058cf493ad` | `src/locate.rs` |
| `internal/locate/sorted_btree.go` | 151 | `1f02b13b1da270d37410b07e3ac200b63ae3934a83952c611ebd3620d387dab4` | ordered/version/ID indexes in `src/region_cache.rs` |
| `internal/locate/store_cache.go` | 1,222 | `9161d61825fb2dd7a0cf754347664a7379f72139abe0c1a63bdd1bad59c24dfe` | `src/region_cache.rs`, `src/locate.rs`, `src/pd/client.rs`, `src/store/endpoint.rs` |
| `internal/locate/main_test.go` | 32 | `9e5983e12991fcea417b0bba33550436f6b4eba7572e9d71e0abd49c26222d42` | owned Tokio task, cancellation, transport, cache, and mock-server lifetimes |
| `internal/locate/metrics_collector_test.go` | 157 | `f955505306aec2209afc55203bdd8538c9d5c012f2f24efc6ff5833caae875d2` | source network-collector tests in `src/traffic.rs` |
| `internal/locate/pd_codec_test.go` | 40 | `5238610c6201442746c7fa19352b15974a636acf224c2333a88ad97b5330164e` | source codec/keyspace tests in `src/pd/{codec,client}.rs` and `src/request/keyspace.rs` |
| `internal/locate/region_cache_test.go` | 3,497 | `01ea61ab384fa514b3fcb9c2f4b47aa950bc2f9dc3dba4fb7a629c5b43623b3e` | source region/cache/store tests in `src/{region,region_cache,locate}.rs` and `src/pd/client.rs` |
| `internal/locate/region_request3_test.go` | 1,800 | `a3c19f0a612461b52178e28727c1fc734785fbbb7e56622481904038dc2bada6` | selector/sender integration tests in `src/{locate,region_cache}.rs`, `src/pd/client.rs`, and `src/request/plan.rs` |
| `internal/locate/region_request_state_test.go` | 769 | `00f0b6fa2029711ef5ec41c540647acfa21e512918e0637e35db7415e0ab1e87` | stale/replica state tests in `src/locate.rs`, `src/pd/client.rs`, and `src/request/plan.rs` |
| `internal/locate/region_request_test.go` | 1,274 | `b54b485970507a13f74ff87c6fcf2bb37b343811d511ebab3af0e42f3ad34b3d` | sender/error/runtime/transport tests in `src/{region_request,region_cache}.rs`, `src/request/plan.rs`, and `src/store` |
| `internal/locate/replica_selector_test.go` | 3,916 | `4d606b54f40504d49608b285853712d9142462df80a98cdf5bf79e3473c1ab7f` | exhaustive source-derived selector state and route tests in `src/locate.rs`, `src/pd/client.rs`, and `src/request/plan.rs` |

## Production symbol and behavior mapping

| Source file/surface | Rust mapping and integration decision |
| --- | --- |
| `accessmode.go` | `AccessMode` preserves TiKV-only, TiFlash-only, and all-store names and routing separation. `nextgen` disables replica/stale access while retaining explicit store/label filters, matching the source build selection. |
| `metrics_collector.go` | `NetworkTrafficDetails` implements the complete request/response size matrices, KV versus MPP totals, cross-zone accounting, stale-read request/byte counters, and leader/follower local/remote observations. It is attached at physical dispatch and survives shard/retry clones. |
| `pd_codec.go` | `CodecPdClient` and `PdRegionCodec` cover V1 raw identity, V1 transactional memcomparable coding, numeric V2 keyspace prefixes, key/previous/ID/scan/batch-scan/split operations, in-place region/bucket decoding, canonical keyspace metadata, disabled/V3 rejection, caller wrapping, and the exact V2 keyspace-end bound. |
| Region identity, range, buckets, and labels | `RegionWithLeader`, `RegionVerId`, and `Bucket` preserve half-open and inclusive-end containment, version identity/display, leaders, pending/down/witness filtering, bucket lookup/clamping/version replacement, four named TiFlash label filters, and immutable PD metadata. Client-go mutates a private peer slice while constructing a region; Rust filters the immutable PD snapshot at every candidate/TiFlash route, yielding the same selectable peers without destroying diagnostic metadata. |
| Region cache lifetime/configuration | Process-wide TTL and jitter controls, exact strict expiration/renewal, all four synchronization flags, preload/full refresh versus bounded GC, retained GC cursor, background cancellation/join, bucket-refresh singleflight, by-ID singleflight, and zero-interval disabling are implemented in `RegionCache`. |
| Region indexes | Native `BTreeMap` plus version and region-ID maps replace `google/btree`. Replacement, stale epoch refusal, maximum empty end key, intersecting removal, ordered cache scans, holes, clear/refresh, TTL checks, and coherent three-index removal preserve `SortedRegions` behavior. Rust's standard tree is memory-safe and needs no custom `btree.Item` wrapper. |
| Region loading/location | Point, inclusive-end, cache-only, exact-version, direct-by-ID PD, key-range, ID-range, repeated range, bounded scan, batch scan, old-PD fallback, gap validation, leader requirement, split-range continuation, cached/fresh merging, `GroupKeysByRegion` boundary filtering, and rejected-stale-load retry are integrated. |
| PD region-meta circuit breaker | Public Rust settings and change hook preserve the source disabled-by-default threshold, 30-second error window, 10-QPS minimum, 10-second cooldown, one-success half-open probe, window-delayed setting changes, closed/open/half-open transitions, and fast failure. Only gRPC `DeadlineExceeded`, `Unavailable`, and `ResourceExhausted` count as overload, exactly matching the pinned PD interceptor. The state is attached only to region metadata calls. |
| Store cache and discovery | Store insertion/resolution/tombstone states, metadata refresh in place, labels, endpoint type, address/peer address, all-store/TiFlash/TiFlash-compute discovery, compute reload invalidation, event triggering, store-list refresh, and stale metric cleanup are cache-owned. |
| Liveness, health, and load | Process-wide liveness timeout, singleflight probes, reachable/unknown/unreachable states, health-loop ownership, periodic metadata re-resolution, failure epochs, client/TiKV slow scores, ten-sample sliding statistics, health feedback, server wait estimate decay, and replica-flow counters are integrated with physical transport. |
| RPC context and routing | `RegionStore` separates logical peer/store from physical destination/proxy, retains endpoint/access location, replica/stale identity, bucket version, token ownership, forwarding host, store epoch, and health state across plan clones. Leader/follower/mixed/learner/TiFlash/all-store paths consume the same cache snapshot. |
| `replica_selector.go` | `ReplicaSelectorState` and cache-backed candidate selection implement source scoring, labels/stores, slow/unreachable/down/tombstone/witness exclusion, random-tie-equivalent seed selection, ten-attempt/fifty-second leader budget, attempt/error flags, suspected leaders, follower stale retry, busy leader probe, DataIsNotReady retry, flashback leader forcing, NextGen behavior, forwarding proxy reuse/rotation/exhaustion, pending per-store backoff, and exact route mutation. |
| `region_request.go` sender | Typed futures replace the duplicate sync/callback façades while preserving one physical state machine: cancellation, timeout, liveness, compare-close, region/store error precedence, retry/backoff classes, configurable timeout fast paths, epoch/bucket updates, resend markers, source labels, cluster identity, read-TS validation, token limits/metrics, resource-control ordering, runtime statistics, replica traces, and terminal errors. `UndeterminedResult` is checked before all typed region errors and before cache/selector side effects. |
| Runtime diagnostics | `RegionRequestRuntimeStats` preserves first-seen command order, counts/durations, clone/merge, the 16-label region-error bound and repetition rule, five detailed replica accesses then per-peer counts, source protobuf field-order labels, NotLeader suffixes, RPC context formatting, and duration formatting. |

## External inputs and native boundaries

Client-go pins kvproto `059694ae4472276644613acccefa24cbc89d959f` and PD client `afa43111d1494d620c225e51461097097661d127`. The package directly consumes API/keyspace, coprocessor, disaggregated, error, KV RPC, region/store, MPP, PD, and TiKV service messages. All ten required proto inputs are byte-identical to that kvproto pin and their checked-in Prost/Tonic outputs were regenerated:

| Protocol input | SHA-256 |
| --- | --- |
| `proto/apipb.proto` | `ec7e7672893c2c44544b10b8d591a2113f5ac851ce25d78422699df46b115a91` |
| `proto/coprocessor.proto` | `047bf9a5593908327fb0e9f87a9def843ddcc03c75236ab0a4f5b50fda5aa158` |
| `proto/disaggregated.proto` | `87f4a3c6ee5e742cf07111d8608155de359324e1168bb2944de0d3c168485c5f` |
| `proto/errorpb.proto` | `9a217e2ab8a8a77ab407a508ee3224a26c5f99a9911192b1acc52d3d9c93e1ea` |
| `proto/keyspacepb.proto` | `33dacfe45a870857eb401a5e6a5c525120e7ade07663c889f33b75d5208c1f15` |
| `proto/kvrpcpb.proto` | `d107a80efae8c17afd39f9274c0688b1bdfdd9c6819481aec73a3fbe8e963a2c` |
| `proto/metapb.proto` | `e1f5ea1f9f7701d087847a6a18385f3fa25f3355996fedc8435c55b8bca3a045` |
| `proto/mpp.proto` | `479798510cbd229b718bf5699e58bc6eea45d9d917d53b9313f6b4af8d81a166` |
| `proto/pdpb.proto` | `6632daa9db20aa416be5fcee2a66f2da86552ded250bb8960647fda41b544830` |
| `proto/tikvpb.proto` | `4549bc2657d6ecb67407f4aa6f18a6cce41e9485cca975a6ee6dbbbd7efe2615` |

The completed `internal/apicodec`, `internal/client`, and `tikvrpc` receipts own their exhaustive request/service matrices. This package consumes the typed bindings and owns routing/error semantics rather than duplicating generated code.

The pinned PD circuit-breaker dependency was inventoried with the package: the native state machine preserves its settings, request-time transitions, old-state in-flight result accounting, and overload classification. PD-library Prometheus registration is dependency observability rather than a package-local source artifact; client-rust exposes the behavioral settings hook and typed fast-fail error.

Go's synchronous and asynchronous test variants call the same sender algorithms. Rust has one future-based implementation, so each `UsingAsyncAPI` declaration maps to the same production path and deterministic evidence as its paired test. Go's randomized loops are represented by deterministic state tables, seeded route choices, and bounded concurrency tests. No source assertion is discarded because of that native consolidation.

## Original test mapping

All 147 original declarations are named below. A comma-separated source group maps to the listed deterministic Rust evidence; paired sync/async declarations intentionally share evidence because Rust has only the future path.

### Harness, metrics, codec, and stale-state tests (6)

| Source declarations | Rust evidence |
| --- | --- |
| `TestMain` | Every cache/health/refresh/batch/mock-server task has cancellation/drop and join ownership; close, idle retirement, panic recovery, stream recreation, and force-stop tests cover package-created tasks. |
| `TestNetworkCollectorOnReq`, `TestNetworkCollectorOnResp` | `source_network_collector_request_and_response_accounting`, `source_network_collector_cross_zone_mpp_and_replica_metrics`, and physical dispatch integration. |
| `TestGetKeyspaceIDRejectsV3Identity` | `source_get_keyspace_id_loads_canonical_name_and_rejects_v3_identity`, codec V1/V2 matrices, and malformed decode classification. |
| `TestRegionCacheStaleRead`, `TestRegionCacheStaleReadUsingAsyncAPI` | Source selector-state, stale-timeout follower, lock-triggered stale disabling, attempt-path, route-context, and resend tests. |

### `region_cache_test.go` (67)

| Source declarations | Rust evidence |
| --- | --- |
| `TestBackgroundRunner`, `TestRefreshCache`, `TestRegionCacheStartNonEmpty`, `TestRefreshCacheConcurrency`, `TestRegionCacheValidAfterLoading` | Bounded GC cursor/coherent expiry, preload/full-index replacement, periodic lifecycle, notification preservation, zero-interval, and close/join regressions. |
| `TestRegionCache`, `TestSimple`, `TestContains`, `TestContainsByEnd`, `TestListRegionIDsInCache`, `TestScanRegions`, `TestLocateRegionByIDFromPD` | Point/end/ID/range/cache-only/exact-version lookup, half-open/inclusive boundaries, cache hits/misses, direct PD bypass, and range-ID tests. |
| `TestStoreLabels`, `TestResolveStateTransition`, `TestReconnect`, `TestStoreRestartWithNewLabels`, `TestHealthCheckWithAddressChange` | Exact label matching, resolve/tombstone transitions, in-place metadata refresh, address change, store epochs, and connection identity tests. |
| `TestReturnRegionWithNoLeader`, `TestFilterDownPeersOrPeersOnTombstoneOrDroppedStores`, `TestPeersLenChange`, `TestPeersLenChangedByWitness`, `TestLoadRegionsWithLeader` | Leader requirement and down/tombstone/dropped/witness filtering in cache, candidate, and TiFlash paths. |
| `TestNeedExpireRegionAfterTTL`, `TestTiFlashRecoveredFromDown`, `TestSendFailedInHibernateRegion`, `TestBackgroundCacheGC` | TTL freeze/renew/expiry, delayed flags, unhealthy store snapshots, TiFlash recovery, and bounded GC evidence. |
| `TestUpdateLeader`, `TestUpdateLeader2`, `TestUpdateLeader3`, `TestSwitchPeerWhenNoLeader`, `TestFollowerReadFallback`, `TestMixedReadFallback`, `TestLabelSelectorTiKVPeer` | Concrete/hintless leaders, out-of-region hints, candidate labels, access modes, fallback, and cached leader preservation. |
| `TestSendFailedButLeaderNotChange`, `TestSendFailInvalidateRegionsInSameStore`, `TestSendFailedInMultipleNode`, `TestShouldNotRetryFlashback` | Store-epoch invalidation scope, unchanged leader, multi-store failure, flashback terminal handling, and selector side effects. |
| `TestSplit`, `TestMerge`, `TestRemoveIntersectingRegions`, `TestRegionEpochAheadOfTiKV`, `TestRegionEpochOnTiFlash`, `TestSplitThenLocateInvalidRegion`, `TestSplitThenLocateRegionNeedReloadOnAccess`, `TestSplitThenLocateRegionNeedDelayedReload` | Stale epoch rejection, intersection removal, exact-epoch preservation, TiFlash epoch behavior, and immediate/delayed reload transitions. |
| `TestBatchLoadRegions`, `TestBatchScanRegionsMerger`, `TestSplitKeyRanges`, `TestBatchScanRegions`, `TestBatchScanRegionsFallback`, `TestRangesAreCoveredCheck`, `TestScanRegionsWithGaps`, `TestBatchLoadLimitRanges` | Complete source tables for cached/fresh merging, split continuation, range caps, leader/bucket options, gap detection/retry, and Unimplemented fallback. |
| `TestNoBackoffWhenFailToDecodeRegion`, `TestIssue1401` | Typed API decode errors and stale metadata are classified before PD retry; malformed/empty responses retain source failure behavior. |
| `TestBuckets`, `TestLocateBucket`, `TestBucketClampingToRegion`, `TestUpdateBucketsConcurrently` | Bucket versioning, lookup, stale-hole fallback, region clamping, source boundary tables, background refresh deduplication, and older-version refusal. |
| `TestSlowScoreStat`, `TestTiKVSideSlowScore`, `TestStoreHealthStatus`, `TestRegionCacheHandleHealthStatus` | Sliding slow score, client/TiKV timing gates, stale decay, health feedback, active refresh, server-load estimate, and combined health status. |
| `TestRegionCacheWithDelay`, `TestInsertStaleRegion`, `TestStaleGetRegion`, `TestFollowerGetStaleRegion` | Delayed PD responses, newer intersecting metadata winning insertion, second-load retry, and leader/follower stale outcomes. |

### `region_request_test.go` (27)

| Source declarations | Rust evidence |
| --- | --- |
| `TestRegionRequestToSingleStore`, `TestSendReqCtx`, `TestSendReqAsync` | The typed future sender, retained shard route, physical dispatch, loopback unary/batch paths, response extraction, and timeout ownership. |
| `TestOnRegionError`, `TestOnMaxTimestampNotSyncedError` | Complete region-error label/action tables, mixed-field precedence, terminal/retry classes, epoch/bucket/cache updates, and max-timestamp retry. |
| `TestOnSendFailByResourceGroupThrottled`, `TestKVReadTimeoutWithDisableBatchClient` | Resource-group throttling and physical timeout classifications preserve source retry/backoff boundaries. |
| `TestOnSendFailedWithStoreRestart`, `TestOnSendFailedWithStoreRestartUsingAsyncAPI`, `TestOnSendFailedWithCloseKnownStoreThenUseNewOne`, `TestOnSendFailedWithCloseKnownStoreThenUseNewOneUsingAsyncAPI`, `TestCloseConnectionOnStoreNotMatch` | Store version/epoch snapshots, liveness, compare-close, re-resolution, connection retirement, and forwarding target isolation. |
| `TestOnSendFailedWithCancelled`, `TestOnSendFailedWithCancelledUsingAsyncAPI`, `TestNoReloadRegionWhenCtxCanceled`, `TestNoReloadRegionWhenCtxCanceledUsingAsyncAPI`, `TestNoReloadRegionForGrpcWhenCtxCanceled` | Cancellation is terminal before liveness/cache/store side effects for direct and gRPC-cancelled paths. |
| `TestGetRegionByIDFromCache`, `TestClusterIDInReq`, `TestClientExt` | Cached by-ID routing, cluster/keyspace/context attachment, and native typed client extension points. |
| `TestBatchClientSendLoopPanic` | Completed transport receipt plus sender integration proves panic recovery, pending failure, recreation, and later dispatch. |
| `TestRegionRequestSenderString`, `TestRegionRequestStats`, `TestGetErrMsg`, `TestRPCContextString`, `TestBackoffErrWithRPCContext` | Runtime stats, 16-error cap, replica trace, source error labels/suffixes, route/context strings, and contextual backoff formatting. |
| `TestRegionRequestValidateReadTS` | Source scope timestamp validation blocks transport and preserves exact request ownership. |

### `region_request3_test.go` (21)

| Source declarations | Rust evidence |
| --- | --- |
| `TestRegionRequestToThreeStores`, `TestReplicaSelector`, `TestSendReqWithReplicaSelector` | Cache-backed candidate snapshots, selector state, logical/physical routes, attempt accounting, route mutation, and integrated dispatch. |
| `TestStoreTokenLimit` | Shared per-logical-store optimistic token counter, all-store bypass, address/store-labeled rejection metric, and RAII release after every result. |
| `TestSwitchPeerWhenNoLeader`, `TestSwitchPeerWhenNoLeaderErrorWithNewLeaderInfo`, `TestReplicaReadFallbackToLeaderRegionError` | Hintless/concrete leaders, untried peer switching, leader final chance, follower probing, and region-error fallback. |
| `TestForwarding` | Cached proxy reuse, one-attempt gate, rotation, physical versus logical failure, forwarded metadata, and reload/store-epoch exhaustion. |
| `TestLearnerReplicaSelector`, `TestLoadBasedReplicaRead`, `TestPreferLeader` | Learner/mixed/prefer-leader scores, label priority, busy thresholds, slow exclusion, and deterministic tied choice. |
| `TestReplicaReadWithFlashbackInProgress`, `TestAccessFollowerAfter1TiKVDown`, `TestDoNotTryUnreachableLeader`, `TestTiKVRecoveredFromDown` | Flashback leader forcing, unavailable leader follower access, liveness probing, health-loop recovery, and source cache preservation. |
| `TestSendReqFirstTimeout`, `TestStaleReadTryFollowerAfterTimeout`, `TestLeaderStuck` | Configurable timeout fast path, stale follower retry, ten-attempt/fifty-second budget, suspect/busy leader probes, and pending backoff. |
| `TestLogging`, `TestRetryRequestSource`, `TestStaleReadMetrics` | Source region-error labels/NotLeader suffix, resend context flag and request source, runtime stats, traffic and stale-read metrics. |

### `replica_selector_test.go` (26)

| Source declarations | Rust evidence |
| --- | --- |
| `TestReplicaSelectorBasic`, `TestReplicaSelectorCalculateScore`, `TestCanFastRetry`, `TestPendingBackoff`, `TestReplicaFlag` | Selector initialization, exact scoring/ties, attempt/error flags, fast-retry gates, store-keyed pending backoff replacement/consumption, and leader final chance. |
| `TestNextGenReadFeaturesDisabled` | Build/config selection disables follower/stale features while retaining stores/labels filters. |
| `TestReplicaReadAccessPathByCase`, `TestReplicaReadAccessPathByCaseUsingAsyncAPI`, `TestReplicaReadAccessPathByCase2`, `TestReplicaReadAccessPathByCase2UsingAsyncAPI` | Source case tables map NotLeader, busy, stale, DataIsNotReady, transport, and attempt transitions through the one Rust future path. |
| `TestReplicaReadAccessPathByBasicCase`, `TestReplicaReadAccessPathByBasicCaseUsingAsyncAPI`, `TestReplicaReadAccessPathByLeaderCase`, `TestReplicaReadAccessPathByLeaderCaseUsingAsyncAPI`, `TestReplicaReadAccessPathByFollowerCase`, `TestReplicaReadAccessPathByFollowerCaseUsingAsyncAPI` | Leader/follower policy, threshold, fallback, repeated attempt, concrete leader, and unavailable store route sequences. |
| `TestReplicaReadAccessPathByMixedAndPreferLeaderCase`, `TestReplicaReadAccessPathByMixedAndPreferLeaderCaseUsingAsyncAPI`, `TestReplicaReadAccessPathByTryIdleReplicaCase`, `TestReplicaReadAvoidSlowStore` | Mixed/prefer-leader/idle/slow-store scores, labels, busy exclusion, fallback, and health gates. |
| `TestReplicaReadAccessPathByStaleReadCase`, `TestReplicaSelectorLeaderBusyProbe`, `TestReplicaReadAccessPathByFlashbackInProgressCase` | Stale timeout transition, second-rejection busy leader probe, and threshold-free flashback leader retry. |
| `TestReplicaReadAccessPathByProxyCase` | Forwarding proxy selection/reuse/rotation/exhaustion, physical failures, and reload/store epoch effects. |
| `TestReplicaReadAccessPathByLearnerCase` | Learner-only and mixed learner scoring with down/tombstone/witness and attempt filtering. |
| `TestTiKVClientReadTimeout` | Configurable read timeout is strictly below the source short timeout and preserves deadline-specific retry. |

## Consumer audit

All 28 Go files importing `internal/locate` were assigned. Completing this package does not promote its consumers.

| Consumer group | Exact source files and ownership |
| --- | --- |
| RawKV | `rawkv/rawkv.go`, `rawkv/rawkv_test.go`, `rawkv/test_prob.go`; retains its independent ledger status and consumes typed cache/sender routing. |
| Root `tikv` | `tikv/gc.go`, `tikv/interface.go`, `tikv/kv.go`, `tikv/region.go`, `tikv/split_region.go`, `tikv/test_util.go`; has a separate completed receipt and owns public wrappers/orchestration. |
| Internal KV batching | `internal/kvrpc/batch.go`; consumes region grouping/routing and retains its own package status. |
| Range tasks | `txnkv/rangetask/delete_range.go`; its complete package consumes the now-complete locate boundary. |
| Transaction locks | `txnkv/txnlock/lock_resolver.go`, `txnkv/txnlock/test_probe.go`; lock ownership remains independent. |
| Transaction package | `txnkv/transaction/2pc.go`, `commit.go`, `pessimistic.go`, `pipelined_flush.go`, `prewrite.go`, `test_probe.go`, `test_util.go`, `txn_file.go`, `txn_file_test.go`, `txn_test.go`; has a separate completed receipt and owns transaction algorithms. |
| Snapshot package | `txnkv/txnsnapshot/client_helper.go`, `scan.go`, `snapshot.go`, `snapshot_async.go`, `test_probe.go`; has a separate completed receipt and owns snapshot/scanner state. |

## Validation boundary

The package is exercised through deterministic cache/selector tests and completed real loopback Tonic transport tests. A live TiKV/PD cluster is not required for package-owned selection, retry, index, error, coding, and lifecycle behavior; cross-client live-cluster workflows remain a final repository gate for high-level raw, snapshot, transaction, and root `tikv` consumers.

The 147 original declarations are fully mapped above, including duplicated sync/async façades and the goleak harness. The configured Go 1.25.12 toolchain subsequently passed the complete pinned `go test --tags=intest ./...` and `go test -race --tags=intest ./...` repository suites. Both default and all-feature Rust suites, all-target checking, rustdoc, formatting, and diff validation remain required by the ledger receipt.
