// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Complete source inventory for `metrics/shortcuts.go`.

/// The source handle type of one pre-bound metric shortcut.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShortcutKind {
    /// A histogram or summary observer.
    Observer,
    /// A counter child.
    Counter,
    /// A gauge child.
    Gauge,
}

/// Exact source metadata for one declared shortcut global.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShortcutSpec {
    /// Go shortcut global name.
    pub source_name: &'static str,
    /// Source handle type.
    pub kind: ShortcutKind,
    /// Parent metric global, or `None` when the source leaves this shortcut nil.
    pub metric_source_name: Option<&'static str>,
    /// Ordered values bound to the parent metric's variable labels.
    pub label_values: &'static [&'static str],
}

impl ShortcutSpec {
    /// Returns whether client-go's `initShortcuts` initializes this declaration.
    pub const fn initialized(self) -> bool {
        self.metric_source_name.is_some()
    }
}

macro_rules! shortcut_spec {
    ($source:literal, $kind:expr, $metric:literal, $labels:expr) => {
        ShortcutSpec {
            source_name: $source,
            kind: $kind,
            metric_source_name: Some($metric),
            label_values: $labels,
        }
    };
}

macro_rules! uninitialized_shortcut_spec {
    ($source:literal, $kind:expr) => {
        ShortcutSpec {
            source_name: $source,
            kind: $kind,
            metric_source_name: None,
            label_values: &[],
        }
    };
}

/// All 151 globals declared by pinned `metrics/shortcuts.go`, in source order.
pub const CLIENT_GO_SHORTCUT_SPECS: &[ShortcutSpec] = &[
    shortcut_spec!(
        "TxnCmdHistogramWithCommitInternal",
        ShortcutKind::Observer,
        "TiKVTxnCmdHistogram",
        &["commit", "internal"]
    ),
    shortcut_spec!(
        "TxnCmdHistogramWithCommitGeneral",
        ShortcutKind::Observer,
        "TiKVTxnCmdHistogram",
        &["commit", "general"]
    ),
    shortcut_spec!(
        "TxnCmdHistogramWithRollbackInternal",
        ShortcutKind::Observer,
        "TiKVTxnCmdHistogram",
        &["rollback", "internal"]
    ),
    shortcut_spec!(
        "TxnCmdHistogramWithRollbackGeneral",
        ShortcutKind::Observer,
        "TiKVTxnCmdHistogram",
        &["rollback", "general"]
    ),
    shortcut_spec!(
        "TxnCmdHistogramWithBatchGetInternal",
        ShortcutKind::Observer,
        "TiKVTxnCmdHistogram",
        &["batch_get", "internal"]
    ),
    shortcut_spec!(
        "TxnCmdHistogramWithBatchGetGeneral",
        ShortcutKind::Observer,
        "TiKVTxnCmdHistogram",
        &["batch_get", "general"]
    ),
    shortcut_spec!(
        "TxnCmdHistogramWithGetInternal",
        ShortcutKind::Observer,
        "TiKVTxnCmdHistogram",
        &["get", "internal"]
    ),
    shortcut_spec!(
        "TxnCmdHistogramWithGetGeneral",
        ShortcutKind::Observer,
        "TiKVTxnCmdHistogram",
        &["get", "general"]
    ),
    shortcut_spec!(
        "TxnCmdHistogramWithLockKeysInternal",
        ShortcutKind::Observer,
        "TiKVTxnCmdHistogram",
        &["lock_keys", "internal"]
    ),
    shortcut_spec!(
        "TxnCmdHistogramWithLockKeysGeneral",
        ShortcutKind::Observer,
        "TiKVTxnCmdHistogram",
        &["lock_keys", "general"]
    ),
    shortcut_spec!(
        "TxnCmdHistogramWithSharedLockKeysInternal",
        ShortcutKind::Observer,
        "TiKVTxnCmdHistogram",
        &["shared_lock_keys", "internal"]
    ),
    shortcut_spec!(
        "TxnCmdHistogramWithSharedLockKeysGeneral",
        ShortcutKind::Observer,
        "TiKVTxnCmdHistogram",
        &["shared_lock_keys", "general"]
    ),
    shortcut_spec!(
        "RawkvCmdHistogramWithGet",
        ShortcutKind::Observer,
        "TiKVRawkvCmdHistogram",
        &["get"]
    ),
    shortcut_spec!(
        "RawkvCmdHistogramWithBatchGet",
        ShortcutKind::Observer,
        "TiKVRawkvCmdHistogram",
        &["batch_get"]
    ),
    shortcut_spec!(
        "RawkvCmdHistogramWithBatchPut",
        ShortcutKind::Observer,
        "TiKVRawkvCmdHistogram",
        &["batch_put"]
    ),
    shortcut_spec!(
        "RawkvCmdHistogramWithDelete",
        ShortcutKind::Observer,
        "TiKVRawkvCmdHistogram",
        &["delete"]
    ),
    shortcut_spec!(
        "RawkvCmdHistogramWithBatchDelete",
        ShortcutKind::Observer,
        "TiKVRawkvCmdHistogram",
        &["batch_delete"]
    ),
    shortcut_spec!(
        "RawkvCmdHistogramWithRawScan",
        ShortcutKind::Observer,
        "TiKVRawkvCmdHistogram",
        &["raw_scan"]
    ),
    shortcut_spec!(
        "RawkvCmdHistogramWithRawReversScan",
        ShortcutKind::Observer,
        "TiKVRawkvCmdHistogram",
        &["raw_reverse_scan"]
    ),
    shortcut_spec!(
        "RawkvSizeHistogramWithKey",
        ShortcutKind::Observer,
        "TiKVRawkvSizeHistogram",
        &["key"]
    ),
    shortcut_spec!(
        "RawkvSizeHistogramWithValue",
        ShortcutKind::Observer,
        "TiKVRawkvSizeHistogram",
        &["value"]
    ),
    shortcut_spec!(
        "RawkvCmdHistogramWithRawChecksum",
        ShortcutKind::Observer,
        "TiKVRawkvSizeHistogram",
        &["raw_checksum"]
    ),
    shortcut_spec!(
        "BackoffHistogramRPC",
        ShortcutKind::Observer,
        "TiKVBackoffHistogram",
        &["tikvRPC"]
    ),
    shortcut_spec!(
        "BackoffHistogramLock",
        ShortcutKind::Observer,
        "TiKVBackoffHistogram",
        &["txnLock"]
    ),
    shortcut_spec!(
        "BackoffHistogramLockFast",
        ShortcutKind::Observer,
        "TiKVBackoffHistogram",
        &["tikvLockFast"]
    ),
    shortcut_spec!(
        "BackoffHistogramPD",
        ShortcutKind::Observer,
        "TiKVBackoffHistogram",
        &["pdRPC"]
    ),
    shortcut_spec!(
        "BackoffHistogramRegionMiss",
        ShortcutKind::Observer,
        "TiKVBackoffHistogram",
        &["regionMiss"]
    ),
    shortcut_spec!(
        "BackoffHistogramRegionScheduling",
        ShortcutKind::Observer,
        "TiKVBackoffHistogram",
        &["regionScheduling"]
    ),
    shortcut_spec!(
        "BackoffHistogramServerBusy",
        ShortcutKind::Observer,
        "TiKVBackoffHistogram",
        &["serverBusy"]
    ),
    shortcut_spec!(
        "BackoffHistogramTiKVDiskFull",
        ShortcutKind::Observer,
        "TiKVBackoffHistogram",
        &["tikvDiskFull"]
    ),
    shortcut_spec!(
        "BackoffHistogramRegionRecoveryInProgress",
        ShortcutKind::Observer,
        "TiKVBackoffHistogram",
        &["regionRecoveryInProgress"]
    ),
    shortcut_spec!(
        "BackoffHistogramStaleCmd",
        ShortcutKind::Observer,
        "TiKVBackoffHistogram",
        &["staleCommand"]
    ),
    shortcut_spec!(
        "BackoffHistogramDataNotReady",
        ShortcutKind::Observer,
        "TiKVBackoffHistogram",
        &["dataNotReady"]
    ),
    shortcut_spec!(
        "BackoffHistogramIsWitness",
        ShortcutKind::Observer,
        "TiKVBackoffHistogram",
        &["isWitness"]
    ),
    shortcut_spec!(
        "BackoffHistogramEmpty",
        ShortcutKind::Observer,
        "TiKVBackoffHistogram",
        &[""]
    ),
    shortcut_spec!(
        "TxnRegionsNumHistogramWithSnapshotInternal",
        ShortcutKind::Observer,
        "TiKVTxnRegionsNumHistogram",
        &["snapshot", "internal"]
    ),
    shortcut_spec!(
        "TxnRegionsNumHistogramWithSnapshot",
        ShortcutKind::Observer,
        "TiKVTxnRegionsNumHistogram",
        &["snapshot", "general"]
    ),
    shortcut_spec!(
        "TxnRegionsNumHistogramPrewriteInternal",
        ShortcutKind::Observer,
        "TiKVTxnRegionsNumHistogram",
        &["2pc_prewrite", "internal"]
    ),
    shortcut_spec!(
        "TxnRegionsNumHistogramPrewrite",
        ShortcutKind::Observer,
        "TiKVTxnRegionsNumHistogram",
        &["2pc_prewrite", "general"]
    ),
    shortcut_spec!(
        "TxnRegionsNumHistogramCommitInternal",
        ShortcutKind::Observer,
        "TiKVTxnRegionsNumHistogram",
        &["2pc_commit", "internal"]
    ),
    shortcut_spec!(
        "TxnRegionsNumHistogramCommit",
        ShortcutKind::Observer,
        "TiKVTxnRegionsNumHistogram",
        &["2pc_commit", "general"]
    ),
    shortcut_spec!(
        "TxnRegionsNumHistogramCleanupInternal",
        ShortcutKind::Observer,
        "TiKVTxnRegionsNumHistogram",
        &["2pc_cleanup", "internal"]
    ),
    shortcut_spec!(
        "TxnRegionsNumHistogramCleanup",
        ShortcutKind::Observer,
        "TiKVTxnRegionsNumHistogram",
        &["2pc_cleanup", "general"]
    ),
    shortcut_spec!(
        "TxnRegionsNumHistogramPessimisticLockInternal",
        ShortcutKind::Observer,
        "TiKVTxnRegionsNumHistogram",
        &["2pc_pessimistic_lock", "internal"]
    ),
    shortcut_spec!(
        "TxnRegionsNumHistogramPessimisticLock",
        ShortcutKind::Observer,
        "TiKVTxnRegionsNumHistogram",
        &["2pc_pessimistic_lock", "general"]
    ),
    shortcut_spec!(
        "TxnRegionsNumHistogramPessimisticRollbackInternal",
        ShortcutKind::Observer,
        "TiKVTxnRegionsNumHistogram",
        &["2pc_pessimistic_rollback", "internal"]
    ),
    shortcut_spec!(
        "TxnRegionsNumHistogramPessimisticRollback",
        ShortcutKind::Observer,
        "TiKVTxnRegionsNumHistogram",
        &["2pc_pessimistic_rollback", "general"]
    ),
    shortcut_spec!(
        "TxnRegionsNumHistogramWithCoprocessorInternal",
        ShortcutKind::Observer,
        "TiKVTxnRegionsNumHistogram",
        &["coprocessor", "internal"]
    ),
    shortcut_spec!(
        "TxnRegionsNumHistogramWithCoprocessor",
        ShortcutKind::Observer,
        "TiKVTxnRegionsNumHistogram",
        &["batch_coprocessor", "general"]
    ),
    shortcut_spec!(
        "TxnRegionsNumHistogramWithBatchCoprocessorInternal",
        ShortcutKind::Observer,
        "TiKVTxnRegionsNumHistogram",
        &["coprocessor", "internal"]
    ),
    shortcut_spec!(
        "TxnRegionsNumHistogramWithBatchCoprocessor",
        ShortcutKind::Observer,
        "TiKVTxnRegionsNumHistogram",
        &["batch_coprocessor", "general"]
    ),
    shortcut_spec!(
        "TxnWriteKVCountHistogramInternal",
        ShortcutKind::Observer,
        "TiKVTxnWriteKVCountHistogram",
        &["internal"]
    ),
    shortcut_spec!(
        "TxnWriteKVCountHistogramGeneral",
        ShortcutKind::Observer,
        "TiKVTxnWriteKVCountHistogram",
        &["general"]
    ),
    shortcut_spec!(
        "TxnWriteSizeHistogramInternal",
        ShortcutKind::Observer,
        "TiKVTxnWriteSizeHistogram",
        &["internal"]
    ),
    shortcut_spec!(
        "TxnWriteSizeHistogramGeneral",
        ShortcutKind::Observer,
        "TiKVTxnWriteSizeHistogram",
        &["general"]
    ),
    shortcut_spec!(
        "LockResolverCountWithBatchResolve",
        ShortcutKind::Counter,
        "TiKVLockResolverCounter",
        &["batch_resolve"]
    ),
    shortcut_spec!(
        "LockResolverCountWithExpired",
        ShortcutKind::Counter,
        "TiKVLockResolverCounter",
        &["expired"]
    ),
    shortcut_spec!(
        "LockResolverCountWithNotExpired",
        ShortcutKind::Counter,
        "TiKVLockResolverCounter",
        &["not_expired"]
    ),
    shortcut_spec!(
        "LockResolverCountWithWaitExpired",
        ShortcutKind::Counter,
        "TiKVLockResolverCounter",
        &["wait_expired"]
    ),
    shortcut_spec!(
        "LockResolverCountWithResolve",
        ShortcutKind::Counter,
        "TiKVLockResolverCounter",
        &["resolve"]
    ),
    shortcut_spec!(
        "LockResolverCountWithResolveForWrite",
        ShortcutKind::Counter,
        "TiKVLockResolverCounter",
        &["resolve_for_write"]
    ),
    shortcut_spec!(
        "LockResolverCountWithResolveAsync",
        ShortcutKind::Counter,
        "TiKVLockResolverCounter",
        &["resolve_async_commit"]
    ),
    shortcut_spec!(
        "LockResolverCountWithQueryTxnStatus",
        ShortcutKind::Counter,
        "TiKVLockResolverCounter",
        &["query_txn_status"]
    ),
    shortcut_spec!(
        "LockResolverCountWithQueryTxnStatusCommitted",
        ShortcutKind::Counter,
        "TiKVLockResolverCounter",
        &["query_txn_status_committed"]
    ),
    shortcut_spec!(
        "LockResolverCountWithQueryTxnStatusRolledBack",
        ShortcutKind::Counter,
        "TiKVLockResolverCounter",
        &["query_txn_status_rolled_back"]
    ),
    shortcut_spec!(
        "LockResolverCountWithQueryCheckSecondaryLocks",
        ShortcutKind::Counter,
        "TiKVLockResolverCounter",
        &["query_check_secondary_locks"]
    ),
    shortcut_spec!(
        "LockResolverCountWithResolveLocks",
        ShortcutKind::Counter,
        "TiKVLockResolverCounter",
        &["query_resolve_locks"]
    ),
    shortcut_spec!(
        "LockResolverCountWithResolveLockLite",
        ShortcutKind::Counter,
        "TiKVLockResolverCounter",
        &["query_resolve_lock_lite"]
    ),
    shortcut_spec!(
        "LockResolverCountWithAsyncResolveAsyncCommitFallback",
        ShortcutKind::Counter,
        "TiKVLockResolverCounter",
        &["async_resolve_async_commit_fallback"]
    ),
    shortcut_spec!(
        "LockResolverCountWithReadAsyncResolveFallback",
        ShortcutKind::Counter,
        "TiKVLockResolverCounter",
        &["read_async_resolve_fallback"]
    ),
    shortcut_spec!(
        "LockResolverCountWithAsyncCheckSecondariesFallback",
        ShortcutKind::Counter,
        "TiKVLockResolverCounter",
        &["async_check_secondaries_fallback"]
    ),
    shortcut_spec!(
        "LockResolverCountWithAsyncResolveAsyncCommitRegionFallback",
        ShortcutKind::Counter,
        "TiKVLockResolverCounter",
        &["async_resolve_async_commit_region_fallback"]
    ),
    shortcut_spec!(
        "LockResolverAsyncRunningTasksForReadResolve",
        ShortcutKind::Gauge,
        "TiKVLockResolverAsyncRunningTasks",
        &["read_resolve"]
    ),
    shortcut_spec!(
        "LockResolverAsyncRunningTasksForResolveAsyncCommit",
        ShortcutKind::Gauge,
        "TiKVLockResolverAsyncRunningTasks",
        &["resolve_async_commit"]
    ),
    shortcut_spec!(
        "LockResolverAsyncRunningTasksForCheckSecondaries",
        ShortcutKind::Gauge,
        "TiKVLockResolverAsyncRunningTasks",
        &["check_secondaries"]
    ),
    shortcut_spec!(
        "LockResolverAsyncRunningTasksForResolveAsyncCommitRegion",
        ShortcutKind::Gauge,
        "TiKVLockResolverAsyncRunningTasks",
        &["resolve_async_commit_region"]
    ),
    shortcut_spec!(
        "RegionCacheCounterWithInvalidateRegionFromCacheOK",
        ShortcutKind::Counter,
        "TiKVRegionCacheCounter",
        &["invalidate_region_from_cache", "ok"]
    ),
    shortcut_spec!(
        "RegionCacheCounterWithSendFail",
        ShortcutKind::Counter,
        "TiKVRegionCacheCounter",
        &["send_fail", "ok"]
    ),
    shortcut_spec!(
        "RegionCacheCounterWithGetRegionByIDOK",
        ShortcutKind::Counter,
        "TiKVRegionCacheCounter",
        &["get_region_by_id", "ok"]
    ),
    shortcut_spec!(
        "RegionCacheCounterWithGetRegionByIDError",
        ShortcutKind::Counter,
        "TiKVRegionCacheCounter",
        &["get_region_by_id", "err"]
    ),
    shortcut_spec!(
        "RegionCacheCounterWithGetCacheMissOK",
        ShortcutKind::Counter,
        "TiKVRegionCacheCounter",
        &["get_region_when_miss", "ok"]
    ),
    shortcut_spec!(
        "RegionCacheCounterWithGetCacheMissError",
        ShortcutKind::Counter,
        "TiKVRegionCacheCounter",
        &["get_region_when_miss", "err"]
    ),
    shortcut_spec!(
        "RegionCacheCounterWithScanRegionsOK",
        ShortcutKind::Counter,
        "TiKVRegionCacheCounter",
        &["scan_regions", "ok"]
    ),
    shortcut_spec!(
        "RegionCacheCounterWithScanRegionsError",
        ShortcutKind::Counter,
        "TiKVRegionCacheCounter",
        &["scan_regions", "err"]
    ),
    shortcut_spec!(
        "RegionCacheCounterWithBatchScanRegionsOK",
        ShortcutKind::Counter,
        "TiKVRegionCacheCounter",
        &["batch_scan_regions", "ok"]
    ),
    shortcut_spec!(
        "RegionCacheCounterWithBatchScanRegionsError",
        ShortcutKind::Counter,
        "TiKVRegionCacheCounter",
        &["batch_scan_regions", "err"]
    ),
    shortcut_spec!(
        "RegionCacheCounterWithGetStoreOK",
        ShortcutKind::Counter,
        "TiKVRegionCacheCounter",
        &["get_store", "ok"]
    ),
    shortcut_spec!(
        "RegionCacheCounterWithGetStoreError",
        ShortcutKind::Counter,
        "TiKVRegionCacheCounter",
        &["get_store", "err"]
    ),
    shortcut_spec!(
        "RegionCacheCounterWithInvalidateStoreRegionsOK",
        ShortcutKind::Counter,
        "TiKVRegionCacheCounter",
        &["invalidate_store_regions", "ok"]
    ),
    shortcut_spec!(
        "LoadRegionCacheHistogramWhenCacheMiss",
        ShortcutKind::Observer,
        "TiKVLoadRegionCacheHistogram",
        &["get_region_when_miss"]
    ),
    shortcut_spec!(
        "LoadRegionCacheHistogramWithRegions",
        ShortcutKind::Observer,
        "TiKVLoadRegionCacheHistogram",
        &["scan_regions"]
    ),
    shortcut_spec!(
        "LoadRegionCacheHistogramWithBatchScanRegions",
        ShortcutKind::Observer,
        "TiKVLoadRegionCacheHistogram",
        &["batch_scan_regions"]
    ),
    shortcut_spec!(
        "LoadRegionCacheHistogramWithRegionByID",
        ShortcutKind::Observer,
        "TiKVLoadRegionCacheHistogram",
        &["get_region_by_id"]
    ),
    shortcut_spec!(
        "LoadRegionCacheHistogramWithGetStore",
        ShortcutKind::Observer,
        "TiKVLoadRegionCacheHistogram",
        &["get_store"]
    ),
    shortcut_spec!(
        "TxnHeartBeatHistogramOK",
        ShortcutKind::Observer,
        "TiKVTxnHeartBeatHistogram",
        &["ok"]
    ),
    shortcut_spec!(
        "TxnHeartBeatHistogramError",
        ShortcutKind::Observer,
        "TiKVTxnHeartBeatHistogram",
        &["err"]
    ),
    shortcut_spec!(
        "StatusCountWithOK",
        ShortcutKind::Counter,
        "TiKVStatusCounter",
        &["ok"]
    ),
    shortcut_spec!(
        "StatusCountWithError",
        ShortcutKind::Counter,
        "TiKVStatusCounter",
        &["err"]
    ),
    shortcut_spec!(
        "SecondaryLockCleanupFailureCounterCommit",
        ShortcutKind::Counter,
        "TiKVSecondaryLockCleanupFailureCounter",
        &["commit"]
    ),
    shortcut_spec!(
        "SecondaryLockCleanupFailureCounterRollback",
        ShortcutKind::Counter,
        "TiKVSecondaryLockCleanupFailureCounter",
        &["rollback"]
    ),
    shortcut_spec!(
        "TwoPCTxnCounterOk",
        ShortcutKind::Counter,
        "TiKVTwoPCTxnCounter",
        &["ok"]
    ),
    shortcut_spec!(
        "TwoPCTxnCounterError",
        ShortcutKind::Counter,
        "TiKVTwoPCTxnCounter",
        &["err"]
    ),
    shortcut_spec!(
        "AsyncCommitTxnCounterOk",
        ShortcutKind::Counter,
        "TiKVAsyncCommitTxnCounter",
        &["ok"]
    ),
    shortcut_spec!(
        "AsyncCommitTxnCounterError",
        ShortcutKind::Counter,
        "TiKVAsyncCommitTxnCounter",
        &["err"]
    ),
    shortcut_spec!(
        "OnePCTxnCounterOk",
        ShortcutKind::Counter,
        "TiKVOnePCTxnCounter",
        &["ok"]
    ),
    shortcut_spec!(
        "OnePCTxnCounterError",
        ShortcutKind::Counter,
        "TiKVOnePCTxnCounter",
        &["err"]
    ),
    shortcut_spec!(
        "OnePCTxnCounterFallback",
        ShortcutKind::Counter,
        "TiKVOnePCTxnCounter",
        &["fallback"]
    ),
    uninitialized_shortcut_spec!("BatchRecvHistogramOK", ShortcutKind::Observer),
    uninitialized_shortcut_spec!("BatchRecvHistogramError", ShortcutKind::Observer),
    shortcut_spec!(
        "PrewriteAssertionUsageCounterNone",
        ShortcutKind::Counter,
        "TiKVPrewriteAssertionUsageCounter",
        &["none"]
    ),
    shortcut_spec!(
        "PrewriteAssertionUsageCounterExist",
        ShortcutKind::Counter,
        "TiKVPrewriteAssertionUsageCounter",
        &["exist"]
    ),
    shortcut_spec!(
        "PrewriteAssertionUsageCounterNotExist",
        ShortcutKind::Counter,
        "TiKVPrewriteAssertionUsageCounter",
        &["not-exist"]
    ),
    shortcut_spec!(
        "PrewriteAssertionUsageCounterUnknown",
        ShortcutKind::Counter,
        "TiKVPrewriteAssertionUsageCounter",
        &["unknown"]
    ),
    shortcut_spec!(
        "AggressiveLockedKeysNew",
        ShortcutKind::Counter,
        "TiKVAggressiveLockedKeysCounter",
        &["new"]
    ),
    shortcut_spec!(
        "AggressiveLockedKeysDerived",
        ShortcutKind::Counter,
        "TiKVAggressiveLockedKeysCounter",
        &["derived"]
    ),
    shortcut_spec!(
        "AggressiveLockedKeysLockedWithConflict",
        ShortcutKind::Counter,
        "TiKVAggressiveLockedKeysCounter",
        &["locked_with_conflict"]
    ),
    shortcut_spec!(
        "AggressiveLockedKeysNonForceLock",
        ShortcutKind::Counter,
        "TiKVAggressiveLockedKeysCounter",
        &["non_force_lock"]
    ),
    shortcut_spec!(
        "StaleReadHitCounter",
        ShortcutKind::Counter,
        "TiKVStaleReadCounter",
        &["hit"]
    ),
    shortcut_spec!(
        "StaleReadMissCounter",
        ShortcutKind::Counter,
        "TiKVStaleReadCounter",
        &["miss"]
    ),
    shortcut_spec!(
        "StaleReadReqLocalCounter",
        ShortcutKind::Counter,
        "TiKVStaleReadReqCounter",
        &["local"]
    ),
    shortcut_spec!(
        "StaleReadReqCrossZoneCounter",
        ShortcutKind::Counter,
        "TiKVStaleReadReqCounter",
        &["cross-zone"]
    ),
    shortcut_spec!(
        "StaleReadLocalInBytes",
        ShortcutKind::Counter,
        "TiKVStaleReadBytes",
        &["local", "in"]
    ),
    shortcut_spec!(
        "StaleReadLocalOutBytes",
        ShortcutKind::Counter,
        "TiKVStaleReadBytes",
        &["local", "out"]
    ),
    shortcut_spec!(
        "StaleReadRemoteInBytes",
        ShortcutKind::Counter,
        "TiKVStaleReadBytes",
        &["cross-zone", "in"]
    ),
    shortcut_spec!(
        "StaleReadRemoteOutBytes",
        ShortcutKind::Counter,
        "TiKVStaleReadBytes",
        &["cross-zone", "out"]
    ),
    shortcut_spec!(
        "AsyncSendReqCounterWithOK",
        ShortcutKind::Counter,
        "TiKVAsyncSendReqCounter",
        &["ok"]
    ),
    shortcut_spec!(
        "AsyncSendReqCounterWithRegionError",
        ShortcutKind::Counter,
        "TiKVAsyncSendReqCounter",
        &["region_error"]
    ),
    shortcut_spec!(
        "AsyncSendReqCounterWithRPCError",
        ShortcutKind::Counter,
        "TiKVAsyncSendReqCounter",
        &["rpc_error"]
    ),
    shortcut_spec!(
        "AsyncSendReqCounterWithSendError",
        ShortcutKind::Counter,
        "TiKVAsyncSendReqCounter",
        &["send_error"]
    ),
    shortcut_spec!(
        "AsyncSendReqCounterWithOtherError",
        ShortcutKind::Counter,
        "TiKVAsyncSendReqCounter",
        &["other_error"]
    ),
    shortcut_spec!(
        "AsyncBatchGetCounterWithOK",
        ShortcutKind::Counter,
        "TiKVAsyncBatchGetCounter",
        &["ok"]
    ),
    shortcut_spec!(
        "AsyncBatchGetCounterWithRegionError",
        ShortcutKind::Counter,
        "TiKVAsyncBatchGetCounter",
        &["region_error"]
    ),
    shortcut_spec!(
        "AsyncBatchGetCounterWithLockError",
        ShortcutKind::Counter,
        "TiKVAsyncBatchGetCounter",
        &["lock_error"]
    ),
    shortcut_spec!(
        "AsyncBatchGetCounterWithOtherError",
        ShortcutKind::Counter,
        "TiKVAsyncBatchGetCounter",
        &["other_error"]
    ),
    shortcut_spec!(
        "ReadRequestLeaderLocalBytes",
        ShortcutKind::Observer,
        "TiKVReadRequestBytes",
        &["leader", "local"]
    ),
    shortcut_spec!(
        "ReadRequestLeaderRemoteBytes",
        ShortcutKind::Observer,
        "TiKVReadRequestBytes",
        &["leader", "cross-zone"]
    ),
    shortcut_spec!(
        "ReadRequestFollowerLocalBytes",
        ShortcutKind::Observer,
        "TiKVReadRequestBytes",
        &["follower", "local"]
    ),
    shortcut_spec!(
        "ReadRequestFollowerRemoteBytes",
        ShortcutKind::Observer,
        "TiKVReadRequestBytes",
        &["follower", "cross-zone"]
    ),
    shortcut_spec!(
        "LagCommitTSWaitHistogramWithOK",
        ShortcutKind::Observer,
        "TiKVTxnLagCommitTSWaitHistogram",
        &["ok"]
    ),
    shortcut_spec!(
        "LagCommitTSWaitHistogramWithError",
        ShortcutKind::Observer,
        "TiKVTxnLagCommitTSWaitHistogram",
        &["err"]
    ),
    shortcut_spec!(
        "LagCommitTSAttemptHistogramWithOK",
        ShortcutKind::Observer,
        "TiKVTxnLagCommitTSAttemptHistogram",
        &["ok"]
    ),
    shortcut_spec!(
        "LagCommitTSAttemptHistogramWithError",
        ShortcutKind::Observer,
        "TiKVTxnLagCommitTSAttemptHistogram",
        &["err"]
    ),
    shortcut_spec!(
        "TxnFileRequestsOk",
        ShortcutKind::Counter,
        "TiKVTxnFileRequestCounter",
        &["ok"]
    ),
    shortcut_spec!(
        "TxnFileRequestsError",
        ShortcutKind::Counter,
        "TiKVTxnFileRequestCounter",
        &["err"]
    ),
    shortcut_spec!(
        "TxnFileErrorAccounting",
        ShortcutKind::Counter,
        "TiKVTxnFileErrorCounter",
        &["accounting"]
    ),
    shortcut_spec!(
        "TxnFileWriteBytesInternal",
        ShortcutKind::Counter,
        "TiKVTxnFileWriteBytes",
        &["internal"]
    ),
    shortcut_spec!(
        "TxnFileWriteBytesGeneral",
        ShortcutKind::Counter,
        "TiKVTxnFileWriteBytes",
        &["general"]
    ),
    shortcut_spec!(
        "TxnFileMutationSizeInternal",
        ShortcutKind::Observer,
        "TiKVTxnFileMutationSizeHistogram",
        &["internal"]
    ),
    shortcut_spec!(
        "TxnFileMutationSizeGeneral",
        ShortcutKind::Observer,
        "TiKVTxnFileMutationSizeHistogram",
        &["general"]
    ),
    shortcut_spec!(
        "TxnFileDurationInternal",
        ShortcutKind::Observer,
        "TiKVTxnFileDuration",
        &["internal"]
    ),
    shortcut_spec!(
        "TxnFileDurationGeneral",
        ShortcutKind::Observer,
        "TiKVTxnFileDuration",
        &["general"]
    ),
];
