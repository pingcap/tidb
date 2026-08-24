// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex as StdMutex;
use std::sync::RwLock as StdRwLock;
use std::time::Duration;

use fail::fail_point;
use log::debug;
use log::error;
use log::warn;
use tokio::sync::{Mutex, RwLock, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::sleep;

use crate::backoff::Backoff;
use crate::backoff::DEFAULT_REGION_BACKOFF;
use crate::backoff::OPTIMISTIC_BACKOFF;
use crate::config::get_global_config;
use crate::config::NEXT_GEN;
use crate::interceptor::RpcInterceptorChain;
use crate::kv::HexRepr;
use crate::pd::PdClient;
use crate::resource_control::ResourceGroupControllerHandle;
use crate::stats;

use crate::proto::kvrpcpb;
use crate::proto::kvrpcpb::TxnInfo;
use crate::proto::pdpb::Timestamp;
use crate::region::RegionVerId;
use crate::request::plan::handle_region_error;
use crate::request::plan::{invalidate_connection_for_error, is_grpc_error};
use crate::request::CollectSingle;
use crate::request::CollectWithShard;
use crate::request::Keyspace;
use crate::request::Plan;
use crate::store::RegionStore;
use crate::timestamp::TimestampExt;
use crate::transaction::requests;
use crate::transaction::requests::new_check_secondary_locks_request;
use crate::transaction::requests::new_check_txn_status_request;
use crate::transaction::requests::SecondaryLocksStatus;
use crate::transaction::requests::TransactionStatus;
use crate::transaction::requests::TransactionStatusKind;
use crate::Error;
use crate::Result;

pub(crate) fn format_key_for_log(key: &[u8]) -> String {
    let prefix_len = key.len().min(16);
    format!("len={}, prefix={}", key.len(), HexRepr(&key[..prefix_len]))
}

/// client-go's `txnlock.ResolvedCacheSize`.
const RESOLVED_CACHE_SIZE: usize = 2048;
/// client-go `internal/client.MaxWriteExecutionTime`.
const LOCK_RESOLVER_MAX_WRITE_EXECUTION_DURATION: Duration = Duration::from_secs(20);
/// client-go's process-wide `AsyncResolveLockSemaphoreLimit`.
const ASYNC_READ_RESOLVE_LOCK_LIMIT: usize = 10_000;
static ASYNC_READ_RESOLVE_LOCKS: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(ASYNC_READ_RESOLVE_LOCK_LIMIT)));

#[derive(Default)]
struct ResolvedStatusCache {
    statuses: HashMap<u64, Arc<TransactionStatus>>,
    insertion_order: VecDeque<u64>,
}

#[derive(Default)]
struct ResolvingLocks {
    locks: HashMap<u64, Vec<Option<Vec<kvrpcpb::LockInfo>>>>,
    concurrency: HashMap<u64, usize>,
}

/// A lock operation currently being resolved on behalf of a transaction.
///
/// This is the native representation of client-go's `txnlock.ResolvingLock`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvingLock {
    pub txn_id: u64,
    pub lock_txn_id: u64,
    pub key: Vec<u8>,
    pub primary: Vec<u8>,
}

/// Extract the lock carried by a TiKV key error.
///
/// This mirrors client-go's `ExtractLockFromKeyErr`: a shared-lock wrapper is
/// returned as-is so callers that require exactly one lock retain the source
/// behavior. Use [`extract_locks_from_key_error`] when the caller can resolve
/// every shared holder.
pub fn extract_lock_from_key_error(key_error: &kvrpcpb::KeyError) -> Result<kvrpcpb::LockInfo> {
    key_error
        .locked
        .clone()
        .ok_or_else(|| Error::KeyError(Box::new(key_error.clone())))
}

/// Extract every lock represented by a TiKV key error.
///
/// A shared-lock wrapper has unset transaction fields; client-go expands its
/// `shared_lock_infos` instead of passing that wrapper to the resolver. An
/// exclusive lock remains a single-element result.
pub fn extract_locks_from_key_error(
    key_error: &kvrpcpb::KeyError,
) -> Result<Vec<kvrpcpb::LockInfo>> {
    let lock = extract_lock_from_key_error(key_error)?;
    if lock.shared_lock_infos.is_empty() {
        Ok(vec![lock])
    } else {
        Ok(lock.shared_lock_infos)
    }
}

/// Refuse to resolve a shared-lock wrapper before it can be mis-handled.
///
/// The contract (`kvrpcpb.LockInfo.shared_lock_infos`) is explicit: a shared lock's
/// real holders live ONLY in `shared_lock_infos` — "DO NOT read from the wrapper
/// LockInfo", whose own `key`/`lock_version` are unset. Resolving such a wrapper
/// checks transaction 0 and filtering it silently drops the members.
///
/// `SharedPessimisticLock` is different: it is an embedded, concrete holder and
/// client-go's `Lock.IsPessimistic` resolves it through PessimisticRollback.
pub(crate) fn reject_shared_locks(locks: &[kvrpcpb::LockInfo]) -> Result<()> {
    let shared = |l: &kvrpcpb::LockInfo| {
        !l.shared_lock_infos.is_empty() || l.lock_type == kvrpcpb::Op::SharedLock as i32
    };
    if locks.iter().any(shared) {
        return Err(Error::StringError(
            "shared lock wrappers are not resolver inputs; expand shared_lock_infos before \
             resolving them — resolving the wrapper would target the wrong transaction"
                .to_owned(),
        ));
    }
    Ok(())
}

fn is_pessimistic_lock(lock: &kvrpcpb::LockInfo) -> bool {
    matches!(
        lock.lock_type,
        value if value == kvrpcpb::Op::PessimisticLock as i32
            || value == kvrpcpb::Op::SharedPessimisticLock as i32
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn resolve_locks_with_ru_details(
    locks: Vec<kvrpcpb::LockInfo>,
    timestamp: Timestamp,
    pd_client: Arc<impl PdClient>,
    keyspace: Keyspace,
    keyspace_name: Option<&str>,
    rpc_interceptor: Option<RpcInterceptorChain>,
    resource_group_name: Option<&str>,
    resource_control: Option<ResourceGroupControllerHandle>,
    ru_details: Option<Arc<crate::RuDetails>>,
) -> Result<Vec<kvrpcpb::LockInfo> /* live_locks */> {
    resolve_locks_with_context(
        locks,
        timestamp,
        pd_client,
        keyspace,
        keyspace_name,
        ResolveLocksContext {
            rpc_interceptor,
            resource_group_name: resource_group_name.map(ToOwned::to_owned),
            resource_control,
            ru_details,
            ..Default::default()
        },
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn resolve_locks_with_context(
    locks: Vec<kvrpcpb::LockInfo>,
    timestamp: Timestamp,
    pd_client: Arc<impl PdClient>,
    keyspace: Keyspace,
    keyspace_name: Option<&str>,
    context: ResolveLocksContext,
) -> Result<Vec<kvrpcpb::LockInfo> /* live_locks */> {
    resolve_locks_with_context_result(
        locks,
        timestamp,
        pd_client,
        keyspace,
        keyspace_name,
        context,
    )
    .await
    .map(|result| result.live_locks)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn resolve_locks_with_context_result(
    locks: Vec<kvrpcpb::LockInfo>,
    timestamp: Timestamp,
    pd_client: Arc<impl PdClient>,
    keyspace: Keyspace,
    keyspace_name: Option<&str>,
    context: ResolveLocksContext,
) -> Result<ResolveLocksResult> {
    resolve_locks_with_context_inner(
        locks,
        timestamp,
        pd_client,
        keyspace,
        keyspace_name,
        context,
        None,
    )
    .await
}

/// Source `LockResolver.ResolveLocksForRead`: record final status in the
/// snapshot context so TiKV can ignore or read through the lock on retry.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn resolve_locks_for_read_with_context(
    locks: Vec<kvrpcpb::LockInfo>,
    timestamp: Timestamp,
    pd_client: Arc<impl PdClient>,
    keyspace: Keyspace,
    keyspace_name: Option<&str>,
    context: ResolveLocksContext,
    read_lock_context: &ReadLockContext,
) -> Result<Vec<kvrpcpb::LockInfo> /* live_locks */> {
    resolve_locks_for_read_with_context_result(
        locks,
        timestamp,
        pd_client,
        keyspace,
        keyspace_name,
        context,
        read_lock_context,
    )
    .await
    .map(|result| result.live_locks)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn resolve_locks_for_read_with_context_result(
    locks: Vec<kvrpcpb::LockInfo>,
    timestamp: Timestamp,
    pd_client: Arc<impl PdClient>,
    keyspace: Keyspace,
    keyspace_name: Option<&str>,
    context: ResolveLocksContext,
    read_lock_context: &ReadLockContext,
) -> Result<ResolveLocksResult> {
    resolve_locks_with_context_inner(
        locks,
        timestamp,
        pd_client,
        keyspace,
        keyspace_name,
        context,
        Some(read_lock_context),
    )
    .await
}

/// The native form of client-go's `ResolveLockResult` used by retry owners.
/// Public Rust compatibility APIs still expose only the live locks.
#[derive(Debug, Default)]
pub(crate) struct ResolveLocksResult {
    pub(crate) live_locks: Vec<kvrpcpb::LockInfo>,
    pub(crate) ms_before_expired: i64,
}

#[allow(clippy::too_many_arguments)]
async fn resolve_locks_with_context_inner(
    locks: Vec<kvrpcpb::LockInfo>,
    timestamp: Timestamp,
    pd_client: Arc<impl PdClient>,
    keyspace: Keyspace,
    keyspace_name: Option<&str>,
    context: ResolveLocksContext,
    read_lock_context: Option<&ReadLockContext>,
) -> Result<ResolveLocksResult> {
    debug!("resolving locks");
    stats::increment_lock_resolver_action("resolve");
    reject_shared_locks(&locks)?;
    // client-go ResolveLocksWithOpts returns before consulting the oracle when
    // no locks were supplied. This also keeps an empty retry path independent
    // of PD availability.
    if locks.is_empty() {
        return Ok(ResolveLocksResult::default());
    }
    let ts = pd_client.clone().get_timestamp().await?;
    let caller_start_ts = timestamp.version();
    let current_ts = ts.version();

    let mut live_locks = Vec::new();
    let mut ms_before_expired = None;
    let mut lock_resolver = LockResolver::new(context);
    let mut read_lite_cleanups = HashMap::new();

    // records the commit version of each primary lock (representing the status of the transaction)
    let mut commit_versions: HashMap<u64, u64> = HashMap::new();
    let mut clean_regions: HashMap<u64, HashSet<RegionVerId>> = HashMap::new();
    // We must check txn status for *all* locks, not only TTL-expired ones.
    //
    // TTL only indicates whether a lock is *possibly* orphaned; it does not mean the transaction
    // is still running. A transaction may already be committed/rolled back while its locks are
    // still visible (e.g. cleanup/resolve hasn't finished, retries after region errors, etc.).
    // If we only resolve TTL-expired locks, we can unnecessarily sleep/backoff until TTL even
    // though `CheckTxnStatus` would already report `Committed`/`RolledBack`.
    //
    // This matches the client-go `LockResolver.ResolveLocksWithOpts` flow: query txn status for
    // each encountered lock, then resolve immediately when the status is final.
    for lock in locks {
        let region_ver_id = pd_client
            .region_for_key(&lock.key.clone().into())
            .await?
            .ver_id();
        // skip if the region is cleaned
        if clean_regions
            .get(&lock.lock_version)
            .map(|regions| regions.contains(&region_ver_id))
            .unwrap_or(false)
        {
            continue;
        }

        let commit_version = match commit_versions.get(&lock.lock_version) {
            Some(&commit_version) => Some(commit_version),
            None => {
                // TODO: handle primary mismatch error.
                let mut status = lock_resolver
                    .get_txn_status_from_lock(
                        OPTIMISTIC_BACKOFF,
                        &lock,
                        caller_start_ts,
                        current_ts,
                        false,
                        pd_client.clone(),
                        keyspace,
                        keyspace_name,
                    )
                    .await?;
                let async_primary = match &status.kind {
                    TransactionStatusKind::Locked(_, lock_info)
                        if lock_info.use_async_commit && status.is_expired =>
                    {
                        Some((lock_info.secondaries.clone(), lock_info.min_commit_ts))
                    }
                    _ => None,
                };

                if let Some((secondary_keys, primary_min_commit_ts)) = async_primary {
                    stats::increment_lock_resolver_action("expired");
                    stats::increment_lock_resolver_action("resolve_async_commit");
                    let mut secondary_status = lock_resolver
                        .check_all_secondaries(
                            pd_client.clone(),
                            keyspace,
                            keyspace_name,
                            secondary_keys,
                            lock.lock_version,
                        )
                        .await?;

                    if let Some(commit_version) = secondary_status
                        .determine_commit_ts(lock.lock_version, primary_min_commit_ts)?
                    {
                        let mut determined_status = (*status).clone();
                        determined_status.kind = if commit_version == 0 {
                            TransactionStatusKind::RolledBack
                        } else {
                            TransactionStatusKind::Committed(Timestamp::from_version(
                                commit_version,
                            ))
                        };
                        determined_status.is_expired = false;
                        let determined_status = Arc::new(determined_status);
                        lock_resolver
                            .ctx
                            .save_resolved(lock.lock_version, determined_status)
                            .await;

                        if let Some(read_lock_context) = read_lock_context {
                            let keys = secondary_status.keys_to_resolve(&lock.primary_lock);
                            schedule_read_async_commit_cleanup(
                                &lock,
                                commit_version,
                                keys,
                                pd_client.clone(),
                                keyspace,
                                keyspace_name,
                                &lock_resolver.ctx,
                            )
                            .await;
                            record_read_lock_status(
                                read_lock_context,
                                lock.lock_version,
                                commit_version,
                                caller_start_ts,
                                status.action,
                            );
                            continue;
                        }

                        commit_versions.insert(lock.lock_version, commit_version);
                        for key in secondary_status.keys_to_resolve(&lock.primary_lock) {
                            let region_ver_id = pd_client
                                .region_for_key(&key.clone().into())
                                .await?
                                .ver_id();
                            if clean_regions
                                .get(&lock.lock_version)
                                .map(|regions| regions.contains(&region_ver_id))
                                .unwrap_or(false)
                            {
                                continue;
                            }
                            let cleaned_region = resolve_lock_with_retry(
                                &key,
                                lock.lock_version,
                                commit_version,
                                lock.is_txn_file,
                                // Async-commit recovery supplies the complete
                                // region key set itself; it must not use the
                                // ordinary single-key lite path.
                                u64::MAX,
                                pd_client.clone(),
                                keyspace,
                                keyspace_name,
                                lock_resolver.ctx.rpc_interceptor.clone(),
                                lock_resolver.ctx.resource_group_name.as_deref(),
                                lock_resolver.ctx.resource_control.clone(),
                                lock_resolver.ctx.ru_details.clone(),
                                OPTIMISTIC_BACKOFF,
                            )
                            .await?;
                            clean_regions
                                .entry(lock.lock_version)
                                .or_default()
                                .insert(cleaned_region);
                        }
                        continue;
                    }

                    // A complete secondary response containing a non-async
                    // lock is client-go's nonAsyncCommitLock fallback.
                    status = lock_resolver
                        .get_txn_status_from_lock(
                            OPTIMISTIC_BACKOFF,
                            &lock,
                            caller_start_ts,
                            current_ts,
                            true,
                            pd_client.clone(),
                            keyspace,
                            keyspace_name,
                        )
                        .await?;
                }

                if is_pessimistic_lock(&lock) {
                    lock_resolver
                        .resolve_pessimistic_lock(pd_client.clone(), keyspace, keyspace_name, &lock)
                        .await?;
                    if let TransactionStatusKind::Locked(_, lock_info) = &status.kind {
                        live_locks.push(lock_info.clone());
                        update_ms_before_expired(
                            &mut ms_before_expired,
                            lock_until_expired_ms(
                                lock.lock_version,
                                lock.lock_ttl,
                                Timestamp::from_version(current_ts),
                            ),
                        );
                    }
                    continue;
                }
                match &status.kind {
                    TransactionStatusKind::Committed(ts) => {
                        stats::increment_lock_resolver_action("expired");
                        let commit_version = ts.version();
                        if let Some(read_lock_context) = read_lock_context {
                            schedule_or_collect_read_lite_cleanup(
                                &mut read_lite_cleanups,
                                &lock,
                                commit_version,
                                pd_client.clone(),
                                keyspace,
                                keyspace_name,
                                &lock_resolver.ctx,
                            )
                            .await;
                            record_read_lock_status(
                                read_lock_context,
                                lock.lock_version,
                                commit_version,
                                caller_start_ts,
                                status.action,
                            );
                            continue;
                        }
                        commit_versions.insert(lock.lock_version, commit_version);
                        Some(commit_version)
                    }
                    TransactionStatusKind::RolledBack => {
                        stats::increment_lock_resolver_action("expired");
                        if let Some(read_lock_context) = read_lock_context {
                            schedule_or_collect_read_lite_cleanup(
                                &mut read_lite_cleanups,
                                &lock,
                                0,
                                pd_client.clone(),
                                keyspace,
                                keyspace_name,
                                &lock_resolver.ctx,
                            )
                            .await;
                            read_lock_context.add_resolved(lock.lock_version);
                            continue;
                        }
                        commit_versions.insert(lock.lock_version, 0);
                        Some(0)
                    }
                    TransactionStatusKind::Locked(ttl, lock_info) => {
                        if let Some(read_lock_context) = read_lock_context {
                            if status.action == kvrpcpb::Action::MinCommitTsPushed {
                                read_lock_context.add_resolved(lock.lock_version);
                                continue;
                            }
                        }
                        if status.is_expired {
                            stats::increment_lock_resolver_action("expired");
                        } else {
                            stats::increment_lock_resolver_action("not_expired");
                        }
                        update_ms_before_expired(
                            &mut ms_before_expired,
                            lock_until_expired_ms(
                                lock.lock_version,
                                *ttl,
                                Timestamp::from_version(current_ts),
                            ),
                        );
                        live_locks.push(lock_info.clone());
                        None
                    }
                }
            }
        };

        if let Some(commit_version) = commit_version {
            if let Some(read_lock_context) = read_lock_context {
                schedule_or_collect_read_lite_cleanup(
                    &mut read_lite_cleanups,
                    &lock,
                    commit_version,
                    pd_client.clone(),
                    keyspace,
                    keyspace_name,
                    &lock_resolver.ctx,
                )
                .await;
                record_read_lock_status(
                    read_lock_context,
                    lock.lock_version,
                    commit_version,
                    caller_start_ts,
                    kvrpcpb::Action::NoAction,
                );
                continue;
            }
            let resolve_lite =
                lock.txn_size < get_global_config().tikv_client.resolve_lock_lite_threshold;
            if resolve_lite && lock.key == lock.primary_lock {
                continue;
            }
            let cleaned_region = resolve_lock_with_retry(
                &lock.key,
                lock.lock_version,
                commit_version,
                lock.is_txn_file,
                lock.txn_size,
                pd_client.clone(),
                keyspace,
                keyspace_name,
                lock_resolver.ctx.rpc_interceptor.clone(),
                lock_resolver.ctx.resource_group_name.as_deref(),
                lock_resolver.ctx.resource_control.clone(),
                lock_resolver.ctx.ru_details.clone(),
                OPTIMISTIC_BACKOFF,
            )
            .await?;
            if !resolve_lite {
                clean_regions
                    .entry(lock.lock_version)
                    .or_default()
                    .insert(cleaned_region);
            }
        }
    }
    for cleanup in read_lite_cleanups.into_values() {
        schedule_read_lite_cleanup(
            cleanup,
            pd_client.clone(),
            keyspace,
            keyspace_name,
            &lock_resolver.ctx,
        )
        .await;
    }
    Ok(ResolveLocksResult {
        live_locks,
        ms_before_expired: ms_before_expired.unwrap_or(0),
    })
}

fn update_ms_before_expired(current: &mut Option<i64>, remaining_ms: i64) {
    let remaining_ms = remaining_ms.max(0);
    *current = Some(current.map_or(remaining_ms, |existing| existing.min(remaining_ms)));
}

struct ReadLiteCleanup {
    lock: kvrpcpb::LockInfo,
    commit_version: u64,
    keys: Vec<Vec<u8>>,
}

#[allow(clippy::too_many_arguments)]
async fn schedule_or_collect_read_lite_cleanup(
    cleanups: &mut HashMap<u64, ReadLiteCleanup>,
    lock: &kvrpcpb::LockInfo,
    commit_version: u64,
    pd_client: Arc<impl PdClient>,
    keyspace: Keyspace,
    keyspace_name: Option<&str>,
    context: &ResolveLocksContext,
) {
    if lock.txn_size < get_global_config().tikv_client.resolve_lock_lite_threshold {
        let cleanup = cleanups
            .entry(lock.lock_version)
            .or_insert_with(|| ReadLiteCleanup {
                lock: lock.clone(),
                commit_version,
                keys: Vec::new(),
            });
        debug_assert_eq!(cleanup.commit_version, commit_version);
        cleanup.keys.push(lock.key.clone());
    } else {
        schedule_read_lock_cleanup(
            lock,
            commit_version,
            pd_client,
            keyspace,
            keyspace_name,
            context,
        )
        .await;
    }
}

fn record_read_lock_status(
    read_lock_context: &ReadLockContext,
    txn_id: u64,
    commit_version: u64,
    caller_start_ts: u64,
    action: kvrpcpb::Action,
) {
    if action == kvrpcpb::Action::MinCommitTsPushed
        || commit_version == 0
        || commit_version > caller_start_ts
    {
        read_lock_context.add_resolved(txn_id);
    } else {
        read_lock_context.add_committed(txn_id);
    }
}

/// Client-go schedules read cleanup outside the caller's context. A full
/// semaphore runs it detached; saturation falls back to inline cleanup but
/// deliberately ignores cleanup failure so the already-classified read can
/// still retry with its TiKV lock hints.
#[allow(clippy::too_many_arguments)]
async fn schedule_read_lock_cleanup(
    lock: &kvrpcpb::LockInfo,
    commit_version: u64,
    pd_client: Arc<impl PdClient>,
    keyspace: Keyspace,
    keyspace_name: Option<&str>,
    context: &ResolveLocksContext,
) {
    let key = lock.key.clone();
    let start_version = lock.lock_version;
    let is_txn_file = lock.is_txn_file;
    let txn_size = lock.txn_size;
    let keyspace_name = keyspace_name.map(ToOwned::to_owned);
    let rpc_interceptor = context.rpc_interceptor.clone();
    let resource_group_name = context.resource_group_name.clone();
    let resource_control = context.resource_control.clone();
    let ru_details = context.ru_details.clone();
    let resolve = Box::pin(async move {
        let _ = resolve_lock_with_retry_for_read_cleanup(
            &key,
            start_version,
            commit_version,
            is_txn_file,
            txn_size,
            pd_client,
            keyspace,
            keyspace_name.as_deref(),
            rpc_interceptor,
            resource_group_name.as_deref(),
            resource_control,
            ru_details,
            // The native attempt-based backoff has the source 40,000-ms
            // async-cleanup budget when its 2-ms exponential prefix and
            // 500-ms cap are allowed to make 87 attempts.
            Backoff::no_jitter_backoff(2, 500, 87),
        )
        .await;
    });

    let _ =
        schedule_read_cleanup_task(resolve, "read_resolve", "read_async_resolve_fallback").await;
}

/// Resolve the collected lite keys once per current region. The source defers
/// this work until every lock status is known so a read neither waits for
/// cleanup nor sends one key-scoped ResolveLock RPC per encountered lock.
#[allow(clippy::too_many_arguments)]
async fn schedule_read_lite_cleanup(
    cleanup: ReadLiteCleanup,
    pd_client: Arc<impl PdClient>,
    keyspace: Keyspace,
    keyspace_name: Option<&str>,
    context: &ResolveLocksContext,
) {
    let primary_lock = cleanup.lock.primary_lock.clone();
    let start_version = cleanup.lock.lock_version;
    let commit_version = cleanup.commit_version;
    let is_txn_file = cleanup.lock.is_txn_file;
    let keys = cleanup.keys;
    let keyspace_name = keyspace_name.map(ToOwned::to_owned);
    let rpc_interceptor = context.rpc_interceptor.clone();
    let resource_group_name = context.resource_group_name.clone();
    let resource_control = context.resource_control.clone();
    let ru_details = context.ru_details.clone();
    let resolve = Box::pin(async move {
        schedule_grouped_read_cleanup(
            keys,
            primary_lock,
            true,
            start_version,
            commit_version,
            is_txn_file,
            pd_client,
            keyspace,
            keyspace_name,
            rpc_interceptor,
            resource_group_name,
            resource_control,
            ru_details,
            "read_resolve",
            "read_async_resolve_fallback",
        )
        .await;
    });

    let _ =
        schedule_read_cleanup_task(resolve, "read_resolve", "read_async_resolve_fallback").await;
}

/// Expired async-commit recovery determines the full transaction outcome, so
/// client-go resolves its recovered secondary set plus primary rather than
/// only the lock which caused the read retry.
#[allow(clippy::too_many_arguments)]
async fn schedule_read_async_commit_cleanup(
    lock: &kvrpcpb::LockInfo,
    commit_version: u64,
    keys: Vec<Vec<u8>>,
    pd_client: Arc<impl PdClient>,
    keyspace: Keyspace,
    keyspace_name: Option<&str>,
    context: &ResolveLocksContext,
) {
    let primary_lock = lock.primary_lock.clone();
    let start_version = lock.lock_version;
    let is_txn_file = lock.is_txn_file;
    let keyspace_name = keyspace_name.map(ToOwned::to_owned);
    let rpc_interceptor = context.rpc_interceptor.clone();
    let resource_group_name = context.resource_group_name.clone();
    let resource_control = context.resource_control.clone();
    let ru_details = context.ru_details.clone();
    let resolve = Box::pin(async move {
        schedule_grouped_read_cleanup(
            keys,
            primary_lock,
            false,
            start_version,
            commit_version,
            is_txn_file,
            pd_client,
            keyspace,
            keyspace_name,
            rpc_interceptor,
            resource_group_name,
            resource_control,
            ru_details,
            "resolve_async_commit_region",
            "async_resolve_async_commit_region_fallback",
        )
        .await;
    });

    let _ = schedule_read_cleanup_task(
        resolve,
        "resolve_async_commit",
        "async_resolve_async_commit_fallback",
    )
    .await;
}

async fn schedule_read_cleanup_task<F>(
    resolve: F,
    task_kind: &'static str,
    fallback_action: &'static str,
) -> Option<JoinHandle<()>>
where
    F: Future<Output = ()> + Send + 'static,
{
    schedule_read_cleanup_task_with_semaphore(
        ASYNC_READ_RESOLVE_LOCKS.clone(),
        resolve,
        task_kind,
        fallback_action,
    )
    .await
}

async fn schedule_read_cleanup_task_with_semaphore<F>(
    semaphore: Arc<Semaphore>,
    resolve: F,
    task_kind: &'static str,
    fallback_action: &'static str,
) -> Option<JoinHandle<()>>
where
    F: Future<Output = ()> + Send + 'static,
{
    match semaphore.try_acquire_owned() {
        Ok(permit) => {
            stats::add_lock_resolver_async_running_tasks(task_kind, 1);
            Some(tokio::spawn(async move {
                let _permit = permit;
                resolve.await;
                stats::add_lock_resolver_async_running_tasks(task_kind, -1);
            }))
        }
        Err(_) => {
            stats::increment_lock_resolver_action(fallback_action);
            resolve.await;
            None
        }
    }
}

/// client-go schedules one task to group a multi-key cleanup, then fans its
/// region RPCs back out through the same bounded pool. Each child retains the
/// native per-key retry and regrouping behavior when a region changes.
#[allow(clippy::too_many_arguments)]
async fn schedule_grouped_read_cleanup(
    keys: Vec<Vec<u8>>,
    primary_lock: Vec<u8>,
    skip_checked_primary: bool,
    start_version: u64,
    commit_version: u64,
    is_txn_file: bool,
    pd_client: Arc<impl PdClient>,
    keyspace: Keyspace,
    keyspace_name: Option<String>,
    rpc_interceptor: Option<RpcInterceptorChain>,
    resource_group_name: Option<String>,
    resource_control: Option<ResourceGroupControllerHandle>,
    ru_details: Option<Arc<crate::RuDetails>>,
    region_task_kind: &'static str,
    region_fallback_action: &'static str,
) {
    if keys.len() == 1 {
        let _ = resolve_lite_locks_with_retry_for_read_cleanup(
            &keys,
            &primary_lock,
            skip_checked_primary,
            start_version,
            commit_version,
            is_txn_file,
            pd_client,
            keyspace,
            keyspace_name.as_deref(),
            rpc_interceptor,
            resource_group_name.as_deref(),
            resource_control,
            ru_details,
            Backoff::no_jitter_backoff(2, 500, 87),
        )
        .await;
        return;
    }

    let mut regions: HashMap<RegionVerId, Vec<Vec<u8>>> = HashMap::new();
    for key in &keys {
        let Ok(store) = pd_client.clone().store_for_key(&key.clone().into()).await else {
            return;
        };
        regions
            .entry(store.region_with_leader.ver_id())
            .or_default()
            .push(key.clone());
    }

    let mut region_tasks = Vec::with_capacity(regions.len());
    for region_keys in regions.into_values() {
        let primary_lock = primary_lock.clone();
        let pd_client = pd_client.clone();
        let keyspace_name = keyspace_name.clone();
        let rpc_interceptor = rpc_interceptor.clone();
        let resource_group_name = resource_group_name.clone();
        let resource_control = resource_control.clone();
        let ru_details = ru_details.clone();
        let resolve = async move {
            let _ = resolve_lite_locks_with_retry_for_read_cleanup(
                &region_keys,
                &primary_lock,
                false,
                start_version,
                commit_version,
                is_txn_file,
                pd_client,
                keyspace,
                keyspace_name.as_deref(),
                rpc_interceptor,
                resource_group_name.as_deref(),
                resource_control,
                ru_details,
                Backoff::no_jitter_backoff(2, 500, 87),
            )
            .await;
        };
        if let Some(task) =
            schedule_read_cleanup_task(resolve, region_task_kind, region_fallback_action).await
        {
            region_tasks.push(task);
        }
    }

    for task in region_tasks {
        let _ = task.await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn resolve_lite_locks_with_retry_for_read_cleanup(
    keys: &[Vec<u8>],
    primary_lock: &[u8],
    skip_checked_primary: bool,
    start_version: u64,
    commit_version: u64,
    is_txn_file: bool,
    pd_client: Arc<impl PdClient>,
    keyspace: Keyspace,
    keyspace_name: Option<&str>,
    rpc_interceptor: Option<RpcInterceptorChain>,
    resource_group_name: Option<&str>,
    resource_control: Option<ResourceGroupControllerHandle>,
    ru_details: Option<Arc<crate::RuDetails>>,
    mut backoff: Backoff,
) -> Result<()> {
    // `resolveLock(lite=true)` skips its already-checked primary. A multi-key
    // batch deliberately retains the primary, matching client-go's
    // `resolveRegionLocks` path.
    if skip_checked_primary && keys.len() == 1 && keys[0] == primary_lock {
        return Ok(());
    }

    loop {
        let mut regions: HashMap<RegionVerId, (RegionStore, Vec<Vec<u8>>)> = HashMap::new();
        for key in keys {
            let store = pd_client.clone().store_for_key(&key.clone().into()).await?;
            regions
                .entry(store.region_with_leader.ver_id())
                .or_insert_with(|| (store, Vec::new()))
                .1
                .push(key.clone());
        }

        let mut retry = false;
        for (_, (store, region_keys)) in regions {
            let mut request =
                requests::new_resolve_lock_request(start_version, commit_version, is_txn_file);
            request.keys = region_keys;
            let plan_builder =
                match crate::request::PlanBuilder::new(pd_client.clone(), keyspace, request)
                    .keyspace_name_option(keyspace_name)
                    .rpc_interceptor_option(rpc_interceptor.clone())
                    .resource_group_option(resource_group_name)
                    .resource_control_option(resource_control.clone())
                    .ru_details_option(ru_details.clone())
                    .max_execution_duration(LOCK_RESOLVER_MAX_WRITE_EXECUTION_DURATION)
                    .single_region_with_store(store.clone())
                    .await
                {
                    Ok(plan_builder) => plan_builder,
                    Err(Error::LeaderNotFound { region }) => {
                        pd_client.invalidate_region_cache(region).await;
                        retry = true;
                        break;
                    }
                    Err(err) => return Err(err),
                };
            match plan_builder.extract_error().plan().execute().await {
                Ok(_) => {}
                Err(Error::ExtractedErrors(mut errors)) => match errors.pop() {
                    Some(Error::RegionError(error)) => {
                        handle_region_error(pd_client.clone(), *error, store).await?;
                        retry = true;
                        break;
                    }
                    Some(Error::KeyError(error)) => return Err(Error::KeyError(error)),
                    Some(error) => return Err(error),
                    None => unreachable!(),
                },
                Err(error) if is_grpc_error(&error) => {
                    pd_client
                        .invalidate_region_cache(store.region_with_leader.ver_id())
                        .await;
                    invalidate_connection_for_error(
                        pd_client.as_ref(),
                        &error,
                        store.region_with_leader.get_store_id().ok(),
                    )
                    .await;
                    retry = true;
                    break;
                }
                Err(error) => return Err(error),
            }
        }
        if !retry {
            return Ok(());
        }
        match backoff.next_delay_duration() {
            Some(delay) => sleep(delay).await,
            None => {
                return Err(Error::StringError(
                    "lite ResolveLock retry exhausted".to_owned(),
                ))
            }
        }
    }
}

async fn resolve_lock_with_retry(
    #[allow(clippy::ptr_arg)] key: &Vec<u8>,
    start_version: u64,
    commit_version: u64,
    is_txn_file: bool,
    txn_size: u64,
    pd_client: Arc<impl PdClient>,
    keyspace: Keyspace,
    keyspace_name: Option<&str>,
    rpc_interceptor: Option<RpcInterceptorChain>,
    resource_group_name: Option<&str>,
    resource_control: Option<ResourceGroupControllerHandle>,
    ru_details: Option<Arc<crate::RuDetails>>,
    backoff: Backoff,
) -> Result<RegionVerId> {
    resolve_lock_with_retry_inner(
        key,
        start_version,
        commit_version,
        is_txn_file,
        txn_size,
        pd_client,
        keyspace,
        keyspace_name,
        rpc_interceptor,
        resource_group_name,
        resource_control,
        ru_details,
        backoff,
        false,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn resolve_lock_with_retry_for_read_cleanup(
    key: &Vec<u8>,
    start_version: u64,
    commit_version: u64,
    is_txn_file: bool,
    txn_size: u64,
    pd_client: Arc<impl PdClient>,
    keyspace: Keyspace,
    keyspace_name: Option<&str>,
    rpc_interceptor: Option<RpcInterceptorChain>,
    resource_group_name: Option<&str>,
    resource_control: Option<ResourceGroupControllerHandle>,
    ru_details: Option<Arc<crate::RuDetails>>,
    backoff: Backoff,
) -> Result<RegionVerId> {
    resolve_lock_with_retry_inner(
        key,
        start_version,
        commit_version,
        is_txn_file,
        txn_size,
        pd_client,
        keyspace,
        keyspace_name,
        rpc_interceptor,
        resource_group_name,
        resource_control,
        ru_details,
        backoff,
        true,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn resolve_lock_with_retry_inner(
    #[allow(clippy::ptr_arg)] key: &Vec<u8>,
    start_version: u64,
    commit_version: u64,
    is_txn_file: bool,
    txn_size: u64,
    pd_client: Arc<impl PdClient>,
    keyspace: Keyspace,
    keyspace_name: Option<&str>,
    rpc_interceptor: Option<RpcInterceptorChain>,
    resource_group_name: Option<&str>,
    resource_control: Option<ResourceGroupControllerHandle>,
    ru_details: Option<Arc<crate::RuDetails>>,
    mut backoff: Backoff,
    server_side_async: bool,
) -> Result<RegionVerId> {
    debug!("resolving locks with retry");
    let mut attempt = 0;
    loop {
        attempt += 1;
        debug!("resolving locks: attempt {}", attempt);
        let store = pd_client.clone().store_for_key(key.into()).await?;
        let ver_id = store.region_with_leader.ver_id();
        let mut request =
            requests::new_resolve_lock_request(start_version, commit_version, is_txn_file);
        let resolve_lite = txn_size < get_global_config().tikv_client.resolve_lock_lite_threshold;
        stats::increment_lock_resolver_action("query_resolve_locks");
        if resolve_lite {
            stats::increment_lock_resolver_action("query_resolve_lock_lite");
            request.keys = vec![key.clone()];
        } else if server_side_async && NEXT_GEN {
            request.is_async = true;
        }
        let plan_builder =
            match crate::request::PlanBuilder::new(pd_client.clone(), keyspace, request)
                .keyspace_name_option(keyspace_name)
                .rpc_interceptor_option(rpc_interceptor.clone())
                .resource_group_option(resource_group_name)
                .resource_control_option(resource_control.clone())
                .ru_details_option(ru_details.clone())
                .max_execution_duration(LOCK_RESOLVER_MAX_WRITE_EXECUTION_DURATION)
                .single_region_with_store(store.clone())
                .await
            {
                Ok(plan_builder) => plan_builder,
                Err(Error::LeaderNotFound { region }) => {
                    pd_client.invalidate_region_cache(region.clone()).await;
                    match backoff.next_delay_duration() {
                        Some(duration) => {
                            sleep(duration).await;
                            continue;
                        }
                        None => return Err(Error::LeaderNotFound { region }),
                    }
                }
                Err(err) => return Err(err),
            };
        let plan = plan_builder.extract_error().plan();
        match plan.execute().await {
            Ok(_) => {
                return Ok(ver_id);
            }
            // Retry on region error
            Err(Error::ExtractedErrors(mut errors)) => {
                // ResolveLockResponse can have at most 1 error
                match errors.pop() {
                    Some(Error::RegionError(e)) => match backoff.next_delay_duration() {
                        Some(duration) => {
                            let region_error_action =
                                handle_region_error(pd_client.clone(), *e, store.clone()).await?;
                            if let crate::request::plan::RegionErrorRetry::Backoff(_) =
                                region_error_action
                            {
                                sleep(duration).await;
                            }
                            continue;
                        }
                        None => return Err(Error::RegionError(e)),
                    },
                    Some(Error::KeyError(key_err)) => {
                        // Keyspace is not truncated here because we need full key info for logging.
                        error!(
                            "resolve_lock error, unexpected resolve err: {:?}, lock: {{key: {}, start_version: {}, commit_version: {}, is_txn_file: {}}}",
                            key_err,
                            format_key_for_log(key),
                            start_version,
                            commit_version,
                            is_txn_file,
                        );
                        return Err(Error::KeyError(key_err));
                    }
                    Some(e) => return Err(e),
                    None => unreachable!(),
                }
            }
            Err(e) if is_grpc_error(&e) => match backoff.next_delay_duration() {
                Some(duration) => {
                    pd_client.invalidate_region_cache(ver_id.clone()).await;
                    invalidate_connection_for_error(
                        pd_client.as_ref(),
                        &e,
                        store.region_with_leader.get_store_id().ok(),
                    )
                    .await;
                    sleep(duration).await;
                    continue;
                }
                None => return Err(e),
            },
            Err(e) => return Err(e),
        }
    }
}

#[derive(Default, Clone)]
pub struct ResolveLocksContext {
    // Record the status of each transaction.
    resolved: Arc<Mutex<ResolvedStatusCache>>,
    resolving: Arc<StdMutex<ResolvingLocks>>,
    pub(crate) clean_regions: Arc<RwLock<HashMap<u64, HashSet<RegionVerId>>>>,
    pub(crate) rpc_interceptor: Option<RpcInterceptorChain>,
    pub(crate) resource_group_name: Option<String>,
    pub(crate) resource_control: Option<ResourceGroupControllerHandle>,
    pub(crate) ru_details: Option<Arc<crate::RuDetails>>,
}

/// Per-snapshot transaction IDs carried in TiKV read contexts.
///
/// client-go owns two `util.TSSet`s on every `KVSnapshot`, rather than on the
/// shared lock resolver: the outcome is valid only for that read timestamp.
#[derive(Default, Clone)]
pub(crate) struct ReadLockContext {
    state: Arc<StdRwLock<ReadLockState>>,
}

#[derive(Default)]
struct ReadLockState {
    resolved: HashSet<u64>,
    committed: HashSet<u64>,
}

impl ReadLockContext {
    pub(crate) fn add_resolved(&self, txn_id: u64) {
        self.state.write().unwrap().resolved.insert(txn_id);
    }

    /// Reset only the timestamp-scoped ignore-lock hints.
    ///
    /// client-go's `KVSnapshot.SetSnapshotTS` leaves committed read-through
    /// hints intact.
    pub(crate) fn clear_resolved(&self) {
        self.state.write().unwrap().resolved.clear();
    }

    pub(crate) fn add_committed(&self, txn_id: u64) {
        self.state.write().unwrap().committed.insert(txn_id);
    }

    pub(crate) fn snapshot(&self) -> (Vec<u64>, Vec<u64>) {
        let state = self.state.read().unwrap();
        (
            state.resolved.iter().copied().collect(),
            state.committed.iter().copied().collect(),
        )
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ResolveLocksOptions {
    pub async_commit_only: bool,
    pub batch_size: u32,
}

impl Default for ResolveLocksOptions {
    fn default() -> Self {
        Self {
            async_commit_only: false,
            batch_size: 1024,
        }
    }
}

impl ResolveLocksContext {
    pub async fn get_resolved(&self, txn_id: u64) -> Option<Arc<TransactionStatus>> {
        self.resolved.lock().await.statuses.get(&txn_id).cloned()
    }

    /// Record a source transaction beginning a lock-resolution attempt and
    /// return its stable slot token.
    pub async fn record_resolving_locks(
        &self,
        locks: &[kvrpcpb::LockInfo],
        caller_start_ts: u64,
    ) -> usize {
        self.record_resolving_locks_sync(locks, caller_start_ts)
    }

    pub(crate) fn record_resolving_locks_sync(
        &self,
        locks: &[kvrpcpb::LockInfo],
        caller_start_ts: u64,
    ) -> usize {
        let mut resolving = self.resolving.lock().unwrap();
        let slots = resolving.locks.entry(caller_start_ts).or_default();
        let token = slots.len();
        slots.push(Some(locks.to_vec()));
        *resolving.concurrency.entry(caller_start_ts).or_default() += 1;
        token
    }

    /// Replace the currently reported locks for a previously recorded slot.
    pub async fn update_resolving_locks(
        &self,
        locks: &[kvrpcpb::LockInfo],
        caller_start_ts: u64,
        token: usize,
    ) {
        self.update_resolving_locks_sync(locks, caller_start_ts, token);
    }

    pub(crate) fn update_resolving_locks_sync(
        &self,
        locks: &[kvrpcpb::LockInfo],
        caller_start_ts: u64,
        token: usize,
    ) {
        let mut resolving = self.resolving.lock().unwrap();
        let slot = resolving
            .locks
            .get_mut(&caller_start_ts)
            .and_then(|slots| slots.get_mut(token))
            .expect("updating an active resolving-lock token");
        *slot = Some(locks.to_vec());
    }

    /// Mark a recorded lock-resolution attempt complete and drop its caller
    /// entry only after the source-equivalent last active slot finishes.
    pub async fn resolving_locks_done(&self, caller_start_ts: u64, token: usize) {
        self.resolving_locks_done_sync(caller_start_ts, token);
    }

    pub(crate) fn resolving_locks_done_sync(&self, caller_start_ts: u64, token: usize) {
        let mut resolving = self.resolving.lock().unwrap();
        let slot = resolving
            .locks
            .get_mut(&caller_start_ts)
            .and_then(|slots| slots.get_mut(token))
            .expect("completing an active resolving-lock token");
        *slot = None;
        let concurrent = resolving
            .concurrency
            .get_mut(&caller_start_ts)
            .expect("resolving-lock concurrency for active token");
        *concurrent = concurrent
            .checked_sub(1)
            .expect("resolving-lock counter underflow");
        if *concurrent == 0 {
            resolving.concurrency.remove(&caller_start_ts);
            resolving.locks.remove(&caller_start_ts);
        }
    }

    /// Snapshot every currently active resolving lock in source insertion
    /// order, omitting completed slots.
    pub async fn resolving_locks(&self) -> Vec<ResolvingLock> {
        let resolving = self.resolving.lock().unwrap();
        resolving
            .locks
            .iter()
            .flat_map(|(&txn_id, slots)| {
                slots.iter().flatten().flat_map(move |locks| {
                    locks.iter().map(move |lock| ResolvingLock {
                        txn_id,
                        lock_txn_id: lock.lock_version,
                        key: lock.key.clone(),
                        primary: lock.primary_lock.clone(),
                    })
                })
            })
            .collect()
    }

    pub async fn save_resolved(&mut self, txn_id: u64, txn_status: Arc<TransactionStatus>) {
        assert!(
            txn_status.is_cacheable(),
            "only determined transaction statuses may enter the resolved cache"
        );
        let mut cache = self.resolved.lock().await;
        if let Some(existing) = cache.statuses.get(&txn_id) {
            assert!(
                same_determined_status(existing, &txn_status),
                "conflicting determined status for transaction {txn_id}"
            );
            return;
        }
        cache.statuses.insert(txn_id, txn_status);
        cache.insertion_order.push_back(txn_id);
        if cache.statuses.len() > RESOLVED_CACHE_SIZE {
            let oldest = cache
                .insertion_order
                .pop_front()
                .expect("nonempty status cache has an insertion order");
            cache.statuses.remove(&oldest);
        }
    }

    pub async fn is_region_cleaned(&self, txn_id: u64, region: &RegionVerId) -> bool {
        self.clean_regions
            .read()
            .await
            .get(&txn_id)
            .map(|regions| regions.contains(region))
            .unwrap_or(false)
    }

    pub async fn save_cleaned_region(&mut self, txn_id: u64, region: RegionVerId) {
        self.clean_regions
            .write()
            .await
            .entry(txn_id)
            .or_insert_with(HashSet::new)
            .insert(region);
    }
}

/// Owns a resolving-lock observer slot for the lifetime of a retry operation.
///
/// Unlike a normal async cleanup call, observer completion must also happen
/// when the owning future is dropped. The registry uses a short synchronous
/// critical section specifically so this guard can uphold client-go's deferred
/// `ResolveLocksDone` lifetime without relying on a Tokio runtime in `Drop`.
pub(crate) struct ResolvingLocksGuard {
    context: ResolveLocksContext,
    caller_start_ts: u64,
    token: usize,
}

impl ResolvingLocksGuard {
    pub(crate) fn new(
        context: ResolveLocksContext,
        locks: &[kvrpcpb::LockInfo],
        caller_start_ts: u64,
    ) -> Self {
        let token = context.record_resolving_locks_sync(locks, caller_start_ts);
        Self {
            context,
            caller_start_ts,
            token,
        }
    }

    pub(crate) fn update(&self, locks: &[kvrpcpb::LockInfo]) {
        self.context
            .update_resolving_locks_sync(locks, self.caller_start_ts, self.token);
    }
}

impl Drop for ResolvingLocksGuard {
    fn drop(&mut self) {
        self.context
            .resolving_locks_done_sync(self.caller_start_ts, self.token);
    }
}

fn same_determined_status(left: &TransactionStatus, right: &TransactionStatus) -> bool {
    match (&left.kind, &right.kind) {
        (TransactionStatusKind::RolledBack, TransactionStatusKind::RolledBack) => true,
        (TransactionStatusKind::Committed(left), TransactionStatusKind::Committed(right)) => {
            left.version() == right.version()
        }
        _ => false,
    }
}

pub struct LockResolver {
    ctx: ResolveLocksContext,
}

impl LockResolver {
    pub fn new(ctx: ResolveLocksContext) -> Self {
        Self { ctx }
    }

    /// Source `RecordResolvingLocks` compatibility entry point.
    pub async fn record_resolving_locks(
        &self,
        locks: &[kvrpcpb::LockInfo],
        caller_start_ts: u64,
    ) -> usize {
        self.ctx
            .record_resolving_locks(locks, caller_start_ts)
            .await
    }

    /// Source `UpdateResolvingLocks` compatibility entry point.
    pub async fn update_resolving_locks(
        &self,
        locks: &[kvrpcpb::LockInfo],
        caller_start_ts: u64,
        token: usize,
    ) {
        self.ctx
            .update_resolving_locks(locks, caller_start_ts, token)
            .await;
    }

    /// Source `ResolveLocksDone` compatibility entry point.
    pub async fn resolving_locks_done(&self, caller_start_ts: u64, token: usize) {
        self.ctx.resolving_locks_done(caller_start_ts, token).await;
    }

    /// Source `Resolving` compatibility entry point.
    pub async fn resolving_locks(&self) -> Vec<ResolvingLock> {
        self.ctx.resolving_locks().await
    }

    /// Source `resolvePessimisticLock` uses PessimisticRollback after the
    /// primary status check. It is intentionally not ResolveLock: the latter
    /// could roll back a different lock type with the same start timestamp.
    async fn resolve_pessimistic_lock(
        &mut self,
        pd_client: Arc<impl PdClient>,
        keyspace: Keyspace,
        keyspace_name: Option<&str>,
        lock: &kvrpcpb::LockInfo,
    ) -> Result<()> {
        if lock.key == lock.primary_lock {
            return Ok(());
        }
        let for_update_ts = if lock.lock_for_update_ts == 0 {
            u64::MAX
        } else {
            lock.lock_for_update_ts
        };
        let request = requests::new_pessimistic_rollback_request(
            vec![lock.key.clone()],
            lock.lock_version,
            for_update_ts,
        );
        let plan = crate::request::PlanBuilder::new(pd_client, keyspace, request)
            .keyspace_name_option(keyspace_name)
            .rpc_interceptor_option(self.ctx.rpc_interceptor.clone())
            .resource_group_option(self.ctx.resource_group_name.as_deref())
            .resource_control_option(self.ctx.resource_control.clone())
            .ru_details_option(self.ctx.ru_details.clone())
            .max_execution_duration(LOCK_RESOLVER_MAX_WRITE_EXECUTION_DURATION)
            .retry_multi_region(DEFAULT_REGION_BACKOFF)
            .extract_error()
            .plan();
        plan.execute().await?;
        Ok(())
    }

    /// _Cleanup_ the given locks. Returns whether all the given locks are resolved.
    ///
    /// Note: Will rollback RUNNING transactions. ONLY use in GC.
    pub async fn cleanup_locks(
        &mut self,
        store: RegionStore,
        locks: Vec<kvrpcpb::LockInfo>,
        pd_client: Arc<impl PdClient>, // TODO: make pd_client a member of LockResolver
        keyspace: Keyspace,
        keyspace_name: Option<&str>,
    ) -> Result<()> {
        // Defense in depth: CleanupLocks::execute refuses these before its filters,
        // but this entry point is public within the crate.
        reject_shared_locks(&locks)?;
        if locks.is_empty() {
            return Ok(());
        }

        fail_point!("before-cleanup-locks", |_| { Ok(()) });

        let region = store.region_with_leader.ver_id();

        let mut txn_infos = HashMap::new();
        for l in locks {
            let txn_id = l.lock_version;
            if txn_infos.contains_key(&txn_id) || self.ctx.is_region_cleaned(txn_id, &region).await
            {
                continue;
            }

            // Use currentTS = math.MaxUint64 means rollback the txn, no matter the lock is expired or not!
            let mut status = self
                .check_txn_status(
                    pd_client.clone(),
                    keyspace,
                    keyspace_name,
                    txn_id,
                    l.primary_lock.clone(),
                    0,
                    u64::MAX,
                    true,
                    false,
                    is_pessimistic_lock(&l),
                    l.is_txn_file,
                )
                .await?;

            // client-go's BatchResolveLocks handles pessimistic locks after
            // their status check with PessimisticRollback. They must not be
            // included in the ordinary batch ResolveLock transaction list.
            if is_pessimistic_lock(&l) {
                self.resolve_pessimistic_lock(pd_client.clone(), keyspace, keyspace_name, &l)
                    .await?;
                continue;
            }

            // If the transaction uses async commit, check_txn_status will reject rolling back the primary lock.
            // Then we need to check the secondary locks to determine the final status of the transaction.
            let async_primary = match &status.kind {
                TransactionStatusKind::Locked(_, lock_info) if lock_info.use_async_commit => {
                    Some((lock_info.secondaries.clone(), lock_info.min_commit_ts))
                }
                _ => None,
            };
            if let Some((secondary_keys, primary_min_commit_ts)) = async_primary {
                let mut secondary_status = self
                    .check_all_secondaries(
                        pd_client.clone(),
                        keyspace,
                        keyspace_name,
                        secondary_keys,
                        txn_id,
                    )
                    .await?;
                let commit_ts =
                    secondary_status.determine_commit_ts(txn_id, primary_min_commit_ts)?;
                debug!(
                    "secondary status, txn_id:{}, commit_ts:{:?}, fallback_2pc:{}",
                    txn_id, commit_ts, secondary_status.fallback_2pc,
                );

                if commit_ts.is_none() {
                    debug!("fallback to 2pc, txn_id:{}, check_txn_status again", txn_id);
                    let resolving_pessimistic_lock = is_pessimistic_lock(&l);
                    status = self
                        .check_txn_status(
                            pd_client.clone(),
                            keyspace,
                            keyspace_name,
                            txn_id,
                            l.primary_lock,
                            0,
                            u64::MAX,
                            true,
                            true,
                            resolving_pessimistic_lock,
                            l.is_txn_file,
                        )
                        .await?;
                } else {
                    txn_infos.insert(txn_id, (commit_ts.unwrap(), l.is_txn_file));
                    continue;
                }
            }

            match &status.kind {
                TransactionStatusKind::Locked(_, lock_info) => {
                    error!(
                        "cleanup_locks fail to clean locks, this result is not expected. txn_id:{}",
                        txn_id
                    );
                    return Err(Error::ResolveLockError(vec![lock_info.clone()]));
                }
                TransactionStatusKind::Committed(ts) => {
                    txn_infos.insert(txn_id, (ts.version(), l.is_txn_file))
                }
                TransactionStatusKind::RolledBack => txn_infos.insert(txn_id, (0, l.is_txn_file)),
            };
        }

        // Source `BatchResolveLocks` returns immediately when every lock was
        // already handled by its per-region deduplication cache. Do not emit
        // an empty BatchResolveLock RPC.
        if txn_infos.is_empty() {
            return Ok(());
        }

        debug!(
            "batch resolve locks, region:{:?}, txn:{:?}",
            store.region_with_leader.ver_id(),
            txn_infos
        );
        let mut txn_ids = Vec::with_capacity(txn_infos.len());
        let mut txn_info_vec = Vec::with_capacity(txn_infos.len());
        for (txn_id, (commit_ts, is_txn_file)) in txn_infos.into_iter() {
            txn_ids.push(txn_id);
            let mut txn_info = TxnInfo::default();
            txn_info.txn = txn_id;
            txn_info.status = commit_ts;
            txn_info.is_txn_file = is_txn_file;
            txn_info_vec.push(txn_info);
        }
        let cleaned_region = self
            .batch_resolve_locks(
                pd_client.clone(),
                keyspace,
                keyspace_name,
                store.clone(),
                txn_info_vec,
            )
            .await?;
        for txn_id in txn_ids {
            self.ctx
                .save_cleaned_region(txn_id, cleaned_region.clone())
                .await;
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn check_txn_status(
        &mut self,
        pd_client: Arc<impl PdClient>,
        keyspace: Keyspace,
        keyspace_name: Option<&str>,
        txn_id: u64,
        primary: Vec<u8>,
        caller_start_ts: u64,
        current_ts: u64,
        rollback_if_not_exist: bool,
        force_sync_commit: bool,
        resolving_pessimistic_lock: bool,
        is_txn_file: bool,
    ) -> Result<Arc<TransactionStatus>> {
        if let Some(txn_status) = self.ctx.get_resolved(txn_id).await {
            return Ok(txn_status);
        }

        // CheckTxnStatus may meet the following cases:
        // 1. LOCK
        // 1.1 Lock expired -- orphan lock, fail to update TTL, crash recovery etc.
        // 1.2 Lock TTL -- active transaction holding the lock.
        // 2. NO LOCK
        // 2.1 Txn Committed
        // 2.2 Txn Rollbacked -- rollback itself, rollback by others, GC tomb etc.
        // 2.3 No lock -- pessimistic lock rollback, concurrence prewrite.
        stats::increment_lock_resolver_action("query_txn_status");
        let req = new_check_txn_status_request(
            primary,
            txn_id,
            caller_start_ts,
            current_ts,
            rollback_if_not_exist,
            force_sync_commit,
            resolving_pessimistic_lock,
            is_txn_file,
        );
        let plan = crate::request::PlanBuilder::new(pd_client.clone(), keyspace, req)
            .keyspace_name_option(keyspace_name)
            .rpc_interceptor_option(self.ctx.rpc_interceptor.clone())
            .resource_group_option(self.ctx.resource_group_name.as_deref())
            .resource_control_option(self.ctx.resource_control.clone())
            .ru_details_option(self.ctx.ru_details.clone())
            .max_execution_duration(LOCK_RESOLVER_MAX_WRITE_EXECUTION_DURATION)
            .retry_multi_region(DEFAULT_REGION_BACKOFF)
            .merge(CollectSingle)
            .extract_error()
            .post_process_default()
            .plan();
        let mut status: TransactionStatus = match plan.execute().await {
            Ok(status) => status,
            Err(Error::ExtractedErrors(mut errors)) => match errors.pop() {
                Some(Error::KeyError(key_err)) => {
                    if let Some(txn_not_found) = key_err.txn_not_found {
                        return Err(Error::TxnNotFound(txn_not_found));
                    }
                    // TODO: handle primary mismatch error.
                    return Err(Error::KeyError(key_err));
                }
                Some(err) => return Err(err),
                None => unreachable!(),
            },
            Err(err) => return Err(err),
        };

        let current = pd_client.clone().get_timestamp().await?;
        status.check_ttl(current);
        match &status.kind {
            TransactionStatusKind::Committed(_) => {
                stats::increment_lock_resolver_action("query_txn_status_committed");
            }
            TransactionStatusKind::RolledBack => {
                stats::increment_lock_resolver_action("query_txn_status_rolled_back");
            }
            TransactionStatusKind::Locked(_, _) => {}
        }
        let res = Arc::new(status);
        if res.is_cacheable() {
            self.ctx.save_resolved(txn_id, res.clone()).await;
        }
        Ok(res)
    }

    async fn check_all_secondaries(
        &mut self,
        pd_client: Arc<impl PdClient>,
        keyspace: Keyspace,
        keyspace_name: Option<&str>,
        keys: Vec<Vec<u8>>,
        txn_id: u64,
    ) -> Result<SecondaryLocksStatus> {
        let req = new_check_secondary_locks_request(keys, txn_id);
        let plan = crate::request::PlanBuilder::new(pd_client.clone(), keyspace, req)
            .keyspace_name_option(keyspace_name)
            .rpc_interceptor_option(self.ctx.rpc_interceptor.clone())
            .resource_group_option(self.ctx.resource_group_name.as_deref())
            .resource_control_option(self.ctx.resource_control.clone())
            .ru_details_option(self.ctx.ru_details.clone())
            .max_execution_duration(LOCK_RESOLVER_MAX_WRITE_EXECUTION_DURATION)
            .preserve_shard()
            .retry_multi_region(DEFAULT_REGION_BACKOFF)
            .extract_error()
            .merge(CollectWithShard)
            .plan();
        plan.execute().await
    }

    async fn batch_resolve_locks(
        &mut self,
        pd_client: Arc<impl PdClient>,
        keyspace: Keyspace,
        keyspace_name: Option<&str>,
        store: RegionStore,
        txn_infos: Vec<TxnInfo>,
    ) -> Result<RegionVerId> {
        let ver_id = store.region_with_leader.ver_id();
        let request = requests::new_batch_resolve_lock_request(txn_infos.clone());
        stats::increment_lock_resolver_action("batch_resolve");
        let plan = crate::request::PlanBuilder::new(pd_client.clone(), keyspace, request)
            .keyspace_name_option(keyspace_name)
            .rpc_interceptor_option(self.ctx.rpc_interceptor.clone())
            .resource_group_option(self.ctx.resource_group_name.as_deref())
            .resource_control_option(self.ctx.resource_control.clone())
            .ru_details_option(self.ctx.ru_details.clone())
            .max_execution_duration(LOCK_RESOLVER_MAX_WRITE_EXECUTION_DURATION)
            .single_region_with_store(store.clone())
            .await?
            .extract_error()
            .plan();
        let _ = plan.execute().await?;
        Ok(ver_id)
    }

    #[allow(clippy::too_many_arguments)]
    async fn get_txn_status_from_lock(
        &mut self,
        mut backoff: Backoff,
        lock: &kvrpcpb::LockInfo,
        caller_start_ts: u64,
        current_ts: u64,
        force_sync_commit: bool,
        pd_client: Arc<impl PdClient>,
        keyspace: Keyspace,
        keyspace_name: Option<&str>,
    ) -> Result<Arc<TransactionStatus>> {
        let current_ts = if lock.lock_ttl == 0 {
            // NOTE: lock_ttl = 0 is a special protocol!!!
            // When the pessimistic txn prewrite meets locks of a txn, it should resolve the lock **unconditionally**.
            // In this case, TiKV use lock TTL = 0 to notify client, and client should resolve the lock!
            // Set current_ts to max uint64 to make the lock expired.
            u64::MAX
        } else {
            current_ts
        };

        let mut rollback_if_not_exist = false;
        loop {
            match self
                .check_txn_status(
                    pd_client.clone(),
                    keyspace,
                    keyspace_name,
                    lock.lock_version,
                    lock.primary_lock.clone(),
                    caller_start_ts,
                    current_ts,
                    rollback_if_not_exist,
                    force_sync_commit,
                    is_pessimistic_lock(lock),
                    lock.is_txn_file,
                )
                .await
            {
                Ok(status) => return Ok(status),
                Err(Error::TxnNotFound(txn_not_found)) => {
                    let current = pd_client.clone().get_timestamp().await?;
                    if lock_until_expired_ms(lock.lock_version, lock.lock_ttl, current) <= 0 {
                        warn!(
                            "lock txn not found, lock has expired, lock {:?}, caller_start_ts {}, current_ts {}",
                            lock, caller_start_ts, current_ts
                        );
                        rollback_if_not_exist = true;
                        continue;
                    } else if is_pessimistic_lock(lock) {
                        let status = TransactionStatus {
                            kind: TransactionStatusKind::Locked(lock.lock_ttl, lock.clone()),
                            action: kvrpcpb::Action::NoAction,
                            is_expired: false,
                        };
                        return Ok(Arc::new(status));
                    }

                    if let Some(duration) = backoff.next_delay_duration() {
                        sleep(duration).await;
                        continue;
                    }
                    return Err(Error::TxnNotFound(txn_not_found));
                }
                Err(Error::KeyError(key_error))
                    if is_pessimistic_lock(lock) && key_error.primary_mismatch.is_some() =>
                {
                    // client-go's primaryMismatch is valid only while resolving a
                    // pessimistic secondary. Treat it as an already-determined
                    // rollback so the caller executes PessimisticRollback on the
                    // actual lock rather than surfacing a protocol error.
                    return Ok(Arc::new(TransactionStatus {
                        kind: TransactionStatusKind::RolledBack,
                        action: kvrpcpb::Action::NoAction,
                        is_expired: false,
                    }));
                }
                Err(Error::MultipleKeyErrors(errors))
                    if is_pessimistic_lock(lock)
                        && matches!(
                            errors.first(),
                            Some(Error::KeyError(key_error)) if key_error.primary_mismatch.is_some()
                        ) =>
                {
                    return Ok(Arc::new(TransactionStatus {
                        kind: TransactionStatusKind::RolledBack,
                        action: kvrpcpb::Action::NoAction,
                        is_expired: false,
                    }));
                }
                Err(err) => return Err(err),
            }
        }
    }
}

pub trait HasLocks {
    fn take_locks(&mut self) -> Vec<kvrpcpb::LockInfo> {
        Vec::new()
    }
}

// Return duration in milliseconds until lock expired.
// If the lock has expired, return a negative value.
pub fn lock_until_expired_ms(lock_version: u64, ttl: u64, current: Timestamp) -> i64 {
    Timestamp::from_version(lock_version).physical + ttl as i64 - current.physical
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    use fail::FailScenario;
    use serial_test::serial;

    use super::*;
    use crate::mock::MockKvClient;
    use crate::mock::MockPdClient;
    use crate::proto::errorpb;
    use crate::{RequestWaitResult, ResourceControlRequestInfo, ResourceGroupController};
    use crate::{ResponseWaitResult, Result};

    struct ResolverResourceController(Arc<AtomicUsize>);

    #[async_trait::async_trait]
    impl ResourceGroupController for ResolverResourceController {
        async fn on_request_wait(
            &self,
            resource_group_name: &str,
            _: ResourceControlRequestInfo,
        ) -> Result<RequestWaitResult> {
            assert_eq!(resource_group_name, "resolver-rg");
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(RequestWaitResult {
                consumption: crate::proto::resource_manager::Consumption {
                    r_r_u: 2.0,
                    w_r_u: 3.0,
                    ..Default::default()
                },
                wait_duration: Duration::from_millis(2),
                ..Default::default()
            })
        }

        fn on_response_wait(
            &self,
            resource_group_name: &str,
            _: ResourceControlRequestInfo,
            _: crate::ResourceControlResponseInfo,
        ) -> Result<ResponseWaitResult> {
            assert_eq!(resource_group_name, "resolver-rg");
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(ResponseWaitResult {
                consumption: crate::proto::resource_manager::Consumption {
                    r_r_u: 5.0,
                    w_r_u: 7.0,
                    ..Default::default()
                },
                wait_duration: Duration::from_millis(3),
            })
        }
    }

    #[test]
    fn shared_lock_wrappers_are_refused_but_shared_pessimistic_holders_are_not() {
        let plain = kvrpcpb::LockInfo {
            key: b"k1".to_vec(),
            lock_version: 7,
            ..Default::default()
        };
        assert!(reject_shared_locks(std::slice::from_ref(&plain)).is_ok());

        // A wrapper: key/lock_version deliberately unset per the contract — resolving
        // it would check transaction 0. Must be refused, not resolved or filtered.
        let wrapper = kvrpcpb::LockInfo {
            shared_lock_infos: vec![kvrpcpb::LockInfo {
                key: b"k2".to_vec(),
                lock_version: 8,
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(reject_shared_locks(&[plain.clone(), wrapper]).is_err());

        // client-go's Lock.IsShared only rejects this wrapper form.
        let by_op = kvrpcpb::LockInfo {
            lock_type: kvrpcpb::Op::SharedLock as i32,
            ..Default::default()
        };
        assert!(reject_shared_locks(&[by_op]).is_err());

        // A concrete shared-pessimistic holder is handled as pessimistic.
        let by_op = kvrpcpb::LockInfo {
            lock_type: kvrpcpb::Op::SharedPessimisticLock as i32,
            ..Default::default()
        };
        assert!(reject_shared_locks(&[by_op.clone()]).is_ok());
        assert!(is_pessimistic_lock(&by_op));
    }

    #[test]
    fn source_key_error_lock_extraction_expands_shared_holders() {
        let first = kvrpcpb::LockInfo {
            key: b"key-1".to_vec(),
            lock_version: 7,
            ..Default::default()
        };
        let second = kvrpcpb::LockInfo {
            key: b"key-2".to_vec(),
            lock_version: 8,
            ..Default::default()
        };
        let shared_error = kvrpcpb::KeyError {
            locked: Some(kvrpcpb::LockInfo {
                lock_type: kvrpcpb::Op::SharedLock as i32,
                shared_lock_infos: vec![first.clone(), second.clone()],
                ..Default::default()
            }),
            ..Default::default()
        };

        assert_eq!(
            extract_lock_from_key_error(&shared_error)
                .unwrap()
                .shared_lock_infos,
            [first.clone(), second.clone()]
        );
        assert_eq!(
            extract_locks_from_key_error(&shared_error).unwrap(),
            [first, second]
        );

        let exclusive = kvrpcpb::KeyError {
            locked: Some(kvrpcpb::LockInfo {
                key: b"key".to_vec(),
                lock_version: 9,
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(extract_locks_from_key_error(&exclusive).unwrap().len(), 1);
        assert!(matches!(
            extract_locks_from_key_error(&kvrpcpb::KeyError::default()),
            Err(Error::KeyError(_))
        ));
    }

    #[tokio::test]
    async fn source_resolved_status_cache_is_fifo_and_bounded() {
        let mut context = ResolveLocksContext::default();
        for txn_id in 0..=RESOLVED_CACHE_SIZE as u64 {
            context
                .save_resolved(
                    txn_id,
                    Arc::new(TransactionStatus {
                        kind: TransactionStatusKind::Committed(Timestamp::from_version(txn_id + 1)),
                        action: kvrpcpb::Action::NoAction,
                        is_expired: false,
                    }),
                )
                .await;
        }

        assert!(context.get_resolved(0).await.is_none());
        assert!(context.get_resolved(1).await.is_some());
        assert!(context
            .get_resolved(RESOLVED_CACHE_SIZE as u64)
            .await
            .is_some());
        assert!(context.clone().get_resolved(1).await.is_some());
    }

    #[tokio::test]
    async fn source_resolving_locks_tracks_slots_updates_and_last_completion() {
        let resolver = LockResolver::new(ResolveLocksContext::default());
        let first = kvrpcpb::LockInfo {
            key: b"first".to_vec(),
            primary_lock: b"primary".to_vec(),
            lock_version: 11,
            ..Default::default()
        };
        let second = kvrpcpb::LockInfo {
            key: b"second".to_vec(),
            primary_lock: b"primary".to_vec(),
            lock_version: 12,
            ..Default::default()
        };
        let replacement = kvrpcpb::LockInfo {
            key: b"replacement".to_vec(),
            primary_lock: b"primary-2".to_vec(),
            lock_version: 13,
            ..Default::default()
        };

        let first_token = resolver.record_resolving_locks(&[first], 100).await;
        let second_token = resolver.record_resolving_locks(&[second], 100).await;
        assert_eq!((first_token, second_token), (0, 1));
        resolver
            .update_resolving_locks(&[replacement], 100, first_token)
            .await;

        assert_eq!(
            resolver.resolving_locks().await,
            vec![
                ResolvingLock {
                    txn_id: 100,
                    lock_txn_id: 13,
                    key: b"replacement".to_vec(),
                    primary: b"primary-2".to_vec(),
                },
                ResolvingLock {
                    txn_id: 100,
                    lock_txn_id: 12,
                    key: b"second".to_vec(),
                    primary: b"primary".to_vec(),
                },
            ]
        );

        resolver.resolving_locks_done(100, first_token).await;
        assert_eq!(
            resolver.resolving_locks().await,
            vec![ResolvingLock {
                txn_id: 100,
                lock_txn_id: 12,
                key: b"second".to_vec(),
                primary: b"primary".to_vec(),
            }]
        );
        resolver.resolving_locks_done(100, second_token).await;
        assert!(resolver.resolving_locks().await.is_empty());
    }

    #[tokio::test]
    async fn source_cleanup_skips_an_empty_batch_resolve() {
        let region = MockPdClient::region1();
        let mut context = ResolveLocksContext::default();
        context.save_cleaned_region(7, region.ver_id()).await;
        let mut resolver = LockResolver::new(context);
        let store = RegionStore::new(
            region,
            Arc::new(MockKvClient::with_dispatch_hook(|_| {
                panic!("an already-clean lock must not send BatchResolveLock")
            })),
        );

        resolver
            .cleanup_locks(
                store,
                vec![kvrpcpb::LockInfo {
                    lock_version: 7,
                    ..Default::default()
                }],
                Arc::new(MockPdClient::default()),
                Keyspace::Disable,
                None,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn source_cleanup_pessimistic_lock_uses_rollback_not_batch_resolve() {
        let rollback_count = Arc::new(AtomicUsize::new(0));
        let rollback_count_captured = rollback_count.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req
                    .downcast_ref::<kvrpcpb::CheckTxnStatusRequest>()
                    .is_some()
                {
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 2,
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if req
                    .downcast_ref::<kvrpcpb::PessimisticRollbackRequest>()
                    .is_some()
                {
                    rollback_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                assert!(req.downcast_ref::<kvrpcpb::ResolveLockRequest>().is_none());
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));
        let store = RegionStore::new(
            MockPdClient::region1(),
            Arc::new(MockKvClient::with_dispatch_hook(|_| {
                panic!("pessimistic cleanup must not send BatchResolveLock")
            })),
        );
        let mut resolver = LockResolver::new(ResolveLocksContext::default());
        resolver
            .cleanup_locks(
                store,
                vec![kvrpcpb::LockInfo {
                    key: vec![2],
                    primary_lock: vec![1],
                    lock_version: 1,
                    lock_type: kvrpcpb::Op::PessimisticLock as i32,
                    ..Default::default()
                }],
                client,
                Keyspace::Disable,
                None,
            )
            .await
            .unwrap();
        assert_eq!(rollback_count.load(Ordering::SeqCst), 1);
    }

    #[rstest::rstest]
    #[case(Keyspace::Disable)]
    #[case(Keyspace::Enable { keyspace_id: 0 })]
    #[tokio::test]
    #[serial]
    async fn test_resolve_lock_with_retry(#[case] keyspace: Keyspace) {
        let _scenario = FailScenario::setup();

        const MAX_REGION_ERROR_RETRIES: u32 = 10;
        let backoff = Backoff::no_jitter_backoff(0, 0, MAX_REGION_ERROR_RETRIES);

        // Test resolve lock within retry limit
        fail::cfg(
            "region-error",
            &format!("{}*return", MAX_REGION_ERROR_RETRIES),
        )
        .unwrap();

        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |_: &dyn Any| {
                fail::fail_point!("region-error", |_| {
                    let resp = kvrpcpb::ResolveLockResponse {
                        // StaleCommand is source-retryable. Do not use an
                        // empty error as a test retry sentinel: client-go
                        // invalidates and returns unknown region errors.
                        region_error: Some(errorpb::Error {
                            stale_command: Some(Default::default()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    };
                    Ok(Box::new(resp) as Box<dyn Any>)
                });
                Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>)
            },
        )));

        let key = vec![1];
        let region1 = MockPdClient::region1();
        let resolved_region = resolve_lock_with_retry(
            &key,
            1,
            2,
            false,
            u64::MAX,
            client.clone(),
            keyspace,
            None,
            None,
            None,
            None,
            None,
            backoff.clone(),
        )
        .await
        .unwrap();
        assert_eq!(region1.ver_id(), resolved_region);

        // Test resolve lock over retry limit
        fail::cfg(
            "region-error",
            &format!("{}*return", MAX_REGION_ERROR_RETRIES + 1),
        )
        .unwrap();
        let key = vec![100];
        resolve_lock_with_retry(
            &key,
            3,
            4,
            false,
            u64::MAX,
            client,
            keyspace,
            None,
            None,
            None,
            None,
            None,
            backoff,
        )
        .await
        .expect_err("should return error");
    }

    #[tokio::test]
    async fn source_lite_lock_resolution_scopes_resolve_lock_to_the_key() {
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::ResolveLockRequest>()
                    .expect("expected ResolveLock request");
                assert_eq!(req.keys, vec![vec![1]]);
                Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>)
            },
        )));
        let key = vec![1];
        resolve_lock_with_retry(
            &key,
            1,
            2,
            false,
            get_global_config().tikv_client.resolve_lock_lite_threshold - 1,
            client,
            Keyspace::Disable,
            None,
            None,
            None,
            None,
            None,
            Backoff::no_jitter_backoff(0, 0, 1),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn source_lite_threshold_itself_uses_region_resolution() {
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::ResolveLockRequest>()
                    .expect("expected ResolveLock request");
                assert!(req.keys.is_empty());
                Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>)
            },
        )));
        let key = vec![1];
        resolve_lock_with_retry(
            &key,
            1,
            2,
            false,
            get_global_config().tikv_client.resolve_lock_lite_threshold,
            client,
            Keyspace::Disable,
            None,
            None,
            None,
            None,
            None,
            Backoff::no_jitter_backoff(0, 0, 1),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn source_lite_primary_skips_resolve_lock_after_status_check() {
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |req: &dyn Any| {
                if req
                    .downcast_ref::<kvrpcpb::CheckTxnStatusRequest>()
                    .is_some()
                {
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 2,
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                assert!(req.downcast_ref::<kvrpcpb::ResolveLockRequest>().is_none());
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));
        let lock = kvrpcpb::LockInfo {
            key: vec![1],
            primary_lock: vec![1],
            lock_version: 1,
            txn_size: get_global_config().tikv_client.resolve_lock_lite_threshold - 1,
            ..Default::default()
        };
        assert!(resolve_locks_with_context(
            vec![lock],
            Timestamp::default(),
            client,
            Keyspace::Disable,
            None,
            ResolveLocksContext::default(),
        )
        .await
        .unwrap()
        .is_empty());
    }

    #[tokio::test]
    async fn source_read_resolution_reads_through_committed_locks_without_waiting_for_cleanup() {
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |req: &dyn Any| {
                if req
                    .downcast_ref::<kvrpcpb::CheckTxnStatusRequest>()
                    .is_some()
                {
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 2,
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if req.downcast_ref::<kvrpcpb::ResolveLockRequest>().is_some() {
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));
        let read_locks = ReadLockContext::default();
        let lock = kvrpcpb::LockInfo {
            key: vec![1],
            primary_lock: vec![1],
            lock_version: 1,
            ..Default::default()
        };

        assert!(resolve_locks_for_read_with_context(
            vec![lock],
            Timestamp::from_version(3),
            client,
            Keyspace::Disable,
            None,
            ResolveLocksContext::default(),
            &read_locks,
        )
        .await
        .unwrap()
        .is_empty());
        let (resolved, committed) = read_locks.snapshot();
        assert!(resolved.is_empty());
        assert_eq!(committed, [1]);
    }

    #[tokio::test]
    async fn source_read_lite_cleanup_batches_transaction_keys_per_region() {
        let cleanup_sent = Arc::new(tokio::sync::Notify::new());
        let cleanup_sent_by_hook = Arc::clone(&cleanup_sent);
        let cleanup_requests = Arc::new(AtomicUsize::new(0));
        let cleanup_requests_by_hook = Arc::clone(&cleanup_requests);
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req
                    .downcast_ref::<kvrpcpb::CheckTxnStatusRequest>()
                    .is_some()
                {
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 2,
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    assert_eq!(req.keys, [vec![1], vec![2]]);
                    assert!(!req.is_async);
                    cleanup_requests_by_hook.fetch_add(1, Ordering::SeqCst);
                    cleanup_sent_by_hook.notify_one();
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));
        let txn_size = get_global_config().tikv_client.resolve_lock_lite_threshold - 1;
        let read_locks = ReadLockContext::default();
        let locks = vec![
            kvrpcpb::LockInfo {
                key: vec![1],
                primary_lock: vec![0],
                lock_version: 1,
                txn_size,
                ..Default::default()
            },
            kvrpcpb::LockInfo {
                key: vec![2],
                primary_lock: vec![0],
                lock_version: 1,
                txn_size,
                ..Default::default()
            },
        ];

        assert!(resolve_locks_for_read_with_context(
            locks,
            Timestamp::from_version(3),
            client,
            Keyspace::Disable,
            None,
            ResolveLocksContext::default(),
            &read_locks,
        )
        .await
        .unwrap()
        .is_empty());
        assert_eq!(read_locks.snapshot().1, [1]);
        tokio::time::timeout(Duration::from_secs(1), cleanup_sent.notified())
            .await
            .expect("detached lite cleanup should send ResolveLock");
        assert_eq!(cleanup_requests.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn source_read_cleanup_is_detached_and_nextgen_uses_tikv_async_resolve() {
        let cleanup_sent = Arc::new(tokio::sync::Notify::new());
        let cleanup_sent_by_hook = Arc::clone(&cleanup_sent);
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req
                    .downcast_ref::<kvrpcpb::CheckTxnStatusRequest>()
                    .is_some()
                {
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 2,
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    assert_eq!(req.is_async, NEXT_GEN);
                    assert!(req.keys.is_empty());
                    cleanup_sent_by_hook.notify_one();
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));
        let read_locks = ReadLockContext::default();
        let lock = kvrpcpb::LockInfo {
            key: vec![1],
            primary_lock: vec![1],
            lock_version: 1,
            txn_size: u64::MAX,
            ..Default::default()
        };

        assert!(resolve_locks_for_read_with_context(
            vec![lock],
            Timestamp::from_version(3),
            client,
            Keyspace::Disable,
            None,
            ResolveLocksContext::default(),
            &read_locks,
        )
        .await
        .unwrap()
        .is_empty());
        tokio::time::timeout(Duration::from_secs(1), cleanup_sent.notified())
            .await
            .expect("detached read cleanup should send ResolveLock");
    }

    #[tokio::test]
    #[serial]
    async fn source_read_cleanup_metrics_track_running_tasks_and_fallbacks() {
        // Ordinary read-cleanup regressions intentionally leave their
        // detached task unjoined, so use a test-only label that concurrent
        // library tests never touch.
        const TEST_TASK_KIND: &str = "source_metrics_test";
        const TEST_FALLBACK_ACTION: &str = "source_metrics_test_fallback";
        let semaphore = Arc::new(Semaphore::new(1));
        let running_before = crate::stats::lock_resolver_async_running_tasks(TEST_TASK_KIND);
        let started = Arc::new(tokio::sync::Notify::new());
        let started_by_task = Arc::clone(&started);
        let release = Arc::new(tokio::sync::Notify::new());
        let release_by_task = Arc::clone(&release);
        schedule_read_cleanup_task_with_semaphore(
            Arc::clone(&semaphore),
            async move {
                started_by_task.notify_one();
                release_by_task.notified().await;
            },
            TEST_TASK_KIND,
            TEST_FALLBACK_ACTION,
        )
        .await;
        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("detached cleanup task should start");
        assert_eq!(
            crate::stats::lock_resolver_async_running_tasks(TEST_TASK_KIND),
            running_before + 1.0
        );
        release.notify_one();
        for _ in 0..10 {
            if crate::stats::lock_resolver_async_running_tasks(TEST_TASK_KIND) <= running_before {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(crate::stats::lock_resolver_async_running_tasks(TEST_TASK_KIND) <= running_before);

        let permits = Arc::clone(&semaphore)
            .try_acquire_owned()
            .expect("test owns its cleanup permit");
        let before = crate::stats::lock_resolver_action_count(TEST_FALLBACK_ACTION);
        schedule_read_cleanup_task_with_semaphore(
            semaphore,
            async {},
            TEST_TASK_KIND,
            TEST_FALLBACK_ACTION,
        )
        .await;
        assert_eq!(
            crate::stats::lock_resolver_action_count(TEST_FALLBACK_ACTION),
            before + 1
        );
        drop(permits);
    }

    #[tokio::test]
    #[serial]
    async fn source_async_resolve_pool_releases_capacity_and_falls_back() {
        const TEST_TASK_KIND: &str = "source_pool_test";
        const TEST_FALLBACK_ACTION: &str = "source_pool_test_fallback";
        let semaphore = Arc::new(Semaphore::new(5));
        let running_before = crate::stats::lock_resolver_async_running_tasks(TEST_TASK_KIND);
        let fallback_before = crate::stats::lock_resolver_action_count(TEST_FALLBACK_ACTION);
        let latches = (0..6)
            .map(|_| Arc::new(tokio::sync::Notify::new()))
            .collect::<Vec<_>>();

        for latch in latches.iter().take(3) {
            let latch = Arc::clone(latch);
            assert!(schedule_read_cleanup_task_with_semaphore(
                Arc::clone(&semaphore),
                async move { latch.notified().await },
                TEST_TASK_KIND,
                TEST_FALLBACK_ACTION,
            )
            .await
            .is_some());
        }
        assert_eq!(
            crate::stats::lock_resolver_async_running_tasks(TEST_TASK_KIND),
            running_before + 3.0
        );

        latches[1].notify_one();
        for _ in 0..10 {
            if crate::stats::lock_resolver_async_running_tasks(TEST_TASK_KIND)
                == running_before + 2.0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            crate::stats::lock_resolver_async_running_tasks(TEST_TASK_KIND),
            running_before + 2.0
        );

        for latch in latches.iter().skip(3).take(3) {
            let latch = Arc::clone(latch);
            assert!(schedule_read_cleanup_task_with_semaphore(
                Arc::clone(&semaphore),
                async move { latch.notified().await },
                TEST_TASK_KIND,
                TEST_FALLBACK_ACTION,
            )
            .await
            .is_some());
        }
        assert_eq!(
            crate::stats::lock_resolver_async_running_tasks(TEST_TASK_KIND),
            running_before + 5.0
        );

        for _ in 0..3 {
            assert!(schedule_read_cleanup_task_with_semaphore(
                Arc::clone(&semaphore),
                async {},
                TEST_TASK_KIND,
                TEST_FALLBACK_ACTION,
            )
            .await
            .is_none());
        }
        assert_eq!(
            crate::stats::lock_resolver_action_count(TEST_FALLBACK_ACTION),
            fallback_before + 3
        );

        latches[3].notify_one();
        for _ in 0..10 {
            if crate::stats::lock_resolver_async_running_tasks(TEST_TASK_KIND)
                == running_before + 4.0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(schedule_read_cleanup_task_with_semaphore(
            Arc::clone(&semaphore),
            async {},
            TEST_TASK_KIND,
            TEST_FALLBACK_ACTION,
        )
        .await
        .is_some());

        for latch in latches {
            latch.notify_one();
        }
        for _ in 0..10 {
            if crate::stats::lock_resolver_async_running_tasks(TEST_TASK_KIND) <= running_before {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            crate::stats::lock_resolver_async_running_tasks(TEST_TASK_KIND),
            running_before
        );
    }

    #[test]
    fn source_read_resolution_classifies_ignore_and_read_through_transaction_ids() {
        let read_locks = ReadLockContext::default();
        record_read_lock_status(&read_locks, 1, 7, 8, kvrpcpb::Action::NoAction);
        record_read_lock_status(&read_locks, 2, 9, 8, kvrpcpb::Action::NoAction);
        record_read_lock_status(&read_locks, 3, 1, 8, kvrpcpb::Action::MinCommitTsPushed);
        let (mut resolved, committed) = read_locks.snapshot();
        resolved.sort_unstable();
        assert_eq!(resolved, [2, 3]);
        assert_eq!(committed, [1]);
    }

    #[tokio::test]
    async fn source_live_lock_result_uses_the_minimum_remaining_ttl() {
        let lock = kvrpcpb::LockInfo {
            key: b"locked".to_vec(),
            primary_lock: b"primary".to_vec(),
            lock_version: Timestamp {
                physical: 10,
                logical: 0,
                ..Default::default()
            }
            .version(),
            lock_ttl: 300,
            ..Default::default()
        };
        let status_lock = lock.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                assert!(req.is::<kvrpcpb::CheckTxnStatusRequest>());
                Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                    lock_ttl: 300,
                    lock_info: Some(status_lock.clone()),
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));
        let current = Timestamp::default();
        let expected = lock_until_expired_ms(lock.lock_version, 300, current.clone());
        let result = resolve_locks_with_context_result(
            vec![lock],
            current,
            client,
            Keyspace::Disable,
            None,
            ResolveLocksContext::default(),
        )
        .await
        .unwrap();

        assert_eq!(result.live_locks.len(), 1);
        assert_eq!(result.ms_before_expired, expected);
    }

    #[tokio::test]
    #[serial]
    async fn test_resolve_locks_resolves_committed_even_if_ttl_not_expired() {
        let check_txn_status_count = Arc::new(AtomicUsize::new(0));
        let resolve_lock_count = Arc::new(AtomicUsize::new(0));

        let check_txn_status_count_captured = check_txn_status_count.clone();
        let resolve_lock_count_captured = resolve_lock_count.clone();
        let resource_control_calls = Arc::new(AtomicUsize::new(0));
        let resource_control: ResourceGroupControllerHandle =
            Arc::new(ResolverResourceController(resource_control_calls.clone()));
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::CheckTxnStatusRequest>() {
                    let context = req.context.as_ref().unwrap();
                    assert_eq!(context.api_version, kvrpcpb::ApiVersion::V2 as i32);
                    assert_eq!(context.keyspace_id, 7);
                    assert_eq!(context.keyspace_name, "tenant");
                    assert_eq!(context.max_execution_duration_ms, 20_000);
                    assert_eq!(
                        context
                            .resource_control_context
                            .as_ref()
                            .unwrap()
                            .resource_group_name,
                        "resolver-rg"
                    );
                    check_txn_status_count_captured.fetch_add(1, Ordering::SeqCst);
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 2,
                        action: kvrpcpb::Action::NoAction as i32,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    let context = req.context.as_ref().unwrap();
                    assert_eq!(context.api_version, kvrpcpb::ApiVersion::V2 as i32);
                    assert_eq!(context.keyspace_id, 7);
                    assert_eq!(context.keyspace_name, "tenant");
                    assert_eq!(context.max_execution_duration_ms, 20_000);
                    assert_eq!(
                        context
                            .resource_control_context
                            .as_ref()
                            .unwrap()
                            .resource_group_name,
                        "resolver-rg"
                    );
                    resolve_lock_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![1];
        lock.lock_version = 1;
        lock.lock_ttl = 100; // not expired under MockPdClient's Timestamp::default()
        lock.txn_size = get_global_config().tikv_client.resolve_lock_lite_threshold;

        let ru_details = Arc::new(crate::RuDetails::new());
        let live_locks = resolve_locks_with_ru_details(
            vec![lock],
            Timestamp::default(),
            client,
            Keyspace::try_enable(7).unwrap(),
            Some("tenant"),
            None,
            Some("resolver-rg"),
            Some(resource_control),
            Some(ru_details.clone()),
        )
        .await
        .unwrap();

        assert!(live_locks.is_empty());
        assert_eq!(check_txn_status_count.load(Ordering::SeqCst), 1);
        assert_eq!(resolve_lock_count.load(Ordering::SeqCst), 1);
        assert_eq!(resource_control_calls.load(Ordering::SeqCst), 4);
        assert_eq!(ru_details.read_ru(), 14.0);
        assert_eq!(ru_details.write_ru(), 20.0);
        assert_eq!(ru_details.ru_wait_duration(), Duration::from_millis(10));
    }

    #[tokio::test]
    async fn source_expired_async_commit_lock_checks_secondaries_and_resolves_each_region_once() {
        let check_txn_status_count = Arc::new(AtomicUsize::new(0));
        let check_secondary_count = Arc::new(AtomicUsize::new(0));
        let resolve_lock_count = Arc::new(AtomicUsize::new(0));
        let start_ts = Timestamp {
            physical: 1,
            logical: 0,
            ..Default::default()
        }
        .version();

        let check_txn_status_count_captured = check_txn_status_count.clone();
        let check_secondary_count_captured = check_secondary_count.clone();
        let resolve_lock_count_captured = resolve_lock_count.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req
                    .downcast_ref::<kvrpcpb::CheckTxnStatusRequest>()
                    .is_some()
                {
                    check_txn_status_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                        lock_ttl: 1,
                        lock_info: Some(kvrpcpb::LockInfo {
                            lock_version: start_ts,
                            primary_lock: vec![1],
                            secondaries: vec![vec![2]],
                            min_commit_ts: start_ts + 1,
                            use_async_commit: true,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if req
                    .downcast_ref::<kvrpcpb::CheckSecondaryLocksRequest>()
                    .is_some()
                {
                    check_secondary_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::new(kvrpcpb::CheckSecondaryLocksResponse {
                        locks: vec![kvrpcpb::LockInfo {
                            key: vec![2],
                            lock_version: start_ts,
                            min_commit_ts: start_ts + 1,
                            use_async_commit: true,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if req.downcast_ref::<kvrpcpb::ResolveLockRequest>().is_some() {
                    resolve_lock_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));
        client.set_timestamp(Timestamp {
            physical: 100,
            logical: 0,
            ..Default::default()
        });

        let lock = kvrpcpb::LockInfo {
            key: vec![1],
            primary_lock: vec![1],
            lock_version: start_ts,
            lock_ttl: 1,
            ..Default::default()
        };
        let live_locks = resolve_locks_with_context(
            vec![lock],
            Timestamp::default(),
            client,
            Keyspace::Disable,
            None,
            ResolveLocksContext::default(),
        )
        .await
        .unwrap();

        assert!(live_locks.is_empty());
        assert_eq!(check_txn_status_count.load(Ordering::SeqCst), 1);
        assert_eq!(check_secondary_count.load(Ordering::SeqCst), 1);
        assert_eq!(resolve_lock_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn source_read_async_commit_cleanup_resolves_recovered_secondaries_and_primary() {
        let cleanup_sent = Arc::new(tokio::sync::Notify::new());
        let cleanup_sent_by_hook = Arc::clone(&cleanup_sent);
        let start_ts = Timestamp {
            physical: 1,
            logical: 0,
            ..Default::default()
        }
        .version();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req
                    .downcast_ref::<kvrpcpb::CheckTxnStatusRequest>()
                    .is_some()
                {
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                        lock_ttl: 1,
                        lock_info: Some(kvrpcpb::LockInfo {
                            lock_version: start_ts,
                            primary_lock: vec![1],
                            secondaries: vec![vec![2]],
                            min_commit_ts: start_ts + 1,
                            use_async_commit: true,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if req
                    .downcast_ref::<kvrpcpb::CheckSecondaryLocksRequest>()
                    .is_some()
                {
                    return Ok(Box::new(kvrpcpb::CheckSecondaryLocksResponse {
                        locks: vec![kvrpcpb::LockInfo {
                            key: vec![2],
                            lock_version: start_ts,
                            min_commit_ts: start_ts + 1,
                            use_async_commit: true,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    assert_eq!(req.keys, [vec![2], vec![1]]);
                    assert!(!req.is_async);
                    cleanup_sent_by_hook.notify_one();
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));
        client.set_timestamp(Timestamp {
            physical: 100,
            logical: 0,
            ..Default::default()
        });
        let read_locks = ReadLockContext::default();
        let lock = kvrpcpb::LockInfo {
            key: vec![1],
            primary_lock: vec![1],
            lock_version: start_ts,
            lock_ttl: 1,
            ..Default::default()
        };

        assert!(resolve_locks_for_read_with_context(
            vec![lock],
            Timestamp::from_version(start_ts + 2),
            client,
            Keyspace::Disable,
            None,
            ResolveLocksContext::default(),
            &read_locks,
        )
        .await
        .unwrap()
        .is_empty());
        assert_eq!(read_locks.snapshot().1, [start_ts]);
        tokio::time::timeout(Duration::from_secs(1), cleanup_sent.notified())
            .await
            .expect("detached async-commit cleanup should resolve the whole transaction");
    }

    #[tokio::test]
    async fn source_shared_pessimistic_lock_uses_pessimistic_rollback_not_resolve_lock() {
        let rollback_count = Arc::new(AtomicUsize::new(0));
        let rollback_count_captured = rollback_count.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req
                    .downcast_ref::<kvrpcpb::CheckTxnStatusRequest>()
                    .is_some()
                {
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 2,
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticRollbackRequest>() {
                    rollback_count_captured.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(req.keys, vec![vec![2]]);
                    assert_eq!(req.start_version, 1);
                    assert_eq!(req.for_update_ts, u64::MAX);
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                assert!(req.downcast_ref::<kvrpcpb::ResolveLockRequest>().is_none());
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));
        let lock = kvrpcpb::LockInfo {
            key: vec![2],
            primary_lock: vec![1],
            lock_version: 1,
            lock_type: kvrpcpb::Op::SharedPessimisticLock as i32,
            ..Default::default()
        };

        assert!(resolve_locks_with_context(
            vec![lock],
            Timestamp::default(),
            client,
            Keyspace::Disable,
            None,
            ResolveLocksContext::default(),
        )
        .await
        .unwrap()
        .is_empty());
        assert_eq!(rollback_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn source_pessimistic_primary_mismatch_rolls_back_the_secondary() {
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req
                    .downcast_ref::<kvrpcpb::CheckTxnStatusRequest>()
                    .is_some()
                {
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                        error: Some(kvrpcpb::KeyError {
                            primary_mismatch: Some(kvrpcpb::PrimaryMismatch {
                                lock_info: Some(kvrpcpb::LockInfo::default()),
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if req
                    .downcast_ref::<kvrpcpb::PessimisticRollbackRequest>()
                    .is_some()
                {
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));
        let lock = kvrpcpb::LockInfo {
            key: vec![2],
            primary_lock: vec![1],
            lock_version: 1,
            lock_type: kvrpcpb::Op::PessimisticLock as i32,
            ..Default::default()
        };

        assert!(resolve_locks_with_context(
            vec![lock],
            Timestamp::default(),
            client,
            Keyspace::Disable,
            None,
            ResolveLocksContext::default(),
        )
        .await
        .unwrap()
        .is_empty());
    }

    #[test]
    fn format_key_for_log_hex_encodes_the_prefix() {
        assert_eq!(format_key_for_log(b"hello"), "len=5, prefix=68656C6C6F");
    }

    #[test]
    fn format_key_for_log_truncates_the_prefix_to_16_bytes() {
        let key: Vec<u8> = (0u8..20).collect();
        assert_eq!(
            format_key_for_log(&key),
            "len=20, prefix=000102030405060708090A0B0C0D0E0F"
        );
    }
}
