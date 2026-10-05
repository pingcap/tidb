// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! TiDB transport and result adapters for client-rust's shared resolver.

use super::{BlockingLock, LockAdmissionError, OptimisticLock};
use crate::region::{
    RegionBackoffBudget, RegionBackoffKind, RegionQueryLoader, RegionRecoveryLoader,
};
use crate::rpc::TonicCoprocessorClient;
use crate::{DirectUnaryClientError, SharedReadRuntime, UnaryCallContext};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tidb_proto::{
    KvrpcCheckSecondaryLocksRequest, KvrpcCheckSecondaryLocksResponse, KvrpcCheckTxnStatusRequest,
    KvrpcCheckTxnStatusResponse, KvrpcContext, KvrpcPessimisticRollbackRequest,
    KvrpcPessimisticRollbackResponse, KvrpcResolveLockRequest, KvrpcResolveLockResponse,
};

/// Exact timestamp authority injected by the caller.
pub trait TimestampSource: fmt::Debug {
    /// Returns a fresh real TSO value on every call.
    ///
    /// Wall-clock synthesis and replaying a previously returned TSO are not
    /// admitted. Callers may require a second value after a slow status RPC.
    fn current_ts(&self) -> Result<u64, String>;
}

/// One-shot injected TSO for paths that can prove they need only one value.
#[derive(Debug)]
pub struct FixedTimestampSource {
    timestamp: Mutex<Option<u64>>,
}

impl FixedTimestampSource {
    /// Creates a source that returns `timestamp` exactly once.
    #[must_use]
    pub const fn new(timestamp: u64) -> Self {
        Self {
            timestamp: Mutex::new(Some(timestamp)),
        }
    }
}

impl TimestampSource for FixedTimestampSource {
    fn current_ts(&self) -> Result<u64, String> {
        let timestamp = self
            .timestamp
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take()
            .ok_or_else(|| "one-shot timestamp source is exhausted".to_owned())?;
        if timestamp == 0 {
            return Err("current timestamp must be a real nonzero TSO".to_owned());
        }
        Ok(timestamp)
    }
}

/// Typed commands required from the sole shared TiKV client.
pub trait LockRecoveryClient {
    /// Foreground health observation through the same client/channel owner.
    /// An unavailable probe is unknown, never proof of a healthy store.
    fn store_liveness_for_route(&mut self, _address: &str) -> crate::region::StoreLiveness {
        crate::region::StoreLiveness::Unknown
    }

    /// Clones this client capability for one concurrent resolver worker.
    /// Test clients that are intentionally single-threaded keep the default
    /// `None` and execute through the caller's inline fallback.
    fn fork_for_async_worker(&self) -> Option<Box<dyn LockRecoveryClient + Send>> {
        None
    }

    /// Sends CheckTxnStatus through the client's existing unary core.
    fn check_txn_status_for_lock(
        &mut self,
        address: &str,
        request: &KvrpcCheckTxnStatusRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> Result<KvrpcCheckTxnStatusResponse, DirectUnaryClientError>;

    /// Sends CheckSecondaryLocks through the client's existing unary core.
    fn check_secondary_locks_for_lock(
        &mut self,
        address: &str,
        request: &KvrpcCheckSecondaryLocksRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> Result<KvrpcCheckSecondaryLocksResponse, DirectUnaryClientError>;

    /// Sends keyed ResolveLock through the client's existing unary core.
    fn resolve_lock_for_read(
        &mut self,
        address: &str,
        request: &KvrpcResolveLockRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> Result<KvrpcResolveLockResponse, DirectUnaryClientError>;

    /// Sends keyed PessimisticRollback cleaning one expired pessimistic lock.
    ///
    /// ResolveLock cannot clean a pessimistic lock: there is no commit record
    /// to redo or undo, only a lock entry that must be dropped at its exact
    /// `for_update_ts`.
    fn pessimistic_rollback_for_lock(
        &mut self,
        address: &str,
        request: &KvrpcPessimisticRollbackRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> Result<KvrpcPessimisticRollbackResponse, DirectUnaryClientError>;
}

impl LockRecoveryClient for TonicCoprocessorClient {
    fn store_liveness_for_route(&mut self, address: &str) -> crate::region::StoreLiveness {
        self.liveness_default(address)
            .unwrap_or(crate::region::StoreLiveness::Unknown)
    }

    fn fork_for_async_worker(&self) -> Option<Box<dyn LockRecoveryClient + Send>> {
        Some(Box::new(self.clone()))
    }

    fn check_txn_status_for_lock(
        &mut self,
        address: &str,
        request: &KvrpcCheckTxnStatusRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> Result<KvrpcCheckTxnStatusResponse, DirectUnaryClientError> {
        self.check_txn_status(address, request, context, call)
    }

    fn check_secondary_locks_for_lock(
        &mut self,
        address: &str,
        request: &KvrpcCheckSecondaryLocksRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> Result<KvrpcCheckSecondaryLocksResponse, DirectUnaryClientError> {
        self.check_secondary_locks(address, request, context, call)
    }

    fn resolve_lock_for_read(
        &mut self,
        address: &str,
        request: &KvrpcResolveLockRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> Result<KvrpcResolveLockResponse, DirectUnaryClientError> {
        self.resolve_lock(address, request, context, call)
    }

    fn pessimistic_rollback_for_lock(
        &mut self,
        address: &str,
        request: &KvrpcPessimisticRollbackRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> Result<KvrpcPessimisticRollbackResponse, DirectUnaryClientError> {
        let decoded = self
            .begin_transaction_pessimistic_rollback(
                address,
                call.forwarded_host(),
                request,
                context,
                call,
            )?
            .complete(call)
            .map_err(|error| DirectUnaryClientError::InvalidRequest(error.to_string()))??;
        Ok(decoded.response)
    }
}

/// Bounded outcome returned to DistSQL's same-task retry owner.
///
/// Go `txnlock.ResolveLockResult` (`lock_resolver.go:405-411`). One batch of
/// locks can end in more than one way at once — one owner still running while
/// another's min-commit-ts was pushed past the reader — so the three answers
/// are fields, not alternatives. Collapsing them into an enum would silently
/// drop the pushed transaction's id and make the reader meet the same lock
/// forever.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LockRecoveryResult {
    /// Shortest wait any still-running owner asked for; zero means none did.
    ///
    /// Go `ResolveLockResult.TTL`, produced by `txnExpireTime`, whose
    /// uninitialised value is likewise `0`.
    pub ttl: Duration,
    /// Transactions whose locks the reader may step over.
    ///
    /// Go `ResolveLockResult.IgnoreLocks` -> `Context.resolved_locks`.
    pub ignore_locks: Vec<u64>,
    /// Transactions committed at or before the reader, whose value the reader
    /// must see through the lock.
    ///
    /// Go `ResolveLockResult.AccessLocks` -> `Context.committed_locks`.
    pub access_locks: Vec<u64>,
}

impl LockRecoveryResult {
    /// Whether at least one owner is still running and asked to be waited for.
    ///
    /// Go tests `msBeforeTxnExpired > 0` at every caller.
    #[must_use]
    pub const fn is_alive(&self) -> bool {
        !self.ttl.is_zero()
    }
}

/// The two per-reader timestamp sets a read replays into its request context.
///
/// Go `KVSnapshot.resolvedLocks` / `KVSnapshot.committedLocks`
/// (`snapshot.go:124-125`), filled from [`LockRecoveryResult`] by
/// `ClientHelper` (`client_helper.go:97-122`) and stamped onto
/// `Context.resolved_locks` / `Context.committed_locks` before every send
/// (`client_helper.go:148-149`). Go guards them with a mutex because a
/// snapshot is shared across goroutines; a reader here is owned by one caller,
/// so the ordered set alone carries the same meaning.
///
/// This set is what makes a lock the reader has already classified stop
/// blocking it. Without it the retry meets the same lock again — the deadloop
/// `client_helper.go`'s own comment warns about.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SnapshotLockSet {
    /// Go `resolvedLocks` -> `Context.resolved_locks`.
    ignore: std::collections::BTreeSet<u64>,
    /// Go `committedLocks` -> `Context.committed_locks`.
    access: std::collections::BTreeSet<u64>,
    /// The read timestamp the classifications were made against. Go scopes
    /// `resolvedLocks`/`committedLocks` to ONE `KVSnapshot`, and a snapshot
    /// has one version -- a pessimistic retry reads through a FRESH snapshot
    /// at `for_update_ts`, so no classification survives a version change.
    /// `None` is a set nothing has classified yet.
    classified_at: Option<u64>,
}

impl SnapshotLockSet {
    /// Go's per-`KVSnapshot` scoping: entering a read at a DIFFERENT
    /// timestamp discards every earlier classification, because ignore/access
    /// verdicts are relative to the version they were decided at. A lock
    /// bypassed as "committed after my snapshot" at `start_ts` may be exactly
    /// the committed value a `for_update_ts` retry exists to observe; a stale
    /// stamp would make TiKV skip that lock forever, a sticky stale read.
    pub fn rescope(&mut self, read_ts: u64) {
        if self.classified_at != Some(read_ts) {
            // Go `KVSnapshot.SetSnapshotTS`
            // (`txnkv/txnsnapshot/snapshot.go:189-202`) clears exactly ONE of
            // the two sets -- `s.resolvedLocks = util.TSSet{}`, "remove the
            // minCommitTS pushed information" -- and deliberately leaves
            // `committedLocks` standing. That asymmetry is right: `ignore`
            // carries this reader's pushed-min-commit-ts decisions, which are
            // relative to the version they were made at, while `access`
            // records that a transaction COMMITTED at or before the reader --
            // and that fact stays true at any LATER version.
            //
            // It does not stay true at an EARLIER one, and this reader does
            // move backwards: `for_update_ts` is statement-local, so the
            // statement after a retried one reads at the transaction's
            // `start_ts` again. Go never has to answer this because it does
            // not reuse the object across the drop -- once
            // `forUpdateTS != startTS`, `base.go:459-467` hands every
            // statement a brand-new `KVSnapshot` whose `resolvedLocks` and
            // `committedLocks` are both empty, and `SetSnapshotTS`
            // (`snapshot.go:187-201`) only ever moves a reused one FORWARD.
            // Carrying `access` down would tell TiKV to read THROUGH a lock
            // whose commit is in the future of the new read -- surfacing a
            // later commit to an earlier reader.
            if self
                .classified_at
                .is_some_and(|previous| read_ts < previous)
            {
                self.access.clear();
            }
            self.ignore.clear();
            self.classified_at = Some(read_ts);
        }
    }

    /// Go `ClientHelper.ResolveLocks`: whatever the resolver classified is put
    /// into the reader's sets and stays there for the SNAPSHOT's life (see
    /// [`Self::rescope`] for the version boundary).
    pub fn absorb(&mut self, result: &LockRecoveryResult) {
        self.ignore.extend(result.ignore_locks.iter().copied());
        self.access.extend(result.access_locks.iter().copied());
    }

    /// Go resolvedLocks.Put: skip a transaction on later requests of this
    /// snapshot, including KVSnapshot.get's MaxTS first-lock shortcut.
    pub fn ignore_lock(&mut self, txn_id: u64) {
        self.ignore.insert(txn_id);
    }

    /// Go `ClientHelper.SendReqCtx`: both sets are stamped onto the context of
    /// every request this reader sends after the first resolve.
    pub fn stamp(&self, context: &mut KvrpcContext) {
        context.resolved_locks = self.ignore.iter().copied().collect();
        context.committed_locks = self.access.iter().copied().collect();
    }

    /// Whether anything has been classified yet.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ignore.is_empty() && self.access.is_empty()
    }
}

/// Fail-closed recovery errors.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LockRecoveryError {
    /// LockInfo does not belong to the bounded optimistic protocol.
    Admission(LockAdmissionError),
    /// Injected timestamp authority failed.
    Timestamp(String),
    /// Native topology recovery returned a terminal region error.
    RegionError(String),
    /// Native recovery returned a terminal key error.
    KeyError(String),
    /// The same client/core failed the typed unary command.
    Rpc(String),
    /// The canonical read cancellation won before further lock recovery.
    CallerCancelled,
    /// The borrowed retry budget was exhausted; preserve its longest-sleep
    /// category so the caller can retain Go's registered error identity.
    BackoffExhausted(crate::region::RegionBackoffExhausted),
    /// The caller's deadline expired during lock recovery.
    StatusRetryDeadlineExceeded,
}

impl fmt::Display for LockRecoveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission(error) => write!(formatter, "lock admission failed: {error}"),
            Self::Timestamp(error) => write!(formatter, "timestamp source failed: {error}"),
            Self::RegionError(error) => {
                write!(formatter, "lock RPC returned region error: {error}")
            }
            Self::KeyError(error) => write!(formatter, "lock RPC returned key error: {error}"),
            Self::Rpc(error) => write!(formatter, "lock RPC failed: {error}"),
            Self::CallerCancelled => formatter.write_str("lock recovery cancelled by caller"),
            Self::BackoffExhausted(error) => {
                write!(formatter, "lock backoff budget ran out: {error:?}")
            }
            Self::StatusRetryDeadlineExceeded => {
                formatter.write_str("lock recovery caller deadline exceeded")
            }
        }
    }
}

impl std::error::Error for LockRecoveryError {}

impl From<LockAdmissionError> for LockRecoveryError {
    fn from(error: LockAdmissionError) -> Self {
        Self::Admission(error)
    }
}

impl<T: TimestampSource + ?Sized> TimestampSource for Arc<T> {
    fn current_ts(&self) -> Result<u64, String> {
        (**self).current_ts()
    }
}

/// Resolves one pass through the process-owned native resolver.
pub fn resolve_optimistic_locks<C, L, T>(
    runtime: &SharedReadRuntime<C, L>,
    locks: &[OptimisticLock],
    caller_start_ts: u64,
    base_context: &KvrpcContext,
    call: &UnaryCallContext,
    timestamp_source: &Arc<T>,
    for_read: bool,
) -> Result<LockRecoveryResult, LockRecoveryError>
where
    C: LockRecoveryClient + Send + 'static,
    L: RegionRecoveryLoader + RegionQueryLoader + Send + 'static,
    T: TimestampSource + Send + Sync + 'static + ?Sized,
{
    let locks = locks
        .iter()
        .cloned()
        .map(BlockingLock::Optimistic)
        .collect::<Vec<_>>();
    resolve_blocking_locks(
        runtime,
        &locks,
        caller_start_ts,
        base_context,
        call,
        timestamp_source,
        for_read,
    )
}

/// Compatibility entry point for callers without an existing retry budget.
pub fn resolve_blocking_locks<C, L, T>(
    runtime: &SharedReadRuntime<C, L>,
    locks: &[BlockingLock],
    caller_start_ts: u64,
    base_context: &KvrpcContext,
    call: &UnaryCallContext,
    timestamp_source: &Arc<T>,
    for_read: bool,
) -> Result<LockRecoveryResult, LockRecoveryError>
where
    C: LockRecoveryClient + Send + 'static,
    L: RegionRecoveryLoader + RegionQueryLoader + Send + 'static,
    T: TimestampSource + Send + Sync + 'static + ?Sized,
{
    resolve_blocking_locks_with_backoff(
        runtime,
        locks,
        caller_start_ts,
        base_context,
        call,
        timestamp_source,
        for_read,
        &mut RegionBackoffBudget::new(Duration::from_secs(20)),
    )
}

/// TiDB's cop task lends its existing owner to client-rust, as Go does.
pub fn resolve_blocking_locks_with_backoff<C, L, T>(
    runtime: &SharedReadRuntime<C, L>,
    locks: &[BlockingLock],
    caller_start_ts: u64,
    base_context: &KvrpcContext,
    call: &UnaryCallContext,
    timestamp_source: &Arc<T>,
    for_read: bool,
    backoff: &mut RegionBackoffBudget,
) -> Result<LockRecoveryResult, LockRecoveryError>
where
    C: LockRecoveryClient + Send + 'static,
    L: RegionRecoveryLoader + RegionQueryLoader + Send + 'static,
    T: TimestampSource + Send + Sync + 'static + ?Sized,
{
    use tikv_client::txnkv::txnlock::{
        LockHintsInRequest, LockResolver, ResolveLocksOptions, ResolvingLocksGuard,
    };
    if locks.is_empty() {
        return Ok(LockRecoveryResult::default());
    }
    if call.cancellation().is_cancelled() {
        return Err(LockRecoveryError::CallerCancelled);
    }
    if call.timeout().is_zero() {
        return Err(LockRecoveryError::StatusRetryDeadlineExceeded);
    }
    let executor = crate::driver::client_bridge::runtime();
    let pd = crate::driver::client_bridge::ClientPd::new_resolver(
        runtime.clone(),
        timestamp_source.clone(),
    );
    pd.set_call(call);
    let context = runtime.native_lock_resolver_context();
    let locks: Vec<_> = locks.iter().map(native_lock).collect();
    let _record = ResolvingLocksGuard::new(context.clone(), &locks, caller_start_ts);
    let resolver = LockResolver::new(context).with_request_context(base_context);
    let owner = backoff.native_owner();
    let cancellation = tikv_client::async_util::Cancellation::default();
    let result = run_on_resolver_runtime(&executor, async {
        owner.lock().await.set_cancellation(cancellation.clone());
        let operation = resolver.resolve_locks_with_opts(
            pd,
            tikv_client::tikv::Keyspace::Disable,
            None,
            owner.clone(),
            ResolveLocksOptions {
                caller_start_ts,
                locks,
                for_read,
                lock_hints_in_request: LockHintsInRequest::new(
                    &base_context.resolved_locks,
                    &base_context.committed_locks,
                ),
                ..Default::default()
            },
        );
        tokio::select! {
            biased;
            result = operation => result.map_err(|error| map_native_error(error, &owner.try_lock().expect("resolver returned its retry owner"))),
            _ = call.cancellation().cancelled() => {
                cancellation.cancel();
                Err(LockRecoveryError::CallerCancelled)
            }
            _ = async { match call.deadline() {
                Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
                None => std::future::pending().await,
            }} => {
                cancellation.cancel();
                Err(LockRecoveryError::StatusRetryDeadlineExceeded)
            }
        }
    })?;
    Ok(LockRecoveryResult {
        ttl: Duration::from_millis(result.ttl.max(0) as u64),
        ignore_locks: result.ignore_locks,
        access_locks: result.access_locks,
    })
}

// Coprocessor continuations are synchronous but may run on a Tokio worker.
// Hand that worker back before waiting on the shared native resolver runtime.
// A current-thread caller cannot use block_in_place, so move only its wait to
// a scoped thread; routing, retries and cancellation still have the same owner.
fn run_on_resolver_runtime<F>(executor: &tokio::runtime::Runtime, operation: F) -> F::Output
where
    F: std::future::Future + Send,
    F::Output: Send,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| executor.block_on(operation))
        }
        Ok(_) => std::thread::scope(|scope| {
            scope
                .spawn(|| executor.block_on(operation))
                .join()
                .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
        }),
        Err(_) => executor.block_on(operation),
    }
}

fn native_lock(lock: &BlockingLock) -> tikv_client::proto::kvrpcpb::LockInfo {
    use tikv_client::proto::kvrpcpb::LockInfo;
    match lock {
        BlockingLock::Optimistic(lock) => LockInfo {
            key: lock.key.clone(),
            primary_lock: lock.primary.clone(),
            lock_version: lock.txn_id,
            lock_ttl: lock.ttl_ms,
            txn_size: lock.txn_size,
            lock_type: lock.lock_type,
            min_commit_ts: lock.min_commit_ts,
            use_async_commit: lock.use_async_commit,
            secondaries: lock.secondaries.clone(),
            ..Default::default()
        },
        BlockingLock::Pessimistic(lock) => LockInfo {
            key: lock.key.clone(),
            primary_lock: lock.primary.clone(),
            lock_version: lock.txn_id,
            lock_ttl: lock.ttl_ms,
            lock_type: lock.lock_type,
            lock_for_update_ts: lock.for_update_ts,
            duration_to_last_update_ms: lock.duration_to_last_update_ms,
            ..Default::default()
        },
    }
}

fn map_native_error(
    error: tikv_client::Error,
    owner: &tikv_client::retry::RetryBackoffer,
) -> LockRecoveryError {
    use tikv_client::Error;
    match error {
        Error::ContextCanceled => LockRecoveryError::CallerCancelled,
        Error::Io(error)
            if error.get_ref().is_some_and(|cause| {
                cause.is::<crate::driver::client_bridge::ResolverBridgeError>()
            }) =>
        {
            match error
                .get_ref()
                .unwrap()
                .downcast_ref::<crate::driver::client_bridge::ResolverBridgeError>()
                .unwrap()
            {
                crate::driver::client_bridge::ResolverBridgeError::Timestamp(message) => {
                    LockRecoveryError::Timestamp(message.clone())
                }
                crate::driver::client_bridge::ResolverBridgeError::CallerCancelled => {
                    LockRecoveryError::CallerCancelled
                }
            }
        }
        Error::ExtractedErrors(mut errors) | Error::MultipleKeyErrors(mut errors)
            if errors.len() == 1 =>
        {
            map_native_error(errors.remove(0), owner)
        }
        Error::Connection { source, .. }
        | Error::UndeterminedError(source)
        | Error::PessimisticLockError { inner: source, .. } => map_native_error(*source, owner),
        Error::KeyError(error) => LockRecoveryError::KeyError(format!("{error:?}")),
        Error::RegionError(error) => LockRecoveryError::RegionError(format!("{error:?}")),
        Error::PdServerTimeout(_)
            if owner.longest_sleep_config().is_some_and(|config| {
                config.terminal_error == tikv_client::retry::RetryTerminal::PdServerTimeout
            }) =>
        {
            LockRecoveryError::BackoffExhausted(RegionBackoffBudget::exhausted(
                owner,
                RegionBackoffKind::PdRpc,
            ))
        }
        Error::Static(error)
            if owner.longest_sleep_config().is_some_and(|config| {
                config.terminal_error == tikv_client::retry::RetryTerminal::Static(error)
            }) =>
        {
            LockRecoveryError::BackoffExhausted(RegionBackoffBudget::exhausted(
                owner,
                RegionBackoffKind::TxnLockFast,
            ))
        }
        error => LockRecoveryError::Rpc(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn a_reader_moving_back_in_time_forgets_the_later_versions_commits() {
        fn committed(locks: &SnapshotLockSet) -> Vec<u64> {
            let mut context = tidb_proto::KvrpcContext::default();
            locks.stamp(&mut context);
            context.committed_locks
        }

        let mut locks = SnapshotLockSet::default();
        locks.rescope(100);
        locks.absorb(&LockRecoveryResult {
            ignore_locks: Vec::new(),
            access_locks: vec![90],
            ..LockRecoveryResult::default()
        });
        assert!(
            committed(&locks).contains(&90),
            "the commit is visible at the version that classified it"
        );

        // A pessimistic retry advances the statement; the classification made
        // there is still sound at the newer version.
        locks.rescope(200);
        assert!(
            committed(&locks).contains(&90),
            "moving FORWARD keeps it: a commit at or before 100 is also at or \
             before 200"
        );

        // The next statement reads at the transaction's own timestamp again.
        locks.rescope(100);
        assert!(
            !committed(&locks).contains(&90),
            "moving BACK must drop it, or TiKV is told to read through a lock \
             whose commit the earlier reader must not see"
        );
    }
}
