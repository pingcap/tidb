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

//! The lock-resolution results TiDB's reads keep: client-rust's resolver
//! (the store's `GetLockResolver()`) does the resolving.

use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tidb_proto::KvrpcContext;

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

impl<T: TimestampSource + ?Sized> TimestampSource for Arc<T> {
    fn current_ts(&self) -> Result<u64, String> {
        (**self).current_ts()
    }
}
