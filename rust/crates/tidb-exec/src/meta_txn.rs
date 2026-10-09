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

//! A live TiKV transaction as Go's `kv.Transaction`, for the two package loops
//! that take one: `meta.NewMutator(txn)` and `kv.RunInNewTxn`.
//!
//! Both loops were ported long ago -- `tidb_meta::transaction::Mutator`
//! carries Go's whole `meta.Mutator` surface and `tidb_txnkv::run_in_new_txn`
//! is Go's retry loop -- but neither had a production transaction under it.
//! `Mutator` ran only over `MemoryTransaction`, and `run_in_new_txn` only over
//! test storages, so every Go path written as
//!
//! ```text
//! kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
//!     return meta.NewMutator(txn).SetRUStats(stats)
//! })
//! ```
//!
//! had no way to reach a real cluster. [`MetaTxn`] is that missing
//! transaction, and [`MetaTxnStorage`] is the `kv.Storage.Begin` that hands one
//! out per attempt.
//!
//! Go's `kv.Transaction` buffers writes in its memory buffer and serves reads
//! from that buffer before the snapshot. The live transaction here commits a
//! mutation list instead, so [`MetaTxn`] keeps the buffer itself: a write lands
//! in it, a read consults it first, a scan merges it over the snapshot, and the
//! commit turns it into mutations. That is Go's read-your-writes contract for
//! the single transaction a `RunInNewTxn` attempt owns.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use tidb_meta::transaction::{RawRangeVisitor, RawTransaction};
use tidb_meta::MetaError;
use tidb_txnkv::rpc::UnaryCallContext;
use tidb_txnkv::transaction::{
    BufferMutation, OptimisticCommitOutcome, RealOptimisticTransaction,
    RealOptimisticTransactionOpener, StorePdCapability, StoreWriteClient, StoreWriteLoader,
};
use tidb_txnkv::{NewTxnError, NewTxnStorage, NewTxnTransaction, OptionKey, TxnOptionValue};

use crate::cluster_catalog::prefix_scan_end;

type LiveTransaction<C, L, P> =
    RealOptimisticTransaction<C, L, tidb_txnkv::pd_capability::CapabilityTimestampSource<P>>;

/// Why an attempt failed, carrying Go's retry classification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetaTxnError {
    message: String,
    retryable: bool,
}

impl MetaTxnError {
    fn terminal(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: false,
        }
    }

    /// Go `kv.IsTxnRetryableError`: true only for `ErrTxnRetryable`,
    /// `ErrWriteConflict` and `ErrWriteConflictInTiDB`
    /// (`pkg/kv/error.go:82-92`). A commit rolled back by a write conflict is
    /// the one this transaction can produce.
    fn write_conflict(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: true,
        }
    }
}

impl fmt::Display for MetaTxnError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for MetaTxnError {}

impl NewTxnError for MetaTxnError {
    fn is_retryable(&self) -> bool {
        self.retryable
    }
}

impl From<MetaError> for MetaTxnError {
    fn from(error: MetaError) -> Self {
        Self::terminal(error.to_string())
    }
}

/// One live TiKV transaction with Go's memory-buffer semantics.
pub struct MetaTxn<C, L, P: StorePdCapability> {
    /// Taken by commit or rollback, which consume the live transaction.
    transaction: Option<LiveTransaction<C, L, P>>,
    /// Go's memory buffer: `Some` is a set, `None` a delete.
    writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    start_ts: u64,
    timeout: Duration,
    /// Whether `meta.NewMutator` configured this transaction. Go applies
    /// `PriorityHigh` and `DiskFullOpt_AllowedOnAlmostFull`; the live
    /// transaction exposes neither knob, so the request is recorded where a
    /// later transport option can read it rather than silently dropped.
    configured_for_meta: bool,
}

impl<C, L, P> MetaTxn<C, L, P>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    fn new(transaction: LiveTransaction<C, L, P>, timeout: Duration) -> Self {
        let start_ts = transaction.start_ts();
        Self {
            transaction: Some(transaction),
            writes: BTreeMap::new(),
            start_ts,
            timeout,
            configured_for_meta: false,
        }
    }

    /// Whether `meta.NewMutator` has configured this transaction.
    #[must_use]
    pub fn configured_for_meta(&self) -> bool {
        self.configured_for_meta
    }

    fn live(&mut self) -> Result<&mut LiveTransaction<C, L, P>, MetaError> {
        self.transaction
            .as_mut()
            .ok_or_else(|| MetaError::Storage("the transaction has already finished".to_owned()))
    }

    fn call(&self) -> UnaryCallContext {
        UnaryCallContext::with_timeout(self.timeout)
    }

    /// The snapshot over `[start, end)` with the buffer applied on top, in key
    /// order: Go's union iterator over the memory buffer and the snapshot.
    fn merged_range(
        &mut self,
        start: &[u8],
        end: &[u8],
    ) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, MetaError> {
        let call = self.call();
        let snapshot = self
            .live()?
            .snapshot_scan(start, end, None, &call)
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        let mut merged: BTreeMap<Vec<u8>, Vec<u8>> = snapshot.into_iter().collect();
        for (key, write) in self.writes.range(start.to_vec()..end.to_vec()) {
            match write {
                Some(value) => {
                    merged.insert(key.clone(), value.clone());
                }
                None => {
                    merged.remove(key);
                }
            }
        }
        Ok(merged)
    }
}

impl<C, L, P> RawTransaction for MetaTxn<C, L, P>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    fn start_ts(&self) -> u64 {
        self.start_ts
    }

    fn configure_meta_mutator(&mut self) {
        self.configured_for_meta = true;
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, MetaError> {
        if let Some(write) = self.writes.get(key) {
            return Ok(write.clone());
        }
        let call = self.call();
        self.live()?
            .snapshot_get(key, &call)
            .map(|result| result.value)
            .map_err(|error| MetaError::Storage(error.to_string()))
    }

    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), MetaError> {
        self.live()?;
        self.writes.insert(key, Some(value));
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), MetaError> {
        self.live()?;
        self.writes.insert(key.to_vec(), None);
        Ok(())
    }

    fn scan_prefix(&mut self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, MetaError> {
        let Some(end) = prefix_scan_end(prefix) else {
            return Err(MetaError::Storage(
                "meta prefix has no finite scan end".to_owned(),
            ));
        };
        Ok(self.merged_range(prefix, &end)?.into_iter().collect())
    }

    fn iterate_range(
        &mut self,
        start: &[u8],
        end: &[u8],
        visit: &mut RawRangeVisitor<'_>,
    ) -> Result<(), MetaError> {
        for (key, value) in self.merged_range(start, end)? {
            visit(&key, &value)?;
        }
        Ok(())
    }
}

impl<C, L, P> tidb_txnkv::TxnResourceGroup for MetaTxn<C, L, P>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    fn set_resource_group_name(&mut self, name: &str) {
        if let Some(transaction) = self.transaction.as_mut() {
            transaction.set_resource_group_name(name);
        }
    }
}

impl<C, L, P> NewTxnTransaction for MetaTxn<C, L, P>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    type Error = MetaTxnError;

    fn start_ts(&self) -> u64 {
        self.start_ts
    }

    /// Go `setRequestSourceForInnerTxn` sets the request source on the inner
    /// transaction. The live transaction derives its own internal source, so
    /// the options are accepted without a separate effect here.
    fn set_option(&mut self, _option: OptionKey, _value: TxnOptionValue) {}

    fn rollback(&mut self) -> Result<(), MetaTxnError> {
        self.writes.clear();
        match self.transaction.take() {
            Some(transaction) => transaction
                .finish_without_writes()
                .map(|_| ())
                .map_err(|error| MetaTxnError::terminal(error.to_string())),
            None => Ok(()),
        }
    }

    fn commit(&mut self) -> Result<(), MetaTxnError> {
        let transaction = self
            .transaction
            .take()
            .ok_or_else(|| MetaTxnError::terminal("the transaction has already finished"))?;
        if self.writes.is_empty() {
            // Go commits an empty memory buffer without a 2PC round trip.
            return transaction
                .finish_without_writes()
                .map(|_| ())
                .map_err(|error| MetaTxnError::terminal(error.to_string()));
        }
        let mut mutations = Vec::with_capacity(self.writes.len());
        for (key, write) in std::mem::take(&mut self.writes) {
            let mutation = match write {
                Some(value) => BufferMutation::set(key, value),
                None => BufferMutation::delete(key),
            }
            .map_err(|error| MetaTxnError::terminal(error.to_string()))?;
            mutations.push(mutation);
        }
        let call = self.call();
        match transaction
            .commit(mutations, &call)
            .map_err(|error| MetaTxnError::terminal(error.to_string()))?
        {
            OptimisticCommitOutcome::Committed(_) => Ok(()),
            OptimisticCommitOutcome::RolledBack(rolled_back)
                if matches!(
                    rolled_back.cause,
                    tidb_txnkv::transaction::TransactionCause::WriteConflict { .. }
                ) =>
            {
                Err(MetaTxnError::write_conflict(format!(
                    "{:?}",
                    rolled_back.cause
                )))
            }
            other => Err(MetaTxnError::terminal(format!(
                "meta transaction did not commit: {:?}",
                other.state()
            ))),
        }
    }
}

/// Go `kv.Storage.Begin` for [`tidb_txnkv::run_in_new_txn`]: one fresh live
/// transaction per attempt.
pub struct MetaTxnStorage<'opener, C, L, P> {
    opener: &'opener RealOptimisticTransactionOpener<C, L, P>,
    timeout: Duration,
}

impl<'opener, C, L, P> MetaTxnStorage<'opener, C, L, P> {
    /// Binds the opener and the per-request deadline each attempt stamps.
    pub fn new(opener: &'opener RealOptimisticTransactionOpener<C, L, P>, timeout: Duration) -> Self {
        Self { opener, timeout }
    }
}

impl<C, L, P> NewTxnStorage for MetaTxnStorage<'_, C, L, P>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    type Transaction = MetaTxn<C, L, P>;
    type Error = MetaTxnError;

    fn begin(&mut self) -> Result<MetaTxn<C, L, P>, MetaTxnError> {
        self.opener
            .begin()
            .map(|transaction| MetaTxn::new(transaction, self.timeout))
            .map_err(|error| MetaTxnError::terminal(error.to_string()))
    }
}
