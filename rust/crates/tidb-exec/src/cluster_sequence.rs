// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The cluster tier's home for one sequence's stored counter: Go's meta fields
//! `SequenceValue` and `SequenceCycle`, read and written in a transaction of
//! the reservation's OWN.
//!
//! This is [`crate::cluster_auto_id`]'s shape applied to sequences: Go puts a
//! sequence's counter in meta and reserves from it inside `kv.RunInNewTxn`
//! (`autoid.alloc4Sequence`), so two TiDB nodes hand out disjoint batches of
//! the same ladder only because a reservation is one committed meta write.
//! The allocation rules -- which value comes next, when the ladder wraps --
//! stay in `tidb_executor::sequence`; what changes here is only WHERE the
//! number lives.
//!
//! Go reads and writes the counter through `AutoIDAccessor`s:
//!
//! * `acc.SequenceValue().Get()` / `.Inc(delta)` -- the batch end;
//! * `acc.SequenceCycle().Get()` / `.Put(round)` -- how many times `CYCLE`
//!   wrapped (the round picks the congruence offset after a wrap).
//!
//! A missing field reads as zero (`TxStructure.HGetInt64`). Go advances with
//! an atomic `Inc`; this tier computes the absolute end from its read snapshot
//! and commits it as one optimistic mutation, losing prewrite races to a
//! re-read -- which is what keeps two nodes' ranges disjoint.

use std::sync::Arc;
use std::time::Duration;

use tidb_executor::sequence::{
    calc_sequence_batch_size, sequence_offset, SequenceCounter, SequenceError, SequenceInfo,
};
use tidb_meta::structure::encode_hash_data_key;
use tidb_meta::{key, value};
use tidb_pd_client::PdClient;
use tidb_txnkv::rpc::TonicCoprocessorClient;
use tidb_txnkv::rpc::UnaryCallContext;
use tidb_txnkv::transaction::{
    OptimisticCommitOutcome, OptimisticMutation, RealOptimisticTransaction,
    RealOptimisticTransactionOpener,
};
use tidb_txnkv::pd_capability::CapabilityTimestampSource;
use tidb_txnkv::transaction::{StorePdCapability, StoreWriteClient, StoreWriteLoader};
use tidb_txnkv::PdRegionLoader;

use crate::cluster_auto_id::MAX_RESERVE_RETRIES;
use crate::cluster_catalog::MetaSnapshot;

/// One sequence's shared counter over the cluster meta keys.
pub struct ClusterSequenceCounter<
    C = TonicCoprocessorClient,
    L = PdRegionLoader,
    P = PdClient,
> where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    opener: RealOptimisticTransactionOpener<C, L, P>,
    /// Go's `SEV:<tableID>` field under the database hash key: the batch end.
    value_key: Vec<u8>,
    /// Go's `SEC:<tableID>` field under the same hash key; written only when
    /// the sequence cycles.
    cycle_key: Vec<u8>,
    timeout: Duration,
}

/// The concrete transaction type [`RealOptimisticTransactionOpener::begin`]
/// hands back.
type CounterTransaction<C, L, P> =
    RealOptimisticTransaction<C, L, CapabilityTimestampSource<P>>;

impl<C, L, P> std::fmt::Debug for ClusterSequenceCounter<C, L, P>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClusterSequenceCounter")
            .field("value_key", &self.value_key)
            .finish_non_exhaustive()
    }
}

impl<C, L, P> ClusterSequenceCounter<C, L, P>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    /// The counter for sequence `table_id` stored in database `db_id`.
    #[must_use]
    pub fn new(
        opener: RealOptimisticTransactionOpener<C, L, P>,
        db_id: i64,
        table_id: i64,
        timeout: Duration,
    ) -> Self {
        ClusterSequenceCounter {
            opener,
            value_key: encode_hash_data_key(&key::db_key(db_id), &key::sequence_key(table_id)),
            cycle_key: encode_hash_data_key(
                &key::db_key(db_id),
                &key::sequence_cycle_key(table_id),
            ),
            timeout,
        }
    }

    /// As a [`SequenceCounter`] a catalog entry can be given.
    #[must_use]
    pub fn shared(self) -> Arc<dyn SequenceCounter> {
        Arc::new(self)
    }

    /// Reads both fields at one snapshot. Go `HGetInt64`: a missing field is
    /// zero.
    fn read(
        &self,
        transaction: &mut CounterTransaction<C, L, P>,
    ) -> Result<(i64, i64), String> {
        let mut snapshot = crate::real_tikv_catalog::TransactionMetaSnapshot::new(
            transaction,
            self.timeout,
        );
        let stored = snapshot.get(&self.value_key).map_err(|e| e.to_string())?;
        let round = snapshot.get(&self.cycle_key).map_err(|e| e.to_string())?;
        let parse = |bytes: Option<Vec<u8>>| match bytes {
            None => Ok(0),
            Some(bytes) => value::parse_int_value(&bytes).map_err(|error| error.to_string()),
        };
        let stored = parse(stored)?;
        let round = parse(round)?;
        Ok((stored, round))
    }

    fn commit_end_and_round(
        &self,
        call: UnaryCallContext,
        transaction: CounterTransaction<C, L, P>,
        new_end: i64,
        round: i64,
        round_changed: bool,
    ) -> Result<(), CommitFailure> {
        let mut mutations = vec![
            OptimisticMutation::meta_put(
                self.value_key.clone(),
                value::encode_int_value(new_end),
            )
            .map_err(|error| CommitFailure::Failed(error.to_string()))?,
        ];
        if round_changed {
            mutations.push(
                OptimisticMutation::meta_put(
                    self.cycle_key.clone(),
                    value::encode_int_value(round),
                )
                .map_err(|error| CommitFailure::Failed(error.to_string()))?,
            );
        }
        match transaction
            .commit(mutations, &call)
            .map_err(|error| CommitFailure::Failed(error.to_string()))?
        {
            OptimisticCommitOutcome::Committed(_) => Ok(()),
            // A peer reserved from this same key first. Go's `RunInNewTxn`
            // re-reads and tries again, which is the only answer that keeps
            // the two ranges disjoint.
            other => {
                let _ = other.state();
                Err(CommitFailure::Conflict)
            }
        }
    }
}

/// What one reservation commit came back as.
enum CommitFailure {
    /// A peer reserved first; re-read and retry.
    Conflict,
    /// The store refused; retrying cannot help.
    Failed(String),
}

impl<C, L, P> SequenceCounter for ClusterSequenceCounter<C, L, P>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    fn alloc_seq_cache(&self, info: &SequenceInfo) -> Result<(i64, i64, i64), SequenceError> {
        let size = if info.cache { info.cache_value } else { 1 };
        let mut conflicts = 0usize;
        loop {
            let call = UnaryCallContext::with_timeout(self.timeout);
            // Two keys at most: the batch end, and the cycle round only when
            // a wrap happens -- sharing the transaction so the wrap and its
            // round bump commit together.
            let mut transaction = self
                .opener
                .begin(2, 64)
                .map_err(|error| SequenceError::Store(error.to_string()))?;
            let (mut base, mut round) =
                self.read(&mut transaction).map_err(SequenceError::Store)?;
            // Go `alloc4Sequence`: the real offset is the wrapped-to bound
            // once `CYCLE` has wrapped at least once.
            let mut offset = sequence_offset(info, if info.cycle { round } else { 0 });
            let step = match calc_sequence_batch_size(
                base,
                size,
                info.increment,
                offset,
                info.min_value,
                info.max_value,
            ) {
                Some(step) => step,
                None => {
                    if !info.cycle {
                        return Err(SequenceError::RunOut);
                    }
                    // Go resets the counter one step OUTSIDE the wrapped-to
                    // bound, PUTs it together with the bumped round, then
                    // recomputes -- all inside this one reservation.
                    if info.increment > 0 {
                        base = info.min_value - 1;
                        offset = info.min_value;
                    } else {
                        base = info.max_value + 1;
                        offset = info.max_value;
                    }
                    round += 1;
                    let recomputed = calc_sequence_batch_size(
                        base,
                        size,
                        info.increment,
                        offset,
                        info.min_value,
                        info.max_value,
                    )
                    .ok_or(SequenceError::RunOut)?;
                    let delta = if info.increment > 0 {
                        recomputed
                    } else {
                        -recomputed
                    };
                    let new_end = base + delta;
                    match self.commit_end_and_round(call, transaction, new_end, round, true) {
                        Ok(()) => return Ok((base, new_end, round)),
                        Err(CommitFailure::Conflict) => {
                            conflicts += 1;
                            if conflicts >= MAX_RESERVE_RETRIES {
                                return Err(SequenceError::Store(format!(
                                    "the sequence counter could not be reserved after \
                                     {conflicts} attempts"
                                )));
                            }
                        }
                        Err(CommitFailure::Failed(message)) => {
                            return Err(SequenceError::Store(message));
                        }
                    }
                    continue;
                }
            };
            let delta = if info.increment > 0 { step } else { -step };
            let new_end = base + delta;
            match self.commit_end_and_round(call, transaction, new_end, round, false) {
                Ok(()) => return Ok((base, new_end, round)),
                Err(CommitFailure::Conflict) => {
                    conflicts += 1;
                    if conflicts >= MAX_RESERVE_RETRIES {
                        return Err(SequenceError::Store(format!(
                            "the sequence counter could not be reserved after \
                             {conflicts} attempts"
                        )));
                    }
                }
                Err(CommitFailure::Failed(message)) => {
                    return Err(SequenceError::Store(message));
                }
            }
        }
    }

    fn rebase_seq(
        &self,
        info: &SequenceInfo,
        required: i64,
    ) -> Result<(i64, bool), SequenceError> {
        loop {
            let call = UnaryCallContext::with_timeout(self.timeout);
            let mut transaction = self
                .opener
                .begin(1, 64)
                .map_err(|error| SequenceError::Store(error.to_string()))?;
            let (stored, _) = self.read(&mut transaction).map_err(SequenceError::Store)?;
            // Go `rebase4Sequence`: already at or past `required` means no
            // write at all -- `alreadySatisfied`.
            let already_satisfied = if info.increment > 0 {
                stored >= required
            } else {
                stored <= required
            };
            if already_satisfied {
                transaction
                    .finish_without_writes()
                    .map_err(|error| SequenceError::Store(error.to_string()))?;
                return Ok((0, true));
            }
            let mutation = OptimisticMutation::meta_put(
                self.value_key.clone(),
                value::encode_int_value(required),
            )
            .map_err(|error| SequenceError::Store(error.to_string()))?;
            match transaction
                .commit(vec![mutation], &call)
                .map_err(|error| SequenceError::Store(error.to_string()))?
            {
                OptimisticCommitOutcome::Committed(_) => return Ok((required, false)),
                // A peer moved the counter first; Go's RunInNewTxn re-reads.
                other => {
                    let _ = other.state();
                }
            }
        }
    }

    fn restart(&self, info: &SequenceInfo, with: i64) {
        let stored = if info.increment > 0 {
            with - 1
        } else {
            with + 1
        };
        // Go `AlterSequence` RESTART PUTs the counter directly. This node does
        // not serve `ALTER SEQUENCE`; the arm exists so the trait contract
        // holds should that change.
        let call = UnaryCallContext::with_timeout(self.timeout);
        match self.opener.begin(1, 64) {
            Ok(transaction) => {
                match OptimisticMutation::meta_put(
                    self.value_key.clone(),
                    value::encode_int_value(stored),
                ) {
                    Ok(mutation) => {
                        if let Err(error) = transaction.commit(vec![mutation], &call) {
                            eprintln!(
                                "{{\"event\":\"sequence_restart_commit_failed\",\"error\":{:?}}}",
                                error
                            );
                        }
                    }
                    Err(error) => {
                        eprintln!("{{\"event\":\"sequence_restart_mutation_failed\",\"error\":{:?}}}", error);
                    }
                }
            }
            Err(error) => {
                eprintln!("{{\"event\":\"sequence_restart_open_failed\",\"error\":{:?}}}", error);
            }
        }
    }
}
