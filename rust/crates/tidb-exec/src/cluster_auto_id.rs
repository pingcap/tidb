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

//! The cluster tier's home for one table's auto-increment counter: the meta
//! key Go keeps it in, read and written in a transaction of its OWN.
//!
//! This is the half `tidb_executor`'s [`AutoIdStore`] leaves open. The
//! allocation rules -- which id comes next, which explicit value rebases,
//! when the domain is exhausted -- are the allocator's and are the same on
//! both tiers; what changes here is only WHERE the number lives, which is the
//! whole reason this tier can now serve a table it used to refuse.
//!
//! # Which key, and why it is not the obvious one
//!
//! Go has two auto-id meta keys and picks between them by a rule that is easy
//! to get backwards: `pkg/meta/autoid/autoid.go`'s
//! `NewAllocatorsFromTblInfo` gives an AUTO_INCREMENT column the
//! `AutoIncrementType` allocator -- the `IID:<tableID>` key -- ONLY when
//! `tblInfo.SepAutoInc()`, which is `Version >= TableInfoVersion5 &&
//! AutoIDCache == 1`. Every ordinary table has `AutoIDCache == 0`, so its
//! AUTO_INCREMENT ids come from `RowIDAllocType`: the SAME `TID:<tableID>`
//! key that hands out `_tidb_rowid`. `Allocators::Get` makes that explicit by
//! rewriting a request for `AutoIncrementType` into `RowIDAllocType` whenever
//! `SepAutoInc` is false.
//!
//! Choosing `IID:` because the name matches would put this node's counter in
//! a key no Go `tidb-server` on the same cluster reads, and the two would
//! hand out the same ids from separate counters with nothing to detect it.
//! [`auto_id_key_for`] makes the choice once, from the stored `TableInfo`, so
//! the rule lives in one place.
//!
//! # Why a transaction of its own, and why it retries
//!
//! Go reserves through `kv.RunInNewTxn` (`alloc4Signed`, `rebase4Signed`),
//! not through the statement's transaction. That is what burns an id the
//! moment it is issued: a statement that fails afterwards, or a transaction
//! that rolls back, does not give the id back, and no peer can be handed it
//! either. Staging the bump in the row's own transaction would return ids on
//! rollback and let two nodes commit the same id.
//!
//! `RunInNewTxn` retries on a write conflict, and so does this: two nodes
//! reserving at once means one of them loses the prewrite race, and that node
//! must re-read and try again rather than fail an INSERT. Losing the race is
//! the mechanism that keeps the counters disjoint, so it is a normal event,
//! not an error.

use std::sync::Arc;
use std::time::Duration;

use tidb_executor::kv_table::{advance, calc_needed_batch_size, AutoIdStore, AutoIdStoreError};
use tidb_meta::{key, value};
use tidb_model::table_info::TableInfo;
use tidb_txnkv::rpc::UnaryCallContext;
use tidb_txnkv::transaction::{
    OptimisticCommitOutcome, OptimisticMutation, RealOptimisticTransactionOpener,
};

use crate::cluster_catalog::MetaSnapshot;
use crate::real_tikv_catalog::TransactionMetaSnapshot;

/// How many times a reservation re-reads after losing a prewrite race.
///
/// Go's `kv.RunInNewTxn` retries `maxRetryCnt` (100) times. The number only
/// has to outlast the nodes contending for one table's counter; a reservation
/// covers [`DEFAULT_AUTO_ID_STEP`] ids, so a node reaches this key rarely
/// enough that even a handful of peers do not queue this deep.
///
/// [`DEFAULT_AUTO_ID_STEP`]: tidb_executor::kv_table::DEFAULT_AUTO_ID_STEP
const MAX_RESERVE_RETRIES: usize = 100;

/// The meta key holding `table`'s auto-increment counter, as Go chooses it.
///
/// See the module doc: `IID:` only for a separate-allocator table
/// (`AUTO_ID_CACHE 1`), `TID:` -- the row-id key -- for every other one.
#[must_use]
pub fn auto_id_key_for(db_id: i64, table: &TableInfo) -> Vec<u8> {
    if table.sep_auto_inc() {
        key::auto_increment_id_kv_key(db_id, table.id)
    } else {
        key::auto_table_id_kv_key(db_id, table.id)
    }
}

/// One table's counter, living in the cluster's meta keys.
///
/// Held by the node and SHARED by every session on it, which is what makes a
/// reservation worth its transaction: Go caches a range per `tidb-server`,
/// not per connection, so a hundred sysbench connections inserting into one
/// table read this key as rarely as one connection does. A per-session store
/// would be correct and would burn a whole step per connection.
#[derive(Clone)]
pub struct ClusterAutoIdStore {
    opener: RealOptimisticTransactionOpener,
    /// Go's `mDBs` hash key the counter field hangs off.
    counter_key: Vec<u8>,
    /// How long each meta read and the commit may take.
    timeout: Duration,
}

impl std::fmt::Debug for ClusterAutoIdStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClusterAutoIdStore")
            .field("counter_key", &self.counter_key)
            .finish_non_exhaustive()
    }
}

impl ClusterAutoIdStore {
    /// The counter for `table` in database `db_id`.
    #[must_use]
    pub fn new(
        opener: RealOptimisticTransactionOpener,
        db_id: i64,
        table: &TableInfo,
        timeout: Duration,
    ) -> Self {
        ClusterAutoIdStore {
            opener,
            counter_key: auto_id_key_for(db_id, table),
            timeout,
        }
    }

    /// As an [`AutoIdStore`] the table can be given.
    #[must_use]
    pub fn shared(self) -> Arc<dyn AutoIdStore> {
        Arc::new(self)
    }

    /// Runs one read-modify-write attempt against the counter key.
    ///
    /// `decide` sees the stored value as its 64-bit pattern -- Go's absent key
    /// reads as 0 (`HGetInt64`) -- and returns what the key should hold plus
    /// what the caller gets back. Returning `None` for the new value commits
    /// nothing, which is Go's "required base satisfied, we don't need to
    /// update KV".
    fn transact<T>(&self, decide: impl Fn(u64) -> (Option<u64>, T)) -> Result<T, AutoIdStoreError> {
        let mut conflicts = 0usize;
        loop {
            let call = UnaryCallContext::with_timeout(self.timeout);
            let mut transaction = self
                .opener
                // One key, whose value is a decimal integer.
                .begin(1, 64)
                .map_err(|error| store_error("open", &error))?;
            let stored = {
                let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, self.timeout);
                snapshot
                    .get(&self.counter_key)
                    .map_err(|error| store_error("read", &error))?
            };
            // Go `TxStructure.HGetInt64`: a missing field is zero, and a
            // stored value is a decimal string.
            let current = match stored {
                None => 0i64,
                Some(bytes) => {
                    value::parse_int_value(&bytes).map_err(|error| store_error("decode", &error))?
                }
            };
            let (new_value, outcome) = decide(current as u64);
            let Some(new_value) = new_value else {
                transaction
                    .finish_without_writes()
                    .map_err(|error| store_error("finish", &error))?;
                return Ok(outcome);
            };
            let mutation = OptimisticMutation::meta_put(
                self.counter_key.clone(),
                value::encode_int_value(new_value as i64),
            )
            .map_err(|error| store_error("encode", &error))?;
            match transaction
                .commit(vec![mutation], &call)
                .map_err(|error| store_error("commit", &error))?
            {
                OptimisticCommitOutcome::Committed(_) => return Ok(outcome),
                // A peer reserved from this same key first. Go's
                // `RunInNewTxn` re-reads and tries again, which is the only
                // answer that keeps the two ranges disjoint.
                other => {
                    conflicts += 1;
                    if conflicts >= MAX_RESERVE_RETRIES {
                        return Err(AutoIdStoreError(format!(
                            "the auto-increment counter could not be reserved after \
                             {conflicts} attempts, the last ending {:?}",
                            other.state()
                        )));
                    }
                }
            }
        }
    }
}

impl AutoIdStore for ClusterAutoIdStore {
    fn reserve(&self, step: u64, unsigned: bool) -> Result<(u64, u64), AutoIdStoreError> {
        self.transact(|current| {
            let end = advance(current, step, unsigned);
            // No room left: say so by handing back an empty range instead of
            // writing the key, which is Go returning from inside the
            // reservation without ever calling `Inc`.
            if end == current {
                (None, (current, current))
            } else {
                (Some(end), (current, end))
            }
        })
    }

    fn reserve_batch(
        &self,
        minimum_step: u64,
        n: u64,
        increment: u64,
        offset: u64,
        unsigned: bool,
    ) -> Result<(u64, u64), AutoIdStoreError> {
        self.transact(|current| {
            batch_reservation(current, minimum_step, n, increment, offset, unsigned)
        })
    }

    fn rebase(&self, required: u64, unsigned: bool) -> Result<(), AutoIdStoreError> {
        self.transact(|current| {
            if tidb_executor::kv_table::exceeds(required, current, unsigned) {
                (Some(required), ())
            } else {
                (None, ())
            }
        })
    }

    fn reset(&self) -> Result<(), AutoIdStoreError> {
        self.transact(|current| {
            if current == 0 {
                (None, ())
            } else {
                (Some(0), ())
            }
        })
    }
}

/// The decision run inside the counter transaction for one batch request.
/// Computing `needed` here, after the current global base was read, is the
/// source invariant exercised by `TestAllocComputationIssue`.
fn batch_reservation(
    current: u64,
    minimum_step: u64,
    n: u64,
    increment: u64,
    offset: u64,
    unsigned: bool,
) -> (Option<u64>, (u64, u64)) {
    let needed = calc_needed_batch_size(current, n, increment, offset, unsigned);
    let end = advance(current, minimum_step.max(needed), unsigned);
    if end == current {
        (None, (current, current))
    } else {
        (Some(end), (current, end))
    }
}

/// One phrase for every way the counter's home can be out of reach, so the
/// statement that surfaces it says which step failed.
fn store_error(step: &str, error: &impl std::fmt::Display) -> AutoIdStoreError {
    AutoIdStoreError(format!(
        "the table's auto-increment counter could not be {step}: {error}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Source: `pkg/meta/autoid/autoid_test.go::TestAllocComputationIssue`.
    #[test]
    fn test_alloc_computation_issue() {
        // The stale allocator-local bases in the Go regression are 9 and 4.
        // Its transaction reads the actual shared bases 10 and 7, and the
        // next two values on an increment-3 ladder therefore need six slots.
        assert_eq!(
            batch_reservation(10, 3, 2, 3, 1, true),
            (Some(16), (10, 16))
        );
        assert_eq!(batch_reservation(7, 3, 2, 3, 1, false), (Some(13), (7, 13)));

        // The configured reservation step may be larger, but never smaller
        // than the batch recomputed from the transaction's own base.
        assert_eq!(
            batch_reservation(10, 30, 2, 3, 1, false),
            (Some(40), (10, 40))
        );
    }
}
