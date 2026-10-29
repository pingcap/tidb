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

//! `CREATE SEQUENCE` metadata and its allocator: Go `model.SequenceInfo`
//! (`pkg/meta/model`), `sequenceCommon` (`pkg/table/tables/tables.go`) and the
//! sequence half of `pkg/meta/autoid/autoid.go`.
//!
//! A sequence is NOT an auto-increment column with different option names, and
//! the differences are behavioural, each captured from real TiDB rather than
//! assumed:
//!
//! * The counter is TWO numbers, not one. `SequenceValue` in the meta store is
//!   the end of the last CACHE BATCH handed to a table, and the table's own
//!   `base`/`end` walk inside that batch. Cache size is therefore OBSERVABLE:
//!   `ALTER SEQUENCE` throws the cache away, so the next value is recomputed
//!   from the batch end rather than from the last value read (captured:
//!   `create sequence s increment by 3 cache 2` yields 1, 4; after
//!   `alter sequence s increment by 5` it yields 6, 11, 16 -- NOT 9).
//! * The next value is a function of the batch base and the START offset, not
//!   `base + increment`: `nextval` seeks the first value congruent to the
//!   offset. `SETVAL` can move `base` off the ladder, which is exactly why Go
//!   seeks instead of adding.
//! * A rolled-back transaction does NOT give the value back (captured: a
//!   `nextval` inside `BEGIN`/`ROLLBACK` still consumes it, and the next read
//!   returns the following value). Allocation runs in its own meta
//!   transaction, outside the statement's.
//! * `SETVAL` only ever moves the sequence FORWARD. `setval(s, 50)` after the
//!   sequence has passed 100 returns NULL and changes nothing.
//! * `LASTVAL` is SESSION-scoped and NULL until this session has called
//!   `nextval` on that sequence; it is not the stored counter.
//! * Exhaustion is error 4135 `Sequence '<db>.<name>' has run out`, not the
//!   auto-id allocator's 1467.
//!
//! A cloned allocator shares its table-local cache, so a staged catalog copy
//! cannot undo a value by swapping an older cache back in. A
//! [`SequenceAllocator::peer`] has its own cache but shares the stored counter,
//! matching two TiDB nodes whose table objects independently reserve batches
//! from the same meta key.

use tidb_model::table::SequenceInfo;
use tidb_autoid::sequence::{
    calc_sequence_batch_size, seek_to_first_sequence_value, sequence_offset, LocalSequenceCounter,
    SequenceCounter, SequenceError,
};
use std::sync::{Arc, Mutex};

/// Go `sequenceCommon`: one table instance's local cache window.
#[derive(Debug)]
struct SequenceState {
    /// Go `sequenceCommon.base`: the last value this table handed out.
    base: i64,
    /// Go `sequenceCommon.end`: the end of the batch `base` walks inside.
    /// `base == end` means "no cache", which is also the initial state.
    end: i64,
    /// The cycle round attached to this particular reserved batch. Non-zero
    /// switches the congruence offset from `START` to `MIN`/`MAXVALUE`.
    round: i64,
}

/// A sequence allocator with a shared meta counter and a table-local cache.
///
/// [`Clone`] shares both handles because catalog staging is another handle to
/// the same table object. [`Self::peer`] instead models a separately built
/// table/allocator: its cache starts empty while its meta counter is shared.
#[derive(Clone, Debug)]
pub struct SequenceAllocator {
    info: SequenceInfo,
    store: Arc<dyn SequenceCounter>,
    state: Arc<Mutex<SequenceState>>,
}

impl SequenceAllocator {
    /// A sequence that has issued nothing yet.
    ///
    /// Go's `CREATE SEQUENCE` seeds the stored value one INTEGER outside
    /// `START`: `start - 1` for ascending sequences and `start + 1` for
    /// descending ones. The congruence seek still lands on `START`, while the
    /// exact base remains observable through the allocated cache range.
    #[must_use]
    pub fn new(info: SequenceInfo) -> Self {
        let stored = if info.increment > 0 {
            info.start.wrapping_sub(1)
        } else {
            info.start.wrapping_add(1)
        };
        SequenceAllocator::over_counter(info, Arc::new(LocalSequenceCounter::new(stored)))
    }

    /// An allocator whose shared counter lives somewhere else -- a cluster's
    /// meta store. Everything else about the allocator (the local cache, the
    /// seek, exhaustion) is unchanged, because those are Go's table-side
    /// halves and do not care where the number lives.
    #[must_use]
    pub fn over_counter(info: SequenceInfo, store: Arc<dyn SequenceCounter>) -> Self {
        SequenceAllocator {
            info,
            store,
            state: Arc::new(Mutex::new(SequenceState {
                base: 0,
                end: 0,
                round: 0,
            })),
        }
    }

    /// Builds another Go `NewSequenceAllocator`/table instance over the same
    /// meta keys. Its local `sequenceCommon` cache is empty, but reservations
    /// advance the same stored `SequenceValue` and cannot overlap this one.
    #[must_use]
    pub fn peer(&self) -> Self {
        SequenceAllocator {
            info: self.info.clone(),
            store: Arc::clone(&self.store),
            state: Arc::new(Mutex::new(SequenceState {
                base: 0,
                end: 0,
                round: 0,
            })),
        }
    }
    /// The sequence's options, for `SHOW CREATE SEQUENCE`.
    #[must_use]
    pub fn info(&self) -> SequenceInfo {
        self.info.clone()
    }

    /// Go `ddl.AlterSequence`: the new options replace the old, and the cached
    /// batch is thrown away so the next value is recomputed from the stored
    /// counter. The stored counter itself is NOT reset (captured: after
    /// `alter sequence s increment by 5` on a sequence that had reached a
    /// batch end of 4, the next value is 6 -- seeded from 4, not from 0).
    pub fn alter(&mut self, info: SequenceInfo) {
        self.info = info;
        let mut state = self.state.lock().expect("sequence state");
        state.base = state.end;
    }

    /// Go `ddl.AlterSequence` with `RESTART`: the stored counter goes one
    /// integer outside `restart_with`, so the next congruence seek returns the
    /// requested value.
    pub fn restart(&mut self, restart_with: i64) {
        self.store.restart(&self.info, restart_with);
        let mut state = self.state.lock().expect("sequence state");
        // The stored counter now sits exactly one integer outside
        // `restart_with`; the cache collapses onto it either way.
        let stored = if self.info.increment > 0 {
            restart_with.wrapping_sub(1)
        } else {
            restart_with.wrapping_add(1)
        };
        state.base = stored;
        state.end = stored;
        state.round = 0;
    }

    /// Go `sequenceCommon.getOffset`: the congruence offset is `START` until
    /// `CYCLE` has wrapped at least once, and the wrapped-to bound afterwards.
    fn offset(&self, round: i64) -> i64 {
        sequence_offset(&self.info, round)
    }

    /// Go `TableCommon.GetSequenceNextVal`: seek inside the cached batch, and
    /// refill from the store when the batch has nothing congruent left.
    pub fn next_val(&self) -> Result<i64, SequenceError> {
        let mut state = self.state.lock().expect("sequence state");
        // Go seeks with (end, base) swapped for a descending sequence, so the
        // window is always passed low-to-high.
        let seek_in_cache = |state: &SequenceState| {
            let (lo, hi) = if self.info.increment > 0 {
                (state.base, state.end)
            } else {
                (state.end, state.base)
            };
            seek_to_first_sequence_value(
                state.base,
                self.info.increment,
                self.offset(state.round),
                lo,
                hi,
            )
        };

        // `base == end` is the no-cache state; otherwise the batch may still
        // hold a congruent value. It is a seek rather than `base + increment`
        // because `SETVAL` can leave `base` off the ladder.
        let cached = if state.base == state.end {
            None
        } else {
            seek_in_cache(&state)
        };
        if let Some(next) = cached {
            state.base = next;
            return Ok(next);
        }

        self.refill(&mut state)?;
        let next = seek_in_cache(&state).ok_or(SequenceError::RunOut)?;
        state.base = next;
        Ok(next)
    }

    /// Go `Allocator.AllocSeqCache`: atomically reserve one batch from the
    /// shared meta counter. The returned bounds belong exclusively to the
    /// caller; consuming values inside them needs no store lock.
    pub fn alloc_seq_cache(&self) -> Result<(i64, i64, i64), SequenceError> {
        self.store.alloc_seq_cache(&self.info)
    }

    /// Refill one table instance's local cache from [`Self::alloc_seq_cache`].
    fn refill(&self, state: &mut SequenceState) -> Result<(), SequenceError> {
        let (base, end, round) = self.alloc_seq_cache()?;
        state.base = base;
        state.end = end;
        state.round = round;
        Ok(())
    }

    /// Go `TableCommon.SetSequenceVal`: move the sequence forward to
    /// `new_val`. `Ok(None)` is Go's `alreadySatisfied`, which `SETVAL`
    /// reports as SQL `NULL` -- a sequence never moves backwards.
    pub fn set_val(&self, new_val: i64) -> Result<Option<i64>, SequenceError> {
        let mut state = self.state.lock().expect("sequence state");
        if self.info.increment > 0 {
            if new_val <= state.base {
                return Ok(None);
            }
            if new_val <= state.end {
                state.base = new_val;
                return Ok(Some(new_val));
            }
        } else {
            if new_val >= state.base {
                return Ok(None);
            }
            if new_val >= state.end {
                state.base = new_val;
                return Ok(Some(new_val));
            }
        }
        // Past the cached batch: rebase the store, and record the new value as
        // BOTH bounds. Go comments the reason -- with `base`/`end` left at
        // their initial equality a second, lower `SETVAL` would not report
        // NULL (captured: `setval(s, 100)` then `setval(s, 50)` gives 100 then
        // NULL).
        // Go `rebase4Sequence`: a store already at or past `new_val` is left
        // untouched and reported as `alreadySatisfied`. The cache window is
        // collapsed onto `new_val` EITHER WAY.
        let (_, already_satisfied) = self.store.rebase_seq(&self.info, new_val)?;
        state.base = new_val;
        state.end = new_val;
        if already_satisfied {
            Ok(None)
        } else {
            Ok(Some(new_val))
        }
    }

    /// Go `GetSequenceBaseEndRound`, exposed for tests that pin the cache.
    #[must_use]
    pub fn base_end_round(&self) -> (i64, i64, i64) {
        let state = self.state.lock().expect("sequence state");
        (state.base, state.end, state.round)
    }
}


#[cfg(test)]
mod tests;

/// A sequence in the catalog: the name, Go `SequenceInfo.Comment`, and its
/// allocator.
///
/// The allocator is `Arc`-shared inside, so cloning this entry (as a staged
/// catalog copy does) shares the counter rather than forking it.
#[derive(Clone, Debug)]
pub struct SequenceDef {
    /// Go TableInfo.ID, stable across ALTER and distinct after DROP/CREATE.
    pub id: i64,
    /// The name as written, for `SHOW CREATE SEQUENCE` and `SHOW TABLES`.
    pub name: String,
    /// Go `model.SequenceInfo.Comment`, retained for sequence metadata and
    /// appended to `SHOW CREATE SEQUENCE` when non-empty and exposed through
    /// the shared information-schema sequence reader.
    pub comment: String,
    /// The value source. See [`crate::sequence`].
    pub allocator: SequenceAllocator,
    /// Go `TableInfo.UpdateTS`, the TSO of the job that created it (0 when
    /// none was recorded).
    pub update_ts: u64,
}

/// Go `tableCommon`'s `util.SequenceTable` half.
impl tidb_util::misc::SequenceTable for SequenceDef {
    fn get_sequence_id(&self) -> i64 {
        self.id
    }

    fn get_sequence_next_val(
        &self,
        _ctx: &dyn std::any::Any,
        db_name: &str,
        seq_name: &str,
    ) -> Result<i64, tidb_terror::TerrorError> {
        self.allocator
            .next_val()
            .map_err(|_| sequence_has_run_out(db_name, seq_name))
    }

    fn set_sequence_val(
        &self,
        _ctx: &dyn std::any::Any,
        new_val: i64,
        db_name: &str,
        seq_name: &str,
    ) -> Result<(i64, bool), tidb_terror::TerrorError> {
        match self.allocator.set_val(new_val) {
            Ok(Some(value)) => Ok((value, false)),
            Ok(None) => Ok((0, true)),
            Err(_) => Err(sequence_has_run_out(db_name, seq_name)),
        }
    }
}

/// Go `table.ErrSequenceHasRunOut.GenWithStackByArgs(dbName, seqName)`.
fn sequence_has_run_out(db_name: &str, seq_name: &str) -> tidb_terror::TerrorError {
    crate::table_errors::ERR_SEQUENCE_HAS_RUN_OUT
        .generate_with_stack_by_args(&[db_name.into(), seq_name.into()])
}
