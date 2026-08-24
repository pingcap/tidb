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

use std::sync::{Arc, Mutex};

/// Go `autoid.EncodeIntToCmpUint`: maps the signed domain onto the unsigned
/// one order-preservingly, so a single `u64` subtraction spans a range that
/// straddles zero without overflowing.
const fn encode_int_to_cmp_uint(value: i64) -> u64 {
    (value as u64) ^ (1u64 << 63)
}

/// Inverse of [`encode_int_to_cmp_uint`] (Go `autoid.DecodeCmpUintToInt`).
const fn decode_cmp_uint_to_int(value: u64) -> i64 {
    (value ^ (1u64 << 63)) as i64
}

/// Go `table.ErrSequenceHasRunOut` (4135), plus the way a cluster counter
/// reports its storage being unreachable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SequenceError {
    /// 4135: no value is left, and the sequence does not `CYCLE`.
    RunOut,
    /// The shared counter could not be read or advanced (cluster tier). Go
    /// surfaces the raw meta/KV error here; this tier carries it as text
    /// because the statement reports the sequence name around it.
    Store(String),
}

/// Go `model.SequenceInfo`, reduced to the fields that decide values.
///
/// NOT MODELLED (documented): `Comment`, and the `Charset`/`Collate` a
/// sequence carries as a `TableInfo` -- neither reaches a value or the
/// `SHOW CREATE SEQUENCE` text this tier prints.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SequenceInfo {
    /// `START WITH`.
    pub start: i64,
    /// `INCREMENT BY`; never zero (Go rejects that with 4136).
    pub increment: i64,
    /// `MINVALUE`.
    pub min_value: i64,
    /// `MAXVALUE`.
    pub max_value: i64,
    /// `CACHE n`; `1` when `NOCACHE` was written.
    pub cache_value: i64,
    /// Whether `CACHE` (rather than `NOCACHE`) is in effect. Go keeps this
    /// separate from `cache_value` and forces a batch of 1 when it is false.
    pub cache: bool,
    /// `CYCLE`.
    pub cycle: bool,
}

/// Go's defaults for an option-free `CREATE SEQUENCE`, captured from
/// `SHOW CREATE SEQUENCE`:
/// `start with 1 minvalue 1 maxvalue 9223372036854775806 increment by 1
/// cache 1000 nocycle` -- note `maxvalue` is `i64::MAX - 1`, not `i64::MAX`.
pub const DEFAULT_SEQUENCE_MAX_VALUE: i64 = i64::MAX - 1;
/// See [`DEFAULT_SEQUENCE_MAX_VALUE`]; the descending default mirrors it.
pub const DEFAULT_SEQUENCE_MIN_VALUE: i64 = i64::MIN + 1;
/// See [`DEFAULT_SEQUENCE_MAX_VALUE`].
pub const DEFAULT_SEQUENCE_CACHE: i64 = 1000;

impl Default for SequenceInfo {
    fn default() -> Self {
        SequenceInfo {
            start: 1,
            increment: 1,
            min_value: 1,
            max_value: DEFAULT_SEQUENCE_MAX_VALUE,
            cache_value: DEFAULT_SEQUENCE_CACHE,
            cache: true,
            cycle: false,
        }
    }
}

/// Go `autoid.SeekToFirstSequenceValue`: the first value in `[minv, maxv]`
/// strictly beyond `base` that is congruent to `offset` modulo `increment`.
/// `None` is Go's `ok == false`, which the caller turns into "refill the
/// cache" or, if the store is also spent, into 4135.
///
/// The arithmetic is done on `encode_int_to_cmp_uint` images, wrapping, exactly
/// as Go does: a range spanning zero would overflow a signed subtraction, and
/// the `%` of a negative operand differs between the two domains.
#[must_use]
pub fn seek_to_first_sequence_value(
    base: i64,
    increment: i64,
    offset: i64,
    minv: i64,
    maxv: i64,
) -> Option<i64> {
    let u_base = encode_int_to_cmp_uint(base);
    let u_offset = encode_int_to_cmp_uint(offset);
    if increment > 0 {
        if base >= maxv {
            return None;
        }
        let u_max = encode_int_to_cmp_uint(maxv);
        let u_increment = increment.unsigned_abs();
        if u_max.wrapping_sub(u_base) < u_increment {
            // Fewer than one whole step is left: Go enumerates the handful of
            // remaining positions rather than computing one.
            let mut i = u_base.wrapping_add(1);
            while i <= u_max {
                if i.wrapping_sub(u_offset).is_multiple_of(u_increment) {
                    return Some(decode_cmp_uint_to_int(i));
                }
                i = i.wrapping_add(1);
            }
            return None;
        }
        let nr = u_base
            .wrapping_add(u_increment)
            .wrapping_sub(u_offset)
            .wrapping_div(u_increment);
        return Some(decode_cmp_uint_to_int(
            nr.wrapping_mul(u_increment).wrapping_add(u_offset),
        ));
    }
    if base <= minv {
        return None;
    }
    let u_min = encode_int_to_cmp_uint(minv);
    let u_increment = increment.unsigned_abs();
    if u_base.wrapping_sub(u_min) < u_increment {
        let mut i = u_base.wrapping_sub(1);
        loop {
            if u_offset.wrapping_sub(i).is_multiple_of(u_increment) {
                return Some(decode_cmp_uint_to_int(i));
            }
            if i == u_min {
                return None;
            }
            i = i.wrapping_sub(1);
        }
    }
    let nr = u_offset
        .wrapping_sub(u_base)
        .wrapping_add(u_increment)
        .wrapping_div(u_increment);
    Some(decode_cmp_uint_to_int(
        u_offset.wrapping_sub(nr.wrapping_mul(u_increment)),
    ))
}

/// Go `autoid.CalcSequenceBatchSize`: how far past `base` the store must be
/// advanced to cover `size` values. `None` is Go's `ErrAutoincReadFailed`,
/// which means the store itself is spent.
///
/// The returned size is always a positive magnitude; the caller applies the
/// sign of `increment` when it advances the store.
#[must_use]
pub fn calc_sequence_batch_size(
    base: i64,
    size: i64,
    increment: i64,
    offset: i64,
    minv: i64,
    maxv: i64,
) -> Option<i64> {
    if increment > 0 {
        if increment == 1 {
            if base >= maxv {
                return None;
            }
            // The rest of the sequence is shorter than one cache batch.
            return Some((maxv - base).min(size));
        }
        let nr = seek_to_first_sequence_value(base, increment, offset, minv, maxv)?;
        if maxv - nr < (size - 1) * increment {
            return Some(maxv - base);
        }
        return Some((nr - base) + (size - 1) * increment);
    }
    if increment == -1 {
        if base <= minv {
            return None;
        }
        return Some((base - minv).min(size));
    }
    let nr = seek_to_first_sequence_value(base, increment, offset, minv, maxv)?;
    if nr - minv < (size - 1) * -increment {
        return Some(base - minv);
    }
    Some((base - nr) + (size - 1) * -increment)
}

/// Go's meta-store half of a sequence. Every allocator for the same sequence
/// serializes batch reservation through this state.
#[derive(Debug)]
struct SequenceStoreState {
    /// Go meta `SequenceValue`: the end of the last batch handed out.
    stored: i64,
    /// Go meta `SequenceCycle`: how many times `CYCLE` has wrapped.
    round: i64,
}

/// The shared counter one sequence's reservations serialize through: Go's
/// meta keys `SequenceValue` (`TID:<id>` hash field `SEV`) and `SequenceCycle`.
/// The in-process tier keeps it behind a mutex ([`LocalSequenceCounter`]); a
/// cluster tier replaces it with one transaction per reservation, which is
/// what lets two TiDB nodes share the ladder without ever overlapping.
///
/// Go `pkg/meta/autoid/autoid.go`: `AllocSeqCache` -> `alloc4Sequence`,
/// `RebaseSeq` -> `rebase4Sequence`; `ALTER SEQUENCE ... RESTART` PUTs the
/// counter directly (`ddl/sequence.go`).
pub trait SequenceCounter: std::fmt::Debug + Send + Sync {
    /// Go `Allocator.AllocSeqCache`: atomically reserve one batch from the
    /// shared counter, returning `(base, end, round)` of the reserved window.
    fn alloc_seq_cache(
        &self,
        info: &SequenceInfo,
    ) -> Result<(i64, i64, i64), SequenceError>;

    /// Go `Allocator.RebaseSeq`: move the shared counter forward to
    /// `required`. `(0, true)` is Go's `alreadySatisfied`, meaning the counter
    /// was already at or past it and nothing was written.
    fn rebase_seq(
        &self,
        info: &SequenceInfo,
        required: i64,
    ) -> Result<(i64, bool), SequenceError>;

    /// Go `AlterSequence` with `RESTART`: the stored counter goes one integer
    /// outside `with`, so the next congruence seek returns `with` itself.
    fn restart(&self, info: &SequenceInfo, with: i64);
}

/// The process-local counter: Go's behaviour when the "meta store" is this
/// node's own memory (the in-process tier, and every test of it).
#[derive(Debug)]
pub struct LocalSequenceCounter(Mutex<SequenceStoreState>);

impl LocalSequenceCounter {
    #[must_use]
    pub fn new(stored: i64) -> Self {
        LocalSequenceCounter(Mutex::new(SequenceStoreState {
            stored,
            round: 0,
        }))
    }
}

impl SequenceCounter for LocalSequenceCounter {
    fn alloc_seq_cache(
        &self,
        info: &SequenceInfo,
    ) -> Result<(i64, i64, i64), SequenceError> {
        let size = if info.cache { info.cache_value } else { 1 };
        let mut store = self.0.lock().expect("sequence store state");
        let mut base = store.stored;
        let mut offset = sequence_offset(info, store.round);
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
                // Go resets the counter one step OUTSIDE the wrapped-to bound
                // so the first seek after the wrap lands on the bound itself.
                if info.increment > 0 {
                    base = info.min_value - 1;
                    offset = info.min_value;
                } else {
                    base = info.max_value + 1;
                    offset = info.max_value;
                }
                store.round += 1;
                store.stored = base;
                calc_sequence_batch_size(
                    base,
                    size,
                    info.increment,
                    offset,
                    info.min_value,
                    info.max_value,
                )
                .ok_or(SequenceError::RunOut)?
            }
        };
        let delta = if info.increment > 0 { step } else { -step };
        store.stored = base + delta;
        Ok((base, store.stored, store.round))
    }

    fn rebase_seq(
        &self,
        info: &SequenceInfo,
        required: i64,
    ) -> Result<(i64, bool), SequenceError> {
        let mut store = self.0.lock().expect("sequence store state");
        let already_satisfied = if info.increment > 0 {
            store.stored >= required
        } else {
            store.stored <= required
        };
        if !already_satisfied {
            store.stored = required;
        }
        Ok((required, already_satisfied))
    }

    fn restart(&self, info: &SequenceInfo, with: i64) {
        let mut store = self.0.lock().expect("sequence store state");
        store.stored = if info.increment > 0 { with - 1 } else { with + 1 };
        store.round = 0;
    }
}

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
            info.start - 1
        } else {
            info.start + 1
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
            info: self.info,
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
        self.info
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
            restart_with - 1
        } else {
            restart_with + 1
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

/// Go `sequenceCommon.getOffset`, free-standing: both counter implementations
/// need the same congruence offset for the current cycle round.
#[must_use]
pub fn sequence_offset(info: &SequenceInfo, round: i64) -> i64 {
    if info.cycle && round > 0 {
        if info.increment > 0 {
            info.min_value
        } else {
            info.max_value
        }
    } else {
        info.start
    }
}

#[cfg(test)]
mod tests;
