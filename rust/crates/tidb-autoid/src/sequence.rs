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

//! Go `pkg/meta/autoid/autoid.go`'s sequence half: the stored counter a
//! sequence reserves cache batches from (`AllocSeqCache` -> `alloc4Sequence`,
//! `RebaseSeq`), the congruence seek (`SeekToFirstSequenceValue`) and the
//! batch size (`CalcSequenceBatchSize`). The table's own cache window over
//! it is `tidb_table::sequence` (Go `sequenceCommon`).

use std::sync::Mutex;

use tidb_model::table::SequenceInfo;

/// Go `autoid.EncodeIntToCmpUint`: maps the signed domain onto the unsigned
/// one order-preservingly, so a single `u64` subtraction spans a range that
/// straddles zero without overflowing.
pub const fn encode_int_to_cmp_uint(value: i64) -> u64 {
    (value as u64) ^ (1u64 << 63)
}

/// Inverse of [`encode_int_to_cmp_uint`] (Go `autoid.DecodeCmpUintToInt`).
pub const fn decode_cmp_uint_to_int(value: u64) -> i64 {
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

impl std::fmt::Display for SequenceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RunOut => formatter.write_str("sequence has run out"),
            Self::Store(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for SequenceError {}

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
    // Go evaluates these signed expressions with two's-complement wraparound;
    // keep that contract explicit instead of inheriting Rust's debug panics.
    if increment > 0 {
        if increment == 1 {
            if base >= maxv {
                return None;
            }
            // The rest of the sequence is shorter than one cache batch.
            return Some(maxv.wrapping_sub(base).min(size));
        }
        let nr = seek_to_first_sequence_value(base, increment, offset, minv, maxv)?;
        let remaining = maxv.wrapping_sub(nr);
        let requested = size.wrapping_sub(1).wrapping_mul(increment);
        if remaining < requested {
            return Some(maxv.wrapping_sub(base));
        }
        return Some(nr.wrapping_sub(base).wrapping_add(requested));
    }
    if increment == -1 {
        if base <= minv {
            return None;
        }
        return Some(base.wrapping_sub(minv).min(size));
    }
    let nr = seek_to_first_sequence_value(base, increment, offset, minv, maxv)?;
    let remaining = nr.wrapping_sub(minv);
    let requested = size.wrapping_sub(1).wrapping_mul(increment.wrapping_neg());
    if remaining < requested {
        return Some(base.wrapping_sub(minv));
    }
    Some(base.wrapping_sub(nr).wrapping_add(requested))
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
    fn alloc_seq_cache(&self, info: &SequenceInfo) -> Result<(i64, i64, i64), SequenceError>;

    /// Go `Allocator.RebaseSeq`: move the shared counter forward to
    /// `required`. `(0, true)` is Go's `alreadySatisfied`, meaning the counter
    /// was already at or past it and nothing was written.
    fn rebase_seq(&self, info: &SequenceInfo, required: i64) -> Result<(i64, bool), SequenceError>;

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
        LocalSequenceCounter(Mutex::new(SequenceStoreState { stored, round: 0 }))
    }
}

impl SequenceCounter for LocalSequenceCounter {
    fn alloc_seq_cache(&self, info: &SequenceInfo) -> Result<(i64, i64, i64), SequenceError> {
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
                    base = info.min_value.wrapping_sub(1);
                    offset = info.min_value;
                } else {
                    base = info.max_value.wrapping_add(1);
                    offset = info.max_value;
                }
                store.round = store.round.wrapping_add(1);
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
        let delta = if info.increment > 0 {
            step
        } else {
            step.wrapping_neg()
        };
        store.stored = base.wrapping_add(delta);
        Ok((base, store.stored, store.round))
    }

    fn rebase_seq(&self, info: &SequenceInfo, required: i64) -> Result<(i64, bool), SequenceError> {
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
        store.stored = if info.increment > 0 {
            with.wrapping_sub(1)
        } else {
            with.wrapping_add(1)
        };
        store.round = 0;
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
