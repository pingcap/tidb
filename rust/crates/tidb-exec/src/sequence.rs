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
// See the License for the specific language governing permissions and
// limitations under the License.

//! The [`Sequence`] catalog object behind `CREATE SEQUENCE` and the
//! `NEXTVAL`/`LASTVAL`/`SETVAL` functions — a FAITHFUL port of real
//! TiDB's own three-layer machinery rather than a simplified "logical
//! counter" model, because the layers' interaction is OBSERVABLE:
//!
//! - the local cache window (`base`/`end`/`round`, from
//!   `pkg/table/tables/tables.go`'s `sequenceCommon`),
//! - the persistent counter + cycle flag (`kv_value`/`kv_cycle`, standing
//!   in for the two KV cells `pkg/meta/autoid`'s `SequenceValue()`/
//!   `SequenceCycle()` accessors read and write), and
//! - the allocation/seek/rebase algorithms (`alloc4Sequence`,
//!   `CalcSequenceBatchSize`, `SeekToFirstSequenceValue`,
//!   `rebase4Sequence`, `GetSequenceNextVal`, `SetSequenceVal` — each
//!   ported line-for-line from the Go source named on the function).
//!
//! The observable interaction this faithfulness exists for (confirmed via
//! `gorun`, recorded in task #128's notes): `SETVAL` on a sequence with
//! NO cache loaded yet short-circuits against the ZERO-initialized
//! `base`/`end` sentinel — so on a fresh DECREASING sequence,
//! `SETVAL(seq, v)` with `v >= 0` answers `NULL` and changes NOTHING,
//! where a pure logical-counter model would rebase and answer `v`
//! (probed: fresh `INCREMENT -1 START 10`; `SETVAL(seq, 3)` → `NULL`,
//! then `NEXTVAL` → `10`, not `2`). Porting the real structure makes
//! that case fall out naturally instead of needing a special case —
//! eliminating the edge case rather than checking for it.
//!
//! Sequence allocation is NON-transactional (confirmed via `gorun`:
//! `ROLLBACK` does not undo a `NEXTVAL`) — which is why `Database` keeps
//! sequences outside the `tables` map captured by the transaction rollback
//! catalog.

use tidb_ast::{AlterSequenceStmt, CreateSequenceStmt, DropSequenceStmt, SequenceOption};

use crate::catalog::table_key;
use crate::{Database, ExecError};

/// Real TiDB's own sequence defaults and bounds, read directly from
/// `pkg/meta/model/table.go`'s constants (not assumed).
const DEFAULT_CACHE_VALUE: i64 = 1000;
const DEFAULT_POSITIVE_MIN: i64 = 1;
const DEFAULT_POSITIVE_MAX: i64 = 9223372036854775806;
const DEFAULT_POSITIVE_START: i64 = 1;
const DEFAULT_NEGATIVE_MAX: i64 = -1;
const DEFAULT_NEGATIVE_MIN: i64 = -9223372036854775807;
const DEFAULT_NEGATIVE_START: i64 = -1;

/// One sequence: its `CREATE SEQUENCE` metadata plus the runtime counter
/// state (see the module doc for the three-layer structure and why the
/// cache layer is modelled at all).
#[derive(Debug, Clone)]
pub(crate) struct Sequence {
    increment: i64,
    start: i64,
    min_value: i64,
    max_value: i64,
    cache: bool,
    cache_value: i64,
    cycle: bool,
    /// Local cache window: `base < end` when `increment > 0`, `base > end`
    /// when negative; `base == end` means "no cache loaded" (the
    /// zero-initialized sentinel the module doc's SETVAL artifact hinges
    /// on).
    base: i64,
    end: i64,
    /// Cycle round counter (`> 0` once the sequence has wrapped, which
    /// moves the seek offset from `start` to `min_value`/`max_value`).
    round: i64,
    /// The persistent counter (`SequenceValue()` KV cell): initialized to
    /// `start - 1` / `start + 1` by `createSequenceWithCheck`, advanced in
    /// cache-sized batches by [`Sequence::alloc_seq_cache`].
    kv_value: i64,
    /// The persistent cycle flag (`SequenceCycle()` KV cell).
    kv_cycle: i64,
}

impl Sequence {
    /// Builds a sequence from a `CREATE SEQUENCE` statement: option
    /// application, sign-aware defaults, and validation, ported from
    /// `pkg/ddl/sequence.go`'s `handleSequenceOptions` +
    /// `validateSequenceOptions` + `buildSequenceInfo`; the counter
    /// initialization (`start ∓ 1`) from `createSequenceWithCheck`.
    pub(crate) fn from_stmt(stmt: &CreateSequenceStmt) -> Result<Sequence, ExecError> {
        let mut increment = 1i64;
        let mut cache = true;
        let mut cache_value = DEFAULT_CACHE_VALUE;
        let mut cycle = false;
        let (mut start, mut min_value, mut max_value) = (0i64, 0i64, 0i64);
        let (mut min_set, mut max_set, mut start_set) = (false, false, false);
        for opt in &stmt.options {
            match opt {
                SequenceOption::IncrementBy(n) => increment = *n,
                SequenceOption::StartWith(n) => {
                    start = *n;
                    start_set = true;
                }
                SequenceOption::MinValue(n) => {
                    min_value = *n;
                    min_set = true;
                }
                SequenceOption::MaxValue(n) => {
                    max_value = *n;
                    max_set = true;
                }
                SequenceOption::Cache(n) => {
                    cache_value = *n;
                    cache = true;
                }
                SequenceOption::NoCache => {
                    cache_value = 0;
                    cache = false;
                }
                SequenceOption::Cycle => cycle = true,
                SequenceOption::NoCycle => cycle = false,
                // `NO MINVALUE`/`NO MAXVALUE` mean "use the default",
                // which is exactly what NOT setting the flag produces —
                // matching the Go switch, which has no case for either.
                SequenceOption::NoMinValue | SequenceOption::NoMaxValue => {}
                // ALTER-only options can't appear here — the parser
                // rejects them in CREATE (task #128's grammar tests).
                SequenceOption::Restart | SequenceOption::RestartWith(_) => {
                    return Err(ExecError::Unsupported("RESTART in CREATE SEQUENCE"))
                }
            }
        }
        // Sign-aware defaults (`handleSequenceOptions`' fill-in block).
        if !(min_set && max_set && start_set) {
            if increment >= 0 {
                if !min_set {
                    min_value = DEFAULT_POSITIVE_MIN;
                }
                if !start_set {
                    start = min_value.max(DEFAULT_POSITIVE_START);
                }
                if !max_set {
                    max_value = DEFAULT_POSITIVE_MAX;
                }
            } else {
                if !max_set {
                    max_value = DEFAULT_NEGATIVE_MAX;
                }
                if !start_set {
                    start = max_value.min(DEFAULT_NEGATIVE_START);
                }
                if !min_set {
                    min_value = DEFAULT_NEGATIVE_MIN;
                }
            }
        }
        // `validateSequenceOptions` — every violation is an
        // EXECUTION-time error in real TiDB too (parse succeeds;
        // confirmed via `gorun`: `CREATE SEQUENCE s INCREMENT BY 0` is a
        // real ERR).
        if !validate(increment, start, min_value, max_value, cache, cache_value) {
            return Err(ExecError::Unsupported("invalid sequence options"));
        }
        let kv_value = if increment >= 0 { start - 1 } else { start + 1 };
        Ok(Sequence {
            increment,
            start,
            min_value,
            max_value,
            cache,
            cache_value,
            cycle,
            base: 0,
            end: 0,
            round: 0,
            kv_value,
            kv_cycle: 0,
        })
    }

    /// `ALTER SEQUENCE` — `onAlterSequence` + `alterSequenceOptions` +
    /// `restartSequenceValue`, ported. Applies each option to the live
    /// meta (`RESTART [WITH n]` is accepted only here — the parser
    /// rejects it in `CREATE`), validates the RESULT (an invalid alter is
    /// rejected with the meta UNCHANGED, matching real TiDB's
    /// copy-validate-then-commit order), then — since real TiDB bumps the
    /// schema version, which rebuilds the in-memory `sequenceCommon` —
    /// INVALIDATES the local cache window so the next `NEXTVAL`
    /// re-allocates from the persistent counter. A `RESTART` additionally
    /// rebases that counter to `start ∓ 1` (bare `RESTART` uses the
    /// post-option `start`; `RESTART WITH n` uses `n`), WITHOUT touching
    /// the cycle flag (`RestartSequenceValue` writes only the value cell —
    /// confirmed against the Go source). The observable cache-window
    /// interaction (e.g. `INCREMENT BY 10` after the counter has advanced
    /// yielding a value far past the naive next — task #130's recorded
    /// `1105` probe) falls out of the ported `next_val`/`alloc_seq_cache`
    /// for free.
    pub(crate) fn alter(&mut self, options: &[SequenceOption]) -> Result<(), ExecError> {
        let mut next = self.clone();
        let mut restart = false;
        let mut restart_value = 0i64;
        for opt in options {
            match opt {
                SequenceOption::IncrementBy(n) => next.increment = *n,
                SequenceOption::StartWith(n) => next.start = *n,
                SequenceOption::MinValue(n) => next.min_value = *n,
                SequenceOption::MaxValue(n) => next.max_value = *n,
                SequenceOption::Cache(n) => {
                    next.cache_value = *n;
                    next.cache = true;
                }
                SequenceOption::NoCache => {
                    next.cache_value = 0;
                    next.cache = false;
                }
                SequenceOption::Cycle => next.cycle = true,
                SequenceOption::NoCycle => next.cycle = false,
                SequenceOption::NoMinValue | SequenceOption::NoMaxValue => {}
                SequenceOption::Restart => restart = true,
                SequenceOption::RestartWith(n) => {
                    restart = true;
                    restart_value = *n;
                }
            }
        }
        // Bare `RESTART` restarts to the (post-option) `start`;
        // `RESTART WITH n` to `n` (`alterSequenceOptions`).
        if restart
            && !options
                .iter()
                .any(|o| matches!(o, SequenceOption::RestartWith(_)))
        {
            restart_value = next.start;
        }
        if !validate(
            next.increment,
            next.start,
            next.min_value,
            next.max_value,
            next.cache,
            next.cache_value,
        ) {
            return Err(ExecError::Unsupported("invalid sequence options"));
        }
        // Commit the new meta; invalidate the cache window (base == end)
        // so the next `NEXTVAL` refills from the persistent counter.
        *self = next;
        self.base = 0;
        self.end = 0;
        if restart {
            self.kv_value = if self.increment >= 0 {
                restart_value - 1
            } else {
                restart_value + 1
            };
        }
        Ok(())
    }

    /// The seek offset: `start`, or — once the sequence has wrapped
    /// (`cycle` and `round > 0`) — `min_value`/`max_value` by sign
    /// (`sequenceCommon.getOffset`).
    fn offset(&self) -> i64 {
        if self.cycle && self.round > 0 {
            if self.increment > 0 {
                self.min_value
            } else {
                self.max_value
            }
        } else {
            self.start
        }
    }

    /// `NEXTVAL` — `TableCommon.GetSequenceNextVal`, ported: seek within
    /// the local cache window first, refill from the persistent counter
    /// when exhausted (or never loaded), and error when a `NOCYCLE`
    /// sequence runs out.
    pub(crate) fn next_val(&mut self) -> Result<i64, &'static str> {
        let mut next = 0i64;
        let mut update_cache = false;
        if self.base == self.end {
            // No cache yet.
            update_cache = true;
        } else {
            let offset = self.offset();
            let (minv, maxv) = if self.increment > 0 {
                (self.base, self.end)
            } else {
                (self.end, self.base)
            };
            match seek_first(self.base, self.increment, offset, minv, maxv) {
                Some(v) => next = v,
                None => update_cache = true,
            }
        }
        if update_cache {
            let (base, end, round) = self.alloc_seq_cache()?;
            self.base = base;
            self.end = end;
            self.round = round;
            let offset = self.offset();
            let (minv, maxv) = if self.increment > 0 {
                (self.base, self.end)
            } else {
                (self.end, self.base)
            };
            next = seek_first(self.base, self.increment, offset, minv, maxv)
                .ok_or("can't find the first value in sequence cache")?;
        }
        self.base = next;
        Ok(next)
    }

    /// `SETVAL` — `TableCommon.SetSequenceVal` + `rebase4Sequence`,
    /// ported. `None` means the requested value is already satisfied
    /// (the SQL function answers `NULL`). The `base`/`end` short-circuit
    /// runs FIRST, against the zero-initialized sentinel when no cache
    /// is loaded — the module doc's observable artifact.
    pub(crate) fn set_val(&mut self, new_val: i64) -> Option<i64> {
        if self.increment > 0 {
            if new_val <= self.base {
                return None;
            }
            if new_val <= self.end {
                self.base = new_val;
                return Some(new_val);
            }
        } else {
            if new_val >= self.base {
                return None;
            }
            if new_val >= self.end {
                self.base = new_val;
                return Some(new_val);
            }
        }
        // Invalidate the current cache, then rebase the persistent
        // counter (`rebase4Sequence`).
        self.base = self.end;
        if self.increment > 0 {
            if self.kv_value >= new_val {
                return None;
            }
        } else if self.kv_value <= new_val {
            return None;
        }
        self.kv_value = new_val;
        self.base = new_val;
        self.end = new_val;
        Some(new_val)
    }

    /// One cache-batch allocation from the persistent counter —
    /// `alloc4Sequence`, ported: reads the cycle flag, sizes the batch
    /// (`CalcSequenceBatchSize`), and on exhaustion either errors
    /// (`NOCYCLE`) or resets the counter to the wrap point and goes
    /// around (`CYCLE`).
    fn alloc_seq_cache(&mut self) -> Result<(i64, i64, i64), &'static str> {
        let increment = self.increment;
        let mut offset = self.start;
        let cache_size = if self.cache { self.cache_value } else { 1 };
        let mut round = 0;
        if self.cycle {
            round = self.kv_cycle;
            if round > 0 {
                offset = if increment > 0 {
                    self.min_value
                } else {
                    self.max_value
                };
            }
        }
        let mut new_base = self.kv_value;
        let mut seq_step = calc_batch_size(
            new_base,
            cache_size,
            increment,
            offset,
            self.min_value,
            self.max_value,
        );
        if seq_step.is_none() {
            if !self.cycle {
                return Err("sequence has run out");
            }
            // Reset to the wrap point and mark the cycle round.
            if increment > 0 {
                new_base = self.min_value - 1;
                offset = self.min_value;
            } else {
                new_base = self.max_value + 1;
                offset = self.max_value;
            }
            self.kv_value = new_base;
            round += 1;
            self.kv_cycle = round;
            seq_step = calc_batch_size(
                new_base,
                cache_size,
                increment,
                offset,
                self.min_value,
                self.max_value,
            );
        }
        let step = seq_step.ok_or("sequence has run out")?;
        let delta = if increment > 0 { step } else { -step };
        self.kv_value = self.kv_value.wrapping_add(delta);
        Ok((new_base, self.kv_value, round))
    }
}

impl Database {
    /// Publishes a sequence after the DDL coordinator has applied TiDB's
    /// implicit-commit boundary. Source owners: `pkg/ddl/executor.go`'s
    /// `CreateSequence` and `pkg/ddl/sequence.go`'s `onCreateSequence`.
    pub(crate) fn create_sequence(
        &mut self,
        statement: &CreateSequenceStmt,
    ) -> Result<(), ExecError> {
        let key = table_key(&statement.name);
        if self.tables.contains_key(&key) || self.sequences.borrow().contains_key(&key) {
            if statement.if_not_exists {
                return Ok(());
            }
            return Err(ExecError::Unsupported("table or sequence already exists"));
        }
        let sequence = Sequence::from_stmt(statement)?;
        self.sequences.borrow_mut().insert(key, sequence);
        Ok(())
    }

    /// Applies `ALTER SEQUENCE` to the physical sequence owner. The clone-
    /// validate-commit behavior lives in [`Sequence::alter`], matching
    /// `pkg/ddl/sequence.go:onAlterSequence`.
    pub(crate) fn alter_sequence(
        &mut self,
        statement: &AlterSequenceStmt,
    ) -> Result<(), ExecError> {
        let key = table_key(&statement.name);
        let mut sequences = self.sequences.borrow_mut();
        match sequences.get_mut(&key) {
            Some(sequence) => sequence.alter(&statement.options),
            None if statement.if_exists => Ok(()),
            None => Err(ExecError::UnknownTable(key)),
        }
    }

    /// Removes sequence metadata and session `LASTVAL`, following
    /// `pkg/ddl/executor.go:DropSequence` and `pkg/ddl/table.go`'s shared
    /// drop-object transition.
    pub(crate) fn drop_sequence(&mut self, statement: &DropSequenceStmt) -> Result<(), ExecError> {
        for name in &statement.names {
            let key = table_key(name);
            if self.sequences.borrow_mut().remove(&key).is_none() && !statement.if_exists {
                return Err(ExecError::UnknownTable(key));
            }
            self.seq_lastval.borrow_mut().remove(&key);
        }
        Ok(())
    }
}

/// `validateSequenceOptions`, ported — the shared option-consistency
/// check both `CREATE` and `ALTER` run (over the would-be-final values),
/// an execution-time error in real TiDB, not a parse error.
fn validate(
    increment: i64,
    start: i64,
    min_value: i64,
    max_value: i64,
    cache: bool,
    cache_value: i64,
) -> bool {
    let max_increment = increment.wrapping_abs();
    increment != 0
        && !(cache && cache_value <= 0)
        && max_value >= start
        && max_value > min_value
        && start >= min_value
        && max_value != i64::MAX
        && min_value != i64::MIN
        && cache_value < (i64::MAX - max_increment) / max_increment
}

/// `CalcSequenceBatchSize`, ported — how far the persistent counter may
/// advance for one cache batch; `None` is Go's `ErrAutoincReadFailed`
/// (the sequence is exhausted in its current direction).
fn calc_batch_size(
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
            if maxv - base < size {
                return Some(maxv - base);
            }
            return Some(size);
        }
        let nr = seek_first(base, increment, offset, minv, maxv)?;
        if maxv - nr < (size - 1) * increment {
            return Some(maxv - base);
        }
        return Some((nr - base) + (size - 1) * increment);
    }
    if increment == -1 {
        if base <= minv {
            return None;
        }
        if base - minv < size {
            return Some(base - minv);
        }
        return Some(size);
    }
    let nr = seek_first(base, increment, offset, minv, maxv)?;
    if nr - minv < (size - 1) * (-increment) {
        return Some(base - minv);
    }
    Some((base - nr) + (size - 1) * (-increment))
}

/// Order-preserving map of `i64` onto `u64` (Go's `EncodeIntToCmpUint`),
/// so the seek arithmetic below can't overflow mid-computation.
fn enc(v: i64) -> u64 {
    (v as u64) ^ (1 << 63)
}

fn dec(v: u64) -> i64 {
    (v ^ (1 << 63)) as i64
}

/// `SeekToFirstSequenceValue`, ported — the next valid value strictly
/// past `base` on the `offset + k*increment` lattice, `None` when the
/// sequence is already at its end.
fn seek_first(base: i64, increment: i64, offset: i64, minv: i64, maxv: i64) -> Option<i64> {
    if increment > 0 {
        if base >= maxv {
            return None;
        }
        let (u_max, u_base, u_offset) = (enc(maxv), enc(base), enc(offset));
        let u_increment = increment as u64;
        if u_max - u_base < u_increment {
            // Enumerate the few remaining candidates.
            for i in (u_base + 1)..=u_max {
                if (i.wrapping_sub(u_offset)) % u_increment == 0 {
                    return Some(dec(i));
                }
            }
            return None;
        }
        // Wrapping ops throughout: Go's uint64 arithmetic wraps silently
        // and these formulas genuinely rely on it (e.g. `offset` past
        // `base` makes the subtraction "negative" mid-computation, with
        // the wrap cancelling out by the end) — a plain `-` here is a
        // debug-build panic in Rust for inputs Go handles fine.
        let nr = u_base
            .wrapping_add(u_increment)
            .wrapping_sub(u_offset)
            .wrapping_div(u_increment);
        Some(dec(nr.wrapping_mul(u_increment).wrapping_add(u_offset)))
    } else {
        if base <= minv {
            return None;
        }
        let (u_min, u_base, u_offset) = (enc(minv), enc(base), enc(offset));
        let u_increment = (-increment) as u64;
        if u_base - u_min < u_increment {
            let mut i = u_base - 1;
            loop {
                if (u_offset.wrapping_sub(i)) % u_increment == 0 {
                    return Some(dec(i));
                }
                if i == u_min {
                    return None;
                }
                i -= 1;
            }
        }
        // Same wrapping-arithmetic requirement as the positive branch.
        let nr = u_offset
            .wrapping_sub(u_base)
            .wrapping_add(u_increment)
            .wrapping_div(u_increment);
        Some(dec(u_offset.wrapping_sub(nr.wrapping_mul(u_increment))))
    }
}
