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

//! Every expectation here is a value captured from real TiDB, either through
//! `rust/difftests/gorun` or through a testkit probe over a mock store. None is
//! derived from the ported formula.

use super::*;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

/// Reads `n` values, reporting an exhausted sequence as `None` so a whole
/// captured run (values then error) is one assertion.
fn take(alloc: &SequenceAllocator, n: usize) -> Vec<Option<i64>> {
    (0..n).map(|_| alloc.next_val().ok()).collect()
}

/// Complete translation of
/// `pkg/meta/autoid/seq_autoid_test.go::TestSequenceAutoid`.
#[test]
fn test_sequence_autoid() {
    let info = SequenceInfo {
        start: 1,
        cycle: true,
        cache: true,
        min_value: -10,
        max_value: 10,
        increment: 2,
        cache_value: 3,
    };
    let allocator = SequenceAllocator::new(info);
    let mut state = allocator.state.lock().unwrap();
    assert_eq!((state.base, state.end, state.round), (0, 0, 0));

    allocator.refill(&mut state).unwrap();
    assert_eq!((state.base, state.end, state.round), (0, 5, 0));
    assert_eq!(
        calc_sequence_batch_size(0, 3, 2, 1, -10, 10),
        Some(state.end - state.base)
    );
    let mut base = state.base;
    for expected in [1, 3, 5] {
        let next = seek_to_first_sequence_value(base, 2, 1, base, state.end);
        assert_eq!(next, Some(expected));
        base = expected;
    }
    assert_eq!(
        seek_to_first_sequence_value(base, 2, 1, base, state.end),
        None
    );
    state.base = base;

    allocator.refill(&mut state).unwrap();
    assert_eq!((state.base, state.end, state.round), (5, 10, 0));
    assert_eq!(
        calc_sequence_batch_size(0, 3, 2, 1, -10, 10),
        Some(state.end - state.base)
    );
    base = state.base;
    for expected in [7, 9] {
        let next = seek_to_first_sequence_value(base, 2, 1, base, state.end);
        assert_eq!(next, Some(expected));
        base = expected;
    }
    assert_eq!(
        seek_to_first_sequence_value(base, 2, 1, base, state.end),
        None
    );
    state.base = base;

    allocator.refill(&mut state).unwrap();
    assert_eq!((state.base, state.end, state.round), (-11, -6, 1));
    assert_eq!(
        calc_sequence_batch_size(0, 3, 2, 1, -10, 10),
        Some(state.end - state.base)
    );
    base = state.base;
    for expected in [-10, -8, -6] {
        let next = seek_to_first_sequence_value(base, 2, -10, base, state.end);
        assert_eq!(next, Some(expected));
        base = expected;
    }
    assert_eq!(
        seek_to_first_sequence_value(base, 2, -10, base, state.end),
        None
    );
}

/// Complete translation of
/// `pkg/meta/autoid/seq_autoid_test.go::TestConcurrentAllocSequence`.
#[test]
fn test_concurrent_alloc_sequence() {
    let allocator = SequenceAllocator::new(SequenceInfo {
        start: 100,
        cycle: false,
        cache: true,
        min_value: -100,
        max_value: 100,
        increment: -2,
        cache_value: 3,
    });
    let seen = Arc::new(Mutex::new(HashSet::new()));

    let workers = (0..10)
        .map(|worker| {
            // Go constructs a fresh allocator for every goroutine over the
            // same store: each has an independent local cache, but all ten
            // reserve through one SequenceValue meta key.
            let peer = allocator.peer();
            let seen = Arc::clone(&seen);
            thread::spawn(move || -> Result<(), String> {
                thread::sleep(Duration::from_micros(worker));
                for _ in 0..3 {
                    let (base, end, _) = peer
                        .alloc_seq_cache()
                        .map_err(|error| format!("sequence allocation failed: {error:?}"))?;
                    let mut seen = seen.lock().expect("seen sequence ranges");
                    // The source test checks the whole descending reservation,
                    // not only values on the sequence's increment ladder.
                    for value in (end..base).rev() {
                        if !seen.insert(value) {
                            return Err(format!("duplicate id:{value}"));
                        }
                    }
                }
                Ok(())
            })
        })
        .collect::<Vec<_>>();

    for worker in workers {
        worker.join().expect("sequence worker panicked").unwrap();
    }
    // The first `[101, 96]` reservation covers five integers. Each later
    // reservation first realigns to the even START ladder and therefore
    // spans six; Go's 30 source batches consequently cover 5 + 29 * 6.
    assert_eq!(seen.lock().unwrap().len(), 179);
}

/// `create sequence s1` -> 1, 2, 3. The corpus fixture, and the defaults
/// `SHOW CREATE SEQUENCE` prints for an option-free sequence.
#[test]
fn a_default_sequence_counts_from_one() {
    let info = SequenceInfo::default();
    assert_eq!(info.start, 1);
    assert_eq!(info.increment, 1);
    assert_eq!(info.min_value, 1);
    // Captured: `maxvalue 9223372036854775806` -- one BELOW i64::MAX.
    assert_eq!(info.max_value, 9_223_372_036_854_775_806);
    assert_eq!(info.cache_value, 1000);
    assert!(!info.cycle);

    let alloc = SequenceAllocator::new(info);
    assert_eq!(take(&alloc, 3), [Some(1), Some(2), Some(3)]);
}

/// `create sequence s2 start with 5 increment by 3 minvalue 2 maxvalue 20
/// cache 2` -> 5, 8. The START is the congruence offset, so the ladder is
/// 5, 8, 11 ... rather than anything anchored on MINVALUE.
#[test]
fn start_with_is_the_congruence_offset() {
    let alloc = SequenceAllocator::new(SequenceInfo {
        start: 5,
        increment: 3,
        min_value: 2,
        max_value: 20,
        cache_value: 2,
        cache: true,
        cycle: false,
    });
    assert_eq!(take(&alloc, 2), [Some(5), Some(8)]);
}

/// `create sequence s3 maxvalue 3 cycle` -> 1, 2, 3, 1: the wrap restarts at
/// MINVALUE, and `nocycle` instead runs out with 4135.
#[test]
fn cycle_wraps_to_minvalue_and_nocycle_runs_out() {
    let cycling = SequenceAllocator::new(SequenceInfo {
        max_value: 3,
        cycle: true,
        ..SequenceInfo::default()
    });
    assert_eq!(
        take(&cycling, 4),
        [Some(1), Some(2), Some(3), Some(1)],
        "captured: create sequence s3 maxvalue 3 cycle"
    );

    let bounded = SequenceAllocator::new(SequenceInfo {
        max_value: 3,
        cycle: false,
        ..SequenceInfo::default()
    });
    assert_eq!(
        take(&bounded, 4),
        [Some(1), Some(2), Some(3), None],
        "captured: the 4th read is [table:4135] Sequence 'test.s4' has run out"
    );
    assert_eq!(bounded.next_val(), Err(SequenceError::RunOut));
}

/// `create sequence s increment by -1 minvalue -3 maxvalue 10 start with 1`
/// -> 1, 0, -1, -2, -3, then 4135. A descending sequence is not the ascending
/// one with a sign flip: the seek walks DOWN from START toward MINVALUE, and
/// MAXVALUE is never reached even though START is far below it.
#[test]
fn a_descending_sequence_counts_down_from_start() {
    let alloc = SequenceAllocator::new(SequenceInfo {
        start: 1,
        increment: -1,
        min_value: -3,
        max_value: 10,
        cache_value: DEFAULT_SEQUENCE_CACHE,
        cache: true,
        cycle: false,
    });
    assert_eq!(
        take(&alloc, 6),
        [Some(1), Some(0), Some(-1), Some(-2), Some(-3), None]
    );
}

/// The cache window is OBSERVABLE, which is the sharpest difference from the
/// auto-increment allocator. Captured:
///
/// ```text
/// create sequence s13 increment by 3 cache 2
/// select nextval(s13)                 -- 1
/// select nextval(s13)                 -- 4      (cache now spent)
/// alter sequence s13 increment by 5
/// select nextval(s13)                 -- 6      NOT 9
/// select nextval(s13)                 -- 11
/// select nextval(s13)                 -- 16
/// ```
///
/// 6 rather than 9 because ALTER throws the cache away and the next value is
/// seeked from the batch END (4) against the unchanged START offset (1), not
/// added to the last value read.
#[test]
fn alter_sequence_reseeks_from_the_batch_end_not_the_last_value() {
    let mut alloc = SequenceAllocator::new(SequenceInfo {
        increment: 3,
        cache_value: 2,
        ..SequenceInfo::default()
    });
    assert_eq!(take(&alloc, 2), [Some(1), Some(4)]);
    alloc.alter(SequenceInfo {
        increment: 5,
        cache_value: 2,
        ..SequenceInfo::default()
    });
    assert_eq!(take(&alloc, 3), [Some(6), Some(11), Some(16)]);
}

/// A cache batch that would overshoot MAXVALUE is clamped to it, so the last
/// value is MAXVALUE itself rather than the batch end. Captured:
/// `create sequence s14 maxvalue 5 cache 4` -> 1, 2, 3, 4, 5, then 4135.
#[test]
fn a_cache_batch_is_clamped_at_maxvalue() {
    let alloc = SequenceAllocator::new(SequenceInfo {
        max_value: 5,
        cache_value: 4,
        ..SequenceInfo::default()
    });
    assert_eq!(
        take(&alloc, 6),
        [Some(1), Some(2), Some(3), Some(4), Some(5), None]
    );
}

/// `SETVAL` only moves forward, and reports NULL when it would not. Captured:
///
/// ```text
/// create sequence s11
/// select lastval(s11)     -- <nil>   (nothing issued yet)
/// select setval(s11, 100) -- 100
/// select nextval(s11)     -- 101
/// select setval(s11, 50)  -- <nil>   (backwards: refused, and NOT applied)
/// select nextval(s11)     -- 102
/// ```
#[test]
fn setval_only_moves_a_sequence_forward() {
    let alloc = SequenceAllocator::new(SequenceInfo::default());
    assert_eq!(alloc.set_val(100), Some(100));
    assert_eq!(alloc.next_val(), Ok(101));
    // Backwards: NULL, and the sequence keeps its place.
    assert_eq!(alloc.set_val(50), None);
    assert_eq!(alloc.next_val(), Ok(102));
}

/// A second, lower `SETVAL` before anything has been issued still reports
/// NULL. Go leaves a comment on exactly this case: it records `base` AND `end`
/// at the new value so the window is no longer the initial `base == end == 0`,
/// which would otherwise let the lower value through.
#[test]
fn a_lower_setval_reports_null_even_with_no_cache_yet() {
    let alloc = SequenceAllocator::new(SequenceInfo::default());
    assert_eq!(alloc.set_val(100), Some(100));
    assert_eq!(alloc.set_val(50), None);
    assert_eq!(alloc.next_val(), Ok(101));
}

/// `ALTER SEQUENCE ... RESTART WITH n` puts the next value back at `n`.
#[test]
fn restart_with_resets_the_next_value() {
    let mut alloc = SequenceAllocator::new(SequenceInfo::default());
    assert_eq!(take(&alloc, 2), [Some(1), Some(2)]);
    alloc.restart(10);
    assert_eq!(take(&alloc, 2), [Some(10), Some(11)]);
}

/// The allocator is `Arc`-shared, so a staged catalog copy consumes from the
/// same counter -- a value handed out can never be re-issued by a clone.
#[test]
fn a_cloned_allocator_shares_the_counter() {
    let alloc = SequenceAllocator::new(SequenceInfo::default());
    let staged = alloc.clone();
    assert_eq!(alloc.next_val(), Ok(1));
    assert_eq!(staged.next_val(), Ok(2));
    assert_eq!(alloc.next_val(), Ok(3));
}

/// `NOCACHE` forces a one-value batch, which makes every `nextval` refill.
/// Captured: `create sequence s increment by 3 cache 2 nocache` -> 1, 4, 7 --
/// `NOCACHE` wins over an earlier `CACHE 2`, and the VALUES are unchanged.
/// Only the batching differs, which is what makes the `ALTER` case above the
/// place the cache becomes visible.
#[test]
fn nocache_yields_the_same_values_one_at_a_time() {
    let alloc = SequenceAllocator::new(SequenceInfo {
        increment: 3,
        cache: false,
        cache_value: 1,
        ..SequenceInfo::default()
    });
    assert_eq!(take(&alloc, 3), [Some(1), Some(4), Some(7)]);
}

/// The seek is done on `EncodeIntToCmpUint` images: a window spanning zero
/// would overflow a signed subtraction. A sequence whose range straddles zero
/// still lands on the START ladder.
#[test]
fn the_seek_spans_zero_without_overflowing() {
    assert_eq!(
        seek_to_first_sequence_value(-5, 3, -5, i64::MIN + 1, i64::MAX - 1),
        Some(-2)
    );
    // A range covering nearly the whole domain: the difference only fits
    // unsigned.
    assert_eq!(
        seek_to_first_sequence_value(i64::MIN + 1, 1, i64::MIN + 1, i64::MIN + 1, i64::MAX - 1),
        Some(i64::MIN + 2)
    );
}
