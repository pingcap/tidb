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

use super::merge_path_tests::as_multiset;
use super::tests::{eq_on, join_with_memory, run, NoColumns};
use super::*;
use crate::mem_quota::{OomAction, StatementMemory};

/// A build side big enough that its chunks outweigh any quota worth
/// setting, with duplicate keys so each probe row walks a multi-entry
/// bucket -- the case where reading rows back in the wrong order would
/// show up.
fn fixture(rows: i64, modulus: i64) -> Vec<Vec<Datum>> {
    (0..rows)
        .map(|i| vec![Datum::Int(i % modulus), Datum::Int(i)])
        .collect()
}

/// The build side is the RIGHT child for an inner join. 5000 rows at a
/// 1024-row chunk is five chunks, and `i % 1000` puts each key's five
/// duplicates in five DIFFERENT chunks -- so a bucket walk after a spill
/// touches every chunk of the file, in build order.
const BUILD_ROWS: i64 = 5000;
const BUILD_KEYS: i64 = 1000;
const PROBE_ROWS: i64 = 200;

fn inner_join(memory: StatementMemory) -> JoinExec<NoColumns> {
    join_with_memory(
        JoinKind::Inner,
        vec![eq_on(0, 0, 2)],
        fixture(PROBE_ROWS, BUILD_KEYS),
        fixture(BUILD_ROWS, BUILD_KEYS),
        2,
        memory,
    )
}

/// A quota the build side cannot fit in, but which is still far larger
/// than a single chunk -- so the spill has something to release and the
/// read-path cancellation #289 describes is not what is being measured.
const TIGHT_QUOTA_BYTES: i64 = 300 * 1024;

fn tight_quota() -> StatementMemory {
    StatementMemory::new(TIGHT_QUOTA_BYTES, OomAction::Cancel, 1)
}

/// What one build chunk costs, measured rather than assumed, so the
/// quota above can be stated as a MULTIPLE of it -- the regime where a
/// spill has something to release. (#289: at quotas below one chunk Go
/// cancels on read-path accounting before any spill can help, and that
/// is deliberately not what these tests exercise.)
#[test]
fn the_tight_quota_is_several_chunks_not_a_fraction_of_one() {
    // Measured with room to spare, so nothing is released mid-build, and
    // read BEFORE `close` detaches the tracker.
    let mut join = inner_join(StatementMemory::default());
    join.open().unwrap();
    let mut req = join.new_chunk();
    join.next(&mut req).unwrap();
    let chunks = i64::try_from(BUILD_ROWS as usize).unwrap() / CHUNK_ROWS as i64 + 1;
    let one_chunk = join.tracker.bytes_consumed() / chunks;
    assert!(one_chunk > 0, "the build side must account something");
    assert!(
        TIGHT_QUOTA_BYTES > 2 * one_chunk,
        "tight quota must be several chunks, one chunk is {one_chunk}"
    );
    assert!(
        join.tracker.bytes_consumed() > TIGHT_QUOTA_BYTES,
        "the build side must not fit in the quota the spill tests use"
    );
}

const CHUNK_ROWS: usize = 1024;

/// The read-back buffer must not accumulate. Go reuses one `chkBuf` per
/// probe and lets the disk reader recycle it; here it is reset before
/// every row, so after a join that read 1000 rows back from disk it holds
/// exactly the last one. Without the reset this grows by one row per
/// matched pair, which on a large spilled join is the whole build side
/// pulled back into memory -- the precise thing the spill exists to
/// prevent.
#[test]
fn the_read_back_buffer_does_not_accumulate_across_a_spilled_probe() {
    let mut join = inner_join(tight_quota());
    join.open().unwrap();
    let mut req = join.new_chunk();
    let mut seen = 0;
    loop {
        join.next(&mut req).unwrap();
        if req.num_rows() == 0 {
            break;
        }
        seen += req.num_rows();
        assert_eq!(
            join.build_buf_rows(),
            1,
            "a spilled probe must reuse exactly one decoded row after {seen} output rows"
        );
    }
    assert!(join.build_side_spilled());
    assert_eq!(seen, 1000);
}

/// An in-memory build row is already a live row in the row container. Go's
/// conditional read path returns it directly and leaves `chkBuf` empty; only
/// a disk read materializes into that scratch chunk.
#[test]
fn an_unspilled_probe_does_not_materialize_build_scratch() {
    let mut join = inner_join(StatementMemory::default());
    let rows = run(&mut join);
    assert!(!rows.is_empty());
    assert!(!join.build_side_spilled());
    assert_eq!(
        join.build_buf_rows(),
        0,
        "an in-memory row must not be copied into the disk read-back buffer"
    );
}

/// The end-to-end claim: spilled and unspilled produce identical output.
#[test]
fn a_spilled_build_side_answers_exactly_the_unspilled_rows() {
    let mut roomy = inner_join(StatementMemory::default());
    let expected = run(&mut roomy);
    assert!(
        !roomy.build_side_spilled(),
        "the control run must NOT spill, or it proves nothing"
    );
    assert!(!expected.is_empty());

    // An INDEPENDENT oracle, not just the unspilled hash run: the nested
    // loop shares no build-side addressing with the hash path, so a
    // pointer bug that corrupts both hash runs identically still shows
    // up here. (A mutation probe that shifted the chunk index by one
    // survived the spilled-vs-unspilled comparison alone.)
    let mut looped = inner_join(StatementMemory::default());
    looped.force_nested_loop();
    assert_eq!(
        as_multiset(run(&mut looped)),
        as_multiset(expected.clone()),
        "the hash path must agree with the nested loop it replaces"
    );

    let mut tight = inner_join(tight_quota());
    let spilled = run(&mut tight);
    assert!(
        tight.build_side_spilled(),
        "the build side must actually reach disk"
    );
    assert!(
        tight.spilled_bytes() > 0,
        "a spilled build side must have written bytes"
    );
    // Row for row and in order, not merely as a set: a bucket read back
    // from disk out of build order would still match as a multiset.
    assert_eq!(spilled, expected);
}

/// The same claim for a LEFT join, where the probe side is preserved and
/// a build row that fails to come back from disk would look like a
/// legitimate NULL pad rather than an error.
#[test]
fn a_spilled_outer_join_pads_exactly_where_the_unspilled_one_does() {
    let build = |memory| {
        join_with_memory(
            JoinKind::Left,
            vec![eq_on(0, 0, 2)],
            fixture(PROBE_ROWS, BUILD_KEYS),
            // Fewer distinct keys retain less bucket memory than the inner
            // fixture, so use enough rows to exceed the several-chunk quota.
            fixture(BUILD_ROWS * 2, 97),
            2,
            memory,
        )
    };
    let mut roomy = build(StatementMemory::default());
    let expected = run(&mut roomy);
    assert!(!roomy.build_side_spilled());

    let mut tight = build(tight_quota());
    let spilled = run(&mut tight);
    assert!(tight.build_side_spilled());
    assert_eq!(spilled, expected);
}

/// The gate. Go registers the spill action only when
/// `tidb_enable_tmp_storage_on_oom` is on; with it off the memory action
/// is still the cancellation, so the statement fails with 8175 instead of
/// spilling. This is the behaviour that existed before this unit, and it
/// must survive unchanged.
#[test]
fn with_tmp_storage_off_an_over_quota_build_side_is_cancelled_not_spilled() {
    let memory = tight_quota().with_tmp_storage_on_oom(false);
    let mut join = inner_join(memory);
    join.open().unwrap();
    let mut req = join.new_chunk();
    let error = loop {
        match join.next(&mut req) {
            Err(error) => break error,
            Ok(()) if req.num_rows() == 0 => panic!("the quota must be enforced"),
            Ok(()) => {}
        }
    };
    assert!(
        !join.build_side_spilled(),
        "with the gate off nothing may reach disk"
    );
    assert!(matches!(error, ExecError::MemoryExceedForQuery { .. }));
}

/// A spill that fires must not leave the action bound to the session
/// tracker: Go's `Close` calls `UnbindActionFromHardLimit`, and a
/// statement that inherited a closed join's action would spill into a
/// container that no longer exists.
#[test]
fn close_unbinds_the_spill_action_from_the_session_tracker() {
    let memory = tight_quota();
    let mut join = inner_join(memory.clone());
    join.open().unwrap();
    let mut req = join.new_chunk();
    join.next(&mut req).unwrap();
    let spill = join
        .registered_spill_action()
        .expect("the gate is on, so an action was registered");
    // Registered: the spill action is at the head, ahead of the
    // cancellation it pushed down as its fallback.
    let head = memory
        .session_tracker()
        .get_fallback_for_test(false)
        .expect("the session tracker always has an action");
    assert!(Arc::ptr_eq(&head, &spill), "the spill action must be bound");

    join.close().unwrap();

    // Unbound: the chain still ACTS -- the cancellation is back at the
    // head -- but this join's action, whose container is now closed, is
    // gone from it.
    let mut current = memory.session_tracker().get_fallback_for_test(false);
    let mut found_any = false;
    while let Some(action) = current {
        found_any = true;
        assert!(
            !Arc::ptr_eq(&action, &spill),
            "a closed join's spill action must not stay in the chain"
        );
        current = action.get_fallback();
    }
    assert!(
        found_any,
        "unbinding must restore the fallback, not clear the chain"
    );
}

/// The container is the only thing that moves: the hash TABLE stays in
/// memory, so a spilled join still answers a miss without touching disk
/// and a NULL key still matches nothing.
#[test]
fn a_spilled_build_side_keeps_null_and_miss_semantics() {
    let mut build = fixture(BUILD_ROWS, BUILD_KEYS);
    build.push(vec![Datum::Null, Datum::Int(-1)]);
    let probe = vec![
        vec![Datum::Int(7), Datum::Int(0)],
        vec![Datum::Null, Datum::Int(1)],
        vec![Datum::Int(9999), Datum::Int(2)],
    ];
    let make = |memory| {
        join_with_memory(
            JoinKind::Inner,
            vec![eq_on(0, 0, 2)],
            probe.clone(),
            build.clone(),
            2,
            memory,
        )
    };
    let mut roomy = make(StatementMemory::default());
    let expected = run(&mut roomy);
    assert!(!roomy.build_side_spilled());

    let mut tight = make(tight_quota());
    let spilled = run(&mut tight);
    assert!(tight.build_side_spilled());
    assert_eq!(spilled, expected);
    // The NULL-keyed build row and the 9999 probe row match nothing, and
    // the NULL probe row matches nothing either.
    assert!(spilled.iter().all(|row| row[0] == 7));
}

/// A cross join has no equal conditions, so it never reaches the hash
/// path and never gets a container -- Go's v1 spill covers the build side
/// of a hash join only. The nested loop's existing 8175 cancellation is
/// what still bounds it, gate or no gate.
#[test]
fn a_cross_join_still_cancels_rather_than_spilling() {
    let mut join = join_with_memory(
        JoinKind::Inner,
        Vec::new(),
        fixture(PROBE_ROWS, BUILD_KEYS),
        fixture(BUILD_ROWS, BUILD_KEYS),
        2,
        tight_quota(),
    );
    assert!(!join.is_hash_join());
    join.open().unwrap();
    let mut req = join.new_chunk();
    let error = join.next(&mut req).expect_err("the quota must be enforced");
    assert!(matches!(error, ExecError::MemoryExceedForQuery { .. }));
    assert!(!join.build_side_spilled());
}
