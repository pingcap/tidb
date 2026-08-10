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

use super::row_container_test_hooks::*;
use super::*;
use std::sync::atomic::AtomicUsize;
use std::sync::{mpsc, Barrier};
use tidb_datatype::FieldTypeCode as C;
use tidb_util::memory::BaseOomAction;

fn int64_fields() -> Vec<FieldType> {
    vec![FieldType::new(C::LongLong)]
}

fn int64_chunk(sz: usize) -> Chunk {
    let fields = int64_fields();
    let mut chk = Chunk::new_with_capacity(&fields, sz);
    for i in 0..sz {
        chk.append_int64(0, i as i64);
    }
    chk
}

#[derive(Default)]
struct CountingFallback {
    base: BaseOomAction,
    calls: AtomicUsize,
}

impl ActionOnExceed for CountingFallback {
    fn action(&self, _tracker: &Arc<Tracker>) {
        self.calls.fetch_add(1, SeqCst);
    }

    fn set_fallback(&self, action: Option<ArcAction>) {
        self.base.set_fallback(action);
    }

    fn get_fallback(&self) -> Option<ArcAction> {
        self.base.get_fallback()
    }

    fn get_priority(&self) -> i64 {
        0
    }

    fn set_finished(&self) {
        self.base.set_finished();
    }

    fn is_finished(&self) -> bool {
        self.base.is_finished()
    }
}

struct PausingFallback {
    base: BaseOomAction,
    calls: AtomicUsize,
    started: Arc<Barrier>,
    release: Arc<Barrier>,
}

impl ActionOnExceed for PausingFallback {
    fn action(&self, _tracker: &Arc<Tracker>) {
        self.calls.fetch_add(1, SeqCst);
        self.started.wait();
        self.release.wait();
    }

    fn set_fallback(&self, action: Option<ArcAction>) {
        self.base.set_fallback(action);
    }

    fn get_fallback(&self) -> Option<ArcAction> {
        self.base.get_fallback()
    }

    fn get_priority(&self) -> i64 {
        0
    }

    fn set_finished(&self) {
        self.base.set_finished();
    }

    fn is_finished(&self) -> bool {
        self.base.is_finished()
    }
}

/// Every row of the container, in order, as its first column's int64.
fn iterate(rc: &RowContainer) -> Vec<i64> {
    let mut out = Vec::new();
    let mut it = Iterator4RowContainer::new(rc);
    let mut row = it.begin();
    while row.is_some() {
        out.push(row.expect("row").get_int64(0));
        row = it.next_row();
    }
    assert_eq!(it.error(), None);
    out
}

/// Go `TestNewRowContainer`.
#[test]
fn a_new_row_container_has_not_spilled() {
    let rc = RowContainer::new(&int64_fields(), 1024, crate::test_temp_storage::storage());
    assert!(!rc.already_spilled());
    assert_eq!(rc.num_row(), 0);
}

/// Go `GetRowAndAppendToChunkIfInDisk`: a live memory row does not occupy the
/// caller's disk scratch chunk, while the same pointer appends exactly one row
/// after spill.
#[test]
fn conditional_row_read_materializes_only_after_spill() {
    let fields = int64_fields();
    let mut rc = RowContainer::new(&fields, 4, crate::test_temp_storage::storage());
    rc.add(int64_chunk(2)).expect("add rows");
    let mut scratch = Chunk::new_with_capacity(&fields, 1);

    {
        let loaded = rc
            .get_row_and_append_to_chunk_if_in_disk(RowPtr::new(0, 1), &mut scratch)
            .expect("read memory row");
        assert_eq!(loaded.appended_row_index(), None);
        assert_eq!(scratch.num_rows(), 0);
        assert_eq!(loaded.row(&scratch).get_int64(0), 1);
    }

    rc.spill_to_disk();
    let loaded = rc
        .get_row_and_append_to_chunk_if_in_disk(RowPtr::new(0, 1), &mut scratch)
        .expect("read spilled row");
    assert_eq!(loaded.appended_row_index(), Some(0));
    assert_eq!(scratch.num_rows(), 1);
    assert_eq!(loaded.row(&scratch).get_int64(0), 1);
}

/// In memory `GetChunk` exposes the live chunk under a records read guard;
/// it does not deep-clone row buffers merely because the container state is
/// shared.
#[test]
fn get_chunk_keeps_the_live_in_memory_view() {
    let mut rc = RowContainer::new(&int64_fields(), 4, crate::test_temp_storage::storage());
    rc.add(int64_chunk(4)).expect("add");
    let stored = {
        let records = read_unpoisoned(&rc.shared.records);
        records.in_memory.get_chunk(0) as *const Chunk
    };
    let view = rc.get_chunk(0).expect("live chunk view");
    assert_eq!(&*view as *const Chunk, stored);
    assert_eq!(view.get_row(3).get_int64(0), 3);
}

/// Go `TestSel`: the selection vector survives the move to disk.
///
/// Go drives this through `NewMultiIterator(NewIterator4RowContainer(rc),
/// NewIterator4Chunk(chk))`; [`Iterator4RowContainer`] is not a
/// `ChunkIterator` (see its doc), so the container half is iterated on its
/// own here and the trailing chunk is checked separately.
#[test]
fn a_selection_vector_survives_the_spill() {
    let fields = int64_fields();
    let sz = 4usize;
    let n = 64usize;
    let mut rc = RowContainer::new(&fields, sz, crate::test_temp_storage::storage());
    let mut chk = Chunk::new_with_capacity(&fields, sz);
    let mut num_rows = 0;
    for i in 0..(n - sz) {
        chk.append_int64(0, i as i64);
        if chk.num_rows() == sz {
            chk.set_sel(Some(vec![0, 2]));
            num_rows += 2;
            rc.add(chk).expect("add");
            chk = Chunk::new_with_capacity(&fields, sz);
        }
    }
    assert_eq!(rc.num_chunks(), num_rows / 2);
    assert_eq!(rc.num_row(), num_rows);

    // Rows 0 and 2 of each four-row chunk.
    let want: Vec<i64> = (0..(n - sz) as i64)
        .filter(|i| i % 4 == 0 || i % 4 == 2)
        .collect();
    assert_eq!(iterate(&rc), want, "in memory");

    rc.spill_to_disk();
    assert_eq!(rc.spill_error(), None);
    assert!(rc.already_spilled());
    assert_eq!(iterate(&rc), want, "after spilling");

    rc.close();
    assert_eq!(rc.mem_tracker().bytes_consumed(), 0);
    assert!(rc.mem_tracker().max_consumed() > 0);
}

/// Go `TestSpillAction`: the second chunk pushes the tracker past its
/// limit, the container moves to disk, and later adds go straight there.
#[test]
fn the_spill_action_moves_the_container_to_disk() {
    let fields = int64_fields();
    let sz = 4;
    let mut rc = RowContainer::new(&fields, sz, crate::test_temp_storage::storage());
    let chk = int64_chunk(sz);
    let action = rc.action_spill();
    rc.mem_tracker().set_bytes_limit(chk.memory_usage() + 1);
    rc.mem_tracker()
        .fallback_old_and_set_new_action(Arc::clone(&action) as ArcAction);

    assert!(!rc.already_spilled());
    rc.add(chk.clone()).expect("add");
    assert!(!rc.already_spilled(), "one chunk is within the quota");
    assert_eq!(rc.mem_tracker().bytes_consumed(), chk.memory_usage());

    // Go's comment: adding the same chunk twice double-counts its memory;
    // that is the point, it is how the quota is crossed.
    rc.add(chk.clone()).expect("add");
    assert!(rc.already_spilled(), "the quota was crossed");

    {
        let res = rc.get_chunk(0).expect("get_chunk");
        assert_eq!(res.num_rows(), chk.num_rows());
        for row_idx in 0..res.num_rows() {
            assert_eq!(
                res.get_row(row_idx).get_int64(0),
                chk.get_row(row_idx).get_int64(0)
            );
        }
    }

    // Written again, this time straight to the spill file.
    rc.add(chk.clone()).expect("add");
    assert!(rc.already_spilled());
    {
        let res = rc.get_chunk(2).expect("get_chunk");
        assert_eq!(res.num_rows(), chk.num_rows());
        for row_idx in 0..res.num_rows() {
            assert_eq!(
                res.get_row(row_idx).get_int64(0),
                chk.get_row(row_idx).get_int64(0)
            );
        }
    }

    rc.reset();
}

/// `List::add` may make two positive `Consume` calls when its tail was not
/// accounted yet. Both calls reenter the same action stack: the first arms
/// pending spill and every later call must return rather than wait on the
/// add that cannot finish until it returns.
#[test]
fn repeated_reentrant_actions_return_to_the_same_add() {
    let mut rc = RowContainer::new(&int64_fields(), 4, crate::test_temp_storage::storage());
    let seed = int64_chunk(1);
    write_unpoisoned(&rc.shared.records)
        .in_memory
        .append_row(seed.get_row(0));
    assert_eq!(rc.mem_tracker().bytes_consumed(), 0, "tail is unaccounted");

    let action = rc.action_spill();
    rc.mem_tracker().set_bytes_limit(1);
    rc.mem_tracker()
        .set_action_on_exceed(Some(Arc::clone(&action) as ArcAction));
    rc.add(int64_chunk(1)).expect("reentrant add finishes");

    assert!(rc.already_spilled());
    assert_eq!(rc.mem_tracker().bytes_consumed(), 0);
    assert_eq!(iterate(&rc), vec![0, 0]);
}

/// A repeat on the mutating thread must return to `List::add`, but a
/// second thread is a later action: it waits for the pending spill and
/// then checks fallback instead of disappearing with the reentrant call.
#[test]
fn a_concurrent_second_action_waits_for_the_reentrant_add_spill() {
    let mut rc = RowContainer::new(&int64_fields(), 4, crate::test_temp_storage::storage());
    let statement_tracker = Tracker::new(-1, 1);
    rc.mem_tracker().attach_to(&statement_tracker);
    let unrelated_tracker = Tracker::new(-2, -1);
    unrelated_tracker.attach_to(&statement_tracker);
    unrelated_tracker.consume(2);

    let action = rc.action_spill();
    let fallback = Arc::new(CountingFallback::default());
    action.set_fallback(Some(Arc::clone(&fallback) as ArcAction));
    statement_tracker.set_action_on_exceed(Some(Arc::clone(&action) as ArcAction));
    let (reentrant_started, reentrant_release) = pause_next_reentrant_action(&rc);
    let (later_started, later_release) = pause_next_later_action(&rc);

    let mut adding = rc.shallow_copy();
    let add_handle = std::thread::spawn(move || adding.add(int64_chunk(4)));
    reentrant_started.wait();

    let later_action = Arc::clone(&action);
    let later_tracker = Arc::clone(&statement_tracker);
    let (done_tx, done_rx) = mpsc::channel();
    let later_handle = std::thread::spawn(move || {
        later_action.action(&later_tracker);
        done_tx.send(()).expect("report later action");
    });
    later_started.wait();
    assert_eq!(done_rx.try_recv(), Err(mpsc::TryRecvError::Empty));

    later_release.wait();
    reentrant_release.wait();
    add_handle
        .join()
        .expect("add thread")
        .expect("reentrant add");
    later_handle.join().expect("later action thread");
    done_rx.recv().expect("later action completion");

    assert!(rc.already_spilled());
    assert_eq!(fallback.calls.load(SeqCst), 1);
    unrelated_tracker.consume(-2);
    rc.close();
}

/// `List::reset` accounts its final unaccounted tail and can therefore
/// reenter the spill action. Reset releases records before processing that
/// pending spill, which clears the accounted freelist memory.
#[test]
fn resetting_memory_processes_its_reentrant_spill() {
    let mut rc = RowContainer::new(&int64_fields(), 4, crate::test_temp_storage::storage());
    let seed = int64_chunk(1);
    write_unpoisoned(&rc.shared.records)
        .in_memory
        .append_row(seed.get_row(0));
    let action = rc.action_spill();
    rc.mem_tracker().set_bytes_limit(1);
    rc.mem_tracker()
        .set_action_on_exceed(Some(Arc::clone(&action) as ArcAction));

    rc.reset();

    assert!(rc.already_spilled());
    assert_eq!(rc.num_row(), 0);
    assert_eq!(rc.mem_tracker().bytes_consumed(), 0);
    assert_eq!(action.status(), SpillStatus::SpilledYet);
}

/// A quota action belongs to the shared statement tracker, so any child
/// allocation that crosses that quota must be able to spill this
/// container. The spill cannot depend on a later `RowContainer::add`.
#[test]
fn an_unrelated_parent_allocation_spills_without_another_add() {
    let fields = int64_fields();
    let mut rc = RowContainer::new(&fields, 4, crate::test_temp_storage::storage());
    let statement_tracker = Tracker::new(-1, -1);
    rc.mem_tracker().attach_to(&statement_tracker);
    let unrelated_tracker = Tracker::new(-2, -1);
    unrelated_tracker.attach_to(&statement_tracker);

    let action = rc.action_spill();
    statement_tracker.fallback_old_and_set_new_action(Arc::clone(&action) as ArcAction);
    let chk = int64_chunk(4);
    let container_bytes = chk.memory_usage();
    statement_tracker.set_bytes_limit(container_bytes + 1);

    rc.add(chk).expect("final add");
    assert!(!rc.already_spilled(), "the final add is below quota");
    assert_eq!(rc.mem_tracker().bytes_consumed(), container_bytes);
    assert_eq!(iterate(&rc), vec![0, 1, 2, 3]);

    unrelated_tracker.consume(2);

    assert!(
        rc.already_spilled(),
        "the parent action must spill without another RowContainer::add"
    );
    assert_eq!(rc.mem_tracker().bytes_consumed(), 0);
    assert!(rc.disk_tracker().bytes_consumed() > 0);
    assert_eq!(iterate(&rc), vec![0, 1, 2, 3]);

    rc.close();
    unrelated_tracker.consume(-2);
}

/// The first trigger is reserved for spill even when unrelated memory
/// remains above quota. Only a later trigger may invoke fallback.
#[test]
fn fallback_runs_only_after_the_first_trigger_finishes_spilling() {
    let mut rc = RowContainer::new(&int64_fields(), 4, crate::test_temp_storage::storage());
    let statement_tracker = Tracker::new(-1, -1);
    rc.mem_tracker().attach_to(&statement_tracker);
    let unrelated_tracker = Tracker::new(-2, -1);
    unrelated_tracker.attach_to(&statement_tracker);
    let action = rc.action_spill();
    let fallback = Arc::new(CountingFallback::default());
    action.set_fallback(Some(Arc::clone(&fallback) as ArcAction));
    statement_tracker.set_action_on_exceed(Some(Arc::clone(&action) as ArcAction));

    let chk = int64_chunk(4);
    let bytes = chk.memory_usage();
    statement_tracker.set_bytes_limit(bytes + 1);
    rc.add(chk).expect("add below quota");
    unrelated_tracker.consume(bytes + 2);

    assert!(rc.already_spilled());
    assert_eq!(fallback.calls.load(SeqCst), 0, "first trigger spills only");
    unrelated_tracker.consume(1);
    assert_eq!(fallback.calls.load(SeqCst), 1, "later trigger falls back");

    rc.close();
    unrelated_tracker.consume(-(bytes + 3));
}

/// If reset publishes a new generation before a waiting action claims the
/// fallback slot, that action re-enters as the first trigger of the new
/// generation. It must not run a stale fallback from the spilled state.
#[test]
fn reset_wins_the_race_with_a_waiting_fallback() {
    let mut rc = RowContainer::new(&int64_fields(), 4, crate::test_temp_storage::storage());
    let statement_tracker = Tracker::new(-1, 1);
    rc.mem_tracker().attach_to(&statement_tracker);
    let unrelated_tracker = Tracker::new(-2, -1);
    unrelated_tracker.attach_to(&statement_tracker);
    unrelated_tracker.consume(2);
    let action = rc.action_spill();
    let fallback = Arc::new(CountingFallback::default());
    action.set_fallback(Some(Arc::clone(&fallback) as ArcAction));
    statement_tracker.set_action_on_exceed(Some(Arc::clone(&action) as ArcAction));
    rc.add(int64_chunk(4)).expect("initial spill");
    assert!(rc.already_spilled());

    let (claim_started, claim_release) = pause_next_fallback_claim(&rc);
    let waiting_action = Arc::clone(&action);
    let waiting_tracker = Arc::clone(&statement_tracker);
    let action_handle = std::thread::spawn(move || waiting_action.action(&waiting_tracker));
    claim_started.wait();

    rc.reset();
    assert_eq!(rc.phase(), CoordinatorPhase::MemoryIdle);
    claim_release.wait();
    action_handle.join().expect("new-generation action");

    assert!(rc.already_spilled(), "the re-entered first action spills");
    assert_eq!(
        fallback.calls.load(SeqCst),
        0,
        "the old-generation fallback must not run"
    );
    unrelated_tracker.consume(-2);
    rc.close();
}

/// If a later action claims fallback first, reset waits for that callback
/// to finish before it closes storage and publishes the next generation.
#[test]
fn fallback_wins_the_race_with_reset() {
    let mut rc = RowContainer::new(&int64_fields(), 4, crate::test_temp_storage::storage());
    let statement_tracker = Tracker::new(-1, 1);
    rc.mem_tracker().attach_to(&statement_tracker);
    let unrelated_tracker = Tracker::new(-2, -1);
    unrelated_tracker.attach_to(&statement_tracker);
    unrelated_tracker.consume(2);
    let action = rc.action_spill();
    let fallback_started = Arc::new(Barrier::new(2));
    let fallback_release = Arc::new(Barrier::new(2));
    let fallback = Arc::new(PausingFallback {
        base: BaseOomAction::default(),
        calls: AtomicUsize::new(0),
        started: Arc::clone(&fallback_started),
        release: Arc::clone(&fallback_release),
    });
    action.set_fallback(Some(Arc::clone(&fallback) as ArcAction));
    statement_tracker.set_action_on_exceed(Some(Arc::clone(&action) as ArcAction));
    rc.add(int64_chunk(4)).expect("initial spill");
    assert!(rc.already_spilled());

    let waiting_action = Arc::clone(&action);
    let waiting_tracker = Arc::clone(&statement_tracker);
    let action_handle = std::thread::spawn(move || waiting_action.action(&waiting_tracker));
    fallback_started.wait();

    let mut resetting = rc.shallow_copy();
    let (reset_tx, reset_rx) = mpsc::channel();
    let reset_handle = std::thread::spawn(move || {
        resetting.reset();
        reset_tx.send(()).expect("report reset");
    });
    assert_eq!(reset_rx.try_recv(), Err(mpsc::TryRecvError::Empty));

    fallback_release.wait();
    action_handle.join().expect("fallback action");
    reset_handle.join().expect("reset after fallback");
    reset_rx.recv().expect("reset completion");

    assert_eq!(fallback.calls.load(SeqCst), 1);
    assert_eq!(rc.phase(), CoordinatorPhase::MemoryIdle);
    assert!(!rc.already_spilled());
    unrelated_tracker.consume(-2);
    rc.close();
}

/// Go `TestRowContainerResetAndAction`: after a reset the container spills
/// again, which only works if the action's `once` was re-armed.
#[test]
fn a_reset_container_spills_again() {
    let fields = int64_fields();
    let sz = 20;
    let mut rc = RowContainer::new(&fields, sz, crate::test_temp_storage::storage());
    let chk = int64_chunk(sz);
    let action = rc.action_spill();
    rc.mem_tracker().set_bytes_limit(chk.memory_usage() + 1);
    rc.mem_tracker()
        .fallback_old_and_set_new_action(Arc::clone(&action) as ArcAction);

    rc.add(chk.clone()).expect("add");
    assert_eq!(rc.disk_tracker().bytes_consumed(), 0);
    rc.add(chk.clone()).expect("add");
    assert!(rc.disk_tracker().bytes_consumed() > 0);

    rc.reset();
    assert_eq!(rc.disk_tracker().bytes_consumed(), 0);
    assert!(!rc.already_spilled());
    assert_eq!(action.status(), SpillStatus::NotSpilled);

    rc.add(chk.clone()).expect("add");
    rc.add(chk.clone()).expect("add");
    assert!(rc.disk_tracker().bytes_consumed() > 0);
}

/// Go `TestActionBlocked`, case 1: ten adds under a small quota end with
/// the action in `spilledYet`, the memory released, and disk in use.
#[test]
fn ten_adds_under_quota_end_spilled_with_the_memory_released() {
    let fields = int64_fields();
    let sz = 4;
    let mut rc = RowContainer::new(&fields, sz, crate::test_temp_storage::storage());
    let action = rc.action_spill();
    rc.mem_tracker().set_bytes_limit(1450);
    rc.mem_tracker()
        .fallback_old_and_set_new_action(Arc::clone(&action) as ArcAction);
    for _ in 0..10 {
        rc.add(int64_chunk(sz)).expect("add");
    }
    assert_eq!(action.status(), SpillStatus::SpilledYet);
    assert_eq!(rc.mem_tracker().bytes_consumed(), 0);
    assert!(rc.mem_tracker().max_consumed() > 0);
    assert!(rc.disk_tracker().bytes_consumed() > 0);
}

/// Go `TestActionBlocked`, case 2: an action that arrives while a spill is
/// in flight WAITS for it instead of falling through to the fallback,
/// because the memory is about to be released.
#[test]
fn an_action_blocks_while_a_spill_is_in_flight() {
    let mut rc = RowContainer::new(&int64_fields(), 4, crate::test_temp_storage::storage());
    rc.add(int64_chunk(4)).expect("add");
    let tracker = Arc::clone(rc.mem_tracker());
    let action = rc.action_spill();
    let (started, release) = pause_next_spill(&rc);
    let mut spilling = rc.shallow_copy();
    let spill_handle = std::thread::spawn(move || spilling.spill_to_disk());
    started.wait();

    let (done_tx, done_rx) = mpsc::channel();
    let waiting_action = Arc::clone(&action);
    let action_handle = std::thread::spawn(move || {
        waiting_action.action(&tracker);
        done_tx.send(()).expect("report completion");
    });
    assert_eq!(done_rx.try_recv(), Err(mpsc::TryRecvError::Empty));

    release.wait();
    spill_handle.join().expect("spill thread");
    action_handle.join().expect("action thread");
    done_rx.recv().expect("action completion");
    rc.set_spill_start_hook(None);
}

/// Go `TestSpillActionDeadLock`: an action firing CONCURRENTLY with `Add`
/// must not deadlock. Go needs a goroutine to avoid taking the write lock
/// under the caller's read lock. Here the reentrant action only arms the
/// coordinator; `add` releases records before it performs the spill.
#[test]
fn a_concurrent_action_and_add_do_not_deadlock() {
    let fields = int64_fields();
    let sz = 4;
    let mut rc = RowContainer::new(&fields, sz, crate::test_temp_storage::storage());
    let tracker = Arc::clone(rc.mem_tracker());
    let action = rc.action_spill();
    rc.mem_tracker().set_bytes_limit(1);
    rc.mem_tracker()
        .fallback_old_and_set_new_action(Arc::clone(&action) as ArcAction);

    let hammer = Arc::clone(&action);
    let hammer_tracker = Arc::clone(&tracker);
    let handle = std::thread::spawn(move || {
        for _ in 0..100 {
            hammer.action(&hammer_tracker);
        }
    });
    rc.add(int64_chunk(sz)).expect("add");
    handle.join().expect("the action thread must finish");
    assert!(rc.already_spilled());
}

/// Shallow handles share records and synchronization rather than closing
/// or snapshotting one another's state.
#[test]
fn shallow_copy_observes_spill_reset_and_close() {
    let mut rc = RowContainer::new(&int64_fields(), 4, crate::test_temp_storage::storage());
    rc.add(int64_chunk(4)).expect("first add");
    rc.add(int64_chunk(4)).expect("second add");
    let mut copy = rc.shallow_copy();
    assert!(Arc::ptr_eq(&rc.shared, &copy.shared));

    let reading = copy.shallow_copy();
    let reader = std::thread::spawn(move || iterate(&reading));
    rc.spill_to_disk();
    assert_eq!(reader.join().expect("reader"), vec![0, 1, 2, 3, 0, 1, 2, 3]);
    assert!(copy.already_spilled());

    copy.reset();
    assert_eq!(rc.phase(), CoordinatorPhase::MemoryIdle);
    assert_eq!(rc.num_row(), 0);
    rc.add(int64_chunk(2)).expect("add after reset");
    assert_eq!(iterate(&copy), vec![0, 1]);

    rc.close();
    assert_eq!(copy.phase(), CoordinatorPhase::Closed);
    assert!(copy.add(int64_chunk(1)).is_err());
}

/// Reset and close claim lifecycle phases only after an active spill has
/// published its terminal disk phase; neither may strand a waiter.
#[test]
fn reset_and_close_serialize_with_spill() {
    let mut reset_rc = RowContainer::new(&int64_fields(), 4, crate::test_temp_storage::storage());
    reset_rc.add(int64_chunk(4)).expect("reset add");
    let (started, release) = pause_next_spill(&reset_rc);
    let mut spilling = reset_rc.shallow_copy();
    let spill_handle = std::thread::spawn(move || spilling.spill_to_disk());
    started.wait();
    let (reset_tx, reset_rx) = mpsc::channel();
    let mut resetting = reset_rc.shallow_copy();
    let reset_handle = std::thread::spawn(move || {
        resetting.reset();
        reset_tx.send(()).expect("report reset");
    });
    assert_eq!(reset_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
    release.wait();
    spill_handle.join().expect("spill before reset");
    reset_handle.join().expect("reset thread");
    reset_rx.recv().expect("reset completion");
    reset_rc.set_spill_start_hook(None);
    assert_eq!(reset_rc.phase(), CoordinatorPhase::MemoryIdle);
    assert!(!reset_rc.already_spilled());

    let mut close_rc = RowContainer::new(&int64_fields(), 4, crate::test_temp_storage::storage());
    close_rc.add(int64_chunk(4)).expect("close add");
    let (started, release) = pause_next_spill(&close_rc);
    let mut spilling = close_rc.shallow_copy();
    let spill_handle = std::thread::spawn(move || spilling.spill_to_disk());
    started.wait();
    let (close_tx, close_rx) = mpsc::channel();
    let mut closing = close_rc.shallow_copy();
    let close_handle = std::thread::spawn(move || {
        closing.close();
        close_tx.send(()).expect("report close");
    });
    assert_eq!(close_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
    release.wait();
    spill_handle.join().expect("spill before close");
    close_handle.join().expect("close thread");
    close_rx.recv().expect("close completion");
    assert_eq!(close_rc.phase(), CoordinatorPhase::Closed);
    assert_eq!(close_rc.mem_tracker().bytes_consumed(), 0);
    assert_eq!(close_rc.disk_tracker().bytes_consumed(), 0);
}

/// The iterator's cursor protocol, on a container that never spills.
#[test]
fn the_iterator_walks_an_in_memory_container() {
    let fields = int64_fields();
    let mut rc = RowContainer::new(&fields, 4, crate::test_temp_storage::storage());
    rc.add(int64_chunk(4)).expect("add");
    rc.add(int64_chunk(4)).expect("add");

    let mut it = Iterator4RowContainer::new(&rc);
    assert_eq!(it.len(), 8);
    assert_eq!(it.begin().expect("first").get_int64(0), 0);
    let mut seen = 1;
    while it.next_row().is_some() {
        seen += 1;
    }
    assert_eq!(seen, 8);
    // Past the end the cursor stays parked.
    assert!(it.current().is_none());
    assert!(it.next_row().is_none());
}

/// Go `TestInterruptedDuringSpilling`: a KILL raised while a long spill is
/// running is noticed, because the spill loop polls the session killer
/// after every chunk.
///
/// Go proves it by timing -- 102400 chunks, a kill after 200ms, and the
/// spill must stop inside a second. The rule under the timing is the
/// per-chunk poll, so it is checked directly here: the signal is pending
/// before the spill starts, and the first poll must raise it.
#[test]
fn a_kill_signal_stops_a_spill_in_progress() {
    let root = Tracker::new(-1, -1);
    root.is_root_tracker_of_sess
        .store(true, std::sync::atomic::Ordering::SeqCst);
    root.killer.conn_id.store(1, SeqCst);

    let fields = int64_fields();
    let mut rc = RowContainer::new(&fields, 20, crate::test_temp_storage::storage());
    rc.mem_tracker().attach_to(&root);
    rc.add(int64_chunk(20)).expect("add");
    rc.add(int64_chunk(20)).expect("add");

    root.killer
        .send_kill_signal(tidb_util::sqlkiller::KillSignal::QueryInterrupted);
    rc.spill_to_disk();
    // Go recovers the kill panic inside `spillToDisk` and leaves it in
    // `spillError`, which every later read reports.
    let error = rc.spill_error().expect("the kill must abort the spill");
    assert!(
        error.contains("1317") || error.to_lowercase().contains("interrupt"),
        "{error}"
    );
    let mut chk = Chunk::new_with_capacity(&fields, 1);
    assert!(rc
        .get_row_and_always_append_to_chunk(RowPtr::new(0, 0), &mut chk)
        .is_err());

    rc.reset();
    assert_eq!(
        rc.spill_error().as_deref(),
        Some(error.as_str()),
        "Go preserves records.spillError across reset"
    );
    rc.spill_to_disk();
    assert_eq!(rc.spill_error().as_deref(), Some(error.as_str()));
}

/// Go `TestPanicWhenSpillToDisk`: the first disk-quota failure is stored
/// on the shared row-container record. Reads and later adds replay that
/// same error instead of treating the partial spill as an empty container
/// or attempting another write.
#[test]
fn disk_quota_failure_is_sticky_for_reads_and_later_adds() {
    let storage = crate::test_temp_storage::isolated_storage_with_quota(
        "row-container-quota",
        tidb_util::disk::SpillEncryptionMethod::Plaintext,
        1,
    );
    let fields = int64_fields();
    let mut rc = RowContainer::new(&fields, 4, Arc::clone(&storage));
    rc.disk_tracker()
        .attach_to_global_tracker(storage.global_tracker());
    rc.add(int64_chunk(4)).expect("memory add");

    rc.spill_to_disk();
    let stored = rc.spill_error().expect("quota failure must be stored");
    assert_eq!(stored, tidb_util::disk::LOCAL_TEMPORARY_SPACE_QUOTA_ERROR);
    assert!(rc.already_spilled(), "the disk authority was installed");

    let mut scratch = Chunk::new_with_capacity(&fields, 1);
    let conditional =
        match rc.get_row_and_append_to_chunk_if_in_disk(RowPtr::new(0, 0), &mut scratch) {
            Ok(_) => panic!("conditional reads must replay the spill failure"),
            Err(error) => error,
        };
    assert_eq!(conditional.to_string(), stored);
    assert_eq!(scratch.num_rows(), 0, "an error must not modify scratch");
    let read = rc
        .get_row_and_always_append_to_chunk(RowPtr::new(0, 0), &mut scratch)
        .expect_err("reads replay the spill failure");
    assert_eq!(read.to_string(), stored);
    let add = rc
        .add(int64_chunk(1))
        .expect_err("later adds replay the spill failure");
    assert_eq!(add.to_string(), stored);
    assert_eq!(rc.spill_error().as_deref(), Some(stored.as_str()));

    rc.close();
    assert_eq!(storage.global_tracker().bytes_consumed(), 0);
}
