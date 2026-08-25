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

//! Connected source tests for `pkg/store/driver/txn/unionstore_driver.go` over
//! the real client-go transaction buffer.
//!
//! `mutable_transaction_runtime_source.rs` proves the driver's delegation
//! contract against a hand-written fake. These tests prove the same TiDB-facing
//! behavior over `TikvMemBufferBackend`, whose staging, tombstone, flag, and
//! snapshot semantics come from the transcreated client-go `MemDB` in the
//! vendored `tikv-client` crate — the composition Go ships in production.

use tikv_client::transaction::unionstore::MemDb;
use tidb_txnkv::{
    AssertionOp, BatchGetOptions, FlagsOp, Getter, GetOptions, Key, KvIterator, MemBufferBackend,
    MemBufferDriver, TikvMemBufferBackend, TikvMemBufferError,
};

fn k(value: &str) -> Key {
    Key::from(value.as_bytes().to_vec())
}

fn drain<I: KvIterator<Error = TikvMemBufferError>>(iterator: &mut I) -> Vec<(Key, Vec<u8>)> {
    let mut entries = Vec::new();
    while iterator.valid() {
        entries.push((iterator.key().clone(), iterator.value().to_vec()));
        iterator.next().expect("iterator advances");
    }
    entries
}

#[test]
fn reads_writes_and_tombstones_match_the_fake_backend_contract() {
    let mut memdb = MemDb::new();
    let mut backend = TikvMemBufferBackend::new(&mut memdb);
    assert!(backend.is_empty());

    backend.set(k("a"), b"1".to_vec()).unwrap();
    backend.set(k("b"), b"2".to_vec()).unwrap();
    backend.delete(k("b")).unwrap();

    // A live value and a tombstone are both visible buffer entries.
    assert_eq!(backend.len(), 2);
    let live = backend.get(&k("a"), GetOptions::default()).unwrap();
    assert_eq!(live.value, b"1".to_vec());
    // The buffer never supplies a commit timestamp, even when requested.
    let with_ts = backend
        .get(&k("a"), GetOptions::with_return_commit_ts())
        .unwrap();
    assert_eq!(with_ts.commit_ts, 0);
    let tombstone = backend.get(&k("b"), GetOptions::default()).unwrap();
    assert!(tombstone.is_value_empty());

    // Missing keys report the canonical not-found identity.
    let missing = backend.get(&k("c"), GetOptions::default()).unwrap_err();
    assert_eq!(missing, TikvMemBufferError::NotFound);

    // Batch reads omit absent keys instead of failing.
    let batch = backend
        .batch_get(&[k("a"), k("b"), k("c")], BatchGetOptions::default())
        .unwrap();
    assert_eq!(batch.len(), 2);
    assert_eq!(batch[&k("a")].value, b"1".to_vec());
    assert!(batch[&k("b")].is_value_empty());

    // Client-go rejects an empty value through Set: deletions use tombstones.
    assert_eq!(
        backend.set(k("a"), Vec::new()).unwrap_err(),
        TikvMemBufferError::CannotSetNilValue
    );

    // RemoveFromBuffer erases the record entirely instead of writing a
    // tombstone.
    backend.remove_from_buffer(&k("a"));
    assert_eq!(backend.len(), 1);
    assert_eq!(
        backend.get(&k("a"), GetOptions::default()).unwrap_err(),
        TikvMemBufferError::NotFound
    );

    // GetLocal reads buffer bytes without a transaction snapshot.
    assert_eq!(backend.get_local(&k("b")).unwrap(), Vec::<u8>::new());
    assert_eq!(
        backend.get_local(&k("a")).unwrap_err(),
        TikvMemBufferError::NotFound
    );
}

#[test]
fn flags_round_trip_through_the_client_go_bit_layout() {
    let mut memdb = MemDb::new();
    let mut backend = TikvMemBufferBackend::new(&mut memdb);

    backend
        .set_with_flags(k("a"), b"1".to_vec(), &[FlagsOp::SetPresumeKeyNotExists])
        .unwrap();
    let flags = backend.get_flags(&k("a")).unwrap();
    assert!(flags.has_presume_key_not_exists());
    assert!(!flags.has_need_locked());

    backend.update_flags(&k("a"), &[FlagsOp::SetNeedLocked]);
    backend.update_flags(&k("a"), &[FlagsOp::SetNeedConstraintCheckInPrewrite]);
    backend.update_flags(&k("a"), &[FlagsOp::SetPreviousPresumeKeyNotExists]);
    backend.update_assertion_flags(&k("a"), AssertionOp::AssertNotExist);

    let flags = backend.get_flags(&k("a")).unwrap();
    assert!(flags.has_presume_key_not_exists());
    assert!(flags.has_need_locked());
    assert!(flags.has_need_constraint_check_in_prewrite());
    assert!(flags.has_assert_not_exists());

    // Assertion replacement, not accumulation, exactly like the source.
    backend.update_assertion_flags(&k("a"), AssertionOp::AssertExist);
    assert!(backend.get_flags(&k("a")).unwrap().has_assert_exists());
    backend.update_assertion_flags(&k("a"), AssertionOp::AssertNone);
    assert!(!backend.get_flags(&k("a")).unwrap().has_assertion_flags());

    // A tombstone key carries flags too.
    backend
        .delete_with_flags(k("d"), &[FlagsOp::SetNeedLocked])
        .unwrap();
    assert!(backend.get_flags(&k("d")).unwrap().has_need_locked());

    // Flags for a missing key report not-found like Go MemBuffer.GetFlags.
    assert_eq!(
        backend.get_flags(&k("missing")).unwrap_err(),
        TikvMemBufferError::NotFound
    );
}

#[test]
fn staging_rolls_back_releases_and_exposes_touched_entries() {
    let mut memdb = MemDb::new();
    let mut backend = TikvMemBufferBackend::new(&mut memdb);
    backend.set(k("base"), b"0".to_vec()).unwrap();

    // A cleaned-up stage rolls its writes back.
    let stage = backend.staging();
    backend.set(k("base"), b"dirty".to_vec()).unwrap();
    backend.set(k("staged"), b"1".to_vec()).unwrap();
    backend.cleanup(stage);
    assert_eq!(
        backend.get(&k("base"), GetOptions::default()).unwrap().value,
        b"0".to_vec()
    );
    assert_eq!(
        backend.get(&k("staged"), GetOptions::default()).unwrap_err(),
        TikvMemBufferError::NotFound
    );

    // A released stage keeps its writes, and inspect_stage visits exactly the
    // entries the stage touched, with their flags.
    let stage = backend.staging();
    backend
        .set_with_flags(k("staged"), b"2".to_vec(), &[FlagsOp::SetPresumeKeyNotExists])
        .unwrap();
    let mut inspected = Vec::new();
    backend.inspect_stage(stage, &mut |key, flags, value| {
        inspected.push((key.clone(), flags, value.to_vec()));
    });
    assert_eq!(inspected.len(), 1);
    assert_eq!(inspected[0].0, k("staged"));
    assert!(inspected[0].1.has_presume_key_not_exists());
    assert_eq!(inspected[0].2, b"2".to_vec());
    backend.release(stage);
    assert_eq!(
        backend.get(&k("staged"), GetOptions::default()).unwrap().value,
        b"2".to_vec()
    );

    // Nested stages: an inner cleanup does not disturb outer writes.
    let outer = backend.staging();
    backend.set(k("outer"), b"o".to_vec()).unwrap();
    let inner = backend.staging();
    backend.set(k("inner"), b"i".to_vec()).unwrap();
    backend.cleanup(inner);
    assert_eq!(
        backend.get(&k("inner"), GetOptions::default()).unwrap_err(),
        TikvMemBufferError::NotFound
    );
    backend.release(outer);
    assert_eq!(
        backend.get(&k("outer"), GetOptions::default()).unwrap().value,
        b"o".to_vec()
    );
}

#[test]
fn iteration_bounds_match_the_source_driver_contract() {
    let mut memdb = MemDb::new();
    let mut backend = TikvMemBufferBackend::new(&mut memdb);
    for (key, value) in [("a", "1"), ("b", "2"), ("c", "3"), ("d", "4")] {
        backend.set(k(key), value.as_bytes().to_vec()).unwrap();
    }
    backend.delete(k("b")).unwrap();

    // Forward: [start, upper), tombstones included.
    let mut iterator = backend.iter(&k("a"), Some(&k("d"))).unwrap();
    let entries = drain(&mut iterator);
    assert_eq!(
        entries,
        vec![
            (k("a"), b"1".to_vec()),
            (k("b"), Vec::new()),
            (k("c"), b"3".to_vec()),
        ]
    );

    // Reverse: [lower, start), descending.
    let mut iterator = backend.iter_reverse(Some(&k("d")), Some(&k("b"))).unwrap();
    let entries = drain(&mut iterator);
    assert_eq!(entries, vec![(k("c"), b"3".to_vec()), (k("b"), Vec::new())]);
}

#[test]
fn buffer_snapshots_exclude_the_active_stage_and_pipelined_views_are_empty() {
    let mut memdb = MemDb::new();
    let mut backend = TikvMemBufferBackend::new(&mut memdb);
    backend.set(k("a"), b"committed".to_vec()).unwrap();
    let _stage = backend.staging();
    backend.set(k("a"), b"staged".to_vec()).unwrap();
    backend.set(k("b"), b"staged-only".to_vec()).unwrap();

    // The snapshot getter sees the pre-stage state, like Go SnapshotGetter.
    let mut getter = backend.snapshot_getter();
    assert_eq!(
        getter.get(&k("a"), GetOptions::default()).unwrap().value,
        b"committed".to_vec()
    );
    assert_eq!(
        getter.get(&k("b"), GetOptions::default()).unwrap_err(),
        TikvMemBufferError::NotFound
    );

    // The snapshot iterator sees the same stable view.
    let mut iterator = backend.snapshot_iter(&k("a"), None);
    let entries = drain(&mut iterator);
    assert_eq!(entries, vec![(k("a"), b"committed".to_vec())]);

    // The live iterator, by contrast, sees the staged writes.
    let mut iterator = backend.iter(&k("a"), None).unwrap();
    let entries = drain(&mut iterator);
    assert_eq!(
        entries,
        vec![(k("a"), b"staged".to_vec()), (k("b"), b"staged-only".to_vec())]
    );

    // Pipelined DML suppresses stable snapshot views through the driver.
    let mut driver = MemBufferDriver::new(backend, true);
    let mut iterator = driver.snapshot_iter(&k("a"), None);
    assert!(!iterator.valid());
    let mut getter = driver.snapshot_getter();
    assert_eq!(
        getter.get(&k("a"), GetOptions::default()).unwrap_err(),
        TikvMemBufferError::NotFound
    );
}
