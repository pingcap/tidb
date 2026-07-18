// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(missing_docs)]

#[path = "../src/lock/model.rs"]
mod model;

use model::{decode_lock_observation, LockAdmissionError};
use tidb_proto::KvrpcLockInfo;

fn optimistic(key: &[u8], primary: &[u8], txn_id: u64) -> KvrpcLockInfo {
    KvrpcLockInfo {
        key: key.to_vec(),
        primary_lock: primary.to_vec(),
        lock_version: txn_id,
        lock_ttl: 3_000,
        txn_size: 2,
        lock_type: 0,
        min_commit_ts: txn_id + 1,
        ..KvrpcLockInfo::default()
    }
}

#[test]
fn maps_single_and_shared_lock_info_without_wrapper_fields() {
    let single = decode_lock_observation(&optimistic(b"s", b"p", 11)).unwrap();
    assert_eq!(single.len(), 1);
    assert_eq!(single[0].key, b"s");
    assert_eq!(single[0].primary, b"p");
    assert_eq!(single[0].txn_id, 11);
    assert_eq!(single[0].ttl_ms, 3_000);
    assert_eq!(single[0].txn_size, 2);
    assert_eq!(single[0].min_commit_ts, 12);

    let wrapper = KvrpcLockInfo {
        key: b"ignored-wrapper-key".to_vec(),
        lock_type: 7,
        shared_lock_infos: vec![optimistic(b"s1", b"p1", 21), optimistic(b"s2", b"p2", 22)],
        ..KvrpcLockInfo::default()
    };
    let shared = decode_lock_observation(&wrapper).unwrap();
    assert_eq!(shared.len(), 2);
    assert_eq!(shared[0].key, b"s1");
    assert_eq!(shared[1].key, b"s2");
}

#[test]
fn fails_closed_for_every_protocol_outside_the_bounded_path() {
    let mut lock = optimistic(b"s", b"p", 11);
    lock.lock_type = 5;
    assert_eq!(
        decode_lock_observation(&lock),
        Err(LockAdmissionError::Pessimistic(5))
    );

    lock = optimistic(b"s", b"p", 11);
    lock.use_async_commit = true;
    assert_eq!(
        decode_lock_observation(&lock),
        Err(LockAdmissionError::AsyncCommit)
    );

    lock = optimistic(b"s", b"p", 11);
    lock.is_txn_file = true;
    assert_eq!(
        decode_lock_observation(&lock),
        Err(LockAdmissionError::TransactionFile)
    );

    lock = optimistic(b"s", b"p", 11);
    lock.lock_type = 7;
    assert_eq!(
        decode_lock_observation(&lock),
        Err(LockAdmissionError::UnsupportedLockType(7))
    );

    lock = optimistic(b"s", b"p", 11);
    lock.primary_lock.clear();
    assert_eq!(
        decode_lock_observation(&lock),
        Err(LockAdmissionError::MissingIdentity)
    );
}
