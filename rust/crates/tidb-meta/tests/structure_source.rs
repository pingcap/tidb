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

//! Go `pkg/structure/structure_test.go`, run over the deterministic raw
//! transaction instead of a mock TiKV store. The Go suite drives one
//! `kv.Transaction` through `NewStructure(txn, txn, prefix)` and a read-only
//! `NewStructure(txn, nil, prefix)`; here those are [`TxStructure::new`] and
//! [`TxStructure::read_only`] over one [`MemoryTransaction`].

use tidb_meta::structure::{HashPair, TxStructure};
use tidb_meta::transaction::MemoryTransaction;
use tidb_meta::MetaError;

// Go `TestString`.
#[test]
fn string_operations_cover_set_get_inc_iterate_clear_and_snapshot_refusal() {
    let mut transaction = MemoryTransaction::default();
    let mut tx = TxStructure::new(&mut transaction, &[0x00]);

    let key = b"a";
    let value = b"1";
    tx.set(key, value).unwrap();

    let mut visited = Vec::new();
    tx.iterate(b"a", b"b", &mut |k, v| {
        visited.push((k.to_vec(), v.to_vec()));
        Ok(())
    })
    .unwrap();
    assert_eq!(visited, vec![(key.to_vec(), value.to_vec())]);

    assert_eq!(tx.get(key).unwrap().as_deref(), Some(value.as_slice()));

    assert_eq!(tx.inc(key, 1).unwrap(), 2);
    assert_eq!(tx.get(key).unwrap().as_deref(), Some(b"2".as_slice()));
    assert_eq!(tx.get_int64(key).unwrap(), 2);

    tx.clear(key).unwrap();
    assert_eq!(tx.get(key).unwrap(), None);

    let mut tx1 = TxStructure::read_only(&mut transaction, &[0x01]);
    assert_eq!(tx1.set(key, value), Err(MetaError::WriteOnSnapshot));
    assert_eq!(tx1.inc(key, 1), Err(MetaError::WriteOnSnapshot));
    assert_eq!(tx1.clear(key), Err(MetaError::WriteOnSnapshot));
}

// Go `TestList`.
#[test]
fn list_operations_cover_push_pop_index_set_clear_and_snapshot_refusal() {
    let mut transaction = MemoryTransaction::default();
    let mut tx = TxStructure::new(&mut transaction, &[0x00]);

    let key = b"a";
    tx.lpush(key, &[b"3", b"2", b"1"]).unwrap();

    // Test LGetAll.
    tx.lpush(key, &[b"11"]).unwrap();
    assert_eq!(
        tx.lget_all(key).unwrap(),
        vec![b"3".to_vec(), b"2".to_vec(), b"1".to_vec(), b"11".to_vec()]
    );
    assert_eq!(tx.lpop(key).unwrap().as_deref(), Some(b"11".as_slice()));

    assert_eq!(tx.llen(key).unwrap(), 3);

    assert_eq!(tx.lindex(key, 1).unwrap().as_deref(), Some(b"2".as_slice()));

    tx.lset(key, 1, b"4").unwrap();
    assert_eq!(tx.lindex(key, 1).unwrap().as_deref(), Some(b"4".as_slice()));
    tx.lset(key, 1, b"2").unwrap();

    // Go `LSet(key, 100, ...)`: the error reports the adjusted index.
    assert_eq!(
        tx.lset(key, 100, b"2"),
        Err(MetaError::InvalidListIndex(97))
    );

    assert_eq!(
        tx.lindex(key, -1).unwrap().as_deref(),
        Some(b"3".as_slice())
    );

    assert_eq!(tx.lpop(key).unwrap().as_deref(), Some(b"1".as_slice()));
    assert_eq!(tx.llen(key).unwrap(), 2);

    tx.rpush(key, &[b"4"]).unwrap();
    assert_eq!(tx.llen(key).unwrap(), 3);
    assert_eq!(
        tx.lindex(key, -1).unwrap().as_deref(),
        Some(b"4".as_slice())
    );

    assert_eq!(tx.rpop(key).unwrap().as_deref(), Some(b"4".as_slice()));
    assert_eq!(tx.rpop(key).unwrap().as_deref(), Some(b"3".as_slice()));
    assert_eq!(tx.rpop(key).unwrap().as_deref(), Some(b"2".as_slice()));
    assert_eq!(tx.llen(key).unwrap(), 0);

    tx.lpush(key, &[b"1"]).unwrap();
    tx.lclear(key).unwrap();
    assert_eq!(tx.llen(key).unwrap(), 0);

    let mut tx1 = TxStructure::read_only(&mut transaction, &[0x01]);
    assert_eq!(tx1.lpush(key, &[b"1"]), Err(MetaError::WriteOnSnapshot));
    assert_eq!(tx1.rpop(key), Err(MetaError::WriteOnSnapshot));
    assert_eq!(tx1.lset(key, 1, b"2"), Err(MetaError::WriteOnSnapshot));
    assert_eq!(tx1.lclear(key), Err(MetaError::WriteOnSnapshot));
}

// Go `TestHash`.
#[test]
fn hash_operations_cover_set_get_iterate_last_n_inc_del_clear_and_nil_values() {
    let mut transaction = MemoryTransaction::default();
    let mut tx = TxStructure::new(&mut transaction, &[0x00]);

    let key = b"a";

    let _ = tx.encode_hash_auto_id_key_value(key, key, 5);

    tx.hset(key, b"1", b"1").unwrap();
    tx.hset(key, b"2", b"2").unwrap();

    assert_eq!(
        tx.hget(key, b"1").unwrap().as_deref(),
        Some(b"1".as_slice())
    );
    assert_eq!(tx.hget(key, b"fake").unwrap(), None);

    assert_eq!(tx.hkeys(key).unwrap(), vec![b"1".to_vec(), b"2".to_vec()]);

    let all = tx.hget_all(key).unwrap();
    assert_eq!(
        all,
        vec![
            HashPair {
                field: b"1".to_vec(),
                value: b"1".to_vec()
            },
            HashPair {
                field: b"2".to_vec(),
                value: b"2".to_vec()
            }
        ]
    );

    let mut index = 0;
    tx.hget_iter(key, &mut |pair| {
        assert!(index < 2);
        assert_eq!(all[index], pair);
        index += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(tx.hget_len(key).unwrap(), 2);

    assert_eq!(
        tx.hget_last_n(key, 1).unwrap(),
        vec![HashPair {
            field: b"2".to_vec(),
            value: b"2".to_vec()
        }]
    );
    assert_eq!(
        tx.hget_last_n(key, 2).unwrap(),
        vec![
            HashPair {
                field: b"2".to_vec(),
                value: b"2".to_vec()
            },
            HashPair {
                field: b"1".to_vec(),
                value: b"1".to_vec()
            }
        ]
    );

    tx.hdel(key, &[b"1"]).unwrap();
    assert_eq!(tx.hget(key, b"1").unwrap(), None);

    assert_eq!(tx.hinc(key, b"1", 1).unwrap(), 1);

    // Test set new value which equals to old value.
    assert_eq!(
        tx.hget(key, b"1").unwrap().as_deref(),
        Some(b"1".as_slice())
    );
    tx.hset(key, b"1", b"1").unwrap();
    assert_eq!(
        tx.hget(key, b"1").unwrap().as_deref(),
        Some(b"1".as_slice())
    );

    assert_eq!(tx.hinc(key, b"1", 1).unwrap(), 2);
    assert_eq!(tx.hinc(key, b"1", 1).unwrap(), 3);
    assert_eq!(tx.hget_int64(key, b"1").unwrap(), 3);

    tx.hclear(key).unwrap();
    tx.hdel(key, &[b"fake_key"]).unwrap();

    // Test set nil value.
    assert_eq!(tx.hget(key, b"nil_key").unwrap(), None);
    tx.hset(key, b"nil_key", b"").unwrap();
    tx.hset(key, b"nil_key", b"1").unwrap();
    assert_eq!(
        tx.hget(key, b"nil_key").unwrap().as_deref(),
        Some(b"1".as_slice())
    );
    // Writing nil over a stored value reaches Go's kv layer and fails there.
    assert_eq!(
        tx.hset(key, b"nil_key", b""),
        Err(MetaError::CannotSetNilValue)
    );
    assert_eq!(
        tx.hget(key, b"nil_key").unwrap().as_deref(),
        Some(b"1".as_slice())
    );
    tx.hset(key, b"nil_key", b"2").unwrap();
    assert_eq!(
        tx.hget(key, b"nil_key").unwrap().as_deref(),
        Some(b"2".as_slice())
    );

    let mut tx1 = TxStructure::read_only(&mut transaction, &[0x01]);
    assert_eq!(tx1.hinc(key, b"1", 1), Err(MetaError::WriteOnSnapshot));
    assert_eq!(tx1.hdel(key, &[b"1"]), Err(MetaError::WriteOnSnapshot));

    // Go commits and reuses the store in a fresh transaction.
    let mut new_transaction = MemoryTransaction::default();
    let mut new_tx = TxStructure::new(&mut new_transaction, &[0x00]);
    new_tx.set(key, b"abc").unwrap();
    assert_eq!(new_tx.get(key).unwrap().as_deref(), Some(b"abc".as_slice()));
}

// Go `TestError`: every structure error carries its own code, not ErrUnknown.
#[test]
fn structure_errors_expose_their_source_error_codes() {
    const ERR_UNKNOWN: u16 = 1105;
    for (error, code) in [
        (MetaError::UnexpectedTypeFlag(b'x'), 8217),
        (MetaError::InvalidListIndex(0), 8218),
        (MetaError::InvalidListMetaData, 8219),
        (MetaError::WriteOnSnapshot, 8220),
    ] {
        assert_eq!(error.code(), Some(code), "err: {error}");
        assert_ne!(error.code(), Some(ERR_UNKNOWN));
    }
}

// Go `TestIterateHashWithBoundedKey`: undecodable keys inside the bound are
// skipped, decodable hash entries are visited.
#[test]
fn bounded_hash_iteration_skips_undecodable_keys() {
    let mut transaction = MemoryTransaction::default();
    let mut tx = TxStructure::new(&mut transaction, &[]);

    let meta_aa = tx.encode_hash_meta_key(b"aa");
    tx.set(&meta_aa, b"meta").unwrap();
    tx.hset(b"b", b"", b"value1").unwrap();
    let meta_c = tx.encode_hash_meta_key(b"c");
    tx.set(&meta_c, b"meta").unwrap();
    tx.hset(b"d", b"", b"value2").unwrap();
    let meta_dd = tx.encode_hash_meta_key(b"dd");
    tx.set(&meta_dd, b"meta").unwrap();

    let mut count = 0;
    tx.iterate_hash_with_bounded_key(b"a", b"e", &mut |_, _, _| {
        count += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(count, 2);
}
