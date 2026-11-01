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

//! Ported from Go `pkg/table/temptable/interceptor_test.go`.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use super::*;

fn inc_last_byte(key: &[u8]) -> Vec<u8> {
    let mut key = key.to_vec();
    *key.last_mut().unwrap() += 1;
    key
}

fn dec_last_byte(key: &[u8]) -> Vec<u8> {
    let mut key = key.to_vec();
    *key.last_mut().unwrap() -= 1;
    key
}

fn encode_table_key(tbl_id: i64, suffix: &[u8]) -> Vec<u8> {
    let mut key = encode_table_prefix(tbl_id);
    key.extend_from_slice(suffix);
    key
}

#[derive(Debug, Default)]
struct MockedSnapshot(BTreeMap<Vec<u8>, Vec<u8>>);

impl ClusterSnapshot for MockedSnapshot {
    fn get(&mut self, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(self.0.get(key.as_bytes()).cloned())
    }

    fn scan(
        &mut self,
        start: &Key,
        end: &Key,
        limit: Option<usize>,
    ) -> Result<SnapshotPairs, StorageError> {
        Ok(self
            .0
            .range(start.as_bytes().to_vec()..)
            .take_while(|(key, _)| end.as_bytes().is_empty() || key.as_slice() < end.as_bytes())
            .take(limit.unwrap_or(usize::MAX))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }
}

fn snapshot(data: &[(Vec<u8>, &str)]) -> Box<dyn ClusterSnapshot> {
    Box::new(MockedSnapshot(
        data.iter()
            .map(|(key, value)| (key.clone(), value.as_bytes().to_vec()))
            .collect(),
    ))
}

fn session_data(data: &[(Vec<u8>, &str)]) -> Arc<Mutex<TemporaryTableData>> {
    let mut session = TemporaryTableData::new();
    for (key, value) in data {
        session
            .set_table_key(
                decode_table_id(key),
                Key::from_bytes(key.clone()),
                value.as_bytes().to_vec(),
            )
            .unwrap();
    }
    Arc::new(Mutex::new(session))
}

/// Go `newMockedInfoSchema(t).AddTable(None, 1).AddTable(Global, 3).AddTable(Local, 5)`.
fn info_schema() -> TemporaryTableKinds {
    Arc::new(std::sync::RwLock::new(HashMap::from([
        (1, TempTableType::NONE),
        (3, TempTableType::GLOBAL),
        (5, TempTableType::LOCAL),
    ])))
}

#[test]
fn test_get_key_accessed_table_id() {
    let tb_prefix = TABLE_PREFIX.to_vec();
    for key in [
        Vec::new(),
        inc_last_byte(&tb_prefix),
        dec_last_byte(&tb_prefix),
        tb_prefix.clone(),
    ] {
        assert_eq!(get_key_accessed_table_id(&key), None);
    }
    let prefix1 = encode_table_key(1, &[]);
    assert_eq!(get_key_accessed_table_id(&prefix1[..prefix1.len() - 1]), None);
    assert_eq!(get_key_accessed_table_id(&prefix1[..tb_prefix.len() + 1]), None);
    for (tbl_id, ok) in [
        (0, false),
        (i64::MAX, false),
        (-1, false),
        (1, true),
        (i64::MAX / 2, true),
        (i64::MAX - 1, true),
    ] {
        for suffix in [&[][..], &[0], &[1], &[0xFF]] {
            let key = encode_table_key(tbl_id, suffix);
            assert_eq!(get_key_accessed_table_id(&key), ok.then_some(tbl_id), "{tbl_id}");
        }
    }
}

#[test]
fn test_not_table_range() {
    let tk = encode_table_key;
    let false_cases: Vec<(Vec<u8>, Vec<u8>)> = vec![
        (vec![], vec![]),
        (vec![], tk(1, &[0])),
        (vec![], tk(1, &[1])),
        (tk(1, &[]), vec![]),
        (tk(1, &[0]), vec![]),
        (tk(1, &[]), tk(1, &[])),
        (tk(1, &[]), tk(1, &[0])),
        (tk(1, &[]), tk(1, &[1])),
        (tk(1, &[]), tk(2, &[])),
        (tk(1, &[]), tk(2, &[0])),
        (tk(1, &[]), tk(2, &[1])),
        (tk(1, &[0]), tk(1, &[1])),
        (tk(1, &[]), inc_last_byte(TABLE_PREFIX)),
    ];
    for (k, upper) in false_cases {
        assert!(!not_table_range(&k, &upper), "{k:?} {upper:?}");
    }
    let dec = dec_last_byte(TABLE_PREFIX);
    let inc = inc_last_byte(TABLE_PREFIX);
    let true_cases: Vec<(Vec<u8>, Vec<u8>)> = vec![
        (vec![], dec.clone()),
        (dec.clone(), [dec.clone(), vec![1]].concat()),
        (inc.clone(), vec![]),
        (inc.clone(), [inc.clone(), vec![1]].concat()),
    ];
    for (k, upper) in true_cases {
        assert!(not_table_range(&k, &upper), "{k:?} {upper:?}");
    }
}

#[test]
fn test_on_get() {
    let tk = encode_table_key;
    let snap_data = vec![(tk(1, &[]), "v1"), (tk(3, &[]), "snap3"), (tk(5, &[]), "snap5")];
    // The session holding a global temporary table's key must not matter
    // (Go `TestGetSessionTemporaryTableKey`).
    let session = session_data(&[(tk(3, &[]), "session3"), (tk(5, &[]), "v5"), (tk(5, &[1]), "")]);
    let mut interceptor =
        TemporaryTableSnapshotInterceptor::new(snapshot(&snap_data), info_schema(), Some(session));
    let get = |interceptor: &mut TemporaryTableSnapshotInterceptor, key: Vec<u8>| {
        interceptor.get(&Key::from_bytes(key)).unwrap()
    };
    // A normal table reads the snapshot.
    assert_eq!(get(&mut interceptor, tk(1, &[])), Some(b"v1".to_vec()));
    // A global temporary table never reads outside the transaction.
    assert_eq!(get(&mut interceptor, tk(3, &[])), None);
    // A local temporary table reads the session; an empty value is absent.
    assert_eq!(get(&mut interceptor, tk(5, &[])), Some(b"v5".to_vec()));
    assert_eq!(get(&mut interceptor, tk(5, &[1])), None);
    assert_eq!(get(&mut interceptor, tk(5, &[2])), None);

    let keys = [tk(1, &[]), tk(3, &[]), tk(5, &[]), tk(5, &[1])].map(Key::from_bytes);
    let mut pairs = interceptor.batch_get(&keys).unwrap();
    pairs.sort();
    assert_eq!(
        pairs,
        vec![(tk(1, &[]), b"v1".to_vec()), (tk(5, &[]), b"v5".to_vec())]
    );

    // Without session data a local temporary table reads nothing.
    let mut no_session =
        TemporaryTableSnapshotInterceptor::new(snapshot(&snap_data), info_schema(), None);
    assert_eq!(get(&mut no_session, tk(5, &[])), None);
}

#[test]
fn test_on_iter() {
    let tk = encode_table_key;
    let no_temp_table_data = vec![
        (tk(1, &[]), "v1"),
        (tk(1, &[1]), "v11"),
        (tk(2, &[]), "v2"),
        (tk(2, &[1]), "v21"),
        (b"s".to_vec(), "vs"),
        (b"s0".to_vec(), "vs0"),
        (b"u".to_vec(), "vu"),
        (b"u0".to_vec(), "vu0"),
        (TABLE_PREFIX.to_vec(), "vt"),
        (tk(0, &[]), "v0"),
        (tk(0, &[1]), "v01"),
        (tk(i64::MAX, &[]), "vm"),
        (tk(i64::MAX, &[1]), "vm1"),
    ];
    let local_temp_table_data = vec![
        (tk(5, &[]), "v5"),
        (tk(5, &[0]), "v50"),
        (tk(5, &[1]), "v51"),
        (tk(5, &[0, 1]), "v501"),
        (tk(5, &[2]), ""),
        (tk(5, &[3]), ""),
    ];
    let s = |v: &str| v.to_owned();
    let all_normal = vec![
        (b"s".to_vec(), s("vs")),
        (b"s0".to_vec(), s("vs0")),
        (TABLE_PREFIX.to_vec(), s("vt")),
        (tk(0, &[]), s("v0")),
        (tk(0, &[1]), s("v01")),
        (tk(1, &[]), s("v1")),
        (tk(1, &[1]), s("v11")),
        (tk(2, &[]), s("v2")),
        (tk(2, &[1]), s("v21")),
    ];
    let local = vec![
        (tk(5, &[]), s("v5")),
        (tk(5, &[0]), s("v50")),
        (tk(5, &[0, 1]), s("v501")),
        (tk(5, &[1]), s("v51")),
    ];
    let tail = vec![
        (tk(i64::MAX, &[]), s("vm")),
        (tk(i64::MAX, &[1]), s("vm1")),
        (b"u".to_vec(), s("vu")),
        (b"u0".to_vec(), s("vu0")),
    ];
    let full = [all_normal.clone(), local.clone(), tail.clone()].concat();
    let cases: Vec<(bool, Vec<u8>, Vec<u8>, Vec<(Vec<u8>, String)>)> = vec![
        (false, vec![], vec![], full.clone()),
        (false, vec![], b"s0".to_vec(), vec![(b"s".to_vec(), s("vs"))]),
        (false, b"u".to_vec(), vec![], tail[2..].to_vec()),
        (false, tk(1, &[]), vec![], [all_normal[5..].to_vec(), local.clone(), tail.clone()].concat()),
        (false, vec![], tk(1, &[1]), all_normal[..6].to_vec()),
        (false, tk(1, &[]), tk(2, &[]), all_normal[5..7].to_vec()),
        (false, tk(1, &[]), tk(3, &[]), all_normal[5..].to_vec()),
        (false, tk(3, &[]), tk(4, &[]), vec![]),
        (false, tk(5, &[]), tk(5, &[3]), local.clone()),
        (true, vec![], vec![], [all_normal.clone(), tail.clone()].concat()),
        (true, tk(5, &[]), tk(5, &[3]), vec![]),
    ];
    for limit in [None, Some(1), Some(2), Some(3)] {
        for (i, (nil_session, k, upper, expected)) in cases.iter().enumerate() {
            let session = (!nil_session).then(|| session_data(&local_temp_table_data));
            let mut interceptor = TemporaryTableSnapshotInterceptor::new(
                snapshot(&no_temp_table_data),
                info_schema(),
                session,
            );
            // A caller drains a limited range batch by batch.
            let mut result = Vec::new();
            let mut start = k.clone();
            loop {
                let batch = interceptor
                    .scan(&Key::from_bytes(start.clone()), &Key::from_bytes(upper.clone()), limit)
                    .unwrap();
                let drained = limit.is_none_or(|limit| batch.len() < limit);
                if let Some((last, _)) = batch.last() {
                    start = [last.clone(), vec![0]].concat();
                }
                result.extend(batch);
                if drained {
                    break;
                }
            }
            let result: Vec<(Vec<u8>, String)> = result
                .into_iter()
                .map(|(key, value)| (key, String::from_utf8(value).unwrap()))
                .collect();
            assert_eq!(&result, expected, "case {i} limit {limit:?}");
        }
    }
}
