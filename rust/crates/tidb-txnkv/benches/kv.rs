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

//! Executable translations of every benchmark in `pkg/kv/key_test.go`.

#![allow(non_snake_case)]

use std::collections::HashMap;
use std::hint::black_box;
use std::time::Instant;

use tidb_codec::encode_key;
use tidb_datatype::Datum;
use tidb_txnkv::{CommonHandle, Handle, IntHandle, Key, KeyRange, MemAwareHandleMap};

fn measure(name: &str, iterations: usize, mut operation: impl FnMut()) {
    let started = Instant::now();
    for _ in 0..iterations {
        operation();
    }
    println!("{name}: {:?}", started.elapsed());
}

fn handles(size: usize) -> Vec<Handle> {
    (0..size)
        .map(|index| {
            if index % 2 == 0 {
                Handle::from(IntHandle::new(index as i64))
            } else {
                Handle::from(
                    CommonHandle::new(
                        encode_key(&[Datum::new_int(index as i64)]).expect("encode handle"),
                    )
                    .expect("common handle"),
                )
            }
        })
        .collect()
}

fn BenchmarkIsPoint() {
    let range = KeyRange::new(
        Key::from_bytes(b"rowkey1".as_slice()),
        Key::from_bytes(b"rowkey2".as_slice()),
    );
    measure("BenchmarkIsPoint", 1_000_000, || {
        black_box(range.is_point());
    });
}

fn BenchmarkMemAwareHandleMap() {
    for size in [1_usize, 100, 10_000, 1_000_000] {
        let handles = handles(size);
        let iterations = (100_000 / size.max(1)).max(1);
        measure(
            &format!("BenchmarkMemAwareHandleMap/MemAwareIntMap_{size}"),
            iterations,
            || {
                let mut map = MemAwareHandleMap::new();
                for (index, handle) in handles.iter().enumerate() {
                    map.set(handle.clone(), index);
                }
                for handle in &handles {
                    black_box(map.get(handle));
                }
            },
        );
    }
}

fn BenchmarkNativeHandleMap() {
    for size in [1_usize, 100, 10_000, 1_000_000] {
        let handles = handles(size);
        let iterations = (100_000 / size.max(1)).max(1);
        measure(
            &format!("BenchmarkNativeHandleMap/NativeIntMap_{size}"),
            iterations,
            || {
                let mut map = HashMap::with_capacity(size);
                for (index, handle) in handles.iter().enumerate() {
                    map.insert(handle.encoded(), index);
                }
                for handle in &handles {
                    black_box(map.get(&handle.encoded()));
                }
            },
        );
    }
}

fn main() {
    BenchmarkIsPoint();
    BenchmarkMemAwareHandleMap();
    BenchmarkNativeHandleMap();
}
