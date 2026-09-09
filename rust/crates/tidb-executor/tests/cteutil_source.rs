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

//! Source-shaped tests for Go `pkg/util/cteutil`.
//! aggregate-test: standalone

use std::path::PathBuf;
use std::sync::Arc;

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_executor::{CteStorage, OomAction, StatementMemory};
use tidb_util::spill_storage::{SpillEncryptionMethod, SpillStorage, SpillStorageSpec};

struct ScratchDir(PathBuf);

impl ScratchDir {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "tidb_cteutil_{}_{}_{}",
            std::process::id(),
            name,
            std::thread::current().name().unwrap_or("test")
        ));
        let _ = std::fs::remove_dir_all(&path);
        Self(path)
    }
}

impl Drop for ScratchDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn int_type() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong)
}

fn unlimited_memory() -> StatementMemory {
    StatementMemory::new(-1, OomAction::Cancel, 1).with_tmp_storage_on_oom(false)
}

fn int_chunk(size: usize) -> Chunk {
    let field_types = [int_type()];
    let mut chunk = Chunk::new_with_capacity(&field_types, size);
    for value in 0..size {
        chunk.append_datum(0, &Datum::Int(value as i64));
    }
    chunk
}

#[test]
fn TestStorageBasic() {
    let mut storage = CteStorage::new(vec![int_type()], 1, unlimited_memory());
    assert!(storage.deref_and_close().is_err());
    storage.open_and_ref().unwrap();
    storage.deref_and_close().unwrap();
    assert!(storage.deref_and_close().is_err());

    storage.open_and_ref().unwrap();
    storage.open_and_ref().unwrap();
    storage.deref_and_close().unwrap();
    storage.deref_and_close().unwrap();
    assert!(storage.deref_and_close().is_err());
}

#[test]
fn TestOpenAndClose() {
    let mut storage = CteStorage::new(vec![int_type()], 1, unlimited_memory());
    for _ in 0..10 {
        storage.open_and_ref().unwrap();
    }
    for _ in 0..10 {
        storage.deref_and_close().unwrap();
    }
    assert!(storage.deref_and_close().is_err());
}

#[test]
fn TestAddAndGetChunk() {
    let mut storage = CteStorage::new(vec![int_type()], 10, unlimited_memory());
    let input = int_chunk(10);
    assert!(storage.add_chunk(input.copy_construct_sel()).is_err());

    storage.open_and_ref().unwrap();
    storage.add_chunk(input.copy_construct_sel()).unwrap();
    let output = storage.get_chunk(0).unwrap();
    let values: Vec<_> = (0..10)
        .map(|row| output.get_row(row).get_datum_row(&[int_type()]))
        .collect();
    assert_eq!(
        values,
        (0..10)
            .map(|value| vec![Datum::Int(value)])
            .collect::<Vec<_>>()
    );
}

#[test]
fn TestSpillToDisk() {
    let scratch = ScratchDir::new("spill");
    let spill = Arc::new(
        SpillStorage::open(SpillStorageSpec {
            path: scratch.0.clone(),
            quota_bytes: -1,
            encryption: SpillEncryptionMethod::Plaintext,
        })
        .unwrap(),
    );
    let input = int_chunk(10);
    let memory = StatementMemory::new(input.memory_usage() + 1, OomAction::Cancel, 11)
        .with_tmp_storage_on_oom(true)
        .with_spill_storage(Arc::clone(&spill));
    let mut storage = CteStorage::new(vec![int_type()], 10, memory);
    storage.open_and_ref().unwrap();

    storage.add_chunk(input.copy_construct_sel()).unwrap();
    assert!(!storage.already_spilled());
    assert!(storage.mem_bytes() > 0);
    assert_eq!(storage.disk_bytes(), 0);

    storage.add_chunk(input.copy_construct_sel()).unwrap();
    assert!(storage.already_spilled());
    assert_eq!(storage.mem_bytes(), 0);
    assert!(storage.disk_bytes() > 0);
    for chunk_index in 0..2 {
        let output = storage.get_chunk(chunk_index).unwrap();
        assert_eq!(
            (0..10)
                .map(|row| output.get_row(row).get_datum_row(&[int_type()]))
                .collect::<Vec<_>>(),
            (0..10)
                .map(|value| vec![Datum::Int(value)])
                .collect::<Vec<_>>()
        );
    }
}

#[test]
fn TestReopen() {
    let mut storage = CteStorage::new(vec![int_type()], 10, unlimited_memory());
    storage.open_and_ref().unwrap();
    let input = int_chunk(10);
    storage.add_chunk(input.copy_construct_sel()).unwrap();
    assert_eq!(storage.num_chunks(), 1);

    storage.reopen().unwrap();
    assert_eq!(storage.num_chunks(), 0);
    storage.add_chunk(input.copy_construct_sel()).unwrap();
    assert_eq!(storage.num_chunks(), 1);
    assert_eq!(
        (0..10)
            .map(|row| storage.get_row(0, row).unwrap())
            .collect::<Vec<_>>(),
        (0..10)
            .map(|value| vec![Datum::Int(value)])
            .collect::<Vec<_>>()
    );
    for _ in 0..100 {
        storage.reopen().unwrap();
    }
    storage.add_chunk(input).unwrap();
    assert_eq!(storage.num_chunks(), 1);
    assert_eq!(
        (0..10)
            .map(|row| storage.get_row(0, row).unwrap())
            .collect::<Vec<_>>(),
        (0..10)
            .map(|value| vec![Datum::Int(value)])
            .collect::<Vec<_>>()
    );
}

#[test]
fn TestSwapData() {
    let mut ints = CteStorage::new(vec![int_type()], 10, unlimited_memory());
    ints.open_and_ref().unwrap();
    ints.add_chunk(int_chunk(10)).unwrap();

    let string_type = FieldType::new(FieldTypeCode::VarString);
    let mut strings = CteStorage::new(vec![string_type.clone()], 10, unlimited_memory());
    strings.open_and_ref().unwrap();
    let mut string_chunk = Chunk::new_with_capacity(std::slice::from_ref(&string_type), 10);
    for value in 0..10 {
        string_chunk.append_datum(0, &Datum::new_string(value.to_string()));
    }
    strings.add_chunk(string_chunk).unwrap();

    ints.swap_data(&mut strings).unwrap();
    assert_eq!(
        (0..10)
            .map(|row| ints.get_row(0, row).unwrap())
            .collect::<Vec<_>>(),
        (0..10)
            .map(|value| vec![Datum::new_string(value.to_string())])
            .collect::<Vec<_>>()
    );
    assert_eq!(
        (0..10)
            .map(|row| strings.get_row(0, row).unwrap())
            .collect::<Vec<_>>(),
        (0..10)
            .map(|value| vec![Datum::Int(value)])
            .collect::<Vec<_>>()
    );
}
