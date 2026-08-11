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

//! Focused source-contract tests for Go `pkg/util/cteutil`.

use std::path::PathBuf;
use std::sync::Arc;

use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_executor::{CteStorage, ExecError, OomAction, StatementMemory};
use tidb_util::disk::{SpillEncryptionMethod, SpillStorage, SpillStorageSpec};

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

#[test]
fn storage_add_get_reopen_and_state_match_the_source_contract() {
    let mut storage = CteStorage::new(vec![int_type()], 4, unlimited_memory());
    assert!(!storage.done());
    assert_eq!(storage.iter(), 0);
    assert_eq!(storage.num_rows(), 0);

    storage
        .add_rows((0..10).map(|value| vec![Datum::Int(value)]))
        .unwrap();
    assert_eq!(storage.num_rows(), 10);
    assert_eq!(storage.num_chunks(), 3);
    assert_eq!(storage.get_row(1, 2).unwrap(), vec![Datum::Int(6)]);
    assert_eq!(
        storage.to_rows().unwrap(),
        (0..10)
            .map(|value| vec![Datum::Int(value)])
            .collect::<Vec<_>>()
    );

    storage.set_done();
    storage.set_iter(7);
    storage.set_error("producer failed");
    assert!(storage.done());
    assert_eq!(storage.iter(), 7);
    assert_eq!(storage.error(), Some("producer failed"));

    storage.reopen().unwrap();
    assert_eq!(storage.num_rows(), 0);
    assert!(!storage.done());
    assert_eq!(storage.iter(), 0);
    assert_eq!(storage.error(), None);

    storage
        .add_rows([vec![Datum::Int(42)]])
        .expect("reopened storage accepts rows");
    assert_eq!(storage.get_row(0, 0).unwrap(), vec![Datum::Int(42)]);
    storage.close();
    assert_eq!(storage.num_rows(), 0);
    assert!(matches!(
        storage.add_rows([vec![Datum::Int(9)]]),
        Err(ExecError::Internal(_))
    ));
}

#[test]
fn swap_moves_rows_and_schema_without_moving_producer_state() {
    let mut ints = CteStorage::new(vec![int_type()], 3, unlimited_memory());
    ints.add_rows((0..3).map(|value| vec![Datum::Int(value)]))
        .unwrap();
    ints.set_done();
    ints.set_iter(4);

    let mut strings = CteStorage::new(
        vec![FieldType::new(FieldTypeCode::VarString)],
        3,
        unlimited_memory(),
    );
    strings
        .add_rows((0..3).map(|value| vec![Datum::new_string(value.to_string())]))
        .unwrap();

    ints.swap_data(&mut strings).unwrap();
    assert_eq!(
        ints.to_rows().unwrap(),
        (0..3)
            .map(|value| vec![Datum::new_string(value.to_string())])
            .collect::<Vec<_>>()
    );
    assert_eq!(
        strings.to_rows().unwrap(),
        (0..3)
            .map(|value| vec![Datum::Int(value)])
            .collect::<Vec<_>>()
    );
    assert!(ints.done());
    assert_eq!(ints.iter(), 4);
    assert!(!strings.done());
    assert_eq!(strings.iter(), 0);
}

#[test]
fn quota_spills_and_the_last_owner_releases_every_resource() {
    let scratch = ScratchDir::new("spill");
    let spill = Arc::new(
        SpillStorage::open(SpillStorageSpec {
            path: scratch.0.clone(),
            quota_bytes: -1,
            encryption: SpillEncryptionMethod::Plaintext,
        })
        .unwrap(),
    );
    let memory = StatementMemory::new(1, OomAction::Cancel, 11)
        .with_tmp_storage_on_oom(true)
        .with_spill_storage(Arc::clone(&spill));

    let shared = {
        let mut storage = CteStorage::new(vec![int_type()], 64, memory.clone());
        storage
            .add_rows((0..128).map(|value| vec![Datum::Int(value)]))
            .unwrap();
        assert!(storage.already_spilled());
        assert_eq!(storage.mem_bytes(), 0);
        assert!(storage.disk_bytes() > 0);
        assert!(spill.global_tracker().bytes_consumed() > 0);
        assert_eq!(storage.get_row(1, 63).unwrap(), vec![Datum::Int(127)]);
        Arc::new(storage)
    };

    let second_owner = Arc::clone(&shared);
    drop(shared);
    assert!(spill.global_tracker().bytes_consumed() > 0);
    drop(second_owner);
    assert_eq!(memory.bytes_consumed(), 0);
    assert_eq!(spill.global_tracker().bytes_consumed(), 0);
}
