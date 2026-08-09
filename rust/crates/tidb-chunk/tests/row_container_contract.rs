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

//! Public row-container boundaries owned by `pkg/util/chunk`.

use std::path::PathBuf;
use std::sync::{mpsc, Arc};
use std::time::Duration;

use tidb_chunk::chunk::Chunk;
use tidb_chunk::list::RowPtr;
use tidb_chunk::row_container::RowContainer;
use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_util::disk::{SpillEncryptionMethod, SpillStorage, SpillStorageSpec};

struct TestStorage {
    authority: Option<Arc<SpillStorage>>,
    path: PathBuf,
}

impl TestStorage {
    fn open(case: &str, quota_bytes: i64) -> Self {
        let path = std::env::temp_dir().join(format!(
            "tidb_chunk_row_container_contract_{case}_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&path);
        let authority = Arc::new(
            SpillStorage::open(SpillStorageSpec {
                path: path.clone(),
                quota_bytes,
                encryption: SpillEncryptionMethod::Plaintext,
            })
            .expect("spill storage"),
        );
        TestStorage {
            authority: Some(authority),
            path,
        }
    }

    fn authority(&self) -> Arc<SpillStorage> {
        Arc::clone(self.authority.as_ref().expect("live test storage"))
    }
}

impl Drop for TestStorage {
    fn drop(&mut self) {
        drop(self.authority.take());
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn int64_chunk(fields: &[FieldType], value: i64) -> Chunk {
    let mut chunk = Chunk::new_with_capacity(fields, 1);
    chunk.append_int64(0, value);
    chunk
}

#[test]
fn row_container_conditional_read_boundary() {
    let fields = vec![FieldType::new(FieldTypeCode::LongLong)];
    let pointer = RowPtr::new(0, 0);

    let storage = TestStorage::open("success", -1);
    let authority = storage.authority();
    let mut rows = RowContainer::new(&fields, 1, Arc::clone(&authority));
    rows.disk_tracker()
        .attach_to_global_tracker(authority.global_tracker());
    rows.add(int64_chunk(&fields, 41)).expect("memory add");

    let mut scratch = Chunk::new_with_capacity(&fields, 1);
    let loaded = tidb_chunk::row_container::RowContainer::get_row_and_append_to_chunk_if_in_disk(
        &rows,
        pointer,
        &mut scratch,
    )
    .expect("in-memory read");
    assert_eq!(loaded.appended_row_index(), None);
    assert_eq!(loaded.row(&scratch).get_int64(0), 41);
    assert_eq!(scratch.num_rows(), 0, "memory reads remain live views");

    let mut spiller = rows.shallow_copy();
    let (spilled_tx, spilled_rx) = mpsc::sync_channel(1);
    let spill = std::thread::spawn(move || {
        spiller.spill_to_disk();
        spilled_tx.send(()).expect("report spill completion");
    });
    if let Err(error) = spilled_rx.recv_timeout(Duration::from_secs(2)) {
        drop(loaded);
        spill.join().expect("spill thread after releasing row");
        panic!("a retained row must not retain the container lock: {error}");
    }
    spill.join().expect("spill thread");
    assert!(rows.already_spilled());
    assert_eq!(loaded.row(&scratch).get_int64(0), 41);
    assert_eq!(scratch.num_rows(), 0);
    drop(loaded);
    let loaded = tidb_chunk::row_container::RowContainer::get_row_and_append_to_chunk_if_in_disk(
        &rows,
        pointer,
        &mut scratch,
    )
    .expect("spilled read");
    assert_eq!(loaded.appended_row_index(), Some(0));
    assert_eq!(loaded.row(&scratch).get_int64(0), 41);
    assert_eq!(scratch.num_rows(), 1, "disk reads append exactly once");
    drop(loaded);
    rows.close();
    drop(rows);
    drop(authority);
    drop(storage);

    let storage = TestStorage::open("quota", 1);
    let authority = storage.authority();
    let mut rows = RowContainer::new(&fields, 1, Arc::clone(&authority));
    rows.disk_tracker()
        .attach_to_global_tracker(authority.global_tracker());
    rows.add(int64_chunk(&fields, 42)).expect("memory add");
    rows.spill_to_disk();
    let stored = rows.spill_error().expect("quota failure is stored");
    let mut scratch = Chunk::new_with_capacity(&fields, 1);
    let error =
        match tidb_chunk::row_container::RowContainer::get_row_and_append_to_chunk_if_in_disk(
            &rows,
            pointer,
            &mut scratch,
        ) {
            Ok(_) => panic!("stored spill errors must fail conditional reads"),
            Err(error) => error,
        };
    assert_eq!(error.to_string(), stored);
    assert_eq!(scratch.num_rows(), 0, "errors must preserve scratch");
    rows.close();
    drop(rows);
    drop(authority);
    drop(storage);
}
