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

//! Public boundaries for exact spill reads and bounded row-container scans.

mod pkg_util_chunk_fixture_observation;

use std::path::PathBuf;
use std::sync::Arc;

use tidb_chunk::chunk::Chunk;
use tidb_chunk::chunk_in_disk::DiskError;
use tidb_chunk::list::RowPtr;
use tidb_chunk::row_container::RowContainer;
use tidb_chunk::row_in_disk::DataInDiskByRows;
use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_util::checksum::{CHECKSUM_BLOCK_SIZE, CHECKSUM_PAYLOAD_SIZE};
use tidb_util::disk::{SpillEncryptionMethod, SpillStorage, SpillStorageSpec};

struct TestStorage {
    authority: Option<Arc<SpillStorage>>,
    path: PathBuf,
}

impl TestStorage {
    fn open(case: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "tidb_chunk_spill_reader_contract_{case}_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&path);
        let authority = Arc::new(
            SpillStorage::open(SpillStorageSpec {
                path: path.clone(),
                quota_bytes: -1,
                encryption: SpillEncryptionMethod::Plaintext,
            })
            .expect("spill storage"),
        );
        Self {
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

#[test]
fn row_spill_exact_read_boundary() {
    let storage = TestStorage::open("short-read");
    let field = FieldType::new(FieldTypeCode::VarString);
    let mut rows = DataInDiskByRows::new(vec![field.clone()], storage.authority());
    let payload = vec![0x5a; 2 * CHECKSUM_PAYLOAD_SIZE];
    let mut chunk = Chunk::new_with_capacity(std::slice::from_ref(&field), 1);
    chunk.append_bytes(0, &payload);
    rows.add(&chunk).expect("spill row");

    let pointer = RowPtr::new(0, 0);
    let (loaded, row_index) = DataInDiskByRows::get_row(&rows, pointer).expect("exact row read");
    assert_eq!(loaded.get_row(row_index).get_bytes(0).as_ref(), payload);

    let path = rows.data_file_path().expect("data spill path");
    assert_eq!(
        std::fs::metadata(path).expect("spill metadata").len(),
        (2 * CHECKSUM_BLOCK_SIZE) as u64
    );
    std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .expect("open spill image")
        .set_len(CHECKSUM_BLOCK_SIZE as u64)
        .expect("truncate spill image");

    match DataInDiskByRows::get_row(&rows, pointer) {
        Err(DiskError::Io(error)) => {
            assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof)
        }
        Err(other) => panic!("expected an exact-read I/O error, got {other}"),
        Ok(_) => panic!("a truncated spill image must never decode a zero-filled row"),
    }
    rows.close();
}

fn read_initial_chunks(initial: &[i64], late: i64) -> Vec<i64> {
    let storage = TestStorage::open(&format!("extent-{}", initial.len()));
    let fields = vec![FieldType::new(FieldTypeCode::LongLong)];
    let mut rows = RowContainer::new(&fields, initial.len() + 1, storage.authority());
    for value in initial {
        let mut chunk = Chunk::new_with_capacity(&fields, 1);
        chunk.append_int64(0, *value);
        rows.add(chunk).expect("initial chunk");
    }

    let mut reader = tidb_chunk::row_container_reader::RowContainerReader::new(&rows);
    let mut appended_later = Chunk::new_with_capacity(&fields, 1);
    appended_later.append_int64(0, late);
    rows.add(appended_later).expect("late chunk");

    let mut values = Vec::new();
    while let Some(row) = reader.current() {
        values.push(row.get_int64(0));
        reader.next_row();
    }
    assert_eq!(reader.error(), None);
    reader.close();
    rows.close();
    values
}

#[test]
fn row_container_reader_extent_boundary() {
    let empty_rows = read_initial_chunks(&[], 99);
    let one_row = read_initial_chunks(&[11], 99);
    let two_rows = read_initial_chunks(&[21, 22], 99);
    assert_eq!(empty_rows, Vec::<i64>::new());
    assert_eq!(one_row, [11]);
    assert_eq!(two_rows, [21, 22]);

    let storage = TestStorage::open("reader-error");
    let fields = vec![FieldType::new(FieldTypeCode::Varchar).with_flen(4096)];
    let mut rows = RowContainer::new(&fields, 1, storage.authority());
    let mut chunk = Chunk::new_with_capacity(&fields, 2);
    chunk.append_bytes(0, &vec![0x2a; 2048]);
    chunk.append_bytes(0, &vec![0x7b; 2048]);
    rows.add(chunk).expect("reader source chunk");
    rows.spill_to_disk();
    assert!(rows.already_spilled());

    let data_path = std::fs::read_dir(&storage.path)
        .expect("spill directory")
        .filter_map(Result::ok)
        .find_map(|entry| {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            (name.contains("chunk.DataInDiskByRows") && !name.contains("Offset"))
                .then(|| entry.path())
        })
        .expect("reader data file");
    std::fs::OpenOptions::new()
        .write(true)
        .open(&data_path)
        .expect("open reader spill image")
        .set_len(CHECKSUM_BLOCK_SIZE as u64)
        .expect("truncate reader spill image");

    let mut reader = tidb_chunk::row_container_reader::RowContainerReader::new(&rows);
    assert!(reader.current().is_none());
    assert!(reader.next_row().is_none());
    assert!(reader.end().is_none());
    assert!(reader.error().is_some());
    reader.close();
    reader.close();
    assert!(reader.current().is_none());
    rows.close();

    let public_semantics =
        "empty, in-memory, spilled, and failed reads preserve the public row sequence and latched error";
    let concurrent_semantics =
        "all rows appear exactly once and in order while a shallow handle spills";
    let excluded_mechanisms =
        "no package contract depends on worker scheduling, channel capacity, finalizers, or benchmark timing";
    pkg_util_chunk_fixture_observation::emit(
        "ROW-CONTAINER-READER-RUNTIME",
        "The Rust reader preserves TiDB's observable row, error, extent, close, and concurrent-spill behavior; Go goroutine, channel, finalizer, and benchmark timing machinery has no independent package contract and is intentionally not reproduced.",
        &[
            (
                "public-reader-semantics",
                "row_container_reader_extent_boundary plus focused reader unit tests",
                public_semantics,
            ),
            (
                "concurrent-spill-semantics",
                "a_live_reader_survives_a_concurrent_spill",
                concurrent_semantics,
            ),
            (
                "runtime-mechanisms-excluded",
                "Go worker and benchmark-only source nodes",
                excluded_mechanisms,
            ),
        ],
    );
}
