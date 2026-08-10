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

//! Public contract for accepted `pkg/util/chunk/chunk_in_disk.go` and its
//! direct `chunk_in_disk_test.go` surface.

use std::path::PathBuf;
use std::sync::Arc;

use tidb_chunk::chunk::Chunk;
use tidb_chunk::chunk_in_disk::{DataInDiskByChunks, DiskError};
use tidb_datatype::{BinaryJSON, FieldType, FieldTypeCode};
use tidb_util::checksum::CHECKSUM_PAYLOAD_SIZE;
use tidb_util::disk::{SpillEncryptionMethod, SpillStorage, SpillStorageSpec};

struct TestStorage {
    authority: Option<Arc<SpillStorage>>,
    path: PathBuf,
}

impl TestStorage {
    fn open(case: &str, quota_bytes: i64) -> Self {
        let path = std::env::temp_dir().join(format!(
            "tidb_chunk_in_disk_contract_{case}_{}",
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
        let _ = std::fs::remove_file(&self.path);
    }
}

fn source_fields() -> Vec<FieldType> {
    vec![
        FieldType::new(FieldTypeCode::VarString),
        FieldType::new(FieldTypeCode::LongLong),
        FieldType::new(FieldTypeCode::VarString),
        FieldType::new(FieldTypeCode::LongLong),
        FieldType::new(FieldTypeCode::Json),
    ]
}

fn source_chunk(
    fields: &[FieldType],
    chunk_index: usize,
    capacity: usize,
    max_chunk_size: usize,
    required_rows: usize,
    virtual_rows: usize,
    selection: Option<Vec<usize>>,
) -> Chunk {
    let mut chunk = Chunk::new(fields, capacity, max_chunk_size);
    chunk.set_required_rows(required_rows as isize, max_chunk_size);
    chunk.set_num_virtual_rows(virtual_rows);
    for row_index in 0..4 {
        chunk.append_string(0, format!("chunk-{chunk_index}-row-{row_index}"));
        chunk.append_null(1);
        chunk.append_null(2);
        chunk.append_int64(3, (chunk_index * 100 + row_index) as i64);
        if chunk_index.is_multiple_of(2) {
            let json = BinaryJSON::parse(&format!(r#""json-{chunk_index}-{row_index}""#))
                .expect("binary JSON");
            chunk.append_json(4, &json);
        } else {
            chunk.append_null(4);
        }
    }
    chunk.set_sel(selection);
    chunk
}

fn assert_same_chunk(expected: &Chunk, actual: &Chunk) {
    assert_eq!(actual.capacity(), expected.capacity());
    assert_eq!(actual.required_rows(), expected.required_rows());
    assert_eq!(actual.num_virtual_rows(), expected.num_virtual_rows());
    assert_eq!(actual.sel(), expected.sel());
    assert_eq!(actual.num_rows(), expected.num_rows());
    assert_eq!(actual.column(0).rows(), expected.column(0).rows());
    for row_index in 0..expected.column(0).rows() {
        assert_eq!(
            actual.column(0).get_bytes(row_index),
            expected.column(0).get_bytes(row_index)
        );
        assert_eq!(
            actual.column(1).is_null(row_index),
            expected.column(1).is_null(row_index)
        );
        assert_eq!(
            actual.column(2).is_null(row_index),
            expected.column(2).is_null(row_index)
        );
        assert_eq!(
            actual.column(3).get_int64(row_index),
            expected.column(3).get_int64(row_index)
        );
        assert_eq!(
            actual.column(4).is_null(row_index),
            expected.column(4).is_null(row_index)
        );
        if !expected.column(4).is_null(row_index) {
            assert_eq!(
                actual.column(4).get_json(row_index).to_string(),
                expected.column(4).get_json(row_index).to_string()
            );
        }
    }
}

#[test]
fn whole_chunk_spill_get_and_fill_preserve_source_state() {
    let storage = TestStorage::open("roundtrip", -1);
    let fields = source_fields();
    let source = [
        source_chunk(&fields, 0, 11, 17, 9, 23, Some(vec![3, 0, 2])),
        source_chunk(&fields, 1, 7, 13, 6, 19, Some(vec![1, 3])),
    ];
    let mut disk = tidb_chunk::chunk_in_disk::DataInDiskByChunks::new(
        fields.clone(),
        "public-roundtrip-",
        storage.authority(),
    );
    for chunk in &source {
        disk.add(chunk).expect("spill chunk");
    }

    assert_eq!(disk.num_chunks(), 2);
    assert_eq!(disk.num_rows(), 5);
    assert!(disk.total_bytes_in_disk() > 0);
    assert_eq!(
        disk.disk_tracker().bytes_consumed(),
        disk.total_bytes_in_disk()
    );
    let path = disk.file_path().cloned().expect("spill file");
    assert!(path.exists());

    let first = DataInDiskByChunks::get_chunk(&mut disk, 0).expect("get first chunk");
    assert_same_chunk(&source[0], &first);

    let mut destination = Chunk::new_with_capacity(&fields, 32);
    destination.set_sel(Some(Vec::with_capacity(8)));
    disk.fill_chunk(1, &mut destination)
        .expect("fill second chunk");
    assert_same_chunk(&source[1], &destination);

    disk.close();
    assert!(!path.exists());
    assert_eq!(disk.disk_tracker().bytes_consumed(), 0);
    disk.close();
}

#[test]
fn fill_without_serialized_selection_preserves_destination_selection() {
    let storage = TestStorage::open("selection", -1);
    let fields = vec![FieldType::new(FieldTypeCode::LongLong)];
    let mut source = Chunk::new_with_capacity(&fields, 2);
    source.append_int64(0, 10);
    source.append_int64(0, 20);

    let mut disk = DataInDiskByChunks::new(fields.clone(), "selection-", storage.authority());
    disk.add(&source).expect("spill unselected chunk");

    let mut destination = Chunk::new_with_capacity(&fields, 2);
    destination.append_int64(0, -1);
    destination.append_int64(0, -2);
    destination.set_sel(Some(vec![1, 0]));
    tidb_chunk::chunk_in_disk::DataInDiskByChunks::fill_chunk(&mut disk, 0, &mut destination)
        .expect("fill chunk");

    assert_eq!(destination.sel(), Some(&[1, 0][..]));
    assert_eq!(destination.get_row(0).get_int64(0), 20);
    assert_eq!(destination.get_row(1).get_int64(0), 10);
}

#[test]
fn empty_and_storage_failures_are_atomic() {
    let fields = vec![FieldType::new(FieldTypeCode::LongLong)];
    let storage = TestStorage::open("failure", -1);
    let mut disk = DataInDiskByChunks::new(fields.clone(), "failure-", storage.authority());

    let empty = Chunk::new_with_capacity(&fields, 1);
    let error = tidb_chunk::chunk_in_disk::DataInDiskByChunks::add(&mut disk, &empty)
        .expect_err("empty chunks are rejected");
    assert_eq!(
        error.to_string(),
        "Chunk spilled to disk should have at least 1 row"
    );
    assert_eq!(disk.num_chunks(), 0);
    assert_eq!(disk.num_rows(), 0);
    assert_eq!(disk.total_bytes_in_disk(), 0);
    assert!(disk.file_path().is_none());

    let displaced = storage.path.with_extension("leased");
    std::fs::rename(&storage.path, &displaced).expect("move leased directory");
    std::fs::write(&storage.path, b"block spill directory").expect("blocking file");

    let mut row = Chunk::new_with_capacity(&fields, 1);
    row.append_int64(0, 7);
    let error = disk.add(&row).expect_err("create-file error propagates");
    assert!(matches!(error, DiskError::Io(_)));
    assert_eq!(disk.num_chunks(), 0);
    assert_eq!(disk.num_rows(), 0);
    assert_eq!(disk.total_bytes_in_disk(), 0);
    assert_eq!(disk.disk_tracker().bytes_consumed(), 0);
    assert!(disk.file_path().is_none());

    drop(disk);
    std::fs::remove_file(&storage.path).expect("remove blocking file");
    std::fs::rename(displaced, &storage.path).expect("restore leased directory");
}

#[test]
fn truncated_chunk_read_is_rejected_before_destination_mutation() {
    let storage = TestStorage::open("truncated", -1);
    let fields = vec![FieldType::new(FieldTypeCode::VarString)];
    let mut source = Chunk::new_with_capacity(&fields, 1);
    let payload = vec![0x5a; 6 * CHECKSUM_PAYLOAD_SIZE];
    source.append_bytes(0, &payload);

    let mut disk = DataInDiskByChunks::new(fields.clone(), "truncated-", storage.authority());
    disk.add(&source).expect("spill large chunk");
    let path = disk.file_path().cloned().expect("spill file");
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .expect("open spill file")
        .set_len(0)
        .expect("truncate spill file");

    let mut destination = Chunk::new_with_capacity(&fields, 1);
    destination.append_bytes(0, b"sentinel");
    let error = disk
        .fill_chunk(0, &mut destination)
        .expect_err("truncated image must be rejected");
    assert!(matches!(error, DiskError::Io(_)));
    assert_eq!(destination.get_row(0).get_bytes(0).as_ref(), b"sentinel");
    assert!(matches!(disk.get_chunk(0), Err(DiskError::Io(_))));
}

#[test]
fn zero_column_virtual_chunk_round_trips_and_close_releases_file() {
    let storage = TestStorage::open("virtual", -1);
    let mut source = Chunk::new(&[], 5, 7);
    source.set_num_virtual_rows(4);
    source.set_sel(Some(vec![3, 1]));

    let mut serialized = Vec::new();
    assert_eq!(
        tidb_chunk::chunk_in_disk::serialize_data_to_buf(&source, &mut serialized) as usize,
        serialized.len()
    );

    let mut disk = DataInDiskByChunks::new(Vec::new(), "virtual-", storage.authority());
    disk.add(&source).expect("spill virtual chunk");
    let path = disk.file_path().cloned().expect("spill file");
    let loaded = disk.get_chunk(0).expect("load virtual chunk");
    assert_eq!(loaded.num_cols(), 0);
    assert_eq!(loaded.capacity(), 5);
    assert_eq!(loaded.required_rows(), 7);
    assert_eq!(loaded.num_virtual_rows(), 4);
    assert_eq!(loaded.sel(), Some(&[3, 1][..]));
    assert_eq!(loaded.num_rows(), 2);

    disk.close();
    assert!(!path.exists());
    assert_eq!(disk.disk_tracker().bytes_consumed(), 0);
}

#[test]
fn process_quota_error_remains_accounted_until_close() {
    let storage = TestStorage::open("quota", 1);
    let fields = vec![FieldType::new(FieldTypeCode::LongLong)];
    let mut source = Chunk::new_with_capacity(&fields, 1);
    source.append_int64(0, 9);

    let mut disk = DataInDiskByChunks::new(fields, "quota-", storage.authority());
    disk.disk_tracker()
        .attach_to_global_tracker(storage.authority().global_tracker());
    let error = disk.add(&source).expect_err("one-byte quota is exceeded");
    assert!(matches!(error, DiskError::QuotaExceeded));
    assert_eq!(disk.num_chunks(), 1);
    assert_eq!(disk.num_rows(), 1);
    assert!(disk.total_bytes_in_disk() > 1);
    assert_eq!(
        storage.authority().global_tracker().bytes_consumed(),
        disk.total_bytes_in_disk()
    );

    tidb_chunk::chunk_in_disk::DataInDiskByChunks::close(&mut disk);
    assert_eq!(storage.authority().global_tracker().bytes_consumed(), 0);
}
