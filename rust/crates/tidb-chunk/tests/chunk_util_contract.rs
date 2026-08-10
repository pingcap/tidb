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

//! Public semantic cluster for accepted `chunk_util.go` and
//! `chunk_util_test.go`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tidb_chunk::chunk::Chunk;
use tidb_chunk::chunk_util::{
    copy_rows, copy_selected_join_rows_direct, copy_selected_join_rows_with_same_outer_rows,
    copy_selected_rows, copy_selected_rows_with_row_id_func, ColumnSwapHelper,
    DiskFileReaderWriter, MSG_ERR_SEL_NOT_NIL,
};
use tidb_chunk::column::Column;
use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_util::checksum::CHECKSUM_PAYLOAD_SIZE;
use tidb_util::disk::{SpillEncryptionMethod, SpillStorage, SpillStorageSpec};

static STORAGE_CASE: AtomicUsize = AtomicUsize::new(0);

struct TestStorage {
    authority: Option<Arc<SpillStorage>>,
    path: PathBuf,
}

impl TestStorage {
    fn open(case: &str, encryption: SpillEncryptionMethod) -> Self {
        let sequence = STORAGE_CASE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "tidb_chunk_util_contract_{case}_{}_{sequence}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&path);
        let authority = Arc::new(
            SpillStorage::open(SpillStorageSpec {
                path: path.clone(),
                quota_bytes: -1,
                encryption,
            })
            .expect("spill storage"),
        );
        Self {
            authority: Some(authority),
            path,
        }
    }

    fn authority(&self) -> &SpillStorage {
        self.authority.as_deref().expect("live spill authority")
    }
}

impl Drop for TestStorage {
    fn drop(&mut self) {
        drop(self.authority.take());
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn fixed_source() -> Column {
    let mut source = Column::new_fixed_len(8, 4);
    source.append_int64(10);
    source.append_null();
    source.append_int64(30);
    source.append_int64(40);
    source
}

fn var_source() -> Column {
    let mut source = Column::new_var_len(4);
    source.append_bytes(b"a");
    source.append_null();
    source.append_bytes(b"ccc");
    source.append_bytes(b"dddd");
    source
}

#[test]
fn selected_column_copy_contract() {
    let selected = [true, true, false, true];

    let fixed = fixed_source();
    let mut fixed_selected = Column::new_fixed_len(8, 0);
    copy_selected_rows(&mut fixed_selected, &fixed, &selected);
    assert_eq!(fixed_selected.rows(), 3);
    assert_eq!(fixed_selected.get_int64(0), 10);
    assert!(fixed_selected.is_null(1));
    assert_eq!(fixed_selected.get_int64(2), 40);

    let mut fixed_inverted = Column::new_fixed_len(8, 0);
    tidb_chunk::chunk_util::copy_expected_rows_with_row_id_func(
        &mut fixed_inverted,
        &fixed,
        &selected,
        false,
        0,
        selected.len(),
        |index| index,
    );
    assert_eq!(fixed_inverted.rows(), 1);
    assert_eq!(fixed_inverted.get_int64(0), 30);

    let remapped = [true, false, true, false];
    let mut fixed_remapped = Column::new_fixed_len(8, 0);
    copy_selected_rows_with_row_id_func(
        &mut fixed_remapped,
        &fixed,
        &remapped,
        0,
        remapped.len(),
        |index| 3 - index,
    );
    assert_eq!(fixed_remapped.rows(), 2);
    assert_eq!(fixed_remapped.get_int64(0), 40);
    assert!(fixed_remapped.is_null(1));

    let variable = var_source();
    let mut variable_rows = Column::new_var_len(0);
    copy_rows(&mut variable_rows, &variable, &[3, 1, 0]);
    assert_eq!(variable_rows.rows(), 3);
    assert_eq!(variable_rows.get_bytes(0).as_ref(), b"dddd");
    assert!(variable_rows.is_null(1));
    assert_eq!(variable_rows.get_bytes(2).as_ref(), b"a");

    let mut empty_fixed = Column::new_fixed_len(8, 0);
    copy_rows(&mut empty_fixed, &fixed, &[]);
    let mut empty_var = Column::new_var_len(0);
    copy_selected_rows(&mut empty_var, &variable, &[]);
    assert_eq!(empty_fixed.rows(), 0);
    assert_eq!(empty_var.rows(), 0);
}

fn join_fields() -> Vec<FieldType> {
    vec![
        FieldType::new(FieldTypeCode::VarString),
        FieldType::new(FieldTypeCode::LongLong),
        FieldType::new(FieldTypeCode::LongLong),
    ]
}

fn join_source() -> Chunk {
    let fields = join_fields();
    let mut source = Chunk::new_with_capacity(&fields, 4);
    for (name, value) in [("a", 1), ("b", 2), ("c", 3), ("d", 4)] {
        source.append_string(0, name);
        source.append_int64(1, value);
        source.append_int64(2, 9);
    }
    source
}

fn assert_selected_join(chunk: &Chunk) {
    assert_eq!(chunk.num_rows(), 2);
    assert_eq!(chunk.num_virtual_rows(), 2);
    assert_eq!(chunk.get_row(0).get_bytes(0).as_ref(), b"a");
    assert_eq!(chunk.get_row(0).get_int64(1), 1);
    assert_eq!(chunk.get_row(0).get_int64(2), 9);
    assert_eq!(chunk.get_row(1).get_bytes(0).as_ref(), b"c");
    assert_eq!(chunk.get_row(1).get_int64(1), 3);
    assert_eq!(chunk.get_row(1).get_int64(2), 9);
}

#[test]
fn selected_join_copy_contract() {
    let fields = join_fields();
    let source = join_source();
    let selected = [true, false, true, false];

    let mut direct = Chunk::new_with_capacity(&fields, 4);
    assert!(tidb_chunk::chunk_util::copy_selected_join_rows_direct(
        &source,
        &selected,
        &mut direct
    )
    .expect("physical source and destination"));
    assert_selected_join(&direct);

    let mut same_outer = Chunk::new_with_capacity(&fields, 4);
    assert!(copy_selected_join_rows_with_same_outer_rows(
        &source,
        0,
        2,
        2,
        1,
        &selected,
        &mut same_outer,
    )
    .expect("physical source and destination"));
    assert_selected_join(&same_outer);

    let mut no_selection = Chunk::new_with_capacity(&fields, 4);
    assert!(
        !copy_selected_join_rows_direct(&source, &[], &mut no_selection)
            .expect("an empty selection copies no rows")
    );

    let mut empty = Chunk::new_with_capacity(&fields, 1);
    let mut empty_destination = Chunk::new_with_capacity(&fields, 1);
    assert!(
        !copy_selected_join_rows_direct(&empty, &[], &mut empty_destination)
            .expect("an empty source is a no-op")
    );
    empty.set_sel(Some(vec![]));
    assert!(
        !copy_selected_join_rows_direct(&empty, &[], &mut empty_destination)
            .expect("empty-source priority precedes selection validation")
    );

    let mut selected_source = join_source();
    selected_source.set_sel(Some(vec![0, 2]));
    let mut destination = Chunk::new_with_capacity(&fields, 4);
    assert_eq!(
        copy_selected_join_rows_direct(&selected_source, &selected, &mut destination),
        Err(MSG_ERR_SEL_NOT_NIL)
    );
    assert_eq!(destination.num_rows(), 0);

    let mut selected_destination = Chunk::new_with_capacity(&fields, 4);
    selected_destination.set_sel(Some(vec![]));
    assert_eq!(
        copy_selected_join_rows_with_same_outer_rows(
            &source,
            0,
            2,
            2,
            1,
            &selected,
            &mut selected_destination,
        ),
        Err(MSG_ERR_SEL_NOT_NIL)
    );

    let mut virtual_source = Chunk::new_with_capacity(&[], 0);
    virtual_source.set_num_virtual_rows(3);
    let mut virtual_destination = Chunk::new_with_capacity(&[], 0);
    assert!(copy_selected_join_rows_direct(
        &virtual_source,
        &[true, false, true],
        &mut virtual_destination,
    )
    .expect("column-less virtual rows"));
    assert_eq!(virtual_destination.num_virtual_rows(), 2);

    let one_field = [FieldType::new(FieldTypeCode::LongLong)];
    let mut repeated_source = Chunk::new_with_capacity(&one_field, 3);
    for _ in 0..3 {
        repeated_source.append_int64(0, 7);
    }
    let mut repeated_destination = Chunk::new_with_capacity(&one_field, 3);
    assert!(copy_selected_join_rows_with_same_outer_rows(
        &repeated_source,
        0,
        0,
        0,
        1,
        &[true, false, true],
        &mut repeated_destination,
    )
    .expect("empty inner range with one repeated outer"));
    assert_eq!(repeated_destination.num_rows(), 2);
    assert_eq!(repeated_destination.get_row(0).get_int64(0), 7);
    assert_eq!(repeated_destination.get_row(1).get_int64(0), 7);
}

fn aliased_input() -> Chunk {
    let fields = [
        FieldType::new(FieldTypeCode::LongLong),
        FieldType::new(FieldTypeCode::LongLong),
    ];
    let mut input = Chunk::new_with_capacity(&fields, 2);
    input.append_int64(0, 99);
    input.make_ref(0, 1);
    input
}

fn four_column_output() -> Chunk {
    let fields = vec![FieldType::new(FieldTypeCode::LongLong); 4];
    Chunk::new_with_capacity(&fields, 2)
}

#[test]
fn column_swap_identity_and_cache_contract() {
    let helper = ColumnSwapHelper::from_mapping(HashMap::from([(0, vec![0, 1]), (1, vec![2, 3])]));
    let mut input = aliased_input();
    let mut output = four_column_output();
    tidb_chunk::chunk_util::ColumnSwapHelper::swap_columns(&helper, &mut input, &mut output)
        .expect("physical chunks");
    for column in 0..4 {
        assert_eq!(output.get_row(0).get_int64(column), 99);
    }
    output.append_int64(0, 100);
    for column in 0..4 {
        assert_eq!(output.get_row(1).get_int64(column), 100);
    }

    let empty = ColumnSwapHelper::new(&[]);
    let mut selected_input = aliased_input();
    selected_input.set_sel(Some(vec![0]));
    let mut selected_output = four_column_output();
    selected_output.set_sel(Some(vec![]));
    empty
        .swap_columns(&mut selected_input, &mut selected_output)
        .expect("an empty mapping is a no-op");

    let nonempty = ColumnSwapHelper::new(&[0]);
    assert_eq!(
        nonempty.swap_columns(&mut selected_input, &mut selected_output),
        Err(MSG_ERR_SEL_NOT_NIL)
    );

    let concurrent = Arc::new(ColumnSwapHelper::new(&[0, 0, 1, 1]));
    let threads: Vec<_> = (0..4)
        .map(|_| {
            let helper = Arc::clone(&concurrent);
            std::thread::spawn(move || {
                let mut input = aliased_input();
                let mut output = four_column_output();
                helper
                    .swap_columns(&mut input, &mut output)
                    .expect("concurrent first use");
                (0..4)
                    .map(|column| output.get_row(0).get_int64(column))
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    for thread in threads {
        assert_eq!(thread.join().expect("swap worker"), [99, 99, 99, 99]);
    }
}

fn disk_case(encryption: SpillEncryptionMethod, case: &str) {
    let storage = TestStorage::open(case, encryption);
    let mut file = DiskFileReaderWriter::default();
    assert!(file.write(b"closed").is_err());
    assert!(file.read_full_at(&mut [0; 1], 0).is_err());

    file.init_with_file_name(storage.authority(), "chunk-util-")
        .expect("open spill file");
    let path = file.path().expect("spill path").clone();
    let first = vec![0x2a; CHECKSUM_PAYLOAD_SIZE + 17];
    let second = vec![0x7b; CHECKSUM_PAYLOAD_SIZE + 29];
    assert_eq!(file.write(&first).expect("first exact write"), first.len());
    assert_eq!(
        file.write(&second).expect("second exact write"),
        second.len()
    );
    assert_eq!(file.off_write(), (first.len() + second.len()) as i64);

    let mut expected = first;
    expected.extend_from_slice(&second);
    let mut read = vec![0; expected.len() - 23];
    assert_eq!(
        file.read_full_at(&mut read, 11).expect("live-cache read"),
        read.len()
    );
    assert_eq!(read, expected[11..expected.len() - 12]);

    assert!(path.exists());
    file.close();
    assert!(!path.exists());
    file.close();
}

#[test]
fn spill_file_plaintext_and_aes_contract() {
    let mut unopened = DiskFileReaderWriter::default();
    assert!(tidb_chunk::chunk_util::DiskFileReaderWriter::write(&mut unopened, b"closed").is_err());
    disk_case(SpillEncryptionMethod::Plaintext, "plaintext");
    disk_case(SpillEncryptionMethod::Aes128Ctr, "aes128-ctr");
}

#[test]
fn benchmark_semantic_workload_contract() {
    let fields = join_fields();
    let mut source = Chunk::new_with_capacity(&fields, 1_024);
    let mut selected = Vec::with_capacity(1_024);
    for row in 0..1_024_i64 {
        source.append_string(0, format!("row-{row}"));
        source.append_int64(1, row);
        source.append_int64(2, 7);
        selected.push(row % 7 != 0);
    }
    let selected_count = selected.iter().filter(|selected| **selected).count();

    let mut direct = Chunk::new_with_capacity(&fields, 1_024);
    assert!(
        copy_selected_join_rows_direct(&source, &selected, &mut direct)
            .expect("direct benchmark workload")
    );
    assert_eq!(direct.num_rows(), selected_count);
    assert_eq!(direct.num_virtual_rows(), selected_count);
    assert_eq!(direct.get_row(0).get_int64(1), 1);
    assert_eq!(direct.get_row(selected_count - 1).get_int64(1), 1_023);

    let mut same_outer = Chunk::new_with_capacity(&fields, 1_024);
    assert!(copy_selected_join_rows_with_same_outer_rows(
        &source,
        0,
        2,
        2,
        1,
        &selected,
        &mut same_outer,
    )
    .expect("same-outer benchmark workload"));
    assert_eq!(same_outer.num_rows(), selected_count);
    assert_eq!(same_outer.get_row(0).get_int64(1), 1);
    assert_eq!(same_outer.get_row(selected_count - 1).get_int64(1), 1_023);

    let mut row_append = Chunk::new_with_capacity(&fields, 1_024);
    for (row, is_selected) in selected.iter().copied().enumerate() {
        if is_selected {
            row_append.append_row(source.get_row(row));
        }
    }
    assert_eq!(row_append.num_rows(), selected_count);
    assert_eq!(row_append.get_row(0).get_int64(1), 1);
    assert_eq!(row_append.get_row(selected_count - 1).get_int64(1), 1_023);
}
