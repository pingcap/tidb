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

//! Public sorted-row spill contract at the `tidb-chunk` crate boundary.

use std::sync::Arc;

use tidb_chunk::chunk::Chunk;
use tidb_chunk::chunk_in_disk::DiskError;
use tidb_chunk::compare::get_compare_func;
use tidb_chunk::sorted_row_container::SortedRowContainer;
use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_util::disk::{SpillEncryptionMethod, SpillStorage, SpillStorageSpec};

#[test]
fn sorted_container_orders_rows_across_spill_and_rejects_late_adds() {
    let fields = vec![FieldType::new(FieldTypeCode::LongLong)];
    let path = std::env::temp_dir().join(format!(
        "tidb_sorted_row_container_contract_{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&path);
    let storage = Arc::new(
        SpillStorage::open(SpillStorageSpec {
            path,
            quota_bytes: -1,
            encryption: SpillEncryptionMethod::Plaintext,
        })
        .expect("spill storage"),
    );
    let mut rows = SortedRowContainer::new(
        &fields,
        4,
        vec![false],
        vec![0],
        vec![get_compare_func(&fields[0])],
        storage,
    );

    let mut first = Chunk::new(&fields, 2, 4);
    first.append_int64(0, 3);
    first.append_int64(0, 1);
    rows.add(first).expect("first chunk");

    let mut second = Chunk::new(&fields, 2, 4);
    second.append_int64(0, 2);
    second.append_int64(0, 1);
    rows.add(second).expect("second chunk");
    rows.spill_to_disk();

    assert!(rows.already_spilled());
    let values = (0..rows.num_row())
        .map(|index| {
            rows.get_sorted_row(index)
                .expect("sorted row")
                .row()
                .get_int64(0)
        })
        .collect::<Vec<_>>();
    assert_eq!(values, vec![1, 1, 2, 3]);

    let late = Chunk::new(&fields, 1, 4);
    let error = rows.add(late).expect_err("sorted rows reject additions");
    assert!(matches!(&error, DiskError::CannotAddBecauseSorted));
    assert_eq!(error.to_string(), "can not add because sorted");
}
