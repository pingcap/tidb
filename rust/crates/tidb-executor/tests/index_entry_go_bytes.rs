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

//! Go-authoritative index entry bytes for the LIVE write path.
//!
//! Every expected byte string here was produced by
//! `rust/difftests/transaction-tests/fixtures/generate_index_entries.go`
//! calling `tablecodec.GenIndexKey` / `tablecodec.GenIndexValuePortal` with
//! the `needRestoredData` and handle-restored-data decisions
//! `pkg/table/tables/index.go` makes.
//!
//! This suite exists because a round-trip test structurally cannot see the
//! bug it guards: an index KEY is a new-collation SORT KEY, which case-folds
//! and trims, so the entry VALUE is the only place the original bytes
//! survive. A writer that stores its own simpler value round-trips against
//! its own reader perfectly and still hands a Go reader -- an index-only
//! scan, `ADMIN CHECK INDEX`, a DDL backfill -- corrupted data.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use tidb_datatype::{Collation, Datum, FieldType, FieldTypeCode};
use tidb_executor::storage::{MemTableStorage, StorageError, StorageIterator, TableStorage};
use tidb_executor::{IndexRange, KvColumn, KvIndex, KvTable};
use tidb_txnkv::Key;

const FIXTURE: &str =
    include_str!("../../../difftests/transaction-tests/fixtures/index_entries.hex");

const TABLE_ID: i64 = 77;
/// Go `types.UnspecifiedLength`: a key part that stores its whole column.
const WHOLE_COLUMN: i64 = -1;
/// Go `mysql.PriKeyFlag`.
const PRI_KEY_FLAG: u32 = 1 << 1;

fn fixture(name: &str) -> Vec<u8> {
    let prefix = format!("{name}=");
    let hex = FIXTURE
        .lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .unwrap_or_else(|| panic!("fixture has no {name} entry"));
    assert!(hex.len() % 2 == 0, "{name} is not whole bytes");
    hex.as_bytes()
        .chunks_exact(2)
        .map(|pair| (nibble(pair[0]) << 4) | nibble(pair[1]))
        .collect()
}

fn nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        other => panic!("fixture has non-hex byte {other:#x}"),
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// A [`TableStorage`] that mirrors every write, so a test can read back the
/// exact bytes the table stored rather than only the keys.
#[derive(Debug)]
struct Mirror {
    inner: MemTableStorage,
    written: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
}

impl TableStorage for Mirror {
    fn get(&mut self, key: &Key) -> Result<Vec<u8>, StorageError> {
        self.inner.get(key)
    }

    fn set(&mut self, key: Key, value: Vec<u8>) -> Result<(), StorageError> {
        self.written
            .lock()
            .unwrap()
            .insert(key.as_bytes().to_vec(), value.clone());
        self.inner.set(key, value)
    }

    fn delete(&mut self, key: Key) -> Result<(), StorageError> {
        self.written.lock().unwrap().remove(key.as_bytes());
        self.inner.delete(key)
    }

    fn iter(
        &mut self,
        start: Option<&Key>,
        upper_bound: Option<&Key>,
    ) -> Result<Box<dyn StorageIterator>, StorageError> {
        self.inner.iter(start, upper_bound)
    }

    fn key_count(&self) -> usize {
        self.inner.key_count()
    }

    fn clear(&mut self) {
        self.written.lock().unwrap().clear();
        self.inner.clear();
    }

    fn clone_box(&self) -> Box<dyn TableStorage> {
        Box::new(Mirror {
            inner: MemTableStorage::new(),
            written: Arc::clone(&self.written),
        })
    }
}

type Written = Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>;

fn table(columns: Vec<KvColumn>) -> (KvTable, Written) {
    table_with_collation(columns, true)
}

fn table_with_collation(columns: Vec<KvColumn>, use_new_collation: bool) -> (KvTable, Written) {
    let written: Written = Arc::new(Mutex::new(BTreeMap::new()));
    let store = Mirror {
        inner: MemTableStorage::new(),
        written: Arc::clone(&written),
    };
    (
        KvTable::with_storage_and_collation(TABLE_ID, columns, Box::new(store), use_new_collation),
        written,
    )
}

fn column(id: i64, name: &str, field_type: FieldType) -> KvColumn {
    KvColumn {
        name: name.to_owned(),
        id,
        field_type,
        column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
        default_value: None,
        origin_default: None,
        comment: String::new(),
        generated: None,
    }
}

fn varchar(collation: Collation) -> FieldType {
    let mut field_type = FieldType::new(FieldTypeCode::Varchar).with_collation(collation);
    field_type.set_flen(32);
    field_type
}

fn index(id: i64, unique: bool, offsets: Vec<usize>) -> KvIndex {
    let prefix_lengths = vec![WHOLE_COLUMN; offsets.len()];
    KvIndex {
        id,
        name: "idx".to_owned(),
        comment: String::new(),
        unique,
        column_offsets: offsets,
        prefix_lengths,
        visible: true,
        global: false,
        clustered_primary: false,
    }
}

/// The one index entry `written` holds, once the record key is set aside.
fn only_index_entry(written: &Written) -> (Vec<u8>, Vec<u8>) {
    let entries = written.lock().unwrap();
    let mut found: Vec<(Vec<u8>, Vec<u8>)> = entries
        .iter()
        // `_i` marks an index key, `_r` a record key.
        .filter(|(key, _)| key.get(10) == Some(&b'i'))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    assert_eq!(found.len(), 1, "expected exactly one index entry");
    found.pop().unwrap()
}

fn assert_entry(name: &str, written: &Written) {
    let (key, value) = only_index_entry(written);
    assert_eq!(
        hex(&key),
        hex(&fixture(&format!("{name}_key"))),
        "{name}: index key"
    );
    assert_eq!(
        hex(&value),
        hex(&fixture(&format!("{name}_value"))),
        "{name}: index value"
    );
}

/// A `utf8mb4_general_ci` sort key CASE-FOLDS, so the key of `'A'` holds
/// `0x41` folded to `0x41`'s weight and the row's own spelling survives only
/// in the entry value's restored data.
#[test]
fn a_unique_case_insensitive_entry_carries_gos_restored_data() {
    let (mut t, written) = table(vec![column(2, "s", varchar(Collation::Utf8Mb4GeneralCi))]);
    t.add_index(index(1, true, vec![0]), false);
    t.insert_row(
        &[Datum::new_collation_string(
            b"A".to_vec(),
            Collation::Utf8Mb4GeneralCi,
        )],
        &tidb_expr::NoColumns,
    )
    .unwrap();
    assert_entry("unique_general_ci_A", &written);
}

/// Go's table object captures one encoder mode and uses it for both halves of
/// every index entry. Legacy mode stores raw string keys without restored
/// data; new mode stores lossy sort keys and the value that restores them.
#[test]
fn one_captured_mode_drives_both_kv_index_key_and_value() {
    let columns = vec![column(2, "s", varchar(Collation::Utf8Mb4GeneralCi))];
    let (mut legacy, legacy_written) = table_with_collation(columns.clone(), false);
    let (mut modern, modern_written) = table_with_collation(columns, true);
    legacy.add_index(index(1, true, vec![0]), false);
    modern.add_index(index(1, true, vec![0]), false);
    let row = [Datum::new_collation_string(
        b"A".to_vec(),
        Collation::Utf8Mb4GeneralCi,
    )];

    let legacy_handle = legacy.insert_row(&row, &tidb_expr::NoColumns).unwrap();
    let modern_handle = modern.insert_row(&row, &tidb_expr::NoColumns).unwrap();
    let (legacy_key, legacy_value) = only_index_entry(&legacy_written);
    let (modern_key, modern_value) = only_index_entry(&modern_written);

    assert_ne!(legacy_key, modern_key, "the key format follows the mode");
    assert_ne!(
        legacy_value, modern_value,
        "the restored-data value follows the same mode"
    );
    assert_entry("unique_general_ci_A", &modern_written);

    let point = IndexRange {
        low: row.to_vec(),
        high: row.to_vec(),
        low_exclusive: false,
        high_exclusive: false,
    };
    assert_eq!(legacy_handle, modern_handle);
    let zone = tidb_datatype::SessionTimeZone::utc();
    assert_eq!(
        legacy.scan_index_range(1, &point, &zone).unwrap(),
        vec![legacy_handle],
        "legacy range bounds use the legacy entry's key format"
    );
    assert_eq!(
        modern.scan_index_range(1, &point, &zone).unwrap(),
        vec![modern_handle],
        "new-collation range bounds use the new entry's key format"
    );
}

/// The same column under a NON-unique index: the handle moves into the key,
/// and the value is the v0-extensible restored-data form -- not the single
/// `'0'` byte a restore-free index stores.
#[test]
fn a_non_unique_case_insensitive_entry_is_not_the_bare_marker_byte() {
    let (mut t, written) = table(vec![column(2, "s", varchar(Collation::Utf8Mb4GeneralCi))]);
    t.add_index(index(1, false, vec![0]), false);
    t.insert_row(
        &[Datum::new_collation_string(
            b"A".to_vec(),
            Collation::Utf8Mb4GeneralCi,
        )],
        &tidb_expr::NoColumns,
    )
    .unwrap();
    assert_entry("non_unique_general_ci_A", &written);
}

/// `utf8mb4_bin` is still a restoring collation for a VARCHAR: its sort key
/// TRIMS trailing spaces, so `'a '` and `'a'` share one key and only the
/// restored data tells them apart.
#[test]
fn a_bin_collation_entry_restores_the_trailing_space_its_key_trimmed() {
    let (mut t, written) = table(vec![column(2, "s", varchar(Collation::Utf8Mb4Bin))]);
    t.add_index(index(1, true, vec![0]), false);
    t.insert_row(
        &[Datum::new_collation_string(
            b"a ".to_vec(),
            Collation::Utf8Mb4Bin,
        )],
        &tidb_expr::NoColumns,
    )
    .unwrap();
    assert_entry("unique_utf8mb4_bin_a_space", &written);
}

/// A clustered `DECIMAL` primary key encodes to four bytes and is padded to
/// nine; the entry key carries the PADDED form, and a version-1
/// common-handle table's value is its three version bytes rather than `'0'`.
#[test]
fn a_short_common_handle_entry_matches_gos_padded_key_and_v1_value() {
    let mut decimal = FieldType::new(FieldTypeCode::NewDecimal);
    decimal.set_flen(4);
    decimal.set_decimal(0);
    decimal.add_flags(PRI_KEY_FLAG);
    let (mut t, written) = table(vec![
        column(1, "pk", decimal),
        column(2, "v", FieldType::new(FieldTypeCode::LongLong)),
    ]);
    t.set_common_handle_offsets(vec![0]);
    t.add_index(index(2, false, vec![1]), false);
    t.insert_row(
        &[
            Datum::Decimal(tidb_datatype::Decimal::from_int(5)),
            Datum::Int(42),
        ],
        &tidb_expr::NoColumns,
    )
    .unwrap();
    assert_entry("short_common_handle_non_unique", &written);
}

/// A version-1 common-handle table repeats the PRIMARY KEY's restored data in
/// every secondary index entry, so an index-only read can rebuild the handle
/// columns as well as the indexed one.
#[test]
fn a_clustered_varchar_handle_restores_itself_into_every_secondary_entry() {
    let mut pk = varchar(Collation::Utf8Mb4GeneralCi);
    pk.add_flags(PRI_KEY_FLAG);
    let (mut t, written) = table(vec![
        column(1, "pk", pk),
        column(2, "s", varchar(Collation::Utf8Mb4GeneralCi)),
    ]);
    t.set_common_handle_offsets(vec![0]);
    t.add_index(index(2, false, vec![1]), false);
    t.insert_row(
        &[
            Datum::new_collation_string(b"Key".to_vec(), Collation::Utf8Mb4GeneralCi),
            Datum::new_collation_string(b"Val".to_vec(), Collation::Utf8Mb4GeneralCi),
        ],
        &tidb_expr::NoColumns,
    )
    .unwrap();
    assert_entry("common_handle_v1_restored", &written);
}
