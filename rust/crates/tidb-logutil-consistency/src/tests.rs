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

use super::*;
use tidb_terror::TerrorClass;

/// Each prototype carries the class and code of the Go definition it
/// transcribes (`Class.NewStd(code)`), so `Error()` prints Go's
/// `[class:code]`.
#[test]
fn prototypes_carry_their_go_class_and_code() {
    assert_eq!(ERR_ADMIN_CHECK_INCONSISTENT.class(), TerrorClass::Admin);
    assert_eq!(ERR_ADMIN_CHECK_INCONSISTENT.code().value(), errcode::ErrDataInconsistent as isize);
    assert_eq!(ERR_LOOKUP_INCONSISTENT.class(), TerrorClass::Executor);
    assert_eq!(ERR_LOOKUP_INCONSISTENT.code().value(), errcode::ErrDataInconsistentMismatchCount as isize);
    assert_eq!(ERR_ADMIN_CHECK_INCONSISTENT_WITH_COL_INFO.class(), TerrorClass::Executor);
    assert_eq!(
        ERR_ADMIN_CHECK_INCONSISTENT_WITH_COL_INFO.code().value(),
        errcode::ErrDataInconsistentMismatchIndex as isize
    );
}

fn table() -> TableInfo {
    let mut table = TableInfo::default();
    table.name = tidb_ast::CiString::new("t");
    table
}

fn index() -> IndexInfo {
    let mut index = IndexInfo::default();
    index.name = tidb_ast::CiString::new("idx");
    index
}

fn int_handle(value: i64) -> Handle {
    Handle::Int(tidb_txnkv::IntHandle::new(value))
}

fn reporter<'a>(
    tbl: &'a TableInfo,
    idx: &'a IndexInfo,
    encode: &'a dyn Fn(&Handle) -> Option<Vec<u8>>,
    redact: &str,
) -> Reporter<'a> {
    Reporter { handle_encode: encode, index_encode: None, tbl, idx, enable_redact_log: redact.to_owned(), storage: None }
}

/// Go `ReportLookupInconsistent`'s error.
#[test]
fn lookup_inconsistent_message() {
    let (tbl, idx) = (table(), index());
    let encode = |_: &Handle| None;
    let error = reporter(&tbl, &idx, &encode, "OFF").report_lookup_inconsistent(
        2,
        1,
        &[int_handle(2)],
        &[int_handle(1), int_handle(2)],
        &[],
    );
    assert_eq!(error.message(), "data inconsistency in table: t, index: idx, index-count:2 != record-count:1");
    assert_eq!(error.code().value(), errcode::ErrDataInconsistentMismatchCount as isize);
}

/// Go `ReportAdminCheckInconsistent`: `fmt.Sprint` of the handle and of each
/// `*RecordData` (`""` for nil), quoted by the template's `%#v`.
#[test]
fn admin_check_inconsistent_message() {
    let (tbl, idx) = (table(), index());
    let encode = |_: &Handle| None;
    let tbl_row = RecordData { handle: int_handle(1), values: vec![Datum::Int(1), Datum::Int(2)] };
    let error = reporter(&tbl, &idx, &encode, "OFF").report_admin_check_inconsistent(&int_handle(1), None, Some(&tbl_row));
    assert_eq!(
        error.message(),
        "data inconsistency in table: t, index: idx, handle: 1, index-values:\"\" != record-values:\"handle: 1, values: [KindInt64 1 KindInt64 2]\""
    );
}

/// Go `ReportAdminCheckInconsistentWithColInfo`: a nil comparison error
/// prints `<nil>`.
#[test]
fn admin_check_inconsistent_with_col_info_message() {
    let (tbl, idx) = (table(), index());
    let encode = |_: &Handle| None;
    let error = reporter(&tbl, &idx, &encode, "OFF").report_admin_check_inconsistent_with_col_info(
        &int_handle(1),
        "c",
        &Datum::Int(10).string(),
        &Datum::Int(11).string(),
        None,
        None,
    );
    assert_eq!(
        error.message(),
        "data inconsistency in table: t, index: idx, col: c, handle: \"1\", index-values:\"KindInt64 10\" != record-values:\"KindInt64 11\", compare err:<nil>"
    );
}

/// Go `GetMvccByKey` answers nothing for a nil key or a store without the
/// record.
#[test]
fn get_mvcc_by_key_without_key_or_store() {
    let store = tidb_kv::region_map::LocalRegionStore::default();
    assert_eq!(get_mvcc_by_key(&store, None, None), "");
    assert_eq!(get_mvcc_by_key(&store, Some(b"k"), None), "");
}

/// A store answering one key's MVCC record from one region.
#[derive(Debug)]
struct OneRecord(kvrpcpb::MvccGetByKeyResponse);

impl RegionStore for OneRecord {
    fn split_regions(&self, _: Vec<Vec<u8>>, _: bool, _: Option<i64>) -> (Vec<u64>, Option<String>) {
        (Vec::new(), None)
    }
    fn wait_scatter_region_finish(&self, _: u64, _: i64) -> Result<(), String> {
        Ok(())
    }
    fn check_region_in_scattering(&self, _: u64) -> Result<bool, String> {
        Ok(false)
    }
    fn load_regions_in_key_range(&self, _: &[u8], _: &[u8]) -> Result<Vec<tidb_kv::region_map::RegionMeta>, String> {
        Ok(Vec::new())
    }
    fn mvcc_get_by_key(&self, _: &[u8]) -> Result<kvrpcpb::MvccGetByKeyResponse, String> {
        Ok(self.0.clone())
    }
    fn region_id_by_key(&self, _: &[u8]) -> Result<u64, String> {
        Ok(2)
    }
}

/// Go `GetMvccByKey(store, key, DecodeRowMvccData(tbl))`, captured from a Go
/// testkit run of a row deleted under its unique index (`create table t(a int
/// primary key, b int, c varchar(10), ...)`, `insert ... (1, 2, 'x')`, the
/// record key deleted): the sorted map with the response in its field order.
#[test]
fn get_mvcc_by_key_matches_go() {
    use tidb_datatype::{FieldType, FieldTypeCode};
    let short_value = vec![0x80, 0x00, 0x02, 0x00, 0x00, 0x00, 0x02, 0x03, 0x01, 0x00, 0x02, 0x00, 0x02, 0x78];
    let resp = kvrpcpb::MvccGetByKeyResponse {
        info: Some(kvrpcpb::MvccInfo {
            writes: vec![
                kvrpcpb::MvccWrite {
                    r#type: 1,
                    start_ts: 470_657_238_337_585_159,
                    commit_ts: 470_657_238_337_585_160,
                    ..Default::default()
                },
                kvrpcpb::MvccWrite {
                    start_ts: 470_657_238_337_585_156,
                    commit_ts: 470_657_238_337_585_158,
                    short_value: short_value.clone(),
                    ..Default::default()
                },
            ],
            values: vec![
                kvrpcpb::MvccValue { start_ts: 470_657_238_337_585_159, ..Default::default() },
                kvrpcpb::MvccValue { start_ts: 470_657_238_337_585_156, value: short_value },
            ],
            ..Default::default()
        }),
        ..Default::default()
    };
    let column = |id: i64, name: &str, tp: FieldType| {
        let mut column = tidb_model::ColumnInfo::default();
        column.id = id;
        column.name = tidb_ast::CiString::new(name);
        column.field_type = tp;
        column
    };
    let mut tbl = table();
    tbl.columns = tidb_model::GoSharedPointerSlice::from(vec![
        column(1, "a", FieldType::new(FieldTypeCode::Long)),
        column(2, "b", FieldType::new(FieldTypeCode::Long)),
        column(3, "c", FieldType::new(FieldTypeCode::Varchar)),
    ]);
    let key: Vec<u8> = (0..38).step_by(2).map(|i| u8::from_str_radix(&"7480000000000000755F728000000000000001"[i..i + 2], 16).unwrap()).collect();
    let decode = decode_row_mvcc_data(&tbl);
    assert_eq!(
        get_mvcc_by_key(&OneRecord(resp), Some(&key), Some(&decode)),
        r#"{"decoded":{"470657238337585156":{"b":"2","c":"x"}},"key":"7480000000000000755F728000000000000001","mvcc":{"info":{"writes":[{"type":1,"start_ts":470657238337585159,"commit_ts":470657238337585160},{"start_ts":470657238337585156,"commit_ts":470657238337585158,"short_value":"gAACAAAAAgMBAAIAAng="}],"values":[{"start_ts":470657238337585159},{"start_ts":470657238337585156,"value":"gAACAAAAAgMBAAIAAng="}]}},"regionID":2}"#
    );
}
