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

//! Go `pkg/util/logutil/consistency` (`reporter.go`): a data inconsistency
//! found by an index lookup or an admin check is logged with the row's and
//! the index entry's MVCC history under the session's redaction mode, and
//! returned as Go's error.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, LazyLock};

use serde_json::{Map, Value as JsonValue};
use tidb_datatype::Datum;
use tidb_dbterror::catalog_support::{std_err, Class};
use tidb_errno::errcode;
use tidb_kv::region_map::RegionStore;
use tidb_log::{Field, Value};
use tidb_model::index::IndexInfo;
use tidb_model::table_info::TableInfo;
use tidb_mysql::errors::FormatArg;
use tidb_terror::TerrorError;
use tidb_txnkv::Handle;
use tikv_client::proto::kvrpcpb;

/// Go `ErrAdminCheckInconsistent` returns for data inconsistency for admin
/// check.
pub static ERR_ADMIN_CHECK_INCONSISTENT: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Admin, errcode::ErrDataInconsistent));
/// Go `ErrLookupInconsistent` returns for data inconsistency for index
/// lookup.
pub static ERR_LOOKUP_INCONSISTENT: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Executor, errcode::ErrDataInconsistentMismatchCount));
/// Go `ErrAdminCheckInconsistentWithColInfo` returns for data inconsistency
/// for admin check but with column info.
pub static ERR_ADMIN_CHECK_INCONSISTENT_WITH_COL_INFO: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Executor, errcode::ErrDataInconsistentMismatchIndex));

/// Go `errors.RedactLogEnable`.
const REDACT_LOG_ENABLE: &str = "ON";

/// Go `GetMVCCByKeyResp` gets the MVCC resp.
pub fn get_mvcc_by_key_resp(store: &dyn RegionStore, key: &[u8]) -> Result<kvrpcpb::MvccGetByKeyResponse, String> {
    store.mvcc_get_by_key(key)
}

/// What a decode function adds to the MVCC record Go logs.
pub type DecodeMvccFn<'a> = dyn Fn(&[u8], &kvrpcpb::MvccGetByKeyResponse, &mut Map<String, JsonValue>) + 'a;

/// Go `GetMvccByKey` gets the MVCC value by key, and returns a json string
/// including decoded data.
pub fn get_mvcc_by_key(store: &dyn RegionStore, key: Option<&[u8]>, decode_mvcc_fn: Option<&DecodeMvccFn<'_>>) -> String {
    let Some(key) = key else {
        return String::new();
    };
    let Ok(mvcc_resp) = get_mvcc_by_key_resp(store, key) else {
        return String::new();
    };
    let region_id = get_region_id_by_key(store, key);
    let decode_key: String = key.iter().map(|byte| format!("{byte:02X}")).collect();

    // Go `json.Marshal(map[string]any)`: the keys in sorted order, "mvcc"
    // the response struct in its field order.
    let mut resp = Map::new();
    resp.insert("key".to_owned(), JsonValue::String(decode_key));
    resp.insert("regionID".to_owned(), JsonValue::from(region_id));
    if let Some(decode) = decode_mvcc_fn {
        decode(key, &mvcc_resp, &mut resp);
    }
    let mut fields: Vec<(String, String)> = resp
        .into_iter()
        .map(|(name, value)| (name, value.to_string()))
        .collect();
    fields.push(("mvcc".to_owned(), tidb_kv::mvcc_json::marshal(&mvcc_resp)));
    fields.sort_by(|a, b| a.0.cmp(&b.0));
    let body: Vec<String> = fields
        .into_iter()
        .map(|(name, value)| format!("{}:{value}", JsonValue::String(name)))
        .collect();
    let rj = format!("{{{}}}", body.join(","));
    const MAX_MVCC_INFO_LEN: usize = 5000;
    if rj.len() > MAX_MVCC_INFO_LEN {
        let cut = &rj.as_bytes()[..MAX_MVCC_INFO_LEN];
        return format!("{}[truncated]...", String::from_utf8_lossy(cut));
    }
    rj
}

/// Go `getRegionIDByKey`.
fn get_region_id_by_key(store: &dyn RegionStore, encoded_key: &[u8]) -> u64 {
    store.region_id_by_key(encoded_key).unwrap_or(0)
}

/// Go `RecordData` is the record data composed of a handle and values.
#[derive(Clone, Debug)]
pub struct RecordData {
    /// Go `Handle`.
    pub handle: Handle,
    /// Go `Values`.
    pub values: Vec<Datum>,
}

impl fmt::Display for RecordData {
    /// Go `RecordData.String`: `fmt.Sprint` of the handle and of the values
    /// (each datum's `String`, space-separated).
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let values: Vec<String> = self.values.iter().map(Datum::string).collect();
        write!(formatter, "handle: {}, values: [{}]", self.handle, values.join(" "))
    }
}

/// Go `fmt.Sprint(r)` of a `*RecordData`: the empty string for nil.
fn record_string(record: Option<&RecordData>) -> String {
    record.map_or_else(String::new, ToString::to_string)
}

/// Go `Reporter` is a helper to generate report.
pub struct Reporter<'a> {
    /// Go `HandleEncode`.
    pub handle_encode: &'a dyn Fn(&Handle) -> Option<Vec<u8>>,
    /// Go `IndexEncode`; nil when the reporter logs no index entry.
    pub index_encode: Option<&'a dyn Fn(&RecordData) -> Option<Vec<u8>>>,
    /// Go `Tbl`.
    pub tbl: &'a TableInfo,
    /// Go `Idx`.
    pub idx: &'a IndexInfo,
    /// Go `EnableRedactLog`.
    pub enable_redact_log: String,
    /// Go `Storage`, when it is a `helper.Storage`.
    pub storage: Option<Arc<dyn RegionStore>>,
}

/// Go `DecodeRowMvccData` creates a closure that captures the tableInfo to
/// be used a decode function in `GetMvccByKey`.
#[must_use]
pub fn decode_row_mvcc_data(table_info: &TableInfo) -> Box<DecodeMvccFn<'_>> {
    Box::new(move |_key, resp_value, out_map| {
        let col_map: BTreeMap<i64, tidb_datatype::FieldType> = table_info
            .columns
            .iter_deref()
            .map(|col| {
                let col = col.read();
                (col.id, col.field_type.clone())
            })
            .collect();
        let Some(info) = &resp_value.info else {
            return;
        };
        let mut err = None;
        let mut datas: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
        for w in &info.writes {
            if !w.short_value.is_empty() {
                let (record, error) = decode_mvcc_record_value(&w.short_value, &col_map, table_info);
                datas.insert(w.start_ts.to_string(), record);
                err = error;
            }
        }
        for v in &info.values {
            if !v.value.is_empty() {
                let (record, error) = decode_mvcc_record_value(&v.value, &col_map, table_info);
                datas.insert(v.start_ts.to_string(), record);
                err = error;
            }
        }
        if !datas.is_empty() {
            out_map.insert("decoded".to_owned(), serde_json::to_value(datas).unwrap_or(JsonValue::Null));
            if let Some(err) = err {
                out_map.insert("decode_error".to_owned(), JsonValue::String(err));
            }
        }
    })
}

/// Go `DecodeIndexMvccData` creates a closure that captures the indexInfo to
/// be used a decode function in `GetMvccByKey`.
#[must_use]
pub fn decode_index_mvcc_data(index_info: &IndexInfo) -> Box<DecodeMvccFn<'_>> {
    Box::new(move |key, resp_value, out_map| {
        let Some(info) = &resp_value.info else {
            return;
        };
        let columns = index_info.columns.len();
        let mut err = None;
        let mut datas: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
        let mut decode = |start_ts: u64, value: &[u8]| match tidb_tablecodec::decode_index_handle(key, value, columns) {
            Ok(handle) => {
                err = None;
                let handle = handle.map_or_else(String::new, |handle| handle.to_string());
                datas.insert(start_ts.to_string(), BTreeMap::from([("handle".to_owned(), handle)]));
            }
            Err(error) => err = Some(error.to_string()),
        };
        for w in &info.writes {
            if !w.short_value.is_empty() {
                decode(w.start_ts, &w.short_value);
            }
        }
        for v in &info.values {
            if !v.value.is_empty() {
                decode(v.start_ts, &v.value);
            }
        }
        if !datas.is_empty() {
            out_map.insert("decoded".to_owned(), serde_json::to_value(datas).unwrap_or(JsonValue::Null));
            if let Some(err) = err {
                out_map.insert("decode_error".to_owned(), JsonValue::String(err));
            }
        }
    })
}

/// Go `decodeMvccRecordValue`: each decoded column by name, `nil` for NULL.
fn decode_mvcc_record_value(
    bs: &[u8],
    col_map: &BTreeMap<i64, tidb_datatype::FieldType>,
    tb: &TableInfo,
) -> (BTreeMap<String, String>, Option<String>) {
    let utc = tidb_datatype::SessionTimeZone::utc();
    let (rs, mut err) = match tidb_tablecodec::decode_table_row_to_map(bs, col_map, Some(&utc)) {
        Ok(rs) => (rs, None),
        Err(error) => (BTreeMap::new(), Some(error.to_string())),
    };
    let mut record = BTreeMap::new();
    for col in tb.columns.iter_deref() {
        let col = col.read();
        if let Some(c) = rs.get(&col.id) {
            let data = if c.is_null() {
                "nil".to_owned()
            } else {
                match c.sql_string() {
                    Ok(data) => data,
                    Err(error) => {
                        err = Some(error.to_string());
                        String::new()
                    }
                }
            };
            record.insert(col.name.original().to_owned(), data);
        }
    }
    (record, err)
}

impl Reporter<'_> {
    fn redact(&self, input: &str) -> String {
        tidb_redact::string(&self.enable_redact_log, input)
    }

    fn mvcc(&self, store: &dyn RegionStore, key: Option<Vec<u8>>, decode: &DecodeMvccFn<'_>) -> String {
        self.redact(&get_mvcc_by_key(store, key.as_deref(), Some(decode)))
    }

    fn handle_key(&self, handle: &Handle) -> Option<Vec<u8>> {
        (self.handle_encode)(handle)
    }

    fn index_key(&self, idx_row: &RecordData) -> Option<Vec<u8>> {
        self.index_encode.and_then(|encode| encode(idx_row))
    }

    /// Go `ReportLookupInconsistent` reports inconsistent when index rows is
    /// more than record rows.
    #[must_use]
    pub fn report_lookup_inconsistent(
        &self,
        idx_cnt: usize,
        tbl_cnt: usize,
        miss_hd: &[Handle],
        full_hd: &[Handle],
        miss_row_idx: &[RecordData],
    ) -> TerrorError {
        const MAX_FULL_HANDLE_CNT: usize = 50;
        let display_full_hd_cnt = full_hd.len().min(MAX_FULL_HANDLE_CNT);
        let mut fs = vec![
            Field::new("table_name", Value::Str(self.tbl.name.original().to_owned())),
            Field::new("index_name", Value::Str(self.idx.name.original().to_owned())),
            Field::new("index_cnt", Value::I64(idx_cnt as i64)),
            Field::new("table_cnt", Value::I64(tbl_cnt as i64)),
            Field::new("missing_handles", Value::Str(self.redact(&sprint_handles(miss_hd)))),
            Field::new("total_handles", Value::Str(self.redact(&sprint_handles(&full_hd[..display_full_hd_cnt])))),
        ];
        if self.enable_redact_log != REDACT_LOG_ENABLE {
            if let Some(store) = &self.storage {
                let decode_row = decode_row_mvcc_data(self.tbl);
                for (i, hd) in miss_hd.iter().enumerate() {
                    fs.push(Field::new(
                        format!("row_mvcc_{i}"),
                        Value::Str(self.mvcc(store.as_ref(), self.handle_key(hd), &decode_row)),
                    ));
                }
                let decode_index = decode_index_mvcc_data(self.idx);
                for (i, row) in miss_row_idx.iter().enumerate() {
                    fs.push(Field::new(
                        format!("index_mvcc_{i}"),
                        Value::Str(self.mvcc(store.as_ref(), self.index_key(row), &decode_index)),
                    ));
                }
            }
        }
        fs.push(stack_field());
        tidb_logutil::bg_logger().error("indexLookup found data inconsistency", &fs);
        ERR_LOOKUP_INCONSISTENT.gen_with_stack_by_args(&[
            self.tbl.name.original().into(),
            self.idx.name.original().into(),
            (idx_cnt as i64).into(),
            (tbl_cnt as i64).into(),
        ])
    }

    /// Go `ReportAdminCheckInconsistentWithColInfo` reports inconsistent when
    /// the value of index row is different from record row. `idx_dat` and
    /// `tbl_dat` are Go's `fmt.Stringer`s already rendered, `err` the
    /// comparison's error (`nil` when absent).
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn report_admin_check_inconsistent_with_col_info(
        &self,
        handle: &Handle,
        col_name: &str,
        idx_dat: &str,
        tbl_dat: &str,
        err: Option<&str>,
        idx_row: Option<&RecordData>,
    ) -> TerrorError {
        let mut fs = vec![
            Field::new("table_name", Value::Str(self.tbl.name.original().to_owned())),
            Field::new("index_name", Value::Str(self.idx.name.original().to_owned())),
            Field::new("col", Value::Str(col_name.to_owned())),
            Field::new("row_id", Value::Str(self.redact(&handle.to_string()))),
            Field::new("idxDatum", Value::Str(self.redact(idx_dat))),
            Field::new("rowDatum", Value::Str(self.redact(tbl_dat))),
        ];
        if self.enable_redact_log != REDACT_LOG_ENABLE {
            if let Some(store) = &self.storage {
                fs.push(Field::new(
                    "row_mvcc",
                    Value::Str(self.mvcc(store.as_ref(), self.handle_key(handle), &decode_row_mvcc_data(self.tbl))),
                ));
                let index_key = idx_row.and_then(|row| self.index_key(row));
                fs.push(Field::new(
                    "index_mvcc",
                    Value::Str(self.mvcc(store.as_ref(), index_key, &decode_index_mvcc_data(self.idx))),
                ));
            }
        }
        fs.push(Field::new("error", Value::Str(err.unwrap_or("<nil>").to_owned())));
        fs.push(stack_field());
        tidb_logutil::bg_logger().error("admin check found data inconsistency", &fs);
        ERR_ADMIN_CHECK_INCONSISTENT_WITH_COL_INFO.gen_with_stack_by_args(&[
            self.tbl.name.original().into(),
            self.idx.name.original().into(),
            col_name.into(),
            handle.to_string().into(),
            idx_dat.into(),
            tbl_dat.into(),
            err.map_or_else(FormatArg::nil, FormatArg::from),
        ])
    }

    /// Go `ReportAdminCheckInconsistent` reports inconsistent when single
    /// index row not found in record rows.
    #[must_use]
    pub fn report_admin_check_inconsistent(
        &self,
        handle: &Handle,
        idx_row: Option<&RecordData>,
        tbl_row: Option<&RecordData>,
    ) -> TerrorError {
        let mut fs = vec![
            Field::new("table_name", Value::Str(self.tbl.name.original().to_owned())),
            Field::new("index_name", Value::Str(self.idx.name.original().to_owned())),
            Field::new("row_id", Value::Str(self.redact(&handle.to_string()))),
            Field::new("index", Value::Str(self.redact(&record_string(idx_row)))),
            Field::new("row", Value::Str(self.redact(&record_string(tbl_row)))),
        ];
        if let Handle::Int(int) = handle {
            fs.push(Field::new("int_handle", Value::I64(int.value())));
        }
        if let Some(store) = &self.storage {
            let mut add_mvcc_fields = |title: &str, mvcc_resp: &kvrpcpb::MvccGetByKeyResponse| {
                let Some(info) = &mvcc_resp.info else { return };
                for (i, w) in info.writes.iter().enumerate() {
                    let mut w_clone = w.clone();
                    w_clone.short_value = Vec::new();
                    let json = tidb_kv::mvcc_json::marshal_write(&w_clone);
                    fs.push(Field::new(format!("{title}_mvcc_write_{i}"), Value::Str(json)));
                }
                for (i, v) in info.values.iter().enumerate() {
                    let mut v_clone = v.clone();
                    v_clone.value = Vec::new();
                    let json = tidb_kv::mvcc_json::marshal_value(&v_clone);
                    fs.push(Field::new(format!("{title}_mvcc_value_{i}"), Value::Str(json)));
                }
            };
            if let Some(key) = self.handle_key(handle) {
                if let Ok(hdl_mvcc_resp) = get_mvcc_by_key_resp(store.as_ref(), &key) {
                    add_mvcc_fields("row", &hdl_mvcc_resp);
                }
            }
            if let Some(idx_row) = idx_row {
                if let Some(key) = self.index_key(idx_row) {
                    if let Ok(idx_mvcc_resp) = get_mvcc_by_key_resp(store.as_ref(), &key) {
                        add_mvcc_fields("index", &idx_mvcc_resp);
                    }
                }
            }
            if self.enable_redact_log != REDACT_LOG_ENABLE {
                fs.push(Field::new(
                    "row_mvcc",
                    Value::Str(self.mvcc(store.as_ref(), self.handle_key(handle), &decode_row_mvcc_data(self.tbl))),
                ));
                if let Some(idx_row) = idx_row {
                    fs.push(Field::new(
                        "index_mvcc",
                        Value::Str(self.mvcc(store.as_ref(), self.index_key(idx_row), &decode_index_mvcc_data(self.idx))),
                    ));
                }
            }
        }
        fs.push(stack_field());
        tidb_logutil::bg_logger().error("admin check found data inconsistency", &fs);
        ERR_ADMIN_CHECK_INCONSISTENT.gen_with_stack_by_args(&[
            self.tbl.name.original().into(),
            self.idx.name.original().into(),
            handle.to_string().into(),
            record_string(idx_row).into(),
            record_string(tbl_row).into(),
        ])
    }
}

/// Go `fmt.Sprint([]kv.Handle)`.
fn sprint_handles(handles: &[Handle]) -> String {
    let rendered: Vec<String> = handles.iter().map(ToString::to_string).collect();
    format!("[{}]", rendered.join(" "))
}

/// Go `zap.Stack("stack")`.
fn stack_field() -> Field {
    Field::new("stack", Value::Str(std::backtrace::Backtrace::force_capture().to_string()))
}

#[cfg(test)]
mod tests;
