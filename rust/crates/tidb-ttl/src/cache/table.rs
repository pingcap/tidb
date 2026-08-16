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

//! Go `pkg/ttl/cache/table.go`, complete: the physical table a TTL job runs
//! against, the arithmetic that cuts its key space into scan ranges, and the
//! evaluation of its TTL interval into an expiry instant.
//!
//! Every symbol of the Go file is present. [`PhysicalTable`] and its two
//! constructors, `getTableKeyColumns`, `ValidateKeyPrefix`, `FullName`,
//! `ScanRange`, the whole split family (`SplitScanRanges`, `splitIntRanges`,
//! `splitCommonHandleRanges`, `splitRawKeyRanges`, `unsignedEdge`), all four
//! key-decoding helpers (`GetNextIntHandle`,
//! `GetNextIntDatumFromCommonHandle`, `GetNextBytesHandleDatum`,
//! `GetASCIIPrefixDatumFromBytes`) and the expiry family ([`eval_expire_time`],
//! [`PhysicalTable::eval_expire_time`], [`set_mock_expire_time`],
//! [`MockExpireTimeKey`]) come across.
//!
//! Go's `init()` derives `commonHandleBytesByte`/`IntByte`/`UintByte` by
//! encoding a sample datum through `codec.EncodeKey` and taking the leading
//! flag byte; here those bytes are `tidb_codec`'s `BYTES_FLAG`, `INT_FLAG` and
//! `UINT_FLAG` directly — the same values from the same authority, so the
//! derivation has nothing left to do.
//!
//! Narrowings, each named at its own definition site:
//! - [`RegionCache`] `// boundary:` `tikv.Storage`/`tikv.RegionCache`.
//! - [`find_primary_index`] `// boundary:` `pkg/table/tables.FindPrimaryIndex`.
//! - [`TimeUnitType`] `// boundary:` `pkg/parser/ast.TimeUnitType`, not yet in
//!   `tidb-ast`.
//! - [`MockExpireTimeKey`] `// boundary:` Go's `context.Context`, which this
//!   crate does not carry.
//! - [`eval_expire_time`] carries two further boundaries at its own definition:
//!   the zone type its DST correctness depends on, and the fact that Go's one
//!   `exprstatic.ExprContext` serves as both build and eval context where Rust
//!   splits the two roles across traits it does not implement.
//! - Go's `kv.Handle` return of `GetNextIntHandle` becomes `Option<i64>`: the
//!   function only ever yields `nil` or a `kv.IntHandle`, and every caller
//!   reads `IntValue()`.
//! - `PhysicalTable`'s `KeyColumns`/`TimeColumn` are Go `*model.ColumnInfo`
//!   slices; they are owned values here, so Go's pointer identity with
//!   `TableInfo.Columns` becomes value equality. Only names and field types are
//!   ever read off them. The embedded `*model.TableInfo` keeps pointer identity
//!   because `InfoSchemaCache.newTable` compares it with `==`.

use std::cmp::Ordering;

use chrono::{DateTime, Duration, FixedOffset, Local, TimeZone as _, Utc};
use tidb_ast::CiString;
use tidb_codec::{decode_cmp_uint_to_int, decode_one, BYTES_FLAG, INT_FLAG, UINT_FLAG};
use tidb_datatype::{Collation, Datum, FieldType, SessionTimeZone};
use tidb_expr::exprstatic::ExprContext;
use tidb_expr::rewriter::ZonedNoResolver;
use tidb_expr::simple_expr::{parse_simple_expr, BuildOptions};
use tidb_expr::{eval_expression_once, ZonedNoColumns};
use tidb_model::{ColumnInfo, GoShared, IndexInfo, PartitionDefinition, SchemaState, TableInfo};
use tidb_tablecodec::table_key;
use tidb_txnkv::Key;

use super::{error, Result};

/// Go `getTableKeyColumns`: the cluster-index key columns and their types.
pub fn get_table_key_columns(tbl: &TableInfo) -> Result<(Vec<ColumnInfo>, Vec<FieldType>)> {
    if tbl.pk_is_handle {
        for index in 0..tbl.columns.len() {
            let Some(col) = tbl.columns.get(index) else {
                continue;
            };
            let col = col.read();
            if tidb_mysql::has_pri_key_flag(col.field_type.flags() as usize) {
                return Ok((vec![col.clone()], vec![col.field_type.clone()]));
            }
        }
        return Err(error(format!(
            "Cannot find primary key for table: {}",
            tbl.name.original()
        )));
    }

    if tbl.is_common_handle {
        let idx_info = find_primary_index(tbl)
            .ok_or_else(|| error(format!("Cannot find primary index for table: {}", tbl.name)))?;
        let idx_info = idx_info.read();
        let mut columns = Vec::with_capacity(idx_info.columns.len());
        let mut field_types = Vec::with_capacity(idx_info.columns.len());
        for i in 0..idx_info.columns.len() {
            let idx_col = idx_info
                .columns
                .get(i)
                .ok_or_else(|| error("index column is nil"))?;
            let offset = usize::try_from(idx_col.read().offset)
                .map_err(|_| error("negative index column offset"))?;
            let col = tbl
                .columns
                .get(offset)
                .ok_or_else(|| error("index column offset is out of range"))?;
            let col = col.read();
            columns.push(col.clone());
            field_types.push(col.field_type.clone());
        }
        return Ok((columns, field_types));
    }

    let extra_handle_col_info = ColumnInfo::new_extra_handle_col_info();
    let field_type = extra_handle_col_info.field_type.clone();
    Ok((vec![extra_handle_col_info], vec![field_type]))
}

/// `// boundary:` `pkg/table/tables.FindPrimaryIndex`.
///
/// `pkg/table/tables` is not transcreated, and `table.go` calls exactly this
/// one function from it: the first index flagged primary, or none.
fn find_primary_index(tbl: &TableInfo) -> Option<GoShared<IndexInfo>> {
    (0..tbl.indices.len())
        .filter_map(|i| tbl.indices.get(i))
        .find(|idx| idx.read().primary)
}

/// Go `ScanRange`: the range to scan, `[Start, End)`.
///
/// Go's `nil` bound is the empty vector here; both mean "unbounded on that
/// side", and `newFullRange` produces a range with neither bound.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ScanRange {
    /// Go `Start`.
    pub start: Vec<Datum>,
    /// Go `End`.
    pub end: Vec<Datum>,
}

/// Go `newFullRange`.
#[must_use]
pub fn new_full_range() -> ScanRange {
    ScanRange::default()
}

/// Go `newDatumRange`.
#[must_use]
pub fn new_datum_range(start: Datum, end: Datum) -> ScanRange {
    let mut range = ScanRange::default();
    if !matches!(start, Datum::Null) {
        range.start = vec![start];
    }
    if !matches!(end, Datum::Null) {
        range.end = vec![end];
    }
    range
}

/// Go `nullDatum`.
#[must_use]
pub fn null_datum() -> Datum {
    Datum::Null
}

/// Go `PhysicalTable`: information for a physical table in a TTL job.
#[derive(Debug, Clone)]
pub struct PhysicalTable {
    /// Go `ID`: the physical ID of the table.
    pub id: i64,
    /// Go `Schema`: the database name of the table.
    pub schema: CiString,
    /// Go's embedded `*model.TableInfo`.
    pub table_info: GoShared<TableInfo>,
    /// Go `Partition`: the partition name.
    pub partition: CiString,
    /// Go `PartitionDef`: the partition definition.
    pub partition_def: Option<PartitionDefinition>,
    /// Go `KeyColumns`: the cluster index key columns for the table.
    pub key_columns: Vec<ColumnInfo>,
    /// Go `KeyColumnTypes`: the types of the key columns.
    pub key_column_types: Vec<FieldType>,
    /// Go `TimeColumn`: the time column used for TTL.
    pub time_column: Option<ColumnInfo>,
}

impl Default for PhysicalTable {
    fn default() -> Self {
        Self {
            id: 0,
            schema: CiString::default(),
            table_info: GoShared::new(TableInfo::default()),
            partition: CiString::default(),
            partition_def: None,
            key_columns: Vec::new(),
            key_column_types: Vec::new(),
            time_column: None,
        }
    }
}

impl PhysicalTable {
    /// Go's promoted `TableInfo.Name`.
    #[must_use]
    pub fn name(&self) -> CiString {
        self.table_info.read().name.clone()
    }

    /// Go's promoted `TableInfo.Name.O`.
    #[must_use]
    pub fn name_original(&self) -> String {
        self.table_info.read().name.original().to_string()
    }

    /// Go's `ttlTable.TableInfo == tblInfo` pointer comparison in
    /// `InfoSchemaCache.newTable`.
    #[must_use]
    pub fn table_info_ptr_eq(&self, other: &GoShared<TableInfo>) -> bool {
        self.table_info.ptr_eq(other)
    }

    /// Go `(*PhysicalTable).ValidateKeyPrefix`.
    pub fn validate_key_prefix(&self, key: &[Datum]) -> Result<()> {
        if key.len() > self.key_columns.len() {
            return Err(error(format!(
                "invalid key length: {}, expected {}",
                key.len(),
                self.key_columns.len()
            )));
        }
        Ok(())
    }

    /// Go `(*PhysicalTable).FullName`.
    #[must_use]
    pub fn full_name(&self) -> String {
        if !self.partition.lowercase().is_empty() {
            return format!(
                "{}.{}.{}",
                self.schema.original(),
                self.name_original(),
                self.partition.original()
            );
        }
        format!("{}.{}", self.schema.original(), self.name_original())
    }

    pub(crate) fn time_column_ref(&self) -> Result<&ColumnInfo> {
        self.time_column
            .as_ref()
            .ok_or_else(|| error("the table has no TTL time column"))
    }
}

/// Go `NewBasePhysicalTable`: a new `PhysicalTable` with a specific time column.
pub fn new_base_physical_table(
    schema: CiString,
    tbl: &GoShared<TableInfo>,
    partition: CiString,
    time_column: Option<ColumnInfo>,
) -> Result<PhysicalTable> {
    let info = tbl.read();
    if info.state != SchemaState::PUBLIC {
        return Err(error(format!(
            "table '{}.{}' is not a public table",
            schema, info.name
        )));
    }

    let (key_columns, key_column_types) = get_table_key_columns(&info)?;

    let physical_id;
    let mut partition_def = None;
    match info.partition.as_ref() {
        None => {
            if !partition.lowercase().is_empty() {
                return Err(error(format!(
                    "table '{}.{}' is not a partitioned table",
                    schema, info.name
                )));
            }
            physical_id = info.id;
        }
        Some(partition_info) => {
            if partition.lowercase().is_empty() {
                return Err(error(format!(
                    "partition name is required, table '{}.{}' is a partitioned table",
                    schema, info.name
                )));
            }

            let definitions = partition_info.read().definitions.snapshot();
            for def in definitions {
                if def.name.lowercase() == partition.lowercase() {
                    partition_def = Some(def);
                }
            }

            let Some(def) = partition_def.as_ref() else {
                return Err(error(format!(
                    "partition '{}' is not found in ttl table '{}.{}'",
                    partition.original(),
                    schema,
                    info.name
                )));
            };

            physical_id = def.id;
        }
    }

    Ok(PhysicalTable {
        id: physical_id,
        schema,
        table_info: tbl.clone(),
        partition,
        partition_def,
        key_columns,
        key_column_types,
        time_column,
    })
}

/// Go `NewPhysicalTable`.
pub fn new_physical_table(
    schema: CiString,
    tbl: &GoShared<TableInfo>,
    partition: CiString,
) -> Result<PhysicalTable> {
    let (column_name, table_name) = {
        let info = tbl.read();
        let Some(ttl_info) = info.ttl_info.as_ref() else {
            return Err(error(format!(
                "table '{}.{}' is not a ttl table",
                schema, info.name
            )));
        };
        let column_name = ttl_info.read().column_name.clone();
        (column_name, info.name.clone())
    };

    let time_column = tbl
        .read()
        .find_public_column_by_name(column_name.lowercase())
        .map(|col| col.read().clone());

    let Some(time_column) = time_column else {
        return Err(error(format!(
            "time column '{}' is not public in ttl table '{}.{}'",
            column_name, schema, table_name
        )));
    };

    new_base_physical_table(schema, tbl, partition, Some(time_column))
}

// -------------------------------------------------------------------------
// Scan-range splitting
// -------------------------------------------------------------------------

/// Go's `tikv.KeyLocation` as `splitRawKeyRanges` uses it: the two bounds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyLocation {
    /// Go `KeyLocation.StartKey`.
    pub start_key: Vec<u8>,
    /// Go `KeyLocation.EndKey`.
    pub end_key: Vec<u8>,
}

/// Go `kv.KeyRange`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyRange {
    /// Go `KeyRange.StartKey`.
    pub start_key: Vec<u8>,
    /// Go `KeyRange.EndKey`.
    pub end_key: Vec<u8>,
}

/// `// boundary:` `tikv.Storage`'s `GetRegionCache()` and
/// `(*tikv.RegionCache).LocateKeyRange`.
///
/// `client-go` is not transcreated here, and `splitRawKeyRanges` calls exactly
/// one method on the region cache. Go's `NewBackofferWithVars(ctx, maxSleep,
/// nil)` argument is a retry budget with no observable effect on the returned
/// ranges, so it does not appear.
///
/// Go's own tests drive this through a mock PD client; the fixture in
/// `tests/cache_test.rs` implements this trait directly for the same purpose.
pub trait RegionCache {
    /// Go `(*tikv.RegionCache).LocateKeyRange`.
    fn locate_key_range(&self, start_key: &[u8], end_key: &[u8]) -> Result<Vec<KeyLocation>>;
}

/// Go `unsignedEdge`.
#[must_use]
pub fn unsigned_edge(d: &Datum) -> Datum {
    match d {
        Datum::Null => Datum::new_uint(1 << 63),
        Datum::Int(0) => null_datum(),
        Datum::Int(value) => Datum::new_uint(*value as u64),
        other => other.clone(),
    }
}

impl PhysicalTable {
    /// Go `(*PhysicalTable).SplitScanRanges`.
    ///
    /// Go type-asserts the `kv.Storage` to a `tikv.Storage` and falls back to a
    /// single full range when that fails; `None` is that failed assertion.
    pub fn split_scan_ranges(
        &self,
        store: Option<&dyn RegionCache>,
        split_cnt: i64,
    ) -> Result<Vec<ScanRange>> {
        if self.key_columns.is_empty() || split_cnt <= 1 {
            return Ok(vec![new_full_range()]);
        }

        let Some(store) = store else {
            return Ok(vec![new_full_range()]);
        };

        let ft = self.key_columns[0].field_type.clone();
        let flags = ft.flags() as usize;
        match ft.code().mysql_type() {
            tidb_mysql::TypeTiny
            | tidb_mysql::TypeShort
            | tidb_mysql::TypeLong
            | tidb_mysql::TypeLonglong
            | tidb_mysql::TypeInt24 => {
                if self.key_columns.len() > 1 {
                    return self.split_common_handle_ranges(
                        store,
                        split_cnt,
                        true,
                        tidb_mysql::has_unsigned_flag(flags),
                        None,
                    );
                }
                self.split_int_ranges(store, split_cnt)
            }
            tidb_mysql::TypeBit => {
                self.split_common_handle_ranges(store, split_cnt, false, false, None)
            }
            tidb_mysql::TypeString | tidb_mysql::TypeVarString | tidb_mysql::TypeVarchar => {
                let mut decode: Option<fn(&[u8]) -> Datum> = None;
                if !tidb_mysql::has_binary_flag(flags) {
                    match ft.charset_name() {
                        // ASCII and Latin1 are 8-bit charset, we can use
                        // GetASCIIPrefixDatumFromBytes to decode it.
                        "ascii" | "latin1" => decode = Some(get_ascii_prefix_datum_from_bytes),
                        "utf8" | "utf8mb4" => {
                            // We can only use GetASCIIPrefixDatumFromBytes to
                            // decode UTF8 and UTF8MB4 when they are "utf8_bin"
                            // or "utf8mb4_bin" collation.
                            if matches!(
                                ft.collation_name(),
                                "utf8_bin" | "utf8mb4_bin" | "utf8mb4_0900_bin"
                            ) {
                                decode = Some(get_ascii_prefix_datum_from_bytes);
                            }
                        }
                        _ => {}
                    }
                    if decode.is_none() {
                        return Ok(vec![new_full_range()]);
                    }
                }
                self.split_common_handle_ranges(store, split_cnt, false, false, decode)
            }
            _ => Ok(vec![new_full_range()]),
        }
    }

    /// Go `(*PhysicalTable).splitIntRanges`.
    fn split_int_ranges(&self, store: &dyn RegionCache, split_cnt: i64) -> Result<Vec<ScanRange>> {
        let record_prefix = table_key::gen_table_record_prefix(self.id);
        let (start_key, end_key) = table_key::get_table_handle_key_range(self.id);
        let key_ranges = self.split_raw_key_ranges(store, &start_key, &end_key, split_cnt)?;

        if key_ranges.len() <= 1 {
            return Ok(vec![new_full_range()]);
        }

        let unsigned = tidb_mysql::has_unsigned_flag(self.key_column_types[0].flags() as usize);
        let mut scan_ranges = Vec::with_capacity(key_ranges.len() + 1);
        let mut cur_scan_start = null_datum();
        for (i, key_range) in key_ranges.iter().enumerate() {
            if i != 0 && matches!(cur_scan_start, Datum::Null) {
                break;
            }

            let mut cur_scan_end = null_datum();
            if i < key_ranges.len() - 1 {
                if let Some(val) = get_next_int_handle(&key_range.end_key, &record_prefix) {
                    cur_scan_end = Datum::new_int(val);
                }
            }

            if let (Datum::Int(start), Datum::Int(end)) = (&cur_scan_start, &cur_scan_end) {
                if start >= end {
                    continue;
                }
            }

            if !unsigned {
                // primary key is signed or range
                scan_ranges.push(new_datum_range(
                    cur_scan_start.clone(),
                    cur_scan_end.clone(),
                ));
            } else if matches!(&cur_scan_start, Datum::Int(v) if *v >= 0) {
                // primary key is unsigned and range is in the right half side
                scan_ranges.push(new_datum_range(
                    unsigned_edge(&cur_scan_start),
                    unsigned_edge(&cur_scan_end),
                ));
            } else if matches!(&cur_scan_end, Datum::Int(v) if *v <= 0) {
                // primary key is unsigned and range is in the left half side
                scan_ranges.push(new_datum_range(
                    unsigned_edge(&cur_scan_start),
                    unsigned_edge(&cur_scan_end),
                ));
            } else {
                // primary key is unsigned and the start > math.MaxInt64 && end <
                // math.MaxInt64, we must split it to two ranges
                scan_ranges.push(new_datum_range(
                    unsigned_edge(&cur_scan_start),
                    null_datum(),
                ));
                scan_ranges.push(new_datum_range(null_datum(), unsigned_edge(&cur_scan_end)));
            }
            cur_scan_start = cur_scan_end;
        }
        Ok(scan_ranges)
    }

    /// Go `(*PhysicalTable).splitCommonHandleRanges`.
    fn split_common_handle_ranges(
        &self,
        store: &dyn RegionCache,
        split_cnt: i64,
        is_int: bool,
        unsigned: bool,
        decode: Option<fn(&[u8]) -> Datum>,
    ) -> Result<Vec<ScanRange>> {
        let record_prefix = table_key::gen_table_record_prefix(self.id);
        let start_key = record_prefix.clone();
        let end_key = Key::from_bytes(record_prefix.clone())
            .prefix_next()
            .into_bytes();
        let key_ranges = self.split_raw_key_ranges(store, &start_key, &end_key, split_cnt)?;

        if key_ranges.len() <= 1 {
            return Ok(vec![new_full_range()]);
        }

        let mut scan_ranges = Vec::with_capacity(key_ranges.len());
        let mut cur_scan_start = null_datum();
        for (i, key_range) in key_ranges.iter().enumerate() {
            let mut cur_scan_end = null_datum();
            if i != key_ranges.len() - 1 {
                if is_int {
                    cur_scan_end = get_next_int_datum_from_common_handle(
                        &key_range.end_key,
                        &record_prefix,
                        unsigned,
                    );
                } else {
                    cur_scan_end = get_next_bytes_handle_datum(&key_range.end_key, &record_prefix);
                    if let Some(decode) = decode {
                        cur_scan_end = decode(cur_scan_end.go_bytes());
                    }

                    // "" is the smallest value for string/[]byte, skip to add
                    // it to ranges.
                    if cur_scan_end.go_bytes().is_empty() {
                        continue;
                    }
                }
            }

            if !matches!(cur_scan_start, Datum::Null) && !matches!(cur_scan_end, Datum::Null) {
                // Sometimes curScanStart >= curScanEnd because the edge datum
                // is an approximate value. At this time, we should skip this
                // range to ensure the incremental of ranges.
                let cmp = cur_scan_start
                    .compare(&cur_scan_end, Collation::Binary)
                    .map_err(|err| error(err.to_string()))?;
                if cmp != Ordering::Less {
                    continue;
                }
            }

            scan_ranges.push(new_datum_range(
                cur_scan_start.clone(),
                cur_scan_end.clone(),
            ));
            if matches!(cur_scan_end, Datum::Null) {
                break;
            }
            cur_scan_start = cur_scan_end;
        }
        Ok(scan_ranges)
    }

    /// Go `(*PhysicalTable).splitRawKeyRanges`.
    ///
    /// Go closes with an informational log line naming the table and the region
    /// counts; the logging sink is outside this crate's dependency set, so the
    /// computation comes across and the log line does not.
    fn split_raw_key_ranges(
        &self,
        store: &dyn RegionCache,
        start_key: &[u8],
        end_key: &[u8],
        split_cnt: i64,
    ) -> Result<Vec<KeyRange>> {
        let regions = store.locate_key_range(start_key, end_key)?;

        let regions_cnt = i64::try_from(regions.len()).unwrap_or(i64::MAX);
        let regions_per_range = regions_cnt / split_cnt;
        let mut oversize_cnt = regions_cnt % split_cnt;
        let mut ranges =
            Vec::with_capacity(usize::try_from(regions_cnt.min(split_cnt)).unwrap_or(0));

        let mut remaining = regions.as_slice();
        while !remaining.is_empty() {
            let start_region = &remaining[0];

            let mut end_region_idx = regions_per_range - 1;
            if oversize_cnt > 0 {
                end_region_idx += 1;
            }
            // The counts satisfy `regionsCnt == regionsPerRange*splitCnt +
            // oversizeCnt`, so while regions remain either `regionsPerRange > 0`
            // or `oversizeCnt > 0` and the index is in range, exactly as Go's
            // unguarded indexing assumes.
            let Ok(end_region_idx) = usize::try_from(end_region_idx) else {
                break;
            };
            let Some(end_region) = remaining.get(end_region_idx) else {
                break;
            };

            let mut range_start_key = start_region.start_key.clone();
            if range_start_key.as_slice() < start_key {
                range_start_key = start_key.to_vec();
            }

            let mut range_end_key = end_region.end_key.clone();
            if range_end_key.as_slice() > end_key {
                range_end_key = end_key.to_vec();
            }

            ranges.push(KeyRange {
                start_key: range_start_key,
                end_key: range_end_key,
            });
            oversize_cnt -= 1;
            remaining = &remaining[end_region_idx + 1..];
        }
        Ok(ranges)
    }
}

// -------------------------------------------------------------------------
// Key decoding
// -------------------------------------------------------------------------

/// Go `GetNextIntHandle`, used for int handle tables.
///
/// It returns the min handle whose encoded key is or after argument `key`. If
/// it cannot find a valid value, `None` is returned — Go returns a `nil`
/// `kv.Handle` there, and a `kv.IntHandle` otherwise.
#[must_use]
pub fn get_next_int_handle(key: &[u8], record_prefix: &[u8]) -> Option<i64> {
    if key > record_prefix && !key.starts_with(record_prefix) {
        return None;
    }

    if key <= record_prefix {
        return Some(i64::MIN);
    }

    let suffix = &key[record_prefix.len()..];
    let mut encoded_val = [0u8; 8];
    let copy_len = suffix.len().min(8);
    encoded_val[..copy_len].copy_from_slice(&suffix[..copy_len]);

    let find_next = suffix.len() > 8;

    let u = decode_cmp_uint_to_int(u64::from_be_bytes(encoded_val));
    if !find_next {
        return Some(u);
    }

    if u == i64::MAX {
        return None;
    }

    Some(u + 1)
}

/// Go `GetNextIntDatumFromCommonHandle`, used for common handle tables with an
/// int value.
///
/// It returns the min handle whose encoded key is or after argument `key`. If
/// it cannot find a valid value, a null datum is returned.
#[must_use]
pub fn get_next_int_datum_from_common_handle(
    key: &[u8],
    record_prefix: &[u8],
    unsigned: bool,
) -> Datum {
    if key > record_prefix && !key.starts_with(record_prefix) {
        return Datum::Null;
    }

    let type_byte = if unsigned { UINT_FLAG } else { INT_FLAG };

    let min_datum = if unsigned {
        Datum::new_uint(0)
    } else {
        Datum::new_int(i64::MIN)
    };

    if key <= record_prefix {
        return min_datum;
    }

    let mut encoded_val = key[record_prefix.len()..].to_vec();
    if encoded_val[0] < type_byte {
        return min_datum;
    }

    if encoded_val[0] > type_byte {
        return Datum::Null;
    }

    let original_len = encoded_val.len();
    if original_len < 9 {
        encoded_val.resize(9, 0);
    }

    let Ok((_, mut v)) = decode_one(&encoded_val) else {
        // should never happen; Go logs the annotated error and returns null.
        return null_datum();
    };

    if encoded_val.len() > 9 {
        match &v {
            Datum::UInt(value) if unsigned && *value == u64::MAX => return Datum::Null,
            Datum::Int(value) if !unsigned && *value == i64::MAX => return Datum::Null,
            _ => {}
        }

        v = match v {
            Datum::UInt(value) => Datum::new_uint(value + 1),
            Datum::Int(value) => Datum::new_int(value + 1),
            other => other,
        };
    }

    v
}

/// Go `GetNextBytesHandleDatum`, used for a table with one binary or string
/// column common handle.
///
/// It returns the min value whose encoded key is or after argument `key`. If it
/// cannot find a valid value, a null datum is returned.
#[must_use]
pub fn get_next_bytes_handle_datum(key: &[u8], record_prefix: &[u8]) -> Datum {
    if key > record_prefix && !key.starts_with(record_prefix) {
        return Datum::Null;
    }

    if key <= record_prefix {
        return Datum::new_bytes(Vec::new());
    }

    let encoded_val = &key[record_prefix.len()..];
    if encoded_val[0] < BYTES_FLAG {
        return Datum::new_bytes(Vec::new());
    }

    if encoded_val[0] > BYTES_FLAG {
        return Datum::Null;
    }

    if let Ok((remain, mut v)) = decode_one(encoded_val) {
        if !remain.is_empty() {
            v = Datum::new_bytes(Key::from_bytes(v.go_bytes().to_vec()).next().into_bytes());
        }
        return v;
    }

    let encoded_val = &encoded_val[1..];
    let mut broken_group_end_idx = encoded_val.len() as isize - 1;
    let mut broken_group_empty_bytes = encoded_val.len() % 9;
    let mut i = 7;
    while i + 1 < encoded_val.len() {
        let empty_bytes = 255 - usize::from(encoded_val[i + 1]);
        if empty_bytes != 0 || i + 1 == encoded_val.len() - 1 {
            broken_group_end_idx = i as isize;
            broken_group_empty_bytes = empty_bytes;
            break;
        }
        i += 9;
    }

    for _ in 0..broken_group_empty_bytes {
        if broken_group_end_idx < 0 || encoded_val[broken_group_end_idx as usize] > 0 {
            break;
        }
        broken_group_end_idx -= 1;
    }

    if broken_group_end_idx < 0 {
        // Go's `d.SetBytes(nil)`, a non-null bytes datum with no content.
        return Datum::new_bytes(Vec::new());
    }

    let val: Vec<u8> = encoded_val[..=broken_group_end_idx as usize]
        .iter()
        .enumerate()
        .filter_map(|(i, byte)| (i % 9 != 8).then_some(*byte))
        .collect();
    Datum::new_bytes(val)
}

/// Go `GetASCIIPrefixDatumFromBytes`: converts bytes to a string datum holding
/// only the ASCII prefix.
///
/// The ASCII prefix string only contains visible characters and `\t`, `\n`,
/// `\r`:
/// `"abc" -> "abc"`, `"\0abc" -> ""`, `"ab\x01c" -> "ab"`, `"ab\xffc" -> "ab"`,
/// `"ab\rc\xff" -> "ab\rc"`.
#[must_use]
pub fn get_ascii_prefix_datum_from_bytes(bs: &[u8]) -> Datum {
    let mut bs = bs;
    for (i, c) in bs.iter().enumerate() {
        if (0x20..=0x7E).contains(c) {
            // visible characters from ` ` to `~`
            continue;
        }

        if *c == b'\t' || *c == b'\n' || *c == b'\r' {
            continue;
        }

        bs = &bs[..i];
        break;
    }
    Datum::new_string(bs.to_vec())
}

// -------------------------------------------------------------------------
// Expiry evaluation
// -------------------------------------------------------------------------

/// Go `ast.TimeUnitType`, the closed set of `INTERVAL` units.
///
/// `// boundary:` `pkg/parser/ast.TimeUnitType` itself. `tidb-ast` does not
/// carry that enum yet, and `TTLInfo.IntervalTimeUnit` is the raw `int` Go
/// stores, so the discriminants below are pinned to Go's `iota` order and
/// [`TimeUnitType::as_str`] reproduces Go's `String()` exactly — those two
/// properties are the whole of what `EvalExpireTime` reads. When the enum
/// lands in `tidb-ast`, this becomes a re-export.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i64)]
pub enum TimeUnitType {
    /// Go `TimeUnitInvalid`.
    Invalid = 0,
    /// Go `TimeUnitMicrosecond`.
    Microsecond = 1,
    /// Go `TimeUnitSecond`.
    Second = 2,
    /// Go `TimeUnitMinute`.
    Minute = 3,
    /// Go `TimeUnitHour`.
    Hour = 4,
    /// Go `TimeUnitDay`.
    Day = 5,
    /// Go `TimeUnitWeek`.
    Week = 6,
    /// Go `TimeUnitMonth`.
    Month = 7,
    /// Go `TimeUnitQuarter`.
    Quarter = 8,
    /// Go `TimeUnitYear`.
    Year = 9,
    /// Go `TimeUnitSecondMicrosecond`.
    SecondMicrosecond = 10,
    /// Go `TimeUnitMinuteMicrosecond`.
    MinuteMicrosecond = 11,
    /// Go `TimeUnitMinuteSecond`.
    MinuteSecond = 12,
    /// Go `TimeUnitHourMicrosecond`.
    HourMicrosecond = 13,
    /// Go `TimeUnitHourSecond`.
    HourSecond = 14,
    /// Go `TimeUnitHourMinute`.
    HourMinute = 15,
    /// Go `TimeUnitDayMicrosecond`.
    DayMicrosecond = 16,
    /// Go `TimeUnitDaySecond`.
    DaySecond = 17,
    /// Go `TimeUnitDayMinute`.
    DayMinute = 18,
    /// Go `TimeUnitDayHour`.
    DayHour = 19,
    /// Go `TimeUnitYearMonth`.
    YearMonth = 20,
}

impl TimeUnitType {
    /// Go's `ast.TimeUnitType(t.TTLInfo.IntervalTimeUnit)` conversion. An
    /// out-of-range value becomes `TimeUnitInvalid`, which is what Go's
    /// numeric conversion of an unknown `int` behaves as everywhere the unit
    /// is only rendered.
    #[must_use]
    pub const fn from_i64(value: i64) -> Self {
        match value {
            1 => Self::Microsecond,
            2 => Self::Second,
            3 => Self::Minute,
            4 => Self::Hour,
            5 => Self::Day,
            6 => Self::Week,
            7 => Self::Month,
            8 => Self::Quarter,
            9 => Self::Year,
            10 => Self::SecondMicrosecond,
            11 => Self::MinuteMicrosecond,
            12 => Self::MinuteSecond,
            13 => Self::HourMicrosecond,
            14 => Self::HourSecond,
            15 => Self::HourMinute,
            16 => Self::DayMicrosecond,
            17 => Self::DaySecond,
            18 => Self::DayMinute,
            19 => Self::DayHour,
            20 => Self::YearMonth,
            _ => Self::Invalid,
        }
    }

    /// Go `(TimeUnitType).String()`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Invalid => "",
            Self::Microsecond => "MICROSECOND",
            Self::Second => "SECOND",
            Self::Minute => "MINUTE",
            Self::Hour => "HOUR",
            Self::Day => "DAY",
            Self::Week => "WEEK",
            Self::Month => "MONTH",
            Self::Quarter => "QUARTER",
            Self::Year => "YEAR",
            Self::SecondMicrosecond => "SECOND_MICROSECOND",
            Self::MinuteMicrosecond => "MINUTE_MICROSECOND",
            Self::MinuteSecond => "MINUTE_SECOND",
            Self::HourMicrosecond => "HOUR_MICROSECOND",
            Self::HourSecond => "HOUR_SECOND",
            Self::HourMinute => "HOUR_MINUTE",
            Self::DayMicrosecond => "DAY_MICROSECOND",
            Self::DaySecond => "DAY_SECOND",
            Self::DayMinute => "DAY_MINUTE",
            Self::DayHour => "DAY_HOUR",
            Self::YearMonth => "YEAR_MONTH",
        }
    }
}

impl std::fmt::Display for TimeUnitType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Go `mockExpireTimeKey`: the unexported `context.Context` key the test-only
/// expiry override is stored under.
///
/// `// boundary:` Go's `context.Context`. This crate carries no context (see
/// [`crate::session`]'s header for the same narrowing on `PhaseTracer`), so
/// the key becomes the value type the context would have carried, passed
/// explicitly to [`PhysicalTable::eval_expire_time`]. The empty default is the
/// "context carries no mock time" case Go's type assertion fails on, and
/// [`set_mock_expire_time`] is the only way to fill it — so, exactly as in Go,
/// a caller that never calls it cannot observe the override.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MockExpireTimeKey<Tz: chrono::TimeZone>(Option<DateTime<Tz>>);

impl<Tz: chrono::TimeZone> Default for MockExpireTimeKey<Tz> {
    fn default() -> Self {
        Self(None)
    }
}

impl<Tz: chrono::TimeZone> MockExpireTimeKey<Tz> {
    /// Go's `ctx.Value(mockExpireTimeKey{}).(time.Time)`, whose comma-ok is
    /// this `Option`.
    #[must_use]
    pub fn get(&self) -> Option<&DateTime<Tz>> {
        self.0.as_ref()
    }
}

/// Go `SetMockExpireTime`: can only be used in test.
#[must_use]
pub fn set_mock_expire_time<Tz: chrono::TimeZone>(
    ctx: &MockExpireTimeKey<Tz>,
    tm: DateTime<Tz>,
) -> MockExpireTimeKey<Tz> {
    // Go derives a child context from `ctx`; the parent carries nothing else
    // this crate reads, so deriving is replacing.
    let _ = ctx;
    MockExpireTimeKey(Some(tm))
}

/// Go's `time.Time.Truncate(time.Second)`.
///
/// Go truncates toward the zero time, which is a whole number of seconds
/// before every instant reachable here, so this is "drop the sub-second part,
/// rounding down" — not "round toward zero", which would differ before 1970.
fn truncate_to_second<Tz: chrono::TimeZone>(value: DateTime<Tz>) -> DateTime<Tz> {
    let nanos = i64::from(value.timestamp_subsec_nanos());
    value - Duration::nanoseconds(nanos)
}

/// Go `EvalExpireTime`: the expired time.
///
/// Go builds `FROM_UNIXTIME(0) + INTERVAL <start micros> MICROSECOND -
/// INTERVAL <interval> <unit>` with `expression.ParseSimpleExpr` on an
/// `exprstatic.NewExprContext()`, reads the result back with `EvalTime`, and
/// shifts `now` by the difference. Both halves of the evaluator are
/// transcreated: [`tidb_expr::simple_expr::parse_simple_expr`] and
/// [`tidb_expr::exprstatic`].
///
/// # The zone parameter is Go's `*time.Location`
///
/// Go's `time.Time` carries its location, and `now.Add(d)` re-derives the
/// offset at the NEW instant — which is the entire point of the "avoid time
/// shift caused by DST" comment this function is built around. `chrono`
/// reconstructs a `DateTime`'s zone from its `Offset`, so that property holds
/// only for a `Tz` whose `Offset` carries the zone back: `chrono_tz::Tz` (Go's
/// `time.LoadLocation`), [`chrono::FixedOffset`] (Go's `time.FixedZone`),
/// `Utc` and `Local` all do.
///
/// `// boundary:` [`tidb_datatype::SessionTimeZone`] does NOT — its
/// `Offset::from_offset` rebuilds a `Fixed` zone, so a `Named` arm silently
/// freezes its offset across the arithmetic below and answers `01:31` where Go
/// answers `00:31` for `2024-03-10 03:01:00 America/Los_Angeles` less ninety
/// minutes. [`PhysicalTable::eval_expire_time`] therefore splits the session's
/// [`tidb_util::timeutil::TimeZone`] into a concrete `Tz` before calling here
/// rather than passing that union through.
///
/// `// boundary:` Go passes the one `exprstatic.ExprContext` as both the build
/// and the eval context. In Rust the two roles are separate traits
/// (`tidb_expr::rewriter::ColumnResolver` and `tidb_expr::Columns`) that
/// `exprstatic::ExprContext` does not implement, so the context is still built
/// here — it is what pins the evaluation zone, and Go asserts exactly that
/// (`intest.Assert(exprCtx.GetEvalCtx().Location() == time.UTC)`) — and its
/// location is threaded into the zone-carrying resolver/eval pair.
pub fn eval_expire_time<Tz: chrono::TimeZone>(
    now: &DateTime<Tz>,
    interval: &str,
    unit: TimeUnitType,
) -> Result<DateTime<Tz>> {
    // Firstly, we should use the UTC time zone to compute the expired time to
    // avoid time shift caused by DST. The start time should be a time with the
    // same datetime string as `now` but it is in the UTC timezone.
    let start = Utc
        .from_local_datetime(&now.naive_local())
        .single()
        .ok_or_else(|| error("the current time has no UTC representation"))?;

    let expr_ctx = ExprContext::new([]);
    // we need to set the location to UTC to make sure the time is in the same
    // timezone as the start time.
    let location = to_session_time_zone(expr_ctx.get_eval_ctx().location());
    debug_assert!(location.is_utc(), "exprstatic's default location is UTC");

    let sql = format!(
        "FROM_UNIXTIME(0) + INTERVAL {} MICROSECOND - INTERVAL {interval} {unit}",
        start.timestamp_micros()
    );

    let expr = parse_simple_expr(
        &ZonedNoResolver::new(location.clone()),
        &sql,
        &BuildOptions::new(),
    )
    .map_err(|err| error(err.to_string()))?;

    // Go `expr.EvalTime(exprCtx.GetEvalCtx(), chunk.Row{})`.
    let value = eval_expression_once(&expr, &ZonedNoColumns(location))
        .map_err(|err| error(format!("{err:?}")))?;
    let Datum::Time(time) = value else {
        return Err(error(format!(
            "the TTL interval expression did not evaluate to a time: {value:?}"
        )));
    };

    // Go `tm.GoTime(time.UTC)`.
    let end = time
        .core_time()
        .to_datetime(&Utc)
        .map_err(|err| error(err.to_string()))?;

    // Then we should add the duration between the time get from the previous
    // SQL and the start time to the now time. Truncate to second to make sure
    // the precision is always the same with the one stored in a table to avoid
    // some comparing problems in testing.
    Ok(truncate_to_second(now.clone() + (end - start)))
}

/// `tidb_util::timeutil::TimeZone` -> `tidb_datatype::SessionTimeZone`: Go's
/// one `*time.Location` split across two Rust types, joined arm for arm.
fn to_session_time_zone(zone: &tidb_util::timeutil::TimeZone) -> SessionTimeZone {
    match zone {
        tidb_util::timeutil::TimeZone::Local => SessionTimeZone::Local,
        tidb_util::timeutil::TimeZone::Named(zone) => SessionTimeZone::Named(*zone),
        tidb_util::timeutil::TimeZone::Fixed { name, offset_secs } => SessionTimeZone::Fixed {
            name: name.clone(),
            offset_secs: *offset_secs,
        },
    }
}

impl PhysicalTable {
    /// Go `(*PhysicalTable).EvalExpireTime`: the expired time for the current
    /// time.
    ///
    /// It uses the global timezone in session to evaluate the context and the
    /// returned time is in the same timezone as the `now` argument.
    pub fn eval_expire_time<Tz, S>(
        &self,
        ctx: &MockExpireTimeKey<Tz>,
        se: &S,
        now: &DateTime<Tz>,
    ) -> Result<DateTime<Tz>>
    where
        Tz: chrono::TimeZone,
        S: crate::session::Session,
    {
        // Go guards this on `intest.InTest`; the value only exists when a test
        // put it there, so the guard and the lookup collapse into one.
        if let Some(tm) = ctx.get() {
            return Ok(tm.clone());
        }

        // Use the global time zone to compute expire time. Different timezones
        // may have different results even with the same "now" time and TTL
        // expression. Consider a TTL setting with the expiration
        // `INTERVAL 1 MONTH`. If the current timezone is `Asia/Shanghai` and
        // now is `2021-03-01 00:00:00 +0800` the expired time should be
        // `2021-02-01 00:00:00 +0800`, corresponding to UTC time
        // `2021-01-31 16:00:00 UTC`. But if we use the `UTC` time zone, the
        // current time is `2021-02-28 16:00:00 UTC`, and the expired time
        // should be `2021-01-28 16:00:00 UTC` that is not the same as the
        // previous one.
        let global_tz = se
            .global_time_zone()
            .map_err(|err| error(err.to_string()))?;

        let (interval, unit) = {
            let info = self.table_info.read();
            let ttl_info = info
                .ttl_info
                .as_ref()
                .ok_or_else(|| error("the table has no TTL info"))?;
            let ttl_info = ttl_info.read();
            (
                ttl_info.interval_expr_str.clone(),
                TimeUnitType::from_i64(ttl_info.interval_time_unit),
            )
        };

        // Go's `now.In(globalTz)` on a single `*time.Location`. The union is
        // split into a concrete zone type here so daylight saving survives the
        // arithmetic -- see [`eval_expire_time`]'s zone boundary.
        let instant = now.naive_utc().and_utc();
        let expire = match &global_tz {
            tidb_util::timeutil::TimeZone::Named(zone) => {
                eval_expire_time(&instant.with_timezone(zone), &interval, unit)?.naive_utc()
            }
            tidb_util::timeutil::TimeZone::Local => {
                eval_expire_time(&instant.with_timezone(&Local), &interval, unit)?.naive_utc()
            }
            tidb_util::timeutil::TimeZone::Fixed { offset_secs, .. } => {
                let offset = FixedOffset::east_opt(*offset_secs)
                    .ok_or_else(|| error("the global time zone offset is out of range"))?;
                eval_expire_time(&instant.with_timezone(&offset), &interval, unit)?.naive_utc()
            }
        };

        // Go `expire.In(now.Location())`.
        Ok(expire.and_utc().with_timezone(&now.timezone()))
    }
}
