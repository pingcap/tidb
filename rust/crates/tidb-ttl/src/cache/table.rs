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

//! SEED of Go `pkg/ttl/cache/table.go`: the physical table a TTL job runs
//! against, and the arithmetic that cuts its key space into scan ranges.
//!
//! [`PhysicalTable`] and its two constructors, `getTableKeyColumns`,
//! `ValidateKeyPrefix`, `FullName`, `ScanRange`, the whole split family
//! (`SplitScanRanges`, `splitIntRanges`, `splitCommonHandleRanges`,
//! `splitRawKeyRanges`, `unsignedEdge`) and all four key-decoding helpers
//! (`GetNextIntHandle`, `GetNextIntDatumFromCommonHandle`,
//! `GetNextBytesHandleDatum`, `GetASCIIPrefixDatumFromBytes`) come across.
//!
//! Narrowings, each named at its own definition site:
//! - `EvalExpireTime` and `(*PhysicalTable).EvalExpireTime` are **absent**.
//!   `// boundary:` `pkg/expression.ParseSimpleExpr` on a
//!   `pkg/expression/exprstatic` context, evaluating
//!   `FROM_UNIXTIME(0) + INTERVAL n MICROSECOND - INTERVAL i unit` and reading
//!   the result back with `EvalTime`. That evaluator is transcreated in
//!   `tidb-expr`, which this crate may not depend on (see the package header).
//!   Reimplementing MySQL interval arithmetic locally would fork behaviour that
//!   already has one authority, so those two functions, `SetMockExpireTime` and
//!   `mockExpireTimeKey` are deliberately missing rather than approximated;
//!   `TTLInfo.IntervalExprStr`/`IntervalTimeUnit` stay reachable through
//!   [`PhysicalTable::table_info`].
//! - [`RegionCache`] `// boundary:` `tikv.Storage`/`tikv.RegionCache`.
//! - [`find_primary_index`] `// boundary:` `pkg/table/tables.FindPrimaryIndex`.
//! - [`keycodec`] `// boundary:` the slice of `pkg/util/codec` and
//!   `pkg/tablecodec` these functions need. Both are transcreated in
//!   `tidb-codec`/`tidb-tablecodec`, but this crate may not add a dependency
//!   edge to them (see the package header), so the exact bytes those two
//!   packages produce for the int, uint and bytes flags are reproduced here and
//!   nowhere else in the crate.
//! - Go's `kv.Handle` return of `GetNextIntHandle` becomes `Option<i64>`: the
//!   function only ever yields `nil` or a `kv.IntHandle`, and every caller
//!   reads `IntValue()`.
//! - `PhysicalTable`'s `KeyColumns`/`TimeColumn` are Go `*model.ColumnInfo`
//!   slices; they are owned values here, so Go's pointer identity with
//!   `TableInfo.Columns` becomes value equality. Only names and field types are
//!   ever read off them. The embedded `*model.TableInfo` keeps pointer identity
//!   because `InfoSchemaCache.newTable` compares it with `==`.

use std::cmp::Ordering;

use tidb_ast::CiString;
use tidb_datatype::{Collation, Datum, FieldType};
use tidb_model::{ColumnInfo, GoShared, IndexInfo, PartitionDefinition, SchemaState, TableInfo};

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
        let record_prefix = keycodec::gen_table_record_prefix(self.id);
        let (start_key, end_key) = keycodec::get_table_handle_key_range(self.id);
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
        let record_prefix = keycodec::gen_table_record_prefix(self.id);
        let start_key = record_prefix.clone();
        let end_key = keycodec::prefix_next(&record_prefix);
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

    let u = keycodec::decode_cmp_uint_to_int(u64::from_be_bytes(encoded_val));
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

    let type_byte = if unsigned {
        keycodec::UINT_FLAG
    } else {
        keycodec::INT_FLAG
    };

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

    let Ok((_, mut v)) = keycodec::decode_one(&encoded_val) else {
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
    if encoded_val[0] < keycodec::BYTES_FLAG {
        return Datum::new_bytes(Vec::new());
    }

    if encoded_val[0] > keycodec::BYTES_FLAG {
        return Datum::Null;
    }

    if let Ok((remain, mut v)) = keycodec::decode_one(encoded_val) {
        if !remain.is_empty() {
            v = Datum::new_bytes(keycodec::key_next(v.go_bytes()));
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

/// `// boundary:` the slice of `pkg/util/codec` and `pkg/tablecodec` the
/// key-decoding helpers above need.
///
/// Both packages are transcreated — `tidb-codec` and `tidb-tablecodec` — but
/// this crate may not add a dependency edge to either (see the package header),
/// so the exact bytes they produce for the memcomparable int, uint and bytes
/// flags are reproduced here. Nothing outside this module reproduces codec
/// behaviour, and the round-trip is pinned by the tests that build keys with
/// [`encode_key`] and read them back through the `get_next_*` helpers.
pub mod keycodec {
    use tidb_datatype::Datum;

    use crate::cache::CacheError;

    /// The failure `codec.DecodeOne`/`codec.DecodeBytes` raise on a key that is
    /// not a well-formed memcomparable encoding.
    fn malformed() -> CacheError {
        CacheError("insufficient bytes to decode value".to_owned())
    }

    /// Go `codec.bytesFlag`.
    pub const BYTES_FLAG: u8 = 1;
    /// Go `codec.intFlag`.
    pub const INT_FLAG: u8 = 3;
    /// Go `codec.uintFlag`.
    pub const UINT_FLAG: u8 = 4;

    /// Go `codec.encGroupSize`.
    const ENC_GROUP_SIZE: usize = 8;
    /// Go `codec.encMarker`.
    const ENC_MARKER: u8 = 0xFF;
    /// Go `codec.signMask`.
    const SIGN_MASK: u64 = 0x8000_0000_0000_0000;

    /// Go `tablecodec.tablePrefix`.
    const TABLE_PREFIX: &[u8] = b"t";
    /// Go `tablecodec.recordPrefixSep`.
    const RECORD_PREFIX_SEP: &[u8] = b"_r";

    /// Go `codec.DecodeCmpUintToInt`.
    #[must_use]
    pub const fn decode_cmp_uint_to_int(value: u64) -> i64 {
        (value ^ SIGN_MASK) as i64
    }

    /// Go `codec.EncodeIntToCmpUint`.
    #[must_use]
    pub const fn encode_int_to_cmp_uint(value: i64) -> u64 {
        (value as u64) ^ SIGN_MASK
    }

    /// Go `codec.EncodeInt`: the 8-byte memcomparable form of a signed int.
    #[must_use]
    pub fn encode_int(value: i64) -> Vec<u8> {
        encode_int_to_cmp_uint(value).to_be_bytes().to_vec()
    }

    /// Go `tablecodec.GenTablePrefix`.
    #[must_use]
    pub fn gen_table_prefix(table_id: i64) -> Vec<u8> {
        let mut key = Vec::with_capacity(TABLE_PREFIX.len() + 8);
        key.extend_from_slice(TABLE_PREFIX);
        key.extend_from_slice(&encode_int(table_id));
        key
    }

    /// Go `tablecodec.GenTableRecordPrefix`.
    #[must_use]
    pub fn gen_table_record_prefix(table_id: i64) -> Vec<u8> {
        let mut key = gen_table_prefix(table_id);
        key.extend_from_slice(RECORD_PREFIX_SEP);
        key
    }

    /// Go `tablecodec.EncodeRowKey`.
    #[must_use]
    pub fn encode_row_key(table_id: i64, encoded_handle: &[u8]) -> Vec<u8> {
        let mut key = gen_table_record_prefix(table_id);
        key.extend_from_slice(encoded_handle);
        key
    }

    /// Go `tablecodec.EncodeRowKeyWithHandle` for a `kv.IntHandle`.
    #[must_use]
    pub fn encode_row_key_with_int_handle(table_id: i64, handle: i64) -> Vec<u8> {
        encode_row_key(table_id, &encode_int(handle))
    }

    /// Go `tablecodec.GetTableHandleKeyRange`.
    #[must_use]
    pub fn get_table_handle_key_range(table_id: i64) -> (Vec<u8>, Vec<u8>) {
        (
            encode_row_key_with_int_handle(table_id, i64::MIN),
            encode_row_key_with_int_handle(table_id, i64::MAX),
        )
    }

    /// Go `kv.Key.Next`.
    #[must_use]
    pub fn key_next(key: &[u8]) -> Vec<u8> {
        let mut next = Vec::with_capacity(key.len() + 1);
        next.extend_from_slice(key);
        next.push(0);
        next
    }

    /// Go `kv.Key.PrefixNext`.
    #[must_use]
    pub fn prefix_next(key: &[u8]) -> Vec<u8> {
        let mut buf = key.to_vec();
        let mut i = buf.len();
        while i > 0 {
            i -= 1;
            buf[i] = buf[i].wrapping_add(1);
            if buf[i] != 0 {
                return buf;
            }
        }
        // Every byte wrapped: Go returns the all-`0xFF` key of the same length.
        vec![0xFF; key.len()]
    }

    /// Go `codec.encodeBytes` prefixed with `bytesFlag`, and the int/uint
    /// counterparts — together they are `codec.EncodeKey` for the datum kinds
    /// a TTL common handle can hold.
    #[must_use]
    pub fn encode_key(values: &[Datum]) -> Vec<u8> {
        let mut buf = Vec::new();
        for value in values {
            match value {
                Datum::Int(v) => {
                    buf.push(INT_FLAG);
                    buf.extend_from_slice(&encode_int(*v));
                }
                Datum::UInt(v) => {
                    buf.push(UINT_FLAG);
                    buf.extend_from_slice(&v.to_be_bytes());
                }
                Datum::Bytes(v) => {
                    buf.push(BYTES_FLAG);
                    encode_bytes(&mut buf, v);
                }
                Datum::String(v) => {
                    buf.push(BYTES_FLAG);
                    encode_bytes(&mut buf, v.bytes());
                }
                other => panic!("unsupported datum kind for a TTL handle: {other:?}"),
            }
        }
        buf
    }

    /// Go `codec.encodeBytes`.
    fn encode_bytes(buf: &mut Vec<u8>, data: &[u8]) {
        let d_len = data.len();
        let mut idx = 0;
        loop {
            let remain = d_len - idx;
            let pad_count;
            if remain >= ENC_GROUP_SIZE {
                buf.extend_from_slice(&data[idx..idx + ENC_GROUP_SIZE]);
                pad_count = 0;
            } else {
                pad_count = ENC_GROUP_SIZE - remain;
                buf.extend_from_slice(&data[idx..]);
                buf.extend(std::iter::repeat_n(0u8, pad_count));
            }
            buf.push(ENC_MARKER - pad_count as u8);
            idx += ENC_GROUP_SIZE;
            if idx > d_len {
                break;
            }
        }
    }

    /// Go `codec.DecodeOne`, limited to the flags a TTL handle can carry.
    ///
    /// Returns the remaining bytes and the decoded datum, as Go does.
    pub fn decode_one(input: &[u8]) -> Result<(&[u8], Datum), CacheError> {
        let (&flag, rest) = input.split_first().ok_or_else(malformed)?;
        match flag {
            INT_FLAG => {
                if rest.len() < 8 {
                    return Err(malformed());
                }
                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(&rest[..8]);
                Ok((
                    &rest[8..],
                    Datum::new_int(decode_cmp_uint_to_int(u64::from_be_bytes(bytes))),
                ))
            }
            UINT_FLAG => {
                if rest.len() < 8 {
                    return Err(malformed());
                }
                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(&rest[..8]);
                Ok((&rest[8..], Datum::new_uint(u64::from_be_bytes(bytes))))
            }
            BYTES_FLAG => {
                let (remain, data) = decode_bytes(rest)?;
                Ok((remain, Datum::new_bytes(data)))
            }
            _ => Err(malformed()),
        }
    }

    /// Go `codec.DecodeBytes`.
    fn decode_bytes(mut input: &[u8]) -> Result<(&[u8], Vec<u8>), CacheError> {
        let mut data = Vec::with_capacity(input.len());
        loop {
            if input.len() < ENC_GROUP_SIZE + 1 {
                return Err(malformed());
            }
            let group = &input[..ENC_GROUP_SIZE];
            let marker = input[ENC_GROUP_SIZE];
            let pad_count = usize::from(ENC_MARKER - marker);
            if pad_count > ENC_GROUP_SIZE {
                return Err(malformed());
            }
            let real_group_size = ENC_GROUP_SIZE - pad_count;
            data.extend_from_slice(&group[..real_group_size]);
            input = &input[ENC_GROUP_SIZE + 1..];
            if pad_count != 0 {
                // Go verifies the padding bytes are all zero.
                if group[real_group_size..].iter().any(|byte| *byte != 0) {
                    return Err(malformed());
                }
                break;
            }
        }
        Ok((input, data))
    }
}
