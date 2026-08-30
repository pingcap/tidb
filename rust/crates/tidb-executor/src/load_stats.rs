// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! `LOAD STATS 'file.json'`: a statistics dump back into the estimator.
//!
//! Go splits this statement across three layers, and each half is mirrored at
//! its own place here so the JOINTS stay visible:
//!
//! * `pkg/executor/load_stats.go` (`LoadStatsInfo.Update`) parses the JSON
//!   into `statsutil.JSONTable` and hands it to the statistics handle. The
//!   parse and the shape live in this file.
//! * `pkg/statistics/handle/storage/json.go` (`TableStatsFromJSON`) turns one
//!   `JSONTable` into a `statistics.Table`: histograms decoded from their
//!   proto form, CMSketch/TopN rebuilt, stats version resolved.
//!   [`statistics_table_from_json`] is that function; the planner's reduced
//!   view is derived from the resulting canonical table.
//! * `pkg/statistics/handle/storage/stats_read_writer.go`
//!   (`LoadStatsFromJSONNoUpdate`) resolves WHICH physical tables the dump
//!   feeds -- the table itself, or its partitions by name plus the `global`
//!   entry. That routing needs the session's catalog, so it lives in the
//!   session arm (`tidb_session`'s `load_stats_arm`), exactly as Go keeps it
//!   outside `json.go`.
//!
//! # What the bytes in the dump are
//!
//! The JSON is Go's `encoding/json` marshalling of `statsutil.JSONTable`,
//! whose histograms are `tipb.Histogram` messages -- so every `[]byte` field
//! (`lower_bound`, `upper_bound`, a TopN entry's `data`) arrives as BASE64.
//! What the decoded bytes MEAN differs by object, and confusing the two is
//! the silent-corruption path:
//!
//! * A COLUMN histogram was dumped through `col.ConvertTo(..., TypeBlob)`
//!   (`dumpJSONCol`), so each bound is the value's STRING FORM -- `100` is
//!   the three ASCII bytes `"100"`, a datetime is `"2023-12-31 23:50:00"`.
//!   Loading reverses it: `TableStatsFromJSON` runs `hist.ConvertTo` back to
//!   the column's own field type, EXCEPT for string columns (any `ETString`
//!   type that is not ENUM or SET), whose bounds are collation SORT KEYS and
//!   stay bytes -- Go substitutes `TypeBlob` there precisely so no charset
//!   validation mangles a key that is not text. This matches what this
//!   tier's own `ANALYZE` stores: `analyze.rs` keeps string samples as
//!   `Datum::Bytes(collation.key(..))` and everything else as the typed
//!   datum, so a loaded histogram and a computed one estimate identically.
//! * An INDEX histogram's bounds are the index's KEY BYTES
//!   (`codec.EncodeKey` output) and are never converted -- Go's index arm has
//!   no `ConvertTo` call at all. They load as `Datum::Bytes` verbatim, which
//!   is the same shape `analyze.rs` builds from `plan.index_sample`.
//! * TopN `data` bytes are `codec.EncodeKey`-encoded values for columns and
//!   raw index key bytes for indexes; both round-trip untouched
//!   (`TopNFromProto` copies), and both match what the estimator encodes on
//!   the query side.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::sync::{Arc, RwLock};

use base64::Engine as _;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use tidb_datatype::{
    ConversionFlags, Datum, EvalType, FieldType, FieldTypeCode, SessionTimeZone,
    DEFAULT_STATEMENT_FLAGS,
};
use tidb_planner::cardinality::row_count_estimator::{ColumnStats, IndexStats};
use tidb_stats::cmsketch::{
    cmsketch_and_topn_from_proto, CmsSketchProto, CmsSketchProtoRow, CmsSketchProtoTopN,
};
use tidb_stats::histogram::Histogram;
use tidb_stats::{
    fm_sketch_from_proto, ColAndIdxExistenceMap, Column, ColumnInfo, FmSketchProto, HistColl,
    Index, IndexInfo, JsonBucket, JsonCmSketch, JsonColumn, JsonFmSketch, JsonHistogram,
    StatsLoadedStatus, Table,
};
pub use tidb_stats::{JsonTable, TIDB_GLOBAL_STATS};

use crate::access_cost::TableStatistics;
use crate::kv_table::KvTable;

/// The `TableInfo` subset Go `TableStatsFromJSON` reads.
///
/// Keeping this schema-only contract independent of [`KvTable`] lets the
/// cluster storage path convert against its canonical `model.TableInfo`
/// without constructing an executor table or duplicating JSON semantics.
#[derive(Clone, Debug)]
pub struct LoadStatsTableSchema {
    /// Columns in schema order, including hidden columns.
    pub columns: Vec<LoadStatsColumnSchema>,
    /// Indexes in schema order.
    pub indexes: Vec<LoadStatsIndexSchema>,
    /// Go `TableInfo.PKIsHandle`.
    pub pk_is_handle: bool,
}

/// One schema column used while restoring statistics JSON.
#[derive(Clone, Debug)]
pub struct LoadStatsColumnSchema {
    /// Go `ColumnInfo.ID`.
    pub id: i64,
    /// Go `ColumnInfo.Name.L`.
    pub name: String,
    /// Go `ColumnInfo.FieldType`.
    pub field_type: FieldType,
    /// Whether this column carries the primary-key flag.
    pub primary_key: bool,
}

/// One schema index used while restoring statistics JSON.
#[derive(Clone, Debug)]
pub struct LoadStatsIndexSchema {
    /// Go `IndexInfo.ID`.
    pub id: i64,
    /// Go `IndexInfo.Name.L`.
    pub name: String,
    /// Lowercase index-column names.
    pub columns: Vec<String>,
    /// Go `IndexInfo.MVIndex`.
    pub mv_index: bool,
}

impl LoadStatsTableSchema {
    /// Builds the schema contract from the ordinary executor table.
    #[must_use]
    pub fn from_kv_table(table: &KvTable) -> Self {
        let primary_index_offsets = table
            .indexes()
            .iter()
            .find(|index| index.name.eq_ignore_ascii_case("PRIMARY"))
            .map(|index| index.column_offsets.as_slice())
            .unwrap_or_default();
        Self {
            columns: table
                .columns()
                .iter()
                .enumerate()
                .map(|(offset, column)| LoadStatsColumnSchema {
                    id: column.id,
                    name: column.name.to_lowercase(),
                    field_type: column.field_type.clone(),
                    primary_key: table.pk_handle_offset() == Some(offset)
                        || primary_index_offsets.contains(&offset),
                })
                .collect(),
            indexes: table
                .indexes()
                .iter()
                .map(|index| LoadStatsIndexSchema {
                    id: index.id,
                    name: index.name.to_lowercase(),
                    columns: index
                        .column_offsets
                        .iter()
                        .map(|offset| {
                            table
                                .columns()
                                .get(*offset)
                                .expect("index column offset outside table columns")
                                .name
                                .to_lowercase()
                        })
                        .collect(),
                    mv_index: table.mv_key_part_source(index.id).is_some(),
                })
                .collect(),
            pk_is_handle: table.pk_handle_offset().is_some(),
        }
    }
}

/// Why a dump could not be loaded. Go surfaces each of these as the
/// statement's error, so the text names what the caller can act on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoadStatsError {
    /// The file is not the JSON `encoding/json` would have accepted.
    Json(String),
    /// A base64 `[]byte` field did not decode.
    Base64(String),
    /// A column bound would not convert back to the column's type -- Go's
    /// `hist.ConvertTo` error path in `TableStatsFromJSON`.
    Convert(String),
    /// Gzip compression or decompression failed.
    Gzip(String),
    /// Go `BlocksToJSONTable` rejects an empty block sequence.
    EmptyBlocks,
}

impl std::fmt::Display for LoadStatsError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Json(detail) => write!(formatter, "Load Stats: parse json failed: {detail}"),
            Self::Base64(detail) => {
                write!(formatter, "Load Stats: decode histogram bound: {detail}")
            }
            Self::Convert(detail) => {
                write!(formatter, "Load Stats: convert histogram bound: {detail}")
            }
            Self::Gzip(detail) => write!(formatter, "Load Stats: gzip stats json: {detail}"),
            Self::EmptyBlocks => formatter.write_str("Block empty error"),
        }
    }
}

impl std::error::Error for LoadStatsError {}

/// Parses one dump file's text, Go `json.Unmarshal(data, jsonTbl)` in
/// `LoadStatsInfo.Update`.
///
/// A file holding JSON `null` parses to the all-zero table (Go unmarshals
/// `null` into the untouched struct), which the caller's
/// `table_name == "" && version == 0` guard then treats as a no-op rather
/// than an error -- that guard is Go's own, comment and all.
pub fn parse_stats_json(data: &str) -> Result<JsonTable, LoadStatsError> {
    if data.trim() == "null" {
        return Ok(JsonTable::default());
    }
    serde_json::from_str(data).map_err(|error| LoadStatsError::Json(error.to_string()))
}

/// Go `storage.JSONTableToBlocks`.
///
/// # Panics
///
/// Panics for a zero block size, matching Go's integer division by zero.
pub fn json_table_to_blocks(
    table: &JsonTable,
    block_size: usize,
) -> Result<Vec<Vec<u8>>, LoadStatsError> {
    assert_ne!(block_size, 0, "integer divide by zero");
    let json =
        serde_json::to_vec(table).map_err(|error| LoadStatsError::Json(error.to_string()))?;
    let mut writer = GzEncoder::new(Vec::new(), Compression::default());
    writer
        .write_all(&json)
        .map_err(|error| LoadStatsError::Gzip(error.to_string()))?;
    let compressed = writer
        .finish()
        .map_err(|error| LoadStatsError::Gzip(error.to_string()))?;
    Ok(compressed.chunks(block_size).map(<[u8]>::to_vec).collect())
}

/// Go `storage.BlocksToJSONTable`.
pub fn blocks_to_json_table(blocks: &[Vec<u8>]) -> Result<JsonTable, LoadStatsError> {
    if blocks.is_empty() {
        return Err(LoadStatsError::EmptyBlocks);
    }
    let compressed: Vec<u8> = blocks.iter().flatten().copied().collect();
    let mut reader = GzDecoder::new(compressed.as_slice());
    let mut json = Vec::new();
    reader
        .read_to_end(&mut json)
        .map_err(|error| LoadStatsError::Gzip(error.to_string()))?;
    serde_json::from_slice(&json).map_err(|error| LoadStatsError::Json(error.to_string()))
}

fn decode_base64(field: Option<&String>) -> Result<Vec<u8>, LoadStatsError> {
    match field {
        None => Ok(Vec::new()),
        Some(text) => base64::engine::general_purpose::STANDARD
            .decode(text)
            .map_err(|error| LoadStatsError::Base64(error.to_string())),
    }
}

/// Go `statistics.HistogramFromProto`: bounds come back as BYTES datums, and
/// deciding what those bytes mean (decode them, or keep them as key bytes) is
/// the CALLER's job -- exactly why Go documents "the decode will be after
/// this function".
fn histogram_from_json(proto: &JsonHistogram) -> Result<Histogram, LoadStatsError> {
    let buckets = proto.buckets.as_deref().unwrap_or_default();
    let mut histogram = Histogram::new(0, proto.ndv, 0, 0, buckets.len(), 0);
    for bucket in buckets {
        let bucket = bucket.as_ref().map_or(&EMPTY_BUCKET, |bucket| bucket);
        let lower = Datum::Bytes(decode_base64(bucket.lower_bound.as_ref())?);
        let upper = Datum::Bytes(decode_base64(bucket.upper_bound.as_ref())?);
        // A missing per-bucket `ndv` appends with ndv 0 -- the same stored
        // value `AppendBucket` leaves, so the estimator's bucket-NDV path
        // (v2's `equalRowCount` inside a bucket) sees the identical zero.
        histogram.append_bucket_with_ndv(
            lower,
            upper,
            bucket.count,
            bucket.repeats,
            bucket.ndv.unwrap_or(0),
        );
    }
    Ok(histogram)
}

static EMPTY_BUCKET: JsonBucket = JsonBucket {
    count: 0,
    lower_bound: None,
    upper_bound: None,
    repeats: 0,
    ndv: None,
};

/// The dumped `tipb.CMSketch` handed to the shared
/// [`cmsketch_and_topn_from_proto`] conversion -- the same code path Go's
/// `CMSketchAndTopNFromProto` is, so a sketch loads exactly as one decoded
/// from `mysql.stats_histograms` would, TopN sort included. That final
/// `topN.Sort()` matters: the estimator binary-searches the TopN
/// (`TopN.LowerBound`), and the dump wrote entries in pruning order.
fn cmsketch_from_json(
    proto: Option<&JsonCmSketch>,
) -> Result<
    (
        Option<tidb_stats::cmsketch::CmsSketch>,
        Option<tidb_stats::cmsketch::TopN>,
    ),
    LoadStatsError,
> {
    let Some(proto) = proto else {
        return Ok((None, None));
    };
    let rows = match &proto.rows {
        None => None,
        Some(rows) => Some(
            rows.iter()
                .map(|row| {
                    row.as_ref().map(|row| CmsSketchProtoRow {
                        counters: row.counters.clone(),
                    })
                })
                .collect(),
        ),
    };
    let top_n = match &proto.top_n {
        None => None,
        Some(entries) => {
            let mut converted = Vec::with_capacity(entries.len());
            for entry in entries {
                converted.push(match entry {
                    None => None,
                    Some(entry) => Some(CmsSketchProtoTopN {
                        data: Some(decode_base64(entry.data.as_ref())?),
                        count: entry.count,
                    }),
                });
            }
            Some(converted)
        }
    };
    let native = CmsSketchProto {
        rows,
        top_n,
        default_value: proto.default_value,
    };
    Ok(cmsketch_and_topn_from_proto(Some(&native)))
}

/// Go `statistics.FMSketchFromProto` for the JSON proto-shaped value.
fn fmsketch_from_json(proto: Option<&JsonFmSketch>) -> Option<tidb_stats::FmSketch> {
    proto.and_then(|proto| {
        let native = FmSketchProto {
            mask: proto.mask,
            hashset: proto.hashset.clone().unwrap_or_default(),
        };
        fm_sketch_from_proto(Some(&native))
    })
}

/// Go `TableStatsFromJSON`'s stats-version resolution, shared by the column
/// and index arms verbatim:
///
/// > If the statistics are collected without setting stats version (which
/// > happens in v4.0 and earlier versions), we set it to 1.
///
/// The inference matters more than it looks: every fixture dump under
/// `tests/integrationtest/s/` from before 2021 has no `stats_ver` at all, and
/// treating those as version 0 would make the estimator ignore their
/// histograms entirely (a version-0 object is "not analyzed" to Go's
/// `IsAnalyzed`).
fn resolve_stats_version(json: &JsonColumn) -> i64 {
    if let Some(version) = json.stats_ver {
        return version;
    }
    let ndv = json.histogram.as_ref().map_or(0, |histogram| histogram.ndv);
    if ndv > 0 || json.null_count > 0 {
        1
    } else {
        0
    }
}

/// Whether Go's column arm keeps the dumped bounds as BLOB bytes instead of
/// converting them back to the column type.
///
/// Go's exact test (`json.go`): `EvalType() == ETString && GetType() !=
/// TypeEnum && GetType() != TypeSet`, with the comment that new-collation
/// bounds hold the COLLATE KEY, which may be longer than the column's flen
/// and is not valid text in the column's charset -- converting it would
/// raise "Invalid utf8mb4 character string" on statistics that are correct.
fn keep_bounds_as_bytes(field_type: &FieldType) -> bool {
    field_type.eval_type() == EvalType::String
        && !matches!(field_type.code(), FieldTypeCode::Enum | FieldTypeCode::Set)
}

/// Go `statistics.UTCWithAllowInvalidDateCtx`, the conversion context every
/// statistics bound decode runs under: default statement flags plus
/// invalid-date and zero-in-date tolerance, in UTC. A dump may legitimately
/// hold `2007-02-30` as a bucket bound if a row held it; refusing to load the
/// whole table's statistics over it would be worse than the bound.
const STATS_CONVERSION_FLAGS: ConversionFlags = ConversionFlags::from_bits(
    DEFAULT_STATEMENT_FLAGS.bits()
        | ConversionFlags::IGNORE_INVALID_DATE_ERR
        | ConversionFlags::IGNORE_ZERO_IN_DATE_ERR,
);

/// One dumped column into the estimator's [`ColumnStats`]: Go
/// `TableStatsFromJSON`'s column arm.
fn column_from_json(
    column: &LoadStatsColumnSchema,
    json: &JsonColumn,
    physical_id: i64,
    is_handle: bool,
    primary_key: bool,
) -> Result<Column, LoadStatsError> {
    let proto = json
        .histogram
        .as_ref()
        .expect("column stats JSON has no histogram");
    let mut histogram = histogram_from_json(proto)?;
    if !keep_bounds_as_bytes(&column.field_type) {
        // Go: `hist.ConvertTo(UTCWithAllowInvalidDateCtx, &tmpFT)` -- each
        // bound's string form parsed back into the column's own domain, so
        // that `WHERE c > 5` compares an INT datum against an INT bound
        // rather than against the ASCII bytes `"5"` (which would order
        // lexically and put 10 before 9).
        for bucket in &mut histogram.buckets {
            for bound in [&mut bucket.lower_bound, &mut bucket.upper_bound] {
                let converted = bound
                    .convert_to_in(
                        &column.field_type,
                        STATS_CONVERSION_FLAGS,
                        &SessionTimeZone::utc(),
                    )
                    .map_err(|error| {
                        LoadStatsError::Convert(format!("column `{}`: {error:?}", column.name))
                    })?;
                *bound = converted.value;
            }
        }
    }
    histogram.id = column.id;
    histogram.null_count = json.null_count;
    histogram.last_update_version = json.last_update_version;
    histogram.tot_col_size = json.tot_col_size;
    histogram.correlation = json.correlation;
    let (cms, topn) = cmsketch_from_json(json.cm_sketch.as_ref())?;
    Ok(Column {
        cmsketch: cms,
        top_n: topn,
        fm_sketch: fmsketch_from_json(json.fm_sketch.as_ref()),
        info: Some(ColumnInfo {
            id: column.id,
            name: column.name.to_lowercase(),
            primary_key,
        }),
        histogram,
        stats_loaded_status: StatsLoadedStatus::full_load(),
        physical_id,
        stats_version: resolve_stats_version(json),
        is_handle,
    })
}

/// One dumped index into [`IndexStats`]: Go `TableStatsFromJSON`'s index arm,
/// which sets id/null-count/version/correlation on the histogram and nothing
/// else -- crucially NO `ConvertTo`, because index bounds are key bytes.
fn index_from_json(
    index: &LoadStatsIndexSchema,
    json: &JsonColumn,
    physical_id: i64,
) -> Result<Index, LoadStatsError> {
    let proto = json
        .histogram
        .as_ref()
        .expect("index stats JSON has no histogram");
    let mut histogram = histogram_from_json(proto)?;
    histogram.id = index.id;
    histogram.null_count = json.null_count;
    histogram.last_update_version = json.last_update_version;
    histogram.correlation = json.correlation;
    let (cms, topn) = cmsketch_from_json(json.cm_sketch.as_ref())?;
    Ok(Index {
        cmsketch: cms,
        top_n: topn,
        fm_sketch: None,
        info: Some(IndexInfo {
            id: index.id,
            name: index.name.to_lowercase(),
            columns: index.columns.clone(),
            mv_index: index.mv_index,
        }),
        histogram,
        stats_loaded_status: StatsLoadedStatus::full_load(),
        stats_version: resolve_stats_version(json),
        physical_id,
    })
}

/// Go `storage.TableStatsFromJSON`: one `JSONTable` (the whole file, or one
/// `partitions` entry) against one table's schema, producing the canonical
/// full statistics table for one physical table id.
///
/// Name resolution mirrors Go's double loop exactly: a dumped entry that
/// matches no current column or index is DROPPED silently (the schema moved
/// on since the dump), and a column with no dumped entry simply has no
/// statistics -- the estimator falls back to its per-column pseudo rates for
/// it, which is also what Go's `StatsAvailable` gate reaches.
pub fn statistics_table_from_json(
    table: &KvTable,
    physical_id: i64,
    json: &JsonTable,
) -> Result<Table, LoadStatsError> {
    statistics_table_from_json_schema(
        &LoadStatsTableSchema::from_kv_table(table),
        physical_id,
        json,
    )
}

/// Go `storage.TableStatsFromJSON` against the canonical schema-only subset.
pub fn statistics_table_from_json_schema(
    table: &LoadStatsTableSchema,
    physical_id: i64,
    json: &JsonTable,
) -> Result<Table, LoadStatsError> {
    let mut hist_coll = HistColl::new(
        physical_id,
        json.count,
        json.modify_count,
        json.columns.as_ref().map_or(0, BTreeMap::len),
        json.indices.as_ref().map_or(0, BTreeMap::len),
    );
    let mut existence = ColAndIdxExistenceMap::new(table.columns.len(), table.indexes.len());
    if let Some(dumped) = &json.indices {
        for (name, entry) in dumped {
            let entry = entry.as_ref().expect("index stats JSON entry is null");
            for index in &table.indexes {
                // Go: `idxInfo.Name.L != id` -- the LOWERCASED schema name
                // against the raw map key. Dumps write lowercase keys
                // (`GenJSONTableFromStats` uses `.Name.L`), so an uppercase
                // key matches nothing there and must match nothing here.
                if index.name.to_lowercase() != *name {
                    continue;
                }
                let item = index_from_json(index, entry, physical_id)?;
                if item.stats_version != 0 {
                    hist_coll.stats_version = item.stats_version as i32;
                }
                hist_coll.set_index(index.id, item);
                existence.insert_index(index.id, true);
            }
        }
    }
    if let Some(dumped) = &json.columns {
        for (name, entry) in dumped {
            let entry = entry.as_ref().expect("column stats JSON entry is null");
            for column in &table.columns {
                // Same rule as the index loop: `colInfo.Name.L != id`.
                if column.name.to_lowercase() != *name {
                    continue;
                }
                let is_handle = table.pk_is_handle && column.primary_key;
                let item =
                    column_from_json(column, entry, physical_id, is_handle, column.primary_key)?;
                if item.stats_version != 0 {
                    hist_coll.stats_version = item.stats_version as i32;
                }
                hist_coll.set_column(column.id, item);
                existence.insert_column(column.id, true);
            }
        }
    }
    Ok(Table {
        existence_map: Some(Arc::new(RwLock::new(existence))),
        hist_coll,
        version: 0,
        last_analyze_version: 0,
        last_stats_hist_version: 0,
        table_info_update_ts: 0,
        is_pk_handle: table.pk_is_handle,
    })
}

/// The common planner view of a canonical Go-compatible statistics table.
pub fn table_statistics_from_table(stats: &Table, table: &KvTable) -> TableStatistics {
    let columns = table
        .columns()
        .iter()
        .map(|column| (column.id, column.field_type.is_unsigned()))
        .collect::<Vec<_>>();
    let indexes = table
        .indexes()
        .iter()
        .map(|index| (index.id, index.column_offsets.len(), index.unique))
        .collect::<Vec<_>>();
    table_statistics_from_table_schema(stats, &columns, &indexes)
}

/// Restores one non-partitioned executor table and derives its planner view.
pub fn table_statistics_from_json(
    table: &KvTable,
    json: &JsonTable,
) -> Result<TableStatistics, LoadStatsError> {
    let statistics = statistics_table_from_json(table, table.table_id, json)?;
    Ok(table_statistics_from_table(&statistics, table))
}

/// The common planner view when the caller owns Go `TableInfo` rather than
/// an executor `KvTable`. Tuple fields are column `(id, unsigned)` and index
/// `(id, column_count, unique)` metadata.
pub fn table_statistics_from_table_schema(
    stats: &Table,
    schema_columns: &[(i64, bool)],
    schema_indexes: &[(i64, usize, bool)],
) -> TableStatistics {
    let existence = stats.existence_map.as_ref().map(|map| {
        map.read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    });
    let mut column_stats_existence = BTreeMap::new();
    let mut index_stats_existence = BTreeMap::new();
    let mut columns = BTreeMap::new();
    let mut indexes = BTreeMap::new();
    let mut column_load_status = BTreeMap::new();
    let mut index_load_status = BTreeMap::new();
    for column in stats.hist_coll.stable_columns() {
        let column = column
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let id = column.item_id();
        column_stats_existence.insert(
            id,
            existence
                .as_ref()
                .is_some_and(|map| map.has_analyzed(id, false)),
        );
        if !column.stats_available() {
            continue;
        }
        let unsigned = schema_columns
            .iter()
            .find(|schema| schema.0 == id)
            .is_some_and(|schema| schema.1);
        columns.insert(
            id,
            ColumnStats {
                histogram: column.histogram.clone(),
                topn: column.top_n.clone(),
                cms: column.cmsketch.clone(),
                stats_ver: column.stats_version,
                unsigned,
            },
        );
        column_load_status.insert(id, column.stats_loaded_status);
    }
    for index in stats.hist_coll.stable_indices() {
        let index = index
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let id = index.item_id();
        index_stats_existence.insert(
            id,
            existence
                .as_ref()
                .is_some_and(|map| map.has_analyzed(id, true)),
        );
        if !index.is_analyzed() && index.histogram.ndv == 0 && index.histogram.null_count == 0 {
            continue;
        }
        let Some(schema) = schema_indexes.iter().find(|schema| schema.0 == id) else {
            continue;
        };
        indexes.insert(
            id,
            IndexStats {
                histogram: index.histogram.clone(),
                topn: index.top_n.clone(),
                cms: index.cmsketch.clone(),
                stats_ver: index.stats_version,
                num_columns: schema.1,
                unique: schema.2,
            },
        );
        index_load_status.insert(id, index.stats_loaded_status);
    }
    let mut statistics = TableStatistics::new(
        stats.hist_coll.realtime_count,
        stats.hist_coll.modify_count,
        columns,
        indexes,
    )
    .with_stat_versions(stats.version, stats.last_analyze_version)
    .with_load_statuses(column_load_status, index_load_status)
    .with_stats_existence(column_stats_existence, index_stats_existence);
    // Go `GetStatsTable` marks the planner copy pseudo when the canonical
    // table has no initialized column or index statistics. Do not infer this
    // from the reduced maps above: unloaded placeholder items can still have
    // histogram metadata and therefore survive that conversion.
    statistics.pseudo = stats.hist_coll.pseudo || !stats.is_initialized();
    statistics.cache_pseudo = stats.hist_coll.pseudo;
    statistics
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(id: i64, name: &str) -> crate::kv_table::KvColumn {
        crate::kv_table::KvColumn {
            id,
            name: name.to_owned(),
            field_type: FieldType::new(FieldTypeCode::LongLong),
            column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
            default_value: None,
            origin_default: None,
            comment: String::new(),
            generated: None,
        }
    }

    /// The v4.0-era inference: no `stats_ver` key plus a real NDV must read
    /// as version 1, or every pre-2021 fixture's histogram goes unused.
    #[test]
    fn missing_stats_ver_infers_version_one_from_ndv() {
        let json: JsonColumn =
            serde_json::from_str(r#"{"histogram": {"ndv": 5, "buckets": []}, "null_count": 0}"#)
                .expect("parses");
        assert_eq!(resolve_stats_version(&json), 1);
        let empty: JsonColumn =
            serde_json::from_str(r#"{"histogram": {"ndv": 0, "buckets": []}}"#).expect("parses");
        assert_eq!(resolve_stats_version(&empty), 0);
        let versioned: JsonColumn =
            serde_json::from_str(r#"{"histogram": {"ndv": 0}, "stats_ver": 2}"#).expect("parses");
        assert_eq!(resolve_stats_version(&versioned), 2);
    }

    /// A file holding `null` is Go's "stats file with `null`" case: it
    /// parses to the zero table, which the caller then no-ops on.
    #[test]
    fn null_file_parses_to_the_zero_table() {
        let table = parse_stats_json("null").expect("null parses");
        assert_eq!(table.table_name, "");
        assert_eq!(table.version, 0);
    }

    /// Go `storage_test.TestJSONTableToBlocks`.
    #[test]
    fn json_table_blocks_round_trip() {
        let source: JsonTable = serde_json::from_str(
            r#"{
                "database_name":"test",
                "table_name":"t",
                "columns":{"a":{"histogram":{"ndv":2,"buckets":[]},"stats_ver":2}},
                "indices":{},
                "count":6,
                "modify_count":1,
                "version":42,
                "predicate_columns":[{"id":1,"last_used_at":"2026-08-29 00:00:00.000000"}]
            }"#,
        )
        .expect("source JSON table");
        let blocks = json_table_to_blocks(&source, 30).expect("compress table");
        assert!(blocks.len() > 1);
        let converted = blocks_to_json_table(&blocks).expect("decompress table");
        assert_eq!(
            serde_json::to_value(converted).expect("converted JSON"),
            serde_json::to_value(source).expect("source JSON")
        );
    }

    /// Go `TableStatsFromJSON` walks `TableInfo.Columns`, including hidden
    /// expression-index columns, and retains the FM sketch on the canonical
    /// column object.
    #[test]
    fn json_builds_full_table_including_hidden_columns_and_fm_sketch() {
        let mut schema = KvTable::new(41, vec![column(1, "a")]);
        schema.set_pk_handle_offset(0);
        schema.add_hidden_column(column(2, "_V$_expr"));
        let dumped = JsonTable {
            count: 9,
            modify_count: 2,
            columns: Some(BTreeMap::from([
                (
                    "a".to_owned(),
                    Some(JsonColumn {
                        histogram: Some(JsonHistogram {
                            ndv: 1,
                            ..JsonHistogram::default()
                        }),
                        stats_ver: Some(2),
                        ..JsonColumn::default()
                    }),
                ),
                (
                    "_v$_expr".to_owned(),
                    Some(JsonColumn {
                        histogram: Some(JsonHistogram {
                            ndv: 3,
                            ..JsonHistogram::default()
                        }),
                        fm_sketch: Some(JsonFmSketch {
                            mask: 7,
                            hashset: Some(vec![11, 13]),
                        }),
                        stats_ver: Some(2),
                        ..JsonColumn::default()
                    }),
                ),
            ])),
            ..JsonTable::default()
        };

        let stats = statistics_table_from_json(&schema, 99, &dumped).expect("load dump");
        assert_eq!(stats.hist_coll.physical_id, 99);
        assert_eq!(stats.hist_coll.column_count(), 2);
        assert_eq!(stats.hist_coll.stats_version, 2);
        assert!(stats.is_pk_handle);
        let handle = stats.hist_coll.get_column(1).expect("handle stats");
        assert!(handle.read().unwrap().is_handle);
        let hidden = stats.hist_coll.get_column(2).expect("hidden stats");
        let hidden = hidden.read().unwrap();
        assert_eq!(hidden.physical_id, 99);
        assert!(hidden.stats_loaded_status.is_full_load());
        assert_eq!(
            tidb_stats::fm_sketch_to_proto(hidden.fm_sketch.as_ref()),
            FmSketchProto {
                mask: 7,
                hashset: vec![11, 13],
            }
        );
        assert!(stats
            .existence_map
            .as_ref()
            .unwrap()
            .read()
            .unwrap()
            .has_analyzed(2, false));
        drop(hidden);
        let planner = table_statistics_from_table(&stats, &schema);
        assert!(planner.columns.contains_key(&2));
    }

    /// Go `GetStatsTable` consults `Table.IsInitialized`, rather than the
    /// presence of reduced histogram metadata, when marking its planner copy
    /// pseudo.
    #[test]
    fn uninitialized_placeholder_keeps_realtime_count_and_is_pseudo() {
        let schema = KvTable::new(41, vec![column(1, "a")]);
        let stats = statistics_table_from_json(
            &schema,
            41,
            &JsonTable {
                count: 5,
                ..JsonTable::default()
            },
        )
        .expect("build meta-only stats");
        stats.hist_coll.set_column(
            1,
            Column {
                info: Some(ColumnInfo {
                    id: 1,
                    name: "a".to_owned(),
                    primary_key: false,
                }),
                histogram: Histogram {
                    id: 1,
                    ndv: 1,
                    ..Histogram::default()
                },
                physical_id: 41,
                ..Column::default()
            },
        );

        assert!(!stats.is_initialized());
        let planner = table_statistics_from_table(&stats, &schema);
        assert_eq!(planner.row_count, 5);
        assert!(planner.columns.contains_key(&1));
        assert!(planner.pseudo);
        assert!(!planner.cache_pseudo);
    }
}
