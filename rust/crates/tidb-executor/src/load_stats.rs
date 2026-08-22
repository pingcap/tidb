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
//!   [`table_statistics_from_json`] is that function, producing the
//!   [`TableStatistics`] this tier's planner reads instead of a
//!   `statistics.Table`.
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

use base64::Engine as _;
use serde::Deserialize;
use tidb_datatype::{
    ConversionFlags, Datum, EvalType, FieldType, FieldTypeCode, SessionTimeZone,
    DEFAULT_STATEMENT_FLAGS,
};
use tidb_planner::cardinality::row_count_estimator::{ColumnStats, IndexStats};
use tidb_stats::cmsketch::{
    cmsketch_and_topn_from_proto, CmsSketchProto, CmsSketchProtoRow, CmsSketchProtoTopN,
};
use tidb_stats::histogram::Histogram;

use crate::access_cost::TableStatistics;
use crate::kv_table::KvTable;

/// Go `statsutil.TiDBGlobalStats`: the `partitions` key that carries the
/// LOGICAL table's merged statistics beside the per-partition entries.
pub const TIDB_GLOBAL_STATS: &str = "global";

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
        }
    }
}

impl std::error::Error for LoadStatsError {}

/// Go `statsutil.JSONTable`, in the fields this tier consumes.
///
/// Every field defaults: an old dump (v4.0-era files in
/// `tests/integrationtest/s/` really do lack `version`, `stats_ver` and
/// `correlation`) must load, because Go's zero values are exactly what its
/// unmarshaller leaves behind. `predicate_columns` and `is_historical_stats`
/// are accepted and dropped -- they feed `mysql.column_stats_usage` and the
/// history tables, neither of which exists in this tier.
#[derive(Debug, Default, Deserialize)]
pub struct JsonTable {
    /// `columns`, keyed by LOWERCASE column name (`col.Info.Name.L`).
    #[serde(default)]
    pub columns: Option<BTreeMap<String, Option<JsonColumn>>>,
    /// `indices`, keyed by LOWERCASE index name.
    #[serde(default)]
    pub indices: Option<BTreeMap<String, Option<JsonColumn>>>,
    /// `partitions`: per-partition dumps by partition name, plus
    /// [`TIDB_GLOBAL_STATS`]. `None` mirrors Go's nil map, which routes the
    /// load at the table itself even when the table is partitioned --
    /// `LoadStatsFromJSONNoUpdate` tests `jsonTbl.Partitions == nil`, so a
    /// PRESENT-BUT-EMPTY object takes the partition branch and loads nothing.
    #[serde(default)]
    pub partitions: Option<BTreeMap<String, Option<JsonTable>>>,
    /// `database_name`: the SCHEMA the dump names, which is where the load
    /// installs -- not the session's current database.
    #[serde(default)]
    pub database_name: String,
    /// `table_name`, lowercase like every dumped name.
    #[serde(default)]
    pub table_name: String,
    /// `count`: Go `HistColl.RealtimeCount`, what `stats_meta.count` becomes.
    #[serde(default)]
    pub count: i64,
    /// `modify_count`: rows changed since the dump's analyze.
    #[serde(default)]
    pub modify_count: i64,
    /// `version`: the dump's stats-meta version. Only read by the
    /// `LoadStatsInfo.Update` null-file guard (`table_name == "" && version
    /// == 0` means the file held JSON `null`).
    #[serde(default)]
    pub version: u64,
}

/// Go `statsutil.JSONColumn` -- one column's or one index's dump; Go reuses
/// the struct for both, so this does too.
#[derive(Debug, Default, Deserialize)]
pub struct JsonColumn {
    /// The `tipb.Histogram`, JSON-marshalled.
    #[serde(default)]
    pub histogram: Option<JsonHistogram>,
    /// The `tipb.CMSketch`: depth×width counters for stats version 1, and
    /// the TopN rows ride inside it for version 2.
    #[serde(default)]
    pub cm_sketch: Option<JsonCmSketch>,
    /// `stats_ver`. A POINTER in Go because pre-v4.0 dumps predate the field:
    /// absent means "infer it", not "version 0" -- see
    /// [`resolve_stats_version`].
    #[serde(default)]
    pub stats_ver: Option<i64>,
    /// `null_count`.
    #[serde(default)]
    pub null_count: i64,
    /// `tot_col_size`, which feeds the average-row-size costs.
    #[serde(default)]
    pub tot_col_size: i64,
    /// `last_update_version`.
    #[serde(default)]
    pub last_update_version: u64,
    /// `correlation`, the ordering correlation columns carry.
    #[serde(default)]
    pub correlation: f64,
}

/// `tipb.Histogram` as `encoding/json` writes it.
#[derive(Debug, Default, Deserialize)]
pub struct JsonHistogram {
    /// `ndv`.
    #[serde(default)]
    pub ndv: i64,
    /// `buckets`; entries are pointers in Go, so `null` elements survive.
    #[serde(default)]
    pub buckets: Option<Vec<Option<JsonBucket>>>,
}

/// `tipb.Bucket`. `count` is CUMULATIVE through this bucket, exactly the
/// in-memory convention (`HistogramToProto` copies `Buckets[i].Count`
/// straight across), so no re-accumulation happens on load.
#[derive(Debug, Default, Deserialize)]
pub struct JsonBucket {
    /// Cumulative row count.
    #[serde(default)]
    pub count: i64,
    /// Base64 of the lower-bound bytes.
    #[serde(default)]
    pub lower_bound: Option<String>,
    /// Base64 of the upper-bound bytes.
    #[serde(default)]
    pub upper_bound: Option<String>,
    /// Repeat count of the upper bound.
    #[serde(default)]
    pub repeats: i64,
    /// Optional per-bucket NDV; a pointer in tipb, `AppendBucketWithNDV` when
    /// present, plain `AppendBucket` when not (`HistogramFromProto`).
    #[serde(default)]
    pub ndv: Option<i64>,
}

/// `tipb.CMSketch`.
#[derive(Debug, Default, Deserialize)]
pub struct JsonCmSketch {
    /// `rows`: the depth×width counter grid, one entry per hash row.
    #[serde(default)]
    pub rows: Option<Vec<Option<JsonCmSketchRow>>>,
    /// `top_n`.
    #[serde(default)]
    pub top_n: Option<Vec<Option<JsonCmSketchTopN>>>,
    /// `default_value`.
    #[serde(default)]
    pub default_value: u64,
}

/// `tipb.CMSketchRow`.
#[derive(Debug, Default, Deserialize)]
pub struct JsonCmSketchRow {
    /// `counters`.
    #[serde(default)]
    pub counters: Option<Vec<u32>>,
}

/// `tipb.CMSketchTopN`.
#[derive(Debug, Default, Deserialize)]
pub struct JsonCmSketchTopN {
    /// Base64 of the encoded value bytes.
    #[serde(default)]
    pub data: Option<String>,
    /// The value's row count.
    #[serde(default)]
    pub count: u64,
}

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
fn column_stats_from_json(
    column: &crate::kv_table::KvColumn,
    json: &JsonColumn,
) -> Result<ColumnStats, LoadStatsError> {
    let Some(proto) = &json.histogram else {
        // Go dereferences `jsonCol.Histogram` unconditionally, so a dump
        // without one has never existed in the wild; refuse it by name
        // instead of panicking the way Go would.
        return Err(LoadStatsError::Json(format!(
            "column `{}` has no histogram in the stats file",
            column.name
        )));
    };
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
                        LoadStatsError::Convert(format!(
                            "column `{}`: {error:?}",
                            column.name
                        ))
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
    Ok(ColumnStats {
        histogram,
        topn,
        cms,
        stats_ver: resolve_stats_version(json),
        unsigned: column.field_type.is_unsigned(),
    })
}

/// One dumped index into [`IndexStats`]: Go `TableStatsFromJSON`'s index arm,
/// which sets id/null-count/version/correlation on the histogram and nothing
/// else -- crucially NO `ConvertTo`, because index bounds are key bytes.
fn index_stats_from_json(
    index: &crate::kv_table::KvIndex,
    json: &JsonColumn,
) -> Result<IndexStats, LoadStatsError> {
    let Some(proto) = &json.histogram else {
        return Err(LoadStatsError::Json(format!(
            "index `{}` has no histogram in the stats file",
            index.name
        )));
    };
    let mut histogram = histogram_from_json(proto)?;
    histogram.id = index.id;
    histogram.null_count = json.null_count;
    histogram.last_update_version = json.last_update_version;
    histogram.correlation = json.correlation;
    let (cms, topn) = cmsketch_from_json(json.cm_sketch.as_ref())?;
    Ok(IndexStats {
        histogram,
        topn,
        cms,
        stats_ver: resolve_stats_version(json),
        num_columns: index.column_offsets.len(),
        unique: index.unique,
    })
}

/// Go `storage.TableStatsFromJSON`: one `JSONTable` (the whole file, or one
/// `partitions` entry) against one table's schema, producing the
/// [`TableStatistics`] to publish for one physical table id.
///
/// Name resolution mirrors Go's double loop exactly: a dumped entry that
/// matches no current column or index is DROPPED silently (the schema moved
/// on since the dump), and a column with no dumped entry simply has no
/// statistics -- the estimator falls back to its per-column pseudo rates for
/// it, which is also what Go's `StatsAvailable` gate reaches.
pub fn table_statistics_from_json(
    table: &KvTable,
    json: &JsonTable,
) -> Result<TableStatistics, LoadStatsError> {
    let mut indexes = BTreeMap::new();
    if let Some(dumped) = &json.indices {
        for (name, entry) in dumped {
            let Some(entry) = entry else { continue };
            for index in table.indexes() {
                // Go: `idxInfo.Name.L != id` -- the LOWERCASED schema name
                // against the raw map key. Dumps write lowercase keys
                // (`GenJSONTableFromStats` uses `.Name.L`), so an uppercase
                // key matches nothing there and must match nothing here.
                if index.name.to_lowercase() != *name {
                    continue;
                }
                indexes.insert(index.id, index_stats_from_json(index, entry)?);
            }
        }
    }
    let mut columns = BTreeMap::new();
    if let Some(dumped) = &json.columns {
        for (name, entry) in dumped {
            let Some(entry) = entry else { continue };
            for column in table.visible_columns() {
                // Same rule as the index loop: `colInfo.Name.L != id`.
                if column.name.to_lowercase() != *name {
                    continue;
                }
                columns.insert(column.id, column_stats_from_json(column, entry)?);
            }
        }
    }
    // `TableStatistics::new` applies Go `GetStatsTable`'s pseudo rule to the
    // dump's own `count`/`modify_count`, the same numbers Go installs through
    // `SaveMetaToStorage` at the end of `loadStatsFromJSON` -- so a dump of
    // an empty table (count 0) still reads as pseudo here, exactly as a Go
    // node reloading that `stats_meta` row would decide.
    Ok(TableStatistics::new(
        json.count,
        json.modify_count,
        columns,
        indexes,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The v4.0-era inference: no `stats_ver` key plus a real NDV must read
    /// as version 1, or every pre-2021 fixture's histogram goes unused.
    #[test]
    fn missing_stats_ver_infers_version_one_from_ndv() {
        let json: JsonColumn = serde_json::from_str(
            r#"{"histogram": {"ndv": 5, "buckets": []}, "null_count": 0}"#,
        )
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
}
