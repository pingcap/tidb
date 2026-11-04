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

//! Storing one table's statistics in `mysql.stats_meta`,
//! `mysql.stats_histograms`, `mysql.stats_buckets` and `mysql.stats_top_n`.
//!
//! Go source of truth is
//! `pkg/statistics/handle/storage/save.go::SaveAnalyzeResultToStorage`, whose
//! four statements this follows column for column, minus the SQL: the
//! mutations go straight to the record and index keys, the way every
//! `mysql.*` write on this node does (see [`tidb_syssession::system_row_write`]).
//!
//! # The two encodings that make or break the round trip
//!
//! **Bucket counts are stored as deltas.** `Bucket.Count` in memory is
//! cumulative through the bucket; `save.go` subtracts the previous bucket's
//! count before writing, and `read.go` adds them back up. A writer that
//! stored the cumulative value would make every bucket after the first look
//! enormous to a Go server, whose `read.go` re-accumulates on read.
//!
//! **Bounds are stored as blobs, not as datums.** `convertBoundToBlob` is
//! `Datum.ConvertTo(TypeBlob)`, so a `BIGINT` bound is stored as `"90"` and a
//! `DATETIME` bound as `"2024-05-05 05:05:05"`. An index bound and a
//! new-collation string bound are already bytes and convert to themselves.
//! `read.go`'s `convertBoundFromBlob` is the inverse this must satisfy.
//!
//! # Which handle shape the four tables have
//!
//! Not a fixed answer. `mysql.stats_meta` and its histogram tables became
//! `CLUSTERED` only in a recent TiDB; on a v8.5 cluster they are still a
//! `UNIQUE INDEX` over a `_tidb_rowid`, which a writer that assumed the newer
//! shape cannot address at all. This module reads the cluster's own
//! `TableInfo` and writes whichever shape it finds -- see [`StatsRows`].
//!
//! Partition ANALYZE also writes `mysql.stats_fm_sketch`; logical/global
//! writes delete stale sketches and leave no row, matching `needDumpFMS` in
//! Go. `mysql.column_stats_usage` remains a separate predicate-column input.
//! `cm_sketch` is left NULL because analyze v2 stores no CMSketch.

use std::ops::Range;

use tidb_datatype::{ConversionFlags, Datum, FieldType, FieldTypeCode, Time, UNSPECIFIED_LENGTH};
use tidb_model::table_info::TableInfo;
use tidb_sqlescape::{must_escape_sql, must_escape_sql_text, SqlArg};

use crate::cluster_stats_load::ClusterStatsItem;
use tidb_syssession::stats_table_rows::*;

/// Go `batchInsertSize` in `pkg/statistics/handle/storage/save.go`.
const LOADED_STATS_BATCH_INSERT_SIZE: usize = 10;

/// Go `maxInsertLength` in `pkg/statistics/handle/storage/save.go`.
const LOADED_STATS_MAX_INSERT_LENGTH: usize = 1024 * 1024;

/// Go `saveTopNToStorage`'s `INSERT mysql.stats_top_n` statements for one
/// histogram, in its batches (ten rows, the escaped 1 MiB boundary), as
/// text for this tier's parser (`must_escape_sql_text`).
pub fn save_topn_sql(table_id: i64, item: &ClusterStatsItem) -> Vec<String> {
    let Some(topn) = &item.topn else {
        return Vec::new();
    };
    let entries = topn.entries();
    loaded_topn_batches(table_id, item)
        .into_iter()
        .map(|range| {
            let mut sql = String::from(
                "insert into mysql.stats_top_n (table_id, is_index, hist_id, value, count) values ",
            );
            for (offset, entry) in entries[range].iter().enumerate() {
                if offset > 0 {
                    sql.push(',');
                }
                sql.push_str(&must_escape_sql_text(
                    "(%?, %?, %?, %?, %?)",
                    &[
                        SqlArg::Signed(table_id),
                        SqlArg::Signed(i64::from(item.is_index)),
                        SqlArg::Signed(item.id),
                        SqlArg::Bytes(Some(&entry.encoded)),
                        SqlArg::Unsigned(entry.count),
                    ],
                ));
            }
            sql
        })
        .collect()
}

/// Go `saveBucketsToStorage`'s `INSERT mysql.stats_buckets` statements for
/// one histogram: each bucket's own count (the stored counts are
/// cumulative) and its bounds as blobs (`convertBoundToBlob`).
pub fn save_buckets_sql(
    table_id: i64,
    item: &ClusterStatsItem,
) -> Result<Vec<String>, StatsWriteError> {
    let buckets = &item.histogram.buckets;
    let mut statements = Vec::new();
    for range in loaded_bucket_batches(table_id, item)? {
        let mut sql = String::from("insert into mysql.stats_buckets (table_id, is_index, hist_id, bucket_id, count, repeats, lower_bound, upper_bound, ndv) values ");
        for index in range.clone() {
            let bucket = &buckets[index];
            let lower = bound_blob(&bucket.lower_bound)?;
            let upper = bound_blob(&bucket.upper_bound)?;
            let blob = |value: &Datum| match value {
                Datum::Bytes(bytes) => Some(bytes.clone()),
                _ => None,
            };
            let (lower, upper) = (blob(&lower), blob(&upper));
            let previous_count = index
                .checked_sub(1)
                .map_or(0, |previous| buckets[previous].count);
            if index > range.start {
                sql.push(',');
            }
            sql.push_str(&must_escape_sql_text(
                "(%?, %?, %?, %?, %?, %?, %?, %?, %?)",
                &[
                    SqlArg::Signed(table_id),
                    SqlArg::Signed(i64::from(item.is_index)),
                    SqlArg::Signed(item.id),
                    SqlArg::Signed(index as i64),
                    SqlArg::Signed(bucket.count - previous_count),
                    SqlArg::Signed(bucket.repeat),
                    SqlArg::Bytes(lower.as_deref()),
                    SqlArg::Bytes(upper.as_deref()),
                    SqlArg::Signed(bucket.ndv),
                ],
            ));
        }
        statements.push(sql);
    }
    Ok(statements)
}

/// How a column's `mysql.column_stats_usage.last_analyzed_at` is stamped
/// after its histogram is saved.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LastAnalyzedAt {
    /// Go `SaveAnalyzeResultToStorage`: `values(last_analyzed_at)`.
    Values,
    /// Go `SaveColOrIdxStatsToStorage` with `updateAnalyzeTime`:
    /// `current_timestamp()`.
    CurrentTimestamp,
    /// Go `SaveColOrIdxStatsToStorage` without `updateAnalyzeTime`.
    Unchanged,
}

/// The statements Go's `SaveAnalyzeResultToStorage` or
/// `SaveColOrIdxStatsToStorage` runs for one histogram after the
/// `stats_meta` write, in their order: TopN replaced, the FM sketch
/// deleted (and, for an analyze result whose table is a partition, stored),
/// the histogram row replaced, buckets replaced, and a column's
/// `column_stats_usage.last_analyzed_at` stamped.
pub fn save_stats_item_sql(
    table_id: i64,
    item: &ClusterStatsItem,
    version: u64,
    stats_ver: i64,
    dump_fm_sketch: bool,
    last_analyzed_at: LastAnalyzedAt,
) -> Result<Vec<String>, StatsWriteError> {
    let key = [
        SqlArg::Signed(table_id),
        SqlArg::Signed(i64::from(item.is_index)),
        SqlArg::Signed(item.id),
    ];
    let mut statements = vec![must_escape_sql_text(
        "delete from mysql.stats_top_n where table_id = %? and is_index = %? and hist_id = %?",
        &key,
    )];
    statements.extend(save_topn_sql(table_id, item));
    statements.push(must_escape_sql_text(
        "delete from mysql.stats_fm_sketch where table_id = %? and is_index = %? and hist_id = %?",
        &key,
    ));
    if dump_fm_sketch {
        if let Some(fm_sketch) =
            tidb_stats::fmsketch_codec::encode_fm_sketch(item.fm_sketch.as_ref())
        {
            statements.push(must_escape_sql_text(
                "insert into mysql.stats_fm_sketch (table_id, is_index, hist_id, value) values (%?, %?, %?, %?)",
                &[
                    SqlArg::Signed(table_id),
                    SqlArg::Signed(i64::from(item.is_index)),
                    SqlArg::Signed(item.id),
                    SqlArg::Bytes(Some(&fm_sketch)),
                ],
            ));
        }
    }
    // Analyze v2 keeps no CMSketch; Go's encoder turns the nil one into NULL.
    let cm_sketch = if stats_ver == 2 {
        None
    } else {
        tidb_stats::cmsketch::encode_cmsketch_without_topn(item.cms.as_ref())
            .map_err(|error| StatsWriteError::Cms(error.to_string()))?
    };
    statements.push(must_escape_sql_text(
        "replace into mysql.stats_histograms (table_id, is_index, hist_id, distinct_count, version, null_count, cm_sketch, tot_col_size, stats_ver, correlation) values (%?, %?, %?, %?, %?, %?, %?, GREATEST(%?, 0), %?, %?)",
        &[
            SqlArg::Signed(table_id),
            SqlArg::Signed(i64::from(item.is_index)),
            SqlArg::Signed(item.id),
            SqlArg::Signed(item.histogram.ndv),
            SqlArg::Unsigned(version),
            SqlArg::Signed(item.histogram.null_count),
            SqlArg::Bytes(cm_sketch.as_deref()),
            SqlArg::Signed(item.histogram.tot_col_size),
            SqlArg::Signed(stats_ver),
            SqlArg::Float64(item.histogram.correlation),
        ],
    ));
    statements.push(must_escape_sql_text(
        "delete from mysql.stats_buckets where table_id = %? and is_index = %? and hist_id = %?",
        &key,
    ));
    statements.extend(save_buckets_sql(table_id, item)?);
    let usage_sql = match last_analyzed_at {
        LastAnalyzedAt::Values => Some(
            "insert into mysql.column_stats_usage (table_id, column_id, last_analyzed_at) values(%?, %?, current_timestamp()) on duplicate key update last_analyzed_at = values(last_analyzed_at)",
        ),
        LastAnalyzedAt::CurrentTimestamp => Some(
            "insert into mysql.column_stats_usage (table_id, column_id, last_analyzed_at) values(%?, %?, current_timestamp()) on duplicate key update last_analyzed_at = current_timestamp()",
        ),
        LastAnalyzedAt::Unchanged => None,
    };
    if let (false, Some(usage_sql)) = (item.is_index, usage_sql) {
        statements.push(must_escape_sql_text(
            usage_sql,
            &[SqlArg::Signed(table_id), SqlArg::Signed(item.id)],
        ));
    }
    Ok(statements)
}




/// One SQL statement from pinned Go `InsertTableStats2KV`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InsertTableStatsStatement {
    /// `INSERT IGNORE` of the physical table's metadata row.
    Meta {
        /// New table or partition physical ID.
        physical_id: i64,
    },
    /// `INSERT IGNORE` of one column or index histogram placeholder.
    Histogram {
        /// New table or partition physical ID.
        physical_id: i64,
        /// Whether the placeholder belongs to an index rather than a column.
        is_index: bool,
        /// Column or index ID.
        hist_id: i64,
    },
}

pub fn signed_value(value: Option<&Datum>) -> Option<i64> {
    match value {
        Some(Datum::Int(value)) => Some(*value),
        Some(Datum::UInt(value)) => i64::try_from(*value).ok(),
        _ => None,
    }
}

pub fn unsigned_value(value: Option<&Datum>) -> Option<u64> {
    match value {
        Some(Datum::UInt(value)) => Some(*value),
        Some(Datum::Int(value)) => u64::try_from(*value).ok(),
        _ => None,
    }
}

pub fn predicate_time_value(
    table: &TableInfo,
    column: &str,
    value: Option<Time>,
) -> Result<Datum, StatsWriteError> {
    let Some(value) = value else {
        return Ok(Datum::Null);
    };
    // Pinned SaveColumnStatsUsageForTable binds the Time string through
    // CONVERT_TZ. That builtin returns SQL NULL for an incomplete date.
    if value.invalid_zero() {
        return Ok(Datum::Null);
    }
    let field_type = table
        .find_public_column_by_name(column)
        .ok_or_else(|| StatsWriteError::MissingTable(format!("mysql.column_stats_usage.{column}")))?
        .read()
        .field_type
        .clone();
    Datum::Time(value)
        .convert_to(&field_type, ConversionFlags::from_bits(0))
        .map(|converted| converted.value)
        .map_err(|error| StatsWriteError::PredicateTime(format!("{value:?}: {error}")))
}

fn loaded_topn_batches(table_id: i64, item: &ClusterStatsItem) -> Vec<Range<usize>> {
    const PREFIX: &str =
        "insert into mysql.stats_top_n (table_id, is_index, hist_id, value, count) values ";
    let Some(topn) = &item.topn else {
        return Vec::new();
    };
    let entries = topn.entries();
    let mut batches = Vec::new();
    let mut start = 0;
    while start < entries.len() {
        let mut end = (start + LOADED_STATS_BATCH_INSERT_SIZE).min(entries.len());
        let mut sql_len = PREFIX.len();
        for (index, entry) in entries.iter().enumerate().take(end).skip(start) {
            let row = must_escape_sql(
                "(%?, %?, %?, %?, %?)",
                &[
                    SqlArg::Signed(table_id),
                    SqlArg::Signed(i64::from(item.is_index)),
                    SqlArg::Signed(item.id),
                    SqlArg::Bytes(Some(&entry.encoded)),
                    SqlArg::Unsigned(entry.count),
                ],
            );
            let row_len = row.len() + usize::from(index > start);
            if index > start && sql_len + row_len > LOADED_STATS_MAX_INSERT_LENGTH {
                end = index;
                break;
            }
            sql_len += row_len;
        }
        batches.push(start..end);
        start = end;
    }
    batches
}

fn loaded_bucket_batches(
    table_id: i64,
    item: &ClusterStatsItem,
) -> Result<Vec<Range<usize>>, StatsWriteError> {
    const PREFIX: &str = "insert into mysql.stats_buckets (table_id, is_index, hist_id, bucket_id, count, repeats, lower_bound, upper_bound, ndv) values ";
    let buckets = &item.histogram.buckets;
    let mut batches = Vec::new();
    let mut start = 0;
    while start < buckets.len() {
        let mut end = (start + LOADED_STATS_BATCH_INSERT_SIZE).min(buckets.len());
        let mut sql_len = PREFIX.len();
        for (index, bucket) in buckets.iter().enumerate().take(end).skip(start) {
            let lower = bound_blob(&bucket.lower_bound)?;
            let upper = bound_blob(&bucket.upper_bound)?;
            let lower = match &lower {
                Datum::Null => None,
                Datum::Bytes(value) => Some(value.as_slice()),
                other => unreachable!("blob conversion returned {other:?}"),
            };
            let upper = match &upper {
                Datum::Null => None,
                Datum::Bytes(value) => Some(value.as_slice()),
                other => unreachable!("blob conversion returned {other:?}"),
            };
            let previous_count = index
                .checked_sub(1)
                .map_or(0, |previous| buckets[previous].count);
            let row = must_escape_sql(
                "(%?, %?, %?, %?, %?, %?, %?, %?, %?)",
                &[
                    SqlArg::Signed(table_id),
                    SqlArg::Signed(i64::from(item.is_index)),
                    SqlArg::Signed(item.id),
                    SqlArg::Signed(index as i64),
                    SqlArg::Signed(bucket.count - previous_count),
                    SqlArg::Signed(bucket.repeat),
                    SqlArg::Bytes(lower),
                    SqlArg::Bytes(upper),
                    SqlArg::Signed(bucket.ndv),
                ],
            );
            let row_len = row.len() + usize::from(index > start);
            if index > start && sql_len + row_len > LOADED_STATS_MAX_INSERT_LENGTH {
                end = index;
                break;
            }
            sql_len += row_len;
        }
        batches.push(start..end);
        start = end;
    }
    Ok(batches)
}

/// Go `convertBoundToBlob`: `Datum.ConvertTo(TypeBlob)`.
///
/// An index bound and a new-collation string bound are already `Bytes` and
/// convert to themselves; every other column bound becomes its text form,
/// which Go's `convertBoundFromBlob` parses back at the column's declared
/// type.
pub fn bound_blob(bound: &Datum) -> Result<Datum, StatsWriteError> {
    if bound.is_null() {
        return Ok(Datum::Null);
    }
    let blob = FieldType::new(FieldTypeCode::Blob).with_flen(UNSPECIFIED_LENGTH);
    bound
        .convert_to(&blob, ConversionFlags::from_bits(0))
        .map(|converted| match converted.value {
            Datum::String(string) => Datum::Bytes(string.bytes().to_vec()),
            other => other,
        })
        .map_err(|error| StatsWriteError::Bound(error.to_string()))
}

