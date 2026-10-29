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

//! Go `pkg/statistics/util/json_objects.go`: the complete statistics dump
//! object model shared by dump/load and historical statistics paths.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Go `statsutil.TiDBGlobalStats`.
pub const TIDB_GLOBAL_STATS: &str = "global";

/// Go `statsutil.JSONTable`.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct JsonTable {
    /// Column statistics keyed by lowercase column name.
    #[serde(default)]
    pub columns: Option<BTreeMap<String, Option<JsonColumn>>>,
    /// Index statistics keyed by lowercase index name.
    #[serde(default)]
    pub indices: Option<BTreeMap<String, Option<JsonColumn>>>,
    /// Per-partition statistics plus the optional `global` entry.
    #[serde(default)]
    pub partitions: Option<BTreeMap<String, Option<JsonTable>>>,
    /// Database name recorded by the dump.
    #[serde(default)]
    pub database_name: String,
    /// Table name recorded by the dump.
    #[serde(default)]
    pub table_name: String,
    /// Predicate-column usage records.
    #[serde(default)]
    pub predicate_columns: Option<Vec<Option<JsonPredicateColumn>>>,
    /// Realtime row count.
    #[serde(default)]
    pub count: i64,
    /// Modified-row count.
    #[serde(default)]
    pub modify_count: i64,
    /// Statistics metadata version.
    #[serde(default)]
    pub version: u64,
    /// Whether this object came from historical statistics storage.
    #[serde(default)]
    pub is_historical_stats: bool,
}

impl JsonTable {
    /// Go `(*JSONTable).Sort`.
    pub fn sort(&mut self) {
        if let Some(columns) = &mut self.predicate_columns {
            columns.sort_unstable_by_key(|column| {
                column
                    .as_ref()
                    .expect("nil JSONPredicateColumn in JSONTable.Sort")
                    .id
            });
        }
    }
}

/// Go `statsutil.JSONColumn`, used for both columns and indexes.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct JsonColumn {
    /// Protobuf histogram represented through Go's JSON encoder.
    #[serde(default)]
    pub histogram: Option<JsonHistogram>,
    /// Protobuf CMSketch represented through Go's JSON encoder.
    #[serde(default)]
    pub cm_sketch: Option<JsonCmSketch>,
    /// Protobuf FMSketch represented through Go's JSON encoder.
    #[serde(default)]
    pub fm_sketch: Option<JsonFmSketch>,
    /// Optional because old statistics dumps predate this field.
    #[serde(default)]
    pub stats_ver: Option<i64>,
    /// Null count.
    #[serde(default)]
    pub null_count: i64,
    /// Total column size.
    #[serde(default)]
    pub tot_col_size: i64,
    /// Last histogram update version.
    #[serde(default)]
    pub last_update_version: u64,
    /// Column correlation.
    #[serde(default, serialize_with = "serialize_go_float")]
    pub correlation: f64,
}

impl JsonColumn {
    /// Go `(*JSONColumn).TotalMemoryUsage`: the sum of protobuf wire sizes.
    #[must_use]
    pub fn total_memory_usage(&self) -> i64 {
        let size = self
            .histogram
            .as_ref()
            .map_or(0, JsonHistogram::encoded_len)
            + self.cm_sketch.as_ref().map_or(0, JsonCmSketch::encoded_len)
            + self.fm_sketch.as_ref().map_or(0, JsonFmSketch::encoded_len);
        i64::try_from(size).unwrap_or(i64::MAX)
    }
}

/// Go `statsutil.JSONPredicateColumn`.
#[derive(Clone, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
pub struct JsonPredicateColumn {
    /// Last usage timestamp.
    #[serde(default)]
    pub last_used_at: Option<String>,
    /// Last analyze timestamp.
    #[serde(default)]
    pub last_analyzed_at: Option<String>,
    /// Column identifier.
    #[serde(default)]
    pub id: i64,
}

/// JSON form of `tipb.Histogram`.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct JsonHistogram {
    /// Number of distinct values.
    #[serde(default)]
    pub ndv: i64,
    /// Histogram buckets.
    #[serde(default, skip_serializing_if = "option_vec_is_none_or_empty")]
    pub buckets: Option<Vec<Option<JsonBucket>>>,
}

impl JsonHistogram {
    fn encoded_len(&self) -> usize {
        scalar_len(1, self.ndv as u64, true)
            + self
                .buckets
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(|bucket| {
                    let len = bucket.as_ref().map_or(0, JsonBucket::encoded_len);
                    message_len(2, len)
                })
                .sum::<usize>()
    }
}

/// JSON form of `tipb.Bucket`.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct JsonBucket {
    /// Cumulative row count.
    #[serde(default)]
    pub count: i64,
    /// Base64-encoded lower bound.
    #[serde(default, skip_serializing_if = "option_string_is_none_or_empty")]
    pub lower_bound: Option<String>,
    /// Base64-encoded upper bound.
    #[serde(default, skip_serializing_if = "option_string_is_none_or_empty")]
    pub upper_bound: Option<String>,
    /// Upper-bound repeat count.
    #[serde(default)]
    pub repeats: i64,
    /// Optional per-bucket NDV.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ndv: Option<i64>,
}

impl JsonBucket {
    fn encoded_len(&self) -> usize {
        scalar_len(1, self.count as u64, true)
            + optional_bytes_len(2, self.lower_bound.as_deref())
            + optional_bytes_len(3, self.upper_bound.as_deref())
            + scalar_len(4, self.repeats as u64, true)
            + self.ndv.map_or(0, |ndv| scalar_len(5, ndv as u64, true))
    }
}

/// JSON form of `tipb.CMSketch`.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct JsonCmSketch {
    /// Counter rows.
    #[serde(default, skip_serializing_if = "option_vec_is_none_or_empty")]
    pub rows: Option<Vec<Option<JsonCmSketchRow>>>,
    /// TopN rows.
    #[serde(default, skip_serializing_if = "option_vec_is_none_or_empty")]
    pub top_n: Option<Vec<Option<JsonCmSketchTopN>>>,
    /// Default counter value.
    #[serde(default)]
    pub default_value: u64,
}

impl JsonCmSketch {
    fn encoded_len(&self) -> usize {
        self.rows
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|row| {
                let len = row.as_ref().map_or(0, JsonCmSketchRow::encoded_len);
                message_len(1, len)
            })
            .sum::<usize>()
            + self
                .top_n
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(|entry| {
                    let len = entry.as_ref().map_or(0, JsonCmSketchTopN::encoded_len);
                    message_len(2, len)
                })
                .sum::<usize>()
            + scalar_len(3, self.default_value, true)
    }
}

/// JSON form of `tipb.CMSketchRow`.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct JsonCmSketchRow {
    /// Row counters.
    #[serde(default, skip_serializing_if = "option_vec_is_none_or_empty")]
    pub counters: Option<Vec<u32>>,
}

impl JsonCmSketchRow {
    fn encoded_len(&self) -> usize {
        self.counters
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|counter| scalar_len(1, u64::from(*counter), true))
            .sum()
    }
}

/// JSON form of `tipb.CMSketchTopN`.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct JsonCmSketchTopN {
    /// Base64-encoded datum or index key.
    #[serde(default, skip_serializing_if = "option_string_is_none_or_empty")]
    pub data: Option<String>,
    /// Row count.
    #[serde(default)]
    pub count: u64,
}

impl JsonCmSketchTopN {
    fn encoded_len(&self) -> usize {
        optional_bytes_len(1, self.data.as_deref()) + scalar_len(2, self.count, true)
    }
}

/// JSON form of `tipb.FMSketch`.
#[derive(Clone, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
pub struct JsonFmSketch {
    /// Sketch mask.
    #[serde(default)]
    pub mask: u64,
    /// Retained hashes.
    #[serde(default, skip_serializing_if = "option_vec_is_none_or_empty")]
    pub hashset: Option<Vec<u64>>,
}

impl JsonFmSketch {
    fn encoded_len(&self) -> usize {
        scalar_len(1, self.mask, true)
            + self
                .hashset
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(|hash| scalar_len(2, *hash, true))
                .sum::<usize>()
    }
}

fn varint_len(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

fn tag_len(field: u64, wire: u64) -> usize {
    varint_len((field << 3) | wire)
}

fn scalar_len(field: u64, value: u64, present: bool) -> usize {
    present
        .then(|| tag_len(field, 0) + varint_len(value))
        .unwrap_or(0)
}

fn message_len(field: u64, len: usize) -> usize {
    tag_len(field, 2) + varint_len(len as u64) + len
}

fn optional_bytes_len(field: u64, value: Option<&str>) -> usize {
    value
        .map(|value| message_len(field, base64_decoded_len(value)))
        .unwrap_or(0)
}

fn base64_decoded_len(value: &str) -> usize {
    let padding = value
        .as_bytes()
        .iter()
        .rev()
        .take_while(|byte| **byte == b'=')
        .count();
    (value.len().saturating_mul(3) / 4).saturating_sub(padding.min(2))
}

fn option_vec_is_none_or_empty<T>(value: &Option<Vec<T>>) -> bool {
    value.as_ref().is_none_or(Vec::is_empty)
}

fn option_string_is_none_or_empty(value: &Option<String>) -> bool {
    value.as_ref().is_none_or(String::is_empty)
}

fn serialize_go_float<S>(value: &f64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    if value.is_finite()
        && value.fract() == 0.0
        && *value >= i64::MIN as f64
        && *value <= i64::MAX as f64
    {
        serializer.serialize_i64(*value as i64)
    } else {
        serializer.serialize_f64(*value)
    }
}
