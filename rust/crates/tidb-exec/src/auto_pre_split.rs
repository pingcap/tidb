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

//! Pure automatic index pre-split planning from `pkg/ddl/index_auto_presplit.go`.
//!
//! The Go implementation obtains table/column statistics through a provider
//! and then performs the PD split operation.  This module owns the
//! dependency-free planning half: eligibility gates, TopN/histogram merging,
//! quantile sampling, and TiDB index-key construction.  The caller supplies a
//! loaded [`tidb_stats::Column`] so storage loading and PD capabilities remain
//! explicit at the real-TiKV boundary.

use std::fmt;

use tidb_codec::{decode_one, Encoder};
use tidb_datatype::{new_collation_enabled, Datum, UNSPECIFIED_LENGTH};
use tidb_model::index::IndexInfo;
use tidb_model::table_info::TableInfo;
use tidb_stats::{Column, VERSION_2};
use tidb_tablecodec::{
    generate_index_key, IndexColumn as CodecIndexColumn, IndexInfo as CodecIndexInfo,
};
use tidb_txnkv::{Handle, IntHandle};

use crate::system_row_write::codec_table_info;

/// Defaults copied from Go `getAutoPreSplitConfig`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AutoPreSplitConfig {
    /// Minimum realtime table rows before AUTO is considered.
    pub min_table_rows: i64,
    /// Minimum statistics health percentage.
    pub min_stats_healthy: i64,
    /// Fractional step between internal sampled boundaries.
    pub boundary_ratio_step: f64,
}

impl Default for AutoPreSplitConfig {
    fn default() -> Self {
        Self {
            min_table_rows: 1_000_000,
            min_stats_healthy: 80,
            boundary_ratio_step: 0.02,
        }
    }
}

/// Result of the best-effort AUTO planner.  A skipped plan is not an error:
/// Go continues add-index when statistics are unavailable or unreliable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AutoPreSplitPlan {
    /// Sorted, deduplicated index split keys.
    Planned(Vec<Vec<u8>>),
    /// Best-effort skip and its source-compatible diagnostic reason.
    Skipped(String),
}

/// A planning failure that should be logged by the best-effort DDL caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutoPreSplitError(String);

impl AutoPreSplitError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for AutoPreSplitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for AutoPreSplitError {}

#[derive(Clone, Debug)]
struct WeightedValue {
    value: Datum,
    encoded: Vec<u8>,
    count: u64,
}

/// Plans AUTO split keys for one non-partitioned index.
///
/// `stats_healthy` is `None` when the statistics table is pseudo or otherwise
/// cannot report health.  The caller is expected to pass a loaded V2 column;
/// an evicted column must be loaded through the provider before this function
/// is called, matching Go's `ReadColumnDistributionStats` boundary.
pub fn plan_auto_pre_split_index_keys(
    table: &TableInfo,
    index: &IndexInfo,
    realtime_row_count: i64,
    stats_healthy: Option<i64>,
    column: Option<&Column>,
    config: AutoPreSplitConfig,
) -> Result<AutoPreSplitPlan, AutoPreSplitError> {
    if table.get_partition_info().is_some() {
        return Ok(AutoPreSplitPlan::Skipped("partitioned table".to_owned()));
    }
    if index.has_condition() {
        return Ok(AutoPreSplitPlan::Skipped("partial index".to_owned()));
    }
    if index.columns.is_empty() {
        return Ok(AutoPreSplitPlan::Skipped("index has no columns".to_owned()));
    }
    let Some(healthy) = stats_healthy else {
        return Ok(AutoPreSplitPlan::Skipped(
            "stats health unavailable".to_owned(),
        ));
    };
    if healthy < config.min_stats_healthy {
        return Ok(AutoPreSplitPlan::Skipped(format!(
            "stats health {healthy} below threshold {}",
            config.min_stats_healthy
        )));
    }
    if realtime_row_count < config.min_table_rows {
        return Ok(AutoPreSplitPlan::Skipped(format!(
            "row count {realtime_row_count} below threshold {}",
            config.min_table_rows
        )));
    }

    let leading_handle = index
        .columns
        .get(0)
        .ok_or_else(|| AutoPreSplitError::new("index has no leading column"))?;
    let leading = leading_handle.read();
    let offset = usize::try_from(leading.offset)
        .map_err(|_| AutoPreSplitError::new("leading column not found"))?;
    let Some(column_info) = table.columns.get(offset) else {
        return Ok(AutoPreSplitPlan::Skipped(
            "leading column not found".to_owned(),
        ));
    };
    let column_info = column_info.read();
    if column_info.field_type.is_string() && leading.length != UNSPECIFIED_LENGTH {
        return Ok(AutoPreSplitPlan::Skipped(
            "leading string column uses prefix index".to_owned(),
        ));
    }
    let Some(column) = column else {
        return Ok(AutoPreSplitPlan::Skipped(
            "leading column stats metadata missing".to_owned(),
        ));
    };
    if column.stats_version != VERSION_2 {
        return Ok(AutoPreSplitPlan::Skipped(format!(
            "leading column stats version {} is not Analyze V2",
            column.stats_version
        )));
    }
    if column.histogram.null_count < 0 {
        return Err(AutoPreSplitError::new(format!(
            "leading column statistics have negative null count {}",
            column.histogram.null_count
        )));
    }

    let mut values = Vec::new();
    if column.histogram.null_count > 0 {
        values.push(weighted_value(
            Datum::Null,
            column.histogram.null_count as u64,
            &column_info.field_type,
        )?);
    }
    if let Some(top_n) = &column.top_n {
        for entry in top_n.entries() {
            let (remain, mut datum) = decode_one(&entry.encoded).map_err(|error| {
                AutoPreSplitError::new(format!("failed to decode TopN value: {error}"))
            })?;
            if !remain.is_empty() {
                return Err(AutoPreSplitError::new(
                    "failed to decode TopN value: trailing bytes",
                ));
            }
            if column_info.field_type.is_string()
                && datum.kind() == tidb_datatype::DatumKind::String
            {
                datum = Datum::new_bytes(datum.go_bytes());
            }
            values.push(weighted_value(datum, entry.count, &column_info.field_type)?);
        }
    }
    let mut previous = 0_i64;
    for (position, bucket) in column.histogram.buckets.iter().enumerate() {
        if bucket.count < previous {
            return Err(AutoPreSplitError::new(format!(
                "histogram bucket {position} cumulative count {} is below previous count {previous}",
                bucket.count
            )));
        }
        let delta = bucket.count - previous;
        previous = bucket.count;
        if delta == 0 {
            continue;
        }
        let mut upper = bucket.upper_bound.clone();
        if column_info.field_type.is_string() {
            upper = Datum::new_bytes(upper.go_bytes());
        }
        values.push(weighted_value(
            upper,
            delta as u64,
            &column_info.field_type,
        )?);
    }

    let (values, total) = merge_values(values);
    if total == 0 {
        return Ok(AutoPreSplitPlan::Skipped(
            "no usable leading column distribution".to_owned(),
        ));
    }
    let boundaries = sample_values(&values, total, config.boundary_ratio_step);
    if boundaries.is_empty() {
        return Ok(AutoPreSplitPlan::Skipped(
            "no internal distribution boundary".to_owned(),
        ));
    }

    let codec_table = codec_table_info(table);
    let codec_index = codec_index(index)?;
    let first_index_id = table
        .indices
        .iter_deref()
        .next()
        .map(|value| value.read().id);
    let mut keys = Vec::with_capacity(boundaries.len() + 2);
    if first_index_id != Some(index.id) {
        keys.push(tidb_codec::table_key::encode_index_seek_key(
            table.id,
            index.id,
            &[],
        ));
    }
    keys.push(tidb_codec::table_key::encode_index_seek_key(
        table.id,
        index.id + 1,
        &[],
    ));
    for value in boundaries {
        let mut values = vec![value];
        let (key, _) = generate_index_key(
            tidb_codec::Encoder::new(new_collation_enabled()),
            None,
            &codec_table,
            &codec_index,
            table.id,
            &mut values,
            Some(&Handle::Int(IntHandle::new(i64::MIN))),
        )
        .map_err(|error| {
            AutoPreSplitError::new(format!("failed to build auto presplit key: {error}"))
        })?;
        keys.push(key);
    }
    keys.retain(|key| !key.is_empty());
    keys.sort();
    keys.dedup();
    Ok(AutoPreSplitPlan::Planned(keys))
}

fn weighted_value(
    value: Datum,
    count: u64,
    _field_type: &tidb_datatype::FieldType,
) -> Result<WeightedValue, AutoPreSplitError> {
    let encoded = Encoder::new(new_collation_enabled())
        .encode_key(&[value.clone()])
        .map_err(|error| {
            AutoPreSplitError::new(format!("failed to encode distribution value: {error}"))
        })?;
    Ok(WeightedValue {
        value,
        encoded,
        count,
    })
}

fn merge_values(mut values: Vec<WeightedValue>) -> (Vec<WeightedValue>, u64) {
    values.retain(|value| value.count != 0);
    values.sort_by(|left, right| left.encoded.cmp(&right.encoded));
    let mut merged: Vec<WeightedValue> = Vec::with_capacity(values.len());
    let mut total = 0_u64;
    for value in values {
        total = total.saturating_add(value.count);
        if let Some(previous) = merged.last_mut() {
            if previous.encoded == value.encoded {
                previous.count = previous.count.saturating_add(value.count);
                continue;
            }
        }
        merged.push(value);
    }
    (merged, total)
}

fn sample_values(values: &[WeightedValue], total: u64, step: f64) -> Vec<Datum> {
    if total == 0 || step <= 0.0 {
        return Vec::new();
    }
    let mut next_threshold = 1_usize;
    let mut cumulative = 0_u64;
    let mut rows = Vec::new();
    for value in values {
        cumulative = cumulative.saturating_add(value.count);
        let threshold = next_threshold as f64 * step;
        if threshold >= 1.0 {
            break;
        }
        let ratio = cumulative as f64 / total as f64;
        if ratio < threshold {
            continue;
        }
        rows.push(value.value.clone());
        let crossed = (ratio / step).floor() as usize;
        next_threshold = (next_threshold + 1).max(crossed + 1);
    }
    rows
}

fn codec_index(index: &IndexInfo) -> Result<CodecIndexInfo, AutoPreSplitError> {
    let columns = index
        .columns
        .iter_deref()
        .map(|column| {
            let column = column.read();
            Ok(CodecIndexColumn {
                offset: usize::try_from(column.offset)
                    .map_err(|_| AutoPreSplitError::new("index column offset is negative"))?,
                length: column.length,
                use_changing_type: column.use_changing_type,
            })
        })
        .collect::<Result<Vec<_>, AutoPreSplitError>>()?;
    Ok(CodecIndexInfo {
        id: index.id,
        columns,
        unique: index.unique,
        global: index.global,
        global_index_version: index.global_index_version,
        primary: index.primary,
    })
}

#[cfg(test)]
mod tests {
    use super::{merge_values, sample_values, weighted_value};
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};

    #[test]
    fn topn_and_histogram_values_merge_before_quantile_sampling() {
        let values = vec![
            weighted_value(
                Datum::new_int(10),
                15,
                &FieldType::new(FieldTypeCode::LongLong),
            )
            .unwrap(),
            weighted_value(
                Datum::new_int(10),
                5,
                &FieldType::new(FieldTypeCode::LongLong),
            )
            .unwrap(),
            weighted_value(
                Datum::new_int(20),
                20,
                &FieldType::new(FieldTypeCode::LongLong),
            )
            .unwrap(),
        ];
        let (merged, total) = merge_values(values);
        assert_eq!(total, 40);
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].count, 20);
        assert_eq!(sample_values(&merged, total, 0.5), vec![Datum::new_int(10)]);
    }

    #[test]
    fn one_value_crossing_multiple_thresholds_is_emitted_once() {
        let values = vec![
            weighted_value(
                Datum::new_int(10),
                60,
                &FieldType::new(FieldTypeCode::LongLong),
            )
            .unwrap(),
            weighted_value(
                Datum::new_int(20),
                40,
                &FieldType::new(FieldTypeCode::LongLong),
            )
            .unwrap(),
        ];
        assert_eq!(
            sample_values(&values, 100, 0.2),
            vec![Datum::new_int(10), Datum::new_int(20)]
        );
    }
}
