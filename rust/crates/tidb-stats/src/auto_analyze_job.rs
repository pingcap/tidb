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

//! Dependency-closed auto-analyze job metadata from
//! `pkg/statistics/handle/autoanalyze/priorityqueue/job.go`.
//!
//! Concrete jobs still own schema/session validation, SQL execution, hooks,
//! and stringer output. This leaf keeps the scalar indicator representation,
//! its source JSON formatting, and the dynamic-job kind predicate available
//! for those future owners.

/// Scalar indicators used to rank an auto-analyze job.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AnalysisIndicators {
    /// Fraction of rows changed since the previous analysis.
    pub change_percentage: f64,
    /// Table size in rows multiplied by the number of columns.
    pub table_size: f64,
    /// Time since the previous analysis, in nanoseconds.
    pub last_analysis_duration_nanos: i64,
}

impl Default for AnalysisIndicators {
    fn default() -> Self {
        Self {
            change_percentage: 0.0,
            table_size: 0.0,
            last_analysis_duration_nanos: 0,
        }
    }
}

/// Source-shaped alias retained for future concrete auto-analyze jobs.
pub type Indicators = AnalysisIndicators;

/// JSON-shaped indicator strings produced by Go's `asJSONIndicators`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndicatorsJson {
    /// Change percentage formatted with two fractional digits and `%`.
    pub change_percentage: String,
    /// Table size formatted with two fractional digits.
    pub table_size: String,
    /// Go `time.Duration.String()` representation.
    pub last_analysis_duration: String,
}

/// Source-shaped alias for the JSON indicator payload.
pub type IndicatorsJSON = IndicatorsJson;

/// Concrete auto-analyze job kinds known by the priority queue.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AnalysisJobKind {
    /// A non-partitioned table or table-index job.
    NonPartitioned,
    /// A dynamic partitioned table or index job.
    DynamicPartitioned,
    /// A static partitioned table or index job.
    StaticPartitioned,
    /// A future or external job kind.
    Other,
}

/// Returns whether a job kind is the source dynamic-partitioned variant.
#[must_use]
pub const fn is_dynamic_partitioned_table_analysis_job(kind: AnalysisJobKind) -> bool {
    matches!(kind, AnalysisJobKind::DynamicPartitioned)
}

/// Converts source indicators to their JSON string representation.
#[must_use]
pub fn as_json_indicators(indicators: AnalysisIndicators) -> IndicatorsJson {
    IndicatorsJson {
        change_percentage: format!("{:.2}%", indicators.change_percentage * 100.0),
        table_size: format!("{:.2}", indicators.table_size),
        last_analysis_duration: format_duration(indicators.last_analysis_duration_nanos),
    }
}

fn format_duration(nanos: i64) -> String {
    if nanos == 0 {
        return "0s".to_owned();
    }

    let negative = nanos < 0;
    let magnitude = nanos.unsigned_abs();
    let seconds = magnitude / 1_000_000_000;
    let remainder = magnitude % 1_000_000_000;
    let mut result = String::new();
    if negative {
        result.push('-');
    }

    if seconds >= 3600 {
        result.push_str(&(seconds / 3600).to_string());
        result.push('h');
        result.push_str(&((seconds % 3600) / 60).to_string());
        result.push('m');
        append_seconds(&mut result, seconds % 60, remainder);
    } else if seconds >= 60 {
        result.push_str(&(seconds / 60).to_string());
        result.push('m');
        append_seconds(&mut result, seconds % 60, remainder);
    } else if seconds > 0 {
        append_seconds(&mut result, seconds, remainder);
    } else if magnitude >= 1_000_000 {
        append_fraction(&mut result, magnitude, 1_000_000, "ms");
    } else if magnitude >= 1_000 {
        append_fraction(&mut result, magnitude, 1_000, "µs");
    } else {
        result.push_str(&magnitude.to_string());
        result.push_str("ns");
    }
    result
}

fn append_seconds(result: &mut String, seconds: u64, remainder: u64) {
    result.push_str(&seconds.to_string());
    if remainder > 0 {
        let mut fraction = format!("{:09}", remainder);
        while fraction.ends_with('0') {
            fraction.pop();
        }
        result.push('.');
        result.push_str(&fraction);
    }
    result.push('s');
}

fn append_fraction(result: &mut String, magnitude: u64, unit: u64, suffix: &str) {
    let whole = magnitude / unit;
    let remainder = magnitude % unit;
    result.push_str(&whole.to_string());
    if remainder > 0 {
        let digits = if unit == 1_000_000 { 6 } else { 3 };
        let mut fraction = format!("{:0width$}", remainder, width = digits);
        while fraction.ends_with('0') {
            fraction.pop();
        }
        result.push('.');
        result.push_str(&fraction);
    }
    result.push_str(suffix);
}
