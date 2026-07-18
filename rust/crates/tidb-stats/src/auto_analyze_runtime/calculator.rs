// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0.

use super::model::JobIndicators;

pub const EVENT_NONE: f64 = 0.0;
pub const EVENT_NEW_INDEX: f64 = 2.0;

/// Exact Go priority formula over already-built job indicators.
#[must_use]
pub fn calculate_weight(indicators: JobIndicators, has_new_index: bool) -> f64 {
    calculate_weight_seconds(
        indicators.change_percentage,
        indicators.table_size,
        indicators.last_analysis_duration_nanos as f64 / 1_000_000_000.0,
        has_new_index,
    )
}

/// Scalar entrypoint retaining Go's IEEE behavior for every input domain.
#[must_use]
pub fn calculate_weight_seconds(
    change_percentage: f64,
    table_size: f64,
    last_analysis_duration_seconds: f64,
    has_new_index: bool,
) -> f64 {
    let change_ratio = 100.0 * change_percentage;
    0.6 * (1.0 + change_ratio).log10()
        + 0.1 * (1.0 - (1.0 + table_size).log10())
        + 0.3 * (1.0 + last_analysis_duration_seconds.sqrt()).log10()
        + if has_new_index {
            EVENT_NEW_INDEX
        } else {
            EVENT_NONE
        }
}
