// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0.

//! Compatibility wrappers for canonical job priority calculation.
use crate::auto_analyze_runtime::calculator::calculate_weight_seconds;
pub use crate::auto_analyze_runtime::calculator::{EVENT_NEW_INDEX, EVENT_NONE};

#[must_use]
pub const fn special_event_weight(has_newly_added_index: bool) -> f64 {
    if has_newly_added_index {
        EVENT_NEW_INDEX
    } else {
        EVENT_NONE
    }
}

#[must_use]
pub fn calculate_priority_weight(
    change_percentage: f64,
    table_size: f64,
    last_analysis_duration_seconds: f64,
    has_newly_added_index: bool,
) -> f64 {
    calculate_weight_seconds(
        change_percentage,
        table_size,
        last_analysis_duration_seconds,
        has_newly_added_index,
    )
}
