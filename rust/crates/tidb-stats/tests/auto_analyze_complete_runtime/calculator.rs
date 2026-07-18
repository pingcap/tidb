// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0.

use tidb_stats::auto_analyze_runtime::{
    calculate_weight, JobIndicators, EVENT_NEW_INDEX, EVENT_NONE,
};

#[test]
fn priority_weight_tracks_change_size_interval_and_special_event() {
    let base = JobIndicators {
        change_percentage: 0.1,
        table_size: 10.0,
        last_analysis_duration_nanos: 10_000_000_000,
    };
    assert!(
        calculate_weight(
            JobIndicators {
                change_percentage: 0.9,
                ..base
            },
            false
        ) > calculate_weight(base, false)
    );
    assert!(
        calculate_weight(
            JobIndicators {
                table_size: 1000.0,
                ..base
            },
            false
        ) < calculate_weight(base, false)
    );
    assert!(
        calculate_weight(
            JobIndicators {
                last_analysis_duration_nanos: 100_000_000_000,
                ..base
            },
            false
        ) > calculate_weight(base, false)
    );
    assert_eq!(
        calculate_weight(base, true) - calculate_weight(base, false),
        EVENT_NEW_INDEX
    );
}

#[test]
fn special_event_constants_are_source_exact() {
    assert_eq!(EVENT_NONE, 0.0);
    assert_eq!(EVENT_NEW_INDEX, 2.0);
}
