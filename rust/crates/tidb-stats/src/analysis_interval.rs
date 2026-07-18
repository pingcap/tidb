// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0.

//! Compatibility exports for canonical injected interval execution.
pub use crate::auto_analyze_runtime::interval::{
    average_analysis_duration_from_seconds, average_duration_query,
    last_failed_analysis_duration_from_seconds, last_failed_duration_query,
    AVG_PARTITIONS as AVG_DURATION_QUERY_FOR_PARTITION, AVG_TABLE as AVG_DURATION_QUERY_FOR_TABLE,
    DEFAULT_FAILED_ANALYSIS_WAIT_NANOS,
    FAILED_PARTITIONS as LAST_FAILED_DURATION_QUERY_FOR_PARTITION,
    FAILED_TABLE as LAST_FAILED_DURATION_QUERY_FOR_TABLE, JUST_FAILED, NO_RECORD,
};
