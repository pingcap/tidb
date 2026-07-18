// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0.

//! Concrete, dependency-injected auto-analyze runtime.

pub mod calculator;
pub mod ddl;
pub mod factory;
pub mod interval;
pub mod jobs;
pub mod model;
pub mod ports;

pub use calculator::{calculate_weight, EVENT_NEW_INDEX, EVENT_NONE};
pub use ddl::{DdlEvent, DdlHandleOutcome, DdlRuntime, LiveQueueAdapter};
pub use factory::{AnalysisJobFactory, AutoAnalysisTimeWindow};
pub use interval::{average_analysis_duration, last_failed_analysis_duration};
pub use jobs::{
    AnalysisJobRuntime, DynamicPartitionedJob, NonPartitionedJob, StaticPartitionedJob,
};
pub use model::*;
pub use ports::*;
