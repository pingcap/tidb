// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Go `resource_group/controller/metrics`. The collectors, their names, help
//! strings, labels and buckets are generated from the pinned Go package and
//! owned by `crate::pd::metrics` together with PD's client metrics, so one
//! initialization chooses the constant labels of both. This module is the
//! controller's view of that package.
//!
//! Go's package variables are reassigned by `InitAndRegisterMetrics`; here a
//! snapshot is taken where Go reads a variable, which observes the same
//! generation of collectors.

pub use crate::pd::metrics::resource_group::{global_metrics, init_and_register_metrics, Metrics};
