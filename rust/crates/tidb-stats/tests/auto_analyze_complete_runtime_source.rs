// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0.

#![allow(missing_docs)]

#[path = "auto_analyze_complete_runtime/calculator.rs"]
mod calculator;
#[path = "auto_analyze_complete_runtime/ddl.rs"]
mod ddl;
#[path = "auto_analyze_complete_runtime/dynamic_partitioned.rs"]
mod dynamic_partitioned;
#[path = "auto_analyze_complete_runtime/end_to_end.rs"]
mod end_to_end;
#[path = "auto_analyze_complete_runtime/factory.rs"]
mod factory;
#[path = "auto_analyze_complete_runtime/interval.rs"]
mod interval;
#[path = "auto_analyze_complete_runtime/non_partitioned.rs"]
mod non_partitioned;
#[path = "auto_analyze_complete_runtime/static_partitioned.rs"]
mod static_partitioned;
