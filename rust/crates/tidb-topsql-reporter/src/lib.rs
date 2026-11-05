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

//! Go `pkg/util/topsql/reporter`: the Top SQL reporter, its data models,
//! and the data sinks it reports to.
//!
//! - [`datamodel`], [`ru_datamodel`], [`ru_window_aggregator`] — the
//!   in-memory collection models (`datamodel.go`, `ru_datamodel.go`,
//!   `ru_window_aggregator.go`).
//! - [`reporter`] — `reporter.go`: `RemoteTopSQLReporter`, which collects
//!   CPU time from the profiler-fed SQL CPU collector, statement stats and
//!   RU increments, and reports each minute to its registered data sinks.
//! - [`datasink`] — `datasink.go`: `DataSink`, `ReportData`, and the
//!   registerer that turns Top SQL / Top RU on while a sink needs them.
//! - [`pubsub`] — `pubsub.go`: the `tipb.TopSQLPubSub` service a subscriber
//!   (NG Monitoring) streams the reports from.
//! - [`single_target`] — `single_target.go`: the sink that pushes reports to
//!   the configured `top-sql.receiver-address` agent.
//! - [`report_ticker`] — `report_ticker.go`.

pub mod datamodel;
pub mod datasink;
pub mod pubsub;
pub mod report_ticker;
pub mod reporter;
pub mod ru_datamodel;
pub mod ru_window_aggregator;
pub mod single_target;
