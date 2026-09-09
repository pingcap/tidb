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

//! `pkg/planner.part14` DOCUMENTED GAP port for
//! `pkg/planner/core/plan_replayer_capture_test.go:34
//! TestPlanReplayerCaptureRecordJsonStats`.
//!
//! go-parity-gap: needs the plan-replayer capture machinery (session vars,
//! `PlanReplayerSink`/`PlanReplayerTaskKey` and the Domain stats handle).
//! Go builds `t1`/`t2` with analyzed statistics, enables
//! `EnablePlanReplayerCapture` on a MockContext-backed builder, and pins
//! that the captured plan registers JSON-statistics tasks for exactly the
//! tables the statement touches: one for `select * from t1`, one for
//! `select * from t2`, TWO for `select * from t1,t2`
//! (`getTableStats` :58-77 walking `plancodec`-decoded plan node table
//! ids). The carrier (`plan replayer` capture) is unported in this
//! workspace, so the observation cannot run.

/// GO PARITY GAP port of `pkg/planner/core/plan_replayer_capture_test.go:34
/// TestPlanReplayerCaptureRecordJsonStats`.
///
/// go-parity-gap: plan-replayer capture sink + stats-handle session stack
/// unported; the per-table JSON-stats task count is unobservable.
#[test]
#[ignore = "go-parity-gap: plan-replayer capture (EnablePlanReplayerCapture + stats handle) unported"]
fn plan_replayer_capture_records_json_stats_for_each_touched_table() {}
