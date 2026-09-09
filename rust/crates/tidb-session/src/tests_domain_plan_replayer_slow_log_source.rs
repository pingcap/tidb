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

//! Port of `pkg/domain/plan_replayer_slow_log_test.go::TestPlanReplayerInternalQuery`
//! (origin/master, :32): queries issued by the plan-replayer task collector
//! must be marked INTERNAL — visible as `Is_internal: true` in the slow log
//! and excluded from the statement summary when
//! `tidb_stmt_summary_internal_query = OFF`.
//!
//! The Go test runs a full server (slow-log file, statement summary, live
//! Domain); the internal-SQL source-type marking that decides this lives in
//! the executor/session layers this tier does not have, so the port is a
//! documentary ignored gap.

#![cfg(test)]

/// Go
/// `pkg/domain/plan_replayer_slow_log_test.go:32::TestPlanReplayerInternalQuery`:
/// after `CollectPlanReplayerTask` runs against a slow log with threshold 0
/// and `tidb_stmt_summary_internal_query = 0`, the slow log's
/// plan-replayer entries carry `Is_internal` true and no statement-summary
/// entry records them.
// go-parity-gap: slow-log capture + internal-statement classification are
// not transcreated.
#[test]
#[ignore = "go-parity-gap: slow log + internal-query classification are not \
           transcreated"]
fn plan_replayer_internal_query() {}
