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

//! Port of `pkg/domain/extract_test.go` (origin/master):
//! `TestExtractPlanWithoutHistoryView` (:33),
//! `TestExtractWithoutStmtSummaryPersistedEnabled` (:52), and
//! `TestExtractHandlePlanTask` (:73).
//!
//! All three need `extstore.NewExtStorage` over a temp dir, the global
//! ext-storage swap, a live Domain's `GetExtractHandle()`, and (for two of
//! them) the persisted statements-summary machinery. `pkg/domain/extract.go`
//! was screened and declined for transcreation (`tidb_domain`'s crate doc):
//! `ExtractTask` (extract.go:90) orchestrates internal-SQL reads over
//! `information_schema.statements_summary_history` plus
//! `plancodec.DecodePlan` and the ext-store dump tail.

#![cfg(test)]

/// Go `pkg/domain/extract_test.go:33::TestExtractPlanWithoutHistoryView`:
/// `ExtractTask` over a `NewExtractPlanTask` (extract.go:120) with
/// `UseHistoryView = false` succeeds against file-backed ext storage.
// go-parity-gap: extract.go is unported (internal-SQL session + plancodec +
// ext-store dump tail); see tidb_domain's crate doc.
#[test]
#[ignore = "go-parity-gap: extract.go ExtractTask is not transcreated"]
fn extract_plan_without_history_view() {}

/// Go
/// `pkg/domain/extract_test.go:52::TestExtractWithoutStmtSummaryPersistedEnabled`:
/// with the persisted statements summary set up, `UseHistoryView = true`
/// makes `ExtractTask` ERROR (the history view needs the persisted store).
// go-parity-gap: extract.go + stmtsummary/v2 persisted store are not
// transcreated.
#[test]
#[ignore = "go-parity-gap: extract.go + persisted stmtsummary are not \
           transcreated"]
fn extract_without_stmt_summary_persisted_enabled() {}

/// Go `pkg/domain/extract_test.go:73::TestExtractHandlePlanTask`: after a
/// real `select` against `test.t`, an extract task bounded by
/// [startTime, end] over `STATEMENTS_SUMMARY_HISTORY` (stmt_type Select,
/// schema test, table_names test.t) produces a non-empty dump name.
// go-parity-gap: extract.go's statements-summary SQL read path is not
// transcreated.
#[test]
#[ignore = "go-parity-gap: extract.go's summary-history read path is not \
           transcreated"]
fn extract_handle_plan_task() {}
