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
//! `pkg/planner/core/rule/collect_column_stats_usage_test.go:127
//! TestSkipSystemTables`.
//!
//! go-parity-gap: needs parse → Preprocess → PlanBuilder.Build →
//! `core.LogicalOptimizeTest` over a session infoschema, plus the rule's
//! unported carrier (`CollectColumnStatsUsage` over the logical plan tree).
//! Go plans `select * from mysql.stats_meta where a > 1` and requires
//! `checkColumnStatsUsageForPredicates` (:104-124) to report an EMPTY
//! column list both BEFORE and AFTER logical optimization — system
//! (mysql.) tables are skipped by the column-stats-usage collection, and
//! the predicate columns never surface as load items. The sibling
//! `tests_extractor_memtable_infoschema_source.rs` family pins other parts
//! of that file; this item is distinct and unported.

/// GO PARITY GAP port of
/// `pkg/planner/core/rule/collect_column_stats_usage_test.go:127
/// TestSkipSystemTables`.
///
/// go-parity-gap: collect-column-stats-usage rule + session optimize
/// pipeline unported; the empty-usage contract for mysql.* tables is
/// unobservable.
#[test]
#[ignore = "go-parity-gap: CollectColumnStatsUsage rule + session optimize pipeline unported"]
fn skip_system_tables_reports_no_predicate_columns() {}
