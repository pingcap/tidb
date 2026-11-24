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

//! Go-compatible non-prepared physical-plan caching.
//!
//! Mirrors the non-prepared half of Go `pkg/planner/core/plan_cacheable_checker.go`
//! (`NonPreparedPlanCacheableWithCtx`, `isSelectStmtNonPrepCacheableFastCheck`,
//! `extractTableNames`, and the `nonPreparedPlanCacheableChecker` visitor)
//! plus Go `getPlanFromNonPreparedPlanCache`: literal parameterization creates
//! one retained marker-bearing statement, and the same `PlanCacheStmt`
//! used by SQL/binary PREPARE uses the shared session physical-entry owner. Parameter types,
//! schema/statistics versions, matched binding SQL, admission, recursive range
//! rebuild, and executor construction therefore have one implementation.

pub use tidb_planner::optimize::{NonPreparedPlan, NonPreparedPlanCache, NonPreparedPrivileges};
