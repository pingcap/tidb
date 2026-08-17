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

//! Go `pkg/planner/core/operator/logicalop/logical_show_ddl_jobs.go`:
//! `LogicalShowDDLJobs`, the `ADMIN SHOW DDL JOBS` leaf.
//!
//! SEED of `pkg/planner/core`. `LogicalShowDDLJobs` was a
//! [`crate::logical::TodoLogicalOp`] before this batch.
//!
//! The crate's `logical_show_ddl_jobs` identity leaf is KEPT rather than
//! merged: `difftests/planner-tests/tests/logical_show_ddl_jobs.rs` consumes its
//! `LogicalShowDDLJobsIdentity`/`ShowDDLJobsColumnIdentity` from OUTSIDE this
//! crate.
//!
//! Every member of this operator except `DeriveStats` INHERITS the base body,
//! so this file is exactly `JobNumber` plus that one override — which is itself
//! the same `getFakeStats` [`crate::logical::LogicalShow`] uses.

use tidb_expr::schema::Schema;

use crate::logical::show::get_fake_stats;
use crate::logical::BaseLogicalPlan;
use crate::stats_info::StatsInfo;

/// Go `logicalop.LogicalShowDDLJobs` (`logical_show_ddl_jobs.go:25`).
#[derive(Clone, Debug, Default)]
pub struct LogicalShowDDLJobs {
    /// The shared logical base.
    pub base: BaseLogicalPlan,
    /// Go `JobNumber`: how many jobs `ADMIN SHOW DDL JOBS <n>` asked for.
    pub job_number: i64,
}

impl LogicalShowDDLJobs {
    /// Go `plancodec.TypeShowDDLJobs`.
    pub const TYPE: &'static str = "ShowDDLJobs";

    /// Go `LogicalShowDDLJobs.Init(ctx)` (`logical_show_ddl_jobs.go:32`), which
    /// fixes the query-block offset at 0.
    #[must_use]
    pub const fn new(base: BaseLogicalPlan, job_number: i64) -> Self {
        Self { base, job_number }
    }

    /// Go `LogicalShowDDLJobs.DeriveStats(_, selfSchema, _, reloads)`
    /// (`logical_show_ddl_jobs.go:60`): Go's own words, "a fake count, just to
    /// avoid panic now".
    pub fn derive_stats(&mut self, self_schema: &Schema, reloads: &[bool]) -> (StatsInfo, bool) {
        let reload = reloads.len() == 1 && reloads[0];
        if !reload {
            if let Some(existing) = self.base.base.stats_info() {
                return (existing.clone(), false);
            }
        }
        let profile = get_fake_stats(self_schema);
        self.base.base.set_stats(Some(profile.clone()));
        (profile, true)
    }

    /// This operator's own fields with NO children; see
    /// [`crate::logical::LogicalPlan::clone_shallow`].
    #[must_use]
    pub fn clone_shallow(&self) -> Self {
        Self {
            base: self.base.shell(),
            job_number: self.job_number,
        }
    }
}
