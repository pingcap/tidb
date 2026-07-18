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

//! Physical SHOW planning metadata from
//! `pkg/planner/core/operator/physicalop/physical_show.go`.
//!
//! The Go operators carry schema, ShowContents, extractor, context, property,
//! and task objects. This leaf preserves plan kind, pseudo row-count
//! initialization, DDL job-number identity, and the shared index-join/sort
//! admission gates; catalog/schema resolution, task wiring, SHOW execution,
//! and extractor behavior remain external boundaries.

/// Physical SHOW operator kind.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ShowPlanKind {
    /// Regular PhysicalShow.
    Show,
    /// PhysicalShowDDLJobs.
    DdlJobs,
}

/// Minimal initialized physical SHOW plan metadata.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PhysicalShowPlan {
    kind: ShowPlanKind,
    row_count: u64,
    job_number: Option<i64>,
}

impl PhysicalShowPlan {
    /// The source pseudo statistics row count for both SHOW operators.
    pub const PSEUDO_ROW_COUNT: u64 = 1;

    /// Initializes a regular PhysicalShow plan.
    #[must_use]
    pub const fn init_show() -> Self {
        Self {
            kind: ShowPlanKind::Show,
            row_count: Self::PSEUDO_ROW_COUNT,
            job_number: None,
        }
    }

    /// Initializes a PhysicalShowDDLJobs plan with its job-number identity.
    #[must_use]
    pub const fn init_ddl_jobs(job_number: i64) -> Self {
        Self {
            kind: ShowPlanKind::DdlJobs,
            row_count: Self::PSEUDO_ROW_COUNT,
            job_number: Some(job_number),
        }
    }

    /// Returns the physical SHOW operator kind.
    #[must_use]
    pub const fn kind(self) -> ShowPlanKind {
        self.kind
    }

    /// Returns the pseudo row count assigned by Init.
    #[must_use]
    pub const fn row_count(self) -> u64 {
        self.row_count
    }

    /// Returns the DDL job-number identity, if this is a DDL-jobs plan.
    #[must_use]
    pub const fn job_number(self) -> Option<i64> {
        self.job_number
    }

    /// Applies the shared `findBestTask` property gates for either SHOW kind.
    #[must_use]
    pub const fn find_best_task(
        kind: ShowPlanKind,
        has_index_join_prop: bool,
        has_sort_items: bool,
        job_number: i64,
    ) -> Option<Self> {
        if has_index_join_prop || has_sort_items {
            return None;
        }
        Some(match kind {
            ShowPlanKind::Show => Self::init_show(),
            ShowPlanKind::DdlJobs => Self::init_ddl_jobs(job_number),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{PhysicalShowPlan, ShowPlanKind};

    #[test]
    fn both_show_operators_use_pseudo_one_row_stats() {
        let show = PhysicalShowPlan::init_show();
        let ddl = PhysicalShowPlan::init_ddl_jobs(5);
        assert_eq!(show.kind(), ShowPlanKind::Show);
        assert_eq!(ddl.kind(), ShowPlanKind::DdlJobs);
        assert_eq!(show.row_count(), 1);
        assert_eq!(ddl.row_count(), PhysicalShowPlan::PSEUDO_ROW_COUNT);
    }

    #[test]
    fn ddl_job_number_is_preserved_only_for_ddl_show() {
        assert_eq!(PhysicalShowPlan::init_ddl_jobs(-9).job_number(), Some(-9));
        assert_eq!(PhysicalShowPlan::init_show().job_number(), None);
    }

    #[test]
    fn index_join_and_sort_properties_reject_both_show_kinds() {
        for kind in [ShowPlanKind::Show, ShowPlanKind::DdlJobs] {
            assert!(PhysicalShowPlan::find_best_task(kind, true, false, 1).is_none());
            assert!(PhysicalShowPlan::find_best_task(kind, false, true, 1).is_none());
            assert!(PhysicalShowPlan::find_best_task(kind, true, true, 1).is_none());
        }
    }

    #[test]
    fn admitted_properties_emit_the_requested_show_kind() {
        assert_eq!(
            PhysicalShowPlan::find_best_task(ShowPlanKind::Show, false, false, 8),
            Some(PhysicalShowPlan::init_show())
        );
        assert_eq!(
            PhysicalShowPlan::find_best_task(ShowPlanKind::DdlJobs, false, false, 8),
            Some(PhysicalShowPlan::init_ddl_jobs(8))
        );
    }
}
