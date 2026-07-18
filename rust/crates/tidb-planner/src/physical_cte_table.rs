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

//! PhysicalCTETable's dependency-closed identity and task eligibility from
//! `pkg/planner/core/operator/physicalop/physical_cte_table.go`.
//!
//! The Go operator carries schema, statistics, plan context, and runtime task
//! objects.  This leaf keeps the source-visible CTE storage identity and the
//! two pure task rejection gates; those external planner/runtime objects stay
//! outside this boundary.

/// Minimal physical CTE table identity.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PhysicalCteTable {
    id_for_storage: i32,
}

impl PhysicalCteTable {
    /// Creates a physical CTE table with the source storage identity.
    #[must_use]
    pub const fn new(id_for_storage: i32) -> Self {
        Self { id_for_storage }
    }

    /// Returns the source CTE storage identity.
    #[must_use]
    pub const fn id_for_storage(self) -> i32 {
        self.id_for_storage
    }

    /// Returns the source `ExplainInfo` text.
    #[must_use]
    pub fn explain_info(self) -> String {
        format!("Scan on CTE_{}", self.id_for_storage)
    }

    /// Builds the source root-task candidate when both rejection gates pass.
    ///
    /// `findBestTask4LogicalCTETable` rejects index-join properties and any
    /// requested sort items before constructing the root physical plan.
    #[must_use]
    pub const fn find_best_task(
        id_for_storage: i32,
        has_index_join_prop: bool,
        has_sort_items: bool,
    ) -> Option<Self> {
        if has_index_join_prop || has_sort_items {
            None
        } else {
            Some(Self::new(id_for_storage))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::PhysicalCteTable;

    #[test]
    fn explain_info_preserves_signed_storage_identity() {
        assert_eq!(PhysicalCteTable::new(0).explain_info(), "Scan on CTE_0");
        assert_eq!(PhysicalCteTable::new(7).explain_info(), "Scan on CTE_7");
        assert_eq!(PhysicalCteTable::new(-1).explain_info(), "Scan on CTE_-1");
    }

    #[test]
    fn find_best_task_rejects_each_source_gate() {
        assert!(PhysicalCteTable::find_best_task(3, true, false).is_none());
        assert!(PhysicalCteTable::find_best_task(3, false, true).is_none());
        assert!(PhysicalCteTable::find_best_task(3, true, true).is_none());
    }

    #[test]
    fn find_best_task_keeps_id_when_no_gate_is_set() {
        let task = PhysicalCteTable::find_best_task(-4, false, false).unwrap();
        assert_eq!(task.id_for_storage(), -4);
        assert_eq!(task.explain_info(), "Scan on CTE_-4");
    }
}
