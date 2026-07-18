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

//! Physical LOCK planning metadata from
//! `pkg/planner/core/operator/physicalop/physical_lock.go`.
//!
//! The Go operator carries AST lock metadata, table-handle maps, physical
//! table columns, context, statistics, and task objects. This leaf keeps the
//! dependency-closed plan kind, zero query-block offset, and explain text, and
//! models the source TiFlash rejection before plan creation. Catalog/schema
//! resolution, handle cloning, statistics scaling, warning publication, task
//! wiring, and lock execution remain external boundaries.

/// The source plan-codec type assigned by `PhysicalLock.Init`.
pub const PLAN_TYPE: &str = "Lock";

/// `PhysicalLock.Init` always assigns the root query-block offset.
pub const QUERY_BLOCK_OFFSET: i32 = 0;

/// The dependency-closed lock metadata retained by a physical plan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalLockPlan {
    lock_type: String,
    wait_sec: u64,
}

impl PhysicalLockPlan {
    /// Initializes source-shaped lock metadata.
    #[must_use]
    pub fn init(lock_type: impl Into<String>, wait_sec: u64) -> Self {
        Self {
            lock_type: lock_type.into(),
            wait_sec,
        }
    }

    /// Returns the source plan-codec type.
    #[must_use]
    pub const fn plan_type(&self) -> &'static str {
        PLAN_TYPE
    }

    /// Returns the fixed query-block offset assigned by `Init`.
    #[must_use]
    pub const fn query_block_offset(&self) -> i32 {
        QUERY_BLOCK_OFFSET
    }

    /// Returns the caller-provided rendering of `SelectLockInfo.LockType`.
    #[must_use]
    pub fn lock_type(&self) -> &str {
        &self.lock_type
    }

    /// Returns `SelectLockInfo.WaitSec` without narrowing its uint64 value.
    #[must_use]
    pub const fn wait_sec(&self) -> u64 {
        self.wait_sec
    }

    /// Returns the source `ExplainInfo` shape: lock type followed by wait sec.
    #[must_use]
    pub fn explain_info(&self) -> String {
        format!("{} {}", self.lock_type, self.wait_sec)
    }
}

/// Outcome of `ExhaustPhysicalPlans4LogicalLock`'s dependency-closed gate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LockExhaustion {
    /// TiFlash/MPP properties reject Lock; Go returns handled=true/no plans.
    UnsupportedFlash,
    /// The source emits one physical Lock plan.
    Planned(PhysicalLockPlan),
}

/// Applies the source TiFlash rejection before constructing a Lock plan.
#[must_use]
pub fn exhaust_physical_lock(
    is_flash_prop: bool,
    lock_type: impl Into<String>,
    wait_sec: u64,
) -> LockExhaustion {
    if is_flash_prop {
        LockExhaustion::UnsupportedFlash
    } else {
        LockExhaustion::Planned(PhysicalLockPlan::init(lock_type, wait_sec))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        exhaust_physical_lock, LockExhaustion, PhysicalLockPlan, PLAN_TYPE, QUERY_BLOCK_OFFSET,
    };

    #[test]
    fn flash_properties_reject_before_lock_plan_creation() {
        assert_eq!(
            exhaust_physical_lock(true, "for update", 0),
            LockExhaustion::UnsupportedFlash
        );
    }

    #[test]
    fn admitted_lock_preserves_plan_kind_and_wait_seconds() {
        let outcome = exhaust_physical_lock(false, "lock in share mode", 17);
        let LockExhaustion::Planned(plan) = outcome else {
            unreachable!();
        };
        assert_eq!(plan.plan_type(), PLAN_TYPE);
        assert_eq!(plan.plan_type(), "Lock");
        assert_eq!(plan.query_block_offset(), QUERY_BLOCK_OFFSET);
        assert_eq!(plan.query_block_offset(), 0);
        assert_eq!(plan.lock_type(), "lock in share mode");
        assert_eq!(plan.wait_sec(), 17);
    }

    #[test]
    fn explain_info_matches_select_lock_plan_tree_text() {
        let plan = PhysicalLockPlan::init("for update", 0);
        assert_eq!(plan.explain_info(), "for update 0");
        assert_eq!(
            PhysicalLockPlan::init("for update", u64::MAX).explain_info(),
            format!("for update {}", u64::MAX)
        );
    }

    #[test]
    fn lock_type_is_opaque_and_empty_text_is_preserved() {
        let plan = PhysicalLockPlan::init("", 3);
        assert_eq!(plan.lock_type(), "");
        assert_eq!(plan.explain_info(), " 3");
    }
}
