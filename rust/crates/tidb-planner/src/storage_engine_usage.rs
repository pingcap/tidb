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

//! Physical-plan storage boundaries used by Go's alternative engine rounds.
//!
//! Go sources: `pkg/planner/core/operator/physicalop/storage_engine_usage.go`
//! and `single_scan_index_join.go`. The helpers inspect the shared physical
//! tree without introducing a second plan representation. The optimizer round
//! driver remains an explicit integration boundary; these functions provide
//! the dependency-closed tree semantics needed by that driver.

use crate::physical::{PhysicalCTE, PhysicalIndexJoin, PhysicalPlan};
use crate::physical_table_reader::StoreType;

/// Whether a physical plan reads from TiKV and/or TiFlash.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StorageEngineUsage {
    /// At least one TiKV reader occurs in the plan.
    pub has_tikv: bool,
    /// At least one TiFlash reader occurs in the plan.
    pub has_tiflash: bool,
}

impl StorageEngineUsage {
    const TIKV: Self = Self {
        has_tikv: true,
        has_tiflash: false,
    };
    const TIFLASH: Self = Self {
        has_tikv: false,
        has_tiflash: true,
    };

    fn merge(self, other: Self) -> Self {
        Self {
            has_tikv: self.has_tikv || other.has_tikv,
            has_tiflash: self.has_tiflash || other.has_tiflash,
        }
    }
}

/// Reports which storage engines are read by a physical plan tree.
///
/// Reader nodes are storage boundaries: operators below a reader execute on
/// that reader's engine and are intentionally not descended into. TiDB-side
/// readers and operators count as neither TiKV nor TiFlash. CTE seed and
/// recursive plans are separate Go fields, so both are traversed explicitly.
#[must_use]
pub fn storage_engine_usage(plan: Option<&PhysicalPlan>) -> StorageEngineUsage {
    let Some(plan) = plan else {
        return StorageEngineUsage::default();
    };
    match plan {
        PhysicalPlan::TableReader(reader) => match reader.store_type {
            StoreType::TiKv => StorageEngineUsage::TIKV,
            StoreType::TiFlash => StorageEngineUsage::TIFLASH,
            StoreType::Unknown(_) => StorageEngineUsage::default(),
        },
        PhysicalPlan::IndexReader(_)
        | PhysicalPlan::IndexLookUpReader(_)
        | PhysicalPlan::IndexMergeReader(_)
        | PhysicalPlan::PointGet(_)
        | PhysicalPlan::BatchPointGet(_) => StorageEngineUsage::TIKV,
        PhysicalPlan::CTE(cte) => storage_engine_usage_cte(cte),
        _ => plan
            .children()
            .iter()
            .map(|child| storage_engine_usage(Some(child)))
            .fold(StorageEngineUsage::default(), StorageEngineUsage::merge),
    }
}

fn storage_engine_usage_cte(cte: &PhysicalCTE) -> StorageEngineUsage {
    let recursive = cte
        .recursive_plan
        .as_deref()
        .map(|plan| storage_engine_usage(Some(plan)))
        .unwrap_or_default();
    storage_engine_usage(Some(&cte.seed_plan)).merge(recursive)
}

/// Reports whether a plan contains an index join whose inner side is a single
/// TiKV table/handle probe or covering index scan. Double-read index lookup and
/// index-merge readers are deliberately excluded.
#[must_use]
pub fn has_single_scan_index_join(plan: Option<&PhysicalPlan>) -> bool {
    let Some(plan) = plan else {
        return false;
    };
    if let PhysicalPlan::IndexJoin(join) = plan {
        if let Some(inner) = join.base.children().get(join.inner_child_idx) {
            if is_single_scan_read(inner) {
                return true;
            }
        }
    }
    if let PhysicalPlan::CTE(cte) = plan {
        if has_single_scan_index_join(Some(&cte.seed_plan))
            || cte
                .recursive_plan
                .as_deref()
                .is_some_and(|recur| has_single_scan_index_join(Some(recur)))
        {
            return true;
        }
    }
    plan.children()
        .iter()
        .any(|child| has_single_scan_index_join(Some(child)))
}

fn is_single_scan_read(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::IndexReader(_) => true,
        PhysicalPlan::TableReader(reader) => reader.store_type == StoreType::TiKv,
        PhysicalPlan::IndexLookUpReader(_) | PhysicalPlan::IndexMergeReader(_) => false,
        _ => {
            let children = plan.children();
            children.len() == 1 && is_single_scan_read(&children[0])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::{
        BasePhysicalPlan, PhysicalHashJoin, PhysicalIndexLookUpReader, PhysicalIndexReader,
        PhysicalSelection, PhysicalTableReader,
    };

    fn table_reader(store_type: StoreType) -> PhysicalPlan {
        PhysicalPlan::TableReader(PhysicalTableReader {
            store_type,
            ..PhysicalTableReader::default()
        })
    }

    fn join(children: Vec<PhysicalPlan>) -> PhysicalPlan {
        let mut base = BasePhysicalPlan::default();
        base.set_children(children);
        PhysicalPlan::HashJoin(PhysicalHashJoin {
            base,
            ..PhysicalHashJoin::default()
        })
    }

    #[test]
    fn storage_engine_usage_stops_at_reader_boundaries() {
        assert_eq!(
            storage_engine_usage(Some(&table_reader(StoreType::TiKv))),
            StorageEngineUsage::TIKV
        );
        assert_eq!(
            storage_engine_usage(Some(&table_reader(StoreType::TiFlash))),
            StorageEngineUsage::TIFLASH
        );
        let mixed = join(vec![
            table_reader(StoreType::TiKv),
            table_reader(StoreType::TiFlash),
        ]);
        assert_eq!(
            storage_engine_usage(Some(&mixed)),
            StorageEngineUsage {
                has_tikv: true,
                has_tiflash: true,
            }
        );
    }

    #[test]
    fn single_scan_index_join_requires_inner_single_reader() {
        let mut base = BasePhysicalPlan::default();
        base.set_children(vec![
            table_reader(StoreType::TiFlash),
            table_reader(StoreType::TiKv),
        ]);
        let mut index_join = PhysicalIndexJoin {
            base,
            inner_child_idx: 1,
            ..PhysicalIndexJoin::default()
        };
        let plan = PhysicalPlan::IndexJoin(index_join.clone());
        assert!(has_single_scan_index_join(Some(&plan)));

        index_join.base.set_children(vec![
            table_reader(StoreType::TiFlash),
            PhysicalPlan::IndexLookUpReader(PhysicalIndexLookUpReader::default()),
        ]);
        let double_read = PhysicalPlan::IndexJoin(index_join);
        assert!(!has_single_scan_index_join(Some(&double_read)));

        let mut wrapped_base = BasePhysicalPlan::default();
        wrapped_base.set_children(vec![table_reader(StoreType::TiKv)]);
        let wrapped = PhysicalPlan::Selection(PhysicalSelection {
            base: wrapped_base,
            ..PhysicalSelection::default()
        });
        let mut wrapped_join_base = BasePhysicalPlan::default();
        wrapped_join_base.set_children(vec![table_reader(StoreType::TiFlash), wrapped]);
        let wrapped_join = PhysicalPlan::IndexJoin(PhysicalIndexJoin {
            base: wrapped_join_base,
            inner_child_idx: 1,
            ..PhysicalIndexJoin::default()
        });
        assert!(has_single_scan_index_join(Some(&wrapped_join)));
        assert!(!has_single_scan_index_join(None));
    }
}
