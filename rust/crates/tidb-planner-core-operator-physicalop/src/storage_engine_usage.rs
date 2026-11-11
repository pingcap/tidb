//! Go `physicalop/storage_engine_usage.go`.

use tidb_kv::store_type::StoreType;

use crate::PhysicalPlan;

/// Go `StorageEngineUsage`: whether the physical plan tree reads from TiKV
/// and/or TiFlash. Only storage boundaries are inspected: every operator
/// below a reader runs on that reader's engine. TiDB-side operators count as
/// neither engine.
#[must_use]
pub fn storage_engine_usage(plan: &PhysicalPlan) -> (bool, bool) {
    match plan {
        PhysicalPlan::TableReader(reader) => {
            return (
                reader.store_type == StoreType::TiKv,
                reader.store_type == StoreType::TiFlash,
            );
        }
        PhysicalPlan::IndexReader(_)
        | PhysicalPlan::IndexLookUpReader(_)
        | PhysicalPlan::IndexMergeReader(_)
        | PhysicalPlan::PointGet(_)
        | PhysicalPlan::BatchPointGet(_) => return (true, false),
        PhysicalPlan::CTE(cte) => {
            let (seed_tikv, seed_tiflash) = storage_engine_usage(&cte.seed_plan);
            let (recur_tikv, recur_tiflash) = cte
                .recursive_plan
                .as_deref()
                .map_or((false, false), storage_engine_usage);
            return (seed_tikv || recur_tikv, seed_tiflash || recur_tiflash);
        }
        _ => {}
    }
    let (mut has_tikv, mut has_tiflash) = (false, false);
    for child in plan.children() {
        let (child_tikv, child_tiflash) = storage_engine_usage(child);
        has_tikv |= child_tikv;
        has_tiflash |= child_tiflash;
        if has_tikv && has_tiflash {
            break;
        }
    }
    (has_tikv, has_tiflash)
}
