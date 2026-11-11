//! Go `physicalop/single_scan_index_join.go`.

use tidb_kv::store_type::StoreType;

use crate::PhysicalPlan;

/// Go `HasSingleScanIndexJoin`: whether the physical plan tree contains an
/// index join (any variant) whose inner side is a single scan: a probe on the
/// handle, or on an index that covers everything the join needs. An inner
/// side that double-reads (IndexLookUp, IndexMerge) is not reported.
#[must_use]
pub fn has_single_scan_index_join(plan: &PhysicalPlan) -> bool {
    let inner_is_single_scan = match plan {
        PhysicalPlan::IndexJoin(join) => plan
            .children()
            .get(join.inner_child_idx)
            .is_some_and(is_single_scan_read),
        PhysicalPlan::CTE(cte) => {
            has_single_scan_index_join(&cte.seed_plan)
                || cte
                    .recursive_plan
                    .as_deref()
                    .is_some_and(has_single_scan_index_join)
        }
        _ => false,
    };
    inner_is_single_scan || plan.children().iter().any(has_single_scan_index_join)
}

/// Go `isSingleScanRead`: whether an index join's inner side reads its rows
/// in a single scan. The reader can sit under single-child wrapper operators
/// (Selection, Projection, UnionScan).
fn is_single_scan_read(mut plan: &PhysicalPlan) -> bool {
    loop {
        match plan {
            PhysicalPlan::IndexReader(_) => return true,
            PhysicalPlan::TableReader(reader) => return reader.store_type == StoreType::TiKv,
            PhysicalPlan::IndexLookUpReader(_) | PhysicalPlan::IndexMergeReader(_) => return false,
            _ => {}
        }
        let [child] = plan.children() else {
            return false;
        };
        plan = child;
    }
}
