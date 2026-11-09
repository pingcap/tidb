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

//! Go `pkg/planner/core/operator/physicalop/fragment.go`: an MPP plan cut
//! into fragments at its exchanges. Go's fragments point into their cloned
//! plan trees and fill tasks in place; a `Fragment` here owns its sink tree,
//! and the generator addresses the receivers and lower sinks it fills by
//! plan id and fragment index.

use crate::{ExchangeType, PhysicalPlan};
use tidb_planner_core_base::PlanError;

/// Go `Fragment`: one piece of the pushed-down plan, cut from the whole by
/// network communication (shuffling, broadcasting, passing through).
#[derive(Clone, Debug, Default)]
pub struct Fragment {
    /// Go `TableScan`: the plan id of the fragment's table scan.
    pub table_scan: Option<i32>,
    /// Go `ExchangeReceivers`: the plan ids of the fragment's receivers.
    pub exchange_receivers: Vec<i32>,
    /// Go `Sink`: the data exporter, which owns the fragment's tree.
    pub sink: Option<PhysicalPlan>,
    /// Go `IsRoot`.
    pub is_root: bool,
    /// Go `singleton`: the fragment runs on a single node.
    singleton: bool,
}

impl Fragment {
    /// Go `Fragment.init`: collects the fragment's table scan and receivers
    /// below `p`, stopping at each receiver.
    pub(crate) fn init(&mut self, p: &PhysicalPlan) -> Result<(), PlanError> {
        match p {
            PhysicalPlan::TableScan(_) => {
                if self.table_scan.is_some() {
                    return Err(PlanError::internal("one task contains at most one table scan"));
                }
                self.table_scan = Some(p.id());
            }
            PhysicalPlan::ExchangeReceiver(_) => {
                // TODO: after we support partial merge, we should check whether all the target exchangeReceiver is same.
                self.singleton = self.singleton
                    || matches!(
                        p.children().first(),
                        Some(PhysicalPlan::ExchangeSender(sender)) if sender.exchange_type == ExchangeType::PassThrough
                    );
                self.exchange_receivers.push(p.id());
            }
            PhysicalPlan::UnionAll(_) => return Err(PlanError::internal("unexpected union all detected")),
            _ => {
                for ch in p.children() {
                    self.init(ch)?;
                }
            }
        }
        Ok(())
    }

    /// Go `singleton`.
    #[must_use]
    pub const fn singleton(&self) -> bool {
        self.singleton
    }
}

/// Go `untwistPlanAndRemoveUnionAll`: removes every union-all by copying
/// the plans above it, so each route from the root sink to a leaf (a table
/// scan or a receiver) becomes its own tree, and so its own fragment. For
/// `select c1 from t union all select c1 from s` the forest is
/// `ExchangeSender -> Projection -> TableScan(t)` and
/// `ExchangeSender -> Projection -> TableScan(s)`.
///
/// # Errors
/// A node with no route to a leaf, or a CTE producer, which this port's MPP
/// plans do not carry yet.
pub fn untwist_plan_and_remove_union_all<'a>(
    stack: &mut Vec<&'a PhysicalPlan>,
    forest: &mut Vec<PhysicalPlan>,
) -> Result<(), PlanError> {
    let cur = *stack.last().expect("the stack holds the route");
    match cur {
        // This should be the leaf node.
        PhysicalPlan::TableScan(_) | PhysicalPlan::ExchangeReceiver(_) => {
            // Go clones the route top-down, each node a copy whose single
            // child (a join's probe side) is the next one; built bottom-up
            // here. A leaf and a join keep their other children's copies.
            let mut p = cur.clone();
            for node in stack[..stack.len() - 1].iter().rev() {
                if matches!(node, PhysicalPlan::UnionAll(_) | PhysicalPlan::Sequence(_)) {
                    continue;
                }
                p = if let Some(inner_child_idx) = node.inner_child_idx().filter(|_| matches!(node, PhysicalPlan::HashJoin(_))) {
                    let mut join = (*node).clone();
                    let mut children = join.base_mut().take_children();
                    children[1 - inner_child_idx] = p;
                    join.base_mut().set_children(children);
                    join
                } else {
                    let mut ch = node.clone_shallow();
                    ch.base_mut().set_children(vec![p]);
                    ch
                };
            }
            if !matches!(p, PhysicalPlan::ExchangeSender(_)) {
                return Err(PlanError::internal(format!("unexpected sink plan {}", p.explain_id(false))));
            }
            forest.push(p);
        }
        PhysicalPlan::HashJoin(join) => {
            stack.push(&cur.children()[1 - join.inner_child_idx]);
            let result = untwist_plan_and_remove_union_all(stack, forest);
            stack.pop();
            return result;
        }
        PhysicalPlan::UnionAll(_) => {
            for ch in cur.children() {
                stack.push(ch);
                let result = untwist_plan_and_remove_union_all(stack, forest);
                stack.pop();
                result?;
            }
        }
        PhysicalPlan::Sequence(_) | PhysicalPlan::CTE(_) => {
            // Go's CTE producers and readers (`CTEGroups`, `PhysicalCTESink`,
            // `PhysicalCTESource`) come with the MPP CTE operators.
            return Err(PlanError::internal(format!(
                "MPP CTE {} is not supported by this node's fragments yet",
                cur.explain_id(false)
            )));
        }
        _ => {
            let [ch] = cur.children() else {
                return Err(PlanError::internal(format!("unexpected plan {}", cur.explain_id(false))));
            };
            stack.push(ch);
            let result = untwist_plan_and_remove_union_all(stack, forest);
            stack.pop();
            return result;
        }
    }
    Ok(())
}

/// Go `mppTaskGenerator.buildFragments`: the fragments of a sink's routes.
///
/// # Errors
/// As [`untwist_plan_and_remove_union_all`] and [`Fragment::init`].
pub fn build_fragments(s: &PhysicalPlan) -> Result<Vec<Fragment>, PlanError> {
    let mut forest = Vec::with_capacity(1);
    untwist_plan_and_remove_union_all(&mut vec![s], &mut forest)?;
    let mut fragments = Vec::with_capacity(forest.len());
    for s in forest {
        let mut f = Fragment::default();
        f.init(&s)?;
        f.sink = Some(s);
        fragments.push(f);
    }
    Ok(fragments)
}

#[cfg(test)]
mod tests {
    use super::Fragment;
    use crate::{BasePhysicalPlan, ExchangeType, PhysicalExchangeReceiver, PhysicalExchangeSender, PhysicalHashJoin, PhysicalPlan};

    fn receiver(id: i32, exchange_type: ExchangeType) -> PhysicalPlan {
        let mut sender = PhysicalExchangeSender { exchange_type, ..PhysicalExchangeSender::default() };
        sender.base = BasePhysicalPlan::with_id(id + 100, "ExchangeSender", 0);
        let mut base = BasePhysicalPlan::with_id(id, "ExchangeReceiver", 0);
        base.set_children(vec![PhysicalPlan::ExchangeSender(sender)]);
        PhysicalPlan::ExchangeReceiver(PhysicalExchangeReceiver { base, ..PhysicalExchangeReceiver::default() })
    }

    fn join(left: PhysicalPlan, right: PhysicalPlan) -> PhysicalPlan {
        let mut base = BasePhysicalPlan::with_id(1, "HashJoin", 0);
        base.set_children(vec![left, right]);
        PhysicalPlan::HashJoin(PhysicalHashJoin { base, ..PhysicalHashJoin::default() })
    }

    fn node(id: i32, tp: &str, children: Vec<PhysicalPlan>) -> crate::BasePhysicalPlan {
        let mut base = BasePhysicalPlan::with_id(id, tp, 0);
        base.set_children(children);
        base
    }

    fn scan(id: i32) -> PhysicalPlan {
        PhysicalPlan::TableScan(crate::PhysicalTableScan { base: node(id, "TableScan", vec![]), ..Default::default() })
    }

    fn ids(plan: &PhysicalPlan) -> String {
        let children: Vec<String> = plan.children().iter().map(ids).collect();
        if children.is_empty() {
            plan.explain_id(false)
        } else {
            format!("{}({})", plan.explain_id(false), children.join(","))
        }
    }

    /// Go `untwistPlanAndRemoveUnionAll`'s documented example: each route
    /// through the union becomes its own sink tree.
    #[test]
    fn untwist_copies_the_plans_above_a_union_all() {
        let union = PhysicalPlan::UnionAll(crate::PhysicalUnionAll { base: node(3, "Union", vec![scan(4), scan(5)]), mpp: true });
        let projection =
            PhysicalPlan::Projection(crate::PhysicalProjection { base: node(2, "Projection", vec![union]), ..Default::default() });
        let sender = PhysicalPlan::ExchangeSender(PhysicalExchangeSender {
            base: node(1, "ExchangeSender", vec![projection]),
            ..Default::default()
        });
        let fragments = super::build_fragments(&sender).unwrap();
        let shapes: Vec<String> = fragments.iter().map(|f| ids(f.sink.as_ref().unwrap())).collect();
        assert_eq!(
            shapes,
            ["ExchangeSender_1(Projection_2(TableScan_4))", "ExchangeSender_1(Projection_2(TableScan_5))"]
        );
        assert_eq!(fragments[0].table_scan, Some(4));
        assert_eq!(fragments[1].table_scan, Some(5));
    }

    /// A hash join's routes go through its probe side; its build side is
    /// copied whole into each.
    #[test]
    fn untwist_follows_a_hash_join_probe_side() {
        let union = PhysicalPlan::UnionAll(crate::PhysicalUnionAll { base: node(4, "Union", vec![scan(5), scan(6)]), mpp: true });
        let join = PhysicalPlan::HashJoin(PhysicalHashJoin {
            base: node(2, "HashJoin", vec![receiver(3, ExchangeType::Broadcast), union]),
            inner_child_idx: 0,
            ..Default::default()
        });
        let sender = PhysicalPlan::ExchangeSender(PhysicalExchangeSender {
            base: node(1, "ExchangeSender", vec![join]),
            ..Default::default()
        });
        let fragments = super::build_fragments(&sender).unwrap();
        let shapes: Vec<String> = fragments.iter().map(|f| ids(f.sink.as_ref().unwrap())).collect();
        assert_eq!(
            shapes,
            [
                "ExchangeSender_1(HashJoin_2(ExchangeReceiver_3(ExchangeSender_103),TableScan_5))",
                "ExchangeSender_1(HashJoin_2(ExchangeReceiver_3(ExchangeSender_103),TableScan_6))",
            ]
        );
        assert_eq!(fragments[0].exchange_receivers, [3]);
        assert!(!fragments[0].singleton());
    }

    /// Go `TestFragmentInitSingleton`.
    #[test]
    fn test_fragment_init_singleton() {
        let r1 = || receiver(2, ExchangeType::PassThrough);
        let r2 = || receiver(3, ExchangeType::Broadcast);

        for (left, right, singleton) in [(r1(), r1(), true), (r1(), r2(), true), (r2(), r1(), true), (r2(), r2(), false)] {
            let mut f = Fragment::default();
            f.init(&join(left, right)).unwrap();
            assert_eq!(f.singleton(), singleton);
        }
    }
}
