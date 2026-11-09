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
