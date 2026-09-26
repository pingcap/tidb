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

//! Go `physicalop/fragment.go`: MPP fragments cut at exchange receivers.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::physical::{ExchangeType, PhysicalPlan};
use crate::plan_base::PlanError;

/// The runtime identity needed while assigning MPP fragments to TiFlash
/// nodes. This is the native owner of Go `kv.MPPTask.Meta.GetAddress()`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MppTask {
    /// TiFlash address on which this task runs.
    pub address: String,
}

impl MppTask {
    /// Creates task metadata for one TiFlash address.
    #[must_use]
    pub fn at(address: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            address: address.into(),
        })
    }
}

/// A pushed-down plan segment bounded by network communication.
#[derive(Clone, Debug)]
pub struct Fragment {
    /// The fragment's MPP sink.
    pub sink: PhysicalPlan,
    /// Plan id of the fragment's table scan, when present.
    pub table_scan_id: Option<i32>,
    /// Plan ids of exchange receivers at the fragment boundary.
    pub exchange_receiver_ids: Vec<i32>,
    /// Plan ids of CTE reader placeholders in this fragment.
    pub cte_reader_ids: Vec<i32>,
    /// Whether this is one of the root fragments returned to TiDB.
    pub is_root: bool,
    singleton: bool,
}

impl Fragment {
    /// Builds Go's `Fragment` inventory from one sink.
    pub fn from_sink(sink: PhysicalPlan) -> Result<Self, PlanError> {
        let mut fragment = Self {
            sink,
            table_scan_id: None,
            exchange_receiver_ids: Vec::new(),
            cte_reader_ids: Vec::new(),
            is_root: false,
            singleton: false,
        };
        Self::init_plan(
            &fragment.sink,
            &mut fragment.table_scan_id,
            &mut fragment.exchange_receiver_ids,
            &mut fragment.cte_reader_ids,
            &mut fragment.singleton,
        )?;
        Ok(fragment)
    }

    fn init_plan(
        plan: &PhysicalPlan,
        table_scan_id: &mut Option<i32>,
        receivers: &mut Vec<i32>,
        cte_readers: &mut Vec<i32>,
        singleton: &mut bool,
    ) -> Result<(), PlanError> {
        match plan {
            PhysicalPlan::TableScan(_) => {
                if table_scan_id.replace(plan.id()).is_some() {
                    return Err(PlanError::internal(
                        "one task contains at most one table scan",
                    ));
                }
            }
            PhysicalPlan::ExchangeReceiver(receiver) => {
                let Some(PhysicalPlan::ExchangeSender(sender)) = receiver.base.children().first()
                else {
                    return Err(PlanError::internal(
                        "an exchange receiver must have one exchange sender child",
                    ));
                };
                *singleton |= sender.exchange_type == ExchangeType::PassThrough;
                receivers.push(plan.id());
            }
            PhysicalPlan::UnionAll(_) => {
                return Err(PlanError::internal("unexpected union all detected"));
            }
            PhysicalPlan::CTE(_) => cte_readers.push(plan.id()),
            _ => {
                for child in plan.children() {
                    Self::init_plan(child, table_scan_id, receivers, cte_readers, singleton)?;
                }
            }
        }
        Ok(())
    }

    /// Go `Fragment.singleton`.
    #[must_use]
    pub const fn is_singleton(&self) -> bool {
        self.singleton
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CteNodeKind {
    Sink,
    Source,
}

#[derive(Clone, Debug)]
struct CteNodeTasks {
    plan_id: i32,
    cte_id: i32,
    kind: CteNodeKind,
    addresses: Vec<String>,
}

fn sink_tasks(sink: &PhysicalPlan) -> Result<Vec<String>, PlanError> {
    let tasks = match sink {
        PhysicalPlan::ExchangeSender(sender) => &sender.tasks,
        PhysicalPlan::CTESink(sink) => &sink.tasks,
        _ => {
            return Err(PlanError::internal(format!(
                "unexpected sink plan {}",
                sink.tp()
            )));
        }
    };
    Ok(tasks.iter().map(|task| task.address.clone()).collect())
}

fn collect_cte_nodes(
    plan: &PhysicalPlan,
    fragment_addresses: &[String],
    nodes: &mut Vec<CteNodeTasks>,
) {
    match plan {
        PhysicalPlan::CTESink(sink) => nodes.push(CteNodeTasks {
            plan_id: plan.id(),
            cte_id: sink.id_for_storage,
            kind: CteNodeKind::Sink,
            addresses: sink.tasks.iter().map(|task| task.address.clone()).collect(),
        }),
        PhysicalPlan::CTESource(source) => {
            nodes.push(CteNodeTasks {
                plan_id: plan.id(),
                cte_id: source.id_for_storage,
                kind: CteNodeKind::Source,
                addresses: fragment_addresses.to_vec(),
            });
            return;
        }
        PhysicalPlan::ExchangeReceiver(_) | PhysicalPlan::TableScan(_) => return,
        _ => {}
    }
    for child in plan.children() {
        collect_cte_nodes(child, fragment_addresses, nodes);
    }
}

fn uniform_local_count(
    cte_id: i32,
    addresses: &[String],
    counts: &BTreeMap<String, u32>,
    count_name: &str,
) -> Result<u32, PlanError> {
    let mut values = addresses.iter().map(|address| counts[address]);
    let Some(first) = values.next() else {
        return Ok(0);
    };
    if values.any(|value| value != first) {
        return Err(PlanError::internal(format!(
            "MPP shared CTE {cte_id} has different local {count_name} counts in one fragment"
        )));
    }
    Ok(first)
}

fn set_cte_counts(plan: &mut PhysicalPlan, plan_id: i32, sink_num: u32, source_num: u32) {
    if plan.id() == plan_id {
        match plan {
            PhysicalPlan::CTESink(sink) => {
                sink.cte_sink_num = sink_num;
                sink.cte_source_num = source_num;
            }
            PhysicalPlan::CTESource(source) => {
                source.cte_sink_num = sink_num;
                source.cte_source_num = source_num;
            }
            _ => {}
        }
        return;
    }
    for child in plan.base_mut().children_mut() {
        set_cte_counts(child, plan_id, sink_num, source_num);
    }
}

/// Go `mppTaskGenerator.fillLocalCTECounts`: write task-local CTE producer
/// and consumer counts to every split CTE node.
pub fn fill_local_cte_counts(fragments: &mut [Fragment]) -> Result<(), PlanError> {
    let mut nodes = Vec::new();
    for fragment in fragments.iter() {
        let addresses = sink_tasks(&fragment.sink)?;
        collect_cte_nodes(&fragment.sink, &addresses, &mut nodes);
    }

    let mut sink_counts: BTreeMap<i32, BTreeMap<String, u32>> = BTreeMap::new();
    let mut source_counts: BTreeMap<i32, BTreeMap<String, u32>> = BTreeMap::new();
    for node in &nodes {
        let counts = match node.kind {
            CteNodeKind::Sink => sink_counts.entry(node.cte_id).or_default(),
            CteNodeKind::Source => source_counts.entry(node.cte_id).or_default(),
        };
        for address in &node.addresses {
            *counts.entry(address.clone()).or_default() += 1;
        }
    }

    for node in nodes {
        let empty = BTreeMap::new();
        let sink_num = uniform_local_count(
            node.cte_id,
            &node.addresses,
            sink_counts.get(&node.cte_id).unwrap_or(&empty),
            "sink",
        )?;
        let source_num = uniform_local_count(
            node.cte_id,
            &node.addresses,
            source_counts.get(&node.cte_id).unwrap_or(&empty),
            "source",
        )?;
        for fragment in fragments.iter_mut() {
            set_cte_counts(&mut fragment.sink, node.plan_id, sink_num, source_num);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::{
        BasePhysicalPlan, PhysicalCTESink, PhysicalCTESource, PhysicalExchangeReceiver,
        PhysicalExchangeSender, PhysicalHashJoin,
    };

    fn sender(id: i32, exchange_type: ExchangeType) -> PhysicalPlan {
        PhysicalPlan::ExchangeSender(PhysicalExchangeSender {
            base: BasePhysicalPlan::with_id(id, "ExchangeSender", 0),
            exchange_type,
            ..PhysicalExchangeSender::default()
        })
    }

    fn receiver(id: i32, exchange_type: ExchangeType) -> PhysicalPlan {
        let mut base = BasePhysicalPlan::with_id(id, "ExchangeReceiver", 0);
        base.set_children(vec![sender(id + 100, exchange_type)]);
        PhysicalPlan::ExchangeReceiver(PhysicalExchangeReceiver {
            base,
            ..PhysicalExchangeReceiver::default()
        })
    }

    fn join(left: PhysicalPlan, right: PhysicalPlan) -> PhysicalPlan {
        let mut base = BasePhysicalPlan::with_id(1, "HashJoin", 0);
        base.set_children(vec![left, right]);
        PhysicalPlan::HashJoin(PhysicalHashJoin {
            base,
            ..PhysicalHashJoin::default()
        })
    }

    #[test]
    fn fragment_init_singleton_matches_exchange_matrix() {
        for (left, right, expected) in [
            (ExchangeType::PassThrough, ExchangeType::PassThrough, true),
            (ExchangeType::PassThrough, ExchangeType::Broadcast, true),
            (ExchangeType::Broadcast, ExchangeType::PassThrough, true),
            (ExchangeType::Broadcast, ExchangeType::Broadcast, false),
        ] {
            let fragment = Fragment::from_sink(join(receiver(2, left), receiver(3, right)))
                .expect("valid fragment");
            assert_eq!(fragment.is_singleton(), expected);
        }
    }

    fn cte_sink(id: i32, address: &str) -> Fragment {
        Fragment::from_sink(PhysicalPlan::CTESink(PhysicalCTESink {
            base: BasePhysicalPlan::with_id(id, "PhysicalCTESink", 0),
            id_for_storage: 1,
            tasks: vec![MppTask::at(address)],
            ..PhysicalCTESink::default()
        }))
        .expect("valid CTE sink fragment")
    }

    fn cte_source(id: i32) -> Fragment {
        let mut source_base = BasePhysicalPlan::with_id(id, "PhysicalCTESource", 0);
        source_base.base.set_schema(None);
        let source = PhysicalPlan::CTESource(PhysicalCTESource {
            base: source_base,
            id_for_storage: 1,
            ..PhysicalCTESource::default()
        });
        let mut sender_base = BasePhysicalPlan::with_id(id + 100, "ExchangeSender", 0);
        sender_base.set_children(vec![source]);
        Fragment::from_sink(PhysicalPlan::ExchangeSender(PhysicalExchangeSender {
            base: sender_base,
            tasks: vec![MppTask::at("tiflash0"), MppTask::at("tiflash1")],
            ..PhysicalExchangeSender::default()
        }))
        .expect("valid CTE source fragment")
    }

    #[test]
    fn local_cte_counts_use_each_tiflash_address() {
        let mut fragments = vec![
            cte_sink(1, "tiflash0"),
            cte_sink(2, "tiflash1"),
            cte_source(3),
            cte_source(4),
        ];
        fill_local_cte_counts(&mut fragments).expect("uniform local counts");

        for fragment in &fragments[..2] {
            let PhysicalPlan::CTESink(sink) = &fragment.sink else {
                panic!("CTE sink")
            };
            assert_eq!((sink.cte_sink_num, sink.cte_source_num), (1, 2));
        }
        for fragment in &fragments[2..] {
            let PhysicalPlan::CTESource(source) = &fragment.sink.children()[0] else {
                panic!("CTE source")
            };
            assert_eq!((source.cte_sink_num, source.cte_source_num), (1, 2));
        }
    }
}
