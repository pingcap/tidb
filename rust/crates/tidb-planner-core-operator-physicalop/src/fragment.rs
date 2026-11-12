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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::{ExchangeType, MppTask, PhysicalPlan, PhysicalTableScan};
use tidb_planner_core_base::PlanError;
use tidb_txnkv::{KeyRange, MppBuildTasksRequest, MppQueryId, MppTaskLocation, MppVersion, PartitionIdAndRanges};

/// What Go's `mppTaskGeneratorTableReaderCacheHit` and
/// `mppTaskGeneratorTableReaderCacheMiss` failpoints replace.
#[cfg(any(test, feature = "test-util"))]
pub mod mock {
    use std::sync::{Arc, Mutex, PoisonError};

    type Call = Option<Arc<dyn Fn() + Send + Sync>>;

    static TABLE_READER_CACHE_HIT: Mutex<Call> = Mutex::new(None);
    static TABLE_READER_CACHE_MISS: Mutex<Call> = Mutex::new(None);

    /// Go `failpoint.EnableCall("mppTaskGeneratorTableReaderCacheHit", f)`:
    /// `f` runs when a table reader takes its tasks from the cache.
    pub fn set_mpp_task_generator_table_reader_cache_hit(call: Call) {
        *TABLE_READER_CACHE_HIT.lock().unwrap_or_else(PoisonError::into_inner) = call;
    }

    /// Go `failpoint.EnableCall("mppTaskGeneratorTableReaderCacheMiss", f)`:
    /// `f` runs when a table reader's tasks are built.
    pub fn set_mpp_task_generator_table_reader_cache_miss(call: Call) {
        *TABLE_READER_CACHE_MISS.lock().unwrap_or_else(PoisonError::into_inner) = call;
    }

    fn inject_call(call: &Mutex<Call>) {
        let call = call.lock().unwrap_or_else(PoisonError::into_inner).clone();
        if let Some(call) = call {
            call();
        }
    }

    pub(super) fn table_reader_cache_hit() {
        inject_call(&TABLE_READER_CACHE_HIT);
    }

    pub(super) fn table_reader_cache_miss() {
        inject_call(&TABLE_READER_CACHE_MISS);
    }
}

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

/// Go `mppAddr`: the location of a task placed by address only.
#[derive(Debug)]
pub struct MppAddr {
    addr: String,
}

impl MppTaskLocation for MppAddr {
    fn address(&self) -> &str {
        &self.addr
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// What Go's `mppTaskGenerator` reads off its `sessionctx.Context`.
pub trait MppTaskGeneratorContext {
    /// Go `SessionVars.ChooseMppVersion()`.
    fn choose_mpp_version(&self) -> MppVersion;
    /// Go `SessionVars.ConnectionID`.
    fn connection_id(&self) -> u64;
    /// Go `SessionVars.SessionAlias`.
    fn session_alias(&self) -> String;
    /// Go `AllocMPPTaskID`: the statement's next MPP task id.
    fn alloc_mpp_task_id(&self) -> i64;
    /// Go `StmtCtx.UseDynamicPartitionPrune()`.
    fn use_dynamic_partition_prune(&self) -> bool;
    /// Go `distsql.TableHandleRangesToKVRanges(ctx, []int64{tid},
    /// isCommonHandle, ranges).FirstPartitionRange()`.
    ///
    /// # Errors
    /// A range cannot be encoded.
    fn table_handle_ranges_to_kv_ranges(
        &self,
        tid: i64,
        is_common_handle: bool,
        ranges: &[tidb_ranger::types::Range],
    ) -> Result<Vec<KeyRange>, PlanError>;
    /// Go `partitionPruning` over the scan's `PlanPartInfo`: the physical
    /// ids of the partitions it reads (at least one).
    ///
    /// # Errors
    /// Pruning failed.
    fn partition_pruning(&self, ts: &PhysicalTableScan) -> Result<Vec<i64>, PlanError>;
    /// Go `GetMPPClient().ConstructMPPTasks(ctx, req, ttl, dispatchPolicy,
    /// tiflashReplicaRead, StmtCtx.AppendWarning)` with the session's
    /// dispatch policy and replica-read policy.
    ///
    /// # Errors
    /// The store could not place the tasks.
    fn construct_mpp_tasks(&self, req: &MppBuildTasksRequest) -> Result<Vec<Arc<dyn MppTaskLocation>>, PlanError>;
    /// Go `PhysicalTableScan.ResolveCorrelatedColumns`: the scan's ranges
    /// rebuilt from its access conditions with the correlated columns'
    /// current values.
    ///
    /// # Errors
    /// The ranges could not be rebuilt.
    fn resolve_correlated_columns(&self, ts: &PhysicalTableScan) -> Result<Vec<tidb_ranger::types::Range>, PlanError>;
}

/// Go `tasksAndFrags`: a sink's tasks and the indices of its fragments.
#[derive(Clone)]
struct TasksAndFrags {
    tasks: Vec<MppTask>,
    frags: Vec<usize>,
}

/// Go `mppTaskGenerator`.
pub struct MppTaskGenerator<'a> {
    ctx: &'a dyn MppTaskGeneratorContext,
    start_ts: u64,
    gather_id: u64,
    mpp_query_id: MppQueryId,
    frags: Vec<Fragment>,
    cache: HashMap<i32, TasksAndFrags>,
    /// Go `KVRanges`: for an `MPPGather` under `UnionScan`, the ranges to
    /// scan the mem buffer with.
    kv_ranges: Vec<KeyRange>,
    node_info: BTreeMap<String, bool>,
    table_reader_cache: HashMap<String, Vec<Arc<dyn MppTaskLocation>>>,
}

/// Go `GenerateRootMPPTasks`: every fragment of the plan under `sender`
/// (lower fragments first) with their tasks; the root fragments send to the
/// TiDB-side task. Also the key ranges the fragments read and the TiFlash
/// nodes they run on.
///
/// # Errors
/// A fragment cannot be built or placed, or the plan reads no key range.
pub fn generate_root_mpp_tasks(
    ctx: &dyn MppTaskGeneratorContext,
    start_ts: u64,
    mpp_gather_id: u64,
    mpp_query_id: MppQueryId,
    sender: &PhysicalPlan,
) -> Result<(Vec<Fragment>, Vec<KeyRange>, BTreeMap<String, bool>), PlanError> {
    let mut g = MppTaskGenerator {
        ctx,
        gather_id: mpp_gather_id,
        start_ts,
        mpp_query_id,
        frags: Vec::new(),
        cache: HashMap::new(),
        kv_ranges: Vec::new(),
        node_info: BTreeMap::new(),
        table_reader_cache: HashMap::new(),
    };
    g.generate_mpp_tasks(sender)?;
    if g.kv_ranges.is_empty() {
        return Err(PlanError::internal("kvRanges for MPPTask should not be empty"));
    }
    Ok((g.frags, g.kv_ranges, g.node_info))
}

/// The plan node with `id` in `plan`'s tree.
fn find_mut(plan: &mut PhysicalPlan, id: i32) -> Option<&mut PhysicalPlan> {
    if plan.id() == id {
        return Some(plan);
    }
    plan.base_mut().children_mut().iter_mut().find_map(|child| find_mut(child, id))
}

impl MppTaskGenerator<'_> {
    /// Go `generateMPPTasks`.
    fn generate_mpp_tasks(&mut self, s: &PhysicalPlan) -> Result<(), PlanError> {
        let tidb_task: MppTask = Arc::new(tidb_txnkv::MppTask {
            location: Arc::new(MppAddr { addr: String::new() }) as Arc<dyn MppTaskLocation>,
            id: -1,
            start_ts: self.start_ts,
            gather_id: self.gather_id,
            query_id: self.mpp_query_id,
            table_id: 0,
            version: self.ctx.choose_mpp_version(),
            session_id: self.ctx.connection_id(),
            session_alias: self.ctx.session_alias(),
            partition_table_ids: Vec::new(),
            tiflash_static_prune: false,
        });
        let TasksAndFrags { frags, .. } = self.generate_mpp_tasks_for_sink(s)?;
        for idx in frags {
            let frag = &mut self.frags[idx];
            if let Some(PhysicalPlan::ExchangeSender(sink)) = frag.sink.as_mut() {
                sink.target_tasks = vec![Arc::clone(&tidb_task)];
            }
            frag.is_root = true;
        }
        Ok(())
    }

    /// Go `generateMPPTasksForSink` / `generateMPPTasksForExchangeSender`.
    fn generate_mpp_tasks_for_sink(&mut self, s: &PhysicalPlan) -> Result<TasksAndFrags, PlanError> {
        if let Some(cached) = self.cache.get(&s.id()) {
            return Ok(cached.clone());
        }
        let mut frags = build_fragments(s)?;
        let mut results = Vec::with_capacity(frags.len());
        for f in &mut frags {
            // from the bottom up
            let tasks = self.generate_mpp_tasks_for_fragment(f)?;
            results.extend(tasks);
        }
        let first = self.frags.len();
        self.frags.extend(frags);
        let cached = TasksAndFrags { tasks: results, frags: (first..self.frags.len()).collect() };
        self.cache.insert(s.id(), cached.clone());
        Ok(cached)
    }

    /// Go `generateMPPTasksForFragment`.
    fn generate_mpp_tasks_for_fragment(&mut self, f: &mut Fragment) -> Result<Vec<MppTask>, PlanError> {
        let sink = f.sink.as_mut().expect("a fragment owns its sink");
        let mut receiver_tasks = Vec::with_capacity(f.exchange_receivers.len());
        let mut lower_frags = Vec::with_capacity(f.exchange_receivers.len());
        for &r in &f.exchange_receivers {
            let receiver = find_mut(sink, r).expect("the fragment's receiver");
            // chain call: to get lower fragments and tasks
            let sender = receiver.children().first().cloned().expect("a receiver's sender");
            let TasksAndFrags { tasks, frags } = self.generate_mpp_tasks_for_sink(&sender)?;
            if let PhysicalPlan::ExchangeReceiver(receiver) = receiver {
                receiver.tasks.clone_from(&tasks);
            }
            receiver_tasks.push(tasks);
            lower_frags.push(frags);
        }
        let tasks = if let Some(ts) = f.table_scan {
            let PhysicalPlan::TableScan(ts) = find_mut(sink, ts).expect("the fragment's table scan") else {
                unreachable!("a fragment's table scan is a table scan")
            };
            let ts = ts.clone();
            let tasks = self.construct_mpp_tasks_impl(&ts)?;
            if tasks.is_empty() {
                return Err(PlanError::internal(
                    "In mpp mode, the number of tasks for table scan should not be zero. \
                     Please set tidb_allow_mpp = 0, and then rerun sql.",
                ));
            }
            tasks
        } else {
            let mut children_tasks: Vec<MppTask> = receiver_tasks.into_iter().flatten().collect();
            if f.singleton && !children_tasks.is_empty() {
                children_tasks.truncate(1);
            }
            self.construct_mpp_tasks_by_children_tasks(&children_tasks)
        };
        for idx in lower_frags.into_iter().flatten() {
            if let Some(PhysicalPlan::ExchangeSender(lower_sink)) = self.frags[idx].sink.as_mut() {
                lower_sink.target_tasks.extend(tasks.iter().cloned());
            }
        }
        if let Some(PhysicalPlan::ExchangeSender(sink)) = f.sink.as_mut() {
            sink.tasks.clone_from(&tasks);
        }
        Ok(tasks)
    }

    /// Go `constructMPPTasksByChildrenTasks`: one task per address the lower
    /// tasks cover.
    fn construct_mpp_tasks_by_children_tasks(&self, tasks: &[MppTask]) -> Vec<MppTask> {
        let mut address_map = std::collections::HashSet::new();
        let mut new_tasks = Vec::with_capacity(tasks.len());
        let mpp_version = self.ctx.choose_mpp_version();
        let session_id = self.ctx.connection_id();
        let session_alias = self.ctx.session_alias();
        for task in tasks {
            let addr = task.location.address().to_owned();
            // for upper fragment, the task num is equal to address num covered by lower tasks
            if address_map.insert(addr.clone()) {
                new_tasks.push(Arc::new(tidb_txnkv::MppTask {
                    location: Arc::new(MppAddr { addr }) as Arc<dyn MppTaskLocation>,
                    id: self.ctx.alloc_mpp_task_id(),
                    gather_id: self.gather_id,
                    query_id: self.mpp_query_id,
                    start_ts: self.start_ts,
                    table_id: -1,
                    version: mpp_version,
                    session_id,
                    session_alias: session_alias.clone(),
                    partition_table_ids: Vec::new(),
                    tiflash_static_prune: false,
                }));
            }
        }
        new_tasks
    }

    /// Go `constructMPPTasksImpl`: the tasks of a fragment that reads a
    /// table (a table without partitions, or a single partition of one).
    fn construct_mpp_tasks_impl(&mut self, ts: &PhysicalTableScan) -> Result<Vec<MppTask>, PlanError> {
        // update ranges according to correlated columns in access conditions like in the Open() of TableReaderExecutor
        let ranges = if ts.have_cor_col() {
            self.ctx.resolve_correlated_columns(ts)?
        } else {
            ts.ranges.clone()
        };
        let table = ts.table.as_ref().ok_or_else(|| PlanError::internal("an MPP table scan has no table"))?.read().clone();
        let (splited_ranges, _) =
            tidb_distsql::split_ranges_across_int64_boundary(ranges, false, false, table.is_common_handle);
        // True when:
        // 1. Is partition table.
        // 2. Dynamic prune is not used.
        let mut tiflash_static_prune = false;
        let mut all_partitions_ids = Vec::new();
        let req = if table.get_partition_info().is_some() {
            tiflash_static_prune = !self.ctx.use_dynamic_partition_prune();
            if tiflash_static_prune {
                let req = MppBuildTasksRequest {
                    key_ranges: Some(self.ctx.table_handle_ranges_to_kv_ranges(ts.physical_table_id, table.is_common_handle, &splited_ranges)?),
                    start_ts: self.start_ts,
                    ..MppBuildTasksRequest::default()
                };
                self.kv_ranges.extend(req.key_ranges.iter().flatten().cloned());
                req
            } else {
                let mut partitions = self.ctx.partition_pruning(ts)?;
                partitions.sort_unstable();
                let mut partition_id_and_ranges = Vec::with_capacity(partitions.len());
                for &pid in &partitions {
                    let key_ranges = self.ctx.table_handle_ranges_to_kv_ranges(pid, table.is_common_handle, &splited_ranges)?;
                    partition_id_and_ranges.push(PartitionIdAndRanges { id: pid, key_ranges });
                }
                all_partitions_ids = partitions;
                for partition_kv_ranges in &partition_id_and_ranges {
                    self.kv_ranges.extend(partition_kv_ranges.key_ranges.iter().cloned());
                }
                MppBuildTasksRequest { start_ts: self.start_ts, partition_id_and_ranges, ..MppBuildTasksRequest::default() }
            }
        } else {
            let req = MppBuildTasksRequest {
                key_ranges: Some(self.ctx.table_handle_ranges_to_kv_ranges(table.id, table.is_common_handle, &splited_ranges)?),
                start_ts: self.start_ts,
                ..MppBuildTasksRequest::default()
            };
            self.kv_ranges.extend(req.key_ranges.iter().flatten().cloned());
            req
        };

        let val = req.cache_key();
        let metas = if let Some(metas) = self.table_reader_cache.get(&val) {
            let metas = metas.clone();
            #[cfg(any(test, feature = "test-util"))]
            mock::table_reader_cache_hit();
            metas
        } else {
            let metas = self.ctx.construct_mpp_tasks(&req)?;
            self.table_reader_cache.insert(val, metas.clone());
            #[cfg(any(test, feature = "test-util"))]
            mock::table_reader_cache_miss();
            metas
        };

        let mpp_version = self.ctx.choose_mpp_version();
        let session_id = self.ctx.connection_id();
        let session_alias = self.ctx.session_alias();
        let mut tasks = Vec::with_capacity(metas.len());
        for meta in metas {
            let addr = meta.address().to_owned();
            tasks.push(Arc::new(tidb_txnkv::MppTask {
                location: meta,
                id: self.ctx.alloc_mpp_task_id(),
                start_ts: self.start_ts,
                gather_id: self.gather_id,
                query_id: self.mpp_query_id,
                table_id: table.id,
                partition_table_ids: all_partitions_ids.clone(),
                tiflash_static_prune,
                version: mpp_version,
                session_id,
                session_alias: session_alias.clone(),
            }));
            self.node_info.insert(addr, true);
        }
        Ok(tasks)
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

    /// A fake session placing every table read on two TiFlash stores.
    struct FakeCtx {
        next_task_id: std::cell::Cell<i64>,
    }

    impl super::MppTaskGeneratorContext for FakeCtx {
        fn choose_mpp_version(&self) -> tidb_txnkv::MppVersion {
            tidb_txnkv::MppVersion::V3
        }
        fn connection_id(&self) -> u64 {
            7
        }
        fn session_alias(&self) -> String {
            String::new()
        }
        fn alloc_mpp_task_id(&self) -> i64 {
            let id = self.next_task_id.get() + 1;
            self.next_task_id.set(id);
            id
        }
        fn use_dynamic_partition_prune(&self) -> bool {
            true
        }
        fn table_handle_ranges_to_kv_ranges(
            &self,
            tid: i64,
            _: bool,
            _: &[tidb_ranger::types::Range],
        ) -> Result<Vec<tidb_txnkv::KeyRange>, tidb_planner_core_base::PlanError> {
            Ok(vec![tidb_txnkv::KeyRange::new(tidb_txnkv::Key::from_bytes(&tid.to_be_bytes()), tidb_txnkv::Key::from_bytes(&(tid + 1).to_be_bytes()))])
        }
        fn partition_pruning(&self, _: &crate::PhysicalTableScan) -> Result<Vec<i64>, tidb_planner_core_base::PlanError> {
            unreachable!("no partitioned table here")
        }
        fn construct_mpp_tasks(
            &self,
            _: &tidb_txnkv::MppBuildTasksRequest,
        ) -> Result<Vec<std::sync::Arc<dyn tidb_txnkv::MppTaskLocation>>, tidb_planner_core_base::PlanError> {
            Ok(["tiflash0", "tiflash1"]
                .into_iter()
                .map(|addr| std::sync::Arc::new(super::MppAddr { addr: addr.to_owned() }) as std::sync::Arc<dyn tidb_txnkv::MppTaskLocation>)
                .collect())
        }
        fn resolve_correlated_columns(
            &self,
            ts: &crate::PhysicalTableScan,
        ) -> Result<Vec<tidb_ranger::types::Range>, tidb_planner_core_base::PlanError> {
            Ok(ts.ranges.clone())
        }
    }

    fn task_ids(tasks: &[crate::MppTask]) -> Vec<i64> {
        tasks.iter().map(|task| task.id).collect()
    }

    /// Go `generateMPPTasksForFragment`, bottom up: the scan fragment's tasks
    /// come from the store, the upper one's from the addresses its children
    /// cover; each sink sends to the fragment above it, and the root's to the
    /// TiDB-side task.
    #[test]
    fn generate_root_mpp_tasks_wires_each_fragment_to_the_next() {
        let table = tidb_model::TableInfo { id: 42, ..tidb_model::TableInfo::default() };
        let scan = PhysicalPlan::TableScan(crate::PhysicalTableScan {
            base: node(5, "TableScan", vec![]),
            table: Some(tidb_model::GoShared::new(table)),
            physical_table_id: 42,
            ..Default::default()
        });
        let lower_sender = PhysicalPlan::ExchangeSender(PhysicalExchangeSender {
            base: node(4, "ExchangeSender", vec![scan]),
            exchange_type: ExchangeType::Hash,
            ..Default::default()
        });
        let receiver = PhysicalPlan::ExchangeReceiver(PhysicalExchangeReceiver {
            base: node(3, "ExchangeReceiver", vec![lower_sender]),
            ..Default::default()
        });
        let agg = PhysicalPlan::HashAgg(crate::PhysicalHashAgg { base: node(2, "HashAgg", vec![receiver]), ..Default::default() });
        let root_sender = PhysicalPlan::ExchangeSender(PhysicalExchangeSender {
            base: node(1, "ExchangeSender", vec![agg]),
            ..Default::default()
        });

        let ctx = FakeCtx { next_task_id: std::cell::Cell::new(0) };
        let (frags, kv_ranges, node_info) =
            super::generate_root_mpp_tasks(&ctx, 100, 1, tidb_txnkv::MppQueryId::default(), &root_sender).unwrap();
        assert_eq!(frags.len(), 2);
        let sink = |f: &super::Fragment| match f.sink.as_ref().unwrap() {
            PhysicalPlan::ExchangeSender(sender) => sender.clone(),
            _ => unreachable!(),
        };
        let (lower, upper) = (sink(&frags[0]), sink(&frags[1]));
        assert_eq!(task_ids(&lower.tasks), [1, 2]);
        assert_eq!(task_ids(&upper.tasks), [3, 4]);
        assert_eq!(task_ids(&lower.target_tasks), [3, 4]);
        assert_eq!(task_ids(&upper.target_tasks), [-1]);
        assert!(!frags[0].is_root);
        assert!(frags[1].is_root);
        let receiver = &frags[1].sink.as_ref().unwrap().children()[0].children()[0];
        let PhysicalPlan::ExchangeReceiver(receiver) = receiver else { unreachable!() };
        assert_eq!(task_ids(&receiver.tasks), [1, 2]);
        assert_eq!(lower.tasks[0].table_id, 42);
        assert_eq!(upper.tasks[0].table_id, -1);
        assert_eq!(kv_ranges.len(), 1);
        assert_eq!(node_info.keys().cloned().collect::<Vec<_>>(), ["tiflash0", "tiflash1"]);
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
