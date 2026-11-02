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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `handleFineGrainedShuffle` (`pkg/planner/core/optimizer.go`): the MPP
//! fragments whose data partition survives from a hash `ExchangeSender` up to
//! a window, a hash aggregation or a hash join's build side run that stretch
//! as `TiFlashFineGrainedShuffleStreamCount` parallel streams.
//!
//! Go's helper collects pointers to the plans of the stretch and sets them
//! when it reaches the sender. Here the walk records each plan's path from
//! the root, and the counts are set by path afterwards.

use tidb_planner_core_operator_physicalop::{ExchangeType, PhysicalHashJoin, PhysicalPlan};
use tidb_sessionctx::{TiFlashClusterInfo, TiFlashFineGrainedShuffleSession};

use crate::optimize::OptimizeContext;

/// The step into a `PhysicalTableReader`'s `TablePlan`, beside child indexes.
const TABLE_PLAN: usize = usize::MAX;

/// Go `DefStreamCountWhenMaxThreadsNotSet`.
const DEF_STREAM_COUNT_WHEN_MAX_THREADS_NOT_SET: u64 =
    tidb_vardef::defaults::DEF_STREAM_COUNT_WHEN_MAX_THREADS_NOT_SET as u64;

#[derive(Clone, Copy, PartialEq, Eq)]
enum ShuffleTarget {
    Unknown,
    Window,
    JoinBuild,
    HashAgg,
}

struct Helper {
    target: ShuffleTarget,
    plans: Vec<Vec<usize>>,
    join_keys: Vec<tidb_expr::column::Column>,
}

impl Helper {
    fn new(target: ShuffleTarget) -> Self {
        Self {
            target,
            plans: Vec::new(),
            join_keys: Vec::new(),
        }
    }

    fn clear(&mut self) {
        self.target = ShuffleTarget::Unknown;
        self.plans.clear();
        self.join_keys.clear();
    }

    fn update_target(&mut self, target: ShuffleTarget, path: &[usize]) {
        self.target = target;
        self.plans.push(path.to_vec());
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum InfoStatus {
    UnInitialized,
    Initialized,
    Failed,
}

/// Go `tiflashClusterInfo`.
#[derive(Clone, Copy)]
struct ClusterInfoItem {
    status: InfoStatus,
    value: u64,
}

struct Walk<'a> {
    cluster: Option<&'a dyn TiFlashClusterInfo>,
    hash_join_v2_session: bool,
    stream_count_info: ClusterInfoItem,
    server_count_info: ClusterInfoItem,
    /// The stream count each recorded plan takes.
    assignments: Vec<(Vec<usize>, u64)>,
}

/// Go `handleFineGrainedShuffle`.
pub(crate) fn handle_fine_grained_shuffle<C: OptimizeContext>(ctx: &C, plan: &mut PhysicalPlan) {
    let TiFlashFineGrainedShuffleSession {
        stream_count,
        max_threads,
        hash_join_v2,
    } = ctx.tiflash_fine_grained_shuffle();
    if stream_count < 0 {
        return;
    }
    let stream_count = if stream_count == 0 && max_threads > 0 {
        max_threads
    } else {
        stream_count
    };
    let mut walk = Walk {
        cluster: ctx.tiflash_cluster_info().map(|cluster| &**cluster),
        hash_join_v2_session: hash_join_v2,
        stream_count_info: if stream_count != 0 {
            ClusterInfoItem {
                status: InfoStatus::Initialized,
                value: stream_count as u64,
            }
        } else {
            ClusterInfoItem {
                status: InfoStatus::UnInitialized,
                value: 0,
            }
        },
        server_count_info: ClusterInfoItem {
            status: InfoStatus::UnInitialized,
            value: 0,
        },
        assignments: Vec::new(),
    };
    let mut path = Vec::new();
    walk.setup(plan, &mut path);
    for (path, count) in std::mem::take(&mut walk.assignments) {
        node_at(plan, &path).base_mut().tiflash_fine_grained_shuffle_stream_count = count;
    }
}

fn node_at<'p>(mut plan: &'p mut PhysicalPlan, path: &[usize]) -> &'p mut PhysicalPlan {
    for &step in path {
        plan = if step == TABLE_PLAN {
            let PhysicalPlan::TableReader(reader) = plan else {
                unreachable!("a table-plan step follows a table reader");
            };
            reader
                .table_plan
                .as_deref_mut()
                .expect("a recorded table plan exists")
        } else {
            &mut plan.base_mut().children_mut()[step]
        };
    }
    plan
}

/// Go `PhysicalHashJoin.CanTiFlashUseHashJoinV2`.
fn can_tiflash_use_hash_join_v2(join: &PhysicalHashJoin, session: bool) -> bool {
    use tidb_planner_core_base::LogicalJoinType;
    if !session {
        return false;
    }
    match join.join_type {
        LogicalJoinType::Inner => {
            join.na_equal_conditions.is_empty()
                && !join.left_join_keys.is_empty()
                && !join.is_null_eq.iter().any(|null_eq| *null_eq)
        }
        _ => false,
    }
}

impl Walk<'_> {
    /// Go `setupFineGrainedShuffle`.
    fn setup(&mut self, plan: &PhysicalPlan, path: &mut Vec<usize>) {
        if let PhysicalPlan::TableReader(reader) = plan {
            if let Some(table_plan @ PhysicalPlan::ExchangeSender(_)) = reader.table_plan.as_deref() {
                let mut helper = Helper::new(ShuffleTarget::Unknown);
                path.push(TABLE_PLAN);
                self.setup_internal(table_plan, &mut helper, path);
                path.pop();
            }
            return;
        }
        for (index, child) in plan.children().iter().enumerate() {
            path.push(index);
            self.setup(child, path);
            path.pop();
        }
    }

    fn setup_child(&mut self, plan: &PhysicalPlan, index: usize, helper: &mut Helper, path: &mut Vec<usize>) {
        path.push(index);
        self.setup_internal(&plan.children()[index], helper, path);
        path.pop();
    }

    /// Go `setupFineGrainedShuffleInternal`.
    fn setup_internal(&mut self, plan: &PhysicalPlan, helper: &mut Helper, path: &mut Vec<usize>) {
        match plan {
            PhysicalPlan::Window(_) => {
                // The window executor keeps the data partition.
                helper.update_target(ShuffleTarget::Window, path);
                self.setup_child(plan, 0, helper, path);
            }
            PhysicalPlan::Sort(sort) => {
                if sort.is_partial_sort {
                    helper.plans.push(path.clone());
                } else {
                    // A global sort breaks the data partition.
                    helper.clear();
                }
                self.setup_child(plan, 0, helper, path);
            }
            PhysicalPlan::Selection(_) | PhysicalPlan::Projection(_) | PhysicalPlan::ExchangeReceiver(_) => {
                helper.plans.push(path.clone());
                self.setup_child(plan, 0, helper, path);
            }
            PhysicalPlan::HashAgg(_) => {
                let mut agg_helper = Helper::new(ShuffleTarget::HashAgg);
                agg_helper.plans.push(path.clone());
                self.setup_child(plan, 0, &mut agg_helper, path);
            }
            PhysicalPlan::HashJoin(join) => {
                let (build, probe, join_keys) = if join.inner_child_idx != 0 {
                    (1, 0, &join.right_join_keys)
                } else {
                    (0, 1, &join.left_join_keys)
                };
                if !join_keys.is_empty() && !can_tiflash_use_hash_join_v2(join, self.hash_join_v2_session) {
                    let mut build_helper = Helper::new(ShuffleTarget::JoinBuild);
                    build_helper.plans.push(path.clone());
                    build_helper.join_keys.clone_from(join_keys);
                    self.setup_child(plan, build, &mut build_helper, path);
                } else {
                    let mut build_helper = Helper::new(ShuffleTarget::Unknown);
                    self.setup_child(plan, build, &mut build_helper, path);
                }
                // The probe side takes no fine-grained shuffle.
                helper.clear();
                self.setup_child(plan, probe, helper, path);
            }
            PhysicalPlan::ExchangeSender(sender) => {
                if sender.exchange_type == ExchangeType::Hash {
                    let exchange_col_count = plan.schema().map_or(0, |schema| schema.columns.len());
                    let count = match helper.target {
                        ShuffleTarget::Window => Some(self.infer_stream_count_for_window()),
                        ShuffleTarget::HashAgg => self.check_for_join_agg(exchange_col_count, 1200),
                        ShuffleTarget::JoinBuild => {
                            // Only when the shuffle keys are the join keys, so
                            // the shuffle hash matches the join hash.
                            let keys_match = sender.hash_cols.len() == helper.join_keys.len()
                                && helper.join_keys.iter().zip(&sender.hash_cols).all(|(key, column)| {
                                    column.col.equal_column(&tidb_expr::expression::Expression::Column(key.clone()))
                                });
                            if keys_match {
                                self.check_for_join_agg(exchange_col_count, 600)
                            } else {
                                None
                            }
                        }
                        ShuffleTarget::Unknown => None,
                    };
                    if let Some(count) = count {
                        self.assignments.push((path.clone(), count));
                        for plan_path in &helper.plans {
                            self.assignments.push((plan_path.clone(), count));
                        }
                    }
                }
                // The exchange sender breaks the data partition.
                helper.clear();
                self.setup_child(plan, 0, helper, path);
            }
            _ => {
                for index in 0..plan.children().len() {
                    let mut child_helper = Helper::new(ShuffleTarget::Unknown);
                    self.setup_child(plan, index, &mut child_helper, path);
                }
            }
        }
    }

    fn tiflash_server_info(&self) -> Result<Vec<tidb_sessionctx::TiFlashServer>, String> {
        match self.cluster {
            Some(cluster) => cluster.tiflash_server_info(),
            None => Err("no TiFlash cluster information".to_owned()),
        }
    }

    /// Go `calculateTiFlashStreamCountUsingMinLogicalCores`.
    fn stream_count_using_min_logical_cores(&self, servers: &[tidb_sessionctx::TiFlashServer]) -> Option<u64> {
        self.cluster.and_then(|cluster| cluster.min_logical_cores(servers))
    }

    /// Go `setDefaultStreamCount`.
    fn set_default_stream_count(&mut self) {
        self.stream_count_info = ClusterInfoItem {
            status: InfoStatus::Initialized,
            value: DEF_STREAM_COUNT_WHEN_MAX_THREADS_NOT_SET,
        };
    }

    /// Go `checkFineGrainedShuffleForJoinAgg`: the stream count to apply, if
    /// the exchange stays within `split_limit`.
    fn check_for_join_agg(&mut self, exchange_col_count: usize, split_limit: u64) -> Option<u64> {
        let stream_count = match self.stream_count_info.status {
            // Assume an 8-core node as the minimum: 8 / 2.
            InfoStatus::UnInitialized => 4,
            InfoStatus::Initialized => self.stream_count_info.value,
            InfoStatus::Failed => return None,
        };
        let server_count = match self.server_count_info.status {
            InfoStatus::UnInitialized => match self.tiflash_server_info() {
                Ok(servers) => {
                    let count = servers.len() as u64;
                    self.server_count_info = ClusterInfoItem {
                        status: InfoStatus::Initialized,
                        value: count,
                    };
                    count
                }
                Err(_) => {
                    self.server_count_info = ClusterInfoItem {
                        status: InfoStatus::Failed,
                        value: 0,
                    };
                    if self.stream_count_info.status == InfoStatus::UnInitialized {
                        self.set_default_stream_count();
                    }
                    return None;
                }
            },
            InfoStatus::Initialized => self.server_count_info.value,
            InfoStatus::Failed => return None,
        };
        let cols = exchange_col_count as u64;
        if server_count * cols * stream_count > split_limit {
            return None;
        }
        if self.stream_count_info.status == InfoStatus::Initialized {
            return Some(stream_count);
        }
        let servers = match self.tiflash_server_info() {
            Ok(servers) => servers,
            Err(_) => {
                self.server_count_info = ClusterInfoItem {
                    status: InfoStatus::Failed,
                    value: 0,
                };
                return None;
            }
        };
        let Some(stream_count) = self.stream_count_using_min_logical_cores(&servers) else {
            self.set_default_stream_count();
            self.server_count_info.status = InfoStatus::Failed;
            return None;
        };
        self.stream_count_info = ClusterInfoItem {
            status: InfoStatus::Initialized,
            value: stream_count,
        };
        (server_count * cols * stream_count <= split_limit).then_some(stream_count)
    }

    /// Go `inferFineGrainedShuffleStreamCountForWindow`.
    fn infer_stream_count_for_window(&mut self) -> u64 {
        match self.stream_count_info.status {
            InfoStatus::UnInitialized => {
                if self.server_count_info.status == InfoStatus::Failed {
                    self.set_default_stream_count();
                    return self.stream_count_info.value;
                }
                let servers = match self.tiflash_server_info() {
                    Ok(servers) => servers,
                    Err(_) => {
                        self.set_default_stream_count();
                        self.server_count_info.status = InfoStatus::Failed;
                        return self.stream_count_info.value;
                    }
                };
                if self.server_count_info.status == InfoStatus::UnInitialized {
                    self.server_count_info = ClusterInfoItem {
                        status: InfoStatus::Initialized,
                        value: servers.len() as u64,
                    };
                }
                let Some(stream_count) = self.stream_count_using_min_logical_cores(&servers) else {
                    self.set_default_stream_count();
                    self.server_count_info.status = InfoStatus::Failed;
                    return self.stream_count_info.value;
                };
                self.stream_count_info = ClusterInfoItem {
                    status: InfoStatus::Initialized,
                    value: stream_count,
                };
                stream_count
            }
            InfoStatus::Initialized => self.stream_count_info.value,
            InfoStatus::Failed => {
                self.set_default_stream_count();
                self.stream_count_info.value
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_planner_core_operator_physicalop::{
        BasePhysicalPlan, PhysicalExchangeReceiver, PhysicalExchangeSender, PhysicalProjection, PhysicalSort,
        PhysicalTableReader, PhysicalTableScan, PhysicalWindow,
    };

    fn node(id: i32, tp: &str, children: Vec<PhysicalPlan>) -> BasePhysicalPlan {
        let mut base = BasePhysicalPlan::with_id(id, tp, 0);
        base.set_children(children);
        base
    }

    /// Go's MPP window fragment: `TableReader -> ExchangeSender(PassThrough)
    /// -> Projection -> Window -> Sort(partial) -> ExchangeReceiver ->
    /// ExchangeSender(Hash) -> TableScan`.
    fn mpp_window_plan(partial_sort: bool) -> PhysicalPlan {
        let scan = PhysicalPlan::TableScan(PhysicalTableScan {
            base: node(1, "TableFullScan", Vec::new()),
            ..Default::default()
        });
        let hash_sender = PhysicalPlan::ExchangeSender(PhysicalExchangeSender {
            base: node(2, "ExchangeSender", vec![scan]),
            exchange_type: ExchangeType::Hash,
            hash_cols: Vec::new(),
        });
        let receiver = PhysicalPlan::ExchangeReceiver(PhysicalExchangeReceiver {
            base: node(3, "ExchangeReceiver", vec![hash_sender]),
            ..Default::default()
        });
        let sort = PhysicalPlan::Sort(PhysicalSort {
            base: node(4, "Sort", vec![receiver]),
            is_partial_sort: partial_sort,
            ..Default::default()
        });
        let window = PhysicalPlan::Window(PhysicalWindow {
            base: node(5, "Window", vec![sort]),
            ..Default::default()
        });
        let projection = PhysicalPlan::Projection(PhysicalProjection {
            base: node(6, "Projection", vec![window]),
            ..Default::default()
        });
        let pass_through = PhysicalPlan::ExchangeSender(PhysicalExchangeSender {
            base: node(7, "ExchangeSender", vec![projection]),
            exchange_type: ExchangeType::PassThrough,
            hash_cols: Vec::new(),
        });
        PhysicalPlan::TableReader(PhysicalTableReader {
            base: node(8, "TableReader", Vec::new()),
            table_plan: Some(Box::new(pass_through)),
            ..Default::default()
        })
    }

    /// Each plan's stream count, from the reader's table plan down.
    fn stream_counts(plan: &PhysicalPlan) -> Vec<(String, u64)> {
        let PhysicalPlan::TableReader(reader) = plan else {
            unreachable!("the test plan is a reader");
        };
        let mut counts = Vec::new();
        let mut current = reader.table_plan.as_deref();
        while let Some(plan) = current {
            counts.push((plan.tp().to_owned(), plan.base().tiflash_fine_grained_shuffle_stream_count));
            current = plan.children().first();
        }
        counts
    }

    fn run(stream_count: i64, max_threads: i64, partial_sort: bool) -> Vec<(String, u64)> {
        let ctx = tidb_sessionctx::StmtContext::default().with_tiflash_fine_grained_shuffle(
            TiFlashFineGrainedShuffleSession {
                stream_count,
                max_threads,
                hash_join_v2: false,
            },
            None,
        );
        let mut plan = mpp_window_plan(partial_sort);
        handle_fine_grained_shuffle(&ctx, &mut plan);
        stream_counts(&plan)
    }

    fn expected(counts: [u64; 7]) -> Vec<(String, u64)> {
        ["ExchangeSender", "Projection", "Window", "Sort", "ExchangeReceiver", "ExchangeSender", "TableFullScan"]
            .into_iter()
            .zip(counts)
            .map(|(tp, count)| (tp.to_owned(), count))
            .collect()
    }

    /// Go `handleFineGrainedShuffle` for a window (captured EXPLAIN: every
    /// plan from the hash sender up to the window's projection shows
    /// `stream_count: 8` when no TiFlash reports its cores).
    #[test]
    fn a_window_fragment_takes_the_default_stream_count() {
        assert_eq!(run(0, -1, true), expected([0, 8, 8, 8, 8, 8, 0]));
    }

    /// A negative `tiflash_fine_grained_shuffle_stream_count` disables the
    /// pass; with a zero count, `tidb_max_tiflash_threads` sizes the streams.
    #[test]
    fn the_session_variables_turn_off_or_size_the_streams() {
        assert_eq!(run(-1, -1, true), expected([0; 7]));
        assert_eq!(run(0, 5, true), expected([0, 5, 5, 5, 5, 5, 0]));
        assert_eq!(run(3, 5, true), expected([0, 3, 3, 3, 3, 3, 0]));
    }

    /// A global sort breaks the data partition: the window above it loses
    /// its target, so the hash sender below finds none and no plan takes
    /// streams.
    #[test]
    fn a_global_sort_breaks_the_partition() {
        assert_eq!(run(0, -1, false), expected([0; 7]));
    }
}
