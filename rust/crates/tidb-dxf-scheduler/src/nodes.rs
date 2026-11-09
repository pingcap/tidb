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

//! Go `nodes.go`.

use std::collections::HashSet;
use std::sync::{Arc, Mutex, RwLock};

use tidb_dxf::ManagedNode;
use tidb_log::{Field, Value};

use crate::scheduler::{get_live_exec_ids, ServerInfoSource};
use crate::{SlotManager, TaskManager};

/// Go `nodesCheckInterval`: the tick of checking the live nodes.
pub(crate) fn nodes_check_interval() -> std::time::Duration {
    2 * crate::scheduler::check_task_finished_interval()
}

/// Go `NodeManager`: the live TiDB nodes of the cluster, and the nodes the
/// framework manages.
pub struct NodeManager {
    logger: tidb_logutil::Logger,
    sample_logger: tidb_logutil::SampledLogger,
    /// The live nodes of the last check.
    prev_live_nodes: Mutex<HashSet<String>>,
    /// The cached nodes the framework manages; see
    /// `TaskManager::get_all_nodes`.
    nodes: RwLock<Arc<Vec<ManagedNode>>>,
}

impl NodeManager {
    /// Go `newNodeManager`.
    #[must_use]
    pub(crate) fn new(server_id: &str) -> Self {
        let mut logger = tidb_logutil::err_verbose_logger();
        let mut fields = Vec::new();
        if tidb_intest::IN_TEST {
            fields.push(Field::new("server-id", Value::Str(server_id.to_owned())));
            logger = logger.with_fields(&fields);
        }
        Self {
            logger,
            sample_logger: tidb_dxf_handle::new_sample_err_verbose_logger(&fields),
            prev_live_nodes: Mutex::new(HashSet::new()),
            nodes: RwLock::new(Arc::new(Vec::with_capacity(10))),
        }
    }

    /// Go `maintainLiveNodes`: removes the dead nodes from
    /// `dist_framework_meta` (see the task executor's `recoverMetaLoop` for
    /// how a node gets there).
    pub(crate) fn maintain_live_nodes(&self, server_info: &dyn ServerInfoSource, task_mgr: &dyn TaskManager) {
        // Safe to discard errors since this function can be called at regular intervals.
        let live_exec_ids = match get_live_exec_ids(server_info) {
            Ok(ids) => ids,
            Err(error) => {
                self.sample_logger.warn(
                    "generate task executor nodes met error",
                    &[Field::new("error", Value::Str(error))],
                );
                return;
            }
        };
        let mut prev_live_nodes = self.prev_live_nodes.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut node_changed = live_exec_ids.len() != prev_live_nodes.len();
        let mut curr_live_nodes = HashSet::with_capacity(live_exec_ids.len());
        for exec_id in live_exec_ids {
            if !prev_live_nodes.contains(&exec_id) {
                node_changed = true;
            }
            curr_live_nodes.insert(exec_id);
        }
        if !node_changed {
            return;
        }

        let old_nodes = match task_mgr.get_all_nodes() {
            Ok(nodes) => nodes,
            Err(error) => {
                self.sample_logger.warn("get all nodes met error", &[Field::new("error", Value::Str(error.to_string()))]);
                return;
            }
        };

        let dead_nodes: Vec<String> =
            old_nodes.into_iter().filter(|node| !curr_live_nodes.contains(&node.id)).map(|node| node.id).collect();
        if dead_nodes.is_empty() {
            *prev_live_nodes = curr_live_nodes;
            return;
        }
        self.logger.info(
            "delete dead nodes from dist_framework_meta",
            &[Field::new("dead-nodes", Value::Str(format!("{dead_nodes:?}")))],
        );
        if let Err(error) = task_mgr.delete_dead_nodes(&dead_nodes) {
            self.logger.warn(
                "delete dead nodes from dist_framework_meta failed",
                &[Field::new("error", Value::Str(error.to_string()))],
            );
            return;
        }
        *prev_live_nodes = curr_live_nodes;
    }

    /// Go `refreshNodes`: the nodes the framework manages, and the slot
    /// capacity from their CPU count.
    pub(crate) fn refresh_nodes(&self, task_mgr: &dyn TaskManager, slot_mgr: &SlotManager) {
        let new_nodes = match task_mgr.get_all_nodes() {
            Ok(nodes) => nodes,
            Err(error) => {
                self.sample_logger.warn("get managed nodes met error", &[Field::new("error", Value::Str(error.to_string()))]);
                return;
            }
        };

        let cpu_count = new_nodes.iter().find(|node| node.cpu_count > 0).map_or(0, |node| node.cpu_count);
        slot_mgr.update_capacity(cpu_count);
        self.store_nodes(new_nodes);
    }

    pub(crate) fn store_nodes(&self, nodes: Vec<ManagedNode>) {
        *self.nodes.write().unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::new(nodes);
    }

    /// Go `getNodes`: a copy of the nodes the framework manages.
    pub(crate) fn get_nodes(&self) -> Vec<ManagedNode> {
        self.nodes.read().unwrap_or_else(std::sync::PoisonError::into_inner).as_ref().clone()
    }

    #[cfg(test)]
    pub(crate) fn prev_live_nodes(&self) -> HashSet<String> {
        self.prev_live_nodes.lock().unwrap().clone()
    }
}

/// Go `filterByScope`: the nodes of the target scope; for the default
/// scope, the "background" nodes when there are any.
pub(crate) fn filter_by_scope(nodes: &[ManagedNode], target_scope: &str) -> Vec<String> {
    let have_background = nodes.iter().any(|node| node.role == "background");
    // prefer to use "background" node instead of "" node.
    if target_scope.is_empty() && have_background {
        return nodes.iter().filter(|node| node.role == "background").map(|node| node.id.clone()).collect();
    }
    nodes.iter().filter(|node| node.role == target_scope).map(|node| node.id.clone()).collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use tidb_dxf::ManagedNode;
    use tidb_dxf_storage::StorageError;

    use super::{filter_by_scope, NodeManager};
    use crate::mock::{MockServerInfo, MockTaskManager};
    use crate::SlotManager;

    fn node(id: &str, cpu_count: isize) -> ManagedNode {
        ManagedNode { id: id.to_owned(), role: String::new(), cpu_count }
    }

    fn set(ids: &[&str]) -> HashSet<String> {
        ids.iter().map(|id| (*id).to_owned()).collect()
    }

    /// Go `TestMaintainLiveNodes`.
    #[test]
    fn test_maintain_live_nodes() {
        let mock_task_mgr = MockTaskManager::default();
        let server_info = MockServerInfo::new(&[":4000"]);

        let node_mgr = NodeManager::new("");
        mock_task_mgr.expect_get_all_nodes(Err(StorageError::Other("mock error".to_owned())));
        node_mgr.maintain_live_nodes(&server_info, &mock_task_mgr);
        assert!(node_mgr.prev_live_nodes().is_empty());
        mock_task_mgr.assert_satisfied();
        // no change
        mock_task_mgr.expect_get_all_nodes(Ok(vec![node(":4000", 0)]));
        node_mgr.maintain_live_nodes(&server_info, &mock_task_mgr);
        assert_eq!(node_mgr.prev_live_nodes(), set(&[":4000"]));
        mock_task_mgr.assert_satisfied();
        // run again, return fast
        node_mgr.maintain_live_nodes(&server_info, &mock_task_mgr);
        assert_eq!(node_mgr.prev_live_nodes(), set(&[":4000"]));
        mock_task_mgr.assert_satisfied();

        // scale out 1 node
        server_info.store(&[":4000", ":4001"]);

        // fail on clean
        mock_task_mgr.expect_get_all_nodes(Ok(vec![node(":4000", 0), node(":4001", 0), node(":4002", 0)]));
        mock_task_mgr.expect_delete_dead_nodes(Err(StorageError::Other("mock error".to_owned())));
        node_mgr.maintain_live_nodes(&server_info, &mock_task_mgr);
        assert_eq!(node_mgr.prev_live_nodes(), set(&[":4000"]));
        mock_task_mgr.assert_satisfied();
        // remove 1 node
        mock_task_mgr.expect_get_all_nodes(Ok(vec![node(":4000", 0), node(":4001", 0), node(":4002", 0)]));
        mock_task_mgr.expect_delete_dead_nodes(Ok(()));
        node_mgr.maintain_live_nodes(&server_info, &mock_task_mgr);
        assert_eq!(node_mgr.prev_live_nodes(), set(&[":4000", ":4001"]));
        mock_task_mgr.assert_satisfied();
        // run again, return fast
        node_mgr.maintain_live_nodes(&server_info, &mock_task_mgr);
        assert_eq!(node_mgr.prev_live_nodes(), set(&[":4000", ":4001"]));
        mock_task_mgr.assert_satisfied();

        // scale in 1 node
        server_info.store(&[":4000"]);

        mock_task_mgr.expect_get_all_nodes(Ok(vec![node(":4000", 0), node(":4001", 0), node(":4002", 0)]));
        mock_task_mgr.expect_delete_dead_nodes(Ok(()));
        node_mgr.maintain_live_nodes(&server_info, &mock_task_mgr);
        assert_eq!(node_mgr.prev_live_nodes(), set(&[":4000"]));
        mock_task_mgr.assert_satisfied();
        // run again, return fast
        node_mgr.maintain_live_nodes(&server_info, &mock_task_mgr);
        assert_eq!(node_mgr.prev_live_nodes(), set(&[":4000"]));
        mock_task_mgr.assert_satisfied();
    }

    /// Go `TestMaintainManagedNodes`.
    #[test]
    fn test_maintain_managed_nodes() {
        let mock_task_mgr = MockTaskManager::default();
        let node_mgr = NodeManager::new("");

        let slot_mgr = SlotManager::new();
        mock_task_mgr.expect_get_all_nodes(Err(StorageError::Other("mock error".to_owned())));
        node_mgr.refresh_nodes(&mock_task_mgr, &slot_mgr);
        assert_eq!(slot_mgr.get_capacity(), tidb_cpu::get_cpu_count() as isize);
        assert!(node_mgr.get_nodes().is_empty());
        mock_task_mgr.assert_satisfied();

        mock_task_mgr.expect_get_all_nodes(Ok(vec![node(":4000", 100), node(":4001", 100)]));
        node_mgr.refresh_nodes(&mock_task_mgr, &slot_mgr);
        assert_eq!(node_mgr.get_nodes(), [node(":4000", 100), node(":4001", 100)]);
        assert_eq!(slot_mgr.get_capacity(), 100);
        mock_task_mgr.assert_satisfied();
        mock_task_mgr.expect_get_all_nodes(Ok(Vec::new()));
        node_mgr.refresh_nodes(&mock_task_mgr, &slot_mgr);
        assert!(node_mgr.get_nodes().is_empty());
        assert_eq!(slot_mgr.get_capacity(), 100);
        mock_task_mgr.assert_satisfied();
    }

    /// Go `mockManagedNode`.
    fn mock_managed_node(id: &str, role: &str) -> ManagedNode {
        ManagedNode { id: id.to_owned(), role: role.to_owned(), cpu_count: 100 }
    }

    /// Go `TestFilterByScope`.
    #[test]
    fn test_filter_by_scope() {
        let n = mock_managed_node;
        let cases: Vec<(Vec<ManagedNode>, &str, Vec<&str>)> = vec![
            (vec![n("1", "background"), n("2", "background"), n("3", "")], "", vec!["1", "2"]),
            (vec![n("1", ""), n("2", ""), n("3", "")], "", vec!["1", "2", "3"]),
            (vec![n("1", "1"), n("2", ""), n("3", "")], "", vec!["2", "3"]),
            (vec![n("1", "1"), n("2", ""), n("3", "")], "1", vec!["1"]),
            (vec![n("1", "1"), n("2", "2"), n("3", "2")], "1", vec!["1"]),
            (vec![n("1", "1"), n("2", "2"), n("3", "3")], "2", vec!["2"]),
            (vec![n("1", "1"), n("2", "2"), n("3", "background")], "background", vec!["3"]),
            (vec![n("1", "1"), n("2", ""), n("3", "background")], "", vec!["3"]),
            (vec![n("1", ""), n("2", ""), n("3", "")], "background", vec![]),
        ];
        for (i, (nodes, target_scope, expected_nodes)) in cases.into_iter().enumerate() {
            assert_eq!(filter_by_scope(&nodes, target_scope), expected_nodes, "case-{i}");
        }
    }
}
