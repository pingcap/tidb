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


//! Go `nodes.go`: the framework's managed nodes in
//! `mysql.dist_framework_meta`, and the CPU each offers.

use std::sync::RwLock;

use tidb_datatype::Datum;
use tidb_dxf::{schstatus, ManagedNode, NodeResource, SUBTASK_STATE_PENDING, SUBTASK_STATE_RUNNING};
use tidb_sqlescape::SqlArg;

use crate::{exec_sql, Result, SessionContext, StorageError, TaskManager};

/// Go's `nodeResource`, which the domain sets at startup; tests that start
/// no domain read Go's `init` default.
static NODE_RESOURCE: RwLock<Option<NodeResource>> = RwLock::new(None);

/// Go `GetNodeResource`.
#[must_use]
pub fn get_node_resource() -> NodeResource {
    NODE_RESOURCE
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
        // Go `init`: `proto.NewNodeResource(8, 16*units.GiB, 100*units.GiB)`.
        .unwrap_or_else(|| NodeResource::new(8, 16 << 30, 100 << 30))
}

/// Go `SetNodeResource`.
pub fn set_node_resource(resource: NodeResource) {
    *NODE_RESOURCE.write().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(resource);
}

/// Go `GetDXFCPUCount`: the CPU count usable to DXF.
#[must_use]
pub fn get_dxf_cpu_count() -> isize {
    get_node_resource().total_cpu
}

fn row_string(row: &[Datum], idx: usize) -> String {
    match row.get(idx) {
        Some(Datum::String(value)) => String::from_utf8_lossy(value.bytes()).into_owned(),
        Some(Datum::Bytes(value)) => String::from_utf8_lossy(value).into_owned(),
        _ => String::new(),
    }
}

fn row_int(row: &[Datum], idx: usize) -> i64 {
    match row.get(idx) {
        Some(Datum::Int(value)) => *value,
        Some(Datum::UInt(value)) => *value as i64,
        Some(Datum::Decimal(value)) => value.to_i64_trunc().0,
        _ => 0,
    }
}

impl TaskManager {
    /// Go `InitMeta`: inserts this manager into `dist_framework_meta`.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn init_meta(&self, ctx: &tidb_gocontext::Context, tidb_id: &str, role: &str) -> Result<()> {
        self.with_new_session(&mut |se| Self::init_meta_session(ctx, se, tidb_id, role))
    }

    /// Go `InitMetaSession`: inserts the manager, or updates its CPU count
    /// and role.
    ///
    /// # Errors
    /// The statement failed.
    pub fn init_meta_session(ctx: &tidb_gocontext::Context, se: &dyn SessionContext, exec_id: &str, role: &str) -> Result<()> {
        let cpu_count = get_dxf_cpu_count() as i64;
        exec_sql(ctx,
            se,
            "
		insert into mysql.dist_framework_meta(host, role, cpu_count, keyspace_id)
		values (%?, %?, %?, -1)
		on duplicate key
		update cpu_count = %?, role = %?",
            &[
                SqlArg::String(exec_id.as_bytes()),
                SqlArg::String(role.as_bytes()),
                SqlArg::Signed(cpu_count),
                SqlArg::Signed(cpu_count),
                SqlArg::String(role.as_bytes()),
            ],
        )
        .map(drop)
    }

    /// Go `RecoverMeta`: inserts the manager, or updates its CPU count only
    /// (its role changes only through `set global tidb_service_scope`).
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn recover_meta(&self, ctx: &tidb_gocontext::Context, exec_id: &str, role: &str) -> Result<()> {
        let cpu_count = get_dxf_cpu_count() as i64;
        self.execute_sql_with_new_session(ctx, 
            "
		insert into mysql.dist_framework_meta(host, role, cpu_count, keyspace_id)
		values (%?, %?, %?, -1)
		on duplicate key
		update cpu_count = %?",
            &[
                SqlArg::String(exec_id.as_bytes()),
                SqlArg::String(role.as_bytes()),
                SqlArg::Signed(cpu_count),
                SqlArg::Signed(cpu_count),
            ],
        )
        .map(drop)
    }

    /// Go `DeleteDeadNodes`.
    ///
    /// # Errors
    /// The transaction or the statement failed.
    pub fn delete_dead_nodes(&self, ctx: &tidb_gocontext::Context, nodes: &[String]) -> Result<()> {
        if nodes.is_empty() {
            return Ok(());
        }
        // Go formats each host as `"%s"` into the statement text.
        let delete_sql = format!(
            "delete from mysql.dist_framework_meta where host in({})",
            nodes.iter().map(|node| format!("\"{node}\"")).collect::<Vec<_>>().join(", ")
        );
        self.with_new_txn(ctx, &mut |se| exec_sql(ctx, se, &delete_sql, &[]).map(drop))
    }

    /// Go `GetAllNodes`.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn get_all_nodes(&self, ctx: &tidb_gocontext::Context) -> Result<Vec<ManagedNode>> {
        let mut nodes = Vec::new();
        self.with_new_session(&mut |se| {
            nodes = Self::get_all_nodes_with_session(ctx, se)?;
            Ok(())
        })?;
        Ok(nodes)
    }

    fn get_all_nodes_with_session(ctx: &tidb_gocontext::Context, se: &dyn SessionContext) -> Result<Vec<ManagedNode>> {
        let rows = exec_sql(ctx,
            se,
            "
		select host, role, cpu_count
		from mysql.dist_framework_meta
		order by host",
            &[],
        )?;
        Ok(rows
            .iter()
            .map(|row| ManagedNode {
                id: row_string(row, 0),
                role: row_string(row, 1),
                cpu_count: row_int(row, 2) as isize,
            })
            .collect())
    }

    /// Go `GetBusyNodes`: the nodes running subtasks.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn get_busy_nodes(&self, ctx: &tidb_gocontext::Context) -> Result<Vec<schstatus::Node>> {
        let mut exec_ids = Vec::new();
        let (pending, running) = (SUBTASK_STATE_PENDING.to_string(), SUBTASK_STATE_RUNNING.to_string());
        self.with_new_session(&mut |se| {
            let rows = exec_sql(ctx,
                se,
                "
			select distinct exec_id from mysql.tidb_background_subtask
			where state in (%?, %?)",
                &[
                    SqlArg::String(pending.as_bytes()),
                    SqlArg::String(running.as_bytes()),
                ],
            )?;
            exec_ids = rows
                .iter()
                .map(|row| schstatus::Node {
                    id: row_string(row, 0),
                    ..schstatus::Node::default()
                })
                .collect();
            Ok(())
        })?;
        Ok(exec_ids)
    }

    /// Go `GetUsedSlotsOnNodes`: the slots each node's pending and running
    /// subtasks hold (a step's subtasks share their concurrency, hence
    /// `max`).
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn get_used_slots_on_nodes(&self, ctx: &tidb_gocontext::Context) -> Result<std::collections::HashMap<String, isize>> {
        let (pending, running) = (SUBTASK_STATE_PENDING.to_string(), SUBTASK_STATE_RUNNING.to_string());
        let rows = self.execute_sql_with_new_session(ctx, 
            "
		select
			exec_id, sum(concurrency)
		from (
			select exec_id, task_key, max(concurrency) concurrency
			from mysql.tidb_background_subtask
			where state in (%?, %?)
			group by exec_id, task_key
		) a
		group by exec_id",
            &[
                SqlArg::String(pending.as_bytes()),
                SqlArg::String(running.as_bytes()),
            ],
        )?;
        Ok(rows
            .iter()
            .map(|row| (row_string(row, 0), row_int(row, 1) as isize))
            .collect())
    }

    /// Go `GetCPUCountOfNode`.
    ///
    /// # Errors
    /// No node offers CPU, or the session failed.
    pub fn get_cpu_count_of_node(&self, ctx: &tidb_gocontext::Context) -> Result<isize> {
        let mut count = 0;
        self.with_new_session(&mut |se| {
            count = Self::get_cpu_count_of_node_by_role_with_session(ctx, se, "", true)?;
            Ok(())
        })?;
        Ok(count)
    }

    /// Go `GetCPUCountOfNodeByRole`.
    ///
    /// # Errors
    /// No node of `role` offers CPU, or the session failed.
    pub fn get_cpu_count_of_node_by_role(&self, ctx: &tidb_gocontext::Context, role: &str) -> Result<isize> {
        let mut count = 0;
        self.with_new_session(&mut |se| {
            count = Self::get_cpu_count_of_node_by_role_with_session(ctx, se, role, false)?;
            Ok(())
        })?;
        Ok(count)
    }

    /// Go `getCPUCountOfNodeByRole`: the CPU count of the first managed node
    /// (of `role`, unless `arbitrary`) that has one.
    pub(crate) fn get_cpu_count_of_node_by_role_with_session(ctx: &tidb_gocontext::Context, 
        se: &dyn SessionContext,
        role: &str,
        arbitrary: bool,
    ) -> Result<isize> {
        let nodes = Self::get_all_nodes_with_session(ctx, se)?;
        if nodes.is_empty() {
            return Err(StorageError::Other("no managed nodes".to_owned()));
        }
        let cpu_count = nodes
            .iter()
            .filter(|node| arbitrary || node.role == role)
            .map(|node| node.cpu_count)
            .find(|count| *count > 0)
            .unwrap_or(0);
        if cpu_count == 0 {
            return Err(StorageError::Other(
                "no managed node have enough resource for dist task".to_owned(),
            ));
        }
        Ok(cpu_count)
    }
}
