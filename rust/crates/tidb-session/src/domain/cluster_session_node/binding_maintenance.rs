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

//! Binding cache refresh and maintenance use the existing internal-session
//! authority, including its transaction, record locks and SQL executor.
use super::{Domain, DomainSessions as _, InternalBindingWriter};
use tidb_bindinfo::binding::Binding;
use crate::binding_arm::GlobalBindingWriter;
use tidb_bindinfo::binding_utils::{InternalSqlRunner, BINDING_STORAGE_COLUMNS};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Weak,
};
use tidb_datatype::Datum;

pub(super) struct BindingSessions {
    factory: Weak<Domain>,
    next_id: AtomicU64,
}
impl BindingSessions {
    pub(super) fn new(factory: &Arc<Domain>) -> Self {
        Self {
            factory: Arc::downgrade(factory),
            next_id: AtomicU64::new(1_u64 << 61),
        }
    }
    fn open(&self) -> Result<BindingRunner, String> {
        let factory = self
            .factory
            .upgrade()
            .ok_or("binding session factory is stopped")?;
        let mut session = factory
            .open_storage_session(self.next_id.fetch_add(2, Ordering::Relaxed))
            .map_err(|e| e.message)?;
        session.enable_privilege_bypass();
        // Go bindinfo's `kv.WithInternalSourceType(ctx, kv.InternalTxnBindInfo)`.
        session.set_internal_request_source(tidb_txnkv::INTERNAL_TXN_BIND_INFO);
        Ok(BindingRunner(super::SystemSqlExecutor::new(session)))
    }
}
impl tidb_bindinfo::cluster_binding_seam::BindingSessionPool for BindingSessions {
    fn load(&self, boundary: Option<&str>) -> Result<Vec<Vec<Datum>>, String> {
        let runner = self.open()?;
        let (condition, args) = match boundary {
            Some(boundary) => (
                "USE INDEX (time_index) WHERE update_time > %?",
                vec![tidb_sqlescape::SqlArg::from(boundary)],
            ),
            None => ("", Vec::new()),
        };
        tidb_bindinfo::binding_utils::exec_rows(&runner, &format!("SELECT {BINDING_STORAGE_COLUMNS} FROM mysql.bind_info {condition} ORDER BY update_time, create_time"), &args).map_err(|e| e.to_string())
    }
    fn gc(&self) -> Result<(), String> {
        let factory = self
            .factory
            .upgrade()
            .ok_or("binding session factory is stopped")?;
        let bindings = factory
            .binding_handle()
            .ok_or("binding cache is not installed")?;
        let writer = InternalBindingWriter {
            factory: self.factory.clone(),
            bindings: Arc::clone(bindings),
            connection_id: self.next_id.fetch_add(2, Ordering::Relaxed),
        };
        writer
            .execute(&mut |storage| tidb_bindinfo::binding_operator::gc_binding(storage).map(|()| 0))
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
    fn write_usage(&self, bindings: &[Arc<Binding>]) -> Result<(), String> {
        let mut runner = self.open()?;
        let mut snapshots = bindings
            .iter()
            .map(|binding| binding.usage_snapshot())
            .collect::<Vec<_>>();
        let result = tidb_bindinfo::binding_utils::update_binding_usage_info_to_storage(
            &mut runner,
            &mut snapshots,
            chrono::Utc::now(),
        );
        // A later batch failure must not erase successful earlier commits.
        for (binding, usage) in bindings.iter().zip(snapshots) {
            if let Some(saved) = usage.last_saved_at {
                binding.mark_usage_saved(saved);
            }
        }
        result.map(|_| ()).map_err(|e| e.to_string())
    }
}
/// A storage session of the node as Go bindinfo's pooled `sctx`.
pub(super) struct BindingRunner(pub(super) super::SystemSqlExecutor);

impl tidb_bindinfo::binding_operator::BindingOperatorSession for BindingRunner {
    fn now_timestamp(&self, offset_micros: i64) -> String {
        self.0.with_session(|session| session.binding_now_timestamp(offset_micros))
    }
}

impl InternalSqlRunner for BindingRunner {
    fn get_sql_executor(&self) -> &dyn tidb_sqlexec::SqlExecutor {
        &self.0
    }

    fn get_restricted_sql_executor(&self) -> &dyn tidb_sqlexec::RestrictedSqlExecutor {
        &self.0
    }

    fn affected_rows(&self) -> u64 {
        self.0.affected_rows()
    }
}

/// Go `Domain.InitBindingHandle`: `NewBindingHandle(do.sysSessionPool)`
/// -- the binding authority loads and maintains `mysql.bind_info` on
/// sessions the session package opens over `domain` -- stored as the
/// domain's handle, its first load, the owner campaign and the worker loop.
///
/// # Errors
///
/// The first load or the owner campaign failed, or the worker did not start.
pub fn init_binding_handle(
    domain: &Arc<Domain>,
    owner: Arc<dyn tidb_owner::Manager>,
) -> Result<tidb_bindinfo::cluster_binding_seam::BindingReloader, String> {
    let (bindings, reloader) = tidb_bindinfo::cluster_binding_seam::start_binding_cache(
        Arc::new(BindingSessions::new(domain)),
        Arc::clone(&domain.catalog),
        Arc::new(domain.global_vars.clone()),
        owner,
    )?;
    domain.init_binding_handle(bindings);
    Ok(reloader)
}
