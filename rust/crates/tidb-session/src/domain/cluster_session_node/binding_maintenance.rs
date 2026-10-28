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
use super::{Domain, InternalBindingWriter};
use crate::Session;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Weak,
};
use tidb_datatype::Datum;
use tidb_executor::DriverError;
use crate::binding::Binding;
use crate::binding_arm::GlobalBindingWriter;
use crate::binding_utils::{InternalSqlRunner, BINDING_STORAGE_COLUMNS};

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
        Ok(BindingRunner { session })
    }
}
impl crate::domain::cluster_binding_seam::BindingSessionPool for BindingSessions {
    fn load(&self, boundary: Option<&str>) -> Result<Vec<Vec<Datum>>, String> {
        let mut runner = self.open()?;
        let (condition, args) = match boundary {
            Some(boundary) => (
                "USE INDEX (time_index) WHERE update_time > %?",
                vec![Datum::new_string(boundary.to_owned())],
            ),
            None => ("", Vec::new()),
        };
        runner.exec_rows(&format!("SELECT {BINDING_STORAGE_COLUMNS} FROM mysql.bind_info {condition} ORDER BY update_time, create_time"), &args).map_err(|e| e.to_string())
    }
    fn gc(&self) -> Result<(), String> {
        let factory = self
            .factory
            .upgrade()
            .ok_or("binding session factory is stopped")?;
        let bindings = factory
            .bindings
            .as_ref()
            .ok_or("binding cache is not installed")?;
        let writer = InternalBindingWriter {
            factory: self.factory.clone(),
            bindings: Arc::clone(bindings),
            connection_id: self.next_id.fetch_add(2, Ordering::Relaxed),
        };
        writer
            .execute(&mut |session| session.gc_global_bindings().map(|()| 0))
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
    fn write_usage(&self, bindings: &[Arc<Binding>]) -> Result<(), String> {
        let mut runner = self.open()?;
        let mut snapshots = bindings
            .iter()
            .map(|binding| binding.usage_snapshot())
            .collect::<Vec<_>>();
        let result = crate::binding_utils::update_binding_usage_info_to_storage(
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
struct BindingRunner {
    session: Session,
}
fn driver_error(error: crate::domain::session_api::SqlQueryError) -> DriverError {
    DriverError::Mysql(
        tidb_executor::MysqlError::from_parts(error.code, error.state, error.message)
            .with_identity_opt(error.identity),
    )
}
impl BindingRunner {
    fn render(&self, sql: &str, args: &[Datum]) -> Result<String, DriverError> {
        tidb_executor::bind_parameters(
            &sql.replace("%?", "?"),
            args,
            tidb_parser::SqlMode::default(),
        )
    }
    fn rows(&mut self, sql: &str) -> Result<Vec<Vec<Datum>>, DriverError> {
        self.session.node_conn().execute_internal(sql).map_err(driver_error)
    }
}
impl InternalSqlRunner for BindingRunner {
    fn exec(&mut self, sql: &str, args: &[Datum]) -> Result<u64, DriverError> {
        let sql = self.render(sql, args)?;
        self.session.node_conn().execute_internal(&sql).map_err(driver_error)?;
        Ok(self.session.affected_rows())
    }
    fn exec_rows(&mut self, sql: &str, args: &[Datum]) -> Result<Vec<Vec<Datum>>, DriverError> {
        let sql = self.render(sql, args)?;
        self.rows(&sql)
    }
}

/// Go `NewBindingHandle`'s `sPool`: the binding authority runs its
/// maintenance on sessions the session package opens over `domain`.
pub fn attach_binding_sessions(
    domain: &Arc<Domain>,
    bindings: &dyn crate::domain::cluster_binding_seam::ClusterBindings,
) {
    bindings.attach_session_pool(Arc::new(BindingSessions::new(domain)));
}
