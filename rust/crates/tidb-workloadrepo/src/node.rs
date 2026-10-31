// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `SetupRepository(dom)` on a node: the repository's system sessions
//! (`dom.SysSessionPool()`), its owner over the node's etcd client, the
//! sysvars it registers (`variable.RegisterSysVar` in `worker.go`'s `init`),
//! and Go `executor.TakeSnapshot`. The server's main runs it.

use std::sync::Arc;

use tidb_session::domain::cluster_session_node::DomainSessions as _;
use tidb_domain_core::domain::Domain;
use tidb_session::domain::session_api::SessionContext;
use tidb_domain::global_sysvars::GlobalSysvars;
use tidb_session::Session;

use crate::{OwnerFactory, RepositoryStore, Worker};

/// Go `SetupRepository(dom)`: the node's workload repository, its sysvar
/// hooks registered and Go `executor.TakeSnapshot` installed, started when
/// `tidb_workload_repository_dest` is `table`.
pub fn setup_repository(factory: &Arc<Domain>) -> Arc<Worker> {
    let etcd = factory.etcd_client();
    let store = etcd
        .as_ref()
        .map(|client| Arc::clone(client) as Arc<dyn RepositoryStore>);
    let instance_id = factory
        .server_info()
        .map(|server_info| server_info.local_server_info().static_info.id.clone())
        .unwrap_or_default();
    let owner_factory = etcd.as_ref().map(|client| {
        let client = Arc::clone(client);
        let instance_id = instance_id.clone();
        Arc::new(move |key: &str, prompt: &str| {
            Arc::new(tidb_owner::OwnerManager::new(
                tidb_owner::Context::background(),
                Arc::clone(&client) as Arc<dyn tidb_owner::OwnerStore>,
                prompt,
                instance_id.clone(),
                key,
            )) as Arc<dyn tidb_owner::Manager>
        }) as OwnerFactory
    });
    let worker = Worker::new(
        store,
        Some(Arc::new(WorkloadRepositorySessionPool::new(factory))),
        owner_factory,
        instance_id,
    );
    let globals = factory.global_vars();
    register_sysvar_hooks(&globals, &worker);
    for (name, apply) in [
        (
            crate::REPOSITORY_SAMPLING_INTERVAL,
            Worker::set_sampling_interval as fn(&Worker, &str) -> Result<(), String>,
        ),
        (crate::REPOSITORY_SNAPSHOT_INTERVAL, Worker::set_snapshot_interval),
        (crate::REPOSITORY_RETENTION_DAYS, Worker::set_retention_days),
    ] {
        if let Ok(value) = globals.get(name) {
            let _ = apply(&worker, &value);
        }
    }
    let snapshot_worker = Arc::clone(&worker);
    assert!(
        factory
            .set_take_snapshot(Arc::new(move || snapshot_worker.take_snapshot()))
            .is_ok(),
        "workload repository is installed once"
    );
    if globals
        .get(crate::REPOSITORY_DEST)
        .is_ok_and(|value| value == "table")
        && worker.start().is_err()
    {
        worker.stop();
    }
    worker
}

/// `worker.go`'s `init`: the four sysvars whose `SetGlobal` reconfigures the
/// worker.
pub fn register_sysvar_hooks(globals: &GlobalSysvars, worker: &Arc<Worker>) {
    let hook = |apply: fn(&Arc<Worker>, &str) -> Result<(), String>| {
        let worker = Arc::clone(worker);
        Arc::new(move |value: &str| apply(&worker, value)) as tidb_domain::global_sysvars::SetGlobalHook
    };
    globals.register_set_global_hook(
        crate::REPOSITORY_DEST,
        hook(|worker, value| worker.set_repository_dest(value)),
    );
    globals.register_set_global_hook(
        crate::REPOSITORY_RETENTION_DAYS,
        hook(|worker, value| worker.set_retention_days(value)),
    );
    globals.register_set_global_hook(
        crate::REPOSITORY_SAMPLING_INTERVAL,
        hook(|worker, value| worker.set_sampling_interval(value)),
    );
    globals.register_set_global_hook(
        crate::REPOSITORY_SNAPSHOT_INTERVAL,
        hook(|worker, value| worker.set_snapshot_interval(value)),
    );
}

/// Go `dom.SysSessionPool()` as the repository draws its system sessions.
pub struct WorkloadRepositorySessionPool {
    factory: std::sync::Weak<Domain>,
    next_connection_id: std::sync::atomic::AtomicU64,
}

impl WorkloadRepositorySessionPool {
    /// A pool over `factory`, holding only a weak reference back to it.
    pub fn new(factory: &Arc<Domain>) -> Self {
        Self {
            factory: Arc::downgrade(factory),
            next_connection_id: std::sync::atomic::AtomicU64::new(1_u64 << 62),
        }
    }
}

impl crate::SessionPool for WorkloadRepositorySessionPool {
    fn get(&self) -> Result<Box<dyn crate::RepositorySession>, String> {
        use std::sync::atomic::Ordering;

        let factory = self
            .factory
            .upgrade()
            .ok_or_else(|| "cluster session factory is stopped".to_owned())?;
        let connection_id = self.next_connection_id.fetch_add(1, Ordering::Relaxed);
        let session = factory
            .open_session(SessionContext {
                connection_id,
                peer_addr: tidb_session::domain::peer_address::PeerAddress::Tcp(
                "127.0.0.1:0".parse().expect("loopback socket address"),
            ),
                identity: tidb_session::domain::configured_user_store::AuthenticatedIdentity::internal(),
                client_found_rows: false,
                tls_state: None,
                compression_algorithm: tidb_mysql::consts::CompressionNone,
                compression_level: 0,
                cancellation: tidb_session::domain::session_api::ConnectionCancellation::default(),
                close: tidb_session::domain::session_api::ConnectionClose::default(),
            })
            .map_err(|error| error.message)?;
        Ok(Box::new(WorkloadRepositorySession { session }))
    }
}

/// One system session the repository runs its SQL on.
pub struct WorkloadRepositorySession {
    session: Session,
}

fn workload_sql_literal(value: &crate::SqlArg) -> String {
    match value {
        crate::SqlArg::Null => "NULL".to_owned(),
        crate::SqlArg::UInt(value) => value.to_string(),
        crate::SqlArg::String(value) => {
            format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
        }
    }
}

fn bind_workload_sql(sql: &str, args: &[crate::SqlArg]) -> Result<String, String> {
    let mut rendered = sql.to_owned();
    for argument in args {
        let Some(index) = rendered.find("%?") else {
            return Err("too many workload repository SQL arguments".to_owned());
        };
        rendered.replace_range(index..index + 2, &workload_sql_literal(argument));
    }
    if rendered.contains("%?") {
        return Err("not enough workload repository SQL arguments".to_owned());
    }
    Ok(rendered)
}

fn workload_arg_from_datum(
    value: tidb_datatype::Datum,
) -> Result<crate::SqlArg, String> {
    match value {
        tidb_datatype::Datum::Null => Ok(crate::SqlArg::Null),
        tidb_datatype::Datum::UInt(value) => Ok(crate::SqlArg::UInt(value)),
        tidb_datatype::Datum::Int(value) => u64::try_from(value)
            .map(crate::SqlArg::UInt)
            .map_err(|_| "negative integer in workload repository metadata".to_owned()),
        tidb_datatype::Datum::Bytes(value) | tidb_datatype::Datum::Raw(value) => {
            String::from_utf8(value)
                .map(crate::SqlArg::String)
                .map_err(|error| error.to_string())
        }
        tidb_datatype::Datum::String(value) => value
            .as_utf8()
            .map(|value| crate::SqlArg::String(value.to_owned()))
            .map_err(|error| error.to_string()),
        other => Err(format!(
            "unsupported workload repository result value {other:?}"
        )),
    }
}

impl crate::RepositorySession for WorkloadRepositorySession {
    fn execute(
        &mut self,
        sql: &str,
        args: &[crate::SqlArg],
    ) -> Result<Vec<Vec<crate::SqlArg>>, String> {
        let sql = bind_workload_sql(sql, args)?;
        self.session
            .node_conn()
            .execute_internal(&sql)
            .map_err(|error| error.message)?
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(workload_arg_from_datum)
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect()
    }

    fn schema_exists(&self, schema: &str) -> bool {
        self.session
            .shared_catalog()
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .has_database(schema)
    }

    fn table_info(
        &mut self,
        schema: &str,
        table: &str,
    ) -> Result<crate::TableInfo, String> {
        let catalog = self.session.shared_catalog();
        let catalog = catalog.lock().unwrap_or_else(|error| error.into_inner());
        let entry = catalog
            .table_in(schema, table)
            .ok_or_else(|| format!("table `{schema}`.`{table}` does not exist"))?;
        let columns = match entry {
            tidb_infoschema::TableEntry::Kv(table) => table
                .visible_columns()
                .into_iter()
                .map(|(_, column)| crate::Column {
                    name: column.name.clone(),
                    type_desc: column
                        .field_type
                        .type_desc(tidb_datatype::STRICT_INTEGER_DISPLAY_WIDTH),
                    comment: column.comment.clone(),
                })
                .collect(),
            _ => entry
                .column_types()
                .into_iter()
                .map(|(name, field_type)| crate::Column {
                    name,
                    type_desc: field_type.type_desc(tidb_datatype::STRICT_INTEGER_DISPLAY_WIDTH),
                    comment: String::new(),
                })
                .collect(),
        };
        let partitions = match entry {
            tidb_infoschema::TableEntry::Kv(table) => table
                .partition()
                .map(|partition| {
                    partition
                        .definitions
                        .iter()
                        .map(|definition| definition.name.clone())
                        .collect()
                })
                .unwrap_or_default(),
            _ => Vec::new(),
        };
        Ok(crate::TableInfo {
            columns,
            partitions,
        })
    }
}
