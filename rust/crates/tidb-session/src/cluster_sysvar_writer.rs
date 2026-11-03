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

//! Go `session.SetGlobalSysVar`'s cache side: the copy of the node's sysvar
//! cache an assignment is validated against and runs its `SetGlobal` hooks
//! on, and Go `Domain.NotifyUpdateSysVarCache` once the session has written
//! `mysql.global_variables`. The domain holds it as its `ClusterSysvarWriter`.

use std::sync::Arc;
use std::time::Duration;

use tidb_domain::global_sysvars::GlobalSysvars;
use tidb_domain_core::cluster_sysvar_seam::*;
use tidb_infoschema::cluster_catalog::load_info_schema;
use tidb_infoschema_issyncer::real_tikv_catalog::TransactionMetaSnapshot;
use tidb_meta::transaction::Mutator;
use tidb_pd_client::EtcdClient;
use tidb_store_driver_txn::meta_txn::{MetaTxnError, MetaTxnStorage};
use tidb_sqlescape::SqlArg;
use tidb_store_driver_error::sql_query_error::SqlQueryError;
use tidb_store_driver_txn::transaction::{RealOptimisticTransactionOpener, StoreClient};

pub struct RealClusterSysvarWriter<S = tikv_client::PdRpcClient>
where
    S: StoreClient,
{
    /// The node's LIVE sysvar cache -- the one every session shares a clone
    /// of.
    live: GlobalSysvars,
    /// The etcd client this node announces sysvar changes through, so peers'
    /// sysvar watches fire promptly. `None` leaves them to their reload tick;
    /// a failed announcement is a warning, never a failed statement.
    notifier: Option<Arc<EtcdClient>>,
    publication_fence: SysvarPublicationFence,
    _store: std::marker::PhantomData<fn() -> S>,
}

impl<S> RealClusterSysvarWriter<S>
where
    S: StoreClient,
{
    /// Binds the writer to an already-connected authority and the live
    /// sysvar cache a change publishes into; registers Go's
    /// `variable.SwitchMDL` (`d.SwitchMDL`) as `tidb_enable_metadata_lock`'s
    /// `SetGlobal` hook.
    #[must_use]
    pub fn new(
        opener: Arc<RealOptimisticTransactionOpener<S>>,
        live: GlobalSysvars,
        timeout: Duration,
        notifier: Option<Arc<EtcdClient>>,
        publication_fence: SysvarPublicationFence,
    ) -> Self {
        let switch_opener = opener;
        live.register_set_global_hook(
            tidb_vardef::tidb_vars::TIDB_ENABLE_MDL,
            Arc::new(move |value: &str| {
                // Go `TiDBEnableMDL`'s `SetGlobal`.
                let enable = tidb_vardef::option_values::tidb_opt_on(value);
                if tidb_vardef::is_mdl_enabled(false) != enable {
                    switch_mdl(&switch_opener, timeout, enable)?;
                }
                Ok(())
            }),
        );
        Self {
            live,
            notifier,
            publication_fence,
            _store: std::marker::PhantomData,
        }
    }
}

/// Go `ddl.SwitchMDL`: refused while a DDL job is queued; otherwise the
/// process flag flips and the meta key follows.
fn switch_mdl<S: StoreClient>(
    opener: &RealOptimisticTransactionOpener<S>,
    timeout: Duration,
    enable: bool,
) -> Result<(), String> {
    if tidb_vardef::is_mdl_enabled(false) == enable {
        return Ok(());
    }
    // Check if there is any DDL running (Go `select 1 from
    // mysql.tidb_ddl_job`).
    let running = {
        let mut transaction = opener.begin_read_only().map_err(|error| error.to_string())?;
        let running = {
            let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
            let catalog = load_info_schema(&mut snapshot).map_err(|error| error.to_string())?;
            tidb_ddl_systable::ddl_job_table::DdlJobTable::locate(&catalog)
                .and_then(|table| table.has_jobs(&mut snapshot))
                .map_err(|error| error.to_string())?
        };
        transaction
            .finish_without_writes()
            .map_err(|error| error.to_string())?;
        running
    };
    if running {
        return Err("please wait for all jobs done".to_owned());
    }
    tidb_vardef::set_enable_mdl(enable);
    let mut storage = MetaTxnStorage::new(opener, timeout);
    let context = tidb_txnkv::RunInNewTxnContext {
        request_source: Some(tidb_txnkv::RequestSource {
            internal: true,
            source_type: tidb_txnkv::INTERNAL_TXN_DDL.to_owned(),
            explicit_source_type: String::new(),
        }),
    };
    let written = tidb_txnkv::run_in_new_txn(&context, &mut storage, true, |transaction| {
        let mutator = Mutator::new(&mut *transaction);
        let old_enable = mutator.metadata_lock().map_err(MetaTxnError::from)?.unwrap_or(false);
        if old_enable != enable {
            mutator.set_metadata_lock(enable).map_err(MetaTxnError::from)?;
        }
        Ok(())
    });
    if let Err(error) = written {
        tidb_logutil::bg_logger().warn(
            "switch metadata lock feature",
            &[
                tidb_log::Field::new("enable", tidb_log::Value::Bool(enable)),
                tidb_logutil::zap_error(&error),
            ],
        );
        return Err(error.to_string());
    }
    tidb_logutil::bg_logger().info(
        "switch metadata lock feature",
        &[tidb_log::Field::new("enable", tidb_log::Value::Bool(enable))],
    );
    Ok(())
}

impl<S> ClusterSysvarWriter for RealClusterSysvarWriter<S>
where
    S: StoreClient,
{
    fn begin(&self) -> Result<Box<dyn PendingSysvarChange>, String> {
        let initial = self.live.overrides();
        // The assignment runs the node's registered `SetGlobal` hooks, as
        // Go's registry is process-wide, against a copy of the cache.
        let scratch =
            GlobalSysvars::from_cluster_rows(initial.clone()).sharing_set_global_hooks(&self.live);
        Ok(Box::new(RealPendingSysvarChange {
            initial,
            scratch,
            live: self.live.clone(),
            notifier: self.notifier.clone(),
            publication_fence: self.publication_fence.clone(),
        }))
    }
}

struct RealPendingSysvarChange {
    /// The cache's overrides the scratch copy started from.
    initial: std::collections::HashMap<String, String>,
    scratch: GlobalSysvars,
    live: GlobalSysvars,
    notifier: Option<Arc<EtcdClient>>,
    publication_fence: SysvarPublicationFence,
}

impl PendingSysvarChange for RealPendingSysvarChange {
    fn table(&self) -> GlobalSysvars {
        self.scratch.clone()
    }

    fn commit(
        self: Box<Self>,
        name: &str,
        stored: &str,
        pool: &dyn tidb_stats_handle_storage::stats_read_writer::StatsSessionPool,
    ) -> Result<(), SqlQueryError> {
        let changed = self.changed_globals(name, stored);
        for (variable, value) in &changed {
            // Go `replaceGlobalVariablesTableValue`.
            pool.call_with_sctx(
                &mut |sctx| {
                    tidb_stats_handle_util::exec_rows(
                        sctx,
                        "REPLACE INTO %n.%n (variable_name, variable_value) VALUES (%?, %?)",
                        &[
                            SqlArg::from("mysql"),
                            SqlArg::from("global_variables"),
                            SqlArg::from(variable.as_str()),
                            SqlArg::from(value.as_str()),
                        ],
                    )
                    .map(|_| ())
                },
                &[],
            )?;
        }
        let names = changed.into_iter().map(|(variable, _)| variable).collect::<Vec<_>>();
        self.notify_update_sys_var_cache(&names, pool);
        Ok(())
    }
}

impl RealPendingSysvarChange {
    /// The rows the assignment of `name` writes: the variables its hooks
    /// moved besides `name` (Go's aliases), then `name` itself.
    fn changed_globals(&self, name: &str, stored: &str) -> Vec<(String, String)> {
        let current = self.scratch.overrides();
        let mut names: Vec<&String> = current
            .keys()
            .chain(self.initial.keys())
            .filter(|other| !other.eq_ignore_ascii_case(name))
            .filter(|other| current.get(*other) != self.initial.get(*other))
            .collect();
        names.sort();
        names.dedup();
        let mut changed: Vec<(String, String)> = names
            .into_iter()
            .filter_map(|other| {
                self.scratch
                    .get(other)
                    .ok()
                    .map(|value| (other.clone(), value))
            })
            .collect();
        let canonical = tidb_variable::sysvar::get_sys_var(name)
            .map_or_else(|| name.to_ascii_lowercase(), |def| def.name.to_owned());
        changed.push((canonical, stored.to_owned()));
        changed
    }

    /// Go `NotifyUpdateSysVarCache(true)`: the cache is rebuilt from the
    /// stored rows, then peers are told. A failed rebuild is logged; the
    /// rows are durable either way.
    fn notify_update_sys_var_cache(
        self: Box<Self>,
        changed: &[String],
        pool: &dyn tidb_stats_handle_storage::stats_read_writer::StatsSessionPool,
    ) {
        // Go `rebuildSysVarCache(nil)`: the rows through the domain's system
        // session pool (`fetchTableValues`).
        let refresh_error = self.publication_fence.publish_local_after_commit_with_read(
            &self.live,
            &self.scratch,
            changed,
            None,
            || tidb_domain_core::cluster_sysvar_seam::fetch_table_values(pool),
        );
        if let Some(error) = refresh_error {
            tidb_logutil::bg_logger()
                .error("rebuilding sysvar cache failed", &[tidb_logutil::zap_error(error)]);
        }
        notify_sysvar_update(self.notifier.as_deref(), changed);
    }
}
