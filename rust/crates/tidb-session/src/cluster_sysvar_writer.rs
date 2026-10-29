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

//! Go `session.SetGlobalSysVar` over the cluster: the writer that commits a
//! `SET GLOBAL` into `mysql.global_variables` and hands it to the domain's
//! publication fence. The domain holds it as its `ClusterSysvarWriter`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;
use tidb_txnkv::transaction::StoreClient;

use tidb_infoschema::cluster_catalog::{load_cluster_catalog, ClusterCatalog};
use tidb_syssession::cluster_sysvar_load::load_cluster_sysvars;
use tidb_syssession::cluster_sysvar_write::plan_sysvar_write;
use tidb_datatype::current_time::utc_now_timestamp;
use tidb_infoschema_issyncer::real_tikv_catalog::TransactionMetaSnapshot;
use tidb_pd_client::EtcdClient;
use tidb_domain::global_sysvars::GlobalSysvars;
use tidb_txnkv::rpc::UnaryCallContext;
use tidb_txnkv::transaction::{RealOptimisticTransactionOpener, RealTransaction};

use tidb_store_driver::pessimistic_lock_error::cluster_commit_error;
use tidb_store_driver::sql_query_error::SqlQueryError;
use tidb_domain_core::cluster_sysvar_seam::*;

/// The production sysvar writer: one real transaction per statement, the
/// optimistic 2PC, then the live table and the etcd announcement.
pub struct RealClusterSysvarWriter<S = tikv_client::PdRpcClient>
where
    S: StoreClient,
{
    opener: Arc<RealOptimisticTransactionOpener<S>>,
    /// The node's LIVE sysvar table -- the one every session shares a clone
    /// of. Published into only after a commit.
    live: GlobalSysvars,
    timeout: Duration,
    /// The etcd client this node announces sysvar changes through, so peers'
    /// sysvar watches fire promptly. `None` leaves them to their reload tick;
    /// a failed announcement is a warning, never a failed statement.
    notifier: Option<Arc<EtcdClient>>,
    publication_fence: SysvarPublicationFence,
}

impl<S> RealClusterSysvarWriter<S>
where
    S: StoreClient,
{
    /// Binds the writer to an already-connected authority and the live
    /// sysvar table a successful change publishes into.
    #[must_use]
    pub fn new(
        opener: Arc<RealOptimisticTransactionOpener<S>>,
        live: GlobalSysvars,
        timeout: Duration,
        notifier: Option<Arc<EtcdClient>>,
        publication_fence: SysvarPublicationFence,
    ) -> Self {
        Self {
            opener,
            live,
            timeout,
            notifier,
            publication_fence,
        }
    }
}

impl<S> ClusterSysvarWriter for RealClusterSysvarWriter<S>
where
    S: StoreClient,
{
    fn begin(&self) -> Result<Box<dyn PendingSysvarChange>, String> {
        let mut transaction = self.opener.begin().map_err(|error| error.to_string())?;
        let (catalog, scratch) = {
            let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, self.timeout);
            let catalog = load_cluster_catalog(&mut snapshot).map_err(|error| error.to_string())?;
            let loaded =
                load_cluster_sysvars(&mut snapshot, &catalog).map_err(|error| error.to_string())?;
            // The statement's assignments run the node's registered
            // `SetGlobal` hooks, as Go's registry is process-wide.
            let scratch =
                GlobalSysvars::from_cluster_rows(loaded).sharing_set_global_hooks(&self.live);
            (catalog, scratch)
        };
        Ok(Box::new(RealPendingSysvarChange {
            transaction: Some(transaction),
            catalog,
            scratch,
            live: self.live.clone(),
            timeout: self.timeout,
            notifier: self.notifier.clone(),
            publication_fence: self.publication_fence.clone(),
            reload_opener: Arc::clone(&self.opener),
        }))
    }
}

struct RealPendingSysvarChange<S>
where
    S: StoreClient,
{
    /// `None` only after [`PendingSysvarChange::commit`] has taken it.
    transaction: Option<RealTransaction<S>>,
    catalog: ClusterCatalog,
    scratch: GlobalSysvars,
    live: GlobalSysvars,
    timeout: Duration,
    notifier: Option<Arc<EtcdClient>>,
    publication_fence: SysvarPublicationFence,
    reload_opener: Arc<RealOptimisticTransactionOpener<S>>,
}

impl<S> PendingSysvarChange for RealPendingSysvarChange<S>
where
    S: StoreClient,
{
    fn table(&self) -> GlobalSysvars {
        self.scratch.clone()
    }

    fn commit(mut self: Box<Self>) -> Result<Vec<String>, SqlQueryError> {
        let mut transaction = self
            .transaction
            .take()
            .ok_or_else(|| SqlQueryError::unknown("the sysvar change was already committed"))?;
        // The scratch table's current overrides ARE the desired image: it was
        // seeded from this same snapshot's own stored rows and then the
        // statement ran against it, so a name the statement reset to DEFAULT
        // is simply absent here, and `plan_sysvar_write`'s whole-image diff
        // turns that absence into a delete of its stored row.
        let desired: std::collections::BTreeMap<String, String> =
            self.scratch.overrides().into_iter().collect();
        let mut plan = {
            let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, self.timeout);
            plan_sysvar_write(
                &mut snapshot,
                &self.catalog,
                &desired,
                utc_now_timestamp(),
                &mut |db, table, minimum, count| {
                    tidb_executor::cluster_account_seam::reserve_system_row_ids(
                        &self.reload_opener,
                        self.timeout,
                        db,
                        table,
                        minimum,
                        count,
                    )
                },
            )
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?
        };
        // Go `TiDBEnableMDL`'s `SetGlobal` hook runs `SwitchMDL`
        // (`pkg/ddl/ddl.go:1245`): it REFUSES while any DDL job is running and,
        // when it proceeds, persists the switch to the `metadataLock` meta key
        // in the same transaction. `SET GLOBAL tidb_enable_metadata_lock` must
        // do both here; without the meta write a restarting peer would
        // re-initialise the flag from the stale meta value, and without the
        // guard a node could split the cluster's DDL-ack keys mid-job.
        if plan
            .changed
            .iter()
            .any(|name| name.eq_ignore_ascii_case(tidb_vardef::tidb_vars::TIDB_ENABLE_MDL))
        {
            let enable = desired
                .get(tidb_vardef::tidb_vars::TIDB_ENABLE_MDL)
                .map_or(tidb_vardef::defaults::DEF_TIDB_ENABLE_MDL, |value| {
                    value.eq_ignore_ascii_case("ON") || value == "1"
                });
            // `SwitchMDL`'s first line: a value the process already runs
            // with is not a switch, so neither the job probe nor the meta
            // write happens.
            if enable != tidb_vardef::is_mdl_enabled(false) {
                let running = {
                    let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, self.timeout);
                    tidb_ddl::ddl_job_table::DdlJobTable::locate(&self.catalog)
                        .and_then(|table| table.has_jobs(&mut snapshot))
                        .map_err(|error| SqlQueryError::unknown(error.to_string()))?
                };
                if running {
                    // Go's exact refusal (`ddl.go:1267`).
                    return Err(SqlQueryError::unknown(
                        "please wait for all jobs done".to_owned(),
                    ));
                }
                plan.mutations.push(
                    tidb_syssession::cluster_sysvar_write::metadata_lock_mutation(enable)
                        .map_err(|error| SqlQueryError::unknown(error.to_string()))?,
                );
            }
        }
        let plan_is_empty = plan.is_empty();
        let changed = plan.changed;
        let commit_ts = if plan_is_empty {
            transaction
                .finish_without_writes()
                .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
            None
        } else {
            let call = UnaryCallContext::with_timeout(self.timeout);
            let outcome = transaction
                .commit(plan.mutations, &call)
                .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
            if let Some(error) = cluster_commit_error(&outcome, "sysvar change") {
                return Err(error);
            }
            Some(outcome.receipt().commit_ts)
        };
        // Rebuild from durable cluster truth inside the same publication
        // boundary the periodic reloader uses. Two local commits may finish
        // in one order and reach this point in another; the fresh image makes
        // either arrival publish the latest cluster state. This reread is a
        // cache refresh after a confirmed commit, so its failure is warned
        // and never changes the client's successful SQL outcome.
        let refresh_error = self.publication_fence.publish_local_after_commit_with_read(
            &self.live,
            &self.scratch,
            &changed,
            commit_ts,
            || {
                tidb_domain_core::real_tikv_privileges::load_sysvars_from_cluster(
                    &self.reload_opener,
                    self.timeout,
                )
                .map_err(|error| error.to_string())
            },
        );
        if let Some(error) = refresh_error {
            // Go `NotifyUpdateSysVarCache`'s local rebuild failure.
            tidb_util::logutil::bg_logger()
                .error("rebuilding sysvar cache failed", &[tidb_util::logutil::zap_error(error)]);
        }
        notify_sysvar_update(self.notifier.as_deref(), &changed);
        Ok(changed)
    }
}
