// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `crossks/ddl_submit.go`: enqueue in the target store without a DDL owner.

use std::sync::Arc;
use std::time::Duration;
use tidb_ddl_build::session_context::DdlSessionContext as _;
use tidb_domain_core::sys_session::DxfSessionPool;
use tidb_gocontext::Context;
use tidb_infoschema_issyncer::real_tikv_catalog::TransactionMetaSnapshot;
use tidb_model::{AlterTableModeTarget, Job, JobArgsValue};
use tidb_store_driver_txn::transaction::{RealOptimisticTransactionOpener, StoreClient};

/// Go `ddlClient` and its `jobsubmit.SubmitOptions` dependencies.
pub struct DdlClient<S: StoreClient> {
    pub(crate) opener: RealOptimisticTransactionOpener<S>,
    pub(crate) meta_store: Arc<dyn tidb_meta::MetaStore>,
    pub(crate) pool: Arc<DxfSessionPool>,
    pub(crate) server_state: Arc<dyn tidb_ddl_serverstate::Syncer>,
    pub(crate) min_job_id: Arc<tidb_ddl_systable::MinJobIdRefresher>,
    pub(crate) catalog: Arc<tidb_infoschema_issyncer::catalog_watch::InfoCache>,
    pub(crate) etcd: Option<Arc<tidb_pd_client::EtcdClient>>,
    /// Go `SubmitOptions.SessPool`: the target's DDL session pool.
    pub(crate) ddl_pool: Arc<crate::ddl_session_pool::CrossKsDdlSessionPool>,
}

impl<S: StoreClient> DdlClient<S> {
    /// Go `ddlClient.resolveAlterTableModeTarget`: fresh transactional metadata.
    pub fn resolve_alter_table_mode_target(
        &self,
        ctx: &Context,
        req: &AlterTableModeTarget,
    ) -> Result<AlterTableModeTarget, String> {
        let mut database = None;
        let mut table = None;
        self.meta_store
            .run_in_new_txn(ctx, tidb_txnkv::INTERNAL_TXN_DDL, false, &mut |txn| {
                let reader = tidb_meta::transaction::Mutator::new(txn);
                database = reader.database(req.schema_id)?;
                if database.is_some() {
                    table = reader.table(req.schema_id, req.table_id)?;
                }
                Ok(())
            })?;
        let database = database.ok_or_else(|| {
            tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS
                .gen_with_stack_by_args(&[format!("(Schema ID {})", req.schema_id).into()])
                .to_string()
        })?;
        let table = table.ok_or_else(|| {
            tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                .gen_with_stack_by_args(&[
                    format!("(Schema ID {})", database.id).into(),
                    format!("(Table ID {})", req.table_id).into(),
                ])
                .to_string()
        })?;
        if req.schema_name.lowercase() != database.name.lowercase() {
            return Err(format!(
                "expected schema name {} does not match target schema name {}",
                req.schema_name.original(),
                database.name.original()
            ));
        }
        if req.table_name.lowercase() != table.name.lowercase() {
            return Err(format!(
                "expected table name {} does not match target table name {}",
                req.table_name.original(),
                table.name.original()
            ));
        }
        let mut target = req.clone();
        target.current_mode = table.mode;
        Ok(target)
    }

    /// Go `ddlClient.buildAlterTableModeJob`: use one target pool session's vars.
    fn build_alter_table_mode_job(
        &self,
        target: &AlterTableModeTarget,
    ) -> Result<Option<tidb_ddl_jobsubmit::JobSpec>, String> {
        let session = self.pool.get().map_err(|error| error.to_string())?;
        let result = session.with_session_context(|context| -> tidb_syssession::Result<_> {
            let context = (context as &dyn std::any::Any)
                .downcast_ref::<super::cluster_session_node::ClusterStatsSessionContext>()
                .expect("cross-KS pool contains native sessions");
            let vars =
                tidb_domain_core::sys_session::DomainSysSession::ddl_statement_context(context)
                    .map_err(tidb_syssession::SysSessionError::new)?;
            tidb_ddl_jobsubmit::build_alter_table_mode_job(
                vars.ddl_cdc_write_source(),
                vars.ddl_sql_mode(),
                target,
            )
            .map(|job| {
                job.map(|(job, args)| tidb_ddl_jobsubmit::JobSpec {
                    job,
                    args: JobArgsValue::AlterTableMode(Some(args)),
                    id_allocated: false,
                })
            })
            .map_err(|error| tidb_syssession::SysSessionError::new(error.to_string()))
        });
        self.pool.put(&session);
        result.map_err(|error| error.to_string())
    }

    /// Go `ddlClient.refreshServerState`: no watch or session is initialized.
    pub fn refresh_server_state(&self, ctx: &Context) -> Result<(), String> {
        if let Some(error) = ctx.err() {
            return Err(error.to_string());
        }
        self.server_state
            .get_global_state(&tidb_timeutil::SleepContext::background())
            .map_err(|error| error.to_string())?;
        if let Some(error) = ctx.err() {
            return Err(error.to_string());
        }
        Ok(())
    }

    /// Go `ddlClient.alterTableMode`: resolve, build, refresh state, submit, wait.
    pub fn alter_table_mode(
        &self,
        ctx: &Context,
        req: &AlterTableModeTarget,
    ) -> Result<(), String> {
        let target = self.resolve_alter_table_mode_target(ctx, req)?;
        let Some(spec) = self.build_alter_table_mode_job(&target)? else {
            return Ok(());
        };
        self.refresh_server_state(ctx)?;
        let mut specs = [spec];
        self.submit_batch(ctx, &mut specs)?;
        if let Some(etcd) = &self.etcd {
            // Go NotifyDDLOwnerByEtcd is best effort; history remains authoritative.
            let _ = etcd.put(b"/tidb/ddl/add_ddl_job_general", b"0");
        }
        self.wait_ddl_finished(ctx, specs[0].job.id)
    }

    /// Go `jobsubmit.SubmitBatch` without a submit loop or owner campaign.
    fn submit_batch(
        &self,
        ctx: &Context,
        specs: &mut [tidb_ddl_jobsubmit::JobSpec],
    ) -> Result<(), String> {
        if let Some(error) = ctx.err() {
            return Err(error.to_string());
        }
        // Go `SubmitBatch`: the flashback check precedes the start-TS read.
        tidb_ddl::real_tikv_ddl::check_flashback_cluster_job(
            self.ddl_pool.as_ref(),
            self.min_job_id.get_curr_min_job_id(),
        )
        .map_err(|error| error.to_string())?;
        let mut txn = tidb_txnkv::kv_storage::Storage::begin_read_only(&self.opener)
            .map_err(|error| error.to_string())?;
        let start_ts = txn.start_ts();
        let mut snapshot =
            TransactionMetaSnapshot::new(txn.as_mut(), tikv_client::tikv::READ_TIMEOUT_SHORT);
        tidb_ddl_jobsubmit::prepare_submit_batch(
            &mut snapshot,
            specs,
            start_ts,
            self.server_state.is_upgrading_state(),
        )
        .map_err(|error| error.to_string())?;
        txn.finish_without_writes()
            .map_err(|error| error.to_string())?;
        if let Some(error) = ctx.err() {
            return Err(error.to_string());
        }
        tidb_ddl::real_tikv_ddl::gen_gid_and_insert_jobs_with_retry(
            Arc::new(self.opener.clone()),
            self.ddl_pool.as_ref(),
            specs,
            tikv_client::tikv::READ_TIMEOUT_SHORT,
            |_| None::<fn()>,
        )
        .map_err(|error| error.to_string())
    }

    /// Go `ddlClient.getHistoryJob`.
    fn get_history_job(&self, ctx: &Context, id: i64) -> Result<Option<Job>, String> {
        let mut job = None;
        self.meta_store
            .run_in_new_txn(ctx, tidb_txnkv::INTERNAL_TXN_DDL, false, &mut |txn| {
                job = tidb_meta::transaction::Mutator::new(txn).get_history_ddl_job::<Job>(id)?;
                Ok(())
            })?;
        Ok(job)
    }

    /// Go `ddlClient.waitDDLFinished`: a 100ms poll, retrying transient reads.
    pub fn wait_ddl_finished(&self, ctx: &Context, id: i64) -> Result<(), String> {
        loop {
            match ctx.done().recv_timeout(Duration::from_millis(100)) {
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                _ => return Err(ctx.err().expect("canceled context").to_string()),
            }
            if let Ok(Some(job)) = self.get_history_job(ctx, id) {
                if job.is_synced() {
                    return Ok(());
                }
                if let Some(error) = job.error {
                    return Err(error.read().to_string());
                }
                return Err(format!(
                    "target DDL job {id} finished in unexpected state {}",
                    job.state
                ));
            }
        }
    }
}
