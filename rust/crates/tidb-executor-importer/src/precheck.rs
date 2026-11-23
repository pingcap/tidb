//! Go `pkg/executor/importer/precheck.go`: the requirements an import
//! checks before it submits its task.

use crate::DriverError;
use tidb_domain_serverinfo::serverinfo_syncer::EtcdOps;
use tidb_sqlexec::{ExecutionContext, SqlExecutor};

use super::LoadDataController;

fn pre_check_failed(reason: &str) -> DriverError {
    DriverError::from(tidb_exeerrors::ERR_LOAD_DATA_PRE_CHECK_FAILED.fast_gen_by_args(&[reason.into()]))
}

/// What Go's prechecks reach through the session: its SQL executor and,
/// for the PiTR and changefeed check, the cluster's etcd.
pub struct PrecheckSession<'a> {
    /// The statement context the internal SQL runs under.
    pub ctx: &'a dyn ExecutionContext,
    /// Go `se.GetSQLExecutor()`.
    pub conn: &'a dyn SqlExecutor,
    /// Go `GetEtcdClient(se.GetStore())`; `None` when the store has no etcd.
    pub etcd: Option<&'a dyn EtcdOps>,
}

/// Go `streamhelper.PrefixOfTask`.
const PITR_TASK_PREFIX: &str = "/tidb/br-stream/info/";

impl LoadDataController {
    /// Go `CheckRequirements`.
    pub fn check_requirements(&self, go_ctx: &tidb_gocontext::Context, se: &PrecheckSession<'_>) -> Result<(), DriverError> {
        self.check_requirements_inner(go_ctx, se, true)
    }

    /// Go `CheckRequirementsBeforeInitDataFiles`: the checks that do not need
    /// the file list, then that the source can be read.
    pub fn check_requirements_before_init_data_files(
        &self,
        go_ctx: &tidb_gocontext::Context,
        se: &PrecheckSession<'_>,
    ) -> Result<(), DriverError> {
        self.check_requirements_inner(go_ctx, se, false)?;
        if self.plan.data_source_type == "file" {
            return self.check_data_source_access(go_ctx);
        }
        Ok(())
    }

    fn check_requirements_inner(
        &self,
        go_ctx: &tidb_gocontext::Context,
        se: &PrecheckSession<'_>,
        check_total_file_size: bool,
    ) -> Result<(), DriverError> {
        let meta = self.plan.table_info.clone().unwrap_or_else(|| self.table.meta().read().clone());
        if meta.ttl_info.as_ref().is_some_and(|ttl| ttl.read().enable) {
            return Err(pre_check_failed("target table has TTL enabled, please disable TTL before IMPORT INTO"));
        }
        if self.plan.data_source_type == "file" || tidb_config_kerneltype::is_next_gen() {
            let cnt = super::job::get_active_job_cnt(se.ctx, se.conn, &self.plan.db_name, meta.name.lowercase())?;
            if cnt > 0 {
                return Err(pre_check_failed("there is active job on the target table already"));
            }
        }
        if check_total_file_size && self.plan.data_source_type == "file" {
            self.check_import_data_size()?;
        }
        self.check_table_empty(se)?;
        if !self.plan.disable_precheck {
            check_cdc_pitr_tasks(se)?;
        }
        if self.plan.is_global_sort() {
            return self.check_global_sort_store_privilege(go_ctx);
        }
        Ok(())
    }

    /// Go `CheckImportDataSize`.
    pub fn check_import_data_size(&self) -> Result<(), DriverError> {
        if self.plan.total_file_size == 0 {
            return Err(pre_check_failed(
                "No file matched, or the file is empty. Please provide a valid file location.",
            ));
        }
        self.check_starter_max_import_data_size()
    }

    /// Go `checkStarterMaxImportDataSize`.
    fn check_starter_max_import_data_size(&self) -> Result<(), DriverError> {
        if !tidb_config_deploymode::is_starter() {
            return Ok(());
        }
        let max = tidb_config::config_tree::config::get_global_config().starter_params.max_import_data_size.0;
        if max == 0 || self.total_real_size <= 0 || self.total_real_size as u64 <= max {
            return Ok(());
        }
        let size = |v: f64| tidb_config_configtypes::bytes_size(v);
        Err(pre_check_failed(&format!(
            "total real import data size {} exceeds maximum import size limit {} (total file size {})",
            size(self.total_real_size as f64),
            size(max as f64),
            size(self.plan.total_file_size as f64),
        )))
    }

    /// Go `checkTableEmpty`.
    fn check_table_empty(&self, se: &PrecheckSession<'_>) -> Result<(), DriverError> {
        let sql = format!(
            "SELECT 1 FROM {}.{} USE INDEX() LIMIT 1",
            tidb_lightning_common::escape_identifier(&self.plan.db_name),
            tidb_lightning_common::escape_identifier(self.table.meta().read().name.lowercase()),
        );
        let rows = tidb_sqlexec::execute_sql(se.ctx, se.conn, &sql, &[])
            .map_err(tidb_driver_error::sql_exec_driver_error)?;
        if !rows.is_empty() {
            return Err(pre_check_failed("target table is not empty"));
        }
        Ok(())
    }

    /// Go `checkGlobalSortStorePrivilege`: the sort store can be read,
    /// listed, written and deleted.
    fn check_global_sort_store_privilege(&self, go_ctx: &tidb_gocontext::Context) -> Result<(), DriverError> {
        use tidb_objstore::storeapi::Permission;
        let target = "cloud storage";
        let mut u = tidb_objstore::parse_raw_url(&self.plan.cloud_storage_uri).map_err(|e| {
            DriverError::from(tidb_exeerrors::ERR_LOAD_DATA_INVALID_URI.gen_with_stack_by_args(&[target.into(), e.into()]))
        })?;
        let backend = tidb_objstore::parse_backend_from_url(&mut u, None).map_err(|e| {
            DriverError::from(tidb_exeerrors::ERR_LOAD_DATA_INVALID_URI.gen_with_stack_by_args(&[target.into(), e.into()]))
        })?;
        if !super::import::is_supported_cloud_storage_backend(&backend) {
            let scheme = String::from_utf8_lossy(&u.scheme).into_owned();
            return Err(pre_check_failed(&format!("unsupported cloud storage uri scheme: {scheme}")));
        }
        let opt = tidb_objstore::storeapi::Options {
            check_permissions: vec![Permission::GetObject, Permission::ListObjects, Permission::PutAndDeleteObject],
            ..Default::default()
        };
        tidb_objstore::new(go_ctx, &backend, Some(&opt))
            .map(|_| ())
            .map_err(|e| pre_check_failed(&format!("check cloud storage uri access: {e}")))
    }
}

/// Go `checkCDCPiTRTasks`: no PiTR log task and no running changefeed.
fn check_cdc_pitr_tasks(se: &PrecheckSession<'_>) -> Result<(), DriverError> {
    let Some(etcd) = se.etcd else { return Ok(()) };
    let tasks = etcd.get_prefix(PITR_TASK_PREFIX).map_err(super::import::plain)?;
    if !tasks.is_empty() {
        let mut names = Vec::with_capacity(tasks.len());
        for (_, value) in &tasks {
            let info = <tidb_proto::backup::StreamBackupTaskInfo as prost::Message>::decode(value.as_slice())
                .map_err(|e| super::import::plain(e.to_string()))?;
            names.push(info.name);
        }
        return Err(pre_check_failed(&format!("found PiTR log streaming task(s): [{}],", names.join(" "))));
    }
    let name_set = tidb_cdcutil::get_running_changefeeds(etcd).map_err(super::import::plain)?;
    if !name_set.is_empty() {
        return Err(pre_check_failed(&name_set.message_to_user()));
    }
    Ok(())
}
