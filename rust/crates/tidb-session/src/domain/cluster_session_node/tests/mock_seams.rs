//! The stored-state seams without a cluster: the DDL executor (which refuses
//! every DDL; DDL tests run on the in-process cluster), the account writer,
//! the global-variable writer and the analyzer.
//!
//! The account, global-variable and analyze seams replace the 2PC of their
//! production counterparts ([`tidb_domain_core::cluster_account_seam`],
//! [`tidb_domain_core::cluster_sysvar_seam`],
//! [`tidb_executor::cluster_analyze_seam`]) with a switch a test can flip,
//! keeping the part this node owns: the routing contract and the
//! scratch-then-publish ordering that decides what a failed statement leaves
//! behind.

use super::super::*;
use tidb_store_driver_error::sql_query_error::SqlQueryError;
use std::sync::atomic::{AtomicBool, Ordering};
use tidb_ddl::real_tikv_ddl::ClusterDdlReport;

/// Go `DBInfo.Deprecated.Tables` as the test fixtures read and edit it.
pub(super) trait DeprecatedTables {
    fn tables(&self) -> Vec<tidb_model::table_info::TableInfo>;
    fn with_tables<R>(&mut self, edit: impl FnOnce(&mut Vec<tidb_model::table_info::TableInfo>) -> R) -> R;
    /// The tables as pointers this database alone holds, to write through:
    /// a mock catalog is a copy of the one before it.
    fn tables_mut(&mut self) -> Vec<tidb_model::GoShared<tidb_model::table_info::TableInfo>>;
}

impl DeprecatedTables for tidb_model::db::DBInfo {
    fn tables(&self) -> Vec<tidb_model::table_info::TableInfo> {
        self.deprecated_tables
            .iter_deref()
            .map(|table| table.read().clone())
            .collect()
    }

    fn with_tables<R>(&mut self, edit: impl FnOnce(&mut Vec<tidb_model::table_info::TableInfo>) -> R) -> R {
        let mut tables = self.tables();
        let result = edit(&mut tables);
        self.deprecated_tables = tables.into();
        result
    }

    fn tables_mut(&mut self) -> Vec<tidb_model::GoShared<tidb_model::table_info::TableInfo>> {
        self.deprecated_tables = self.tables().into();
        self.deprecated_tables.iter_deref().collect()
    }
}


/// A loaded catalog shares its `DBInfo` pointers with the version before it,
/// so an edit copies the database and replaces the slot.
impl DeprecatedTables for tidb_model::GoShared<tidb_model::db::DBInfo> {
    fn tables(&self) -> Vec<tidb_model::table_info::TableInfo> {
        self.read().tables()
    }

    fn with_tables<R>(&mut self, edit: impl FnOnce(&mut Vec<tidb_model::table_info::TableInfo>) -> R) -> R {
        let mut copy = self.read().clone();
        let result = copy.with_tables(edit);
        *self = tidb_model::GoShared::new(copy);
        result
    }

    fn tables_mut(&mut self) -> Vec<tidb_model::GoShared<tidb_model::table_info::TableInfo>> {
        let mut copy = self.read().clone();
        let tables = copy.tables_mut();
        *self = tidb_model::GoShared::new(copy);
        tables
    }
}

/// The DDL executor of a node without a cluster: it refuses every DDL, so a
/// mock-node test that reaches it fails loudly. DDL runs on the in-process
/// cluster (`unistore_cop`), where the executor, the job queue and the
/// schema loader are the real ones.
pub(super) struct MockDdl {
    pub(super) catalog: Arc<InfoCache>,
    /// How many times Go's `DDL.Stop` reached this executor.
    pub(super) stops: std::sync::atomic::AtomicUsize,
}

impl MockDdl {
    pub(super) fn new(catalog: Arc<InfoCache>) -> Self {
        Self { catalog, stops: std::sync::atomic::AtomicUsize::new(0) }
    }
}

impl ClusterDdl for MockDdl {
    fn stop(&self) {
        self.stops.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    fn create_schema_with_info(
        &self,
        _: tidb_model::DBInfo,
        _: tidb_ddl::OnExist,
        _: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the mock catalog writer does not execute persisted jobs"))
    }

    fn create_table_with_info(
        &self,
        _: &str,
        _: tidb_model::TableInfo,
        _: &[tidb_model::InvolvingSchemaInfo],
        _: tidb_ddl::CreateTableConfig,
        _: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the mock catalog writer does not execute persisted jobs"))
    }

    fn batch_create_table_with_info(
        &self,
        _: &str,
        _: Vec<tidb_model::TableInfo>,
        _: tidb_ddl::CreateTableConfig,
        _: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the mock catalog writer does not execute persisted jobs"))
    }

    fn create_placement_policy_with_info(
        &self,
        _: tidb_model::PolicyInfo,
        _: tidb_ddl::OnExist,
        _: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the mock catalog writer does not execute persisted jobs"))
    }

    fn update_table_replica_info(
        &self,
        _: i64,
        _: bool,
        _: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the mock catalog writer does not execute DDL"))
    }

    fn alter_table(
        &self,
        _: &tidb_ast::AlterTableStmt,
        _: &str,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the mock catalog writer does not execute DDL"))
    }

    fn recover_schema(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the mock catalog writer does not execute DDL"))
    }

    fn recover_table(
        &self,
        _: &Option<Vec<String>>,
        _: i64,
        _: &str,
        _: bool,
        _: &str,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the mock catalog writer does not execute DDL"))
    }

    fn drop_index(
        &self,
        _: &tidb_ast::DropIndexStmt,
        _: &str,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the mock catalog writer does not execute DDL"))
    }

    fn create_index(
        &self,
        _: &tidb_ast::CreateIndexStmt,
        _: &str,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the mock catalog writer does not execute DDL"))
    }

    fn alter_table_set_tiflash_replica(
        &self,
        _: &str,
        _: &str,
        _: u64,
        _: &[String],
        _: bool,
        _: bool,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the mock catalog writer does not execute DDL"))
    }

    fn alter_schema(
        &self,
        _: &str,
        _: &[tidb_ast::DatabaseOption],
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the mock catalog writer does not execute DDL"))
    }

    fn drop_placement_policy(
        &self,
        _: &tidb_ast::DropPlacementPolicyStmt,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the mock catalog writer does not execute DDL"))
    }

    fn alter_placement_policy(
        &self,
        _: &tidb_ast::AlterPlacementPolicyStmt,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the mock catalog writer does not execute DDL"))
    }

    fn create_placement_policy(
        &self,
        _: &tidb_ast::CreatePlacementPolicyStmt,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the mock catalog writer does not execute DDL"))
    }

    fn refresh_meta(
        &self,
        _: i64,
        _: i64,
        _: &str,
        _: &str,
        _: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the mock catalog writer does not execute DDL"))
    }

    fn alter_table_mode(
        &self,
        _: tidb_model::TableMode,
        _: i64,
        _: i64,
        _: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the mock catalog writer does not execute DDL"))
    }

    fn lock_tables(
        &self,
        _: &[tidb_ast::TableLock],
        _: &str,
        _: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the mock catalog writer does not execute DDL"))
    }

    fn unlock_tables(
        &self,
        _: Vec<tidb_model::TableLockTpInfo>,
        _: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the mock catalog writer does not execute DDL"))
    }

    fn cleanup_table_lock(
        &self,
        _: &[Vec<String>],
        _: &str,
        _: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the mock catalog writer does not execute DDL"))
    }

    fn create_masking_policy(
        &self,
        _: &tidb_ast::CreateMaskingPolicyStmt,
        _: &str,
        _: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        unreachable!("these tests create no masking policy")
    }

    fn create_materialized_view_log(
        &self,
        _: &tidb_ast::CreateMaterializedViewLogStmt,
        _: &str,
        _: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        unreachable!("these tests create no materialized view log")
    }

    fn alter_materialized_view(
        &self,
        _: &tidb_ast::AlterMaterializedViewStmt,
        _: &str,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        unreachable!("these tests alter no materialized view")
    }

    fn drop_materialized_view(
        &self,
        _: &tidb_ast::DropMaterializedViewStmt,
        _: &str,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        unreachable!("these tests drop no materialized view")
    }

    fn create_materialized_view(
        &self,
        _: &tidb_ast::CreateMaterializedViewStmt,
        _: &str,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        unreachable!("these tests create no materialized view")
    }

    fn alter_materialized_view_log(
        &self,
        _: &tidb_ast::AlterMaterializedViewLogStmt,
        _: &str,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        unreachable!("these tests alter no materialized view log")
    }

    fn drop_materialized_view_log(
        &self,
        _: &tidb_ast::DropMaterializedViewLogStmt,
        _: &str,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        unreachable!("these tests drop no materialized view log")
    }

    fn create_view(
        &self,
        _: &str,
        _: &str,
        _: bool,
        _: &tidb_model::TableInfo,
        _: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the mock catalog writer does not execute DDL"))
    }

    fn do_ddl_job_wrapper(
        &self,
        _: &dyn tidb_ddl_build::session_context::DdlSessionContext,
        _: tidb_ddl::JobWrapper,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the mock catalog writer does not execute DDL"))
    }

    fn drop_table(
        &self,
        _: &tidb_ast::DropTableStmt,
        _: &str,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the mock catalog writer does not execute DDL"))
    }

    fn drop_sequence(
        &self,
        _: &tidb_ast::DropSequenceStmt,
        _: &str,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the mock catalog writer does not execute DDL"))
    }

    fn rename_table(
        &self,
        _: &tidb_ast::RenameTableStmt,
        _: &str,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the mock catalog writer does not execute DDL"))
    }

    fn drop_view(
        &self,
        _: &[Vec<String>],
        _: bool,
        _: &str,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the mock catalog writer does not execute DDL"))
    }

    fn drop_schema(
        &self,
        _: &str,
        _: bool,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the mock catalog writer does not execute DDL"))
    }

    fn create_table(
        &self,
        _: &tidb_ast::CreateTableStmt,
        _: &str,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the mock catalog writer does not execute DDL"))
    }

    fn create_sequence(
        &self,
        _: &tidb_ast::CreateSequenceStmt,
        _: &str,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the mock catalog writer does not execute DDL"))
    }

    fn alter_sequence(
        &self,
        _: &tidb_ast::AlterSequenceStmt,
        _: &str,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the mock catalog writer does not execute DDL"))
    }

    fn repair_table(
        &self,
        _: &tidb_ast::RepairTableStmt,
        _: &str,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the mock catalog writer does not execute DDL"))
    }

    fn truncate_table(
        &self,
        _: &[String],
        _: &str,
        _: &tidb_ddl::cluster_ddl::DdlContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the mock catalog writer does not execute DDL"))
    }

    fn create_schema(
        &self,
        _: &str,
        _: bool,
        _: &[tidb_ast::DatabaseOption],
        _: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        Err(SqlQueryError::unknown("the mock catalog writer does not execute DDL"))
    }

    // A cluster without TiFlash stores.
    fn tiflash_store_count(&self) -> Result<u64, SqlQueryError> {
        Ok(0)
    }
}

/// The account seam without a cluster: the "stored" accounts are the
/// cluster's `mysql.*` privilege rows, which an account statement validates
/// against as the registry Go's `MySQLPrivilege.LoadAll` builds from them,
/// and the live table reloads from them after a committed statement (Go
/// `NotifyUpdatePrivilege`). Statements that write rows need a store with
/// `mysql.*` tables; the unistore fixtures run those.
pub(super) struct MockAccountWriter {
    /// What the "cluster" stores: its privilege tables' rows.
    pub(super) stored: std::sync::Mutex<tidb_privilege::loaded::ClusterPrivileges>,
    /// The node's live table.
    pub(super) live: PrivilegeRegistry,
}

impl MockAccountWriter {
    pub(super) fn new() -> Self {
        let live = PrivilegeRegistry::default();
        Self {
            stored: std::sync::Mutex::new(tidb_privilege::load::cluster_image_from_registry(&live)),
            live,
        }
    }

    /// Writes rows into the cluster's privilege tables and reloads the live
    /// table, as a committed account statement and Go's
    /// `NotifyUpdatePrivilege` do.
    pub(super) fn write_rows(&self, write: impl FnOnce(&mut tidb_privilege::loaded::ClusterPrivileges)) {
        write(&mut self.stored.lock().unwrap());
        self.notify_all_privileges_changed().unwrap();
    }

    /// `CREATE USER user@'%'` with no password.
    pub(super) fn create_user(&self, user: &str) {
        self.write_rows(|rows| {
            rows.users.push(tidb_privilege::loaded::LoadedUser::new(user, "%", "mysql_native_password", ""));
        });
    }

    /// `GRANT privileges ON database.* TO user@'%'` (printed names).
    pub(super) fn grant_db(&self, user: &str, database: &str, privileges: &[&'static str]) {
        self.write_rows(|rows| {
            rows.db_grants.push(tidb_privilege::loaded::LoadedDbGrant {
                host: "%".to_owned(),
                user: user.to_owned(),
                database: database.to_owned(),
                privileges: privileges.to_vec(),
            });
        });
    }

    /// `GRANT privileges ON database.table TO user@'%'` (`Table_priv` labels).
    pub(super) fn grant_table(&self, user: &str, database: &str, table: &str, privileges: &[&str]) {
        self.write_rows(|rows| {
            rows.table_grants.push(tidb_privilege::loaded::LoadedTableGrant {
                host: "%".to_owned(),
                user: user.to_owned(),
                database: database.to_owned(),
                table: table.to_owned(),
                privileges: privileges.iter().map(|privilege| (*privilege).to_owned()).collect(),
            });
        });
    }
}

impl ClusterAccountWriter for MockAccountWriter {
    fn load_accounts(&self) -> Result<PrivilegeRegistry, String> {
        Ok(tidb_privilege::load::registry_from_cluster(&self.stored.lock().unwrap()).registry)
    }

    /// The mock's handle loads every user: a reload is `UpdateAll`.
    fn notify_update_privilege(&self, _users: &[String]) -> Result<(), String> {
        self.notify_all_privileges_changed()
    }

    fn notify_all_privileges_changed(&self) -> Result<(), String> {
        self.live.replace_from(&self.load_accounts()?);
        Ok(())
    }
}

/// The sysvar seam without a cluster: the "stored" overrides are one [`GlobalSysvars`] table, and a
/// change is persisted by publishing the scratch copy into it.
pub(super) struct MockSysvarWriter {
    pub(super) stored: GlobalSysvars,
    pub(super) live: GlobalSysvars,
    pub(super) persists: Arc<AtomicBool>,
}

impl MockSysvarWriter {
    /// A bootstrapped cluster's `mysql.global_variables`, as every Go node
    /// finds them.
    pub(super) fn new() -> Self {
        let rows = crate::mysql_bootstrap::bootstrapped_global_variables();
        Self {
            stored: GlobalSysvars::from_cluster_rows(rows.clone()),
            live: GlobalSysvars::from_cluster_rows(rows),
            persists: Arc::new(AtomicBool::new(true)),
        }
    }
}

impl tidb_domain_core::cluster_sysvar_seam::ClusterSysvarWriter for MockSysvarWriter {
    fn begin(&self) -> Result<Box<dyn tidb_domain_core::cluster_sysvar_seam::PendingSysvarChange>, String> {
        let scratch = GlobalSysvars::from_cluster_rows(self.stored.overrides());
        Ok(Box::new(MockPendingSysvarChange {
            scratch,
            stored: self.stored.clone(),
            live: self.live.clone(),
            persists: Arc::clone(&self.persists),
        }))
    }
}

pub(super) struct MockPendingSysvarChange {
    pub(super) scratch: GlobalSysvars,
    pub(super) stored: GlobalSysvars,
    pub(super) live: GlobalSysvars,
    pub(super) persists: Arc<AtomicBool>,
}

impl tidb_domain_core::cluster_sysvar_seam::PendingSysvarChange for MockPendingSysvarChange {
    fn table(&self) -> GlobalSysvars {
        self.scratch.clone()
    }

    fn commit(
        self: Box<Self>,
        _name: &str,
        _stored: &str,
        _pool: &dyn tidb_stats_handle_storage::stats_read_writer::StatsSessionPool,
    ) -> Result<(), tidb_store_driver_error::sql_query_error::SqlQueryError> {
        if !self.persists.load(Ordering::Acquire) {
            return Err(SqlQueryError::unknown("the persist was rejected"));
        }
        self.stored
            .replace_from(&GlobalSysvars::from_cluster_rows(self.scratch.overrides()));
        self.live
            .replace_from(&GlobalSysvars::from_cluster_rows(self.stored.overrides()));
        Ok(())
    }
}

/// The mock node has no rows in a TiKV to sample, so its analyzer refuses
/// by name: what these tests exercise is the ROUTE -- that `ANALYZE TABLE`
/// reaches the statistics seam at all, and that its refusal reaches the
/// client -- not the histogram arithmetic, which
/// [`tidb_stats::builder`] owns and tests directly.
pub(super) struct MockAnalyze;

impl ClusterAnalyze for MockAnalyze {
    fn execute(
        &self,
        statements: &[AnalyzeStatement],
        _: &str,
        _: &dyn tidb_pdhelper::ApproximateTableCountProvider,
        _: &tidb_sqlkiller::SqlKiller,
        _: &dyn tidb_executor::real_tikv_analyze::AnalyzeJobLifecycle,
        _: &dyn tidb_stats_handle_storage::stats_read_writer::StatsSessionPool,
    ) -> Result<tidb_executor::real_tikv_analyze::AnalyzeExecReport, SqlQueryError> {
        Err(SqlQueryError::unknown(format!(
            "the mock node stores no statistics for `{}`.`{}`",
            statements[0].schema, statements[0].table
        )))
    }
}

