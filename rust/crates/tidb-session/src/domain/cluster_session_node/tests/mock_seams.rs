//! The stored-state seams without a cluster: the catalog writer, the account
//! writer, the global-variable writer and the analyzer.
//!
//! Each replaces the 2PC of its production counterpart
//! ([`crate::domain::cluster_session_node::RealClusterDdl`],
//! [`tidb_domain_core::cluster_account_seam`], [`tidb_domain_core::cluster_sysvar_seam`],
//! [`tidb_executor::cluster_analyze_seam`]) with a switch a test can flip, keeping the
//! part this node owns: the routing contract, the published catalog moving to
//! a new schema version, and the scratch-then-publish ordering that decides
//! what a failed statement leaves behind.

use super::super::*;
use super::node_fixture::*;
use tidb_store_driver_error::sql_query_error::SqlQueryError;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use tidb_ast::CiString;
use tidb_datatype::FieldTypeFlags;
use tidb_infoschema::SessionExtendedInfoSchema;
use tidb_ddl::real_tikv_ddl::ClusterDdlReport;
use tidb_model::db::DBInfo;

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

/// The catalog writer, offline: the meta-key encoding and the 2PC are
/// proven by `tidb-exec`'s own `cluster_ddl_source` tests, so what is
/// modelled here is the part this node owns -- the published catalog
/// moving, at a new schema version, from the statement's own thread.
///
/// The `TableInfo` it publishes comes from the same catalog-aware build recipe
/// as the real path.
pub(super) struct MockDdl {
    pub(super) catalog: Arc<InfoCache>,
    /// Stands in for `NextGlobalID`.
    pub(super) next_id: AtomicI64,
    /// SessionExtendedInfoSchema changes actually published.
    pub(super) applied: AtomicUsize,
}

impl MockDdl {
    pub(super) fn new(catalog: Arc<InfoCache>) -> Self {
        Self {
            catalog,
            next_id: AtomicI64::new(200),
            applied: AtomicUsize::new(0),
        }
    }

    pub(super) fn allocate(&self) -> i64 {
        self.next_id.fetch_add(1, Ordering::AcqRel) + 1
    }
}

fn mock_rename_table(
    databases: &mut [tidb_model::GoShared<DBInfo>],
    from_schema: &str,
    from_table: &str,
    to_schema: &str,
    to_table: &str,
) -> Result<(), SqlQueryError> {
    let database_index = |name: &str| {
        let name = name.to_lowercase();
        databases
            .iter()
            .position(|database| database.read().name.lowercase() == name)
    };
    let source_at = database_index(from_schema)
        .ok_or_else(|| SqlQueryError::unknown(format!("Unknown database '{from_schema}'")))?;
    let target_at = database_index(to_schema)
        .ok_or_else(|| SqlQueryError::unknown(format!("Unknown database '{to_schema}'")))?;
    let source_name = from_table.to_lowercase();
    let source_table = databases[source_at]
        .read().deprecated_tables
        .iter_deref()
        .position(|stored| stored.read().name.lowercase() == source_name)
        .ok_or_else(|| {
            SqlQueryError::unknown(format!("Unknown table '{from_schema}.{from_table}'"))
        })?;
    let target_name = to_table.to_lowercase();
    if databases[target_at]
        .read().deprecated_tables
        .iter_deref()
        .any(|stored| stored.read().name.lowercase() == target_name)
    {
        return Err(SqlQueryError::unknown(format!(
            "Table '{to_schema}.{to_table}' already exists"
        )));
    }
    let mut table = databases[source_at].with_tables(|tables| tables.remove(source_table));
    table.name = CiString::new(to_table.to_owned());
    databases[target_at].with_tables(|tables| tables.push(table));
    Ok(())
}

impl ClusterDdl for MockDdl {
    // A cluster without TiFlash stores.
    fn tiflash_store_count(&self) -> Result<u64, SqlQueryError> {
        Ok(0)
    }

    fn execute(
        &self,
        statement: &DdlStatement,
        _job_context: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        let current = self.catalog.get_latest();
        // The next version's `DBInfo`s, built into its image below (Go's
        // loader: `InitWithDBInfos`).
        let mut databases = current.stored_schemas();
        let find = |databases: &mut Vec<tidb_model::GoShared<DBInfo>>, name: &str| -> Option<usize> {
            let name = name.to_lowercase();
            databases
                .iter()
                .position(|database| database.read().name.lowercase() == name)
        };
        let mut created_id = None;
        match statement {
            DdlStatement::CreateDatabase {
                name,
                if_not_exists,
                charset,
                collate,
                ..
            } => {
                if find(&mut databases, name).is_some() {
                    if *if_not_exists {
                        return Ok(ClusterDdlReport::AlreadySatisfied {
                            warnings: Vec::new(),
                            detail: format!("database `{name}` already exists"),
                        });
                    }
                    return Err(SqlQueryError::unknown(format!(
                        "Can't create database '{name}'; database exists"
                    )));
                }
                let id = self.allocate();
                created_id = Some(id);
                databases.push(tidb_model::GoShared::new(DBInfo {
                    id,
                    name: CiString::new(name.clone()),
                    charset: charset.clone(),
                    collate: collate.clone(),
                    deprecated_tables: Vec::new().into(),
                    ..DBInfo::default()
                }));
            }
            DdlStatement::DropDatabase { name, if_exists } => {
                match find(&mut databases, name) {
                    Some(at) => {
                        databases.remove(at);
                    }
                    None if *if_exists => {
                        return Ok(ClusterDdlReport::AlreadySatisfied {
                            warnings: Vec::new(),
                            detail: format!("database `{name}` does not exist"),
                        })
                    }
                    None => {
                        return Err(SqlQueryError::unknown(format!("Unknown database '{name}'")))
                    }
                }
            }
            DdlStatement::CreateTable {
                schema,
                table,
                if_not_exists,
                build,
            } => {
                let at = find(&mut databases, schema).ok_or_else(|| {
                    SqlQueryError::unknown(format!("Unknown database '{schema}'"))
                })?;
                let lowered = table.to_lowercase();
                if databases[at]
                    .read().deprecated_tables
                    .iter_deref()
                    .any(|stored| stored.read().name.lowercase() == lowered)
                {
                    if *if_not_exists {
                        return Ok(ClusterDdlReport::AlreadySatisfied {
                            warnings: Vec::new(),
                            detail: format!("table `{schema}`.`{table}` already exists"),
                        });
                    }
                    return Err(SqlQueryError::unknown(format!(
                        "Table '{schema}.{table}' already exists"
                    )));
                }
                let id = self.allocate();
                created_id = Some(id);
                let database = &databases[at];
                let mut info = build
                    .for_database(&database.read().charset, &database.read().collate)
                    .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
                info.id = id;
                // The real writer draws one further id per partition, right
                // after the table's own. Leaving them zero here would build a
                // fixture whose partitions all share physical table 0, which
                // is a shape no cluster ever stores.
                if let Some(partition) = &info.partition {
                    let partition = partition.read();
                    let count = partition.definitions.with_visible(<[_]>::len);
                    for ordinal in 0..count {
                        let physical = self.allocate();
                        partition
                            .definitions
                            .update(ordinal, |definition| definition.id = physical);
                    }
                }
                databases[at].with_tables(|tables| tables.push(info));
            }
            DdlStatement::CreateView {
                schema,
                name,
                or_replace,
                info,
            } => {
                let at = find(&mut databases, schema).ok_or_else(|| {
                    SqlQueryError::unknown(format!("Unknown database '{schema}'"))
                })?;
                let lowered = name.to_lowercase();
                let existing = databases[at]
                    .read().deprecated_tables
                    .iter_deref()
                    .position(|stored| stored.read().name.lowercase() == lowered);
                if existing.is_some() && !or_replace {
                    return Err(SqlQueryError::unknown(format!(
                        "Table '{schema}.{name}' already exists"
                    )));
                }
                if let Some(old) = existing {
                    databases[at].with_tables(|tables| tables.remove(old));
                }
                let id = self.allocate();
                created_id = Some(id);
                let mut info = (**info).clone();
                info.id = id;
                databases[at].with_tables(|tables| tables.push(info));
            }
            DdlStatement::DropView { names, if_exists } => {
                let mut missing = Vec::new();
                for (schema, name) in names {
                    let Some(at) = find(&mut databases, schema) else {
                        missing.push(format!("{schema}.{name}"));
                        continue;
                    };
                    let lowered = name.to_lowercase();
                    let Some(stored) = databases[at]
                        .read().deprecated_tables
                        .iter_deref()
                        .position(|stored| stored.read().name.lowercase() == lowered)
                    else {
                        missing.push(format!("{schema}.{name}"));
                        continue;
                    };
                    if databases[at].tables()[stored].view.is_none() {
                        return Err(SqlQueryError::unknown(format!(
                            "'{schema}.{name}' is a base table, not a VIEW"
                        )));
                    }
                    databases[at].with_tables(|tables| tables.remove(stored));
                }
                if !missing.is_empty() && !if_exists {
                    return Err(SqlQueryError::unknown(format!(
                        "Unknown table '{}'",
                        missing.join(",")
                    )));
                }
            }
            DdlStatement::RebaseAutoRandom {
                schema,
                table,
                next: requested,
                force,
            } => {
                let at = find(&mut databases, schema).ok_or_else(|| {
                    SqlQueryError::unknown(format!("Unknown database '{schema}'"))
                })?;
                let lowered = table.to_lowercase();
                let stored = databases[at]
                    .tables_mut()
                    .into_iter()
                    .find(|stored| stored.read().name.lowercase() == lowered)
                    .ok_or_else(|| {
                        SqlQueryError::unknown(format!("Unknown table '{schema}.{table}'"))
                    })?;
                let mut stored = stored.write();
                if stored.auto_random_bits == 0 {
                    return Err(SqlQueryError::new(
                        8216,
                        *b"HY000",
                        "Invalid auto random: alter auto_random_base of a non auto_random table",
                    ));
                }
                if *force && *requested == 0 {
                    return Err(SqlQueryError::new(
                        1467,
                        *b"HY000",
                        "Failed to read auto-increment value from storage engine",
                    ));
                }
                stored.auto_rand_id = if *force {
                    *requested
                } else {
                    (*requested).max(stored.auto_rand_id)
                };
            }
            DdlStatement::ModifyAutoIdCache {
                schema,
                table,
                new_cache,
            } => {
                let at = find(&mut databases, schema).ok_or_else(|| {
                    SqlQueryError::unknown(format!("Unknown database '{schema}'"))
                })?;
                let lowered = table.to_lowercase();
                let stored = databases[at]
                    .tables_mut()
                    .into_iter()
                    .find(|stored| stored.read().name.lowercase() == lowered)
                    .ok_or_else(|| {
                        SqlQueryError::unknown(format!("Unknown table '{schema}.{table}'"))
                    })?;
                let mut stored = stored.write();
                if (*new_cache == 1) != (stored.auto_id_cache == 1) {
                    return Err(SqlQueryError::unknown(
                        "Can't Alter AUTO_ID_CACHE between 1 and non-1, the underlying implementation is different",
                    ));
                }
                stored.auto_id_cache = *new_cache;
            }
            DdlStatement::AlterAutoRandomBits {
                schema,
                table,
                column,
                shard_bits,
                range_bits,
                ..
            } => {
                let at = find(&mut databases, schema).ok_or_else(|| {
                    SqlQueryError::unknown(format!("Unknown database '{schema}'"))
                })?;
                let lowered = table.to_lowercase();
                let stored = databases[at]
                    .tables_mut()
                    .into_iter()
                    .find(|stored| stored.read().name.lowercase() == lowered)
                    .ok_or_else(|| {
                        SqlQueryError::unknown(format!("Unknown table '{schema}.{table}'"))
                    })?;
                let mut stored = stored.write();
                if stored.auto_random_bits > *shard_bits {
                    return Err(SqlQueryError::new(
                        8216,
                        *b"HY000",
                        "Invalid auto random: decreasing auto_random shard bits is not supported",
                    ));
                }
                let converting = stored.auto_random_bits == 0;
                let mut updated = stored.clone_like_go();
                let target = tidb_model::column::find_column_info(&updated.columns, column)
                    .ok_or_else(|| SqlQueryError::unknown(format!("Unknown column '{column}'")))?;
                if converting {
                    target
                        .write()
                        .del_flag(u64::from(FieldTypeFlags::AUTO_INCREMENT));
                }
                updated.auto_random_bits = *shard_bits;
                updated.auto_random_range_bits = *range_bits;
                *stored = updated;
            }
            DdlStatement::DropTable {
                schema,
                table,
                if_exists,
            } => {
                let at = find(&mut databases, schema).ok_or_else(|| {
                    SqlQueryError::unknown(format!("Unknown database '{schema}'"))
                })?;
                let lowered = table.to_lowercase();
                let found = databases[at]
                    .read().deprecated_tables
                    .iter_deref()
                    .position(|stored| stored.read().name.lowercase() == lowered);
                match found {
                    Some(index) => {
                        databases[at].with_tables(|tables| tables.remove(index));
                    }
                    None if *if_exists => {
                        return Ok(ClusterDdlReport::AlreadySatisfied {
                            warnings: Vec::new(),
                            detail: format!("table `{schema}`.`{table}` does not exist"),
                        })
                    }
                    None => {
                        return Err(SqlQueryError::unknown(format!(
                            "Unknown table '{schema}.{table}'"
                        )))
                    }
                }
            }
            DdlStatement::RenameTable {
                from_schema,
                from_table,
                to_schema,
                to_table,
                ..
            } => mock_rename_table(&mut databases, from_schema, from_table, to_schema, to_table)?,
            DdlStatement::RenameTables { pairs } => {
                for pair in pairs {
                    mock_rename_table(
                        &mut databases,
                        &pair.from_schema,
                        &pair.from_table,
                        &pair.to_schema,
                        &pair.to_table,
                    )?;
                }
            }
            // An index change is the one catalog change whose correctness is
            // not finished by the metadata: it also owes the existing rows
            // their entries, and this mock has no rows to walk. Modelling only
            // the metadata half would make a test PASS on exactly the shape
            // that returns wrong rows in production, so it is refused here and
            // exercised where the rows are real -- `plan_ddl`'s own tests for
            // the write set, and `run-sysbench-ladder.sh`'s `ADMIN CHECK
            // TABLE` against a Go server for the entries.
            // Go `onLockTables`/`onUnlockTables` over the mock's stored
            // tables, through the planner's shared lock edits.
            DdlStatement::LockTables {
                lock_tables,
                unlock_tables,
                session,
            } => {
                mock_unlock_tables(&mut databases, unlock_tables, session, false);
                for check in [true, false] {
                    for lock in lock_tables {
                        let info =
                            mock_table_by_ids(&mut databases, lock.schema_id, lock.table_id)
                                .ok_or_else(|| SqlQueryError::unknown("locked table not found"))?;
                        let mut info = info.write();
                        let name = info.name.lowercase().to_owned();
                        let current = info.lock.as_ref().map(|lock| lock.read().clone());
                        if check {
                            tidb_ddl_build::table_lock::check_table_locked(
                                &name,
                                current.as_ref(),
                                lock.tp,
                                session,
                            )
                            .map_err(|error| map_error(error.into()))?;
                        } else {
                            let locked = tidb_ddl_build::table_lock::lock_table_in_one_step(
                                &name,
                                current.as_ref(),
                                lock.tp,
                                session,
                                0,
                            )
                            .map_err(|error| map_error(error.into()))?;
                            info.lock = Some(tidb_model::GoShared::new(locked));
                        }
                    }
                }
            }
            DdlStatement::UnlockTables {
                unlock_tables,
                session,
                is_cleanup,
            } => mock_unlock_tables(&mut databases, unlock_tables, session, *is_cleanup),
            DdlStatement::CreateIndex { .. } | DdlStatement::DropIndex { .. } => {
                return Err(SqlQueryError::unknown(
                    "the mock catalog writer holds no rows, so it cannot model an index \
                     change's backfill"
                        .to_owned(),
                ))
            }
            // A PLACEMENT POLICY is a schema object of its own, not a change
            // to a database or table this mock models. `cluster_ddl_source`
            // owns those plans, as it does the column and truncate changes
            // below. Multi-table drops and option-only ALTERs use an injected
            // report seam in schema_changes to check warning routing without
            // duplicating the production planner in this catalog mock.
            DdlStatement::DropTables { .. }
            | DdlStatement::AcceptedNoOp { .. }
            | DdlStatement::CreatePlacementPolicy { .. }
            | DdlStatement::AlterPlacementPolicy { .. }
            | DdlStatement::DropPlacementPolicy { .. }
            | DdlStatement::ModifyTableComment { .. }
            | DdlStatement::ShardRowId { .. }
            | DdlStatement::AlterTablePlacement { .. }
            | DdlStatement::ReorganizePartitions { .. }
            | DdlStatement::AlterTtl { .. }
            | DdlStatement::RemoveTtl { .. }
            | DdlStatement::CreateSequence { .. }
            | DdlStatement::AlterSequence { .. }
            | DdlStatement::DropSequence { .. }
            | DdlStatement::DropSequences { .. }
            | DdlStatement::ModifyEngineAttribute { .. }
            | DdlStatement::RebaseAutoIncrementId { .. }
            | DdlStatement::IgnoredTableOption { .. }
            | DdlStatement::OrderByColumns { .. }
            | DdlStatement::SetColumnDefault { .. }
            | DdlStatement::RenameIndex { .. }
            | DdlStatement::ModifySchemaCharsetAndCollate { .. }
            | DdlStatement::ModifySchemaDefaultPlacement { .. }
            | DdlStatement::ModifySchemaSetTiFlashReplica { .. }
            | DdlStatement::ModifyTableCharsetAndCollate { .. }
            | DdlStatement::CreateTableLike { .. }
            | DdlStatement::DropPrimaryKey { .. }
            | DdlStatement::AlterIndexVisibility { .. }
            | DdlStatement::AddCheckConstraint { .. }
            | DdlStatement::DropCheckConstraint { .. }
            | DdlStatement::AlterCheckConstraint { .. }
            | DdlStatement::IgnoredCheckConstraint { .. }
            | DdlStatement::AddColumn { .. }
            | DdlStatement::ModifyColumn { .. }
            | DdlStatement::AddForeignKey { .. }
            | DdlStatement::DropForeignKey { .. }
            | DdlStatement::AddForeignKeyRollback { .. }
            | DdlStatement::AlterTableCache { .. }
            | DdlStatement::RenameColumn { .. }
            | DdlStatement::DropColumn { .. }
            | DdlStatement::MultiSchemaChange { .. }
            | DdlStatement::AddPartitions { .. }
            | DdlStatement::DropPartitions { .. }
            | DdlStatement::TruncatePartitions { .. }
            | DdlStatement::ExchangePartition { .. }
            | DdlStatement::SetTiFlashReplica { .. }
            | DdlStatement::AlterTableSetRegionSplitPolicy { .. }
            | DdlStatement::AlterTableAffinity { .. }
            | DdlStatement::AlterTableAttributes { .. }
            | DdlStatement::AlterTablePartitionAttributes { .. }
            | DdlStatement::AlterTablePartitionPlacement { .. }
            | DdlStatement::CreateResourceGroup { .. }
            | DdlStatement::AlterResourceGroup { .. }
            | DdlStatement::DropResourceGroup { .. }
            | DdlStatement::UpdateTiFlashReplicaStatus { .. }
            | DdlStatement::TruncateTable { .. }
            | DdlStatement::RecoverTable { .. }
            | DdlStatement::FlashbackDatabase { .. } => {
                return Err(SqlQueryError::unknown(
                    "the mock catalog writer does not model column or truncate changes; \
                     the plan tests in cluster_ddl_source own those"
                        .to_owned(),
                ))
            }
        }
        let next = SessionExtendedInfoSchema::init_with_db_infos(
            databases,
            current.policies().cloned().collect(),
            current.resource_groups().cloned().collect(),
            current.schema_meta_version() + 1,
        );
        let schema_version = next.schema_meta_version();
        // The real writer refreshes the node's catalog inline, before it
        // answers; so does this one.
        self.catalog.upsert(next);
        self.applied.fetch_add(1, Ordering::AcqRel);
        Ok(ClusterDdlReport::Applied {
            schema_version,
            created_id,
            warnings: Vec::new(),
        })
    }
}

fn mock_table_by_ids(
    databases: &mut [tidb_model::GoShared<DBInfo>],
    schema_id: i64,
    table_id: i64,
) -> Option<tidb_model::GoShared<tidb_model::TableInfo>> {
    databases
        .iter_mut()
        .find(|database| database.read().id == schema_id)?
        .tables_mut()
        .into_iter()
        .find(|table| table.read().id == table_id)
}

fn mock_unlock_tables(
    databases: &mut [tidb_model::GoShared<DBInfo>],
    unlock_tables: &[tidb_model::TableLockTpInfo],
    session: &tidb_model::SessionInfo,
    is_cleanup: bool,
) {
    for unlock in unlock_tables {
        let Some(info) = mock_table_by_ids(databases, unlock.schema_id, unlock.table_id) else {
            continue;
        };
        let mut info = info.write();
        let current = info.lock.as_ref().map(|lock| lock.read().clone());
        if let Some(next) =
            tidb_ddl_build::table_lock::unlock_table(current.as_ref(), session, is_cleanup)
        {
            info.lock = next.map(tidb_model::GoShared::new);
        }
    }
}

/// The account seam without a cluster: the "stored" accounts are one
/// registry, which an account statement validates against, and the live
/// table reloads from it after a committed statement (Go
/// `NotifyUpdatePrivilege`). Statements that write rows need a store with
/// `mysql.*` tables; the unistore fixtures run those.
pub(super) struct MockAccountWriter {
    /// What the "cluster" stores.
    pub(super) stored: PrivilegeRegistry,
    /// The node's live table.
    pub(super) live: PrivilegeRegistry,
}

impl MockAccountWriter {
    pub(super) fn new() -> Self {
        Self {
            stored: PrivilegeRegistry::default(),
            live: PrivilegeRegistry::default(),
        }
    }
}

impl ClusterAccountWriter for MockAccountWriter {
    fn load_accounts(&self) -> Result<PrivilegeRegistry, String> {
        Ok(clone_registry(&self.stored))
    }

    fn notify_update_privilege(&self) -> Result<(), String> {
        self.live.replace_from(&clone_registry(&self.stored));
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
        statement: &AnalyzeStatement,
        _: &str,
        _: &dyn tidb_pdhelper::ApproximateTableCountProvider,
        _: &tidb_sqlkiller::SqlKiller,
        _: &dyn Fn() -> bool,
        _: &dyn tidb_executor::real_tikv_analyze::AnalyzeJobLifecycle,
        _: &dyn tidb_stats_handle_storage::stats_read_writer::StatsSessionPool,
    ) -> Result<tidb_executor::real_tikv_analyze::ClusterAnalyzeReport, SqlQueryError> {
        Err(SqlQueryError::unknown(format!(
            "the mock node stores no statistics for `{}`.`{}`",
            statement.schema, statement.table
        )))
    }
}

