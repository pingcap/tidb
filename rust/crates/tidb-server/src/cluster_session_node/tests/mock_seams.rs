//! The stored-state seams without a cluster: the catalog writer, the account
//! writer, the global-variable writer and the analyzer.
//!
//! Each replaces the 2PC of its production counterpart
//! ([`crate::cluster_session_node::RealClusterDdl`],
//! [`crate::cluster_account_seam`], [`crate::cluster_sysvar_seam`],
//! [`crate::cluster_analyze_seam`]) with a switch a test can flip, keeping the
//! part this node owns: the routing contract, the published catalog moving to
//! a new schema version, and the scratch-then-publish ordering that decides
//! what a failed statement leaves behind.

use super::super::*;
use super::node_fixture::*;
use crate::cluster_account_seam::PendingAccountChange;
use crate::sql_node::SqlQueryError;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use tidb_ast::CiString;
use tidb_datatype::FieldTypeFlags;
use tidb_exec::cluster_catalog::{ClusterCatalog, LoadedDatabase};
use tidb_model::db::DBInfo;

/// The catalog writer, offline: the meta-key encoding and the 2PC are
/// proven by `tidb-exec`'s own `cluster_ddl_source` tests, so what is
/// modelled here is the part this node owns -- the published catalog
/// moving, at a new schema version, from the statement's own thread.
///
/// The `TableInfo` it publishes comes from the same catalog-aware build recipe
/// as the real path.
pub(super) struct MockDdl {
    pub(super) catalog: Arc<SharedClusterCatalog>,
    /// Stands in for `NextGlobalID`.
    pub(super) next_id: AtomicI64,
    /// Catalog changes actually published.
    pub(super) applied: AtomicUsize,
}

impl MockDdl {
    pub(super) fn new(catalog: Arc<SharedClusterCatalog>) -> Self {
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
    catalog: &mut ClusterCatalog,
    from_schema: &str,
    from_table: &str,
    to_schema: &str,
    to_table: &str,
) -> Result<(), SqlQueryError> {
    let database_index = |name: &str| {
        let name = name.to_lowercase();
        catalog
            .databases
            .iter()
            .position(|database| database.info.name.lowercase() == name)
    };
    let source_at = database_index(from_schema)
        .ok_or_else(|| SqlQueryError::unknown(format!("Unknown database '{from_schema}'")))?;
    let target_at = database_index(to_schema)
        .ok_or_else(|| SqlQueryError::unknown(format!("Unknown database '{to_schema}'")))?;
    let source_name = from_table.to_lowercase();
    let source_table = catalog.databases[source_at]
        .tables
        .iter()
        .position(|stored| stored.name.lowercase() == source_name)
        .ok_or_else(|| {
            SqlQueryError::unknown(format!("Unknown table '{from_schema}.{from_table}'"))
        })?;
    let target_name = to_table.to_lowercase();
    if catalog.databases[target_at]
        .tables
        .iter()
        .any(|stored| stored.name.lowercase() == target_name)
    {
        return Err(SqlQueryError::unknown(format!(
            "Table '{to_schema}.{to_table}' already exists"
        )));
    }
    let mut table = catalog.databases[source_at].tables.remove(source_table);
    table.name = CiString::new(to_table.to_owned());
    catalog.databases[target_at].tables.push(table);
    Ok(())
}

impl ClusterDdl for MockDdl {
    fn execute(&self, statement: &DdlStatement) -> Result<ClusterDdlReport, SqlQueryError> {
        let current = self.catalog.load();
        let mut next = ClusterCatalog {
            schema_version: current.schema_version + 1,
            databases: current.databases.clone(),
        };
        let find = |databases: &mut Vec<LoadedDatabase>, name: &str| -> Option<usize> {
            let name = name.to_lowercase();
            databases
                .iter()
                .position(|database| database.info.name.lowercase() == name)
        };
        let mut created_id = None;
        match statement {
            DdlStatement::CreateDatabase {
                name,
                if_not_exists,
                charset,
                collate,
            } => {
                if find(&mut next.databases, name).is_some() {
                    if *if_not_exists {
                        return Ok(ClusterDdlReport::AlreadySatisfied {
                            warning: None,
                            detail: format!("database `{name}` already exists"),
                        });
                    }
                    return Err(SqlQueryError::unknown(format!(
                        "Can't create database '{name}'; database exists"
                    )));
                }
                let id = self.allocate();
                created_id = Some(id);
                next.databases.push(LoadedDatabase {
                    info: DBInfo {
                        id,
                        name: CiString::new(name.clone()),
                        charset: charset.clone(),
                        collate: collate.clone(),
                        ..DBInfo::default()
                    },
                    tables: Vec::new(),
                });
            }
            DdlStatement::DropDatabase { name, if_exists } => {
                match find(&mut next.databases, name) {
                    Some(at) => {
                        next.databases.remove(at);
                    }
                    None if *if_exists => {
                        return Ok(ClusterDdlReport::AlreadySatisfied {
                            warning: None,
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
                let at = find(&mut next.databases, schema).ok_or_else(|| {
                    SqlQueryError::unknown(format!("Unknown database '{schema}'"))
                })?;
                let lowered = table.to_lowercase();
                if next.databases[at]
                    .tables
                    .iter()
                    .any(|stored| stored.name.lowercase() == lowered)
                {
                    if *if_not_exists {
                        return Ok(ClusterDdlReport::AlreadySatisfied {
                            warning: None,
                            detail: format!("table `{schema}`.`{table}` already exists"),
                        });
                    }
                    return Err(SqlQueryError::unknown(format!(
                        "Table '{schema}.{table}' already exists"
                    )));
                }
                let id = self.allocate();
                created_id = Some(id);
                let database = &next.databases[at].info;
                let mut info = build
                    .for_database(&database.charset, &database.collate)
                    .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
                info.id = id;
                next.databases[at].tables.push(info);
            }
            DdlStatement::CreateView {
                schema,
                name,
                or_replace,
                info,
            } => {
                let at = find(&mut next.databases, schema).ok_or_else(|| {
                    SqlQueryError::unknown(format!("Unknown database '{schema}'"))
                })?;
                let lowered = name.to_lowercase();
                let existing = next.databases[at]
                    .tables
                    .iter()
                    .position(|stored| stored.name.lowercase() == lowered);
                if existing.is_some() && !or_replace {
                    return Err(SqlQueryError::unknown(format!(
                        "Table '{schema}.{name}' already exists"
                    )));
                }
                if let Some(old) = existing {
                    next.databases[at].tables.remove(old);
                }
                let id = self.allocate();
                created_id = Some(id);
                let mut info = (**info).clone();
                info.id = id;
                next.databases[at].tables.push(info);
            }
            DdlStatement::DropView { names, if_exists } => {
                let mut missing = Vec::new();
                for (schema, name) in names {
                    let Some(at) = find(&mut next.databases, schema) else {
                        missing.push(format!("{schema}.{name}"));
                        continue;
                    };
                    let lowered = name.to_lowercase();
                    let Some(stored) = next.databases[at]
                        .tables
                        .iter()
                        .position(|stored| stored.name.lowercase() == lowered)
                    else {
                        missing.push(format!("{schema}.{name}"));
                        continue;
                    };
                    if next.databases[at].tables[stored].view.is_none() {
                        return Err(SqlQueryError::unknown(format!(
                            "'{schema}.{name}' is a base table, not a VIEW"
                        )));
                    }
                    next.databases[at].tables.remove(stored);
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
                let at = find(&mut next.databases, schema).ok_or_else(|| {
                    SqlQueryError::unknown(format!("Unknown database '{schema}'"))
                })?;
                let lowered = table.to_lowercase();
                let stored = next.databases[at]
                    .tables
                    .iter_mut()
                    .find(|stored| stored.name.lowercase() == lowered)
                    .ok_or_else(|| {
                        SqlQueryError::unknown(format!("Unknown table '{schema}.{table}'"))
                    })?;
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
                let at = find(&mut next.databases, schema).ok_or_else(|| {
                    SqlQueryError::unknown(format!("Unknown database '{schema}'"))
                })?;
                let lowered = table.to_lowercase();
                let stored = next.databases[at]
                    .tables
                    .iter_mut()
                    .find(|stored| stored.name.lowercase() == lowered)
                    .ok_or_else(|| {
                        SqlQueryError::unknown(format!("Unknown table '{schema}.{table}'"))
                    })?;
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
                let at = find(&mut next.databases, schema).ok_or_else(|| {
                    SqlQueryError::unknown(format!("Unknown database '{schema}'"))
                })?;
                let lowered = table.to_lowercase();
                let stored = next.databases[at]
                    .tables
                    .iter_mut()
                    .find(|stored| stored.name.lowercase() == lowered)
                    .ok_or_else(|| {
                        SqlQueryError::unknown(format!("Unknown table '{schema}.{table}'"))
                    })?;
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
                let at = find(&mut next.databases, schema).ok_or_else(|| {
                    SqlQueryError::unknown(format!("Unknown database '{schema}'"))
                })?;
                let lowered = table.to_lowercase();
                let found = next.databases[at]
                    .tables
                    .iter()
                    .position(|stored| stored.name.lowercase() == lowered);
                match found {
                    Some(index) => {
                        next.databases[at].tables.remove(index);
                    }
                    None if *if_exists => {
                        return Ok(ClusterDdlReport::AlreadySatisfied {
                            warning: None,
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
            } => mock_rename_table(&mut next, from_schema, from_table, to_schema, to_table)?,
            DdlStatement::RenameTables { pairs } => {
                for pair in pairs {
                    mock_rename_table(
                        &mut next,
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
            DdlStatement::CreateIndex { .. } | DdlStatement::DropIndex { .. } => {
                return Err(SqlQueryError::unknown(
                    "the mock catalog writer holds no rows, so it cannot model an index \
                     change's backfill"
                        .to_owned(),
                ))
            }
            DdlStatement::ModifyTableComment { .. }
            | DdlStatement::RebaseAutoIncrementId { .. }
            | DdlStatement::IgnoredTableOption { .. }
            | DdlStatement::OrderByColumns { .. }
            | DdlStatement::SetColumnDefault { .. }
            | DdlStatement::RenameIndex { .. }
            | DdlStatement::ModifySchemaCharsetAndCollate { .. }
            | DdlStatement::AlterIndexVisibility { .. }
            | DdlStatement::AddColumn { .. }
            | DdlStatement::ModifyColumn { .. }
            | DdlStatement::RenameColumn { .. }
            | DdlStatement::DropColumn { .. }
            | DdlStatement::MultiSchemaChange { .. }
            | DdlStatement::TruncateTable { .. } => {
                return Err(SqlQueryError::unknown(
                    "the mock catalog writer does not model column or truncate changes; \
                     the plan tests in cluster_ddl_source own those"
                        .to_owned(),
                ))
            }
        }
        let schema_version = next.schema_version;
        // The real writer refreshes the node's catalog inline, before it
        // answers; so does this one.
        self.catalog.store(next);
        self.applied.fetch_add(1, Ordering::AcqRel);
        Ok(ClusterDdlReport::Applied {
            schema_version,
            created_id,
            warning: None,
        })
    }
}

/// The account seam without a cluster: the "stored" accounts are one
/// registry, and a change is persisted by publishing the scratch copy into
/// it. That is the whole routing contract -- read the stored table, run
/// the statement against a scratch copy, publish only on a successful
/// persist -- with the 2PC replaced by a switch a test can flip.
pub(super) struct MockAccountWriter {
    /// What the "cluster" stores.
    pub(super) stored: PrivilegeRegistry,
    /// The node's live table, which only a committed change reaches.
    pub(super) live: PrivilegeRegistry,
    /// Whether the persist step succeeds, so a test can prove that a
    /// failed persist changes neither side.
    pub(super) persists: Arc<AtomicBool>,
}

impl MockAccountWriter {
    pub(super) fn new() -> Self {
        let stored = PrivilegeRegistry::default();
        let live = PrivilegeRegistry::default();
        Self {
            stored,
            live,
            persists: Arc::new(AtomicBool::new(true)),
        }
    }
}

impl ClusterAccountWriter for MockAccountWriter {
    fn begin(&self) -> Result<Box<dyn PendingAccountChange>, String> {
        // The scratch table starts as a copy of what the cluster stores,
        // which is what makes the statement validate against the cluster's
        // truth rather than this node's.
        let scratch = PrivilegeRegistry::default();
        scratch.replace_from(&clone_registry(&self.stored));
        Ok(Box::new(MockPendingChange {
            scratch,
            stored: self.stored.clone(),
            live: self.live.clone(),
            persists: Arc::clone(&self.persists),
        }))
    }
}

pub(super) struct MockPendingChange {
    pub(super) scratch: PrivilegeRegistry,
    pub(super) stored: PrivilegeRegistry,
    pub(super) live: PrivilegeRegistry,
    pub(super) persists: Arc<AtomicBool>,
}

impl PendingAccountChange for MockPendingChange {
    fn registry(&self) -> PrivilegeRegistry {
        self.scratch.clone()
    }

    fn commit(self: Box<Self>) -> Result<Vec<String>, SqlQueryError> {
        if !self.persists.load(Ordering::Acquire) {
            return Err(SqlQueryError::unknown("the persist was rejected"));
        }
        let changed: Vec<String> = self
            .scratch
            .accounts()
            .into_iter()
            .map(|(user, host)| format!("'{user}'@'{host}'"))
            .collect();
        self.stored.replace_from(&clone_registry(&self.scratch));
        self.live.replace_from(&clone_registry(&self.scratch));
        Ok(changed)
    }
}

/// The sysvar seam without a cluster, mirroring [`MockAccountWriter`]
/// exactly: the "stored" overrides are one [`GlobalSysvars`] table, and a
/// change is persisted by publishing the scratch copy into it.
pub(super) struct MockSysvarWriter {
    pub(super) stored: GlobalSysvars,
    pub(super) live: GlobalSysvars,
    pub(super) persists: Arc<AtomicBool>,
}

impl MockSysvarWriter {
    pub(super) fn new() -> Self {
        Self {
            stored: GlobalSysvars::default(),
            live: GlobalSysvars::default(),
            persists: Arc::new(AtomicBool::new(true)),
        }
    }
}

impl crate::cluster_sysvar_seam::ClusterSysvarWriter for MockSysvarWriter {
    fn begin(&self) -> Result<Box<dyn crate::cluster_sysvar_seam::PendingSysvarChange>, String> {
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

impl crate::cluster_sysvar_seam::PendingSysvarChange for MockPendingSysvarChange {
    fn table(&self) -> GlobalSysvars {
        self.scratch.clone()
    }

    fn commit(self: Box<Self>) -> Result<Vec<String>, SqlQueryError> {
        if !self.persists.load(Ordering::Acquire) {
            return Err(SqlQueryError::unknown("the persist was rejected"));
        }
        let before = self.stored.overrides();
        let after = self.scratch.overrides();
        let changed: Vec<String> = after
            .iter()
            .filter(|(name, value)| before.get(*name) != Some(*value))
            .map(|(name, _)| name.clone())
            .chain(
                before
                    .keys()
                    .filter(|name| !after.contains_key(*name))
                    .cloned(),
            )
            .collect();
        self.stored
            .replace_from(&GlobalSysvars::from_cluster_rows(after));
        self.live
            .replace_from(&GlobalSysvars::from_cluster_rows(self.stored.overrides()));
        Ok(changed)
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
    ) -> Result<tidb_exec::real_tikv_analyze::ClusterAnalyzeReport, SqlQueryError> {
        Err(SqlQueryError::unknown(format!(
            "the mock node stores no statistics for `{}`.`{}`",
            statement.schema, statement.table
        )))
    }
}
