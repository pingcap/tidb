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
use tidb_exec::cluster_catalog::{ClusterCatalog, LoadedDatabase};
use tidb_model::db::DBInfo;
use tidb_model::TableInfo;

/// The catalog writer, offline: the meta-key encoding and the 2PC are
/// proven by `tidb-exec`'s own `cluster_ddl_source` tests, so what is
/// modelled here is the part this node owns -- the published catalog
/// moving, at a new schema version, from the statement's own thread.
///
/// The `TableInfo` it publishes is not invented: it is the template
/// `lower_ddl`/`build_table_info` produced from the statement text, which
/// is what the real path writes too.
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
            } => {
                if find(&mut next.databases, name).is_some() {
                    if *if_not_exists {
                        return Ok(ClusterDdlReport::AlreadySatisfied {
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
                template,
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
                            detail: format!("table `{schema}`.`{table}` already exists"),
                        });
                    }
                    return Err(SqlQueryError::unknown(format!(
                        "Table '{schema}.{table}' already exists"
                    )));
                }
                let id = self.allocate();
                created_id = Some(id);
                let mut info = TableInfo::clone(template);
                info.id = id;
                next.databases[at].tables.push(info);
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
        }
        let schema_version = next.schema_version;
        // The real writer refreshes the node's catalog inline, before it
        // answers; so does this one.
        self.catalog.store(next);
        self.applied.fetch_add(1, Ordering::AcqRel);
        Ok(ClusterDdlReport::Applied {
            schema_version,
            created_id,
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
