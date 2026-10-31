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

//! The catalog: the position Go's `infoschema` occupies, reduced to what this
//! tier resolves a name against -- databases, their tables, a table's backing
//! store, and views.
//!
//! Names are case-insensitive as in MySQL, and a bare table name resolves in
//! the session's current database ([`split_table_path`]) -- an empty one being
//! Go's `ErrNoDB`. [`TableResolver`] is the other half of the same lookup:
//! once a name has reached a table, it binds the column references in that
//! table's scope.

use tidb_table::sequence::SequenceDef;
use std::collections::HashMap;
use std::sync::Arc;
use tidb_datatype::{Datum, FieldType};
use tidb_table::kv_table::KvTable;
use tidb_driver_error::DriverError;
use tidb_table::kv_table::TableCharset;
use tidb_hack::GoCaseCompare;
use tidb_hack::GoToLower;

// Cache versions must distinguish independently rebuilt catalogs and schema
// mutations on divergent clones, not just count mutations in one instance.
// Clones/snapshots retain their version; DML does not allocate a new one.
fn next_metadata_version() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |version| {
        version.checked_add(1)
    })
    .expect("catalog metadata version exhausted")
}

/// The folded schema/table identity, corresponding to Go's two `CIStr.L`
/// fields. Construct once when resolving retained plan metadata; raw-name
/// catalog entrypoints normalize through the same constructor.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CatalogTableKey {
    database: String,
    table: String,
}

impl CatalogTableKey {
    pub fn new(database: &str, table: &str) -> Self {
        Self {
            database: database.go_to_lower(),
            table: table.go_to_lower(),
        }
    }
}

pub mod infoschema_errors;
pub mod metric_table_def;
pub mod session_tables;
pub mod virtual_tables;
pub mod infoschema_meta;
pub mod metricsschema;
pub mod tidb_decode_key;
use tidb_decode_key::TidbDecodeKeySnapshot;
pub mod planner_view;
pub mod table_source;
pub mod sequence;
pub use sequence::SequenceSnapshot;
pub use planner_view::{PlannerCatalog, PlannerSchemaView};
pub mod statistics_access;
pub mod temporary;
pub mod cluster_catalog;

/// An in-memory table: named, typed columns plus row values.
#[derive(Clone, Debug, Default)]
pub struct MemTable {
    /// The columns, in row order: `(name, type)`.
    pub columns: Vec<(String, FieldType)>,
    /// The rows (one `Datum` per column).
    pub rows: Vec<Vec<Datum>>,
}

/// Splits a table reference into its schema and table names. A bare name
/// resolves in the default schema; `db.t` names its schema explicitly.
///
/// Splits a table path for executor-adjacent statement arms.
pub fn split_table_path_pub<'a>(
    path: &'a [String],
    current_db: &'a str,
) -> Result<(&'a str, &'a str), DriverError> {
    split_table_path(path, current_db)
}

/// `current_db` is the session's selected schema (Go `SessionVars.CurrentDB`);
/// an empty one is Go's `ErrNoDB`.
pub fn split_table_path<'a>(
    path: &'a [String],
    current_db: &'a str,
) -> Result<(&'a str, &'a str), DriverError> {
    match path {
        [name] => {
            if current_db.is_empty() {
                return Err(DriverError::from(
                    tidb_plannererrors::ERR_NO_DB.generate_with_stack_by_args(&[]),
                ));
            }
            Ok((current_db, name))
        }
        [database, name] => Ok((database, name)),
        _ => Err(DriverError::unsupported("empty table name")),
    }
}

/// Go's default schema: TiDB's bootstrap runs
/// `CREATE DATABASE IF NOT EXISTS test`, so a fresh server always has it and
/// a connection with no explicit database lands there.
pub const DEFAULT_DATABASE: &str = "test";

/// Go `metadef.SystemDB`: the schema TiDB's bootstrap creates its own tables
/// in. Spelled lower case, which is the name Go stores and reports.
pub const SYSTEM_DATABASE: &str = "mysql";

/// One schema: Go `model.DBInfo`, reduced to the metadata this catalog serves.
#[derive(Clone, Debug, Default)]
struct Database {
    /// Go `DBInfo.ID`, retained for physical-key diagnostics.
    id: i64,
    /// The name as written, for `SHOW DATABASES` output.
    name: String,
    /// The defaults inherited by tables created without explicit options.
    charset: TableCharset,
    /// Go `DBInfo.PlacementPolicyRef`: the schema's default placement,
    /// which a table created without its own inherits.
    placement_policy: Option<tidb_model::PolicyRefInfo>,
    tables: HashMap<String, std::sync::Arc<TableEntry>>,
}

/// Go `infoschema.InfoSchema`: one schema image -- databases, their tables,
/// views and sequences, placement policies and resource groups -- shared by
/// every session that reads it. Database and table names are
/// case-insensitive, as in MySQL.
#[derive(Clone, Debug)]
pub struct InfoSchema {
    // Snapshot creation shares schema maps. Mutation detaches only the outer
    // name map and the database it touches, retaining all other table maps.
    databases: Arc<HashMap<String, Arc<Database>>>,
    /// Go `infoschema`'s policy map, keyed by the FOLDED policy name.
    ///
    /// A placement policy is a schema object in its own right, not an
    /// attribute of a table: `DROP PLACEMENT POLICY` has to know whether any
    /// table or partition still references it, and `ALTER PLACEMENT POLICY`
    /// changes what every referencing object means at once. Both need the
    /// policy to live here rather than being copied into each user.
    policies: HashMap<String, tidb_model::PolicyInfo>,
    /// Go `infoschema`'s resource groups by lowercase name, `default`
    /// always among them.
    resource_groups: HashMap<String, tidb_model::ResourceGroupInfo>,
    /// Sticky indication that a table with a foreign key has existed in
    /// this catalog. DML asks this on every write; keeping the bit avoids
    /// walking and inspecting every table in the common no-FK case. It is
    /// intentionally monotonic: stale `true` only makes the uncommon full
    /// referring-table scan run, while stale `false` would be incorrect.
    foreign_keys_present: bool,
    /// Sticky indication that some table of this catalog has carried a
    /// `LOCK TABLES` lock, which every statement's `CheckTableLock` asks;
    /// monotonic for the same reason as `foreign_keys_present`.
    table_locks_present: bool,
    /// Bumped by every mutation that actually CHANGED something, so a
    /// transaction can detect that the shared catalog moved under it (Go
    /// detects the same at commit through TiKV's optimistic conflict check on
    /// the written keys).
    ///
    /// "Actually changed" is the invariant, not a detail: every mutator used
    /// to bump on entry and then return `false` for the no-op cases, so a
    /// `CREATE DATABASE IF NOT EXISTS` over an existing schema, or any DDL
    /// that failed its precondition, aborted a concurrent transaction at
    /// commit (`tidb-session`'s `txn.rs` compares this against the version it
    /// started from). Go advances its schema version per COMPLETED DDL job.
    version: u64,
    /// Bumped ONLY by mutations that change schema METADATA a key decoder
    /// reads: databases registered or dropped, tables registered, renamed,
    /// dropped, or re-registered with new columns. Deliberately NOT bumped by
    /// [`Self::get_mut_in`] (every write path takes it, and Go's schema
    /// version -- what row-decode caches key on -- moves only on DDL), nor by
    /// staged-write marks, statistics refreshes, or id allocation. Bumped at
    /// each mutator's ENTRY, so a call that declines still invalidates: the
    /// cost of a spurious rebuild is one snapshot walk, never staleness.
    metadata_version: u64,
    /// The immutable domain index view belongs to this metadata version.
    /// Catalog clones share the view; DDL invalidates only the mutated owner.
    latest_index_schema: std::sync::OnceLock<Arc<crate::table_source::LatestIndexSchema>>,
    /// Memoized answer of walking EVERY database's table map for GLOBAL
    /// temporary tables, valid as of one `metadata_version`. Without the memo
    /// the walk runs twice per statement (`txn`'s overlay guard swaps global
    /// row storage in and out), which is pure overhead for the sessions that
    /// hold no temporary tables -- every OLTP workload.
    temporary_sweep: Option<(u64, Vec<(String, String)>)>,
}

/// Go `infoschema.SessionExtendedInfoSchema`: the shared [`InfoSchema`] with
/// the session's LOCAL temporary tables attached for a statement, and the
/// lookup indexes built over both. Every statement plans and executes
/// against one of these.
#[derive(Clone, Debug)]
pub struct Catalog {
    /// The shared schema image.
    schema: InfoSchema,    /// The planner's view of this catalog image, shared by clones like the
    /// index view and dropped by every mutator that moves [`Self::version`]
    /// or the metadata epoch.
    planner_view: std::sync::OnceLock<Arc<PlannerSchemaView>>,
    /// Go InfoSchema's table-ID index. Names resolve against this image's
    /// current entries without retaining a second copy of table storage.
    pub table_id_names: Arc<std::sync::OnceLock<HashMap<i64, Arc<CatalogTableKey>>>>,
    /// Go `SessionExtendedInfoSchema.LocalTemporaryTables`: the LOCAL
    /// temporary tables of the session running a statement, attached for
    /// that statement. A name or an id resolves here before the shared
    /// schema (`SessionExtendedInfoSchema.TableByName` / `TableByID`),
    /// whether or not that schema still exists -- Go's `SessionTables` owns
    /// each table's `DBInfo` -- and no enumeration of the shared schema lists
    /// them.
    local_temporary: Vec<(CatalogTableKey, std::sync::Arc<TableEntry>)>,
    /// While a session's LOCAL temporary tables are attached: the metadata
    /// version of the image without them, and the version attaching gave.
    /// Go's `SessionExtendedInfoSchema` keeps the shared infoschema's
    /// `SchemaMetaVersion`, so a temporary table never moves a plan-cache key
    /// (see [`Catalog::schema_version`]).
    overlay_versions: Option<(u64, u64)>,
    /// The `TableInfo` facts a statistics cache miss reads, by table /
    /// partition id; the statistics themselves live in the domain's
    /// `StatsHandle`, not in this image.
    statistics_schemas: Arc<std::sync::OnceLock<HashMap<i64, Arc<tidb_stats::PseudoStatsTable>>>>,
}

impl Catalog {
    /// Go `infoschema.NewBuilder(...).InitWithDBInfos` before any stored
    /// schema: only the memory schemas Go's `initVirtualTables` builds from
    /// its `drivers` -- `INFORMATION_SCHEMA`, `PERFORMANCE_SCHEMA` and
    /// `METRICS_SCHEMA` with their tables. Every stored schema (`test`,
    /// `mysql`, `sys`, the user's) and every policy and resource group comes
    /// from meta through [`Self::register_database_with_id_and_charset`],
    /// [`Self::set_policy`] and [`Self::set_resource_group`].
    #[must_use]
    pub fn new() -> Self {
        Self::virtual_schemas(true)
    }

    /// A schema image for tests that have no store behind them, Go
    /// `infoschema.MockInfoSchema`'s role: the memory schemas plus the
    /// stored schemas a bootstrapped TiDB holds -- `test`, `mysql` (as an
    /// object with no tables: this tier serves none of its rows, so naming
    /// one answers 1146 while `USE mysql` succeeds) and `sys` -- under
    /// fixed ids, and meta's implicit `default` resource group.
    #[must_use]
    pub fn mock() -> Self {
        let mut catalog = Self::new();
        for (id, name) in [(1, DEFAULT_DATABASE), (3, SYSTEM_DATABASE), (6, "sys")] {
            catalog.register_database_with_id_and_charset(name, id, TableCharset::default());
        }
        catalog.set_resource_group(tidb_model::ResourceGroupInfo::default_group());
        catalog
    }

    /// The memory schemas with none of their tables: the scratch Go's
    /// init-time builders (`infoschema.init`, `perfschema.Init`, the metric
    /// tables) fill once per process.
    pub fn memory_schema_scratch() -> Self {
        Self::virtual_schemas(false)
    }

    /// Go `RegisterVirtualTable` for a memory schema whose `tables` its
    /// init-time builder made: the schema joins every infoschema the DDL
    /// reads (`TableByName`), as Go's `initVirtualTables` adds it.
    pub fn register_virtual_schema(
        db_id: i64,
        name: &str,
        tables: &std::collections::HashMap<String, Arc<TableEntry>>,
    ) {
        let tables = tables
            .values()
            .filter_map(|entry| match &**entry {
                TableEntry::Kv(table) => Some(table.meta().read().clone()),
                _ => None,
            })
            .collect();
        crate::virtual_tables::register_virtual_table(
            crate::virtual_tables::VirtualSchema {
                db_info: tidb_model::db::DBInfo {
                    id: db_id,
                    name: tidb_ast::CiString::new(name.to_owned()),
                    charset: tidb_mysql::charset::DefaultCharset.to_owned(),
                    collate: tidb_mysql::charset::DefaultCollationName.to_owned(),
                    state: tidb_model::SchemaState::PUBLIC,
                    ..tidb_model::db::DBInfo::default()
                },
                tables,
            },
        );
    }

    /// The tables of `database` (a lowercase key), by stored name.
    pub fn tables_of(
        &self,
        database: &str,
    ) -> impl Iterator<Item = (&String, &Arc<TableEntry>)> {
        self.schema.databases
            .get(database)
            .into_iter()
            .flat_map(|database| database.tables.iter())
    }

    fn virtual_schemas(with_memory_tables: bool) -> Self {
        let mut databases = HashMap::new();
        databases.insert(
            "information_schema".to_owned(),
            Database {
                // Go `autoid.InformationSchemaDBID`: a memory schema's id has
                // the system flag, so it can never be a stored schema's id.
                id: (1 << 62) | 1,
                name: "INFORMATION_SCHEMA".to_owned(),
                charset: TableCharset::default(),
                placement_policy: None,
                // Go's `initVirtualTables` builds the memory schemas' tables
                // into the infoschema itself, so they are ordinary objects to
                // every name lookup; each schema's tables are built once per
                // process, as Go's init does.
                tables: if with_memory_tables {
                    crate::infoschema_meta::tables()
                } else {
                    HashMap::new()
                },
            },
        );
        // Go's metrics_schema carries 642 storage-backed metric tables whose
        // queries fail `query metric error: pd unavailable` (the metric
        // storage is absent) -- mirrored here as empty MemTables of the
        // same shape (oracle g-is).
        let (metric_tables, performance_tables) = if with_memory_tables {
            (
                crate::metricsschema::tables(),
                PERFORMANCE_SCHEMA_TABLES.get().map(|tables| tables()).unwrap_or_default(),
            )
        } else {
            (HashMap::new(), HashMap::new())
        };
        for (id, key, display, tables) in [
            // Go `autoid.MetricSchemaDBID` and `PerformanceSchemaDBID`.
            ((1 << 62) | 20000, "metrics_schema", "METRICS_SCHEMA", metric_tables),
            ((1 << 62) | 10000, "performance_schema", "PERFORMANCE_SCHEMA", performance_tables),
        ] {
            databases.insert(
                key.to_owned(),
                Database {
                    id,
                    name: display.to_owned(),
                    charset: TableCharset::default(),
                    placement_policy: None,
                    tables,
                },
            );
        }
        let catalog = Catalog {
            schema: InfoSchema {
                databases: Arc::new(
                    databases
                        .into_iter()
                        .map(|(name, database)| (name, Arc::new(database)))
                        .collect(),
                ),
                policies: HashMap::new(),
                resource_groups: HashMap::new(),
                version: 0,
                metadata_version: next_metadata_version(),
                latest_index_schema: std::sync::OnceLock::new(),
                temporary_sweep: None,
                foreign_keys_present: false,
                table_locks_present: false,
            },
            planner_view: std::sync::OnceLock::new(),
            table_id_names: Arc::default(),
            local_temporary: Vec::new(),
            overlay_versions: None,
            statistics_schemas: Arc::default(),
        };
        catalog
    }
}

/// A view: Go `model.TableInfo` whose `View` field is set. A view stores no
/// rows -- its `SELECT` is re-run whenever the name is read.
///
/// `columns` holds the names the view reports -- the explicit
/// `CREATE VIEW v (...)` list when one was written, the body's field names
/// otherwise -- paired with the types the body had at `CREATE VIEW`. The
/// names are fixed, but the types are not: Go re-plans the body for every
/// read and for every metadata answer, so `ALTER TABLE base MODIFY` shows
/// through immediately (captured: `DESCRIBE` and
/// `information_schema.columns` both report the new type). Use
/// [`view_column_list`] rather than these cached types wherever the answer is
/// user-visible.
///
/// NOT MODELLED (documented): enforcing `WITH CHECK OPTION` on write-through
/// -- the mode is recorded and reported, but this tier refuses writes through
/// a view outright, which is the only place the check would apply.
#[derive(Clone, Debug)]
pub struct ViewDef {
    /// The view name as written, for `SHOW CREATE VIEW`.
    pub name: String,
    /// The output columns, resolved when the view was created. The names are
    /// the explicit `CREATE VIEW v (...)` list when one was written.
    pub columns: Vec<(String, FieldType)>,
    /// The view's `SELECT`, canonicalized as Go stores it: every field
    /// explicitly aliased, every table reference schema-qualified.
    pub select_sql: String,
    /// The definer's user name (empty when the session has no user, which is
    /// this tier's only case).
    pub definer_user: String,
    /// The definer's host name.
    pub definer_host: String,
    /// The creator connection's client character set.
    pub character_set_client: String,
    /// The creator connection's collation.
    pub collation_connection: String,
    /// The `ALGORITHM` as written, defaulting to `UNDEFINED`.
    pub algorithm: String,
    /// The `SQL SECURITY` mode as written, defaulting to `DEFINER`.
    pub security: String,
    /// The `WITH CHECK OPTION` mode, `CASCADED` unless `LOCAL` was written.
    /// Go records it on every view, written or not, and reports it as
    /// `information_schema.views.CHECK_OPTION`; `SHOW CREATE VIEW` never
    /// prints it.
    pub check_option: String,
    /// Go `TableInfo.UpdateTS`, the TSO of the job that created it (0 when
    /// none was recorded).
    pub update_ts: u64,
    /// Go `TableInfo.ID` (0 when the tier allocated none).
    pub id: i64,
}

/// A catalog table's backing store.
#[derive(Clone, Debug)]
pub enum TableEntry {
    /// Read-only materialized rows for virtual tables and query fixtures.
    Mem(MemTable),
    /// Rows stored as real TiKV-format bytes (see [`crate::kv_table`]).
    ///
    /// `Arc`-wrapped so a transient, read-only view of an already-private
    /// (already copy-on-write'd for this transaction, or never yet written
    /// to) table can be handed out for free: see
    /// [`Catalog::physical_kv_table_by_id`]. A caller that mutates goes
    /// through [`std::sync::Arc::make_mut`] the same way [`Self::Kv`]'s own
    /// enclosing `Arc<TableEntry>` already does, so a table already
    /// EXCLUSIVELY owned by this transaction (the overwhelmingly common
    /// case -- nobody else's snapshot points at the same instance) mutates
    /// in place with no clone at all, and one still shared with another
    /// transaction's snapshot forks away first, exactly as before this
    /// change.
    Kv(std::sync::Arc<KvTable>),
    /// A view: a stored `SELECT` rather than stored rows.
    View(ViewDef),
    /// A sequence: a counter rather than rows. It lives in the TABLE
    /// namespace because Go stores it as a `model.TableInfo` with `Sequence`
    /// set -- which is why `CREATE TABLE` over a sequence name collides, and
    /// why `SELECT * FROM <sequence>` and `DROP TABLE <sequence>` are
    /// errors rather than name-not-found.
    Sequence(SequenceDef),
}

impl TableEntry {
    /// The table's columns as `(name, type)` in schema order.
    ///
    /// HIDDEN columns are not here. This is the schema every user-facing
    /// enumeration is built from -- `SELECT *`, an `INSERT`'s arity, name
    /// resolution -- so the hidden column an expression index was rewritten
    /// into is excluded once, at the source. Its physical offset is unchanged
    /// by the exclusion because hidden columns are the tail (see
    /// [`crate::expression_index`]).
    pub fn columns(&self) -> impl Iterator<Item = (&str, &FieldType)> {
        let (pairs, kv): (&[(String, FieldType)], &[tidb_table::kv_table::KvColumn]) = match self {
            TableEntry::Mem(mem) => (&mem.columns, &[]),
            TableEntry::Kv(kv) => (&[], kv.visible_columns()),
            TableEntry::View(view) => (&view.columns, &[]),
            // Go gives a sequence a fixed one-column schema, but no statement
            // this tier accepts ever reads it: `SELECT * FROM <sequence>` is
            // refused, and `nextval` reaches the allocator directly.
            TableEntry::Sequence(_) => (&[], &[]),
        };
        pairs
            .iter()
            .map(|(name, field_type)| (name.as_str(), field_type))
            .chain(
                kv.iter()
                    .map(|column| (column.name.as_str(), &column.field_type)),
            )
    }

    /// An owned schema for consumers that retain or modify column types.
    pub fn column_list(&self) -> Vec<(String, FieldType)> {
        self.columns()
            .map(|(name, field_type)| (name.to_owned(), field_type.clone()))
            .collect()
    }

    /// The table's `(name, type)` pairs in schema order, for callers outside
    /// this crate that must resolve a written column name to its TYPE — the
    /// non-prepared plan cache's filter-column rule, which refuses a filter
    /// over a JSON, `ENUM`, `SET` or `BIT` column.
    #[must_use]
    pub fn column_types(&self) -> Vec<(String, FieldType)> {
        self.column_list()
    }

    /// The table's column names in schema order, for the callers outside
    /// this crate that resolve a written column name against the table
    /// (`GRANT SELECT (a) ON db.t`).
    #[must_use]
    pub fn column_names(&self) -> Vec<String> {
        self.columns().map(|(name, _)| name.to_owned()).collect()
    }

    /// Whether this entry is a view, which decides which of MySQL's two
    /// object kinds a statement is allowed to name.
    #[must_use]
    pub fn is_view(&self) -> bool {
        matches!(self, TableEntry::View(_))
    }

    /// Whether this entry is a sequence, which is the other non-row object
    /// kind a statement may name by mistake.
    #[must_use]
    pub fn is_sequence(&self) -> bool {
        matches!(self, TableEntry::Sequence(_))
    }
}

impl Catalog {
    fn database_mut(&mut self, folded_name: &str) -> Option<&mut Database> {
        Arc::make_mut(&mut self.schema.databases)
            .get_mut(folded_name)
            .map(Arc::make_mut)
    }

    /// Go `MemBuffer.Staging()` for the tables whose rows live in this
    /// catalog's in-process stores (the temporary tables): opens one stage
    /// on each. Tables a session-owned savepoint covers stage nothing here.
    pub fn begin_statement_stage(&mut self) -> StatementStage {
        let needs = |entry: &Arc<TableEntry>| matches!(entry.as_ref(), TableEntry::Kv(table) if !table.has_external_statement_rollback());
        let mut at: Vec<StagedTable> = Vec::new();
        for (database, catalog) in self.schema.databases.iter() {
            for (name, entry) in &catalog.tables {
                if needs(entry) {
                    at.push(StagedTable::Persistent(database.clone(), name.clone()));
                }
            }
        }
        for (index, (_, entry)) in self.local_temporary.iter().enumerate() {
            if needs(entry) {
                at.push(StagedTable::Local(index));
            }
        }
        let mut staged = Vec::with_capacity(at.len());
        for at in at {
            if let Some(TableEntry::Kv(table)) = self.staged_entry_mut(&at) {
                let handle = Arc::make_mut(table).store_mut().staging();
                staged.push((at, handle));
            }
        }
        StatementStage(staged)
    }

    /// Go `MemBuffer.Release(h)` when `keep`, `Cleanup(h)` otherwise, for
    /// every stage [`Self::begin_statement_stage`] opened.
    pub fn end_statement_stage(&mut self, stage: StatementStage, keep: bool) {
        for (at, handle) in stage.0 {
            if let Some(TableEntry::Kv(table)) = self.staged_entry_mut(&at) {
                if keep {
                    Arc::make_mut(table).store_mut().release(handle);
                } else {
                    Arc::make_mut(table).store_mut().cleanup(handle);
                }
            }
        }
    }

    fn staged_entry_mut(&mut self, at: &StagedTable) -> Option<&mut TableEntry> {
        let entry = match at {
            StagedTable::Persistent(database, name) => {
                Arc::make_mut(Arc::make_mut(&mut self.schema.databases).get_mut(database)?)
                    .tables
                    .get_mut(name)?
            }
            StagedTable::Local(index) => &mut self.local_temporary.get_mut(*index)?.1,
        };
        Some(Arc::make_mut(entry))
    }

    /// Registers a matrix-backed `table` in the default database.
    ///
    /// # Panics
    /// If `test` has been dropped. Every caller is a fixture or a bootstrap
    /// step that runs against a freshly [`Default`]ed catalog, where `test`
    /// exists by construction; a statement-driven create names its schema and
    /// goes through [`Catalog::register_kv_in`], which reports 1049 instead.
    pub fn register(&mut self, name: &str, table: MemTable) {
        self.bump_metadata_version();
        self.register_in(DEFAULT_DATABASE, name, TableEntry::Mem(table))
            .expect("the default database exists in a freshly built catalog");
    }

    /// Registers a TiKV-format-byte-backed `table` in the default database.
    ///
    /// # Panics
    /// As [`Catalog::register`].
    pub fn register_kv(&mut self, name: &str, table: KvTable) {
        self.bump_metadata_version();
        self.register_in(DEFAULT_DATABASE, name, TableEntry::Kv(Arc::new(table)))
            .expect("the default database exists in a freshly built catalog");
    }

    /// Registers `table` in `database`, or reports Go's 1049 when that schema
    /// does not exist.
    ///
    /// The signature is the whole point: a `TableEntry` can only be handed to
    /// a schema that was found, so "the table was accepted and dropped on the
    /// floor" is not a state a caller can reach. It used to return `()` and
    /// silently do nothing for an absent schema, which made
    /// `CREATE TABLE nosuchdb.t` answer success and create nothing.
    fn register_in(
        &mut self,
        database: &str,
        name: &str,
        mut table: TableEntry,
    ) -> Result<(), DriverError> {
        let table_is_locked = matches!(&table, TableEntry::Kv(table) if table.lock().is_some());
        let table_has_foreign_keys = matches!(
            &table,
            TableEntry::Kv(table) if !table.foreign_keys().is_empty()
        );
        let schema = self.database_mut(&database.go_to_lower()).ok_or_else(|| {
            DriverError::from(
                crate::infoschema_errors::ERR_DATABASE_NOT_EXISTS
                    .generate_with_stack_by_args(&[(database.to_owned()).into()]),
            )
        })?;
        // Go's infoschema key and `TableInfo.Name` are one identity. Keep the
        // same invariant here so every metadata consumer (including the
        // shared planner) observes the name under which the table was
        // registered. Hand-built tables intentionally start unnamed.
        if let TableEntry::Kv(table) = &mut table {
            Arc::make_mut(table).set_name(name);
        }
        schema
            .tables
            .insert(name.go_to_lower(), std::sync::Arc::new(table));
        self.schema.foreign_keys_present |= table_has_foreign_keys;
        self.schema.table_locks_present |= table_is_locked;
        self.bump_version();
        Ok(())
    }

    /// Every database name, sorted, with `information_schema` first when it
    /// exists -- Go's `fetchShowDatabases` ordering.
    #[must_use]
    pub fn database_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .schema.databases
            .values()
            .map(|database| database.name.clone())
            .collect();
        names.sort();
        if let Some(position) = names
            .iter()
            .position(|name| name.go_equal_fold("information_schema"))
        {
            let front = names.remove(position);
            names.insert(0, front);
        }
        names
    }

    /// Every table name in `database`, sorted as Go's `fetchShowTables` sorts
    /// them. `None` when the database does not exist.
    ///
    /// LOCAL temporary tables are not here: they are not in the infoschema
    /// an enumeration reads (`SchemaSimpleTableInfos`).
    ///
    /// The names are Go's `TableInfo.Name.O`, the spelling the table was
    /// created (or renamed) with, in byte order as Go's `fetchShowTables`
    /// sorts them; the catalog's keys are lower-cased and only for lookup.
    #[must_use]
    pub fn table_names(&self, database: &str) -> Option<Vec<String>> {
        let database = self.schema.databases.get(&database.go_to_lower())?;
        let is_information_schema = database
            .name
            .go_lower_eq(crate::infoschema_meta::INFORMATION_SCHEMA);
        let mut names: Vec<String> = database
            .tables
            .iter()
            .filter_map(|(key, entry)| match entry.as_ref() {
                TableEntry::Kv(table) => Some(table.name()),
                TableEntry::View(view) => Some(view.name.clone()),
                TableEntry::Sequence(sequence) => Some(sequence.name.clone()),
                // A memory table carries no name of its own: its key, or the
                // canonical spelling of an information_schema table.
                TableEntry::Mem(_) if is_information_schema => Some(
                    crate::infoschema_meta::canonical_table_name(key)
                        .map_or_else(|| key.clone(), str::to_owned),
                ),
                TableEntry::Mem(_) => Some(key.clone()),
            })
            .collect();
        names.sort();
        Some(names)
    }

    /// Whether `database` exists (Go `is.SchemaExists`).
    #[must_use]
    pub fn has_database(&self, database: &str) -> bool {
        self.schema.databases.contains_key(&database.go_to_lower())
    }

    /// The effective defaults stored on a database.
    #[must_use]
    pub fn database_charset(&self, database: &str) -> Option<TableCharset> {
        self.schema.databases
            .get(&database.go_to_lower())
            .map(|database| database.charset)
    }

    /// Go `DBInfo.PlacementPolicyRef` of `database`.
    #[must_use]
    pub fn database_placement_policy(&self, database: &str) -> Option<&tidb_model::PolicyRefInfo> {
        self.schema.databases
            .get(&database.go_to_lower())?
            .placement_policy
            .as_ref()
    }

    /// Go `onModifySchemaDefaultPlacement`'s `DBInfo` edit (or the loader
    /// restoring a stored one); `false` for a database that does not exist.
    pub fn set_database_placement_policy(
        &mut self,
        database: &str,
        policy: Option<tidb_model::PolicyRefInfo>,
    ) -> bool {
        self.bump_metadata_version();
        let Some(schema) = self.database_mut(&database.go_to_lower()) else {
            return false;
        };
        schema.placement_policy = policy;
        self.bump_version();
        true
    }

    /// The stored name and defaults needed by `SHOW CREATE DATABASE`.
    #[must_use]
    pub fn database_definition(&self, database: &str) -> Option<(String, TableCharset)> {
        self.schema.databases
            .get(&database.go_to_lower())
            .map(|database| (database.name.clone(), database.charset))
    }

    /// Go `infoschema.PolicyByName`: the policy a name refers to, folded.
    #[must_use]
    pub fn policy(&self, name: &str) -> Option<&tidb_model::PolicyInfo> {
        self.schema.policies
            .get(&tidb_ast::CiString::new(name).lowercase().to_owned())
    }

    /// Every policy, for `information_schema` and `SHOW` surfaces.
    #[must_use]
    pub fn policies(&self) -> impl Iterator<Item = &tidb_model::PolicyInfo> {
        self.schema.policies.values()
    }

    /// Go `infoschema.ResourceGroupByName`, folded.
    #[must_use]
    pub fn resource_group(&self, name: &str) -> Option<&tidb_model::ResourceGroupInfo> {
        self.schema.resource_groups.get(&name.go_to_lower())
    }

    /// Every resource group, for `information_schema.RESOURCE_GROUPS`.
    pub fn resource_groups(&self) -> impl Iterator<Item = &tidb_model::ResourceGroupInfo> {
        self.schema.resource_groups.values()
    }

    /// Go `infoSchemaMisc.setResourceGroup`: a group loaded from meta, as
    /// stored.
    pub fn set_resource_group(&mut self, group: tidb_model::ResourceGroupInfo) {
        self.bump_metadata_version();
        self.schema.resource_groups
            .insert(group.name.lowercase().to_owned(), group);
        self.bump_version();
    }

    /// Go `infoSchemaMisc.setPolicy`: a policy loaded from meta, as stored
    /// (the references to it are by id).
    pub fn set_policy(&mut self, policy: tidb_model::PolicyInfo) {
        self.schema.policies
            .insert(policy.name.lowercase().to_owned(), policy);
    }

    /// Go `CheckPlacementPolicyNotInUseFromInfoSchema`: the first table or
    /// partition still pointing at `name`, if any.
    ///
    /// Go refuses `DROP PLACEMENT POLICY` while anything references it
    /// (8241), because dropping it would leave those objects naming a policy
    /// that no longer exists.
    #[must_use]
    pub fn policy_in_use(&self, name: &str) -> bool {
        let folded = tidb_ast::CiString::new(name).lowercase().to_owned();
        self.schema.databases.values().any(|database| {
            // Go `checkPlacementPolicyNotInUseFromInfoSchema` checks each
            // schema's default before its tables.
            database
                .placement_policy
                .as_ref()
                .is_some_and(|reference| reference.name.lowercase() == folded)
                || database.tables.values().any(|entry| {
                    let crate::TableEntry::Kv(table) = &**entry else {
                        return false;
                    };
                    if table
                        .placement_policy()
                        .is_some_and(|reference| reference.name.lowercase() == folded)
                    {
                        return true;
                    }
                    table.partition().is_some_and(|partition| {
                        partition.definitions.iter().any(|definition| {
                            definition
                                .placement_policy
                                .as_ref()
                                .is_some_and(|reference| reference.name.lowercase() == folded)
                        })
                    })
                })
        })
    }

    /// Go `infoschema.Builder.createSchemaTablesForDB`'s schema half: a
    /// database loaded from meta, under its stored id, charset and collation.
    /// The mock catalog already contains `test`, `mysql` and `sys`; loading
    /// one of those replaces its synthetic id.
    ///
    /// Go's `DBInfo.Charset`/`Collate` reach `SHOW CREATE DATABASE` and
    /// `information_schema.schemata`, and are the default a table created
    /// without its own charset inherits. A loader that dropped them made
    /// `ALTER DATABASE ... CHARACTER SET` apply to the stored catalog and
    /// then be invisible to every reader.
    pub fn register_database_with_id_and_charset(
        &mut self,
        database: &str,
        id: i64,
        charset: TableCharset,
    ) -> bool {
        self.bump_metadata_version();
        let key = database.go_to_lower();
        if let Some(existing) = self.database_mut(&key) {
            let changed =
                existing.id != id || existing.name != database || existing.charset != charset;
            existing.id = id;
            existing.name = database.to_owned();
            existing.charset = charset;
            self.schema.version += u64::from(changed);
            return false;
        }
        Arc::make_mut(&mut self.schema.databases).insert(
            key,
            Arc::new(Database {
                id,
                name: database.to_owned(),
                charset,
                placement_policy: None,
                tables: HashMap::new(),
            }),
        );
        self.bump_version();
        true
    }

    /// Resolves a table in `database`.
    /// The CLUSTER table id behind a stored table's name, or `None` for a
    /// name that is no stored table here (a view, a CTE, a memory table, an
    /// unknown name). The id is the one TiKV keys carry and the one Go's
    /// `mysql.tidb_mdl_info.table_ids` names, which is what the metadata-lock
    /// gate matches on.
    #[must_use]
    pub fn stored_table_id(&self, database: &str, name: &str) -> Option<i64> {
        match self.get_in(database, name)? {
            TableEntry::Kv(table) => Some(table.table_id()),
            _ => None,
        }
    }

    /// Go `preprocessor.tableByName` then `tryLockMDLAndUpdateSchemaIfNecessary`
    /// for one name the statement writes: the id it locks, `Some(None)` for
    /// an object with no TiKV table id (a memory table, a sequence, a view
    /// with no stored id), and `None` when the name resolves to nothing. The
    /// tables a view reads are locked by the planner as it builds the view
    /// (`buildDataSource`).
    pub fn related_table_id_for_mdl(&self, database: &str, name: &str) -> Option<Option<i64>> {
        match self.get_in(database, name)? {
            TableEntry::Kv(table) => Some(Some(table.table_id())),
            TableEntry::View(view) => Some((view.id != 0).then_some(view.id)),
            TableEntry::Mem(_) | TableEntry::Sequence(_) => Some(None),
        }
    }

    pub fn get_in(&self, database: &str, name: &str) -> Option<&TableEntry> {
        self.get_by_key(&CatalogTableKey::new(database, name))
    }

    pub fn get_by_key(&self, key: &CatalogTableKey) -> Option<&TableEntry> {
        self.table_handle_ref(key).map(|entry| &**entry)
    }

    /// Retains immutable statement metadata without cloning the table or its store.
    pub fn table_handle_by_key(&self, key: &CatalogTableKey) -> Option<Arc<TableEntry>> {
        self.table_handle_ref(key).cloned()
    }

    /// Go `SessionExtendedInfoSchema.TableByName`: the session's LOCAL
    /// temporary table of that name, else the shared schema's entry.
    fn table_handle_ref(&self, key: &CatalogTableKey) -> Option<&Arc<TableEntry>> {
        self.local_temporary_entry(key)
            .or_else(|| self.schema_entry(key))
    }

    /// The shared schema's entry, without the session's temporary tables
    /// (Go `DetachLocalTemporaryTableInfoSchema`).
    fn schema_entry(&self, key: &CatalogTableKey) -> Option<&Arc<TableEntry>> {
        self.schema.databases.get(&key.database)?.tables.get(&key.table)
    }

    fn local_temporary_entry(&self, key: &CatalogTableKey) -> Option<&Arc<TableEntry>> {
        self.local_temporary
            .iter()
            .find(|(local, _)| local == key)
            .map(|(_, entry)| entry)
    }

    fn get(&self, name: &str) -> Option<&TableEntry> {
        self.get_in(DEFAULT_DATABASE, name)
    }

    /// Borrows current table metadata without copying names or resolving
    /// them through the catalog again. Callers impose ordering only on the
    /// entries they need; hash-map traversal itself is unordered.
    pub fn table_entries(&self) -> impl Iterator<Item = (&str, &str, &TableEntry)> {
        self.schema.databases.values().flat_map(|database| {
            database
                .tables
                .iter()
                .map(move |(name, entry)| (database.name.as_str(), name.as_str(), entry.as_ref()))
        })
    }

    /// Whether any stored table declares a foreign key.
    ///
    /// The DML executor checks parent-side referential actions for every
    /// mutation. Most OLTP catalogs have no foreign keys, so keep this as a
    /// sticky bit instead of inspecting every table on every write. Once a
    /// foreign key has existed, retaining `true` is safe: it may perform one
    /// extra table-path scan after the constraint is dropped, but never skips
    /// a required referential check.
    pub fn has_foreign_keys(&self) -> bool {
        self.schema.foreign_keys_present
    }

    /// Whether any table of this catalog may carry a `LOCK TABLES` lock.
    /// `false` lets Go's `CheckTableLock` answer without reading a table:
    /// with no lock anywhere and none held by the session, every arm of
    /// `lock.Checker` returns nil.
    #[must_use]
    pub fn has_table_locks(&self) -> bool {
        self.schema.table_locks_present
    }

    /// Go `DBInfo.ID` of `database`.
    #[must_use]
    pub fn database_id(&self, database: &str) -> Option<i64> {
        self.schema.databases
            .get(&database.go_to_lower())
            .map(|schema| schema.id)
    }

    /// The name of the database whose Go `DBInfo.ID` is `id`, as Go's
    /// `is.SchemaByID` finds it.
    #[must_use]
    pub fn database_name_by_id(&self, id: i64) -> Option<String> {
        self.schema.databases
            .values()
            .find(|schema| schema.id == id)
            .map(|schema| schema.name.clone())
    }

    /// The `(schema, table)` names of the stored table whose Go
    /// `TableInfo.ID` is `table_id`, as Go's `is.TableByID` finds it.
    #[must_use]
    pub fn table_names_by_id(&self, table_id: i64) -> Option<(String, String)> {
        // Memory tables are registered here under ID 0, which no Go table
        // has.
        if table_id == 0 {
            return None;
        }
        let (database, table) = self
            .kv_table_by_physical_id(table_id)
            .filter(|(_, table)| table.table_id() == table_id)?;
        let database = self
            .schema.databases
            .get(database)
            .map_or_else(|| database.to_owned(), |schema| schema.name.clone());
        Some((database, table.name()))
    }

    /// Go `infoschema.FindTableByPartitionID`: the schema and table that own
    /// partition `partition_id`, or `None` when no table has a partition by
    /// that ID (a table's own ID is not a partition's).
    pub fn find_table_by_partition_id(&self, partition_id: i64) -> Option<(String, String)> {
        let key = self.table_id_names().get(&partition_id)?;
        let schema = self.schema.databases.get(&key.database)?;
        match schema.tables.get(&key.table)?.as_ref() {
            TableEntry::Kv(table)
                if table.partition().is_some_and(|partition| {
                    partition
                        .definitions
                        .iter()
                        .any(|definition| definition.id == partition_id)
                }) =>
            {
                Some((schema.name.clone(), table.name()))
            }
            _ => None,
        }
    }

    /// Every table carrying a `TableInfo.Lock`, with its schema name -- Go
    /// `ListTablesWithSpecialAttribute(TableLockAttribute)`.
    pub fn locked_tables(&self) -> impl Iterator<Item = (&str, &crate::KvTable)> {
        self.table_entries()
            .filter(|_| self.schema.table_locks_present)
            .filter_map(|(schema, _, entry)| match entry {
                TableEntry::Kv(table) if table.lock().is_some() => Some((schema, &**table)),
                _ => None,
            })
    }

    /// A mutable handle for the referential-integrity paths, which reach
    /// tables the statement did not name.
    pub fn get_mut_for_foreign_key(
        &mut self,
        database: &str,
        name: &str,
    ) -> Option<&mut TableEntry> {
        self.get_rows_mut_in(database, name)
    }

    /// A mutable handle on a table of `database`, for the write paths.
    ///
    /// Taking it bumps [`Catalog::version`], which is what a transaction's
    /// conflict check observes. The count is deliberately over-approximate:
    /// every write path goes through here, so a statement that ends up
    /// changing nothing still bumps it. That can refuse a commit Go would
    /// allow, never the reverse.
    pub fn get_mut_in(&mut self, database: &str, name: &str) -> Option<&mut TableEntry> {
        self.planner_view.take();
        self.get_rows_mut_in(database, name)
    }

    /// [`Catalog::get_mut_in`] for the paths that write rows (DML, foreign-key
    /// actions, the stats-delta flush): it bumps [`Catalog::version`] the
    /// same way but keeps the planner's schema view, which carries schema
    /// metadata only -- Go's `InfoSchema` does not move when rows do. A
    /// caller must not change the table's schema through this handle; a
    /// schema change takes [`Catalog::table_mut_in`].
    pub fn get_rows_mut_in(
        &mut self,
        database: &str,
        name: &str,
    ) -> Option<&mut TableEntry> {
        self.schema.version += 1;
        let key = CatalogTableKey::new(database, name);
        let entry = match self.local_temporary.iter().position(|(local, _)| *local == key) {
            Some(position) => &mut self.local_temporary[position].1,
            None => self.database_mut(&key.database)?.tables.get_mut(&key.table)?,
        };
        // Go's write paths build a new `TableInfo` rather than editing the
        // shared one; `make_mut` is the same copy-on-write at the entry level.
        Some(Arc::make_mut(entry))
    }

    /// The catalog's mutation counter.
    #[must_use]
    pub fn version(&self) -> u64 {
        self.schema.version
    }


    /// Process-local identity of this schema metadata image. Unlike a local
    /// mutation count, this cannot collide when a cluster catalog is rebuilt
    /// with the same number of tables. Clones retain it until metadata changes;
    /// DML leaves it unchanged, like Go's InfoSchema version cache boundary.
    /// This is an internal cache key, not the persisted cluster schema version.
    #[must_use]
    pub fn metadata_version(&self) -> u64 {
        self.schema.metadata_version
    }

    /// Go `InfoSchema.SchemaMetaVersion` as a plan-cache key and plan
    /// validity check read it: the metadata version of the shared image,
    /// which a session attaching its LOCAL temporary tables does not move
    /// (`SessionExtendedInfoSchema` keeps the inner version). A statement
    /// reading a temporary table is refused by the cache on its own.
    #[must_use]
    pub fn schema_version(&self) -> u64 {
        self.overlay_versions
            .map_or(self.schema.metadata_version, |(base, _)| base)
    }

    /// Advances the mutation counter and drops the derived views that
    /// belong to the image it counted.
    ///
    /// Every handle that can change a table takes this, `get_mut_in`
    /// included: a caller holding `&mut TableEntry` may add an index (Go's
    /// DDL owns that, but this tier hands the entry out), so the planner's
    /// view cannot outlive a version. `table_id_names` does not move here:
    /// it maps a table's id to its (database, name) key, which only a
    /// schema-changing operation can move -- `bump_metadata_version` below,
    /// or `temporary_overlay_table_mut`'s own direct call. An ordinary DML
    /// write (every `get_mut_in` caller in production) never reassigns a
    /// table's id or its key, so it does not owe this cache a rebuild.
    fn bump_version(&mut self) {
        self.schema.version += 1;
        self.planner_view.take();
    }

    fn bump_metadata_version(&mut self) {
        self.schema.metadata_version = next_metadata_version();
        self.schema.latest_index_schema.take();
        self.planner_view.take();
        self.statistics_schemas = Arc::default();
        self.invalidate_table_id_names();
    }

    fn invalidate_table_id_names(&mut self) {
        if let Some(names) = Arc::get_mut(&mut self.table_id_names) {
            names.take();
        } else {
            self.table_id_names = Arc::default();
        }
    }

    /// Captures only the immutable schema metadata needed by `TIDB_DECODE_KEY`.
    #[must_use]
    pub fn tidb_decode_key_snapshot(&self) -> crate::TidbDecodeKeySnapshot {
        let mut snapshot = crate::TidbDecodeKeySnapshot::default();
        for database in self.schema.databases.values() {
            for entry in database.tables.values() {
                if let TableEntry::Kv(table) = &**entry {
                    snapshot.insert_table(table);
                }
            }
        }
        snapshot
    }

    /// A mutable table of `database`, for the schema-changing statements.
    pub fn table_mut_in(&mut self, database: &str, name: &str) -> Option<&mut TableEntry> {
        // Go advances InfoSchema.SchemaMetaVersion whenever a DDL job changes
        // table metadata. Keep that epoch separate from `version`, which also
        // moves for ordinary row writes and transaction conflict detection.
        // Every production caller of this accessor is a schema-changing DDL
        // path; DML reaches the deliberately narrower `get_mut_in` instead.
        self.bump_metadata_version();
        self.get_mut_in(database, name)
    }

    /// A table of `database`, for the metadata statements.
    #[must_use]
    pub fn table_in(&self, database: &str, name: &str) -> Option<&TableEntry> {
        self.get_in(database, name)
    }

    /// The base table selected by a retained physical access node.
    ///
    /// Go's executor builder resolves a `PhysicalTableScan.Table.ID` through
    /// the statement's InfoSchema. The cached Rust physical tree owns the
    /// same stable ID, so executor construction must not recover the table by
    /// walking SQL names or aliases again.
    pub fn kv_table_by_id(&self, table_id: i64) -> Option<&crate::KvTable> {
        self.kv_table_by_physical_id(table_id)
            .map(|(_, table)| table)
            .filter(|table| table.table_id() == table_id)
            .map(|table| &**table)
    }

    /// Go `SessionExtendedInfoSchema.TableByID` (or `FindTableByPartitionID`
    /// for a partition's id): the session's LOCAL temporary table with that
    /// id, else the shared schema's table owning it, with its folded
    /// database.
    pub fn kv_table_by_physical_id(
        &self,
        physical_id: i64,
    ) -> Option<(&str, &Arc<crate::KvTable>)> {
        let local = self.local_temporary.iter().find_map(|(key, entry)| match entry.as_ref() {
            TableEntry::Kv(table) if table.table_id() == physical_id => {
                Some((key.database.as_str(), table))
            }
            _ => None,
        });
        local.or_else(|| {
            let key = self.table_id_names().get(&physical_id)?;
            match self.schema_entry(key)?.as_ref() {
                TableEntry::Kv(table) => Some((key.database.as_str(), table)),
                _ => None,
            }
        })
    }

    /// The bytes `key` holds in the table `table_id` names, read from a
    /// shallow copy so the catalog itself is untouched: COMMIT's check of a
    /// key an INSERT presumed absent.
    #[must_use]
    pub fn stored_raw_value(&self, table_id: i64, key: &[u8]) -> Option<Vec<u8>> {
        self.kv_table_by_id(table_id)?.clone().stored_raw_value(key)
    }

    fn table_id_names(&self) -> &HashMap<i64, Arc<CatalogTableKey>> {
        self.table_id_names.get_or_init(|| {
            let mut names = HashMap::new();
            for (database_name, database) in self.schema.databases.iter() {
                for (table_name, entry) in &database.tables {
                    let TableEntry::Kv(table) = entry.as_ref() else {
                        continue;
                    };
                    let key = Arc::new(CatalogTableKey {
                        database: database_name.clone(),
                        table: table_name.clone(),
                    });
                    names.insert(table.table_id(), Arc::clone(&key));
                    if let Some(partition) = table.partition() {
                        for definition in &partition.definitions {
                            names.insert(definition.id, Arc::clone(&key));
                        }
                    }
                }
            }
            names
        })
    }

    /// Resolves Go's physical table ID to a reader handle. A partition ID
    /// names its logical table with the read restricted to that one physical
    /// keyspace, matching executorBuilder's partition-table lookup.
    ///
    /// The common, unpartitioned case (`physical_id == table.table_id`,
    /// every table in most schemas) returns a cheap `Arc::clone` of the
    /// table already sitting in this transaction's own catalog view --
    /// no `KvTable` clone at all, so no `staged_record_keys` deep copy
    /// either. Only the partitioned branch pays `Arc::make_mut`'s clone,
    /// since restricting reads to one partition needs an independent copy.
    pub fn physical_kv_table_by_id(&self, physical_id: i64) -> Option<Arc<crate::KvTable>> {
        let (_, table) = self.kv_table_by_physical_id(physical_id)?;
        let mut physical = Arc::clone(table);
        if table.table_id() != physical_id {
            Arc::make_mut(&mut physical).restrict_read_to_partitions(&[physical_id]);
        }
        Some(physical)
    }

    /// Resolves a logical or partition table ID to its owning database name.
    pub fn physical_kv_table_database_by_id(&self, physical_id: i64) -> Option<&str> {
        let (database, _) = self.kv_table_by_physical_id(physical_id)?;
        Some(self.schema.databases.get(database)?.name.as_str())
    }

    /// A table of the default database, for tests that inspect the entry.
    #[must_use]
    pub fn get_table_for_test(&self, name: &str) -> Option<&TableEntry> {
        self.get(name)
    }

    /// Whether `database` holds a table called `name`.
    #[must_use]
    pub fn contains_in(&self, database: &str, name: &str) -> bool {
        self.get_in(database, name).is_some()
    }

    /// Registers a TiKV-format-byte-backed table in `database`, or reports
    /// 1049 when that schema does not exist.
    /// go's stats meta bumps the table's row count on every write
    /// (`stats_meta.count`), which `I_S.TABLES`' TABLE_ROWS reads back.
    /// A DML that changed `delta` rows adjusts the stored estimate.
    pub fn adjust_table_row_count(&mut self, database: &str, name: &str, delta: i64) {
        let Some(database) = Arc::make_mut(&mut self.schema.databases).get_mut(database) else {
            return;
        };
        let database = std::sync::Arc::make_mut(database);
        let Some(entry) = database.tables.get_mut(name) else {
            return;
        };
        let crate::TableEntry::Kv(table) = std::sync::Arc::make_mut(entry) else {
            return;
        };
        let table = std::sync::Arc::make_mut(table);
        let (rows, average, data, index) = table.storage_statistics();
        let updated = (i64::try_from(rows).unwrap_or(i64::MAX) + delta).max(0) as u64;
        table.set_storage_statistics((updated, average, data, index));
    }

    pub fn register_kv_in(
        &mut self,
        database: &str,
        name: &str,
        table: KvTable,
    ) -> Result<(), DriverError> {
        self.bump_metadata_version();
        self.register_in(database, name, TableEntry::Kv(Arc::new(table)))
    }

    /// Registers an already-admitted view. The caller owns object-kind checks.
    /// Reports 1049 when the schema does not exist.
    pub fn register_view_in(
        &mut self,
        database: &str,
        name: &str,
        view: ViewDef,
    ) -> Result<(), DriverError> {
        self.bump_metadata_version();
        self.register_in(database, name, TableEntry::View(view))
    }

    /// Whether `name` in `database` is a view.
    #[must_use]
    pub fn is_view_in(&self, database: &str, name: &str) -> bool {
        self.get_in(database, name).is_some_and(TableEntry::is_view)
    }

    /// Whether `name` in `database` is a sequence.
    #[must_use]
    pub fn is_sequence_in(&self, database: &str, name: &str) -> bool {
        self.get_in(database, name)
            .is_some_and(TableEntry::is_sequence)
    }

    /// Registers a sequence in `database`, replacing whatever the name held.
    /// Callers own the name-collision check: Go answers 1050
    /// `Table 'db.name' already exists` for `CREATE SEQUENCE` over ANY
    /// existing name, table or sequence (captured).
    pub fn register_sequence_in(
        &mut self,
        database: &str,
        name: &str,
        sequence: SequenceDef,
    ) -> Result<(), DriverError> {
        self.bump_metadata_version();
        self.register_in(database, name, TableEntry::Sequence(sequence))
    }

    /// The sequence `name` in `database`, or `None` when the name is absent
    /// or holds something else.
    #[must_use]
    pub fn sequence_in(&self, database: &str, name: &str) -> Option<&SequenceDef> {
        match self.get_in(database, name) {
            Some(TableEntry::Sequence(sequence)) => Some(sequence),
            _ => None,
        }
    }

    /// Whether a table with `name` exists in the default database.
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.get(name).is_some()
    }
}


/// Go `perfschema`'s `init` registering its virtual tables
/// (`infoschema.RegisterVirtualTable`): the builder of the
/// `performance_schema` tables, which every catalog's memory schemas include
/// once it is set. tidb-infoschema-perfschema sets it (its `register`).
static PERFORMANCE_SCHEMA_TABLES: std::sync::OnceLock<
    fn() -> HashMap<String, Arc<TableEntry>>,
> = std::sync::OnceLock::new();

/// Sets the `performance_schema` table builder; the first call wins.
pub fn register_performance_schema(tables: fn() -> HashMap<String, Arc<TableEntry>>) {
    let _ = PERFORMANCE_SCHEMA_TABLES.set(tables);
}

/// Where a staged table sits in the catalog.
#[derive(Clone, Debug)]
enum StagedTable {
    Persistent(String, String),
    Local(usize),
}

/// The stages [`Catalog::begin_statement_stage`] opened, closed by
/// [`Catalog::end_statement_stage`].
#[derive(Debug)]
#[must_use]
pub struct StatementStage(Vec<(StagedTable, tidb_kv::storage::StagingHandle)>);
