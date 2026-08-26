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

use super::*;
use crate::kv_table::TableCharset;
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
/// Splits a table path for another module in this crate.
pub(crate) fn split_table_path_pub<'a>(
    path: &'a [String],
    current_db: &'a str,
) -> Result<(&'a str, &'a str), DriverError> {
    split_table_path(path, current_db)
}

/// `current_db` is the session's selected schema (Go `SessionVars.CurrentDB`);
/// an empty one is Go's `ErrNoDB`.
pub(crate) fn split_table_path<'a>(
    path: &'a [String],
    current_db: &'a str,
) -> Result<(&'a str, &'a str), DriverError> {
    match path {
        [name] => {
            if current_db.is_empty() {
                return Err(DriverError::Schema(SchemaErrorKind::NoDatabaseSelected));
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
    tables: HashMap<String, std::sync::Arc<TableEntry>>,
}

/// A catalog of databases and their tables, the position Go's `infoschema`
/// occupies. Database and table names are case-insensitive, as in MySQL.
#[derive(Clone, Debug)]
pub struct Catalog {
    databases: HashMap<String, Database>,
    /// Go `infoschema`'s policy map, keyed by the FOLDED policy name.
    ///
    /// A placement policy is a schema object in its own right, not an
    /// attribute of a table: `DROP PLACEMENT POLICY` has to know whether any
    /// table or partition still references it, and `ALTER PLACEMENT POLICY`
    /// changes what every referencing object means at once. Both need the
    /// policy to live here rather than being copied into each user.
    policies: HashMap<String, tidb_model::PolicyInfo>,
    next_policy_id: i64,
    next_database_id: i64,
    next_table_id: i64,
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
    /// The entries a session's LOCAL temporary tables are DISPLACING while
    /// they are attached: `(folded database, folded name, the entry that was
    /// there)`.
    ///
    /// Go models the same shadowing with a wrapper rather than a list:
    /// `infoschema.SessionExtendedInfoSchema.TableByName` consults
    /// `LocalTemporaryTables` first and falls through to the shared
    /// infoschema, so `CREATE TEMPORARY TABLE t` in a schema that already has
    /// a permanent `t` hides it for this session and leaves it untouched for
    /// every other one. This tier resolves names against ONE table map, so
    /// the overlay has to be spelled as "put the temporary one in the slot
    /// and remember what came out"; [`Catalog::take_local_temporary_tables`]
    /// puts the remembered entry back.
    ///
    /// Without the list the permanent table would be DESTROYED by the
    /// temporary one that shadowed it -- the create would overwrite the slot
    /// and the detach would empty it.
    shadowed_by_local_temporary: Vec<(String, String, std::sync::Arc<TableEntry>)>,
    /// Loaded statistics by physical table id, Go's `StatsHandle` cache as
    /// the planner sees it.
    ///
    /// A missing entry is Go's `statistics.PseudoTable`, and that is what
    /// makes an unanalyzed table's `EXPLAIN` print `stats:pseudo`. The map
    /// lives on the catalog rather than on a table because it is loaded from
    /// `mysql.stats_*` on its own cadence (see `tidb-exec`'s `stats_watch`),
    /// so the two are published independently.
    statistics: HashMap<i64, Arc<crate::access_cost::TableStatistics>>,
    /// The store's commit history, shared by every clone of this catalog
    /// (working copies, sessions on the same store): a monotonic TSO-shaped
    /// allocator plus a bounded ring of committed snapshots, which is what
    /// serves Go's stale reads on a tier whose store otherwise keeps no
    /// versions. See [`CommitHistory`].
    commit_history: Arc<std::sync::Mutex<CommitHistory>>,
    /// Memoized answer of walking EVERY database's table map for LOCAL and
    /// GLOBAL temporary tables, valid as of one `metadata_version`. Go asks
    /// this from `temptable`'s own session map in O(1); this tier's maps are
    /// the catalog itself, so without the memo the walk runs twice per
    /// statement (`txn`'s overlay guard swaps global row storage in and out,
    /// and detaches locals), which is pure overhead for the sessions that
    /// hold no temporary tables -- every OLTP workload. Keyed on
    /// `metadata_version` because exactly its mutators move the table set;
    /// [`Self::take_local_temporary_tables`] also drops it explicitly after
    /// removing entries.
    temporary_sweep:
        Option<(u64, Vec<(String, String)>, Vec<(String, String)>)>,
}

/// The narrow store's commit history.
///
/// Go's stale reads run against TiKV's real MVCC versions; this tier's
/// store is the catalog itself, so history is a ring of full snapshots
/// keyed by a fabricated-but-real-shaped TSO. The TSOs matter as much as
/// the snapshots: the corpus does arithmetic on them
/// (`CAST(@ts AS UNSIGNED) - 1`) and Go's `CalculateAsOfTsExpr` validates
/// the physical half against 2013-01-01, so a small-integer scheme would
/// fail the very validation ported beside it.
#[derive(Debug, Default)]
pub struct CommitHistory {
    /// The last TSO handed out; allocation is `max(now_ms << 18, last + 1)`,
    /// strictly increasing like PD's.
    last_tso: u64,
    /// `(commit_ts, the catalog as of that commit)`, oldest first, capped at
    /// [`COMMIT_HISTORY_CAP`].
    entries: std::collections::VecDeque<(u64, Catalog)>,
}

/// How many committed snapshots the ring keeps. The corpus needs the last
/// two; eight leaves room for multi-statement recipes without letting a
/// DML-heavy replay hold thousands of catalog clones.
const COMMIT_HISTORY_CAP: usize = 8;

impl Default for Catalog {
    /// A catalog holding `test`, `INFORMATION_SCHEMA` and `mysql`, the three
    /// schemas a freshly bootstrapped TiDB exposes that this tier can name.
    ///
    /// `INFORMATION_SCHEMA` is present because its tables are implemented
    /// (see `tidb-session`'s `infoschema`), and holds no stored tables of its
    /// own -- its rows are computed at query time.
    ///
    /// `mysql` is present as an OBJECT with no tables, and the distinction is
    /// the whole point. Go's `pkg/session/bootstrap.go` creates it with 61
    /// tables holding real rows (captured: `use mysql; show tables;` lists
    /// `user`, `db`, `tidb`, the `stats_*` family and the rest; `select
    /// count(*) from mysql.user` answers 1). This tier serves NONE of them,
    /// and an absent table is exactly how it says so: `mysql.user` answers
    /// `Table 'mysql.user' doesn't exist` (1146), the same refusal the
    /// cluster tier's `SkippedTable` produces for a table it cannot back.
    /// What the object buys is that `USE mysql` SUCCEEDS, and that matters
    /// out of proportion to the statement it is: a failed `USE` leaves the
    /// session pointed at the previous schema (captured -- Go does the same
    /// on a genuinely unknown name), so every later unqualified name resolved
    /// somewhere else entirely. One refused statement was silently
    /// re-answering all the statements behind it.
    ///
    /// DIVERGENCE (documented): enumerating `mysql` reports it EMPTY --
    /// `show tables` after `use mysql` returns no rows where Go returns 61,
    /// and `information_schema.tables` likewise. That is the honest shape of
    /// "the object exists and its contents are not ported": naming a table
    /// refuses, only counting them under-reports.
    ///
    /// DIVERGENCE (documented): `performance_schema`, `sys` and
    /// `metrics_schema` stay absent. Unlike `mysql` they gate no measured
    /// statement in the corpus -- nothing `USE`s them and nothing connects
    /// with them -- so seeding them would buy nothing and under-report more.
    ///
    /// DIVERGENCE (documented): `DROP DATABASE mysql` is accepted here and
    /// removes the object; Go refuses it with
    /// `[ddl:8267]Drop 'mysql' database is forbidden` (captured). The guard
    /// belongs in the statement arm that calls [`Catalog::drop_database`],
    /// which this unit does not own. `information_schema` has the same hole
    /// today, so this is a pre-existing gap widened by one name rather than a
    /// new class; it is pinned by
    /// `tidb_session`'s `dropping_the_mysql_schema_is_not_refused_yet`.
    fn default() -> Self {
        let mut databases = HashMap::new();
        databases.insert(
            DEFAULT_DATABASE.to_owned(),
            Database {
                id: 1,
                name: DEFAULT_DATABASE.to_owned(),
                charset: TableCharset::default(),
                tables: HashMap::new(),
            },
        );
        databases.insert(
            "information_schema".to_owned(),
            Database {
                id: 2,
                name: "INFORMATION_SCHEMA".to_owned(),
                charset: TableCharset::default(),
                tables: HashMap::new(),
            },
        );
        databases.insert(
            SYSTEM_DATABASE.to_owned(),
            Database {
                id: 3,
                name: SYSTEM_DATABASE.to_owned(),
                charset: TableCharset::default(),
                tables: HashMap::new(),
            },
        );
        let mut catalog = Catalog {
            databases,
            policies: HashMap::new(),
            next_policy_id: 0,
            next_database_id: 3,
            next_table_id: 0,
            version: 0,
            metadata_version: 0,
            shadowed_by_local_temporary: Vec::new(),
            statistics: HashMap::new(),
            commit_history: Arc::new(std::sync::Mutex::new(CommitHistory::default())),
            temporary_sweep: None,
        };
        // Go's bootstrap builds the `information_schema` tables into the
        // infoschema itself, so they are ordinary objects to every name
        // lookup. Doing it here rather than at each construction site is what
        // makes that unforgettable.
        super::infoschema_meta::register_tables(&mut catalog);
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
}

/// A catalog table's backing store.
#[derive(Clone, Debug)]
pub enum TableEntry {
    /// A plain value matrix (the original mock backing).
    Mem(MemTable),
    /// A spill-backed common table expression, scoped to one query catalog.
    Cte(crate::CteTable),
    /// Rows stored as real TiKV-format bytes (see [`crate::kv_table`]).
    Kv(KvTable),
    /// A view: a stored `SELECT` rather than stored rows.
    View(ViewDef),
    /// A sequence: a counter rather than rows. It lives in the TABLE
    /// namespace because Go stores it as a `model.TableInfo` with `Sequence`
    /// set -- which is why `CREATE TABLE` over a sequence name collides, and
    /// why `SELECT * FROM <sequence>` and `DROP TABLE <sequence>` are
    /// errors rather than name-not-found.
    Sequence(SequenceDef),
}

/// A sequence in the catalog: the name as written plus its allocator.
///
/// The allocator is `Arc`-shared inside, so cloning this entry (as a staged
/// catalog copy does) shares the counter rather than forking it.
#[derive(Clone, Debug)]
pub struct SequenceDef {
    /// The name as written, for `SHOW CREATE SEQUENCE` and `SHOW TABLES`.
    pub name: String,
    /// The value source. See [`crate::sequence`].
    pub allocator: crate::sequence::SequenceAllocator,
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
    pub(crate) fn column_list(&self) -> Vec<(String, FieldType)> {
        match self {
            TableEntry::Mem(mem) => mem.columns.clone(),
            TableEntry::Cte(cte) => cte.columns().to_vec(),
            TableEntry::Kv(kv) => kv
                .visible_columns()
                .iter()
                .map(|c| (c.name.clone(), c.field_type.clone()))
                .collect(),
            TableEntry::View(view) => view.columns.clone(),
            // Go gives a sequence a fixed one-column schema, but no statement
            // this tier accepts ever reads it: `SELECT * FROM <sequence>` is
            // refused, and `nextval` reaches the allocator directly.
            TableEntry::Sequence(_) => Vec::new(),
        }
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
        self.column_list()
            .into_iter()
            .map(|(name, _)| name)
            .collect()
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
        self.register_in(DEFAULT_DATABASE, name, TableEntry::Kv(table))
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
        table: TableEntry,
    ) -> Result<(), DriverError> {
        let schema = self
            .databases
            .get_mut(&database.to_lowercase())
            .ok_or_else(|| {
                DriverError::Schema(crate::SchemaErrorKind::UnknownDatabase(database.to_owned()))
            })?;
        schema.tables.insert(name.to_lowercase(), std::sync::Arc::new(table));
        self.version += 1;
        Ok(())
    }

    /// Every database name, sorted, with `information_schema` first when it
    /// exists -- Go's `fetchShowDatabases` ordering.
    #[must_use]
    pub fn database_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .databases
            .values()
            .map(|database| database.name.clone())
            .collect();
        names.sort();
        if let Some(position) = names
            .iter()
            .position(|name| name.eq_ignore_ascii_case("information_schema"))
        {
            let front = names.remove(position);
            names.insert(0, front);
        }
        names
    }

    /// Every table name in `database`, sorted as Go's `fetchShowTables` sorts
    /// them. `None` when the database does not exist.
    ///
    /// LOCAL temporary tables are NOT here. Go cannot list them because they
    /// are not in the infoschema an enumeration reads
    /// (`SchemaSimpleTableInfos`), and it excludes them from the by-name form
    /// too -- `fetchShowInfoByName` (`pkg/executor/show.go:540`) finds the
    /// table through the session overlay and then returns nothing for a
    /// `TempTableLocal`. This tier keeps such a table in the SAME map for the
    /// duration of a statement, so the exclusion has to be stated; it is
    /// stated once, here, because `SHOW TABLES`, `SHOW FULL TABLES` and
    /// `information_schema.tables` all enumerate through this one call and
    /// must not disagree.
    #[must_use]
    pub fn table_names(&self, database: &str) -> Option<Vec<String>> {
        let database = self.databases.get(&database.to_lowercase())?;
        let mut names: Vec<String> = database
            .tables
            .iter()
            .filter(|(_, entry)| {
                !matches!(entry.as_ref(), TableEntry::Kv(table)
                    if table.temp_table_type() == tidb_model::TempTableType::LOCAL)
            })
            .map(|(name, _)| name.clone())
            .collect();
        names.sort();
        Some(names)
    }

    /// Whether `database` exists (Go `is.SchemaExists`).
    #[must_use]
    pub fn has_database(&self, database: &str) -> bool {
        self.databases.contains_key(&database.to_lowercase())
    }

    /// The effective defaults stored on a database.
    #[must_use]
    pub fn database_charset(&self, database: &str) -> Option<TableCharset> {
        self.databases
            .get(&database.to_lowercase())
            .map(|database| database.charset)
    }

    /// The stored name and defaults needed by `SHOW CREATE DATABASE`.
    #[must_use]
    pub fn database_definition(&self, database: &str) -> Option<(String, TableCharset)> {
        self.databases
            .get(&database.to_lowercase())
            .map(|database| (database.name.clone(), database.charset))
    }

    /// Creates `database`, reporting whether it was new. Go raises
    /// `ErrDBCreateExists` (1007) unless `IF NOT EXISTS` was written.
    /// Go `infoschema.PolicyByName`: the policy a name refers to, folded.
    #[must_use]
    pub fn policy(&self, name: &str) -> Option<&tidb_model::PolicyInfo> {
        self.policies
            .get(&tidb_ast::CiString::new(name).lowercase().to_owned())
    }

    /// Every policy, for `information_schema` and `SHOW` surfaces.
    #[must_use]
    pub fn policies(&self) -> impl Iterator<Item = &tidb_model::PolicyInfo> {
        self.policies.values()
    }

    /// Go `CreatePlacementPolicyWithInfo`: stores a new policy.
    ///
    /// Returns `false` when one of that name already exists, leaving the
    /// stored policy untouched -- the caller decides whether that is
    /// `OnExistError` (8238), `OnExistIgnore`, or `OnExistReplace`, exactly
    /// as Go's `OnExist` does.
    pub fn create_policy(&mut self, mut policy: tidb_model::PolicyInfo) -> bool {
        self.bump_metadata_version();
        let key = policy.name.lowercase().to_owned();
        if self.policies.contains_key(&key) {
            return false;
        }
        self.next_policy_id += 1;
        policy.id = self.next_policy_id;
        self.policies.insert(key, policy);
        self.version += 1;
        true
    }

    /// Replaces a stored policy's settings, KEEPING its id.
    ///
    /// Go's `ALTER PLACEMENT POLICY` alters the policy every referencing
    /// object points at, so the id has to survive: the references are by id,
    /// and re-issuing one would orphan them.
    pub fn replace_policy_settings(
        &mut self,
        name: &str,
        settings: tidb_model::PlacementSettings,
    ) -> bool {
        self.bump_metadata_version();
        let key = tidb_ast::CiString::new(name).lowercase().to_owned();
        let Some(policy) = self.policies.get_mut(&key) else {
            return false;
        };
        policy.placement_settings = Some(tidb_model::GoShared::new(settings));
        self.version += 1;
        true
    }

    /// Go `DropPlacementPolicy`: removes a policy by name.
    pub fn drop_policy(&mut self, name: &str) -> bool {
        self.bump_metadata_version();
        let key = tidb_ast::CiString::new(name).lowercase().to_owned();
        if self.policies.remove(&key).is_none() {
            return false;
        }
        self.version += 1;
        true
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
        self.databases.values().any(|database| {
            database.tables.values().any(|entry| {
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

    pub fn create_database(&mut self, database: &str) -> bool {
        self.bump_metadata_version();
        self.create_database_with_charset(database, TableCharset::default())
    }

    /// Creates a database with its resolved charset and collation defaults.
    pub fn create_database_with_charset(&mut self, database: &str, charset: TableCharset) -> bool {
        self.bump_metadata_version();
        let key = database.to_lowercase();
        if self.databases.contains_key(&key) {
            return false;
        }
        self.next_database_id += 1;
        self.databases.insert(
            key,
            Database {
                id: self.next_database_id,
                name: database.to_owned(),
                charset,
                tables: HashMap::new(),
            },
        );
        self.version += 1;
        true
    }

    /// Installs a database with the ID from a persisted cluster catalog.
    ///
    /// The default catalog already contains `test` and `mysql`; loading those
    /// schemas replaces their synthetic IDs rather than silently discarding
    /// the source identity. Returns whether the schema name was newly added.
    pub fn register_database_with_id(&mut self, database: &str, id: i64) -> bool {
        self.bump_metadata_version();
        self.register_database_with_id_and_charset(database, id, TableCharset::default())
    }

    /// [`Self::register_database_with_id`] carrying the database's stored
    /// charset and collation.
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
        let key = database.to_lowercase();
        self.next_database_id = self.next_database_id.max(id);
        if let Some(existing) = self.databases.get_mut(&key) {
            let changed =
                existing.id != id || existing.name != database || existing.charset != charset;
            existing.id = id;
            existing.name = database.to_owned();
            existing.charset = charset;
            self.version += u64::from(changed);
            return false;
        }
        self.databases.insert(
            key,
            Database {
                id,
                name: database.to_owned(),
                charset,
                tables: HashMap::new(),
            },
        );
        self.version += 1;
        true
    }

    /// Moves a table to a new schema and name, which is what RENAME does.
    /// Returns `false`, having changed nothing, when the source table or the
    /// destination schema does not exist.
    ///
    /// The destination schema is checked BEFORE the source is taken out, so
    /// "source removed with nowhere to put it" is not a state this function
    /// can produce and does not need undoing. Both preconditions are also
    /// checked by the caller, which is where the MySQL error text comes from;
    /// re-checking here is what keeps the catalog's own invariant -- a table
    /// is in exactly one schema -- independent of any caller remembering to.
    pub fn rename_table(
        &mut self,
        from_database: &str,
        from_name: &str,
        to_database: &str,
        to_name: &str,
    ) -> bool {
        self.bump_metadata_version();
        let to_key = to_database.to_lowercase();
        if !self.databases.contains_key(&to_key) {
            return false;
        }
        let Some(source) = self
            .databases
            .get_mut(&from_database.to_lowercase())
            .and_then(|database| database.tables.remove(&from_name.to_lowercase()))
        else {
            return false;
        };
        // The table carries its own name for duplicate-key messages. Renames
        // are DDL-rare, so cloning a still-shared entry here is fine.
        let mut source = Arc::unwrap_or_clone(source);
        if let TableEntry::Kv(table) = &mut source {
            table.set_name(to_name);
        }
        let mut source = std::sync::Arc::new(source);
        // Infallible: the key was present at the top of this function and
        // nothing between here and there can remove a schema.
        self.databases
            .get_mut(&to_key)
            .expect("destination schema was checked above")
            .tables
            .insert(to_name.to_lowercase(), source);
        self.version += 1;
        true
    }

    /// Drops one table, reporting whether it existed.
    pub fn drop_table_in(&mut self, database: &str, name: &str) -> bool {
        self.bump_metadata_version();
        let dropped = match self.databases.get_mut(&database.to_lowercase()) {
            Some(database) => database.tables.remove(&name.to_lowercase()).is_some(),
            None => false,
        };
        self.version += u64::from(dropped);
        dropped
    }

    /// Drops `database` and its tables, reporting whether it existed. Go
    /// raises `ErrDBDropExists` (1008) unless `IF EXISTS` was written.
    pub fn drop_database(&mut self, database: &str) -> bool {
        self.bump_metadata_version();
        let dropped = self.databases.remove(&database.to_lowercase()).is_some();
        self.version += u64::from(dropped);
        dropped
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
            TableEntry::Kv(table) => Some(table.table_id),
            _ => None,
        }
    }

    pub(crate) fn get_in(&self, database: &str, name: &str) -> Option<&TableEntry> {
        self.databases
            .get(&database.to_lowercase())?
            .tables
            .get(&name.to_lowercase())
            .map(|entry| &**entry)
    }

    /// A mutable table handle for a read whose storage API advances internal
    /// state. Unlike [`Self::get_mut_in`], a read does not move the catalog's
    /// schema/data version.
    pub(crate) fn get_mut_in_for_read(
        &mut self,
        database: &str,
        name: &str,
    ) -> Option<&mut TableEntry> {
        let entry = self
            .databases
            .get_mut(&database.to_ascii_lowercase())?
            .tables
            .get_mut(&name.to_ascii_lowercase())?;
        Some(Arc::make_mut(entry))
    }

    fn get(&self, name: &str) -> Option<&TableEntry> {
        self.get_in(DEFAULT_DATABASE, name)
    }

    /// Every `(database, table)` in the catalog, which is how referential
    /// integrity finds the tables that REFER to a given one -- Go keeps the
    /// same relation as `infoschema`'s referred-foreign-key index.
    pub(crate) fn table_paths(&self) -> Vec<(String, String)> {
        let mut paths = Vec::new();
        for database in self.databases.values() {
            for name in database.tables.keys() {
                paths.push((database.name.clone(), name.clone()));
            }
        }
        // The catalog is a hash map, so a stable order has to be imposed here
        // for a cascade to visit dependents deterministically.
        paths.sort();
        paths
    }

    /// A mutable handle for the referential-integrity paths, which reach
    /// tables the statement did not name.
    pub(crate) fn get_mut_for_foreign_key(
        &mut self,
        database: &str,
        name: &str,
    ) -> Option<&mut TableEntry> {
        self.get_mut_in(database, name)
    }

    /// A mutable handle on a table of `database`, for the write paths.
    ///
    /// Taking it bumps [`Catalog::version`], which is what a transaction's
    /// conflict check observes. The count is deliberately over-approximate:
    /// every write path goes through here, so a statement that ends up
    /// changing nothing still bumps it. That can refuse a commit Go would
    /// allow, never the reverse.
    pub(crate) fn get_mut_in(&mut self, database: &str, name: &str) -> Option<&mut TableEntry> {
        self.version += 1;
        let entry = self
            .databases
            .get_mut(&database.to_ascii_lowercase())?
            .tables
            .get_mut(&name.to_ascii_lowercase())?;
        // Go's write paths build a new `TableInfo` rather than editing the
        // shared one; `make_mut` is the same copy-on-write at the entry level.
        Some(Arc::make_mut(entry))
    }

    /// The catalog's mutation counter.
    #[must_use]
    pub fn version(&self) -> u64 {
        self.version
    }

    /// The counter that moves only when key-decode-relevant schema metadata
    /// mutators run: the cache key Go's per-infoschema row-decode metadata is
    /// keyed on (a schema version that DDL moves and DML never does).
    #[must_use]
    pub fn metadata_version(&self) -> u64 {
        self.metadata_version
    }

    fn bump_metadata_version(&mut self) {
        self.metadata_version += 1;
    }

    /// Hands out the next TSO -- PD's shape (`now_ms << 18`), strictly
    /// increasing. One allocator per store, shared by every clone.
    pub fn allocate_tso(&self) -> u64 {
        let mut history = self
            .commit_history
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_millis() as u64);
        let candidate = now_ms << 18;
        history.last_tso = candidate.max(history.last_tso + 1);
        history.last_tso
    }

    /// Records this catalog as the state committed at `commit_ts`.
    ///
    /// The snapshot's own history handle still points at the SHARED ring --
    /// snapshots never read it, and sharing keeps the clone shallow there.
    pub fn record_commit(&self, commit_ts: u64) {
        let snapshot = self.clone();
        let mut history = self
            .commit_history
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        history.entries.push_back((commit_ts, snapshot));
        while history.entries.len() > COMMIT_HISTORY_CAP {
            history.entries.pop_front();
        }
    }

    /// The store's state as of `ts`: the newest commit at or below it --
    /// Go's MVCC floor. `None` when no retained commit is old enough, which
    /// the caller reports rather than papering over with the present.
    #[must_use]
    pub fn state_as_of(&self, ts: u64) -> Option<Catalog> {
        let history = self
            .commit_history
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        history
            .entries
            .iter()
            .rev()
            .find(|(commit_ts, _)| *commit_ts <= ts)
            .map(|(_, snapshot)| snapshot.clone())
    }

    /// Captures only the immutable schema metadata needed by `TIDB_DECODE_KEY`.
    #[must_use]
    pub fn tidb_decode_key_snapshot(&self) -> crate::TidbDecodeKeySnapshot {
        let mut snapshot = crate::TidbDecodeKeySnapshot::default();
        for database in self.databases.values() {
            for entry in database.tables.values() {
                if let TableEntry::Kv(table) = &**entry {
                    snapshot.insert_table(table);
                }
            }
        }
        snapshot
    }

    /// Empties every table's staged-write mark: the state Go's transaction
    /// membuffer is in before a transaction has written anything, which is
    /// what `session.HasDirtyContent` (`pkg/session/txn.go:730`) reads.
    ///
    /// A transaction stages its writes in a private COPY of this catalog
    /// rather than a membuffer, so the copy has to be told where the
    /// transaction begins. `tidb_session` calls this at the one boundary Go's
    /// membuffer is empty at: the start of a statement that does not continue
    /// an already-open transaction -- which covers both an explicit `BEGIN`
    /// (Go allocates it a fresh membuffer) and every autocommit statement (Go
    /// discards the previous one at commit).
    ///
    /// The mark itself never changes what a read RETURNS -- the staged rows
    /// are in this catalog either way, which is why read-your-own-writes works
    /// without it. It changes what a read is entitled to REORDER; see
    /// [`crate::kv_table::KvTable::has_dirty_content`].
    pub fn clear_dirty_content(&mut self) {
        // The mark is interior-mutable (`AtomicBool`), so this walks SHARED
        // entries without detaching any of them: a shared entry's cell can
        // only ever hold `false` (a write detaches its entry before marking),
        // so resetting it here cannot disturb another catalog's view.
        for database in self.databases.values() {
            for entry in database.tables.values() {
                if let TableEntry::Kv(table) = &**entry {
                    table.clear_dirty_content();
                }
            }
        }
    }

    /// Publishes one table's loaded statistics, which the access-path choice
    /// and `EXPLAIN`'s `estRows` then read instead of the pseudo constants.
    ///
    /// Two publishers reach here, and they must agree: the cluster session
    /// loads `mysql.stats_*` rows a Go `ANALYZE` wrote
    /// (`tidb_server::cluster_session`), and an in-process `ANALYZE TABLE`
    /// computes them here ([`crate::analyze::kv::analyze_kv_table`]). Both
    /// build the histograms through [`crate::analyze`], so a table's estimates
    /// do not depend on which storage it sits on.
    pub fn set_table_statistics(
        &mut self,
        table_id: i64,
        statistics: Arc<crate::access_cost::TableStatistics>,
    ) {
        self.statistics.insert(table_id, statistics);
    }

    /// One table's loaded statistics; `None` is Go's `PseudoTable`.
    #[must_use]
    pub fn table_statistics(
        &self,
        table_id: i64,
    ) -> Option<&Arc<crate::access_cost::TableStatistics>> {
        self.statistics.get(&table_id)
    }

    /// Makes physical-access statistics queued by the preceding statement
    /// resident before the next statement takes its logical stats snapshot.
    pub fn advance_statistics_loads(&self) {
        for statistics in self.statistics.values() {
            statistics.advance_statistics_loads();
        }
    }

    /// Captures the shared statistics residency before a speculative planning
    /// branch. The metadata and histogram payloads remain shared; only the
    /// mutable Go `StatsHandle`-like load state is restored between branches.
    pub(crate) fn statistics_load_checkpoint(
        &self,
    ) -> Vec<(i64, crate::access_cost::StatsLoadCheckpoint)> {
        self.statistics
            .iter()
            .map(|(table_id, statistics)| (*table_id, statistics.load_checkpoint()))
            .collect()
    }

    /// Restores a checkpoint captured by [`Self::statistics_load_checkpoint`].
    pub(crate) fn restore_statistics_load_checkpoint(
        &self,
        checkpoint: &[(i64, crate::access_cost::StatsLoadCheckpoint)],
    ) {
        for (table_id, state) in checkpoint {
            if let Some(statistics) = self.statistics.get(table_id) {
                statistics.restore_load_checkpoint(state);
            }
        }
    }

    /// A mutable table of `database`, for the schema-changing statements.
    pub fn table_mut_in(&mut self, database: &str, name: &str) -> Option<&mut TableEntry> {
        self.get_mut_in(database, name)
    }

    /// A table of `database`, for the metadata statements.
    #[must_use]
    pub fn table_in(&self, database: &str, name: &str) -> Option<&TableEntry> {
        self.get_in(database, name)
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

    /// Registers a matrix-backed table in `database`, creating the schema
    /// when it does not exist. Used to materialize a virtual table before
    /// running an ordinary plan over it.
    ///
    /// # Panics
    /// Never: the schema is created just above when it is missing.
    pub fn register_mem_in(&mut self, database: &str, name: &str, table: MemTable) {
        self.bump_metadata_version();
        let key = database.to_lowercase();
        if !self.databases.contains_key(&key) {
            self.next_database_id += 1;
            self.databases.insert(
                key,
                Database {
                    id: self.next_database_id,
                    name: database.to_owned(),
                    charset: TableCharset::default(),
                    tables: HashMap::new(),
                },
            );
        }
        self.register_in(database, name, TableEntry::Mem(table))
            .expect("the schema was just created when it was missing");
    }

    /// Registers a query-scoped spill-backed CTE in `database`, creating the
    /// scratch schema when it does not exist.
    pub(crate) fn register_cte_in(&mut self, database: &str, name: &str, table: crate::CteTable) {
        let key = database.to_lowercase();
        if !self.databases.contains_key(&key) {
            self.next_database_id += 1;
            self.databases.insert(
                key,
                Database {
                    id: self.next_database_id,
                    name: database.to_owned(),
                    charset: TableCharset::default(),
                    tables: HashMap::new(),
                },
            );
        }
        self.register_in(database, name, TableEntry::Cte(table))
            .expect("the scratch schema was just created when it was missing");
    }

    /// Registers a TiKV-format-byte-backed table in `database`, or reports
    /// 1049 when that schema does not exist.
    pub fn register_kv_in(
        &mut self,
        database: &str,
        name: &str,
        table: KvTable,
    ) -> Result<(), DriverError> {
        self.bump_metadata_version();
        self.register_in(database, name, TableEntry::Kv(table))
    }

    /// Registers a LOCAL temporary table in `database`, remembering whatever
    /// permanent object the name held so the detach can put it back.
    ///
    /// Go `temptable.CreateLocalTemporaryTable` -> `SessionTables.AddTable`:
    /// the table goes into the SESSION's own map, never into the shared
    /// infoschema, so it neither takes a DDL job nor moves the schema
    /// version. This does not bump [`Catalog::version`] for the same reason:
    /// a session creating a scratch table of its own must not abort a peer's
    /// open transaction, which the version counter is what decides.
    ///
    /// The schema must exist -- Go's `createSessionTemporaryTable` reports
    /// `ErrDatabaseNotExists` (1049) before it builds anything.
    ///
    /// # Errors
    ///
    /// 1049 when `database` does not exist.
    pub fn register_local_temporary_in(
        &mut self,
        database: &str,
        name: &str,
        table: KvTable,
    ) -> Result<(), DriverError> {
        self.bump_metadata_version();
        let folded_database = database.to_lowercase();
        let folded_name = name.to_lowercase();
        let schema = self.databases.get_mut(&folded_database).ok_or_else(|| {
            DriverError::Schema(crate::SchemaErrorKind::UnknownDatabase(database.to_owned()))
        })?;
        if let Some(displaced) = schema
            .tables
            .insert(folded_name.clone(), std::sync::Arc::new(TableEntry::Kv(table)))
        {
            self.shadowed_by_local_temporary
                .push((folded_database, folded_name, displaced));
        }
        Ok(())
    }

    /// Attaches a session's LOCAL temporary tables for the duration of one
    /// statement -- Go `temptable.AttachLocalTemporaryTableInfoSchema`.
    ///
    /// Each entry is `(folded database, folded name, table)`. A name whose
    /// schema has since been dropped is DISCARDED rather than resurrecting
    /// the schema: Go keeps such a table alive in `SessionTables` (which owns
    /// its `DBInfo` by value) and this tier cannot, which is the one
    /// documented gap in the overlay.
    pub fn attach_local_temporary_tables(&mut self, tables: Vec<(String, String, KvTable)>) {
        // The version bump is gated on an ACTUAL visibility change: this runs
        // around EVERY statement via [`crate::txn`]'s overlay guard, and the
        // common case is a session with NO temporary tables, where attaching
        // inserts nothing. Bumping unconditionally moved the key-decode
        // metadata epoch twice per statement, which rebuilt the whole
        // TIDB_DECODE_KEY snapshot per statement (measured: ~60% of process
        // CPU in its allocation trail under sysbench point_select). Go's row-
        // decode caches hang off the INFOSCHEMA version, which a session
        // mounting its own temp tables never moves either.
        let changed = !tables.is_empty();
        if changed {
            self.bump_metadata_version();
        }
        for (database, name, table) in tables {
            let Some(schema) = self.databases.get_mut(&database) else {
                continue;
            };
            if let Some(displaced) = schema.tables.insert(name.clone(), std::sync::Arc::new(TableEntry::Kv(table))) {
                self.shadowed_by_local_temporary
                    .push((database, name, displaced));
            }
        }
    }

    /// Detaches every LOCAL temporary table, restoring what each one shadowed
    /// -- Go `temptable.DetachLocalTemporaryTableInfoSchema`.
    ///
    /// The tables are found by their own metadata rather than by the list
    /// that was attached, which is what makes the statement's own DDL come
    /// out right: a `CREATE TEMPORARY TABLE` run during the statement leaves
    /// here, and a `DROP TEMPORARY TABLE` does not. Restoring a shadowed
    /// entry only into an EMPTY slot is the other half of that: a statement
    /// that dropped the temporary table and created a permanent one under the
    /// same name keeps the new table rather than the resurrected old one.
    pub fn take_local_temporary_tables(&mut self) -> Vec<(String, String, KvTable)> {
        // Same gate as [`Self::attach_local_temporary_tables`]: a session
        // with no local temporary tables detaches nothing, so the key-decode
        // metadata epoch must not move either. The list itself comes from the
        // memoized sweep rather than a fresh walk.
        let slots = self.ensure_temporary_sweep().0.to_vec();
        let moved_entries = !slots.is_empty();
        if moved_entries {
            self.bump_metadata_version();
        }
        let mut taken = Vec::with_capacity(slots.len());
        for (folded_database, folded_name) in slots {
            let Some(schema) = self.databases.get_mut(&folded_database) else {
                continue;
            };
            let Some(entry) = schema.tables.remove(&folded_name) else {
                continue;
            };
            let TableEntry::Kv(table) = Arc::unwrap_or_clone(entry) else {
                continue;
            };
            taken.push((folded_database, folded_name, table));
        }
        for (folded_database, folded_name, entry) in
            std::mem::take(&mut self.shadowed_by_local_temporary)
        {
            let Some(schema) = self.databases.get_mut(&folded_database) else {
                continue;
            };
            schema.tables.entry(folded_name).or_insert_with(|| entry);
        }
        if moved_entries {
            self.temporary_sweep = None;
        }
        taken
    }

    /// Every GLOBAL temporary table in the catalog, by physical id, for the
    /// session that has to swap its own rows in.
    ///
    /// A global temporary table's `TableInfo` is shared by every session (it
    /// is created by a real DDL job) while its ROWS are private to one and
    /// die with the transaction that wrote them -- Go
    /// `temptable.TemporaryTableSnapshotInterceptor`, whose `iterTable`
    /// answers an EMPTY iterator for `TempTableGlobal` so nothing outside the
    /// current transaction's own buffer is ever visible.
    pub fn global_temporary_table_ids(&self) -> Vec<(String, String)> {
        let mut ids = Vec::new();
        for (folded_database, schema) in &self.databases {
            for (folded_name, entry) in &schema.tables {
                if matches!(&**entry, TableEntry::Kv(table)
                    if table.temp_table_type() == tidb_model::TempTableType::GLOBAL)
                {
                    ids.push((folded_database.clone(), folded_name.clone()));
                }
            }
        }
        ids.sort();
        ids
    }

    /// The GLOBAL half of [`Self::global_temporary_table_ids`], served from
    /// the memoized sweep below: `txn`'s overlay guard asks this twice per
    /// statement, and the answer only moves when the table set does -- Go
    /// asks the same question of `temptable`'s own session map in O(1).
    pub fn global_temporary_table_ids_memo(&mut self) -> &[(String, String)] {
        self.ensure_temporary_sweep().1
    }

    /// The LOCAL and GLOBAL temporary-table id lists as of the CURRENT
    /// metadata epoch, rebuilt from ONE catalog walk when the memo is stale.
    ///
    /// Keyed on `metadata_version`, because exactly its mutators move the
    /// table set; [`Self::take_local_temporary_tables`] also drops the memo
    /// explicitly once it has removed entries under it.
    fn ensure_temporary_sweep(&mut self) -> (&[(String, String)], &[(String, String)]) {
        let epoch = self.metadata_version;
        if self
            .temporary_sweep
            .as_ref()
            .is_none_or(|(at, _, _)| *at != epoch)
        {
            let mut local: Vec<(String, String)> = Vec::new();
            let mut global: Vec<(String, String)> = Vec::new();
            for (folded_database, schema) in &self.databases {
                for (folded_name, entry) in &schema.tables {
                    let TableEntry::Kv(table) = &**entry else {
                        continue;
                    };
                    match table.temp_table_type() {
                        tidb_model::TempTableType::LOCAL => {
                            local.push((folded_database.clone(), folded_name.clone()))
                        }
                        tidb_model::TempTableType::GLOBAL => {
                            global.push((folded_database.clone(), folded_name.clone()))
                        }
                        _ => {}
                    }
                }
            }
            global.sort();
            self.temporary_sweep = Some((epoch, local, global));
        }
        match self.temporary_sweep.as_mut() {
            Some((at, locals, globals)) => {
                debug_assert_eq!(*at, epoch);
                (locals.as_slice(), globals.as_slice())
            }
            None => unreachable!("just stored"),
        }
    }

    /// A mutable handle on a table for the temporary-table overlay, which
    /// swaps row storage in and out without changing any schema.
    ///
    /// It deliberately does NOT bump [`Catalog::version`]: moving a session's
    /// own rows into and out of the slot they are read through is not a
    /// schema change, and counting it as one would abort every concurrent
    /// transaction whenever any session touched a temporary table.
    pub fn temporary_overlay_table_mut(
        &mut self,
        database: &str,
        name: &str,
    ) -> Option<&mut KvTable> {
        match self
            .databases
            .get_mut(database)?
            .tables
            .get_mut(name)
            .map(std::sync::Arc::make_mut)
        {
            Some(TableEntry::Kv(table)) => Some(table),
            _ => None,
        }
    }

    /// Registers a view in `database`, replacing whatever the name held --
    /// which is what `CREATE OR REPLACE VIEW` means. Reports 1049 when the
    /// schema does not exist.
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

    /// Every sequence in the catalog, keyed by lowercase `db.name`, with its
    /// allocator handle. The handles are `Arc`-shared, so this is a snapshot of
    /// the NAMES only -- a value consumed through one of them moves the
    /// counter the catalog holds. See [`crate::SequenceSnapshot`].
    #[must_use]
    pub fn sequence_allocators(&self) -> HashMap<String, crate::sequence::SequenceAllocator> {
        let mut out = HashMap::new();
        for (database_key, database) in &self.databases {
            for (table_key, entry) in &database.tables {
                if let TableEntry::Sequence(sequence) = entry.as_ref() {
                    out.insert(
                        format!("{database_key}.{table_key}"),
                        sequence.allocator.clone(),
                    );
                }
            }
        }
        out
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

    /// [`Catalog::sequence_in`] for the `ALTER SEQUENCE` path, which replaces
    /// the options on the entry in place.
    pub fn sequence_mut_in(&mut self, database: &str, name: &str) -> Option<&mut SequenceDef> {
        self.version += 1;
        match self.get_mut_in(database, name) {
            Some(TableEntry::Sequence(sequence)) => Some(sequence),
            _ => None,
        }
    }

    /// Whether a table with `name` exists in the default database.
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.get(name).is_some()
    }

    /// Allocates the next table id (a monotone counter standing in for the
    /// global autoid allocator, like KvTable's handle counter).
    pub fn allocate_table_id(&mut self) -> i64 {
        self.next_table_id += 1;
        self.next_table_id
    }
}

impl crate::keydecoder::KeyInfoCatalog for Catalog {
    fn resolve_physical_table(
        &self,
        physical_id: i64,
    ) -> Option<crate::keydecoder::KeyInfoTableLookup> {
        for database in self.databases.values() {
            for (registered_name, entry) in &database.tables {
                let TableEntry::Kv(table) = entry.as_ref() else {
                    continue;
                };
                if table.table_id == physical_id {
                    return Some(crate::keydecoder::KeyInfoTableLookup::Resolved(
                        key_info_table(database, registered_name, table, 0, String::new()),
                    ));
                }
            }
        }
        for database in self.databases.values() {
            for (registered_name, entry) in &database.tables {
                let TableEntry::Kv(table) = entry.as_ref() else {
                    continue;
                };
                let Some(partition) = table.partition() else {
                    continue;
                };
                if let Some(definition) = partition
                    .definitions
                    .iter()
                    .find(|definition| definition.id == physical_id)
                {
                    return Some(crate::keydecoder::KeyInfoTableLookup::Resolved(
                        key_info_table(
                            database,
                            registered_name,
                            table,
                            definition.id,
                            definition.name.clone(),
                        ),
                    ));
                }
            }
        }
        None
    }
}

fn key_info_table(
    database: &Database,
    registered_name: &str,
    table: &KvTable,
    partition_id: i64,
    partition_name: String,
) -> crate::keydecoder::KeyInfoTable {
    crate::keydecoder::KeyInfoTable {
        db_name: database.name.clone(),
        db_id: database.id,
        table_name: if table.name.is_empty() {
            registered_name.to_owned()
        } else {
            table.name.clone()
        },
        table_id: table.table_id,
        partition_name,
        partition_id,
        indexes: table
            .indexes()
            .iter()
            .map(|index| crate::keydecoder::KeyInfoIndex {
                id: index.id,
                name: index.name.clone(),
            })
            .collect(),
    }
}

/// Resolves unqualified/`t.`-qualified column names against one table's schema
/// (case-insensitive, as in MySQL).
pub(crate) struct TableResolver<'a> {
    pub(crate) table_name: &'a str,
    pub(crate) columns: &'a [(String, FieldType)],
    pub(crate) constant_context: crate::StmtContext,
    /// The statement's session `time_zone` (see [`ColumnResolver::time_zone`]),
    /// taken from the write's `StmtContext` at each build site.
    pub(crate) zone: tidb_expr::SessionTimeZone,
    pub(crate) no_unsigned_subtraction: bool,
    pub(crate) div_precision_increment: u32,
}

impl ColumnResolver for TableResolver<'_> {
    fn time_zone(&self) -> tidb_expr::SessionTimeZone {
        self.zone.clone()
    }

    fn date_modes(&self) -> tidb_datatype::DateModes {
        tidb_expr::Columns::date_modes(&self.constant_context)
    }

    fn connection_charset_info(&self) -> (&str, &str) {
        self.constant_context.connection_charset_info()
    }

    fn like_default_escape(&self) -> u8 {
        self.constant_context.like_default_escape()
    }

    fn no_unsigned_subtraction(&self) -> bool {
        self.no_unsigned_subtraction
    }

    fn div_precision_increment(&self) -> u32 {
        self.div_precision_increment
    }

    fn current_database(&self) -> Option<String> {
        tidb_expr::Columns::current_database(&self.constant_context)
    }

    fn fold_constant(&self, expression: &mut Expression, mode: tidb_expr::ConstantFoldMode) {
        tidb_expr::fold_constant_in_mode(expression, &self.constant_context, mode);
    }

    fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
        let (qualifier, name) = match path {
            [name] => (None, name),
            [table, name] => (Some(table), name),
            // db.t.a qualification waits on a multi-schema catalog.
            _ => return None,
        };
        if let Some(q) = qualifier {
            if !q.eq_ignore_ascii_case(self.table_name) {
                return None;
            }
        }
        self.columns
            .iter()
            .position(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|i| (i, self.columns[i].1.clone(), (i + 1) as i64))
    }
}
