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
        _ => Err(DriverError::Unsupported("empty table name")),
    }
}

/// Go's default schema: TiDB's bootstrap runs
/// `CREATE DATABASE IF NOT EXISTS test`, so a fresh server always has it and
/// a connection with no explicit database lands there.
pub const DEFAULT_DATABASE: &str = "test";

/// One schema: Go `model.DBInfo`, reduced to the name and its tables.
///
/// NOT MODELLED (documented): the schema's charset, collation, placement
/// policy and state, which live on Go's `DBInfo` and matter to DDL rather
/// than to resolving a name.
#[derive(Clone, Debug, Default)]
struct Database {
    /// The name as written, for `SHOW DATABASES` output.
    name: String,
    tables: HashMap<String, TableEntry>,
}

/// A catalog of databases and their tables, the position Go's `infoschema`
/// occupies. Database and table names are case-insensitive, as in MySQL.
#[derive(Clone, Debug)]
pub struct Catalog {
    databases: HashMap<String, Database>,
    next_table_id: i64,
    /// Bumped by every mutation, so a transaction can detect that the shared
    /// catalog moved under it (Go detects the same at commit through TiKV's
    /// optimistic conflict check on the written keys).
    version: u64,
    /// Loaded statistics by physical table id, Go's `StatsHandle` cache as
    /// the planner sees it.
    ///
    /// A missing entry is Go's `statistics.PseudoTable`, and that is what
    /// makes an unanalyzed table's `EXPLAIN` print `stats:pseudo`. The map
    /// lives on the catalog rather than on a table because it is loaded from
    /// `mysql.stats_*` on its own cadence (see `tidb-exec`'s `stats_watch`),
    /// so the two are published independently.
    statistics: HashMap<i64, Arc<crate::access_cost::TableStatistics>>,
}

impl Default for Catalog {
    /// A catalog holding only `test`, as a freshly bootstrapped TiDB does.
    ///
    /// `INFORMATION_SCHEMA` is present because its tables are implemented
    /// (see `tidb-session`'s `infoschema`), and holds no stored tables of its
    /// own -- its rows are computed at query time.
    ///
    /// DIVERGENCE (documented): real TiDB also exposes `mysql`,
    /// `performance_schema`, `sys` and `metrics_schema`. Those are system
    /// schemas whose tables this seed does not implement, and listing them
    /// empty would claim more than is true, so they stay absent until their
    /// contents are ported.
    fn default() -> Self {
        let mut databases = HashMap::new();
        databases.insert(
            DEFAULT_DATABASE.to_owned(),
            Database {
                name: DEFAULT_DATABASE.to_owned(),
                tables: HashMap::new(),
            },
        );
        databases.insert(
            "information_schema".to_owned(),
            Database {
                name: "INFORMATION_SCHEMA".to_owned(),
                tables: HashMap::new(),
            },
        );
        Catalog {
            databases,
            next_table_id: 0,
            version: 0,
            statistics: HashMap::new(),
        }
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
    pub fn register(&mut self, name: &str, table: MemTable) {
        self.register_in(DEFAULT_DATABASE, name, TableEntry::Mem(table));
    }

    /// Registers a TiKV-format-byte-backed `table` in the default database.
    pub fn register_kv(&mut self, name: &str, table: KvTable) {
        self.register_in(DEFAULT_DATABASE, name, TableEntry::Kv(table));
    }

    /// Registers `table` in `database`, which must exist.
    fn register_in(&mut self, database: &str, name: &str, table: TableEntry) {
        self.version += 1;
        if let Some(database) = self.databases.get_mut(&database.to_lowercase()) {
            database.tables.insert(name.to_lowercase(), table);
        }
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
    #[must_use]
    pub fn table_names(&self, database: &str) -> Option<Vec<String>> {
        let database = self.databases.get(&database.to_lowercase())?;
        let mut names: Vec<String> = database.tables.keys().cloned().collect();
        names.sort();
        Some(names)
    }

    /// Whether `database` exists (Go `is.SchemaExists`).
    #[must_use]
    pub fn has_database(&self, database: &str) -> bool {
        self.databases.contains_key(&database.to_lowercase())
    }

    /// Creates `database`, reporting whether it was new. Go raises
    /// `ErrDBCreateExists` (1007) unless `IF NOT EXISTS` was written.
    pub fn create_database(&mut self, database: &str) -> bool {
        self.version += 1;
        let key = database.to_lowercase();
        if self.databases.contains_key(&key) {
            return false;
        }
        self.databases.insert(
            key,
            Database {
                name: database.to_owned(),
                tables: HashMap::new(),
            },
        );
        true
    }

    /// Moves a table to a new schema and name, which is what RENAME does.
    /// Returns `false` when the source does not exist.
    pub fn rename_table(
        &mut self,
        from_database: &str,
        from_name: &str,
        to_database: &str,
        to_name: &str,
    ) -> bool {
        self.version += 1;
        let Some(source) = self
            .databases
            .get_mut(&from_database.to_lowercase())
            .and_then(|database| database.tables.remove(&from_name.to_lowercase()))
        else {
            return false;
        };
        // The table carries its own name for duplicate-key messages.
        let mut source = source;
        if let TableEntry::Kv(table) = &mut source {
            table.set_name(to_name);
        }
        if let Some(database) = self.databases.get_mut(&to_database.to_lowercase()) {
            database.tables.insert(to_name.to_lowercase(), source);
        }
        true
    }

    /// Drops one table, reporting whether it existed.
    pub fn drop_table_in(&mut self, database: &str, name: &str) -> bool {
        self.version += 1;
        match self.databases.get_mut(&database.to_lowercase()) {
            Some(database) => database.tables.remove(&name.to_lowercase()).is_some(),
            None => false,
        }
    }

    /// Drops `database` and its tables, reporting whether it existed. Go
    /// raises `ErrDBDropExists` (1008) unless `IF EXISTS` was written.
    pub fn drop_database(&mut self, database: &str) -> bool {
        self.version += 1;
        self.databases.remove(&database.to_lowercase()).is_some()
    }

    /// Resolves a table in `database`.
    pub(crate) fn get_in(&self, database: &str, name: &str) -> Option<&TableEntry> {
        self.databases
            .get(&database.to_lowercase())?
            .tables
            .get(&name.to_lowercase())
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
        self.databases
            .get_mut(&database.to_ascii_lowercase())?
            .tables
            .get_mut(&name.to_ascii_lowercase())
    }

    /// The catalog's mutation counter.
    #[must_use]
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Publishes one table's loaded statistics, which the access-path choice
    /// and `EXPLAIN`'s `estRows` then read instead of the pseudo constants.
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
    pub fn register_mem_in(&mut self, database: &str, name: &str, table: MemTable) {
        let key = database.to_lowercase();
        self.databases.entry(key).or_insert_with(|| Database {
            name: database.to_owned(),
            tables: HashMap::new(),
        });
        self.register_in(database, name, TableEntry::Mem(table));
    }

    /// Registers a TiKV-format-byte-backed table in `database`.
    pub fn register_kv_in(&mut self, database: &str, name: &str, table: KvTable) {
        self.register_in(database, name, TableEntry::Kv(table));
    }

    /// Registers a view in `database`, replacing whatever the name held --
    /// which is what `CREATE OR REPLACE VIEW` means.
    pub fn register_view_in(&mut self, database: &str, name: &str, view: ViewDef) {
        self.register_in(database, name, TableEntry::View(view));
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
                if let TableEntry::Sequence(sequence) = entry {
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
    pub fn register_sequence_in(&mut self, database: &str, name: &str, sequence: SequenceDef) {
        self.register_in(database, name, TableEntry::Sequence(sequence));
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

/// Resolves unqualified/`t.`-qualified column names against one table's schema
/// (case-insensitive, as in MySQL).
pub(crate) struct TableResolver<'a> {
    pub(crate) table_name: &'a str,
    pub(crate) columns: &'a [(String, FieldType)],
}

impl ColumnResolver for TableResolver<'_> {
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
