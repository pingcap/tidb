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

//! Go `pkg/executor/lockstats` lands as a complete package: the executors
//! behind `LOCK STATS` and `UNLOCK STATS`, plus the two name-to-ID resolution
//! helpers they share.
//!
//! Every production symbol is here. From `lock_stats_executor.go`: `LockExec`
//! ([`LockExec`]) with `Next`, `onlyLockPartitions`, `Open`, and `Close`;
//! `populatePartitionIDAndNames` ([`populate_partition_id_and_names`]);
//! `populateTableAndPartitionIDs` ([`populate_table_and_partition_ids`]); and
//! `genFullPartitionName` ([`gen_full_partition_name`]). From
//! `unlock_stats_executor.go`: `UnlockExec` ([`UnlockExec`]) with `Next`,
//! `onlyUnlockPartitions`, `Open`, and `Close`. Both upstream tests are
//! ported below.
//!
//! The two executors are near-twins with three deliberate differences the
//! source preserves and this module reproduces exactly: the error text
//! (`"Lock Stats: ..."` versus `"Unlock Stats: ..."`, the latter carrying the
//! source's trailing space), the handle methods called, and — the one that
//! actually reaches users — the case of the table name handed to the handle.
//! `LockExec` formats it from `Schema.L`/`Name.L` while `UnlockExec` formats
//! it from `Schema.O`/`Name.O`, so an unlock message echoes the name as
//! written. See [`LockExec::next`] and [`UnlockExec::next`].
//!
//! Narrowed dependencies, each named at its definition:
//!
//! - `// boundary:` Go `pkg/parser/ast.TableName` — a full AST node with
//!   ~10 fields, of which lockstats reads three. [`LockStatsTableName`]
//!   carries exactly those three.
//! - `// boundary:` Go `pkg/domain.GetDomain(ctx).InfoSchema()`,
//!   `pkg/infoschema.InfoSchema.TableByName`, and `pkg/table.Table.Meta()` —
//!   collapsed into [`SchemaResolver`], one name-to-`TableInfo` lookup. The
//!   `domain` hop exists in Go only to reach the info schema off the session
//!   context; nothing in this package uses the domain for anything else.
//! - `// boundary:` Go `pkg/table/tables.FindPartitionByName` — a single
//!   free function over `TableInfo`, reproduced as
//!   [`find_partition_by_name`] rather than pulled in with the rest of the
//!   `tables` package.
//! - `// boundary:` Go `pkg/statistics/handle/types.StatsHandle`'s lock
//!   surface — `LockPartitions`, `LockTables`, `RemoveLockedPartitions`, and
//!   `RemoveLockedTables` — narrowed to [`StatsLockHandle`]. The handle's
//!   storage and session plumbing is not this package's.
//! - `// boundary:` Go `e.Ctx().GetSessionVars().StmtCtx.AppendWarning` —
//!   answered by the crate's existing [`WarningAppender`], so the skipped-
//!   table message a handle returns still surfaces as a statement warning.
//!
//! Go's `exec.BaseExecutor` embedding is not reproduced: it supplies the
//! session context, schema, and children that these two executors never read
//! (`Next` ignores its chunk, `Open`/`Close` are pure no-ops), so the
//! narrowed dependencies above are passed to [`LockExec::next`] directly.

use std::collections::BTreeMap;
use std::fmt;

use tidb_ast::CiString;
use tidb_model::table_info::TableInfo;
use tidb_stats::StatsLockTable;

use crate::warning_publication::WarningAppender;

/// Why a `LOCK STATS` / `UNLOCK STATS` statement could not resolve or apply.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LockStatsError {
    /// Go `errors.New("partition list should not be empty")`.
    EmptyPartitionList,
    /// Go `errors.New("table list should not be empty")`.
    EmptyTableList,
    /// Go `errors.Errorf("table %s is not a partition table")`, carrying the
    /// lowercased `schema.table`.
    NotPartitionTable(String),
    /// The info schema had no such table; carries `schema.table`.
    ///
    /// boundary: Go surfaces `infoschema.ErrTableNotExists` from
    /// `TableByName`; the resolver decides the exact catalog error, so this
    /// module only records which name failed.
    TableNotExists(String),
    /// Go `table.ErrUnknownPartition`.
    UnknownPartition {
        /// The lowercased partition name that was not found.
        partition: String,
        /// The original-case table name, matching Go's `meta.Name.O`.
        table: String,
    },
    /// Go `errors.New("Lock Stats: handle is nil")` and its unlock twin.
    HandleIsNil(&'static str),
    /// Go `errors.New("Lock Stats: table should not empty")` and its unlock
    /// twin (which carries a trailing space upstream).
    TableShouldNotEmpty(&'static str),
    /// The narrowed statistics handle rejected the lock or unlock.
    Handle(String),
}

impl fmt::Display for LockStatsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyPartitionList => formatter.write_str("partition list should not be empty"),
            Self::EmptyTableList => formatter.write_str("table list should not be empty"),
            Self::NotPartitionTable(name) => {
                write!(formatter, "table {name} is not a partition table")
            }
            Self::TableNotExists(name) => write!(formatter, "table {name} doesn't exist"),
            Self::UnknownPartition { partition, table } => {
                write!(
                    formatter,
                    "Unknown partition '{partition}' in table '{table}'"
                )
            }
            Self::HandleIsNil(statement) => write!(formatter, "{statement}: handle is nil"),
            Self::TableShouldNotEmpty(message) => formatter.write_str(message),
            Self::Handle(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for LockStatsError {}

/// boundary: Go `pkg/parser/ast.TableName`, narrowed to the three fields
/// `lockstats` reads. When the statement locks partitions, `partition_names`
/// is non-empty and the executor holds exactly one of these.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LockStatsTableName {
    /// Go `TableName.Schema`.
    pub schema: CiString,
    /// Go `TableName.Name`.
    pub name: CiString,
    /// Go `TableName.PartitionNames`.
    pub partition_names: Vec<CiString>,
}

impl LockStatsTableName {
    /// Builds a table reference with no partition list.
    #[must_use]
    pub fn new(schema: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            schema: CiString::new(schema),
            name: CiString::new(name),
            partition_names: Vec::new(),
        }
    }

    /// Builds a table reference naming specific partitions.
    #[must_use]
    pub fn with_partitions<S: Into<String>>(
        schema: impl Into<String>,
        name: impl Into<String>,
        partitions: impl IntoIterator<Item = S>,
    ) -> Self {
        Self {
            schema: CiString::new(schema),
            name: CiString::new(name),
            partition_names: partitions.into_iter().map(CiString::new).collect(),
        }
    }

    /// Go's `fmt.Sprintf("%s.%s", table.Schema.L, table.Name.L)`.
    #[must_use]
    pub fn lowercase_full_name(&self) -> String {
        format!("{}.{}", self.schema.lowercase(), self.name.lowercase())
    }

    /// Go's `fmt.Sprintf("%s.%s", table.Schema.O, table.Name.O)`, which only
    /// `UnlockExec` uses.
    #[must_use]
    pub fn original_full_name(&self) -> String {
        format!("{}.{}", self.schema.original(), self.name.original())
    }
}

/// boundary: Go `pkg/domain.GetDomain(ctx).InfoSchema()` plus
/// `pkg/infoschema.InfoSchema.TableByName(ctx, schema, name)` and the
/// `table.Table.Meta()` hop that immediately follows every call site here.
///
/// `lockstats` never touches a live `table.Table` — it reads `ID`, `Name`,
/// and `Partition` off the metadata — so the narrowed contract returns
/// `TableInfo` directly.
pub trait SchemaResolver {
    /// Resolves one schema-qualified name to its table metadata.
    ///
    /// # Errors
    ///
    /// Returns [`LockStatsError::TableNotExists`] when the catalog has no
    /// such table.
    fn table_by_name(
        &self,
        schema: &CiString,
        name: &CiString,
    ) -> Result<&TableInfo, LockStatsError>;
}

/// boundary: the lock surface of Go
/// `pkg/statistics/handle/types.StatsHandle`, which the executors reach via
/// `domain.GetDomain(ctx).StatsHandle()`.
///
/// Each method returns the source's `msg` string: empty when everything was
/// applied, otherwise a human-readable note about tables or partitions the
/// handle skipped, which the caller republishes as a statement warning.
pub trait StatsLockHandle {
    /// Go `StatsHandle.LockPartitions`.
    ///
    /// # Errors
    ///
    /// Propagates the handle's own failure as [`LockStatsError::Handle`].
    fn lock_partitions(
        &self,
        table_id: i64,
        table_name: &str,
        partition_names: &BTreeMap<i64, String>,
    ) -> Result<String, LockStatsError>;

    /// Go `StatsHandle.LockTables`.
    ///
    /// # Errors
    ///
    /// Propagates the handle's own failure as [`LockStatsError::Handle`].
    fn lock_tables(&self, tables: &BTreeMap<i64, StatsLockTable>)
        -> Result<String, LockStatsError>;

    /// Go `StatsHandle.RemoveLockedPartitions`.
    ///
    /// # Errors
    ///
    /// Propagates the handle's own failure as [`LockStatsError::Handle`].
    fn remove_locked_partitions(
        &self,
        table_id: i64,
        table_name: &str,
        partition_names: &BTreeMap<i64, String>,
    ) -> Result<String, LockStatsError>;

    /// Go `StatsHandle.RemoveLockedTables`.
    ///
    /// # Errors
    ///
    /// Propagates the handle's own failure as [`LockStatsError::Handle`].
    fn remove_locked_tables(
        &self,
        tables: &BTreeMap<i64, StatsLockTable>,
    ) -> Result<String, LockStatsError>;
}

/// boundary: Go `pkg/table/tables.FindPartitionByName`.
///
/// The source reads `meta.Partition.Definitions` directly, *not*
/// `GetPartitionInfo()`, so a partitioned table whose `Enable` flag is off
/// still resolves here. Callers in this module gate on `GetPartitionInfo`
/// first, which is what makes the difference unobservable.
///
/// # Errors
///
/// Returns [`LockStatsError::UnknownPartition`] when no definition matches.
pub fn find_partition_by_name(
    meta: &TableInfo,
    partition_name: &str,
) -> Result<i64, LockStatsError> {
    let wanted = partition_name.to_lowercase();
    let found = meta.partition.as_ref().and_then(|partition| {
        partition
            .read()
            .definitions
            .map_visible(|definition| {
                definition
                    .name
                    .lowercase()
                    .eq_ignore_ascii_case(&wanted)
                    .then_some(definition.id)
            })
            .into_iter()
            .flatten()
            .next()
    });
    found.ok_or_else(|| LockStatsError::UnknownPartition {
        partition: wanted,
        table: meta.name.original().to_owned(),
    })
}

/// Go `populatePartitionIDAndNames`: the table ID plus a partition-ID-to-name
/// map for the named partitions.
///
/// `table` is optional because the source parameter is a pointer that its own
/// test passes as nil: the empty-partition-list check runs first and returns
/// before the pointer is dereferenced.
///
/// # Errors
///
/// [`LockStatsError::EmptyPartitionList`] when no partitions are named,
/// [`LockStatsError::NotPartitionTable`] when the resolved table is not
/// partitioned, or whatever the resolver and partition lookup report.
pub fn populate_partition_id_and_names(
    table: Option<&LockStatsTableName>,
    partition_names: &[CiString],
    schema: &dyn SchemaResolver,
) -> Result<(i64, BTreeMap<i64, String>), LockStatsError> {
    if partition_names.is_empty() {
        return Err(LockStatsError::EmptyPartitionList);
    }
    let table = table.ok_or(LockStatsError::EmptyPartitionList)?;
    let meta = schema.table_by_name(&table.schema, &table.name)?;

    if meta.get_partition_info().is_none() {
        return Err(LockStatsError::NotPartitionTable(
            table.lowercase_full_name(),
        ));
    }

    let mut partition_id_names = BTreeMap::new();
    for partition_name in partition_names {
        let partition_id = find_partition_by_name(meta, partition_name.lowercase())?;
        partition_id_names.insert(partition_id, partition_name.lowercase().to_owned());
    }

    Ok((meta.id, partition_id_names))
}

/// Go `populateTableAndPartitionIDs`: one [`StatsLockTable`] per named table,
/// keyed by table ID.
///
/// A non-partitioned table contributes a payload whose partition map stays
/// nil (`None`), matching the source's early `continue`; a partitioned table
/// gets an allocated map naming every visible definition.
///
/// # Errors
///
/// [`LockStatsError::EmptyTableList`] when no tables are named, or whatever
/// the resolver reports.
pub fn populate_table_and_partition_ids(
    tables: &[LockStatsTableName],
    schema: &dyn SchemaResolver,
) -> Result<BTreeMap<i64, StatsLockTable>, LockStatsError> {
    if tables.is_empty() {
        return Err(LockStatsError::EmptyTableList);
    }
    let mut table_with_partitions = BTreeMap::new();

    for table in tables {
        let meta = schema.table_by_name(&table.schema, &table.name)?;
        let partition_info = meta.get_partition_info().map(|partition| {
            partition
                .read()
                .definitions
                .map_visible(|definition| {
                    (
                        definition.id,
                        gen_full_partition_name(table, definition.name.lowercase()),
                    )
                })
                .into_iter()
                .collect::<BTreeMap<i64, String>>()
        });
        table_with_partitions.insert(
            meta.id,
            StatsLockTable::new(table.lowercase_full_name(), partition_info),
        );
    }

    Ok(table_with_partitions)
}

/// Go `genFullPartitionName`: `"schema.table partition (partition_name)"`.
#[must_use]
pub fn gen_full_partition_name(table: &LockStatsTableName, partition_name: &str) -> String {
    format!(
        "{}.{} partition ({partition_name})",
        table.schema.lowercase(),
        table.name.lowercase()
    )
}

/// Go `LockExec`: the executor behind `LOCK STATS`.
#[derive(Clone, Debug, Default)]
pub struct LockExec {
    /// Go `LockExec.Tables`. When locking partitions this holds exactly one
    /// entry, whose `partition_names` is non-empty.
    pub tables: Vec<LockStatsTableName>,
}

impl LockExec {
    /// Builds the executor over the statement's table list.
    #[must_use]
    pub fn new(tables: Vec<LockStatsTableName>) -> Self {
        Self { tables }
    }

    /// Go `LockExec.onlyLockPartitions`.
    #[must_use]
    pub fn only_lock_partitions(&self) -> bool {
        self.tables.len() == 1 && !self.tables[0].partition_names.is_empty()
    }

    /// Go `LockExec.Next`. The chunk parameter is unused upstream too — this
    /// executor produces no rows.
    ///
    /// # Errors
    ///
    /// A nil handle, an empty table list, or any resolution or handle
    /// failure. A non-empty skipped-tables message from the handle is not an
    /// error: it is appended as a statement warning.
    pub fn next(
        &self,
        handle: Option<&dyn StatsLockHandle>,
        schema: &dyn SchemaResolver,
        warnings: &dyn WarningAppender,
    ) -> Result<(), LockStatsError> {
        let Some(handle) = handle else {
            return Err(LockStatsError::HandleIsNil("Lock Stats"));
        };
        if self.tables.is_empty() {
            return Err(LockStatsError::TableShouldNotEmpty(
                "Lock Stats: table should not empty",
            ));
        }

        let message = if self.only_lock_partitions() {
            let table = &self.tables[0];
            let (table_id, partition_id_names) =
                populate_partition_id_and_names(Some(table), &table.partition_names, schema)?;
            // Go formats the lock message's table name from the LOWERCASED
            // identifiers; `UnlockExec` uses the original case instead.
            handle.lock_partitions(table_id, &table.lowercase_full_name(), &partition_id_names)?
        } else {
            let table_with_partitions = populate_table_and_partition_ids(&self.tables, schema)?;
            handle.lock_tables(&table_with_partitions)?
        };

        if !message.is_empty() {
            warnings.append_warning(message);
        }
        Ok(())
    }

    /// Go `LockExec.Open`: a no-op.
    pub fn open(&self) {}

    /// Go `LockExec.Close`: a no-op.
    pub fn close(&self) {}
}

/// Go `UnlockExec`: the executor behind `UNLOCK STATS`.
#[derive(Clone, Debug, Default)]
pub struct UnlockExec {
    /// Go `UnlockExec.Tables`.
    pub tables: Vec<LockStatsTableName>,
}

impl UnlockExec {
    /// Builds the executor over the statement's table list.
    #[must_use]
    pub fn new(tables: Vec<LockStatsTableName>) -> Self {
        Self { tables }
    }

    /// Go `UnlockExec.onlyUnlockPartitions`.
    #[must_use]
    pub fn only_unlock_partitions(&self) -> bool {
        self.tables.len() == 1 && !self.tables[0].partition_names.is_empty()
    }

    /// Go `UnlockExec.Next`.
    ///
    /// # Errors
    ///
    /// A nil handle, an empty table list, or any resolution or handle
    /// failure.
    pub fn next(
        &self,
        handle: Option<&dyn StatsLockHandle>,
        schema: &dyn SchemaResolver,
        warnings: &dyn WarningAppender,
    ) -> Result<(), LockStatsError> {
        let Some(handle) = handle else {
            return Err(LockStatsError::HandleIsNil("Unlock Stats"));
        };
        if self.tables.is_empty() {
            // The source's message carries a trailing space here, where the
            // lock twin's does not. Kept byte-exact.
            return Err(LockStatsError::TableShouldNotEmpty(
                "Unlock Stats: table should not empty ",
            ));
        }

        let message = if self.only_unlock_partitions() {
            let table = &self.tables[0];
            let (table_id, partition_id_names) =
                populate_partition_id_and_names(Some(table), &table.partition_names, schema)?;
            // Unlike `LockExec`, the unlock path names the table in its
            // ORIGINAL case.
            handle.remove_locked_partitions(
                table_id,
                &table.original_full_name(),
                &partition_id_names,
            )?
        } else {
            let table_with_partitions = populate_table_and_partition_ids(&self.tables, schema)?;
            handle.remove_locked_tables(&table_with_partitions)?
        };

        if !message.is_empty() {
            warnings.append_warning(message);
        }
        Ok(())
    }

    /// Go `UnlockExec.Open`: a no-op.
    pub fn open(&self) {}

    /// Go `UnlockExec.Close`: a no-op.
    pub fn close(&self) {}
}
