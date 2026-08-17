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

//! `pkg/ddl/schematracker/info_store.go`: `InfoStore`, the
//! `lower_case_table_names`-aware store of `DBInfo`/`TableInfo` used by the
//! schema tracker.
//!
//! # Package status: seed, not a package claim
//!
//! `pkg/ddl/schematracker` has three production files. This module ports
//! exactly one of them, so the package remains a **seed**; it is deliberately
//! not reported as a transcreated package.
//!
//! * `info_store.go` (205 LOC) — ported here.
//! * `dm_tracker.go` (1216 LOC) — **not ported**. It is a full DDL executor
//!   replica that calls roughly forty functions across the unported
//!   `pkg/ddl` executor/column/index surface (`BuildTableInfoWithStmt`,
//!   `AlterTableAddColumn`-class helpers, index build/drop paths, partition
//!   handling). Nothing it needs exists in Rust yet, so porting it would mean
//!   inventing that surface.
//! * `checker.go` (633 LOC) — **not ported**. It is a differential test
//!   harness that runs every DDL statement through both a real TiDB `DDL`
//!   implementation and the tracker and diffs the results; it depends on the
//!   live `ddl.Executor`, `sessionctx`, and `domain` surfaces.
//!
//! # Narrowings
//!
//! * **`infoschema` errors → [`InfoStoreError`]** (`// boundary:`). Go returns
//!   `infoschema.ErrDatabaseNotExists` / `infoschema.ErrTableNotExists`, which
//!   are `dbterror.ClassSchema` standard errors carrying MySQL codes 1049 and
//!   1146. Importing `pkg/infoschema` here would drag the whole InfoSchema
//!   implementation in for two error values, so this module defines a local
//!   error carrying the same MySQL error codes and byte-identical message
//!   text from `pkg/errno/errname.go`.
//! * **`InitFromIS` (`info_store.go:45-60`) is skipped.** Its entire body is
//!   `infoschema.InfoSchema` iteration (`AllSchemas`, `SchemaTableInfos`);
//!   that interface is the boundary this module explicitly does not cross.
//! * **`InfoStoreAdaptor.TableByName` (`info_store.go:194-200`) is skipped.**
//!   It returns a `table.Table` built by `tables.MockTableFromMeta`, i.e. the
//!   `pkg/table` runtime-table surface. The three adaptor methods that only
//!   need `model` types are ported.
//! * **Ownership.** Go stores `*model.DBInfo` / `*model.TableInfo` pointers and
//!   hands them back to callers for in-place mutation. This module owns the
//!   values and lends references; the only Go caller that mutated through the
//!   returned pointer is `dm_tracker.go`, which is not ported.
//! * **[`InfoStore::table_cloned_by_name`]** uses Rust's derived
//!   `TableInfo: Clone`. Go's `TableInfo.Clone` additionally duplicates the
//!   column/index elements, whereas `tidb-model`'s `GoSharedPointerSlice`
//!   clone shares the element handles. `tidb-model` has no port of Go's
//!   semi-deep copy to call, so the depth difference is recorded here rather
//!   than reimplemented.

use std::collections::HashMap;
use std::fmt;

use tidb_ast::CiString;
use tidb_model::db::DBInfo;
use tidb_model::table_info::TableInfo;

/// MySQL `ER_BAD_DB_ERROR`, the code behind `infoschema.ErrDatabaseNotExists`.
///
/// Source: `pkg/errno/errcode.go:70`, `pkg/infoschema/error.go:30`.
pub const ERR_BAD_DB: u16 = 1049;

/// MySQL `ER_NO_SUCH_TABLE`, the code behind `infoschema.ErrTableNotExists`.
///
/// Source: `pkg/errno/errcode.go:167`, `pkg/infoschema/error.go:70`.
pub const ERR_NO_SUCH_TABLE: u16 = 1146;

// boundary: Go raises `infoschema.ErrDatabaseNotExists` and
// `infoschema.ErrTableNotExists`. `pkg/infoschema` is not ported, and pulling
// it in for two error values would import the whole InfoSchema implementation,
// so this local error stands in. It carries the same MySQL error codes and the
// message templates from `pkg/errno/errname.go:73,172`, formatted with the
// names' original spelling exactly as Go's `%s` on `ast.CIStr` does.
/// The errors `InfoStore` lookups can fail with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InfoStoreError {
    /// Go `infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schema)`.
    DatabaseNotExists {
        /// The schema name, in its original spelling.
        database: String,
    },
    /// Go `infoschema.ErrTableNotExists.GenWithStackByArgs(schema, table)`.
    TableNotExists {
        /// The schema name, in its original spelling.
        database: String,
        /// The table name, in its original spelling.
        table: String,
    },
}

impl InfoStoreError {
    /// The MySQL error code, matching what `dbterror.ClassSchema` attaches.
    #[must_use]
    pub fn code(&self) -> u16 {
        match self {
            Self::DatabaseNotExists { .. } => ERR_BAD_DB,
            Self::TableNotExists { .. } => ERR_NO_SUCH_TABLE,
        }
    }
}

impl fmt::Display for InfoStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // `pkg/errno/errname.go:73`: "Unknown database '%-.192s'".
            Self::DatabaseNotExists { database } => {
                write!(formatter, "Unknown database '{database}'")
            }
            // `pkg/errno/errname.go:172`:
            // "Table '%-.192s.%-.192s' doesn't exist".
            Self::TableNotExists { database, table } => {
                write!(formatter, "Table '{database}.{table}' doesn't exist")
            }
        }
    }
}

impl std::error::Error for InfoStoreError {}

/// Go `InfoStore` (`info_store.go:28-33`): a simple store of `DBInfo` and
/// `TableInfo`. It is modifiable and not thread-safe.
#[derive(Clone, Debug)]
pub struct InfoStore {
    /// Same as the `lower_case_table_names` system variable.
    lower_case_table_names: i32,
    dbs: HashMap<String, DBInfo>,
    tables: HashMap<String, HashMap<String, TableInfo>>,
}

impl InfoStore {
    /// Go `NewInfoStore` (`info_store.go:36-42`).
    #[must_use]
    pub fn new(lower_case_table_names: i32) -> Self {
        Self {
            lower_case_table_names,
            dbs: HashMap::new(),
            tables: HashMap::new(),
        }
    }

    /// Go `ciStr2Key` (`info_store.go:62-67`): the name-folding rule. Only
    /// `lower_case_table_names == 0` keys on the original spelling; every other
    /// value keys on the folded form.
    fn ci_str2key(&self, name: &CiString) -> String {
        if self.lower_case_table_names == 0 {
            name.original().to_owned()
        } else {
            name.lowercase().to_owned()
        }
    }

    /// Go `SchemaByName` (`info_store.go:70-73`): the `DBInfo` of the given
    /// name, `None` when not found.
    #[must_use]
    pub fn schema_by_name(&self, name: &CiString) -> Option<&DBInfo> {
        self.dbs.get(&self.ci_str2key(name))
    }

    /// Go `PutSchema` (`info_store.go:76-82`): stores a `DBInfo`, overwriting
    /// any old one, and makes sure the schema has a table map.
    pub fn put_schema(&mut self, db_info: DBInfo) {
        let key = self.ci_str2key(&db_info.name);
        self.dbs.insert(key.clone(), db_info);
        self.tables.entry(key).or_default();
    }

    /// Go `DeleteSchema` (`info_store.go:85-94`): returns true when the schema
    /// existed. Deleting a schema drops its tables with it.
    pub fn delete_schema(&mut self, name: &CiString) -> bool {
        let key = self.ci_str2key(name);
        if self.dbs.remove(&key).is_none() {
            return false;
        }
        self.tables.remove(&key);
        true
    }

    /// Go `TableByName` (`info_store.go:97-110`). Go takes a `context.Context`
    /// it never reads; the parameter is dropped here.
    pub fn table_by_name(
        &self,
        schema: &CiString,
        table: &CiString,
    ) -> Result<&TableInfo, InfoStoreError> {
        let tables = self.tables.get(&self.ci_str2key(schema)).ok_or_else(|| {
            InfoStoreError::DatabaseNotExists {
                database: schema.original().to_owned(),
            }
        })?;
        tables
            .get(&self.ci_str2key(table))
            .ok_or_else(|| InfoStoreError::TableNotExists {
                database: schema.original().to_owned(),
                table: table.original().to_owned(),
            })
    }

    /// Go `TableClonedByName` (`info_store.go:113-119`): like
    /// [`Self::table_by_name`], plus a clone. See the module-level narrowing
    /// note on clone depth.
    pub fn table_cloned_by_name(
        &self,
        schema: &CiString,
        table: &CiString,
    ) -> Result<TableInfo, InfoStoreError> {
        self.table_by_name(schema, table).cloned()
    }

    /// Go `PutTable` (`info_store.go:122-131`): stores a `TableInfo`,
    /// overwriting any old one. Fails when the schema does not exist.
    pub fn put_table(
        &mut self,
        schema_name: &CiString,
        table_info: TableInfo,
    ) -> Result<(), InfoStoreError> {
        let schema_key = self.ci_str2key(schema_name);
        let table_key = self.ci_str2key(&table_info.name);
        let tables =
            self.tables
                .get_mut(&schema_key)
                .ok_or_else(|| InfoStoreError::DatabaseNotExists {
                    database: schema_name.original().to_owned(),
                })?;
        tables.insert(table_key, table_info);
        Ok(())
    }

    /// Go `DeleteTable` (`info_store.go:135-149`): fails with
    /// [`InfoStoreError::DatabaseNotExists`] or
    /// [`InfoStoreError::TableNotExists`] when schema or table is missing.
    pub fn delete_table(
        &mut self,
        schema: &CiString,
        table: &CiString,
    ) -> Result<(), InfoStoreError> {
        let schema_key = self.ci_str2key(schema);
        let table_key = self.ci_str2key(table);
        let tables =
            self.tables
                .get_mut(&schema_key)
                .ok_or_else(|| InfoStoreError::DatabaseNotExists {
                    database: schema.original().to_owned(),
                })?;
        if tables.remove(&table_key).is_none() {
            return Err(InfoStoreError::TableNotExists {
                database: schema.original().to_owned(),
                table: table.original().to_owned(),
            });
        }
        Ok(())
    }

    /// Go `AllSchemaNames` (`info_store.go:152-158`): the stored keys, so the
    /// spelling follows the folding rule. Order is unspecified, matching Go's
    /// map range.
    #[must_use]
    pub fn all_schema_names(&self) -> Vec<String> {
        self.dbs.keys().cloned().collect()
    }

    /// Go `AllTableNamesOfSchema` (`info_store.go:161-172`): the stored table
    /// keys of one schema, in unspecified order.
    pub fn all_table_names_of_schema(
        &self,
        schema: &CiString,
    ) -> Result<Vec<String>, InfoStoreError> {
        let tables = self.tables.get(&self.ci_str2key(schema)).ok_or_else(|| {
            InfoStoreError::DatabaseNotExists {
                database: schema.original().to_owned(),
            }
        })?;
        Ok(tables.keys().cloned().collect())
    }
}

/// Go `InfoStoreAdaptor` (`info_store.go:176-179`): presents an [`InfoStore`]
/// through the slice of the `InfoSchema` interface the DDL layer needs.
///
/// Go embeds `infoschema.InfoSchema` so the unimplemented methods panic on
/// use; there is no such interface in Rust yet, so this is a plain borrowing
/// wrapper exposing only the methods Go actually overrides that stay inside
/// `model` types. `TableByName` (`info_store.go:194-200`) is omitted: it
/// returns a `table.Table` from `tables.MockTableFromMeta`.
#[derive(Clone, Copy, Debug)]
pub struct InfoStoreAdaptor<'store> {
    inner: &'store InfoStore,
}

impl<'store> InfoStoreAdaptor<'store> {
    /// Wraps an [`InfoStore`]. Go builds the struct literal directly.
    #[must_use]
    pub fn new(inner: &'store InfoStore) -> Self {
        Self { inner }
    }

    /// Go `InfoStoreAdaptor.SchemaByName` (`info_store.go:182-185`). Go's
    /// `(dbInfo, dbInfo != nil)` pair collapses into `Option`.
    #[must_use]
    pub fn schema_by_name(&self, schema: &CiString) -> Option<&'store DBInfo> {
        self.inner.schema_by_name(schema)
    }

    /// Go `InfoStoreAdaptor.TableExists` (`info_store.go:188-191`).
    #[must_use]
    pub fn table_exists(&self, schema: &CiString, table: &CiString) -> bool {
        self.inner.table_by_name(schema, table).is_ok()
    }

    /// Go `InfoStoreAdaptor.TableInfoByName` (`info_store.go:203-205`).
    pub fn table_info_by_name(
        &self,
        schema: &CiString,
        table: &CiString,
    ) -> Result<&'store TableInfo, InfoStoreError> {
        self.inner.table_by_name(schema, table)
    }
}
