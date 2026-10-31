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

//! The catalogue seam: what the plan builder is allowed to know about a table.
//!
//! Go sources: `pkg/infoschema.InfoSchema` (`TableByName`, `SchemaByName`),
//! `pkg/meta/model.TableInfo` / `ColumnInfo` / `IndexInfo` / `IndexColumn`,
//! and `pkg/table.Table`, as READ by `PlanBuilder.buildDataSource`
//! (`logical_plan_builder.go:4927`).
//!
//! [`TableSource`] keeps the immutable infoschema lookup behind a trait so the
//! planner does not own storage handles. Each [`SourceTable`] nevertheless
//! carries the shared, complete model objects that preprocessing records in
//! `resolve.Context`; planner-specific access-path inputs remain beside those
//! handles instead of replacing them.
//!
//! # The method set, and why exactly these
//!
//! [`SourceTable`] carries precisely the fields
//! `DataSource` declares, so
//! `crate::plan_builder::PlanBuilder::build_data_source` is a field-for-field
//! copy with no invention: `table_id`, `table_name`, `db_name`,
//! `physical_table_id`, `partition_def_idx`, `partition_definition_names`,
//! `columns` (id / name / `is_primary_key`), `pk_is_handle`, the handle
//! columns and `handle_is_int`, `common_handle_col_offsets` /
//! `common_handle_lens`, and `prefer_store_type`.
//!
//! Two things are here that `DataSource` does NOT store, because the BUILDER
//! needs them and the built operator does not:
//!
//! * [`SourceColumn::ret_type`] — `buildDataSource` turns every column into an
//!   `expression.Column`, which needs a `*types.FieldType`. `DataSource`
//!   keeps the type on its schema columns instead, so it never restates it.
//! * [`SourceTable::indexes`] — `crate::access_path` enumerates one path per
//!   usable index. `DataSource::possible_access_paths` holds the RESULT;
//!   [`SourceIndex`] is the input that result is derived from.
//!
//! # Narrowings, by exact Go symbol
//!
//! * `table.Table` (`pkg/table/table.go`). The runtime handle that reads and
//!   writes rows. Nothing in the LOGICAL build path calls it; the physical
//!   side reaches storage through `tidb-executor`. Not modelled.
//! * `statistics.Table` / `statistics.HistColl` (`TableStats`). Statistics
//!   arrive at `crate::cardinality` already derived; the builder only tags
//!   the operator, so no statistics handle crosses this seam.
//! * The builder's hot path caches `StatePublic` as
//!   [`SourceColumn::is_public`] / [`SourceIndex::is_public`], while the full
//!   states remain available through [`SourceTable::table_info`].
//! * `infoschema.InfoSchema.SchemaMetaVersion` / `AllSchemas` / the
//!   `TableByID` family. `buildDataSource` reaches the catalogue exactly once,
//!   by (db, table) name; the rest of the interface serves DDL and SHOW.
//! * Privilege checks (`visitInfo`). See
//!   `crate::plan_builder`'s header — dropped tree-wide, not narrowed here.

use std::collections::BTreeMap;
use tidb_mysql::PrivilegeType;
use tidb_model::{DBInfo, GoShared, GoSharedPointerSlice, IndexInfo, SchemaState, TableInfo};

/// Go `table.Column`: the shared `*model.ColumnInfo` and its parsed
/// `GeneratedExpr`.
pub type SourceColumn = tidb_table::column::Column;

/// Go `*model.IndexColumn`.
pub type SourceIndexColumn = tidb_model::IndexColumn;

/// Go `*model.IndexInfo`, shared with the table's `TableInfo.Indices`.
pub type SourceIndex = GoShared<IndexInfo>;

/// What the ported builder bodies read off a `*model.TableInfo` plus the
/// partition selection `buildDataSource` has already made.
///
/// One value per logical table. Static partition pruning later copies the
/// resulting `DataSource` once per surviving physical definition, exactly as
/// Go's `PartitionProcessor.makeUnionAllChildren` does.
#[derive(Clone, Debug, Default)]
pub struct SourceTable {
    /// Go `DBInfo`, shared by every table occurrence in one infoschema.
    pub db_info: Option<GoShared<DBInfo>>,
    /// Go `TableInfo`, shared by every occurrence of this table.
    pub table_info: Option<GoShared<TableInfo>>,
    /// Go `table.Type().IsVirtualTable()`: this table is planned through
    /// `buildMemTable`, not `buildDataSource`.
    pub is_memory_table: bool,
    /// Go `table.Type().IsClusterTable()`: a CLUSTER_* table is planned as a
    /// table whose scan reads from the TiDB store, with no extra handle or
    /// commit-ts column.
    pub is_cluster_table: bool,
    /// Go `TableInfo.ID`.
    pub table_id: i64,
    /// Go `TableInfo.Name.O`.
    pub table_name: String,
    /// Go `DBName.O`.
    pub db_name: String,
    /// Go `PhysicalTableID`: the partition's id, or the table's.
    pub physical_table_id: i64,
    /// Go `PartitionDefIdx`.
    pub partition_def_idx: Option<usize>,
    /// Whether Go `TableInfo.GetPartitionInfo()` is non-nil.
    pub is_partitioned: bool,
    /// Go `PartitionExpr.Expr` is a single column reference. Batch point-get
    /// eligibility on partitioned tables uses this same fast-plan condition.
    pub partition_expression_is_column: bool,
    /// Whether the partition metadata includes Go's online DROP overlap map.
    pub partition_is_reorganizing: bool,
    /// Go `TableInfo.GetPartitionInfo().Definitions[i].Name.O`.
    pub partition_definition_names: Vec<String>,
    /// Go `TableInfo.GetPartitionInfo().Definitions[i].ID`.
    pub partition_definition_ids: Vec<i64>,
    /// Go `TableInfo.Columns`, in offset order.
    pub columns: Vec<SourceColumn>,
    /// Go `TableInfo.Indices`, in declaration order.
    pub indexes: Vec<SourceIndex>,
    /// Go `TableInfo.PKIsHandle`.
    pub pk_is_handle: bool,
    /// Go `TableInfo.IsCommonHandle`.
    pub is_common_handle: bool,
    /// Go `TableInfo.CommonHandleVersion`.
    pub common_handle_version: u16,
    /// Go `TableInfo.TempTableType != model.TempTableNone`.
    pub is_temporary: bool,
    /// Go `TableInfo.TempTableType == model.TempTableLocal`.
    pub is_local_temporary: bool,
    /// Go `TableInfo.TableCacheStatusType != model.TableCacheStatusDisable`.
    pub is_cached: bool,
    /// Whether Go `TableInfo.Affinity` is non-nil.
    pub has_affinity: bool,
    /// Offsets into [`Self::columns`] of Go `HandleCols`' columns. Empty when
    /// the table has no usable handle, in which case `buildDataSource` appends
    /// the `_tidb_rowid` extra handle.
    pub handle_col_offsets: Vec<usize>,
    /// Offsets into [`Self::columns`] of Go `CommonHandleCols`.
    pub common_handle_col_offsets: Vec<usize>,
    /// Go `CommonHandleLens`, index-parallel to
    /// [`Self::common_handle_col_offsets`].
    pub common_handle_lens: Vec<i64>,
    /// Go `TableInfo.TiFlashReplica`. `READ_FROM_STORAGE` is resolved by the
    /// planner from it and the active query-block hints; it is not
    /// pre-resolved by the catalog.
    pub tiflash_replica: Option<tidb_model::TiFlashReplicaInfo>,
}

impl SourceTable {
    /// Whether the table has an available TiFlash replica, the one Go
    /// `getPossibleAccessPaths` generates a TiFlash path for.
    #[must_use]
    pub fn has_tiflash_replica(&self) -> bool {
        self.tiflash_replica
            .as_ref()
            .is_some_and(|replica| replica.available)
    }

    /// Attaches the metadata objects that Go's preprocessor records in
    /// `resolve.Context` for this infoschema table: the table's own
    /// `TableInfo` (Go `Table.Meta()`) when it has one, else one built from
    /// this source's columns and indexes.
    pub fn attach_resolve_metadata(
        &mut self,
        db_info: GoShared<DBInfo>,
        table_info: Option<GoShared<TableInfo>>,
    ) {
        self.db_info = Some(db_info);
        if let Some(table_info) = table_info {
            self.table_info = Some(table_info);
            return;
        }
        let columns = self
            .columns
            .iter()
            .map(|column| Some(column.info.clone()))
            .collect();
        let indices = self.indexes.iter().cloned().map(Some).collect();
        self.table_info = Some(GoShared::new(TableInfo {
            id: self.table_id,
            name: tidb_ast::CiString::new(self.table_name.clone()),
            columns: GoSharedPointerSlice::from_handles(columns),
            indices: GoSharedPointerSlice::from_handles(indices),
            state: SchemaState::PUBLIC,
            pk_is_handle: self.pk_is_handle,
            is_common_handle: self.is_common_handle,
            tiflash_replica: self.tiflash_replica.clone().map(GoShared::new),
            ..TableInfo::default()
        }));
    }

    /// Go `HandleCols.IsInt()`: an int handle is a single-column,
    /// non-common-handle handle.
    #[must_use]
    pub fn handle_is_int(&self) -> bool {
        !self.is_common_handle && self.handle_col_offsets.len() == 1
    }

    /// The column at `offset`, or `None` when the implementor handed back an
    /// offset outside its own column list.
    #[must_use]
    pub fn column_at(&self, offset: usize) -> Option<&SourceColumn> {
        self.columns.get(offset)
    }

    /// Go `TableInfo.FindPublicColumnByName`, case-insensitively.
    #[must_use]
    pub fn find_column(&self, name: &str) -> Option<&SourceColumn> {
        self.columns
            .iter()
            .find(|column| tidb_mysql::lowercase_equal(column.read().name.original(), name))
    }
}

/// Go `infoschema.InfoSchema`, narrowed to what the SELECT build path asks.
///
/// See this module's header for why the interface is this small.
pub trait TableSource {
    /// Go `SessionVars.CurrentDB`, used to qualify an unqualified table name.
    fn current_database(&self) -> &str;

    /// Go `tableHasDirtyContent(ctx, tableInfo)` (`planner/core/util.go:156`):
    /// whether the open transaction has staged a write under any of the given
    /// physical table ids (a partitioned table passes every partition's id,
    /// as Go checks each definition). A dirty table is read through a
    /// `LogicalUnionScan`.
    fn table_has_dirty_content(&self, _table_ids: &[i64]) -> bool {
        false
    }

    /// Go `is.TableByName(ctx, dbName, tblName)`, with the partition selection
    /// already made — see [`SourceTable`].
    ///
    /// Both names are matched case-insensitively, as Go does through `CIStr.L`.
    fn find_table(&self, db_name: &str, table_name: &str) -> Option<&SourceTable>;

    /// Go `is.SchemaByName(dbName) != nil`, so an unknown DATABASE reports
    /// `ErrBadDB` rather than `ErrNoSuchTable`.
    fn database_exists(&self, db_name: &str) -> bool;

    /// Go `SessionVars.HypoIndexes[dbName.L][tblInfo.Name.L]`, read by
    /// `getPossibleAccessPaths` only under `StmtCtx.InExplainStmt`; a source
    /// outside an `EXPLAIN` answers none. Each comes back as the index Go
    /// built for it (`BuildIndexInfo`, no id allocated).
    fn hypo_indexes(&self, _db_name: &str, _table_name: &str) -> Vec<SourceIndex> {
        Vec::new()
    }

    /// Go `UsedHypoTiFlashReplicas`: the session declared a hypothetical
    /// TiFlash replica for this table and the statement is an `EXPLAIN`.
    fn uses_hypo_tiflash_replicas(&self, _db_name: &str, _table_name: &str) -> bool {
        false
    }

    /// Go `domain.GetDomain(ctx).InfoSchema()`, snapshotted at the statement
    /// boundary. Test/internal sources without a session have no domain.
    fn latest_index_schema(&self) -> Option<&LatestIndexSchema> {
        None
    }

    /// Go `tableInfo.IsView()` plus `tableInfo.View`
    /// (`logical_plan_builder.go:5047`): the stored definition
    /// `BuildDataSourceFromView` expands.
    ///
    /// The default is `None` — an implementor that has no views at all needs
    /// no view arm, and a `find_table` hit continues to mean a real table.
    fn find_view(&self, _db_name: &str, _view_name: &str) -> Option<&SourceView> {
        None
    }

    /// Go `tableInfo.IsSequence()` for the table `db_name`.`table_name`: the
    /// sequence's stored name (Go `TableInfo.Name`) when it is one, which
    /// `buildDataSource` reads as a one-row `TableDual`. Sequences are not in
    /// [`Self::find_table`], whose hits are tables with rows and columns.
    fn find_sequence(&self, _db_name: &str, _table_name: &str) -> Option<&str> {
        None
    }

    /// [`Self::find_table`] over Go's `temptable.DetachLocalTemporaryTableInfoSchema`:
    /// a session's local temporary table neither resolves nor hides the
    /// permanent table it shadows. Tables inside a view resolve this way.
    fn find_table_ignoring_local_temporary(
        &self,
        db_name: &str,
        table_name: &str,
    ) -> Option<&SourceTable> {
        self.find_table(db_name, table_name)
            .filter(|table| !table.is_local_temporary)
    }

    /// Go `privilege.GetPrivilegeManager(b.ctx)`, which the planner asks for a
    /// `SQL SECURITY DEFINER` view's definer. `None` is Go's nil manager: no
    /// privilege is enforced, so nothing is verified.
    fn privilege_verifier(&self) -> Option<&dyn PrivilegeVerifier> {
        None
    }

    /// Go `tryLockMDLAndUpdateSchemaIfNecessary` as `buildDataSource` calls
    /// it: the table `table_id` joins the statement's metadata-lock map
    /// (`GetRelatedTableForMDL`). The default records nothing, a source with
    /// no transaction to lock for.
    fn record_related_table_for_mdl(&self, _table_id: i64) {}

    /// [`Self::find_view`] over the same detached schema.
    fn find_view_ignoring_local_temporary(
        &self,
        db_name: &str,
        view_name: &str,
    ) -> Option<&SourceView> {
        self.find_view(db_name, view_name)
    }
}

/// Go `model.ViewInfo` plus the `TableInfo` fields
/// `BuildDataSourceFromView`/`buildProjUponView` read off the view itself.
///
/// The definition arrives as SQL TEXT, exactly as Go stores it: the view
/// expander re-parses it so that the body resolves in the view's own schema
/// rather than the reader's.
#[derive(Clone, Debug, Default)]
pub struct SourceView {
    /// Go `dbName.O`.
    pub db_name: String,
    /// Go `TableInfo.Name.O`.
    pub view_name: String,
    /// Go `TableInfo.View.SelectStmt`.
    pub select_sql: String,
    /// Go `TableInfo.ID`; zero for a view with no stored id.
    pub table_id: i64,
    /// Go `TableInfo.View.Cols`: the ORIGIN column names of the underlying
    /// `SELECT` as they stood at `CREATE VIEW`. Empty when the view stores
    /// none, which is the modern shape.
    pub view_cols: Vec<String>,
    /// Go `TableInfo.Cols()`: the view's own columns, whose names the
    /// projection presents.
    pub columns: Vec<SourceColumn>,
    /// Go `TableInfo.View.Definer`: the account a `DEFINER` view is verified as.
    pub definer_user: String,
    /// The definer's host.
    pub definer_host: String,
    /// Go `TableInfo.View.Security == ast.SecurityDefiner`.
    pub security_definer: bool,
}

#[cfg(test)]
mod tests {
    use super::{SourceColumn, SourceTable};
    use tidb_datatype::{FieldType, FieldTypeCode};

    fn column(offset: usize, name: &str) -> SourceColumn {
        tidb_table::column::Column {
            info: tidb_model::GoShared::new({
                let info = tidb_model::ColumnInfo {
                    id: offset as i64 + 1,
                    name: tidb_ast::CiString::new(name.to_owned()),
                    offset: (offset) as i64,
                    field_type: FieldType::new(FieldTypeCode::LongLong),
                    state: tidb_model::SchemaState::PUBLIC,
                    ..tidb_model::ColumnInfo::default()
                };
                info
            }),
            generated_expr: None,
        }
    }

    fn table() -> SourceTable {
        SourceTable {
            table_id: 7,
            table_name: "t".to_owned(),
            db_name: "test".to_owned(),
            physical_table_id: 7,
            columns: vec![column(0, "a"), column(1, "b")],
            pk_is_handle: true,
            handle_col_offsets: vec![0],
            ..SourceTable::default()
        }
    }

    #[test]
    fn test_handle_is_int_matches_go_is_int() {
        let mut tbl = table();
        assert!(tbl.handle_is_int());

        // A common handle is never an int handle, however many columns it has.
        tbl.is_common_handle = true;
        assert!(!tbl.handle_is_int());

        // Neither is a multi-column handle, nor a handle-less table.
        tbl.is_common_handle = false;
        tbl.handle_col_offsets = vec![0, 1];
        assert!(!tbl.handle_is_int());
        tbl.handle_col_offsets.clear();
        assert!(!tbl.handle_is_int());
    }

    #[test]
    fn test_find_column_is_case_insensitive() {
        let tbl = table();
        assert_eq!(tbl.find_column("A").map(|c| c.read().offset), Some(0));
        assert_eq!(tbl.find_column("b").map(|c| c.read().offset), Some(1));
        assert!(tbl.find_column("c").is_none());
        assert_eq!(tbl.column_at(1).map(|c| c.read().name.original().to_owned()).as_deref(), Some("b"));
        assert!(tbl.column_at(9).is_none());
    }
}

/// Go `privilege.Manager.RequestVerificationWithUser`, which the planner asks
/// while it expands a `SQL SECURITY DEFINER` view (`pm.RequestVerificationWithUser`).
pub trait PrivilegeVerifier {
    /// Whether `user`@`host` -- matched against the stored accounts by host
    /// pattern -- holds `privilege` on `database`.`table`.
    fn verify_with_user(
        &self,
        user: &str,
        host: &str,
        database: &str,
        table: &str,
        column: &str,
        privilege: PrivilegeType,
    ) -> bool;

    /// Go `privilege.Manager.RequestVerification(activeRoles, ...)`: whether
    /// the session's own account, with its active roles, holds `privilege`
    /// on `database`.`table`.
    fn request_verification(
        &self,
        database: &str,
        table: &str,
        column: &str,
        privilege: PrivilegeType,
    ) -> bool;
}

/// The latest domain infoschema fields read by Go `GetLatestIndexInfo`.
#[derive(Clone, Debug, Default)]
pub struct LatestIndexSchema {
    /// Go `InfoSchema.SchemaMetaVersion()`.
    pub schema_meta_version: u64,
    /// Go `InfoSchema.TableByID` followed by `TableInfo.Indices`.
    pub table_indexes: BTreeMap<i64, Vec<SourceIndex>>,
}

/// Go `sctx.GetLatestInfoSchema()` as `metabuild.WithInfoSchema` keeps it
/// (`pkg/ddl/metabuild.go`): the same latest domain infoschema.
impl tidb_infoschema_context::MetaOnlyInfoSchema for LatestIndexSchema {
    fn schema_meta_version(&self) -> i64 {
        // Go's `SchemaMetaVersion` is an `int64`; this field holds the same
        // non-negative version.
        self.schema_meta_version as i64
    }
}
