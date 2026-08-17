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
//! # Why a trait and not the model types
//!
//! `tidb-planner` has no `tidb-model` / `tidb-meta` dependency and MUST NOT
//! gain one: the catalogue drags in the DDL job model, the schema-version
//! machinery and the storage handles, none of which any planner body reads.
//! [`crate::logical::data_source::DataSource`] already recorded the narrowing
//! ("what the ported bodies actually READ off them is kept as explicit
//! fields"); this module is the INPUT side of that same narrowing, so the two
//! agree field for field.
//!
//! [`TableSource`] is therefore the whole of Go's
//! `infoschema.InfoSchema` + `model.TableInfo` as far as this crate is
//! concerned. A downstream crate that owns the real catalogue implements it.
//!
//! # The method set, and why exactly these
//!
//! [`SourceTable`] carries precisely the fields
//! [`DataSource`](crate::logical::data_source::DataSource) declares, so
//! [`crate::plan_builder::PlanBuilder::build_data_source`] is a field-for-field
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
//! * [`SourceTable::indexes`] — [`crate::access_path`] enumerates one path per
//!   usable index. `DataSource::possible_access_paths` holds the RESULT;
//!   [`SourceIndex`] is the input that result is derived from.
//!
//! # Narrowings, by exact Go symbol
//!
//! * `table.Table` (`pkg/table/table.go`). The runtime handle that reads and
//!   writes rows. Nothing in the LOGICAL build path calls it; the physical
//!   side reaches storage through `tidb-executor`. Not modelled.
//! * `statistics.Table` / `statistics.HistColl` (`TableStats`). Statistics
//!   arrive at [`crate::cardinality`] already derived; the builder only tags
//!   the operator, so no statistics handle crosses this seam.
//! * `model.TableInfo.State` / `ColumnInfo.State` /
//!   `IndexInfo.State`. Go filters on `== model.StatePublic`. Reduced to the
//!   booleans [`SourceColumn::is_public`] / [`SourceIndex::is_public`]: the
//!   builder only ever asks the yes/no question, and the DDL state machine
//!   that produces the other states is not in this crate.
//! * `infoschema.InfoSchema.SchemaMetaVersion` / `AllSchemas` / the
//!   `TableByID` family. `buildDataSource` reaches the catalogue exactly once,
//!   by (db, table) name; the rest of the interface serves DDL and SHOW.
//! * Privilege checks (`visitInfo`). See
//!   [`crate::plan_builder`]'s header — dropped tree-wide, not narrowed here.

use tidb_datatype::{FieldType, FieldTypeCode};

/// What the ported builder bodies read off a `*model.ColumnInfo`.
///
/// [`Default`] is hand-written rather than derived because
/// [`FieldType`] has no `Default` — Go's zero `types.FieldType` is a
/// `TypeUnspecified`, which [`FieldTypeCode::Unspecified`] spells explicitly.
///
/// The first three fields are exactly
/// [`DataSourceColumn`](crate::logical::data_source::DataSourceColumn); the
/// rest are the builder-only inputs named in this module's header.
#[derive(Clone, Debug)]
pub struct SourceColumn {
    /// Go `ColumnInfo.ID`.
    pub id: i64,
    /// Go `ColumnInfo.Name.O`.
    pub name: String,
    /// Go `mysql.HasPriKeyFlag(col.GetFlag())`.
    pub is_primary_key: bool,
    /// Go `ColumnInfo.Offset`: the position in the table's column list, which
    /// is the schema position `buildDataSource` gives the built column.
    pub offset: usize,
    /// Go `ColumnInfo.FieldType`, which becomes the built
    /// [`Column`](tidb_expr::column::Column)'s `RetType`.
    pub ret_type: FieldType,
    /// Go `ColumnInfo.State == model.StatePublic`. A non-public column is
    /// still SCANNED but is `NotExplicitUsable` in the output names.
    pub is_public: bool,
    /// Go `ColumnInfo.Hidden`: never expanded by `*`, never user-referencable.
    pub is_hidden: bool,
    /// Go `col.IsGenerated() && !col.GeneratedStored`: a virtual generated
    /// column, which the storage layer cannot return and the planner must
    /// compute. Read by `FlagGcSubstitute`'s eligibility.
    pub is_virtual_generated: bool,
}

impl Default for SourceColumn {
    fn default() -> Self {
        Self {
            id: 0,
            name: String::new(),
            is_primary_key: false,
            offset: 0,
            ret_type: FieldType::new(FieldTypeCode::Unspecified),
            is_public: true,
            is_hidden: false,
            is_virtual_generated: false,
        }
    }
}

/// Go `model.IndexColumn` (`pkg/meta/model/index.go`).
#[derive(Clone, Debug, Default)]
pub struct SourceIndexColumn {
    /// Go `IndexColumn.Name.O`.
    pub name: String,
    /// Go `IndexColumn.Offset`: the position in the TABLE's column list.
    pub offset: usize,
    /// Go `IndexColumn.Length`, `types.UnspecifiedLength` for a full column.
    pub length: i64,
}

/// What the ported builder and [`crate::access_path`] bodies read off a
/// `*model.IndexInfo`.
#[derive(Clone, Debug, Default)]
pub struct SourceIndex {
    /// Go `IndexInfo.ID`, the deterministic tie-breaker in path selection.
    pub id: i64,
    /// Go `IndexInfo.Name.O`.
    pub name: String,
    /// Go `IndexInfo.Columns`, in index order.
    pub columns: Vec<SourceIndexColumn>,
    /// Go `IndexInfo.Unique`.
    pub unique: bool,
    /// Go `IndexInfo.Primary`.
    pub primary: bool,
    /// Go `IndexInfo.State == model.StatePublic`.
    pub is_public: bool,
    /// Go `IndexInfo.Invisible` inverted: `getPossibleAccessPaths` skips an
    /// invisible index unless the session opted back in.
    pub is_visible: bool,
    /// Go `IndexInfo.VectorInfo != nil || InvertedInfo != nil || FullTextInfo
    /// != nil`: a columnar index, which only the TiFlash paths may use.
    pub is_columnar: bool,
    /// Go `IndexInfo.MVIndex`.
    pub is_multi_valued: bool,
}

/// What the ported builder bodies read off a `*model.TableInfo` plus the
/// partition selection `buildDataSource` has already made.
///
/// One value per PHYSICAL table read: a partitioned table hands back one of
/// these per targeted partition, with `physical_table_id` and
/// `partition_def_idx` already resolved. That mirrors what
/// [`DataSource`](crate::logical::data_source::DataSource) stores and keeps
/// partition pruning on the implementor's side of the seam, where the
/// partition expression lives.
#[derive(Clone, Debug, Default)]
pub struct SourceTable {
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
    /// Go `TableInfo.GetPartitionInfo().Definitions[i].Name.O`.
    pub partition_definition_names: Vec<String>,
    /// Go `TableInfo.Columns`, in offset order.
    pub columns: Vec<SourceColumn>,
    /// Go `TableInfo.Indices`, in declaration order.
    pub indexes: Vec<SourceIndex>,
    /// Go `TableInfo.PKIsHandle`.
    pub pk_is_handle: bool,
    /// Go `TableInfo.IsCommonHandle`.
    pub is_common_handle: bool,
    /// Offsets into [`Self::columns`] of Go `HandleCols`' columns. Empty when
    /// the table has no usable handle, in which case `buildDataSource` appends
    /// the `_tidb_rowid` extra handle.
    pub handle_col_offsets: Vec<usize>,
    /// Offsets into [`Self::columns`] of Go `CommonHandleCols`.
    pub common_handle_col_offsets: Vec<usize>,
    /// Go `CommonHandleLens`, index-parallel to
    /// [`Self::common_handle_col_offsets`].
    pub common_handle_lens: Vec<i64>,
    /// Go `PreferStoreType`: the already-resolved `READ_FROM_STORAGE` hint.
    pub prefer_store_type: i32,
}

impl SourceTable {
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
            .find(|column| column.name.eq_ignore_ascii_case(name))
    }
}

/// Go `infoschema.InfoSchema`, narrowed to what the SELECT build path asks.
///
/// See this module's header for why the interface is this small.
pub trait TableSource {
    /// Go `SessionVars.CurrentDB`, used to qualify an unqualified table name.
    fn current_database(&self) -> &str;

    /// Go `is.TableByName(ctx, dbName, tblName)`, with the partition selection
    /// already made — see [`SourceTable`].
    ///
    /// Both names are matched case-insensitively, as Go does through `CIStr.L`.
    fn find_table(&self, db_name: &str, table_name: &str) -> Option<&SourceTable>;

    /// Go `is.SchemaByName(dbName) != nil`, so an unknown DATABASE reports
    /// `ErrBadDB` rather than `ErrNoSuchTable`.
    fn database_exists(&self, db_name: &str) -> bool;

    /// Go `tableInfo.IsView()` plus `tableInfo.View`
    /// (`logical_plan_builder.go:5047`): the stored definition
    /// `BuildDataSourceFromView` expands.
    ///
    /// The default is `None` — an implementor that has no views at all needs
    /// no view arm, and a `find_table` hit continues to mean a real table.
    fn find_view(&self, _db_name: &str, _view_name: &str) -> Option<&SourceView> {
        None
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
    /// Go `TableInfo.View.Cols`: the ORIGIN column names of the underlying
    /// `SELECT` as they stood at `CREATE VIEW`. Empty when the view stores
    /// none, which is the modern shape.
    pub view_cols: Vec<String>,
    /// Go `TableInfo.Cols()`: the view's own columns, whose names the
    /// projection presents.
    pub columns: Vec<SourceColumn>,
}

#[cfg(test)]
mod tests {
    use super::{SourceColumn, SourceTable};
    use tidb_datatype::{FieldType, FieldTypeCode};

    fn column(offset: usize, name: &str) -> SourceColumn {
        SourceColumn {
            id: offset as i64 + 1,
            name: name.to_owned(),
            offset,
            ret_type: FieldType::new(FieldTypeCode::LongLong),
            is_public: true,
            ..SourceColumn::default()
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
        assert_eq!(tbl.find_column("A").map(|c| c.offset), Some(0));
        assert_eq!(tbl.find_column("b").map(|c| c.offset), Some(1));
        assert!(tbl.find_column("c").is_none());
        assert_eq!(tbl.column_at(1).map(|c| c.name.as_str()), Some("b"));
        assert!(tbl.column_at(9).is_none());
    }
}
