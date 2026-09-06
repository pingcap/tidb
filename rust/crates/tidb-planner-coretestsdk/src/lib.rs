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

//! Complete Rust mapping of Go `pkg/planner/util/coretestsdk`.
//!
//! This crate is planner test support. It supplies the pinned package's exact
//! metadata fixtures through the planner's existing [`TableSource`] seam; it
//! does not add a production optimizer path.

use tidb_ast::{CiString, PartitionType, ViewSecurity};
use tidb_datatype::{FieldType, FieldTypeCode, FieldTypeFlags, UNSPECIFIED_LENGTH};
use tidb_model::go_runtime::{GoShared, GoSharedPointerSlice, GoSharedSlice};
use tidb_model::serde_helpers::GoValueSlice;
use tidb_model::table::ViewInfo;
use tidb_model::{
    ColumnInfo, DBInfo, IndexColumn, IndexInfo, PartitionDefinition, PartitionInfo, SchemaState,
    TableInfo,
};
use tidb_parser::auth::UserIdentity;
use tidb_planner::plan_builder::catalog::{
    SourceColumn, SourceIndex, SourceIndexColumn, SourceTable, SourceView, TableSource,
};

const DEFAULT_DIV_PRECISION_INCREMENT: u32 = 4;

fn long_type() -> FieldType {
    FieldType::parser(FieldTypeCode::Long)
}

fn string_type() -> FieldType {
    FieldType::new(FieldTypeCode::Varchar)
}

fn date_type() -> FieldType {
    FieldType::parser(FieldTypeCode::Date)
}

fn column(id: i64, offset: i64, name: &str, field_type: FieldType, flags: u32) -> ColumnInfo {
    let mut column = ColumnInfo {
        id,
        offset,
        name: CiString::new(name),
        field_type,
        state: SchemaState::PUBLIC,
        ..ColumnInfo::default()
    };
    column.set_flag(u64::from(flags));
    column
}

fn index_column(name: &str, offset: i64, length: i64) -> IndexColumn {
    IndexColumn {
        name: CiString::new(name),
        offset,
        length,
        ..IndexColumn::default()
    }
}

fn index(
    id: i64,
    name: &str,
    columns: impl IntoIterator<Item = IndexColumn>,
    state: SchemaState,
    unique: bool,
    global: bool,
) -> IndexInfo {
    IndexInfo {
        id,
        name: CiString::new(name),
        columns: GoSharedPointerSlice::from_handles(
            columns
                .into_iter()
                .map(|column| Some(GoShared::new(column)))
                .collect(),
        ),
        state,
        unique,
        global,
        ..IndexInfo::default()
    }
}

fn pointer_slice<T>(values: impl IntoIterator<Item = T>) -> GoSharedPointerSlice<T> {
    GoSharedPointerSlice::from_handles(
        values
            .into_iter()
            .map(|value| Some(GoShared::new(value)))
            .collect(),
    )
}

/// Go `MockSignedTable`.
pub fn mock_signed_table() -> TableInfo {
    let columns = vec![
        column(
            1,
            0,
            "a",
            long_type(),
            FieldTypeFlags::PRI_KEY | FieldTypeFlags::NOT_NULL,
        ),
        column(2, 1, "b", long_type(), FieldTypeFlags::NOT_NULL),
        column(3, 2, "c", long_type(), FieldTypeFlags::NOT_NULL),
        column(4, 3, "d", long_type(), FieldTypeFlags::NOT_NULL),
        column(5, 4, "e", long_type(), 0),
        column(6, 5, "c_str", string_type(), 0),
        column(7, 6, "d_str", string_type(), 0),
        column(8, 7, "e_str", string_type(), 0),
        column(9, 8, "f", long_type(), FieldTypeFlags::NOT_NULL),
        column(10, 9, "g", long_type(), FieldTypeFlags::NOT_NULL),
        column(11, 10, "h", long_type(), FieldTypeFlags::NO_DEFAULT_VALUE),
        column(12, 11, "i_date", date_type(), 0),
    ];
    let indexes = vec![
        index(
            1,
            "c_d_e",
            [
                index_column("c", 2, UNSPECIFIED_LENGTH),
                index_column("d", 3, UNSPECIFIED_LENGTH),
                index_column("e", 4, UNSPECIFIED_LENGTH),
            ],
            SchemaState::PUBLIC,
            true,
            false,
        ),
        index(
            2,
            "x",
            [index_column("e", 4, UNSPECIFIED_LENGTH)],
            SchemaState::WRITE_ONLY,
            true,
            false,
        ),
        index(
            3,
            "f",
            [index_column("f", 8, UNSPECIFIED_LENGTH)],
            SchemaState::PUBLIC,
            true,
            false,
        ),
        index(
            4,
            "g",
            [index_column("g", 9, UNSPECIFIED_LENGTH)],
            SchemaState::PUBLIC,
            false,
            false,
        ),
        index(
            5,
            "f_g",
            [
                index_column("f", 8, UNSPECIFIED_LENGTH),
                index_column("g", 9, UNSPECIFIED_LENGTH),
            ],
            SchemaState::PUBLIC,
            true,
            false,
        ),
        index(
            6,
            "c_d_e_str",
            [
                index_column("c_str", 5, UNSPECIFIED_LENGTH),
                index_column("d_str", 6, UNSPECIFIED_LENGTH),
                index_column("e_str", 7, UNSPECIFIED_LENGTH),
            ],
            SchemaState::PUBLIC,
            false,
            false,
        ),
        index(
            7,
            "e_d_c_str_prefix",
            [
                index_column("e_str", 7, UNSPECIFIED_LENGTH),
                index_column("d_str", 6, UNSPECIFIED_LENGTH),
                index_column("c_str", 5, 10),
            ],
            SchemaState::PUBLIC,
            false,
            false,
        ),
    ];
    TableInfo {
        id: 1,
        name: CiString::new("t"),
        columns: pointer_slice(columns),
        indices: pointer_slice(indexes),
        state: SchemaState::PUBLIC,
        pk_is_handle: true,
        ..TableInfo::default()
    }
}

/// Go `MockUnsignedTable`.
pub fn mock_unsigned_table() -> TableInfo {
    TableInfo {
        id: 2,
        name: CiString::new("t2"),
        columns: pointer_slice([
            column(
                1,
                0,
                "a",
                long_type(),
                FieldTypeFlags::PRI_KEY | FieldTypeFlags::NOT_NULL | FieldTypeFlags::UNSIGNED,
            ),
            column(2, 1, "b", long_type(), FieldTypeFlags::NOT_NULL),
            column(3, 2, "c", long_type(), FieldTypeFlags::UNSIGNED),
        ]),
        indices: pointer_slice([
            index(
                0,
                "b",
                [index_column("b", 1, UNSPECIFIED_LENGTH)],
                SchemaState::PUBLIC,
                true,
                false,
            ),
            index(
                0,
                "b_c",
                [
                    index_column("b", 1, UNSPECIFIED_LENGTH),
                    index_column("c", 2, UNSPECIFIED_LENGTH),
                ],
                SchemaState::PUBLIC,
                false,
                false,
            ),
        ]),
        state: SchemaState::PUBLIC,
        pk_is_handle: true,
        ..TableInfo::default()
    }
}

/// Go `MockNoPKTable`.
pub fn mock_no_pk_table() -> TableInfo {
    TableInfo {
        id: 3,
        name: CiString::new("t3"),
        columns: pointer_slice([
            column(2, 1, "a", long_type(), FieldTypeFlags::NOT_NULL),
            column(3, 2, "b", long_type(), FieldTypeFlags::UNSIGNED),
        ]),
        state: SchemaState::PUBLIC,
        pk_is_handle: true,
        ..TableInfo::default()
    }
}

/// Go `MockView`.
pub fn mock_view() -> TableInfo {
    let columns = [
        ColumnInfo {
            id: 1,
            offset: 0,
            name: CiString::new("b"),
            state: SchemaState::PUBLIC,
            ..ColumnInfo::default()
        },
        ColumnInfo {
            id: 2,
            offset: 1,
            name: CiString::new("c"),
            state: SchemaState::PUBLIC,
            ..ColumnInfo::default()
        },
        ColumnInfo {
            id: 3,
            offset: 2,
            name: CiString::new("d"),
            state: SchemaState::PUBLIC,
            ..ColumnInfo::default()
        },
    ];
    TableInfo {
        id: 4,
        name: CiString::new("v"),
        columns: pointer_slice(columns),
        view: Some(GoShared::new(ViewInfo {
            select_stmt: "select b,c,d from t".to_owned(),
            security: ViewSecurity::DEFINER,
            definer: Some(Box::new(UserIdentity {
                username: "root".to_owned(),
                hostname: String::new(),
                ..UserIdentity::default()
            })),
            cols: GoValueSlice::from(vec![
                CiString::new("b"),
                CiString::new("c"),
                CiString::new("d"),
            ]),
            ..ViewInfo::default()
        })),
        state: SchemaState::PUBLIC,
        ..TableInfo::default()
    }
}

fn partition_column(table: &TableInfo) -> ColumnInfo {
    let last = table
        .columns
        .iter_deref()
        .last()
        .expect("the signed fixture has columns");
    let last = last.read();
    column(last.id + 1, last.offset + 1, "ptn", long_type(), 0)
}

fn partitioned_table(
    id: i64,
    name: &str,
    partition_type: PartitionType,
    definitions: Vec<PartitionDefinition>,
) -> TableInfo {
    let mut table = mock_signed_table();
    table.id = id;
    table.name = CiString::new(name);
    table.columns.push_go(partition_column(&table));
    table.partition = Some(GoShared::new(PartitionInfo {
        partition_type,
        expr: "ptn".to_owned(),
        enable: true,
        num: if partition_type == PartitionType::HASH {
            2
        } else {
            0
        },
        definitions: GoSharedSlice::from_vec(definitions),
        ..PartitionInfo::default()
    }));
    table
}

fn definition(id: i64, name: &str) -> PartitionDefinition {
    PartitionDefinition {
        id,
        name: CiString::new(name),
        ..PartitionDefinition::default()
    }
}

/// Go `MockPartitionInfoSchema`.
pub fn mock_partition_info_schema(definitions: Vec<PartitionDefinition>) -> MockInfoSchema {
    MockInfoSchema::new(vec![partitioned_table(
        1,
        "t",
        PartitionType::RANGE,
        definitions,
    )])
}

/// Go `MockRangePartitionTable`.
pub fn mock_range_partition_table() -> TableInfo {
    let mut p1 = definition(41, "p1");
    p1.less_than = GoSharedSlice::from_vec(vec!["16".to_owned()]);
    let mut p2 = definition(42, "p2");
    p2.less_than = GoSharedSlice::from_vec(vec!["32".to_owned()]);
    partitioned_table(5, "pt1", PartitionType::RANGE, vec![p1, p2])
}

/// Go `MockHashPartitionTable`.
pub fn mock_hash_partition_table() -> TableInfo {
    partitioned_table(
        6,
        "pt2",
        PartitionType::HASH,
        vec![definition(51, "p1"), definition(52, "p2")],
    )
}

/// Go `MockListPartitionTable`.
pub fn mock_list_partition_table() -> TableInfo {
    let mut p1 = definition(61, "p1");
    p1.in_values = GoSharedSlice::from_vec(vec![GoSharedSlice::from_vec(vec!["1".to_owned()])]);
    let mut p2 = definition(62, "p2");
    p2.in_values = GoSharedSlice::from_vec(vec![GoSharedSlice::from_vec(vec!["2".to_owned()])]);
    let table = partitioned_table(7, "pt3", PartitionType::LIST, vec![p1, p2]);
    table.partition.as_ref().unwrap().write().num = 2;
    table
}

/// Go `MockGlobalIndexHashPartitionTable`.
pub fn mock_global_index_hash_partition_table() -> TableInfo {
    let mut table = partitioned_table(
        1,
        "pt2_global_index",
        PartitionType::HASH,
        vec![definition(51, "p1"), definition(52, "p2")],
    );
    for index in [
        index(
            0,
            "b",
            [index_column("b", 1, UNSPECIFIED_LENGTH)],
            SchemaState::PUBLIC,
            false,
            false,
        ),
        index(
            0,
            "b_global",
            [index_column("b", 1, UNSPECIFIED_LENGTH)],
            SchemaState::PUBLIC,
            true,
            true,
        ),
        index(
            0,
            "b_c",
            [
                index_column("b", 1, UNSPECIFIED_LENGTH),
                index_column("c", 2, UNSPECIFIED_LENGTH),
            ],
            SchemaState::PUBLIC,
            false,
            false,
        ),
        index(
            0,
            "b_c_global",
            [
                index_column("b", 1, UNSPECIFIED_LENGTH),
                index_column("c", 2, UNSPECIFIED_LENGTH),
            ],
            SchemaState::PUBLIC,
            true,
            true,
        ),
    ] {
        table.indices.push_go(index);
    }
    table
}

/// Go `MockStateNoneColumnTable`.
pub fn mock_state_none_column_table() -> TableInfo {
    let mut hidden_state = column(3, 2, "c", long_type(), FieldTypeFlags::UNSIGNED);
    hidden_state.state = SchemaState::NONE;
    TableInfo {
        id: 8,
        name: CiString::new("T_StateNoneColumn"),
        columns: pointer_slice([
            column(
                1,
                0,
                "a",
                long_type(),
                FieldTypeFlags::PRI_KEY | FieldTypeFlags::NOT_NULL | FieldTypeFlags::UNSIGNED,
            ),
            column(2, 1, "b", long_type(), FieldTypeFlags::NOT_NULL),
            hidden_state,
        ]),
        indices: pointer_slice([index(
            0,
            "b",
            [index_column("b", 1, UNSPECIFIED_LENGTH)],
            SchemaState::PUBLIC,
            true,
            false,
        )]),
        state: SchemaState::PUBLIC,
        pk_is_handle: true,
        ..TableInfo::default()
    }
}

/// Go `GetFieldValue`, including its deliberate `idx > 0` and `end > 0`
/// boundaries.
pub fn get_field_value(prefix: &str, row: &str) -> String {
    if let Some(index) = row.find(prefix).filter(|index| *index > 0) {
        let start = index + prefix.len();
        if let Some(end) = row[start..].find(' ').filter(|end| *end > 0) {
            return row[start..start + end].trim_matches(',').to_owned();
        }
    }
    String::new()
}

fn source_table(table: GoShared<TableInfo>) -> SourceTable {
    let info = table.read();
    let columns = info
        .columns
        .iter_deref()
        .map(|column| {
            let column = column.read();
            SourceColumn {
                id: column.id,
                name: column.name.original().to_owned(),
                is_primary_key: column.get_flag() & u64::from(FieldTypeFlags::PRI_KEY) != 0,
                offset: usize::try_from(column.offset)
                    .expect("fixture column offsets are nonnegative"),
                ret_type: column.field_type.clone(),
                is_public: column.state == SchemaState::PUBLIC,
                is_hidden: column.hidden,
                ..SourceColumn::default()
            }
        })
        .collect();
    let indexes = info
        .indices
        .iter_deref()
        .map(|index| {
            let index = index.read();
            SourceIndex {
                id: index.id,
                name: index.name.original().to_owned(),
                columns: index
                    .columns
                    .iter_deref()
                    .map(|column| {
                        let column = column.read();
                        SourceIndexColumn {
                            name: column.name.original().to_owned(),
                            offset: usize::try_from(column.offset)
                                .expect("fixture index offsets are nonnegative"),
                            length: column.length,
                        }
                    })
                    .collect(),
                unique: index.unique,
                primary: index.primary,
                is_public: index.state == SchemaState::PUBLIC,
                is_visible: !index.invisible,
                is_columnar: index.vector_info.is_some()
                    || index.inverted_info.is_some()
                    || index.full_text_info.is_some(),
                is_multi_valued: index.mv_index,
                global: index.global,
                condition_expr_string: index.condition_expr_string.clone(),
                affect_column_offsets: index
                    .affect_column
                    .iter_deref()
                    .map(|column| usize::try_from(column.read().offset).unwrap())
                    .collect(),
            }
        })
        .collect();
    let (partition_definition_names, partition_definition_ids) = info
        .partition
        .as_ref()
        .map(|partition| {
            let definitions = partition.read().definitions.snapshot();
            (
                definitions
                    .iter()
                    .map(|definition| definition.name.original().to_owned())
                    .collect(),
                definitions.iter().map(|definition| definition.id).collect(),
            )
        })
        .unwrap_or_default();
    let handle_col_offsets = if info.pk_is_handle {
        info.columns
            .iter_deref()
            .position(|column| column.read().get_flag() & u64::from(FieldTypeFlags::PRI_KEY) != 0)
            .into_iter()
            .collect()
    } else {
        Vec::new()
    };
    SourceTable {
        db_info: Some(GoShared::new(DBInfo {
            name: CiString::new("test"),
            state: SchemaState::PUBLIC,
            ..DBInfo::default()
        })),
        table_info: Some(table.clone()),
        table_id: info.id,
        table_name: info.name.original().to_owned(),
        db_name: "test".to_owned(),
        physical_table_id: info.id,
        is_partitioned: info.partition.is_some(),
        partition_definition_names,
        partition_definition_ids,
        columns,
        indexes,
        pk_is_handle: info.pk_is_handle,
        handle_col_offsets,
        ..SourceTable::default()
    }
}

/// Rust infoschema counterpart for the package fixtures.
#[derive(Clone, Debug, Default)]
pub struct MockInfoSchema {
    tables: Vec<GoShared<TableInfo>>,
    sources: Vec<SourceTable>,
    views: Vec<SourceView>,
}

impl MockInfoSchema {
    /// Go `infoschema.MockInfoSchema` over the supplied tables.
    #[must_use]
    pub fn new(tables: Vec<TableInfo>) -> Self {
        let tables: Vec<_> = tables.into_iter().map(GoShared::new).collect();
        let mut sources = Vec::new();
        let mut views = Vec::new();
        for table in &tables {
            let info = table.read();
            if let Some(view) = &info.view {
                let view = view.read();
                views.push(SourceView {
                    db_name: "test".to_owned(),
                    view_name: info.name.original().to_owned(),
                    select_sql: view.select_stmt.clone(),
                    view_cols: view
                        .cols
                        .iter()
                        .map(|name| name.original().to_owned())
                        .collect(),
                    columns: info
                        .columns
                        .iter_deref()
                        .map(|column| {
                            let column = column.read();
                            SourceColumn {
                                id: column.id,
                                name: column.name.original().to_owned(),
                                offset: usize::try_from(column.offset).unwrap(),
                                ret_type: column.field_type.clone(),
                                is_public: column.state == SchemaState::PUBLIC,
                                ..SourceColumn::default()
                            }
                        })
                        .collect(),
                });
            } else {
                drop(info);
                sources.push(source_table(table.clone()));
            }
        }
        Self {
            tables,
            sources,
            views,
        }
    }

    /// All model tables in source order, including views.
    #[must_use]
    pub fn tables(&self) -> &[GoShared<TableInfo>] {
        &self.tables
    }
}

impl TableSource for MockInfoSchema {
    fn current_database(&self) -> &str {
        "test"
    }

    fn find_table(&self, db_name: &str, table_name: &str) -> Option<&SourceTable> {
        db_name.eq_ignore_ascii_case("test").then(|| {
            self.sources
                .iter()
                .find(|table| table.table_name.eq_ignore_ascii_case(table_name))
        })?
    }

    fn database_exists(&self, db_name: &str) -> bool {
        db_name.eq_ignore_ascii_case("test")
    }

    fn find_view(&self, db_name: &str, view_name: &str) -> Option<&SourceView> {
        db_name.eq_ignore_ascii_case("test").then(|| {
            self.views
                .iter()
                .find(|view| view.view_name.eq_ignore_ascii_case(view_name))
        })?
    }
}

/// Rust counterpart of Go's planner-test mock context.
#[derive(Clone, Debug)]
pub struct MockContext {
    info_schema: MockInfoSchema,
    current_database: String,
    div_precision_increment: u32,
}

impl MockContext {
    /// The bound infoschema.
    #[must_use]
    pub const fn info_schema(&self) -> &MockInfoSchema {
        &self.info_schema
    }

    /// Go `SessionVars.CurrentDB`.
    #[must_use]
    pub fn current_database(&self) -> &str {
        &self.current_database
    }

    /// Go `SessionVars.DivPrecisionIncrement`.
    #[must_use]
    pub const fn div_precision_increment(&self) -> u32 {
        self.div_precision_increment
    }
}

impl TableSource for MockContext {
    fn current_database(&self) -> &str {
        &self.current_database
    }

    fn find_table(&self, db_name: &str, table_name: &str) -> Option<&SourceTable> {
        self.info_schema.find_table(db_name, table_name)
    }

    fn database_exists(&self, db_name: &str) -> bool {
        self.info_schema.database_exists(db_name)
    }

    fn find_view(&self, db_name: &str, view_name: &str) -> Option<&SourceView> {
        self.info_schema.find_view(db_name, view_name)
    }
}

/// Go `MockContext`.
pub fn mock_context() -> MockContext {
    MockContext {
        info_schema: MockInfoSchema::default(),
        current_database: "test".to_owned(),
        div_precision_increment: DEFAULT_DIV_PRECISION_INCREMENT,
    }
}

/// Stateless façade for Go's reusable configured parser.
#[derive(Clone, Copy, Debug, Default)]
pub struct PlannerParser;

impl PlannerParser {
    /// Parses with window functions and strict DOUBLE checking enabled.
    pub fn parse(&self, sql: &str) -> Result<tidb_ast::Stmt, tidb_parser::ParseError> {
        tidb_parser::parse(sql)
    }
}

/// Go `PlannerSuite` adapted to Rust ownership.
#[derive(Clone, Debug)]
pub struct PlannerSuite {
    parser: PlannerParser,
    info_schema: MockInfoSchema,
    context: MockContext,
}

impl PlannerSuite {
    /// Go `GetParser`.
    pub const fn get_parser(&self) -> &PlannerParser {
        &self.parser
    }

    /// Go `GetIS`.
    pub const fn get_is(&self) -> &MockInfoSchema {
        &self.info_schema
    }

    /// Go `GetSCtx`.
    pub const fn get_sctx(&self) -> &MockContext {
        &self.context
    }

    /// Go `GetCtx`; both context accessors alias the same Rust value.
    pub const fn get_ctx(&self) -> &MockContext {
        &self.context
    }

    /// Go `Close`. Rust starts no stats goroutine, so dropping/closing is
    /// intentionally harmless.
    pub fn close(&mut self) {}
}

/// Go `CreatePlannerSuite`.
pub fn create_planner_suite(context: MockContext, info_schema: MockInfoSchema) -> PlannerSuite {
    PlannerSuite {
        parser: PlannerParser,
        info_schema,
        context,
    }
}

/// Go `CreatePlannerSuiteElems`.
pub fn create_planner_suite_elems() -> PlannerSuite {
    let mut tables = vec![
        mock_signed_table(),
        mock_unsigned_table(),
        mock_view(),
        mock_no_pk_table(),
        mock_range_partition_table(),
        mock_hash_partition_table(),
        mock_list_partition_table(),
        mock_state_none_column_table(),
        mock_global_index_hash_partition_table(),
    ];
    let mut id = 1_i64;
    for table in &mut tables {
        table.id = id;
        id += 1;
        if let Some(partition) = &table.partition {
            let mut partition = partition.write();
            let mut definitions = partition.definitions.snapshot();
            for definition in &mut definitions {
                definition.id = id;
                id += 1;
            }
            partition.definitions = GoSharedSlice::from_vec(definitions);
        }
    }
    let schema = MockInfoSchema::new(tables);
    let mut context = mock_context();
    context.info_schema = schema.clone();
    create_planner_suite(context, schema)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[deny(unused_must_use)]
    fn source_api_returns_may_be_ignored_like_go() {
        mock_signed_table();
        mock_unsigned_table();
        mock_no_pk_table();
        mock_view();
        mock_partition_info_schema(Vec::new());
        mock_range_partition_table();
        mock_hash_partition_table();
        mock_list_partition_table();
        mock_global_index_hash_partition_table();
        mock_state_none_column_table();
        get_field_value("", "");
        mock_context();

        let suite = create_planner_suite_elems();
        suite.get_parser();
        suite.get_is();
        suite.get_sctx();
        suite.get_ctx();

        create_planner_suite(mock_context(), MockInfoSchema::default());
        create_planner_suite_elems();
    }

    #[test]
    fn get_field_value_preserves_source_boundaries() {
        assert_eq!(get_field_value("x:", "prefix x:value, tail"), "value");
        assert_eq!(get_field_value("x:", "x:value tail"), "");
        assert_eq!(get_field_value("x:", "prefix x: tail"), "");
        assert_eq!(get_field_value("x:", "prefix x:value"), "");
    }

    #[test]
    fn signed_fixture_matches_every_source_shape() {
        let table = mock_signed_table();
        assert_eq!(
            (table.id, table.name.original(), table.pk_is_handle),
            (1, "t", true)
        );
        let columns: Vec<_> = table
            .columns
            .iter_deref()
            .map(|column| {
                let column = column.read();
                (
                    column.id,
                    column.offset,
                    column.name.original().to_owned(),
                    column.field_type.code(),
                    column.get_flag(),
                    column.state,
                )
            })
            .collect();
        assert_eq!(
            columns,
            [
                (
                    1,
                    0,
                    "a".to_owned(),
                    FieldTypeCode::Long,
                    3,
                    SchemaState::PUBLIC
                ),
                (
                    2,
                    1,
                    "b".to_owned(),
                    FieldTypeCode::Long,
                    1,
                    SchemaState::PUBLIC
                ),
                (
                    3,
                    2,
                    "c".to_owned(),
                    FieldTypeCode::Long,
                    1,
                    SchemaState::PUBLIC
                ),
                (
                    4,
                    3,
                    "d".to_owned(),
                    FieldTypeCode::Long,
                    1,
                    SchemaState::PUBLIC
                ),
                (
                    5,
                    4,
                    "e".to_owned(),
                    FieldTypeCode::Long,
                    0,
                    SchemaState::PUBLIC
                ),
                (
                    6,
                    5,
                    "c_str".to_owned(),
                    FieldTypeCode::Varchar,
                    0,
                    SchemaState::PUBLIC
                ),
                (
                    7,
                    6,
                    "d_str".to_owned(),
                    FieldTypeCode::Varchar,
                    0,
                    SchemaState::PUBLIC
                ),
                (
                    8,
                    7,
                    "e_str".to_owned(),
                    FieldTypeCode::Varchar,
                    0,
                    SchemaState::PUBLIC
                ),
                (
                    9,
                    8,
                    "f".to_owned(),
                    FieldTypeCode::Long,
                    1,
                    SchemaState::PUBLIC
                ),
                (
                    10,
                    9,
                    "g".to_owned(),
                    FieldTypeCode::Long,
                    1,
                    SchemaState::PUBLIC
                ),
                (
                    11,
                    10,
                    "h".to_owned(),
                    FieldTypeCode::Long,
                    4096,
                    SchemaState::PUBLIC
                ),
                (
                    12,
                    11,
                    "i_date".to_owned(),
                    FieldTypeCode::Date,
                    0,
                    SchemaState::PUBLIC
                ),
            ]
        );
        let indexes: Vec<_> = table.indices.iter_deref().collect();
        let indexes: Vec<_> = indexes
            .iter()
            .map(|index| {
                let index = index.read();
                (
                    index.id,
                    index.name.original().to_owned(),
                    index
                        .columns
                        .iter_deref()
                        .map(|column| {
                            let column = column.read();
                            (
                                column.name.original().to_owned(),
                                column.offset,
                                column.length,
                            )
                        })
                        .collect::<Vec<_>>(),
                    index.state,
                    index.unique,
                )
            })
            .collect();
        assert_eq!(
            indexes,
            [
                (
                    1,
                    "c_d_e".to_owned(),
                    vec![
                        ("c".to_owned(), 2, -1),
                        ("d".to_owned(), 3, -1),
                        ("e".to_owned(), 4, -1)
                    ],
                    SchemaState::PUBLIC,
                    true
                ),
                (
                    2,
                    "x".to_owned(),
                    vec![("e".to_owned(), 4, -1)],
                    SchemaState::WRITE_ONLY,
                    true
                ),
                (
                    3,
                    "f".to_owned(),
                    vec![("f".to_owned(), 8, -1)],
                    SchemaState::PUBLIC,
                    true
                ),
                (
                    4,
                    "g".to_owned(),
                    vec![("g".to_owned(), 9, -1)],
                    SchemaState::PUBLIC,
                    false
                ),
                (
                    5,
                    "f_g".to_owned(),
                    vec![("f".to_owned(), 8, -1), ("g".to_owned(), 9, -1)],
                    SchemaState::PUBLIC,
                    true
                ),
                (
                    6,
                    "c_d_e_str".to_owned(),
                    vec![
                        ("c_str".to_owned(), 5, -1),
                        ("d_str".to_owned(), 6, -1),
                        ("e_str".to_owned(), 7, -1)
                    ],
                    SchemaState::PUBLIC,
                    false
                ),
                (
                    7,
                    "e_d_c_str_prefix".to_owned(),
                    vec![
                        ("e_str".to_owned(), 7, -1),
                        ("d_str".to_owned(), 6, -1),
                        ("c_str".to_owned(), 5, 10)
                    ],
                    SchemaState::PUBLIC,
                    false
                ),
            ]
        );
    }

    #[test]
    fn all_specialized_fixtures_match_the_pinned_metadata() {
        let unsigned = mock_unsigned_table();
        assert_eq!(unsigned.columns.len(), 3);
        assert_ne!(
            unsigned
                .columns
                .iter_deref()
                .next()
                .unwrap()
                .read()
                .get_flag()
                & u64::from(FieldTypeFlags::UNSIGNED),
            0
        );
        let no_pk = mock_no_pk_table();
        assert!(no_pk.indices.is_empty());
        assert_eq!(no_pk.columns.iter_deref().next().unwrap().read().offset, 1);
        let view = mock_view();
        let view_info = view.view.unwrap();
        assert_eq!(view_info.read().select_stmt, "select b,c,d from t");
        assert_eq!(view_info.read().security, ViewSecurity::DEFINER);
        assert_eq!(view_info.read().definer.as_ref().unwrap().username, "root");
        assert_eq!(view_info.read().definer.as_ref().unwrap().hostname, "");
        assert_eq!(
            view_info
                .read()
                .cols
                .iter()
                .map(CiString::original)
                .collect::<Vec<_>>(),
            ["b", "c", "d"]
        );
        let range = mock_range_partition_table();
        assert_eq!(
            range.partition.unwrap().read().definitions.snapshot()[1]
                .less_than
                .snapshot(),
            ["32"]
        );
        let hash = mock_hash_partition_table();
        assert_eq!(hash.partition.unwrap().read().num, 2);
        let list = mock_list_partition_table();
        assert_eq!(
            list.partition.unwrap().read().definitions.snapshot()[0]
                .in_values
                .snapshot()[0]
                .snapshot(),
            ["1"]
        );
        let global = mock_global_index_hash_partition_table();
        let global_indexes: Vec<_> = global
            .indices
            .iter_deref()
            .skip(7)
            .map(|index| {
                let index = index.read();
                (index.name.original().to_owned(), index.unique, index.global)
            })
            .collect();
        assert_eq!(
            global_indexes,
            [
                ("b".to_owned(), false, false),
                ("b_global".to_owned(), true, true),
                ("b_c".to_owned(), false, false),
                ("b_c_global".to_owned(), true, true),
            ]
        );
        let state_none = mock_state_none_column_table();
        assert_eq!(
            state_none.columns.iter_deref().nth(2).unwrap().read().state,
            SchemaState::NONE
        );
    }

    #[test]
    fn suite_reassigns_table_and_partition_ids_in_source_order() {
        let mut suite = create_planner_suite_elems();
        assert!(std::ptr::eq(suite.get_sctx(), suite.get_ctx()));
        assert_eq!(suite.get_ctx().current_database(), "test");
        assert_eq!(suite.get_ctx().div_precision_increment(), 4);
        let ids: Vec<_> = suite
            .get_is()
            .tables()
            .iter()
            .map(|table| table.read().id)
            .collect();
        assert_eq!(ids, [1, 2, 3, 4, 5, 8, 11, 14, 15]);
        assert!(suite.get_is().find_table("TEST", "T").is_some());
        assert!(suite.get_is().find_view("test", "V").is_some());
        suite
            .get_parser()
            .parse("SELECT row_number() OVER ()")
            .unwrap();
        suite.close();
    }
}
