// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Complete transcreation of pinned Go `pkg/ddl/copr`.

use std::collections::BTreeSet;

use tidb_ast::CiString;
use tidb_datatype::{FieldName, FieldNameMetadata, FieldType, IdentifierMetadata};
use tidb_expr::column::Column;
use tidb_expr::exprctx::PlanColumnIdAllocator;
use tidb_expr::expression::Expression;
use tidb_expr::rewriter::ColumnResolver;
use tidb_expr::schema::Schema;
use tidb_expr::simple_expr::{
    column_infos_to_columns_and_names_with_collate, compose_dnf_condition, extract_columns,
    parse_simple_expr, BuildOptions, ColumnInfoSource, SimpleExprError,
};
use tidb_model::column::{EXTRA_HANDLE_ID, EXTRA_HANDLE_NAME};
use tidb_model::{ColumnInfo, GoShared, IndexColumn, IndexInfo, TableInfo};

/// Failure returned while building a coprocessor context.
#[derive(Debug)]
pub enum Error {
    /// Go propagates expression parsing/building errors unchanged.
    Expression(SimpleExprError),
    /// Go `fillUsedColumns`' only package-owned error.
    DependentColumnNotFound(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Expression(error) => write!(formatter, "{error}"),
            Self::DependentColumnNotFound(name) => {
                write!(formatter, "dependent column {name} not found")
            }
        }
    }
}

impl std::error::Error for Error {}

impl From<SimpleExprError> for Error {
    fn from(error: SimpleExprError) -> Self {
        Self::Expression(error)
    }
}

/// The expression services carried by Go `exprctx.BuildContext` that this
/// package actually consumes.
#[derive(Clone, Copy)]
pub struct ExprBuildContext<'a> {
    /// Name resolution and session expression settings.
    pub resolver: &'a dyn ColumnResolver,
    /// Plan-column identity allocation.
    pub column_ids: &'a dyn PlanColumnIdAllocator,
}

/// Common immutable fields for single- and multi-index contexts.
#[derive(Clone)]
pub struct CopContextBase<'a> {
    /// Go `TableInfo`.
    pub table_info: GoShared<TableInfo>,
    /// Go `PrimaryKeyInfo`; set only for a common handle.
    pub primary_key_info: Option<GoShared<IndexInfo>>,
    /// Go `ExprCtx`, retained so `GetCondition` rebuilds against the current
    /// build context on every call just as the source does.
    pub expr_context: ExprBuildContext<'a>,
    /// Go `PushDownFlags`.
    pub push_down_flags: u64,
    /// Go `RequestSource`.
    pub request_source: String,
    /// Go `UseNewCollate`.
    pub use_new_collate: bool,
    /// Table columns needed by the index, condition, generated dependencies,
    /// and handle, in table order (plus `_tidb_rowid` when applicable).
    pub column_infos: Vec<GoShared<ColumnInfo>>,
    /// Field types corresponding one-for-one with `column_infos`.
    pub field_types: Vec<FieldType>,
    /// Expression columns corresponding one-for-one with `column_infos`.
    pub expr_column_infos: Vec<Column>,
    /// Output offsets of the physical row handle.
    pub handle_output_offsets: Vec<usize>,
    /// Output offsets of virtual generated columns.
    pub virtual_columns_output_offsets: Vec<usize>,
    /// Types corresponding one-for-one with `virtual_columns_output_offsets`.
    pub virtual_columns_field_types: Vec<FieldType>,
}

/// Go `CopContextSingleIndex`.
#[derive(Clone)]
pub struct CopContextSingleIndex<'a> {
    /// Shared context fields.
    pub base: CopContextBase<'a>,
    index_info: GoShared<IndexInfo>,
    index_column_output_offsets: Vec<usize>,
}

/// Go `CopContextMultiIndex`.
#[derive(Clone)]
pub struct CopContextMultiIndex<'a> {
    /// Shared context fields.
    pub base: CopContextBase<'a>,
    all_index_infos: Vec<GoShared<IndexInfo>>,
    index_column_output_offsets: Vec<Vec<usize>>,
}

/// The two concrete implementations returned by Go `NewCopContext`.
#[derive(Clone)]
pub enum CopContext<'a> {
    /// Exactly one index.
    Single(CopContextSingleIndex<'a>),
    /// Zero or more than one index.
    Multi(CopContextMultiIndex<'a>),
}

impl CopContext<'_> {
    /// Go `GetBase`.
    #[must_use]
    pub fn base(&self) -> &CopContextBase<'_> {
        match self {
            Self::Single(context) => &context.base,
            Self::Multi(context) => &context.base,
        }
    }

    /// Go `IndexColumnOutputOffsets`; `None` is Go nil for an unknown
    /// multi-index id.
    #[must_use]
    pub fn index_column_output_offsets(&self, index_id: i64) -> Option<&[usize]> {
        match self {
            Self::Single(context) => Some(&context.index_column_output_offsets),
            Self::Multi(context) => context.index_column_output_offsets(index_id),
        }
    }

    /// Go `IndexInfo`; `None` is Go nil for an unknown multi-index id.
    #[must_use]
    pub fn index_info(&self, index_id: i64) -> Option<GoShared<IndexInfo>> {
        match self {
            Self::Single(context) => Some(context.index_info.clone()),
            Self::Multi(context) => context.index_info(index_id),
        }
    }

    /// Go `GetCondition`; `None` means a full scan is required.
    pub fn condition(&self) -> Result<Option<Expression>, Error> {
        match self {
            Self::Single(context) => context.condition(),
            Self::Multi(context) => context.condition(),
        }
    }
}

impl CopContextSingleIndex<'_> {
    /// Go `GetBase`.
    #[must_use]
    pub const fn base(&self) -> &CopContextBase<'_> {
        &self.base
    }

    /// Go ignores the id for a single-index context.
    #[must_use]
    pub fn index_column_output_offsets(&self, _index_id: i64) -> &[usize] {
        &self.index_column_output_offsets
    }

    /// Go ignores the id for a single-index context.
    #[must_use]
    pub fn index_info(&self, _index_id: i64) -> GoShared<IndexInfo> {
        self.index_info.clone()
    }

    /// Go `GetCondition`.
    pub fn condition(&self) -> Result<Option<Expression>, Error> {
        build_condition(&self.base, &self.index_info.read())
    }
}

impl CopContextMultiIndex<'_> {
    /// Go `GetBase`.
    #[must_use]
    pub const fn base(&self) -> &CopContextBase<'_> {
        &self.base
    }

    /// Go `IndexColumnOutputOffsets`.
    #[must_use]
    pub fn index_column_output_offsets(&self, index_id: i64) -> Option<&[usize]> {
        self.all_index_infos
            .iter()
            .position(|index| index.read().id == index_id)
            .map(|offset| self.index_column_output_offsets[offset].as_slice())
    }

    /// Go `IndexInfo`.
    #[must_use]
    pub fn index_info(&self, index_id: i64) -> Option<GoShared<IndexInfo>> {
        self.all_index_infos
            .iter()
            .find(|index| index.read().id == index_id)
            .cloned()
    }

    /// Go `GetCondition`.
    pub fn condition(&self) -> Result<Option<Expression>, Error> {
        let mut conditions = Vec::with_capacity(self.all_index_infos.len());
        for index in &self.all_index_infos {
            let Some(condition) = build_condition(&self.base, &index.read())? else {
                return Ok(None);
            };
            conditions.push(condition);
        }
        Ok(compose_dnf_condition(conditions))
    }
}

/// Go `NewCopContext`.
pub fn new_cop_context<'a>(
    expr_context: ExprBuildContext<'a>,
    push_down_flags: u64,
    table_info: GoShared<TableInfo>,
    all_index_infos: Vec<GoShared<IndexInfo>>,
    request_source: impl Into<String>,
    use_new_collate: bool,
) -> Result<CopContext<'a>, Error> {
    if all_index_infos.len() == 1 {
        return new_cop_context_single_index(
            expr_context,
            push_down_flags,
            table_info,
            all_index_infos[0].clone(),
            request_source,
            use_new_collate,
        )
        .map(CopContext::Single);
    }
    new_cop_context_multi_index(
        expr_context,
        push_down_flags,
        table_info,
        all_index_infos,
        request_source,
        use_new_collate,
    )
    .map(CopContext::Multi)
}

/// Go `NewCopContextSingleIndex`.
pub fn new_cop_context_single_index<'a>(
    expr_context: ExprBuildContext<'a>,
    push_down_flags: u64,
    table_info: GoShared<TableInfo>,
    index_info: GoShared<IndexInfo>,
    request_source: impl Into<String>,
    use_new_collate: bool,
) -> Result<CopContextSingleIndex<'a>, Error> {
    let mut columns = index_info.read().columns.iter_deref().collect::<Vec<_>>();
    columns.extend(extract_columns_from_condition(
        expr_context,
        &index_info.read(),
        &table_info.read(),
        use_new_collate,
    )?);
    let columns = dedup_index_columns(columns);
    let base = new_cop_context_base(
        expr_context,
        push_down_flags,
        table_info,
        &columns,
        request_source.into(),
        use_new_collate,
    )?;
    let index_column_output_offsets = {
        let table = base.table_info.read();
        let index = index_info.read();
        resolve_indices_for_index(&base.expr_column_infos, &index, &table)
    };
    Ok(CopContextSingleIndex {
        base,
        index_info,
        index_column_output_offsets,
    })
}

/// Go `NewCopContextMultiIndex`.
pub fn new_cop_context_multi_index<'a>(
    expr_context: ExprBuildContext<'a>,
    push_down_flags: u64,
    table_info: GoShared<TableInfo>,
    all_index_infos: Vec<GoShared<IndexInfo>>,
    request_source: impl Into<String>,
    use_new_collate: bool,
) -> Result<CopContextMultiIndex<'a>, Error> {
    let mut all_columns = Vec::new();
    for index_info in &all_index_infos {
        all_columns.extend(index_info.read().columns.iter_deref());
        all_columns.extend(extract_columns_from_condition(
            expr_context,
            &index_info.read(),
            &table_info.read(),
            use_new_collate,
        )?);
    }
    let all_columns = dedup_index_columns(all_columns);
    let base = new_cop_context_base(
        expr_context,
        push_down_flags,
        table_info,
        &all_columns,
        request_source.into(),
        use_new_collate,
    )?;
    let index_column_output_offsets = {
        let table = base.table_info.read();
        all_index_infos
            .iter()
            .map(|index| resolve_indices_for_index(&base.expr_column_infos, &index.read(), &table))
            .collect()
    };

    Ok(CopContextMultiIndex {
        base,
        all_index_infos,
        index_column_output_offsets,
    })
}

fn new_cop_context_base<'a>(
    expr_context: ExprBuildContext<'a>,
    push_down_flags: u64,
    table_info: GoShared<TableInfo>,
    index_columns: &[GoShared<IndexColumn>],
    request_source: String,
    use_new_collate: bool,
) -> Result<CopContextBase<'a>, Error> {
    let table = table_info.read();
    let mut used_column_ids = fill_used_columns(BTreeSet::new(), index_columns, &table)?;
    let mut handle_ids = Vec::new();
    let mut primary_key_info = None;
    if table.pk_is_handle {
        let primary_column = table
            .get_pk_col_info()
            .expect("PKIsHandle table without primary-key column");
        let id = primary_column.read().id;
        used_column_ids.insert(id);
        handle_ids.push(id);
    } else if table.is_common_handle {
        let primary = table
            .indices
            .iter_deref()
            .find(|index| index.read().primary)
            .expect("common-handle table without primary index");
        for index_column in primary.read().columns.iter_deref() {
            let offset = index_column.read().offset as usize;
            let column = table
                .columns
                .get(offset)
                .expect("primary index column offset outside table");
            handle_ids.push(column.read().id);
        }
        used_column_ids = fill_used_columns(
            used_column_ids,
            &primary.read().columns.iter_deref().collect::<Vec<_>>(),
            &table,
        )?;
        primary_key_info = Some(primary);
    }

    let mut column_infos = table
        .columns
        .iter_deref()
        .filter(|column| used_column_ids.contains(&column.read().id))
        .collect::<Vec<_>>();
    if !table.has_clustered_index() {
        let extra = GoShared::new(ColumnInfo::new_extra_handle_col_info());
        column_infos.push(extra);
        handle_ids.clear();
        handle_ids.push(EXTRA_HANDLE_ID);
    }
    let field_types = column_infos
        .iter()
        .map(|column| column.read().field_type.clone())
        .collect::<Vec<_>>();
    let views = column_infos
        .iter()
        .map(|column| ColumnView::from_column(&column.read()))
        .collect::<Vec<_>>();
    let (expr_column_infos, _) = column_infos_to_columns_and_names_with_collate(
        expr_context.resolver,
        expr_context.column_ids,
        &IdentifierMetadata::default(),
        &table.name,
        &views,
        use_new_collate,
    )?;
    let handle_output_offsets = resolve_indices_for_handle(&expr_column_infos, &handle_ids);
    let (virtual_columns_output_offsets, virtual_columns_field_types) =
        collect_virtual_column_offsets_and_types(&expr_column_infos);
    drop(table);

    Ok(CopContextBase {
        table_info,
        primary_key_info,
        expr_context,
        push_down_flags,
        request_source,
        use_new_collate,
        column_infos,
        field_types,
        expr_column_infos,
        handle_output_offsets,
        virtual_columns_output_offsets,
        virtual_columns_field_types,
    })
}

fn build_condition(
    base: &CopContextBase<'_>,
    index_info: &IndexInfo,
) -> Result<Option<Expression>, Error> {
    if !index_info.has_condition() {
        return Ok(None);
    }
    let (schema, names) = base.schema_and_names();
    let options = BuildOptions::new()
        .with_input_schema_and_names(schema, names)
        .with_use_new_collate(base.use_new_collate);
    let expression = parse_simple_expr(
        base.expr_context.resolver,
        &index_info.condition_expr_string,
        &options,
    )?;
    if extract_columns(&expression)
        .iter()
        .any(|column| column.virtual_expr.is_some())
    {
        return Ok(None);
    }
    Ok(Some(expression))
}

impl CopContextBase<'_> {
    /// Go `GetSchemaAndNames`.
    #[must_use]
    pub fn schema_and_names(&self) -> (Schema, Vec<FieldName>) {
        let table = self.table_info.read();
        let table_name =
            IdentifierMetadata::from_parts(table.name.original(), table.name.lowercase());
        let mut columns = Vec::with_capacity(self.expr_column_infos.len());
        let mut names = Vec::with_capacity(self.expr_column_infos.len());
        for (offset, column) in self.expr_column_infos.iter().enumerate() {
            let mut cloned = column.clone();
            cloned.index = offset as i64;
            let column_name = if column.id == EXTRA_HANDLE_ID {
                CiString::new(EXTRA_HANDLE_NAME)
            } else {
                table
                    .columns
                    .get(column.index as usize)
                    .expect("expression column offset outside table")
                    .read()
                    .name
                    .clone()
            };
            let column_name =
                IdentifierMetadata::from_parts(column_name.original(), column_name.lowercase());
            columns.push(cloned);
            names.push(FieldName::new(FieldNameMetadata {
                original_table: IdentifierMetadata::default(),
                original_column: IdentifierMetadata::default(),
                database: IdentifierMetadata::default(),
                table: table_name.clone(),
                column: column_name,
            }));
        }
        (Schema::new(columns), names)
    }
}

fn extract_columns_from_condition(
    expr_context: ExprBuildContext<'_>,
    index_info: &IndexInfo,
    table_info: &TableInfo,
    use_new_collate: bool,
) -> Result<Vec<GoShared<IndexColumn>>, Error> {
    if !index_info.has_condition() {
        return Ok(Vec::new());
    }
    let table_columns = table_info.columns.iter_deref().collect::<Vec<_>>();
    let views = table_columns
        .iter()
        .map(|column| ColumnView::from_column(&column.read()))
        .collect::<Vec<_>>();
    let (columns, names) = column_infos_to_columns_and_names_with_collate(
        expr_context.resolver,
        expr_context.column_ids,
        &IdentifierMetadata::default(),
        &table_info.name,
        &views,
        use_new_collate,
    )?;
    let options = BuildOptions::new()
        .with_input_schema_and_names(Schema::new(columns), names)
        .with_use_new_collate(use_new_collate);
    let expression = parse_simple_expr(
        expr_context.resolver,
        &index_info.condition_expr_string,
        &options,
    )?;
    Ok(extract_columns(&expression)
        .into_iter()
        .map(|column| {
            let offset = column.index as usize;
            GoShared::new(IndexColumn {
                name: table_info
                    .columns
                    .get(offset)
                    .expect("condition column offset outside table")
                    .read()
                    .name
                    .clone(),
                offset: column.index,
                ..IndexColumn::default()
            })
        })
        .collect())
}

fn dedup_index_columns(columns: Vec<GoShared<IndexColumn>>) -> Vec<GoShared<IndexColumn>> {
    if columns.len() <= 1 {
        return columns;
    }
    let mut seen = BTreeSet::new();
    columns
        .into_iter()
        .filter(|column| seen.insert(column.read().offset))
        .collect()
}

fn fill_used_columns(
    mut used_columns: BTreeSet<i64>,
    index_columns: &[GoShared<IndexColumn>],
    table_info: &TableInfo,
) -> Result<BTreeSet<i64>, Error> {
    let mut columns_to_check = index_columns
        .iter()
        .map(|index_column| {
            table_info
                .columns
                .get(index_column.read().offset as usize)
                .expect("index column offset outside table")
        })
        .collect::<Vec<_>>();
    let mut next = 0;
    while next < columns_to_check.len() {
        let column = columns_to_check[next].clone();
        next += 1;
        let column = column.read();
        used_columns.insert(column.id);
        for dependency in column.dependences.snapshot() {
            let dependency_name = dependency.to_string();
            let dependency_lower = tidb_mysql::to_lowercase(&dependency_name);
            let dependent_column = table_info
                .columns
                .iter_deref()
                .find(|candidate| candidate.read().name.lowercase() == dependency_lower);
            let Some(dependent_column) = dependent_column else {
                return Err(Error::DependentColumnNotFound(dependency_name));
            };
            if !used_columns.contains(&dependent_column.read().id) {
                columns_to_check.push(dependent_column);
            }
        }
    }
    Ok(used_columns)
}

fn resolve_indices_for_index(
    output_columns: &[Column],
    index_info: &IndexInfo,
    table_info: &TableInfo,
) -> Vec<usize> {
    index_info
        .columns
        .iter_deref()
        .filter_map(|index_column| {
            let id = table_info
                .columns
                .get(index_column.read().offset as usize)
                .expect("index column offset outside table")
                .read()
                .id;
            output_columns.iter().position(|column| column.id == id)
        })
        .collect()
}

fn resolve_indices_for_handle(columns: &[Column], handle_ids: &[i64]) -> Vec<usize> {
    handle_ids
        .iter()
        .filter_map(|id| columns.iter().position(|column| column.id == *id))
        .collect()
}

fn collect_virtual_column_offsets_and_types(columns: &[Column]) -> (Vec<usize>, Vec<FieldType>) {
    let mut offsets = Vec::new();
    let mut field_types = Vec::new();
    for (offset, column) in columns.iter().enumerate() {
        if column.virtual_expr.is_some() {
            offsets.push(offset);
            field_types.push(
                column
                    .get_static_type()
                    .expect("expression column without field type")
                    .clone(),
            );
        }
    }
    (offsets, field_types)
}

#[derive(Clone)]
struct ColumnView {
    name: CiString,
    id: i64,
    offset: i64,
    field_type: FieldType,
    hidden: bool,
    virtual_generated_expression: Option<String>,
}

impl ColumnView {
    fn from_column(column: &ColumnInfo) -> Self {
        Self {
            name: column.name.clone(),
            id: column.id,
            offset: column.offset,
            field_type: column.field_type.clone(),
            hidden: column.hidden,
            virtual_generated_expression: (!column.generated_expr_string.is_empty()
                && !column.generated_stored)
                .then(|| column.generated_expr_string.clone()),
        }
    }
}

impl ColumnInfoSource for ColumnView {
    fn column_name(&self) -> &CiString {
        &self.name
    }

    fn column_id(&self) -> i64 {
        self.id
    }

    fn column_offset(&self) -> i64 {
        self.offset
    }

    fn column_field_type(&self) -> &FieldType {
        &self.field_type
    }

    fn column_hidden(&self) -> bool {
        self.hidden
    }

    fn virtual_generated_expr(&self) -> Option<&str> {
        self.virtual_generated_expression.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{FieldTypeCode, FieldTypeFlags};
    use tidb_expr::exprctx::SimplePlanColumnIdAllocator;
    use tidb_expr::rewriter::NoResolver;
    use tidb_model::SchemaState;

    fn column(name: &str, id: i64, offset: i64) -> ColumnInfo {
        let mut column = ColumnInfo::new(id, name, FieldType::new(FieldTypeCode::Tiny));
        column.offset = offset;
        column
    }

    fn index_column(name: &str, offset: i64) -> IndexColumn {
        IndexColumn {
            name: CiString::new(name),
            offset,
            ..IndexColumn::default()
        }
    }

    fn context<'a>(ids: &'a SimplePlanColumnIdAllocator) -> ExprBuildContext<'a> {
        ExprBuildContext {
            resolver: &NoResolver,
            column_ids: ids,
        }
    }

    // Go `TestNewCopContextSingleIndex`.
    #[test]
    fn new_cop_context_single_index_matches_all_handle_shapes() {
        let cases = [
            (0, vec!["c1"], vec!["c1", EXTRA_HANDLE_NAME]),
            (0, vec!["c1", "c3"], vec!["c1", "c3", EXTRA_HANDLE_NAME]),
            (1, vec!["c1"], vec!["c0", "c1"]),
            (2, vec!["c4", "c1"], vec!["c1", "c2", "c4"]),
        ];
        for (case_id, (primary_key_type, index_columns, expected)) in cases.into_iter().enumerate()
        {
            let mut columns = (0..6)
                .map(|offset| column(&format!("c{offset}"), offset, offset))
                .collect::<Vec<_>>();
            if primary_key_type == 1 {
                columns[0].set_flag(u64::from(FieldTypeFlags::PRI_KEY));
            }
            let index = GoShared::new(IndexInfo {
                id: case_id as i64,
                name: CiString::new(format!("i{case_id}")),
                columns: index_columns
                    .into_iter()
                    .map(|name| {
                        let offset = name[1..].parse::<i64>().expect("column suffix");
                        index_column(name, offset)
                    })
                    .collect::<Vec<_>>()
                    .into(),
                state: SchemaState::PUBLIC,
                ..IndexInfo::default()
            });
            let mut indices = vec![index.read().clone()];
            if primary_key_type == 2 {
                indices.push(IndexInfo {
                    columns: vec![index_column("c2", 2), index_column("c4", 4)].into(),
                    state: SchemaState::PUBLIC,
                    primary: true,
                    ..IndexInfo::default()
                });
            }
            let table = GoShared::new(TableInfo {
                name: CiString::new("t"),
                columns: columns.into(),
                indices: indices.into(),
                pk_is_handle: primary_key_type == 1,
                is_common_handle: primary_key_type == 2,
                ..TableInfo::default()
            });
            let ids = SimplePlanColumnIdAllocator::new(0);
            let cop_context =
                new_cop_context_single_index(context(&ids), 0, table, index, "", false)
                    .expect("context builds");
            assert_eq!(cop_context.base.table_info.read().name.lowercase(), "t");
            if primary_key_type != 2 {
                assert!(cop_context.base.primary_key_info.is_none());
            }
            assert_eq!(cop_context.base.column_infos.len(), expected.len());
            assert_eq!(cop_context.base.field_types.len(), expected.len());
            assert_eq!(cop_context.base.expr_column_infos.len(), expected.len());
            let actual = cop_context
                .base
                .column_infos
                .iter()
                .map(|column| column.read().name.lowercase().to_owned())
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
        }
    }

    // Go `TestResolveIndicesForHandle`.
    #[test]
    fn resolve_indices_for_handle_matches_go_cases() {
        let columns = [1, 2, 3]
            .into_iter()
            .map(|id| {
                let mut column = Column::new(id, FieldType::new(FieldTypeCode::Unspecified));
                column.id = id;
                column
            })
            .collect::<Vec<_>>();
        assert_eq!(resolve_indices_for_handle(&columns, &[2]), vec![1]);
        assert_eq!(
            resolve_indices_for_handle(&columns, &[3, 2, 1]),
            vec![2, 1, 0]
        );
        assert_eq!(resolve_indices_for_handle(&columns, &[1, 3]), vec![0, 2]);
    }

    // Go `TestCollectVirtualColumnOffsetsAndTypes`.
    #[test]
    fn collect_virtual_column_offsets_and_types_matches_go_cases() {
        use tidb_datatype::Datum;
        use tidb_expr::constant::Constant;

        let virtual_column = |code| {
            let field_type = FieldType::new(code);
            let mut column = Column::new(0, field_type.clone());
            column.virtual_expr = Some(Box::new(Expression::Constant(Constant::new(
                Datum::Null,
                field_type,
            ))));
            column
        };
        let plain_column = || Column::new(0, FieldType::new(FieldTypeCode::Unspecified));

        let (offsets, types) = collect_virtual_column_offsets_and_types(&[
            virtual_column(FieldTypeCode::Tiny),
            plain_column(),
            virtual_column(FieldTypeCode::Short),
        ]);
        assert_eq!(offsets, vec![0, 2]);
        assert_eq!(
            types.iter().map(FieldType::code).collect::<Vec<_>>(),
            vec![FieldTypeCode::Tiny, FieldTypeCode::Short]
        );

        let (offsets, types) = collect_virtual_column_offsets_and_types(&[
            plain_column(),
            virtual_column(FieldTypeCode::Tiny),
            plain_column(),
        ]);
        assert_eq!(offsets, vec![1]);
        assert_eq!(
            types.iter().map(FieldType::code).collect::<Vec<_>>(),
            vec![FieldTypeCode::Tiny]
        );
    }
}
