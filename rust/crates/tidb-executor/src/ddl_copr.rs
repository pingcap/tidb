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

//! SEED of Go `pkg/ddl/copr` (`copr_ctx.go`): the immutable context a DDL
//! backfill hands to the coprocessor when it reads a table to build indexes.
//!
//! Every production symbol of the Go file is here and carries its own logic:
//! [`CopContext`], [`CopContextBase`], [`CopContextSingleIndex`],
//! [`CopContextMultiIndex`], [`new_cop_context`], [`new_cop_context_base`],
//! [`new_cop_context_single_index`], [`new_cop_context_multi_index`],
//! [`CopContextBase::get_schema_and_names`], and the four file-private helpers
//! `fill_used_columns`, `resolve_indices_for_index`,
//! [`resolve_indices_for_handle`], and
//! [`collect_virtual_column_offsets_and_types`]. All three upstream tests are
//! ported below.
//!
//! Every `pkg/expression` entry point `copr_ctx.go` calls is now REACHED, in
//! `tidb_expr::simple_expr`, rather than narrowed onto a local stub:
//! `ColumnInfos2ColumnsAndNamesWithCollate` (which subsumes the
//! `BuildSimpleExpr` + `ResolveIndices` leg for a virtual generated column),
//! `ParseSimpleExpr` with `WithInputSchemaAndNames`, `ComposeDNFCondition` and
//! `ExtractColumns`. What remains of the old boundary trait is
//! [`CopBuildContext`], now just the two accessors Go's single `exprCtx` value
//! is used for, with [`CopExprContext`] as its production implementation.
//!
//! Reached rather than narrowed: `tables.FindPrimaryIndex` is a three-line
//! scan inlined as [`find_primary_index`] (`pkg/table/tables` has no Rust
//! crate of its own; its ported pieces live in [`crate::kv_table`]),
//! `tables.DedupIndexColumns` is [`crate::kv_table::dedup_index_columns`], and
//! `tables.ExtractColumnsFromCondition` is
//! [`crate::kv_table::extract_columns_from_index_condition`].
//!
//! # Why this is still a SEED
//!
//! Three build options `copr_ctx.go` passes are carried but not yet honored by
//! `tidb_expr::simple_expr` (itself a seed of `pkg/expression`), whose module
//! header names them as ITS boundaries. All three change what this package
//! observes, so they are named here too:
//!
//! - `// boundary:` `WithAllowCastArray(true)`. Go builds every virtual
//!   generated column of a backfilled table with the ARRAY-cast leg enabled,
//!   which is exactly how a MULTI-VALUED index's `cast(j as signed array)`
//!   column is built. `tidb-expr`'s rewriter rejects every `CAST(.. AS ..
//!   ARRAY)`, so a multi-valued-index backfill cannot be constructed through
//!   this port yet; it errors instead of building.
//! - `// boundary:` `WithUseNewCollate(useNewCollate)`. Go steers a generated
//!   column's collation derivation with [`CopContextBase::use_new_collate`];
//!   `tidb-expr` derives collation from the process-wide
//!   `tidb_datatype::new_collation_enabled()` instead, so the flag reaches the
//!   builder (it is passed through in full) but does not yet change the result.
//! - `// boundary:` Go `WithInputSchemaAndNames(schema, names, tblInfo)`'s
//!   third argument. It exists in Go only to resolve `DEFAULT(col)` inside the
//!   built expression, a leg `tidb-expr` does not port; the `table` parameter
//!   of [`CopBuildContext::parse_simple_expr`] is kept and unused so the
//!   dropped argument stays greppable.
//!
//! Go's blank `_ "github.com/pingcap/tidb/pkg/infoschema"` import exists only
//! so the test binary registers `mock.MockInfoschema`; nothing in the package
//! names an info schema, so there is no Rust counterpart.
//!
//! One representation note: Go's `CopContextBase.FieldTypes` are
//! `*types.FieldType` pointers *into* the `ColumnInfos` entries, so a later
//! mutation of a column type is visible through both. Rust stores owned
//! [`FieldType`] clones; the package never mutates them after construction, so
//! the observable contract is unchanged.

use std::sync::Arc;

use tidb_ast::CiString;
use tidb_datatype::{FieldName, FieldNameMetadata, FieldType, IdentifierMetadata};
use tidb_expr::column::Column;
use tidb_expr::exprctx::{PlanColumnIdAllocator, SimplePlanColumnIdAllocator};
use tidb_expr::expression::Expression;
use tidb_expr::rewriter::ColumnResolver;
use tidb_expr::schema::Schema;
use tidb_expr::simple_expr::{self, BuildOptions, ColumnInfoSource};
use tidb_model::column::{find_column_info, EXTRA_HANDLE_ID, EXTRA_HANDLE_NAME};
use tidb_model::{ColumnInfo, GoShared, IndexColumn, IndexInfo, TableInfo};

use crate::kv_table::{dedup_index_columns, extract_columns_from_index_condition};

/// The error a cop-context build reports.
///
/// Go returns a wrapped `error`; the neighbouring ports in this crate
/// (`kv_table::extract_columns_from_index_condition`) carry messages as
/// `String`, so this follows them.
pub type CopResult<T> = Result<T, String>;

/// The slice of Go `exprctx.BuildContext` that `pkg/ddl/copr` reaches.
///
/// Go threads ONE `exprCtx` value into every `pkg/expression` call this file
/// makes. `tidb_expr::exprctx` does not declare the umbrella `BuildContext`
/// interface, so the two halves that file actually uses are named separately:
/// the build-time column/session view ([`ColumnResolver`]) and the plan-wide
/// column-id counter ([`PlanColumnIdAllocator`]). The two `pkg/expression`
/// entry points the `GetCondition` bodies call are provided methods with real
/// bodies over those halves, because Go calls them as free functions taking
/// `exprCtx` -- an implementor supplies the context, not the algorithm.
/// (`ColumnInfos2ColumnsAndNamesWithCollate`, the third, is called directly in
/// [`new_cop_context_base`] for the same reason.) [`CopExprContext`] is the
/// production implementation.
pub trait CopBuildContext: std::fmt::Debug + Send + Sync {
    /// The context every expression build in this package runs under: Go's
    /// `exprCtx` in its `expression.BuildContext` role.
    fn column_resolver(&self) -> &dyn ColumnResolver;

    /// Go `BuildContext.AllocPlanColumnID`'s owner.
    fn plan_column_ids(&self) -> &dyn PlanColumnIdAllocator;

    /// Go `expression.ParseSimpleExpr(ctx, sql, WithInputSchemaAndNames(schema,
    /// names, tblInfo))`.
    ///
    /// boundary: `table` is Go's `tblInfo` argument, which
    /// `WithInputSchemaAndNames` stores only to resolve `DEFAULT(col)` -- a leg
    /// `tidb_expr::simple_expr` does not port. The parameter is kept so the
    /// dropped argument is greppable at this call site.
    fn parse_simple_expr(
        &self,
        sql: &str,
        schema: &Schema,
        names: &[FieldName],
        table: &TableInfo,
    ) -> CopResult<Expression> {
        let _ = table;
        let options =
            BuildOptions::new().with_input_schema_and_names(schema.clone(), names.to_vec());
        simple_expr::parse_simple_expr(self.column_resolver(), sql, &options)
            .map_err(|error| error.to_string())
    }

    /// Go `expression.ComposeDNFCondition(ctx, exprs...)`: `OR` over the
    /// conditions. Go's own helper returns the single expression unchanged when
    /// only one is given, and nil for none -- which is [`Option::None`] here.
    fn compose_dnf_condition(&self, exprs: Vec<Expression>) -> Option<Expression> {
        simple_expr::compose_dnf_condition(exprs)
    }
}

/// The production [`CopBuildContext`]: a build context plus a plan column-id
/// allocator, which is what Go's `sessionctx.Context.GetExprCtx()` supplies to
/// this package.
///
/// Go's allocator lives in the session and keeps counting across statements, so
/// [`Self::with_id_offset`] exists for a caller that must continue an existing
/// numbering rather than start at 1.
pub struct CopExprContext<R> {
    resolver: R,
    ids: SimplePlanColumnIdAllocator,
}

impl<R> CopExprContext<R> {
    /// A context whose plan column ids start at 1, Go's
    /// `NewSimplePlanColumnIDAllocator(0)`.
    pub const fn new(resolver: R) -> Self {
        Self::with_id_offset(resolver, 0)
    }

    /// A context whose first allocated plan column id is `offset + 1`.
    pub const fn with_id_offset(resolver: R, offset: i64) -> Self {
        Self {
            resolver,
            ids: SimplePlanColumnIdAllocator::new(offset),
        }
    }
}

/// Written by hand because a [`ColumnResolver`] is a session view, not a value:
/// [`CopContextBase`] derives `Debug`, and requiring `R: Debug` of every caller
/// would push that on the session instead.
impl<R> std::fmt::Debug for CopExprContext<R> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CopExprContext")
            .field("last_plan_column_id", &self.ids.last_plan_column_id())
            .finish_non_exhaustive()
    }
}

impl<R: ColumnResolver + Send + Sync> CopBuildContext for CopExprContext<R> {
    fn column_resolver(&self) -> &dyn ColumnResolver {
        &self.resolver
    }

    fn plan_column_ids(&self) -> &dyn PlanColumnIdAllocator {
        &self.ids
    }
}

/// Go `CopContext` (`copr_ctx.go:31`): everything needed to build a
/// coprocessor request, unchanged after initialization.
pub trait CopContext {
    /// Go `GetBase`.
    fn get_base(&self) -> &CopContextBase;

    /// Go `IndexColumnOutputOffsets`.
    fn index_column_output_offsets(&self, idx_id: i64) -> &[usize];

    /// Go `IndexInfo`.
    fn index_info(&self, idx_id: i64) -> Option<GoShared<IndexInfo>>;

    /// Go `GetCondition`: the index condition as an expression.
    ///
    /// `Ok(None)` means the whole table must be scanned to build the index.
    /// The condition is what gets pushed down in the cop request, so it may be
    /// a single expression or a DNF expression.
    fn get_condition(&self) -> CopResult<Option<Expression>>;
}

/// Go `CopContextBase` (`copr_ctx.go:43`): fields shared by the single- and
/// multi-index contexts.
#[derive(Clone, Debug)]
pub struct CopContextBase {
    /// Go `TableInfo`.
    pub table_info: GoShared<TableInfo>,
    /// Go `PrimaryKeyInfo`: set only for a common-handle table.
    pub primary_key_info: Option<GoShared<IndexInfo>>,
    /// Go `ExprCtx`.
    pub expr_ctx: Arc<dyn CopBuildContext>,
    /// Go `PushDownFlags`.
    pub push_down_flags: u64,
    /// Go `RequestSource`.
    pub request_source: String,
    /// Go `UseNewCollate`.
    pub use_new_collate: bool,

    /// Go `ColumnInfos`: only the columns the index (and its condition) uses.
    pub column_infos: Vec<GoShared<ColumnInfo>>,
    /// Go `FieldTypes`, one per [`Self::column_infos`] entry.
    pub field_types: Vec<FieldType>,

    /// Go `ExprColumnInfos`.
    pub expr_column_infos: Vec<Column>,
    /// Go `HandleOutputOffsets`.
    pub handle_output_offsets: Vec<usize>,
    /// Go `VirtualColumnsOutputOffsets`.
    pub virtual_columns_output_offsets: Vec<usize>,
    /// Go `VirtualColumnsFieldTypes`.
    pub virtual_columns_field_types: Vec<FieldType>,
}

/// Go `CopContextSingleIndex` (`copr_ctx.go:61`).
#[derive(Clone, Debug)]
pub struct CopContextSingleIndex {
    /// Go's embedded `*CopContextBase`.
    pub base: CopContextBase,

    idx_info: GoShared<IndexInfo>,
    idx_col_output_offsets: Vec<usize>,
}

/// Go `CopContextMultiIndex` (`copr_ctx.go:69`).
#[derive(Clone, Debug)]
pub struct CopContextMultiIndex {
    /// Go's embedded `*CopContextBase`.
    pub base: CopContextBase,

    all_index_infos: Vec<GoShared<IndexInfo>>,
    idx_col_output_offsets: Vec<Vec<usize>>,
}

/// Go `tables.FindPrimaryIndex` (`pkg/table/tables/tables.go:667`): the index
/// flagged `Primary`, if any.
///
/// Inlined rather than imported: `pkg/table/tables` has no Rust crate, and the
/// pieces of it this workspace has ported live in [`crate::kv_table`].
#[must_use]
fn find_primary_index(tbl_info: &TableInfo) -> Option<GoShared<IndexInfo>> {
    tbl_info.indices.iter_deref().find(|idx| idx.read().primary)
}

/// Go `NewCopContextBase` (`copr_ctx.go:78`).
///
/// `idx_cols` contains all the index columns and also the columns referenced
/// by the index condition.
pub fn new_cop_context_base(
    expr_ctx: Arc<dyn CopBuildContext>,
    push_down_flags: u64,
    tbl_info: GoShared<TableInfo>,
    idx_cols: &[GoShared<IndexColumn>],
    request_source: String,
    use_new_collate: bool,
) -> CopResult<CopContextBase> {
    let table = tbl_info.read();
    let mut used_column_ids = std::collections::HashSet::with_capacity(idx_cols.len());
    fill_used_columns(&mut used_column_ids, idx_cols, &table)?;
    let mut handle_ids: Vec<i64> = Vec::new();

    let mut primary_idx: Option<GoShared<IndexInfo>> = None;
    if table.pk_is_handle {
        let pk_col = table
            .get_pk_col_info()
            .expect("PKIsHandle table has a primary-key column");
        let pk_col_id = pk_col.read().id;
        used_column_ids.insert(pk_col_id);
        handle_ids = vec![pk_col_id];
    } else if table.is_common_handle {
        let primary = find_primary_index(&table).expect("IsCommonHandle table has a primary index");
        let primary_columns: Vec<GoShared<IndexColumn>> =
            primary.read().columns.iter_deref().collect();
        handle_ids = Vec::with_capacity(primary_columns.len());
        for pk_col in &primary_columns {
            let offset = pk_col.read().offset;
            let col = table
                .columns
                .get(
                    usize::try_from(offset).map_err(|_| {
                        format!("primary index column offset {offset} is out of range")
                    })?,
                )
                .ok_or_else(|| format!("primary index column offset {offset} is out of range"))?;
            handle_ids.push(col.read().id);
        }
        fill_used_columns(&mut used_column_ids, &primary_columns, &table)?;
        primary_idx = Some(primary);
    }

    // Only collect the columns that are used by the index.
    let mut col_infos: Vec<GoShared<ColumnInfo>> = Vec::with_capacity(idx_cols.len());
    let mut field_tps: Vec<FieldType> = Vec::with_capacity(idx_cols.len());
    for col in table.columns.iter_deref() {
        let id = col.read().id;
        if used_column_ids.contains(&id) {
            field_tps.push(col.read().field_type.clone());
            col_infos.push(col);
        }
    }

    // Append the extra handle column when `_tidb_rowid` is used.
    if !table.has_clustered_index() {
        let extra = ColumnInfo::new_extra_handle_col_info();
        field_tps.push(extra.field_type.clone());
        col_infos.push(GoShared::new(extra));
        handle_ids = vec![EXTRA_HANDLE_ID];
    }

    let col_views: Vec<CopColumnInfo> = col_infos
        .iter()
        .map(|col| CopColumnInfo::of(&col.read()))
        .collect();
    let (exp_col_infos, _names) = simple_expr::column_infos_to_columns_and_names_with_collate(
        expr_ctx.column_resolver(),
        expr_ctx.plan_column_ids(),
        // Go passes an unused empty `dbName` here.
        &IdentifierMetadata::default(),
        &table.name,
        &col_views,
        use_new_collate,
    )
    .map_err(|error| error.to_string())?;
    let hd_col_offsets = resolve_indices_for_handle(&exp_col_infos, &handle_ids);
    let (v_col_offsets, v_col_fts) = collect_virtual_column_offsets_and_types(&exp_col_infos);

    drop(table);
    Ok(CopContextBase {
        table_info: tbl_info,
        primary_key_info: primary_idx,
        expr_ctx,
        push_down_flags,
        request_source,
        use_new_collate,
        column_infos: col_infos,
        field_types: field_tps,
        expr_column_infos: exp_col_infos,
        handle_output_offsets: hd_col_offsets,
        virtual_columns_output_offsets: v_col_offsets,
        virtual_columns_field_types: v_col_fts,
    })
}

/// Go `NewCopContext` (`copr_ctx.go:161`): a `CopContext` with a fixed
/// collation mode; one index takes the single-index shape.
pub fn new_cop_context(
    expr_ctx: Arc<dyn CopBuildContext>,
    push_down_flags: u64,
    tbl_info: GoShared<TableInfo>,
    all_idx_info: &[GoShared<IndexInfo>],
    request_source: String,
    use_new_collate: bool,
) -> CopResult<Box<dyn CopContext>> {
    if all_idx_info.len() == 1 {
        return Ok(Box::new(new_cop_context_single_index(
            expr_ctx,
            push_down_flags,
            tbl_info,
            all_idx_info[0].clone(),
            request_source,
            use_new_collate,
        )?));
    }
    Ok(Box::new(new_cop_context_multi_index(
        expr_ctx,
        push_down_flags,
        tbl_info,
        all_idx_info,
        request_source,
        use_new_collate,
    )?))
}

/// Go `NewCopContextSingleIndex` (`copr_ctx.go:183`).
pub fn new_cop_context_single_index(
    expr_ctx: Arc<dyn CopBuildContext>,
    push_down_flags: u64,
    tbl_info: GoShared<TableInfo>,
    idx_info: GoShared<IndexInfo>,
    request_source: String,
    use_new_collate: bool,
) -> CopResult<CopContextSingleIndex> {
    let mut cols = {
        let index = idx_info.read();
        let table = tbl_info.read();
        let mut cols: Vec<GoShared<IndexColumn>> = index.columns.iter_deref().collect();
        cols.extend(extract_columns_from_index_condition(&index, &table, false)?);
        cols
    };
    cols = dedup_index_columns(cols);

    let base = new_cop_context_base(
        expr_ctx,
        push_down_flags,
        tbl_info.clone(),
        &cols,
        request_source,
        use_new_collate,
    )?;
    let idx_offsets =
        resolve_indices_for_index(&base.expr_column_infos, &idx_info.read(), &tbl_info.read());
    Ok(CopContextSingleIndex {
        base,
        idx_info,
        idx_col_output_offsets: idx_offsets,
    })
}

impl CopContext for CopContextSingleIndex {
    /// Go `(*CopContextSingleIndex).GetBase` (`copr_ctx.go:212`).
    fn get_base(&self) -> &CopContextBase {
        &self.base
    }

    /// Go `(*CopContextSingleIndex).IndexColumnOutputOffsets` (`copr_ctx.go:217`):
    /// the index id is ignored, there is only one index.
    fn index_column_output_offsets(&self, _idx_id: i64) -> &[usize] {
        &self.idx_col_output_offsets
    }

    /// Go `(*CopContextSingleIndex).IndexInfo` (`copr_ctx.go:222`).
    fn index_info(&self, _idx_id: i64) -> Option<GoShared<IndexInfo>> {
        Some(self.idx_info.clone())
    }

    /// Go `(*CopContextSingleIndex).GetCondition` (`copr_ctx.go:227`).
    fn get_condition(&self) -> CopResult<Option<Expression>> {
        let (condition, has_condition) = {
            let index = self.idx_info.read();
            (index.condition_expr_string.clone(), index.has_condition())
        };
        if !has_condition {
            return Ok(None);
        }
        let (schema, names) = self.base.get_schema_and_names();
        let expr = self.base.expr_ctx.parse_simple_expr(
            &condition,
            &schema,
            &names,
            &self.base.table_info.read(),
        )?;
        Ok(reject_virtual_columns(expr))
    }
}

/// Go `NewCopContextMultiIndex` (`copr_ctx.go:250`).
pub fn new_cop_context_multi_index(
    expr_ctx: Arc<dyn CopBuildContext>,
    push_down_flags: u64,
    tbl_info: GoShared<TableInfo>,
    all_idx_info: &[GoShared<IndexInfo>],
    request_source: String,
    use_new_collate: bool,
) -> CopResult<CopContextMultiIndex> {
    let approx_col_len: usize = all_idx_info
        .iter()
        .map(|idx| idx.read().columns.len())
        .sum();
    let mut all_idx_cols: Vec<GoShared<IndexColumn>> = Vec::with_capacity(approx_col_len);
    {
        let table = tbl_info.read();
        for idx_info in all_idx_info {
            let index = idx_info.read();
            all_idx_cols.extend(index.columns.iter_deref());
            all_idx_cols.extend(extract_columns_from_index_condition(&index, &table, false)?);
        }
    }
    all_idx_cols = dedup_index_columns(all_idx_cols);

    let base = new_cop_context_base(
        expr_ctx,
        push_down_flags,
        tbl_info.clone(),
        &all_idx_cols,
        request_source,
        use_new_collate,
    )?;

    let mut idx_offsets = Vec::with_capacity(all_idx_info.len());
    for idx_info in all_idx_info {
        idx_offsets.push(resolve_indices_for_index(
            &base.expr_column_infos,
            &idx_info.read(),
            &tbl_info.read(),
        ));
    }
    Ok(CopContextMultiIndex {
        base,
        all_index_infos: all_idx_info.to_vec(),
        idx_col_output_offsets: idx_offsets,
    })
}

impl CopContext for CopContextMultiIndex {
    /// Go `(*CopContextMultiIndex).GetBase` (`copr_ctx.go:291`).
    fn get_base(&self) -> &CopContextBase {
        &self.base
    }

    /// Go `(*CopContextMultiIndex).IndexColumnOutputOffsets` (`copr_ctx.go:296`).
    fn index_column_output_offsets(&self, index_id: i64) -> &[usize] {
        for (i, idx_info) in self.all_index_infos.iter().enumerate() {
            if idx_info.read().id == index_id {
                return &self.idx_col_output_offsets[i];
            }
        }
        &[]
    }

    /// Go `(*CopContextMultiIndex).IndexInfo` (`copr_ctx.go:306`).
    fn index_info(&self, index_id: i64) -> Option<GoShared<IndexInfo>> {
        self.all_index_infos
            .iter()
            .find(|idx_info| idx_info.read().id == index_id)
            .cloned()
    }

    /// Go `(*CopContextMultiIndex).GetCondition` (`copr_ctx.go:316`): every
    /// index must carry a condition, and the conditions are `OR`-ed together.
    fn get_condition(&self) -> CopResult<Option<Expression>> {
        let mut exprs = Vec::with_capacity(self.all_index_infos.len());
        for idx_info in &self.all_index_infos {
            let (condition, has_condition) = {
                let index = idx_info.read();
                (index.condition_expr_string.clone(), index.has_condition())
            };
            if !has_condition {
                return Ok(None);
            }
            let (schema, names) = self.base.get_schema_and_names();
            let expr = self.base.expr_ctx.parse_simple_expr(
                &condition,
                &schema,
                &names,
                &self.base.table_info.read(),
            )?;
            match reject_virtual_columns(expr) {
                // Virtual generated columns cannot be pushed down.
                None => return Ok(None),
                Some(expr) => exprs.push(expr),
            }
        }

        // Use `OR` to combine all the conditions.
        if exprs.is_empty() {
            return Ok(None);
        }
        Ok(self.base.expr_ctx.compose_dnf_condition(exprs))
    }
}

/// The shared tail of both `GetCondition` bodies: a condition that reads a
/// virtual generated column cannot be pushed down, so it is dropped entirely.
fn reject_virtual_columns(expr: Expression) -> Option<Expression> {
    for col in simple_expr::extract_columns(&expr) {
        if col.virtual_expr.is_some() {
            return None;
        }
    }
    Some(expr)
}

/// Go `fillUsedColumns` (`copr_ctx.go:346`): seeds the used-column set with the
/// index columns, then transitively expands each virtual generated column into
/// the columns it depends on.
fn fill_used_columns(
    used_cols: &mut std::collections::HashSet<i64>,
    idx_cols: &[GoShared<IndexColumn>],
    tbl_info: &TableInfo,
) -> CopResult<()> {
    let mut cols_to_check: std::collections::VecDeque<GoShared<ColumnInfo>> =
        std::collections::VecDeque::with_capacity(idx_cols.len());
    for idx_col in idx_cols {
        let offset = idx_col.read().offset;
        let col = usize::try_from(offset)
            .ok()
            .and_then(|offset| tbl_info.columns.get(offset))
            .ok_or_else(|| format!("index column offset {offset} is out of range"))?;
        cols_to_check.push_back(col);
    }
    while let Some(next) = cols_to_check.pop_front() {
        let next = next.read();
        used_cols.insert(next.id);
        for dep_col_name in next.dependences.snapshot() {
            // Expand the virtual generated columns.
            let dep_col_name = dep_col_name.to_string();
            let dep_col = find_column_info(&tbl_info.columns, &dep_col_name)
                .ok_or_else(|| format!("dependent column {dep_col_name} not found"))?;
            let dep_id = dep_col.read().id;
            if !used_cols.contains(&dep_id) {
                cols_to_check.push_back(dep_col);
            }
        }
    }
    Ok(())
}

/// Go `resolveIndicesForIndex` (`copr_ctx.go:373`): the output offset of each
/// index column, by column id.
fn resolve_indices_for_index(
    output_cols: &[Column],
    idx_info: &IndexInfo,
    tbl_info: &TableInfo,
) -> Vec<usize> {
    let mut offsets = Vec::with_capacity(idx_info.columns.len());
    for idx_col in idx_info.columns.iter_deref() {
        let offset = idx_col.read().offset;
        let Some(hid) = usize::try_from(offset)
            .ok()
            .and_then(|offset| tbl_info.columns.get(offset))
            .map(|col| col.read().id)
        else {
            continue;
        };
        for (j, col) in output_cols.iter().enumerate() {
            if col.id == hid {
                offsets.push(j);
                break;
            }
        }
    }
    offsets
}

/// Go `resolveIndicesForHandle` (`copr_ctx.go:391`): the output offset of each
/// handle column, in handle order. A handle id absent from `cols` contributes
/// nothing, so the result can be shorter than `handle_ids`.
#[must_use]
pub fn resolve_indices_for_handle(cols: &[Column], handle_ids: &[i64]) -> Vec<usize> {
    let mut offsets = Vec::with_capacity(handle_ids.len());
    for hid in handle_ids {
        for (j, col) in cols.iter().enumerate() {
            if col.id == *hid {
                offsets.push(j);
                break;
            }
        }
    }
    offsets
}

/// Go `collectVirtualColumnOffsetsAndTypes` (`copr_ctx.go:404`): the offsets
/// and result types of the virtual generated columns among `cols`.
///
/// Go takes an `expression.EvalContext` only to pass to `col.GetType(ctx)`,
/// whose `Column` implementation ignores it; the Rust `Column` exposes that as
/// the context-free `get_static_type`, so the parameter is dropped.
#[must_use]
pub fn collect_virtual_column_offsets_and_types(cols: &[Column]) -> (Vec<usize>, Vec<FieldType>) {
    let mut offsets = Vec::new();
    let mut fts = Vec::new();
    for (i, col) in cols.iter().enumerate() {
        if col.virtual_expr.is_some() {
            offsets.push(i);
            fts.push(
                col.get_static_type()
                    .cloned()
                    .expect("a virtual column carries a result type"),
            );
        }
    }
    (offsets, fts)
}

impl CopContextBase {
    /// Go `(*CopContextBase).GetSchemaAndNames` (`copr_ctx.go:417`): the schema
    /// and name slice returned from the internal cop request.
    #[must_use]
    pub fn get_schema_and_names(&self) -> (Schema, Vec<FieldName>) {
        let table = self.table_info.read();
        let mut expr_columns = Vec::with_capacity(self.expr_column_infos.len());
        let mut names = Vec::new();
        for (i, col) in self.expr_column_infos.iter().enumerate() {
            let mut new_col = col.clone();
            new_col.index = i as i64;

            // Specially handle the extra handle column: its name is not in the
            // table info.
            let col_name = if col.id == EXTRA_HANDLE_ID {
                IdentifierMetadata::new(EXTRA_HANDLE_NAME)
            } else {
                let source = table
                    .columns
                    .get(usize::try_from(col.index).unwrap_or(usize::MAX))
                    .expect("expression column index addresses a table column");
                let source = source.read();
                IdentifierMetadata::from_parts(source.name.original(), source.name.lowercase())
            };
            expr_columns.push(new_col);

            names.push(FieldName::new(FieldNameMetadata {
                table: IdentifierMetadata::from_parts(
                    table.name.original(),
                    table.name.lowercase(),
                ),
                column: col_name,
                ..Default::default()
            }));
        }
        (Schema::new(expr_columns), names)
    }
}

/// The `model.ColumnInfo` fields `expression.ColumnInfos2ColumnsAndNamesWithCollate`
/// reads, as an owned snapshot.
///
/// `tidb-expr` sits below `tidb-model` and takes that metadata through its
/// [`ColumnInfoSource`] view, so the impl for real column metadata has to live
/// in a crate that sees both -- this one. It is a snapshot rather than a
/// borrow of the [`GoShared<ColumnInfo>`] because the trait hands back
/// references and `GoShared::read` yields a guard, not a reference; nothing in
/// `copr_ctx.go` mutates a column between the snapshot and the build.
struct CopColumnInfo {
    name: CiString,
    id: i64,
    offset: i64,
    field_type: FieldType,
    hidden: bool,
    /// Set only for a VIRTUAL generated column, which is the exact condition
    /// under which Go builds `GeneratedExprString`.
    virtual_generated_expr: Option<String>,
}

impl CopColumnInfo {
    fn of(col: &ColumnInfo) -> Self {
        Self {
            name: col.name.clone(),
            id: col.id,
            offset: col.offset,
            field_type: col.field_type.clone(),
            hidden: col.hidden,
            virtual_generated_expr: col
                .is_virtual_generated()
                .then(|| col.generated_expr_string.clone()),
        }
    }
}

impl ColumnInfoSource for CopColumnInfo {
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
        self.virtual_generated_expr.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::FieldTypeCode;
    use tidb_expr::constant::Constant;
    use tidb_expr::rewriter::NoResolver;
    use tidb_model::SchemaState;

    /// Go `util/mock.NewContext().GetExprCtx()`: the production context over a
    /// resolver that knows no columns of its own, since every column these
    /// fixtures name is reached through the schema the cop context builds.
    fn expr_context() -> Arc<dyn CopBuildContext> {
        Arc::new(CopExprContext::new(NoResolver))
    }

    /// Direct port of `pkg/ddl/copr/copr_ctx_test.go::TestNewCopContextSingleIndex`.
    #[test]
    fn test_new_cop_context_single_index() {
        const COL_CNT: i64 = 6;
        const PK_TYPE_ROW_ID: i32 = 0;
        const PK_TYPE_PK_HANDLE: i32 = 1;
        const PK_TYPE_COMMON_HANDLE: i32 = 2;

        let mock_col_infos: Vec<GoShared<ColumnInfo>> = (0..COL_CNT)
            .map(|i| {
                GoShared::new(ColumnInfo {
                    id: i,
                    offset: i,
                    name: CiString::new(format!("c{i}")),
                    field_type: FieldType::new(FieldTypeCode::from_mysql_type(1)),
                    state: SchemaState::PUBLIC,
                    ..Default::default()
                })
            })
            .collect();
        let find_col_by_name = |name: &str| -> GoShared<ColumnInfo> {
            mock_col_infos
                .iter()
                .find(|info| info.read().name.lowercase() == name)
                .expect("mock column")
                .clone()
        };

        let test_cases: [(i32, &[&str], &[&str]); 4] = [
            (PK_TYPE_ROW_ID, &["c1"], &["c1", "_tidb_rowid"]),
            (PK_TYPE_ROW_ID, &["c1", "c3"], &["c1", "c3", "_tidb_rowid"]),
            (PK_TYPE_PK_HANDLE, &["c1"], &["c0", "c1"]),
            (PK_TYPE_COMMON_HANDLE, &["c4", "c1"], &["c1", "c2", "c4"]),
        ];

        for (i, (pk_type, cols, expected_cols)) in test_cases.into_iter().enumerate() {
            let idx_cols: Vec<Option<IndexColumn>> = cols
                .iter()
                .map(|cn| {
                    Some(IndexColumn {
                        name: CiString::new(*cn),
                        offset: find_col_by_name(cn).read().offset,
                        ..Default::default()
                    })
                })
                .collect();
            let mock_idx_info = GoShared::new(IndexInfo {
                id: i as i64,
                name: CiString::new(format!("i{i}")),
                columns: tidb_model::GoSharedPointerSlice::from_nullable(idx_cols),
                state: SchemaState::PUBLIC,
                ..Default::default()
            });
            let mock_table_info = GoShared::new(TableInfo {
                name: CiString::new("t"),
                columns: tidb_model::GoSharedPointerSlice::from_handles(
                    mock_col_infos.iter().cloned().map(Some).collect(),
                ),
                indices: tidb_model::GoSharedPointerSlice::from_handles(vec![Some(
                    mock_idx_info.clone(),
                )]),
                pk_is_handle: pk_type == PK_TYPE_PK_HANDLE,
                is_common_handle: pk_type == PK_TYPE_COMMON_HANDLE,
                ..Default::default()
            });
            {
                let mut table = mock_table_info.write();
                if table.pk_is_handle {
                    let column = table.columns.get(0).expect("column 0");
                    let mut column = column.write();
                    let flag = column.get_flag();
                    column.set_flag(flag | u64::from(tidb_datatype::FieldTypeFlags::PRI_KEY));
                }
                if table.is_common_handle {
                    table.indices.push_go(IndexInfo {
                        columns: vec![
                            IndexColumn {
                                name: CiString::new("c2"),
                                offset: 2,
                                ..Default::default()
                            },
                            IndexColumn {
                                name: CiString::new("c4"),
                                offset: 4,
                                ..Default::default()
                            },
                        ]
                        .into(),
                        state: SchemaState::PUBLIC,
                        primary: true,
                        ..Default::default()
                    });
                }
            }

            let cop_ctx = new_cop_context_single_index(
                expr_context(),
                0,
                mock_table_info,
                mock_idx_info,
                String::new(),
                false,
            )
            .expect("new_cop_context_single_index");
            let base = cop_ctx.get_base();
            assert_eq!(base.table_info.read().name.lowercase(), "t");
            if pk_type != PK_TYPE_COMMON_HANDLE {
                assert!(base.primary_key_info.is_none());
            }
            let expected_len = expected_cols.len();
            assert_eq!(expected_len, base.column_infos.len());
            assert_eq!(expected_len, base.field_types.len());
            assert_eq!(expected_len, base.expr_column_infos.len());
            for (i, col) in base.column_infos.iter().enumerate() {
                assert_eq!(expected_cols[i], col.read().name.lowercase());
            }
            // Not in Go's assertions, but only observable now that a real
            // build context is used: the plan column ids come from this
            // context's allocator, one per output column, in output order.
            assert_eq!(
                base.expr_column_infos
                    .iter()
                    .map(|col| col.unique_id)
                    .collect::<Vec<_>>(),
                (1..=expected_len as i64).collect::<Vec<_>>()
            );
            // Nothing here is generated, so nothing is virtual.
            assert!(base.virtual_columns_output_offsets.is_empty());
            assert!(base.virtual_columns_field_types.is_empty());
            // No index carries a condition, so the whole table is scanned.
            assert!(cop_ctx.get_condition().expect("no condition").is_none());
        }
    }

    /// A table whose third column `v` is `a + b`, VIRTUAL generated.
    ///
    /// Go's `TestNewCopContextSingleIndex` fixtures carry neither a generated
    /// column nor an index condition, so the legs below are additional
    /// coverage of `copr_ctx.go`'s own code -- `fillUsedColumns`' dependency
    /// expansion, `collectVirtualColumnOffsetsAndTypes`, and both
    /// `GetCondition` bodies -- which could not be reached at all while the
    /// expression builders were stubbed out.
    fn generated_column_table(indices: Vec<IndexInfo>) -> GoShared<TableInfo> {
        let column = |id: i64, name: &str, generated: Option<&str>| {
            GoShared::new(ColumnInfo {
                id,
                offset: id - 1,
                name: CiString::new(name),
                field_type: FieldType::new(FieldTypeCode::LongLong),
                state: SchemaState::PUBLIC,
                generated_expr_string: generated.unwrap_or_default().to_owned(),
                dependences: if generated.is_some() {
                    tidb_model::column::GoStringSet::allocated(["a", "b"])
                } else {
                    tidb_model::column::GoStringSet::default()
                },
                ..Default::default()
            })
        };
        GoShared::new(TableInfo {
            name: CiString::new("t"),
            columns: tidb_model::GoSharedPointerSlice::from_handles(vec![
                Some(column(1, "a", None)),
                Some(column(2, "b", None)),
                Some(column(3, "v", Some("a + b"))),
            ]),
            indices: tidb_model::GoSharedPointerSlice::from_handles(
                indices
                    .into_iter()
                    .map(|idx| Some(GoShared::new(idx)))
                    .collect(),
            ),
            ..Default::default()
        })
    }

    fn index_on(id: i64, name: &str, column: &str, offset: i64, condition: &str) -> IndexInfo {
        IndexInfo {
            id,
            name: CiString::new(name),
            columns: vec![IndexColumn {
                name: CiString::new(column),
                offset,
                ..Default::default()
            }]
            .into(),
            state: SchemaState::PUBLIC,
            condition_expr_string: condition.to_owned(),
            ..Default::default()
        }
    }

    /// An index over a virtual generated column: `fillUsedColumns` pulls in the
    /// columns it depends on, and the built expression column carries the
    /// resolved `VirtualExpr` that `collectVirtualColumnOffsetsAndTypes` reads.
    #[test]
    fn cop_context_builds_a_virtual_generated_column() {
        let table = generated_column_table(vec![index_on(1, "iv", "v", 2, "")]);
        let idx_info = table.read().indices.get(0).expect("index").clone();
        let cop_ctx =
            new_cop_context_single_index(expr_context(), 0, table, idx_info, String::new(), false)
                .expect("cop context");
        let base = cop_ctx.get_base();

        // `v` depends on `a` and `b`, and the table has no clustered index, so
        // `_tidb_rowid` is appended.
        assert_eq!(
            base.column_infos
                .iter()
                .map(|col| col.read().name.lowercase().to_owned())
                .collect::<Vec<_>>(),
            vec!["a", "b", "v", "_tidb_rowid"]
        );
        assert_eq!(base.virtual_columns_output_offsets, vec![2]);
        assert_eq!(base.virtual_columns_field_types.len(), 1);
        assert_eq!(
            base.virtual_columns_field_types[0].code(),
            FieldTypeCode::LongLong
        );

        // The virtual expression is `a + b` with its indices resolved against
        // the schema of the columns built alongside it.
        let virtual_expr = base.expr_column_infos[2]
            .virtual_expr
            .as_ref()
            .expect("virtual expression");
        let referenced = simple_expr::extract_columns(virtual_expr);
        assert_eq!(
            referenced.iter().map(|col| col.id).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(
            referenced.iter().map(|col| col.index).collect::<Vec<_>>(),
            vec![0, 1]
        );
        // The index column output offset still addresses `v` itself.
        assert_eq!(cop_ctx.index_column_output_offsets(1), &[2]);
    }

    /// `(*CopContextSingleIndex).GetCondition`: a partial index's condition is
    /// parsed against the cop request's own schema, unless it reads a virtual
    /// generated column -- which cannot be pushed down.
    #[test]
    fn single_index_get_condition() {
        for (condition, wants_expr) in [("a > 10", true), ("v > 10", false)] {
            let table = generated_column_table(vec![index_on(1, "ia", "a", 0, condition)]);
            let idx_info = table.read().indices.get(0).expect("index").clone();
            let cop_ctx = new_cop_context_single_index(
                expr_context(),
                0,
                table,
                idx_info,
                String::new(),
                false,
            )
            .expect("cop context");

            let got = cop_ctx.get_condition().expect("condition builds");
            assert_eq!(got.is_some(), wants_expr, "condition {condition}");
            let Some(expr) = got else { continue };
            let columns = simple_expr::extract_columns(&expr);
            assert_eq!(columns.len(), 1, "condition {condition}");
            assert_eq!(columns[0].id, 1);
            assert!(columns[0].virtual_expr.is_none());
        }
    }

    /// `(*CopContextMultiIndex).GetCondition`: every index must carry a
    /// condition, and the conditions are `OR`-ed together.
    #[test]
    fn multi_index_get_condition_composes_a_dnf() {
        let table = generated_column_table(vec![
            index_on(1, "ia", "a", 0, "a > 10"),
            index_on(2, "ib", "b", 1, "b < 5"),
        ]);
        let all_idx_info: Vec<GoShared<IndexInfo>> = table.read().indices.iter_deref().collect();
        let cop_ctx = new_cop_context_multi_index(
            expr_context(),
            0,
            table.clone(),
            &all_idx_info,
            String::new(),
            false,
        )
        .expect("cop context");

        let expr = cop_ctx
            .get_condition()
            .expect("condition builds")
            .expect("both indexes carry a condition");
        let Expression::ScalarFunction(root) = &expr else {
            panic!("a two-condition DNF is a scalar function")
        };
        assert_eq!(root.func_name.lowercase(), "or");
        assert_eq!(
            simple_expr::extract_columns(&expr)
                .iter()
                .map(|col| col.id)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(cop_ctx.index_column_output_offsets(2), &[1]);
        assert_eq!(cop_ctx.index_info(2).expect("index").read().id, 2);

        // One index without a condition means the whole table is scanned.
        let table = generated_column_table(vec![
            index_on(1, "ia", "a", 0, "a > 10"),
            index_on(2, "ib", "b", 1, ""),
        ]);
        let all_idx_info: Vec<GoShared<IndexInfo>> = table.read().indices.iter_deref().collect();
        let cop_ctx = new_cop_context_multi_index(
            expr_context(),
            0,
            table,
            &all_idx_info,
            String::new(),
            false,
        )
        .expect("cop context");
        assert!(cop_ctx.get_condition().expect("no condition").is_none());
    }

    /// Direct port of `pkg/ddl/copr/copr_ctx_test.go::TestResolveIndicesForHandle`.
    #[test]
    fn test_resolve_indices_for_handle() {
        let column = |id: i64| {
            let mut col = Column::default();
            col.id = id;
            col
        };
        let cases: [(&str, Vec<i64>, Vec<usize>); 3] = [
            ("Basic 1", vec![2], vec![1]),
            ("Basic 2", vec![3, 2, 1], vec![2, 1, 0]),
            ("Basic 3", vec![1, 3], vec![0, 2]),
        ];
        for (name, handle_ids, want) in cases {
            let cols = [column(1), column(2), column(3)];
            let got = resolve_indices_for_handle(&cols, &handle_ids);
            assert_eq!(got, want, "{name}");
        }
    }

    /// Direct port of
    /// `pkg/ddl/copr/copr_ctx_test.go::TestCollectVirtualColumnOffsetsAndTypes`.
    #[test]
    fn test_collect_virtual_column_offsets_and_types() {
        let virtual_column = |tp: u8| {
            let mut col = Column::default();
            col.virtual_expr = Some(Box::new(Expression::Constant(Constant::default())));
            col.ret_type = Some(FieldType::new(FieldTypeCode::from_mysql_type(tp)));
            col
        };
        let plain_column = || Column::default();

        struct Case {
            name: &'static str,
            cols: Vec<Column>,
            offsets: Vec<usize>,
            field_tp: Vec<u8>,
        }

        let cases = [
            Case {
                name: "Basic 1",
                cols: vec![virtual_column(1), plain_column(), virtual_column(2)],
                offsets: vec![0, 2],
                field_tp: vec![1, 2],
            },
            Case {
                name: "Basic 2",
                cols: vec![plain_column(), virtual_column(1), plain_column()],
                offsets: vec![1],
                field_tp: vec![1],
            },
        ];
        for case in cases {
            let name = case.name;
            let (got_offsets, got_ft) = collect_virtual_column_offsets_and_types(&case.cols);
            assert_eq!(got_offsets, case.offsets, "{name}");
            assert_eq!(got_ft.len(), case.field_tp.len(), "{name}");
            for (i, ft) in got_ft.iter().enumerate() {
                assert_eq!(ft.code().mysql_type(), case.field_tp[i], "{name}");
            }
        }
    }
}
