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

//! Go `pkg/planner/core/operator/logicalop/logical_index_scan.go`:
//! `LogicalIndexScan`, "the logical index scan operator for TiKV".
//!
//! SEED of `pkg/planner/core`. This operator was a
//! [`crate::logical::TodoLogicalOp`] before this batch.
//!
//! # Narrowings, by name
//!
//! * `Ranges []*ranger.Range` is `pkg/util/ranger`'s range type, which is not
//!   transcreated; the field is ABSENT rather than stubbed.
//! * `Index *model.IndexInfo` and `Columns []*model.ColumnInfo` are
//!   `pkg/meta/model`, not a dependency of this crate. The parts every body
//!   below reads — the index's COLUMN NAMES for `ExplainInfo` — are carried as
//!   [`LogicalIndexScan::index_column_names`].
//! * `BuildKeyInfo` calls `ruleutil.CheckIndexCanBeKey(path.Index, is.Columns,
//!   selfSchema)` per non-table access path, which needs those model types;
//!   the keys are therefore the caller's to supply, exactly as
//!   [`crate::logical::DataSource::build_key_info`] already decided. The
//!   `PKIsHandle` half IS ported.
//! * `DeriveStats` is `utilfuncp.DeriveStats4LogicalIndexScan(is, selfSchema)`.
//! * `MatchIndexProp` needs `property.PhysicalProperty.AllSameOrder`, which
//!   this crate's [`PhysicalProperty`] does not carry; the dependency-closed
//!   inner half is [`matches_indices_prop`].

use tidb_datatype::UNSPECIFIED_LENGTH;
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;

use crate::logical::data_source::DataSource;
use crate::logical::BaseLogicalPlan;
use crate::physical_property::SortItem;
use crate::plan_base::PossiblePropertiesInfo;

/// Go `logicalop.LogicalIndexScan` (`logical_index_scan.go:32`).
#[derive(Clone, Debug, Default)]
pub struct LogicalIndexScan {
    /// The shared logical base.
    pub base: BaseLogicalPlan,
    /// Go `Source`, whose comment says "DataSource should be read-only here".
    pub source: Option<Box<DataSource>>,
    /// Go `IsDoubleRead`: the index alone does not cover the query, so the
    /// table has to be read back by handle.
    pub is_double_read: bool,
    /// Go `EqCondCount`: how many LEADING index columns are pinned to a
    /// constant by an equality. Everything up to this point is a constant, so
    /// the index still delivers an order starting at ANY of those positions —
    /// which is what [`Self::prepare_possible_properties`] enumerates.
    pub eq_cond_count: usize,
    /// Go `AccessConds`.
    pub access_conds: Vec<Expression>,
    /// Go `Index.Columns[i].Name.O`, or the generated expression string for a
    /// hidden column; see this module's header.
    pub index_column_names: Vec<String>,
    /// Go `FullIdxCols`: every column of the index.
    pub full_idx_cols: Vec<Column>,
    /// Go `FullIdxColLens`.
    pub full_idx_col_lens: Vec<i64>,
    /// Go `IdxCols`: the prefix of the index this scan actually uses.
    pub idx_cols: Vec<Column>,
    /// Go `IdxColLens`.
    pub idx_col_lens: Vec<i64>,
}

impl LogicalIndexScan {
    /// Go `plancodec.TypeIdxScan`.
    pub const TYPE: &'static str = "IndexScan";

    /// Go `LogicalIndexScan.Init(ctx, offset)` (`logical_index_scan.go:50`).
    #[must_use]
    pub const fn new(base: BaseLogicalPlan) -> Self {
        Self {
            base,
            source: None,
            is_double_read: false,
            eq_cond_count: 0,
            access_conds: Vec::new(),
            index_column_names: Vec::new(),
            full_idx_cols: Vec::new(),
            full_idx_col_lens: Vec::new(),
            idx_cols: Vec::new(),
            idx_col_lens: Vec::new(),
        }
    }

    /// Go `LogicalIndexScan.BuildKeyInfo(selfSchema, _)`'s PORTABLE half
    /// (`logical_index_scan.go:88`): the keys the caller derived from the
    /// non-table access paths, plus the `PKIsHandle` column when the schema
    /// still carries it.
    ///
    /// See this module's header for `ruleutil.CheckIndexCanBeKey`, which is
    /// what produces `index_keys` and `nullable_unique_keys` in Go.
    pub fn build_key_info(
        &self,
        self_schema: &mut Schema,
        index_keys: Vec<Vec<Column>>,
        nullable_unique_keys: Vec<Vec<Column>>,
    ) {
        self_schema.pk_or_uk = index_keys;
        self_schema.nullable_uk.extend(nullable_unique_keys);
        if let Some(handle) = self.get_pk_is_handle_col(self_schema) {
            self_schema.pk_or_uk.push(vec![handle]);
        }
    }

    /// Go `LogicalIndexScan.GetPKIsHandleCol(schema)`
    /// (`logical_index_scan.go:196`), whose comment explains why it does NOT
    /// delegate to `Source.GetPKIsHandleCol`: "we may re-prune p.Columns and
    /// p.schema during the transformation. That will make p.Columns different
    /// from p.Source.Columns."
    ///
    /// So the lookup runs against THIS operator's schema.
    #[must_use]
    pub fn get_pk_is_handle_col(&self, schema: &Schema) -> Option<Column> {
        let source = self.source.as_ref()?;
        if !source.pk_is_handle {
            return None;
        }
        let handle = source.handle_cols.first()?;
        schema
            .columns
            .iter()
            .find(|col| col.unique_id == handle.unique_id)
            .cloned()
    }

    /// Go `LogicalIndexScan.PreparePossibleProperties(_, _)`
    /// (`logical_index_scan.go:117`): one offered order per equality-pinned
    /// PREFIX, from the whole index down to the first unpinned column.
    ///
    /// With `IdxCols = [a, b, c]` and `EqCondCount = 1` (so `a = const`), the
    /// index delivers both `[a, b, c]` and `[b, c]`, because every row already
    /// agrees on `a`. That is `EqCondCount + 1` orders.
    ///
    /// `is_mpp_allowed` is Go's `SCtx().GetSessionVars().IsMPPAllowed()`; see
    /// [`crate::logical::LogicalTableScan::prepare_possible_properties`].
    pub fn prepare_possible_properties(
        &mut self,
        source_has_tiflash: bool,
        is_mpp_allowed: bool,
    ) -> PossiblePropertiesInfo {
        let has_tiflash = self.source.is_some() && source_has_tiflash && is_mpp_allowed;
        self.base.set_has_tiflash(has_tiflash);
        if self.idx_cols.is_empty() {
            return PossiblePropertiesInfo {
                orders: Vec::new(),
                has_tiflash,
            };
        }
        let orders = (0..=self.eq_cond_count)
            .filter_map(|i| self.idx_cols.get(i..).map(<[Column]>::to_vec))
            .collect();
        PossiblePropertiesInfo {
            orders,
            has_tiflash,
        }
    }

    /// Go `LogicalIndexScan.ExplainInfo()` (`logical_index_scan.go:58`): the
    /// source's string, then the index's column names, then the access
    /// conditions.
    ///
    /// The condition list needs an `EvalContext` and is reported by COUNT; the
    /// index names are exact.
    #[must_use]
    pub fn explain_info(&self) -> String {
        let mut buffer = self
            .source
            .as_ref()
            .map(|source| source.explain_info())
            .unwrap_or_default();
        if !self.index_column_names.is_empty() {
            buffer.push_str(", index:");
            buffer.push_str(&self.index_column_names.join(", "));
        }
        if !self.access_conds.is_empty() {
            buffer.push_str(&format!(", cond:{} exprs", self.access_conds.len()));
        }
        buffer
    }

    /// This operator's own fields with NO children; see
    /// [`crate::logical::LogicalPlan::clone_shallow`].
    #[must_use]
    pub fn clone_shallow(&self) -> Self {
        Self {
            base: self.base.shell(),
            source: self.source.clone(),
            is_double_read: self.is_double_read,
            eq_cond_count: self.eq_cond_count,
            access_conds: self.access_conds.clone(),
            index_column_names: self.index_column_names.clone(),
            full_idx_cols: self.full_idx_cols.clone(),
            full_idx_col_lens: self.full_idx_col_lens.clone(),
            idx_cols: self.idx_cols.clone(),
            idx_col_lens: self.idx_col_lens.clone(),
        }
    }
}

/// Go `matchIndicesProp(sctx, idxCols, colLens, propItems)`
/// (`logical_index_scan.go:204`): the index columns satisfy the required order
/// only if they are at least as long, each required column matches positionally,
/// and NO matched index column is a PREFIX index.
///
/// The prefix-length test is the subtle one: a prefix index on `a(10)` orders by
/// the first ten bytes of `a`, which is not an order on `a`.
#[must_use]
pub fn matches_indices_prop(
    idx_cols: &[Column],
    col_lens: &[i64],
    prop_items: &[SortItem],
) -> bool {
    if idx_cols.len() < prop_items.len() {
        return false;
    }
    prop_items.iter().enumerate().all(|(i, item)| {
        col_lens.get(i).copied() == Some(UNSPECIFIED_LENGTH)
            && idx_cols.get(i).is_some_and(|col| col.unique_id == item.col)
    })
}
