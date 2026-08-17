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

//! Go `pkg/planner/core/operator/logicalop/logical_table_scan.go`:
//! `LogicalTableScan`, "the logical table scan operator for TiKV".
//!
//! SEED of `pkg/planner/core`. This operator was a
//! [`crate::logical::TodoLogicalOp`] before this batch.
//!
//! # Narrowings, by name
//!
//! * `Ranges []*ranger.Range` is `pkg/util/ranger`'s range type, which is not
//!   transcreated; the field is ABSENT rather than typed against a
//!   placeholder, and nothing below reads it.
//! * `HandleCols util.HandleCols` becomes the handle COLUMNS, the same
//!   narrowing [`crate::logical::DataSource`] makes.
//! * `DeriveStats` is `utilfuncp.DeriveStats4LogicalTableScan(ts)`, which
//!   needs the session and the histogram collection; it is a named boundary and
//!   this operator derives nothing on its own.
//! * `PreparePossibleProperties` reads
//!   `SCtx().GetSessionVars().IsMPPAllowed()`; that switch is a PARAMETER here.

use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;

use crate::logical::data_source::DataSource;
use crate::logical::BaseLogicalPlan;
use crate::plan_base::PossiblePropertiesInfo;

/// Go `logicalop.LogicalTableScan` (`logical_table_scan.go:30`).
#[derive(Clone, Debug, Default)]
pub struct LogicalTableScan {
    /// The shared logical base.
    pub base: BaseLogicalPlan,
    /// Go `Source`: the data source this scan reads. A SIBLING reference, not
    /// a child edge.
    pub source: Option<Box<DataSource>>,
    /// Go `HandleCols`' columns.
    pub handle_cols: Vec<Column>,
    /// Go `AccessConds`: the CNF the range builder consumed.
    pub access_conds: Vec<Expression>,
}

impl LogicalTableScan {
    /// Go `plancodec.TypeTableScan`.
    pub const TYPE: &'static str = "TableScan";

    /// Go `LogicalTableScan.Init(ctx, offset)` (`logical_table_scan.go:39`).
    #[must_use]
    pub const fn new(base: BaseLogicalPlan) -> Self {
        Self {
            base,
            source: None,
            handle_cols: Vec::new(),
            access_conds: Vec::new(),
        }
    }

    /// Go `LogicalTableScan.BuildKeyInfo(selfSchema, childSchema)`
    /// (`logical_table_scan.go:66`): the SOURCE's, delegated whole.
    ///
    /// A table scan has the same keys as the table, so it borrows
    /// [`DataSource::build_key_info`] rather than deriving anything; see that
    /// body for why the index keys are the caller's to supply.
    pub fn build_key_info(&self, self_schema: &mut Schema, index_keys: Vec<Vec<Column>>) {
        match &self.source {
            Some(source) => source.build_key_info(self_schema, index_keys),
            None => self_schema.pk_or_uk = index_keys,
        }
    }

    /// Go `LogicalTableScan.PreparePossibleProperties(_, _)`
    /// (`logical_table_scan.go:77`): the HANDLE columns are the order a table
    /// scan naturally produces, because TiKV stores rows by handle.
    ///
    /// `is_mpp_allowed` is Go's `SCtx().GetSessionVars().IsMPPAllowed()`; a
    /// TiFlash-capable source is only usable as one when MPP is on. Note that
    /// this operator IGNORES its children's properties entirely — it is a leaf.
    pub fn prepare_possible_properties(
        &mut self,
        source_has_tiflash: bool,
        is_mpp_allowed: bool,
    ) -> PossiblePropertiesInfo {
        let has_tiflash = self.source.is_some() && source_has_tiflash && is_mpp_allowed;
        self.base.set_has_tiflash(has_tiflash);
        PossiblePropertiesInfo {
            orders: if self.handle_cols.is_empty() {
                Vec::new()
            } else {
                vec![self.handle_cols.clone()]
            },
            has_tiflash,
        }
    }

    /// Go `LogicalTableScan.ExplainInfo()` (`logical_table_scan.go:46`): the
    /// source's explain string, then the primary-key column, then the access
    /// conditions.
    ///
    /// # Blocked
    ///
    /// Go renders the handle with
    /// `HandleCols.StringWithCtx(evalCtx, EnableRedactLog)` and the conditions
    /// with `%v` over `expression.Expression`; both need an `EvalContext`. Each
    /// is reported by COUNT so neither is silently missing.
    #[must_use]
    pub fn explain_info(&self) -> String {
        let mut buffer = self
            .source
            .as_ref()
            .map(|source| source.explain_info())
            .unwrap_or_default();
        if !self.handle_cols.is_empty() {
            buffer.push_str(&format!(", pk col:{} cols", self.handle_cols.len()));
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
            handle_cols: self.handle_cols.clone(),
            access_conds: self.access_conds.clone(),
        }
    }
}
