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

//! Go `pkg/planner/core/operator/logicalop/logical_tikv_single_gather.go`:
//! `TiKVSingleGather`, "a leaf logical operator of TiDB layer to gather tuples
//! from TiKV regions".
//!
//! SEED of `pkg/planner/core`. This operator was a
//! [`crate::logical::TodoLogicalOp`] before this batch.
//!
//! # Narrowings, by name
//!
//! * `Index *model.IndexInfo` is used for exactly one thing —
//!   `sg.Index.Name.String()` inside `ExplainInfo` — so the NAME is carried
//!   rather than the whole info; `pkg/meta/model` is not a dependency of this
//!   crate.
//! * `Source *DataSource` is the scan's originating table. It is a SIBLING
//!   reference, not a child edge: Go's gather has the scan as its child and
//!   points back at the data source that produced both.

use tidb_expr::schema::Schema;

use crate::logical::data_source::DataSource;
use crate::logical::BaseLogicalPlan;
use crate::plan_base::PossiblePropertiesInfo;

/// Go `logicalop.TiKVSingleGather` (`logical_tikv_single_gather.go:27`).
#[derive(Clone, Debug, Default)]
pub struct TiKVSingleGather {
    /// The shared logical base.
    pub base: BaseLogicalPlan,
    /// Go `Source`; see this module's header.
    pub source: Option<Box<DataSource>>,
    /// Go `IsIndexGather`, whose comment says why it exists: "in
    /// implementation phase, we need this flag to determine whether to generate
    /// PhysicalTableReader or PhysicalIndexReader".
    pub is_index_gather: bool,
    /// Go `Index.Name.O`, when [`Self::is_index_gather`].
    pub index_name: Option<String>,
}

impl TiKVSingleGather {
    /// Go `plancodec.TypeTiKVSingleGather`.
    pub const TYPE: &'static str = "TiKVSingleGather";

    /// Go `TiKVSingleGather.Init(ctx, offset)`
    /// (`logical_tikv_single_gather.go:39`).
    #[must_use]
    pub const fn new(base: BaseLogicalPlan) -> Self {
        Self {
            base,
            source: None,
            is_index_gather: false,
            index_name: None,
        }
    }

    /// Go `TiKVSingleGather.BuildKeyInfo(selfSchema, childSchema)`
    /// (`logical_tikv_single_gather.go:60`): the child's keys are adopted
    /// WHOLESALE, without the column-survival check
    /// [`crate::logical::schema_producer::propagate_child_keys`] performs.
    ///
    /// That is sound only because a gather does not project: its schema IS its
    /// child's, so every key column necessarily survives.
    pub fn build_key_info(self_schema: &mut Schema, child_schema: &[Schema]) {
        self_schema.pk_or_uk = match child_schema.first() {
            Some(child) => child.pk_or_uk.clone(),
            None => Vec::new(),
        };
    }

    /// Go `TiKVSingleGather.PreparePossibleProperties(_, childrenProperties)`
    /// (`logical_tikv_single_gather.go:74`): the child's orders and TiFlash bit
    /// both pass straight through, because a gather is transparent to order.
    pub fn prepare_possible_properties(
        &mut self,
        child: Option<&PossiblePropertiesInfo>,
    ) -> PossiblePropertiesInfo {
        let Some(child) = child else {
            self.base.set_has_tiflash(false);
            return PossiblePropertiesInfo {
                orders: Vec::new(),
                has_tiflash: false,
            };
        };
        self.base.set_has_tiflash(child.has_tiflash);
        PossiblePropertiesInfo {
            orders: child.orders.clone(),
            has_tiflash: child.has_tiflash,
        }
    }

    /// Go `TiKVSingleGather.ExplainInfo()`
    /// (`logical_tikv_single_gather.go:46`): the source data source's own
    /// explain string, plus the index name for an index gather.
    #[must_use]
    pub fn explain_info(&self) -> String {
        let mut buffer = self
            .source
            .as_ref()
            .map(|source| source.explain_info())
            .unwrap_or_default();
        if self.is_index_gather {
            buffer.push_str(", index:");
            buffer.push_str(self.index_name.as_deref().unwrap_or(""));
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
            is_index_gather: self.is_index_gather,
            index_name: self.index_name.clone(),
        }
    }
}
