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

//! Logical properties shared by every expression in one optimizer group.

use crate::stats_info::StatsInfo;
use tidb_expr::{column::Column, schema::Schema};
use tidb_funcdep::FdSet;

/// Go `property.LogicalProperty`.
#[derive(Clone, Debug, Default)]
pub struct LogicalProperty {
    /// Statistics for the group's output.
    pub stats: Option<StatsInfo>,
    /// Output schema shared by the group.
    pub schema: Option<Schema>,
    /// Functional dependencies of the output columns.
    pub fd: Option<FdSet>,
    /// Whether the group can produce at most one row.
    pub max_one_row: bool,
    /// Candidate order properties, expressed as column lists.
    pub possible_props: Vec<Vec<Column>>,
    /// Whether the group has a TiFlash access path.
    pub has_tiflash: bool,
}

impl LogicalProperty {
    /// Go `NewLogicalProp`: return the zero-valued property.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}
