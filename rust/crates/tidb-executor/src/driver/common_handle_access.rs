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

use tidb_ast::SelectStmt;
use tidb_datatype::{Datum, FieldType, SessionTimeZone};

use crate::index_range::RangeColumn;
use crate::{KvTable, PartitionKind, TableHandle};

pub(super) struct CommonHandlePoints {
    pub(super) handles: Vec<TableHandle>,
    pub(super) columns: Vec<String>,
}

/// The point handles represented by a clustered primary-key access path.
///
/// TiDB keeps the common-handle primary index on its table access path. Rust
/// stores no duplicate secondary-index structure for those same record keys,
/// so ordinary index enumeration cannot expose it. Build the ranges over the
/// handle columns directly and return only the source shape the executor can
/// represent exactly: one point, or the batch-point partition shapes TiDB
/// permits.
pub(super) fn point_ranges(
    select: &SelectStmt,
    table: &KvTable,
    columns: &[(String, FieldType)],
    zone: &SessionTimeZone,
    static_partition_prune: bool,
) -> Option<CommonHandlePoints> {
    let offsets = table.common_handle_offsets();
    let predicate = select.where_clause.as_ref()?;
    if offsets.is_empty() {
        return None;
    }
    let range_columns = offsets
        .iter()
        .map(|offset| {
            let (name, field_type) = columns.get(*offset)?;
            Some(RangeColumn::whole(name.clone(), field_type.clone()))
        })
        .collect::<Option<Vec<_>>>()?;
    let built =
        crate::index_range::detach_cond_and_build_range_for_index(&range_columns, predicate, zone)?;
    if built.ranges.is_empty()
        || built.ranges.iter().any(|range| {
            range.low.len() != offsets.len()
                || !range.is_point(false)
                || range.low.iter().any(Datum::is_null)
        })
    {
        return None;
    }

    if built.ranges.len() > 1
        && table.partition().is_some_and(|partition| {
            !static_partition_prune
                || partition.dependencies.len() != 1
                || !matches!(partition.kind, PartitionKind::Hash | PartitionKind::Key)
        })
    {
        return None;
    }

    let mut handles = Vec::with_capacity(built.ranges.len());
    for range in built.ranges {
        let handle = table.common_handle_from_values(&range.low, zone).ok()?;
        if !handles.contains(&handle) {
            handles.push(handle);
        }
    }
    Some(CommonHandlePoints {
        handles,
        columns: range_columns
            .into_iter()
            .map(|column| column.name)
            .collect(),
    })
}
