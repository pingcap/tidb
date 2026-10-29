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

//! Go `pkg/planner/util/misc.go`.

use tidb_expr::column::Column;

/// Go `util.GetMaxSortPrefix(sortCols, allCols)`
/// (`pkg/planner/util/misc.go`): how many leading `sort_cols` are also in
/// `all_cols`, positionally.
#[must_use]
pub fn max_sort_prefix_len(sort_cols: &[Column], all_cols: &[Column]) -> usize {
    let limit = sort_cols.len().min(all_cols.len());
    let mut length = 0;
    while length < limit {
        if !all_cols
            .iter()
            .any(|column| column.unique_id == sort_cols[length].unique_id)
        {
            break;
        }
        length += 1;
    }
    length
}

#[cfg(test)]
mod tests {
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::column::Column;

    use super::max_sort_prefix_len;

    fn column(unique_id: i64) -> Column {
        Column::new(unique_id, FieldType::new(FieldTypeCode::LongLong))
    }

    /// Go `util.GetMaxSortPrefix`, as `PreparePossibleProperties` uses it.
    #[test]
    fn max_sort_prefix_stops_at_the_first_miss() {
        assert_eq!(
            max_sort_prefix_len(&[column(1), column(9), column(2)], &[column(1), column(2)]),
            1
        );
        assert_eq!(max_sort_prefix_len(&[], &[column(1)]), 0);
    }
}
