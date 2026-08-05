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

//! The clustered integer handle as a trailing key part of a secondary index.
//!
//! A non-unique index entry stores the row's handle IN ITS KEY, right behind
//! the indexed values, so the handle is a real -- if undeclared -- last column
//! of that index. Go treats it as one: `fillIndexPath` appends it to
//! `path.IdxCols` before the ranger runs, and `detachCondAndBuildRangeForPath`
//! trims it back off again before asking for a row count, because the
//! STATISTICS are keyed by the index as DECLARED. Both halves live here,
//! because either one alone is wrong: without the append the ranger cannot
//! narrow on the handle at all, and without the trim the pseudo estimator
//! divides by 100 once for a column no histogram ever had.

use std::borrow::Cow;

use crate::index_range::RangeColumn;
use crate::kv_table::{IndexRange, KvIndex, KvTable};

/// The clustered integer handle as a trailing RANGER key part of `index`, and
/// the row offset it reads -- Go `fillIndexPath` in `pkg/planner/core/stats.go`:
///
/// ```go
/// if !path.Index.Unique && !path.Index.Primary &&
///     len(path.Index.Columns) == len(path.IdxCols) {
///     handleCol := ds.GetPKIsHandleCol()
///     if handleCol != nil && !mysql.HasUnsignedFlag(handleCol.RetType.GetFlag()) {
///         ... if !alreadyHandle { path.IdxCols = append(path.IdxCols, handleCol) }
///     }
/// }
/// ```
///
/// # This is not a widening of what the index can answer
///
/// A NON-DISTINCT index entry stores the handle IN ITS KEY, right after the
/// indexed values (`KvTable::index_key` appends
/// `encode_key([Datum::Int(handle)])` for exactly that reason). So `(c2, c1)`
/// is the literal byte layout of a `KEY c2(c2)` entry on a table whose `c1` is
/// the integer primary key, and a range over both columns is a range over a
/// contiguous key interval -- the same thing a two-column index would build.
/// `explain_easy`'s `where c1 > 1 and c2 = 1 and c3 < 1` is
/// `range:(1 1,1 +inf]` in TiDB for this reason and was `range:[1,1]` here.
///
/// # Why each condition is load-bearing
///
/// * UNIQUE is excluded because a DISTINCT entry does NOT carry the handle in
///   its key -- it lives in the value -- so the second column would not exist
///   in the bytes being ranged over.
/// * UNSIGNED is excluded because the handle is appended as a SIGNED
///   `Datum::Int` whatever the column's declared flag says, so an unsigned
///   handle's key bytes are not the bytes its own field type would range on.
/// * The already-present test keeps a `KEY c2(c2, c1)` from ranging on `c1`
///   twice, which Go calls out as "may cause unexpected errors". MEASURED:
///   removing it changes NO answer in this workspace, because the detacher
///   consumes each conjunct once and so finds nothing left to range on at the
///   repeated position. It is kept because it is Go's own condition and
///   because a three-entry column list for a two-column key is a lie the
///   moment the detacher stops being conjunct-consuming -- a fidelity guard,
///   not the thing producing today's answer.
///
/// A PREFIX key part is NOT a reason to refuse, which was measured rather than
/// assumed: `index sp(s(3))` over a table with an integer handle ranges
/// `("abc" 1,"abc" +inf]` in real TiDB for `where s = 'abcdef' and c1 > 1`.
/// The cut value is still a POINT over the key part's own stored bytes, so the
/// handle sits directly behind it and the two-dimension range is a contiguous
/// key interval like any other. Go's `len(path.Index.Columns) ==
/// len(path.IdxCols)` is about a key part whose COLUMN could not be resolved,
/// which this tier refuses one level up by skipping the index entirely.
///
/// Go's remaining condition, `!path.Index.Primary`, has no representable case
/// here: a clustered primary key never becomes a `KvIndex` (its encoding IS
/// the row key), and a table with a non-clustered primary key has no
/// `pk_handle_offset` for this to fire on.
pub(super) fn appended_handle_column(
    index: &KvIndex,
    table: &KvTable,
) -> Option<(RangeColumn, usize)> {
    if index.unique {
        return None;
    }
    let handle = table.pk_handle_offset()?;
    if index.column_offsets.contains(&handle) {
        return None;
    }
    let column = table.columns.get(handle)?;
    if column.field_type.is_unsigned() {
        return None;
    }
    Some((
        RangeColumn::whole(column.name.clone(), column.field_type.clone()),
        handle,
    ))
}

/// Go `pruneEstimateRange`: each range with its bound tuples cut to
/// `keep_columns` columns, and BOTH exclusion flags carried through unchanged
/// -- which is Go's own field-by-field copy, not an approximation of it.
///
/// Borrowed rather than cloned when nothing is cut, because that is every
/// index that never had a handle appended.
pub(super) fn prune_estimate_range(
    ranges: &[IndexRange],
    keep_columns: usize,
) -> Cow<'_, [IndexRange]> {
    if !ranges
        .iter()
        .any(|range| range.low.len() > keep_columns || range.high.len() > keep_columns)
    {
        return Cow::Borrowed(ranges);
    }
    Cow::Owned(
        ranges
            .iter()
            .map(|range| IndexRange {
                low: range.low[..range.low.len().min(keep_columns)].to_vec(),
                high: range.high[..range.high.len().min(keep_columns)].to_vec(),
                low_exclusive: range.low_exclusive,
                high_exclusive: range.high_exclusive,
            })
            .collect(),
    )
}
