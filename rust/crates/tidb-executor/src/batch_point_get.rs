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

//! Ordering support for Go `pkg/executor/batch_point_get.go`.
//!
//! The retained physical-plan builder owns the executor and both KV reads.
//! This module keeps only Go's `keepOrder` comparator, including unsigned
//! integer-handle ordering. The earlier disconnected Rust executor was
//! deliberately narrow, lacked Go's unique-index read, locking, and snapshot
//! contracts, and had no production callers after direct physical-plan
//! construction was wired.

use std::cmp::Ordering;

use crate::kv_table::TableHandle;

/// Go `slices.SortFunc(e.handles, less)` (:406) with the two comparators of
/// :377-405.
///
/// `unsigned_pk_is_handle` is Go's
/// `tblInfo.PKIsHandle && mysql.HasUnsignedFlag(tblInfo.GetPkColInfo().GetFlag())`:
/// the handle bits are the same, but `18446744073709551615` must sort ABOVE
/// `1`, not below it as the signed `-1` it would otherwise read as.
///
pub fn sort_handles_for_keep_order(
    handles: &mut Vec<TableHandle>,
    desc: bool,
    unsigned_pk_is_handle: bool,
) {
    let compare = |a: &TableHandle, b: &TableHandle| -> Ordering {
        let ordering = if unsigned_pk_is_handle {
            match (a, b) {
                (TableHandle::Int(left), TableHandle::Int(right)) => {
                    // Go's `uintComparator` panics on a non-int handle; an
                    // unsigned `PKIsHandle` cannot produce one.
                    (*left as u64).cmp(&(*right as u64))
                }
                _ => a.cmp(b),
            }
        } else {
            a.cmp(b)
        };
        if desc {
            ordering.reverse()
        } else {
            ordering
        }
    };
    handles.sort_by(compare);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn handles(values: &[i64]) -> Vec<TableHandle> {
        values.iter().map(|v| TableHandle::Int(*v)).collect()
    }

    fn values(handles: &[TableHandle]) -> Vec<i64> {
        handles
            .iter()
            .map(|h| h.int_value().expect("int handle"))
            .collect()
    }

    /// WRITTEN test (Go covers this through `testkit`): the signed comparator.
    #[test]
    fn keep_order_sorts_signed_handles_ascending() {
        let mut list = handles(&[5, -3, 1]);
        sort_handles_for_keep_order(&mut list, false, false);
        assert_eq!(values(&list), vec![-3, 1, 5]);
    }

    #[test]
    fn keep_order_desc_reverses_the_comparator() {
        let mut list = handles(&[5, -3, 1]);
        sort_handles_for_keep_order(&mut list, true, false);
        assert_eq!(values(&list), vec![5, 1, -3]);
    }

    /// The whole point of Go's `uintComparator`: `-1` is `MaxUint64` and must
    /// sort LAST, not first.
    #[test]
    fn an_unsigned_handle_sorts_by_its_bits_not_its_sign() {
        let mut list = handles(&[-1, 1, i64::MIN]);
        sort_handles_for_keep_order(&mut list, false, true);
        // As u64: 1 < 9223372036854775808 (i64::MIN) < 18446744073709551615.
        assert_eq!(values(&list), vec![1, i64::MIN, -1]);
    }

    #[test]
    fn an_unsigned_handle_sorted_signed_would_be_wrong() {
        let mut signed = handles(&[-1, 1]);
        sort_handles_for_keep_order(&mut signed, false, false);
        assert_eq!(values(&signed), vec![-1, 1]);
    }

    #[test]
    fn a_common_handle_sorts_by_its_encoded_bytes() {
        let mut list = vec![
            TableHandle::Common(vec![2, 0]),
            TableHandle::Common(vec![1, 9]),
        ];
        sort_handles_for_keep_order(&mut list, false, false);
        assert_eq!(list[0], TableHandle::Common(vec![1, 9]));
    }
}
