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

//! Ports for `pkg/planner/core/rule/rule_partition_pruning_test.go`
//! at pinned Go revision `e2788410d8d696605e8cb002585877a063ccc909`.
//!
//! The three tests below reproduce Go's package-private partition-range
//! algebra line-for-line. The ordinary pruning path and all remaining original
//! cases are owned by `tidb_executor::partition_pruning`; the benchmark cases
//! are owned by the `tidb-executor` `partition_pruning` benchmark target.
//!
//! | Go function (`rule_partition_pruning_test.go`) | Rust test |
//! | --- | --- |
//! | `:35 TestCanBePrune` | `tidb_executor::partition_pruning::tests::range_pruning_matches_go_monotone_datetime_and_timestamp_cases` |
//! | `:85 TestPruneUseBinarySearchSigned` | [`prune_use_binary_search_signed_table`] (ported) |
//! | `:128 TestPruneUseBinarySearchUnSigned` | [`prune_use_binary_search_unsigned_table`] (ported) |
//! | `:243 TestPartitionRangeForExpr` | `tidb_executor::partition_pruning::tests::range_pruning_matches_go_partition_range_for_expr_matrix` |
//! | `:275 TestPartitionRangeOperation` | [`partition_range_or_intersection_union_simplify_operations`] (ported) |
//! | `:337–639` RANGE COLUMNS cases | executable `tidb_executor::partition_pruning::tests::range_columns_*` cases |
//! | `:663/:667/:671/:675/:679 BenchmarkRangeColumnsPruner{2,10,100,1000,8000}` | `tidb-executor --bench partition_pruning` |

/// GO PORT of `pkg/planner/core/rule/rule_partition_pruning_test.go:85
/// TestPruneUseBinarySearchSigned`.
///
/// Re-derived contract: `lessThan.Data = [-3,4,7,11,14,17,0]`,
/// `Maxvalue: true`, signed. The MAXVALUE slot (index 6) compares greater than
/// anything (`rule_partition_processor.go:936-938`). Every row pins
/// `PruneUseBinarySearch`'s `sort.Search` cutoff per operator: `=` needs the
/// first bound strictly greater than the constant, `<`/`<=` open at zero,
/// `>=`/`>` close at the partition count, `>` evaluates `c+1` (Go notes the
/// wrap-around tolerance at `rule_partition_processor.go:1749`), `isnull`
/// collapses to the first partition, and an unrecognized op keeps the full
/// range (`:1723-1766`). Expected bounds copied row-for-row from the Go table;
/// failure messages mirror `require.Equalf(t, ..., "fail = %d", i)`.
#[test]
fn prune_use_binary_search_signed_table() {
    let less_than = LessThanDataInt {
        data: &[-3, 4, 7, 11, 14, 17, 0],
        unsigned: false,
        maxvalue: true,
    };
    let cases: &[(&str, i64, usize, usize)] = &[
        ("=", 66, 6, 7),
        ("=", 14, 5, 6),
        ("=", 10, 3, 4),
        ("=", 3, 1, 2),
        ("=", -4, 0, 1),
        ("<", 66, 0, 7),
        ("<", 14, 0, 5),
        ("<", 10, 0, 4),
        ("<", 3, 0, 2),
        ("<", -4, 0, 1),
        (">=", 66, 6, 7),
        (">=", 14, 5, 7),
        (">=", 10, 3, 7),
        (">=", 3, 1, 7),
        (">=", -4, 0, 7),
        (">", 66, 6, 7),
        (">", 14, 5, 7),
        (">", 10, 4, 7),
        (">", 3, 2, 7),
        (">", 2, 1, 7),
        (">", -4, 1, 7),
        ("<=", 66, 0, 7),
        ("<=", 14, 0, 6),
        ("<=", 10, 0, 4),
        ("<=", 3, 0, 2),
        ("<=", -4, 0, 1),
        ("isnull", 0, 0, 1),
        ("illegal", 0, 0, 7),
    ];
    for (i, &(op, c, want_start, want_end)) in cases.iter().enumerate() {
        let (start, end) = prune_use_binary_search(
            less_than,
            DataForPrune {
                op,
                c,
                unsigned: false,
            },
        );
        assert_eq!(want_start, start, "fail = {i} (start)");
        assert_eq!(want_end, end, "fail = {i} (end)");
    }
}

/// GO PORT of `pkg/planner/core/rule/rule_partition_pruning_test.go:128
/// TestPruneUseBinarySearchUnSigned`.
///
/// Re-derived contract: `lessThan.Data = [4,7,11,14,17,0]`, `Unsigned: true`,
/// `Maxvalue: true`. Every `DataForPrune` leaves its own `Unsigned` unset, so
/// each comparison pits an UNSIGNED stored bound against a SIGNED constant —
/// `types.CompareInt` (`pkg/types/compare.go:90`) resolves any negative
/// constant below every real bound but above-or-at MAXVALUE, which is why
/// `= -3` still selects partition 0 instead of nothing. Rows copied verbatim
/// from the Go table (`:129-158`).
#[test]
fn prune_use_binary_search_unsigned_table() {
    let less_than = LessThanDataInt {
        data: &[4, 7, 11, 14, 17, 0],
        unsigned: true,
        maxvalue: true,
    };
    let cases: &[(&str, i64, usize, usize)] = &[
        ("=", 66, 5, 6),
        ("=", 14, 4, 5),
        ("=", 10, 2, 3),
        ("=", 3, 0, 1),
        ("=", -3, 0, 1),
        ("<", 66, 0, 6),
        ("<", 14, 0, 4),
        ("<", 10, 0, 3),
        ("<", 3, 0, 1),
        ("<", -3, 0, 1),
        (">=", 66, 5, 6),
        (">=", 14, 4, 6),
        (">=", 10, 2, 6),
        (">=", 3, 0, 6),
        (">=", -3, 0, 6),
        (">", 66, 5, 6),
        (">", 14, 4, 6),
        (">", 10, 3, 6),
        (">", 3, 1, 6),
        (">", 2, 0, 6),
        (">", -3, 0, 6),
        ("<=", 66, 0, 6),
        ("<=", 14, 0, 5),
        ("<=", 10, 0, 3),
        ("<=", 3, 0, 1),
        ("<=", -3, 0, 1),
        ("isnull", 0, 0, 1),
        ("illegal", 0, 0, 6),
    ];
    for (i, &(op, c, want_start, want_end)) in cases.iter().enumerate() {
        let (start, end) = prune_use_binary_search(
            less_than,
            DataForPrune {
                op,
                c,
                unsigned: false,
            },
        );
        assert_eq!(want_start, start, "fail = {i} (start)");
        assert_eq!(want_end, end, "fail = {i} (end)");
    }
}

/// GO PORT of `pkg/planner/core/rule/rule_partition_pruning_test.go:275
/// TestPartitionRangeOperation`.
///
/// Re-derived contract over `PartitionRangeOR` set algebra
/// (`rule_partition_processor.go:955-1052`):
/// - `IntersectionRange(start, end)` intersects every stored range with `[start,
///   end)` and DROPS empty results (:965-978);
/// - `Intersection` short-circuits when either side holds a single range by
///   delegating to the other side's `IntersectionRange` (:1018-1025), otherwise
///   distributes pairwise across the SHORTER side into the LONGER side's copy
///   and simplifies (:1028-1044);
/// - `Union` concatenates then `simplify()` sorts by Start and merges
///   overlapping/adjacent-touching ranges while KEEPING disjoint neighbours
///   intact (:985-1016).
/// All nine expectations copied from the three Go tables (:277-315).
#[test]
fn partition_range_or_intersection_union_simplify_operations() {
    type Or = Vec<PartitionRange>;
    fn or(ranges: &[(usize, usize)]) -> Or {
        ranges
            .iter()
            .map(|&(start, end)| PartitionRange { start, end })
            .collect()
    }
    fn rows(result: &[PartitionRange]) -> Vec<(usize, usize)> {
        result.iter().map(|r| (r.start, r.end)).collect()
    }
    fn want_rows(want: &[(usize, usize)]) -> Vec<(usize, usize)> {
        want.to_vec()
    }

    // IntersectionRange table (`rule_partition_pruning_test.go:277-286`).
    let test_intersection_range: &[(&[(usize, usize)], (usize, usize), &[(usize, usize)])] = &[
        (&[(0, 3), (6, 12)], (4, 7), &[(6, 7)]),
        (&[(0, 5)], (6, 7), &[]),
        (
            &[(0, 4), (6, 7), (8, 11)],
            (3, 9),
            &[(3, 4), (6, 7), (8, 9)],
        ),
    ];
    for (i, (input1, input2, want)) in test_intersection_range.iter().enumerate() {
        let result = intersection_range(&or(input1), input2.0, input2.1);
        assert_eq!(
            want_rows(want),
            rows(&result),
            "IntersectionRange fail = {i}"
        );
    }

    // Intersection table (`:288-303`).
    let test_intersection: &[(&[(usize, usize)], &[(usize, usize)], &[(usize, usize)])] = &[
        (&[(0, 3), (6, 12)], &[(4, 7)], &[(6, 7)]),
        (&[(4, 7)], &[(0, 3), (6, 12)], &[(6, 7)]),
        (
            &[(4, 7), (8, 10)],
            &[(0, 5), (6, 12)],
            &[(4, 5), (6, 7), (8, 10)],
        ),
    ];
    for (i, (input1, input2, want)) in test_intersection.iter().enumerate() {
        let result = intersection(or(input1), or(input2));
        assert_eq!(want_rows(want), rows(&result), "Intersection fail = {i}");
    }

    // Union table (`:305-314`).
    let test_union: &[(&[(usize, usize)], &[(usize, usize)], &[(usize, usize)])] = &[
        (&[(0, 1), (2, 7)], &[(3, 5)], &[(0, 1), (2, 7)]),
        (&[(2, 7)], &[(0, 3), (4, 12)], &[(0, 12)]),
        (&[(4, 7), (8, 10)], &[(0, 5)], &[(0, 7), (8, 10)]),
    ];
    for (i, (input1, input2, want)) in test_union.iter().enumerate() {
        let result = union(or(input1), or(input2));
        assert_eq!(want_rows(want), rows(&result), "Union fail = {i}");
    }
}

// ---------------------------------------------------------------------------
// Verbatim reproductions of the Go kernels the running tests above pin. Both
// bodies are mechanical transcriptions of the pinned Go revision's
// `pkg/planner/core/rule/rule_partition_processor.go` and
// `pkg/types/compare.go`. Rust's production owner consumes ranger intervals,
// so these package-private intermediate structures remain test-local.
// ---------------------------------------------------------------------------

/// Go `types.CompareInt` (`pkg/types/compare.go:90-113`): an integer
/// comparison that honours per-side signedness.
fn compare_int(arg0: i64, arg0_unsigned: bool, arg1: i64, arg1_unsigned: bool) -> i32 {
    let ordering_to_int = |o: std::cmp::Ordering| match o {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    };
    match (arg0_unsigned, arg1_unsigned) {
        (true, true) => ordering_to_int((arg0 as u64).cmp(&(arg1 as u64))),
        // Go: `arg1 < 0 || uint64(arg0) > math.MaxInt64 => 1`.
        (true, false) => {
            if arg1 < 0 || (arg0 as u64) > i64::MAX as u64 {
                1
            } else {
                ordering_to_int(arg0.cmp(&arg1))
            }
        }
        // Go: `arg0 < 0 || uint64(arg1) > math.MaxInt64 => -1`.
        (false, true) => {
            if arg0 < 0 || (arg1 as u64) > i64::MAX as u64 {
                -1
            } else {
                ordering_to_int(arg0.cmp(&arg1))
            }
        }
        (false, false) => ordering_to_int(arg0.cmp(&arg1)),
    }
}

/// Go `LessThanDataInt` (`pkg/planner/core/rule/rule_partition_processor.go:923-927`).
#[derive(Clone, Copy)]
struct LessThanDataInt<'a> {
    data: &'a [i64],
    unsigned: bool,
    maxvalue: bool,
}

impl LessThanDataInt<'_> {
    /// Go `Length()` (:930-932).
    fn length(self) -> usize {
        self.data.len()
    }

    /// Go `compare(ith, v, unsigned)` (:934-941): the trailing MAXVALUE slot
    /// always compares greater.
    fn compare(self, ith: usize, v: i64, v_unsigned: bool) -> i32 {
        if ith == self.length() - 1 && self.maxvalue {
            return 1;
        }
        compare_int(self.data[ith], self.unsigned, v, v_unsigned)
    }
}

/// Go `DataForPrune` (`rule_partition_processor.go:1571-1575`): `f(x) op const`.
struct DataForPrune<'a> {
    /// One of `"="`, `"<"`, `"<="`, `">"`, `">="`, `"isnull"` (Go `ast.X`),
    /// or anything else — which takes the default arm.
    op: &'a str,
    c: i64,
    unsigned: bool,
}

/// Go's `sort.Search(n, f)`: binary search for the smallest i in [0, n) where
/// `f(i)` holds, else n. Reproduced with the same bisection loop so
/// non-monotone predicates would misbehave identically (they do not occur on
/// the monotone bound table).
fn go_sort_search(n: usize, f: impl Fn(usize) -> bool) -> usize {
    let (mut i, mut j) = (0usize, n);
    while i < j {
        let h = (i + j) >> 1;
        if !f(h) {
            i = h + 1;
        } else {
            j = h;
        }
    }
    i
}

/// GO PORT of `PruneUseBinarySearch`
/// (`rule_partition_processor.go:1721-1762`).
fn prune_use_binary_search(
    less_than: LessThanDataInt<'_>,
    data: DataForPrune<'_>,
) -> (usize, usize) {
    let length = less_than.length();
    let (start, end) = match data.op {
        "=" => {
            let pos = go_sort_search(length, |i| less_than.compare(i, data.c, data.unsigned) > 0);
            (pos, pos + 1)
        }
        "<" => {
            let pos = go_sort_search(length, |i| less_than.compare(i, data.c, data.unsigned) >= 0);
            (0, pos + 1)
        }
        ">=" => {
            let pos = go_sort_search(length, |i| less_than.compare(i, data.c, data.unsigned) > 0);
            (pos, length)
        }
        // Go deliberately adds 1 to `data.C` knowing it may wrap
        // (:1753); reproduce with wrapping add.
        ">" => {
            let pos = go_sort_search(length, |i| {
                less_than.compare(i, data.c.wrapping_add(1), data.unsigned) > 0
            });
            (pos, length)
        }
        "<=" => {
            let pos = go_sort_search(length, |i| less_than.compare(i, data.c, data.unsigned) > 0);
            (0, pos + 1)
        }
        "isnull" => (0, 1),
        _ => (0, length),
    };
    let end = if end > length { length } else { end };
    (start, end)
}

/// Go `PartitionRange` (`rule_partition_processor.go:944-947`): `[start, end)`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PartitionRange {
    start: usize,
    end: usize,
}

/// Go `intersectionRange(start, end, newStart, newEnd)`
/// (:1047-1052): the raw half-open interval overlap.
fn overlap_of(start: usize, end: usize, new_start: usize, new_end: usize) -> (usize, usize) {
    (start.max(new_start), end.min(new_end))
}

/// Go `PartitionRangeOR.IntersectionRange`
/// (:965-978): intersect each member, drop empties.
fn intersection_range(or: &[PartitionRange], start: usize, end: usize) -> Vec<PartitionRange> {
    or.iter()
        .filter_map(|r1| {
            let (s, e) = overlap_of(r1.start, r1.end, start, end);
            (e > s).then_some(PartitionRange { start: s, end: e })
        })
        .collect()
}

/// Go `PartitionRangeOR.simplify()` (:990-1016): sort by Start, merge
/// overlapping members.
fn simplify(mut or: Vec<PartitionRange>) -> Vec<PartitionRange> {
    if or.is_empty() {
        return or;
    }
    or.sort_by_key(|r| r.start);
    let mut res: Vec<PartitionRange> = vec![or[0]];
    for curr in or.into_iter().skip(1) {
        let last = res.last_mut().expect("res starts non-empty");
        if curr.start > last.end {
            res.push(curr);
        } else if curr.end > last.end {
            last.end = curr.end;
        }
    }
    res
}

/// Go `PartitionRangeOR.Union` (:985-988).
fn union(or: Vec<PartitionRange>, x: Vec<PartitionRange>) -> Vec<PartitionRange> {
    let mut combined = or;
    combined.extend(x);
    simplify(combined)
}

/// Go `PartitionRangeOR.Intersection` (:1018-1045): single-sided delegation,
/// else distribute the longer side across the shorter side member-by-member
/// and simplify.
fn intersection(or: Vec<PartitionRange>, x: Vec<PartitionRange>) -> Vec<PartitionRange> {
    if or.len() == 1 {
        return intersection_range(&x, or[0].start, or[0].end);
    }
    if x.len() == 1 {
        return intersection_range(&or, x[0].start, x[0].end);
    }
    // Rename so the LONGER side is iterated against the shorter side, as Go
    // does at :1033-1037 (`if or.Len() > x.Len() { x, y = or, x }`).
    let (longer, shorter) = if or.len() > x.len() {
        (&or, &x)
    } else {
        (&x, &or)
    };
    let mut res: Vec<PartitionRange> = Vec::with_capacity(shorter.len());
    for r in shorter {
        res.extend(intersection_range(longer, r.start, r.end));
    }
    simplify(res)
}
