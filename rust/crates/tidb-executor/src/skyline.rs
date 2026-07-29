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

//! Which candidate access paths are worth costing at all: Go's
//! `skylinePruning` / `compareCandidates` (`pkg/planner/core/find_best_task.go`).
//!
//! # Why this is not a cost question
//!
//! Go does not cost every path it enumerates. A path is dropped BEFORE the
//! cost formula ever sees it when another path is at-least-as-good on every
//! dimension of a partial order and strictly better on one. The dimensions
//! are not costs; they are structural facts about the scan:
//!
//! * which COLUMNS the access conditions constrain
//!   (`accessCondsColMap`, compared with `util.CompareCol2Len`),
//! * whether the scan needs a row lookup back into the table
//!   (`compareIndexBack` over `IsSingleScan`, falling back to
//!   `indexCondsColMap` when both need one),
//! * how many of the access conditions are `=`/`IN` rather than ranges
//!   (`compareEqOrIn`).
//!
//! Two paths that both estimate one row cannot be separated by cost, but they
//! can be separated by this order -- which is the whole reason it runs first.
//!
//! # The dominance predicate, verbatim from `compareCandidates`
//!
//! ```text
//! predicateResult = accessResult + riskResult + eqOrInResult
//! totalSum        = accessResult + scanResult + matchResult + globalResult
//! leftDidNotLose  = predicateResult >= 0 && scanResult >= 0
//!                                        && matchResult >= 0 && globalResult >= 0
//! lhs wins  <=>  comparable && leftDidNotLose && totalSum > 0
//! ```
//!
//! and symmetrically for the right. `comparable` is the second return of
//! `CompareCol2Len` on both the access map and (when consulted) the index-cond
//! map: two candidates whose access columns are DIFFERENT sets of the same
//! size are incomparable, and neither prunes the other.
//!
//! # Which dimensions are LIVE here, and which are excluded BY NAME
//!
//! A partially ported partial order is not a slow plan, it is a WRONG plan: a
//! dimension silently read as zero can let a candidate prune one Go keeps.
//! So each dimension is either ported or named, with the direction of the
//! resulting error.
//!
//! LIVE:
//!
//! * `accessResult` -- [`compare_col_sets`] over
//!   [`Candidate::access_columns`]. Exact: this tier has no prefix indexes,
//!   so every `Col2Len` value is `types.UnspecifiedLength` and Go's
//!   `compareLength` is constant zero, which collapses `Col2Len` to a set.
//! * `scanResult` -- [`compare_index_back`] over `single_scan` (Go
//!   `IsSingleScan`, this crate's `is_covering`) then
//!   [`Candidate::index_columns`].
//! * `eqOrInResult` -- [`Candidate::eq_or_in_count`], from the detacher's own
//!   `=`/`IN` prefix walk.
//! * the pseudo-statistics rules -- `isCandidatesPseudo` / `comparePseudo`,
//!   driven by whether THIS index has a loaded histogram.
//! * Fix45132's 1000x `CountAfterAccess` rule.
//! * the `preferRange` post-filter, whose master switch
//!   `tidb_opt_prefer_range_scan` defaults ON (`vardef.DefOptPreferRangeScan`).
//!
//! EXCLUDED, each with its wrong-plan direction:
//!
//! * `matchResult` (`matchProperty`) is held at 0. This is EXACT, not an
//!   approximation, for the call this tier makes: the driver asks for a path
//!   under an EMPTY physical property (any `ORDER BY` is a `Sort` above the
//!   read), and `matchProperty` returns `PropNotMatched` for every path when
//!   `prop.IsSortItemEmpty()`, so Go's own `compareBool(false, false)` is 0
//!   too. The real gap is upstream and is an ENUMERATION gap, not a pruning
//!   one: this tier never explores the second, order-carrying invocation of
//!   `findBestTask`, so an index that would have satisfied an `ORDER BY`
//!   without a sort is not considered. That costs a sort, it does not
//!   mis-prune.
//! * `globalResult` (`compareGlobalIndex`) is held at 0. Also exact: it
//!   compares `Index.Global`, and this tier reads no global (partitioned)
//!   index -- partitioned tables are refused upstream by the session catalog.
//! * `riskResult` (`compareRiskRatio`) is held at 0, and this one is a REAL
//!   exclusion. Go derives `MaxCountAfterAccess`/`MinCountAfterAccess` in
//!   `deriveIndexPathStats`/`adjustCountAfterAccess` (`pkg/planner/core/stats.go`)
//!   from a risk-aware selectivity this rewrite has not ported. Reading it as
//!   0 is what Go itself computes only when neither path carries risk.
//!   Direction of the error: `riskResult` appears in `predicateResult`, and
//!   in the incomparable branch it is the ONLY thing that can produce a
//!   winner. Holding it at 0 therefore makes this port prune STRICTLY LESS
//!   than Go in the incomparable branch (no winner instead of one) -- a
//!   surviving candidate that Go dropped, which cost then judges. In the
//!   comparable branch it can go the other way: a high-risk candidate whose
//!   `riskResult` of -1 would have cancelled its `accessResult` of +1 will
//!   here have `predicateResult` +1 and can prune the safer path Go keeps.
//!   That is a wrong plan, not a slow one, and it is the known live risk of
//!   this unit.
//! * index-merge, multi-valued-index and TiFlash candidates never reach here
//!   -- `access_cost::enumerate_paths` excludes them by name -- so
//!   `isMVIndexPath`, `convergeIndexMergeCandidate` and the
//!   `StoreType == kv.TiFlash` guards have nothing to act on.
//! * `path.Forced` (`USE`/`FORCE INDEX`) and `ShouldPreferIndexMerge` are
//!   held at false: this tier parses no index hint, so no path is forced.
//!   Direction: a hinted query would be planned as if unhinted.
//! * the empty-range `TableDual` short-circuit
//!   (`if len(path.Ranges) == 0 { return []*candidatePath{{path}} }`) is
//!   ported in [`skyline_pruning`], because a contradictory `WHERE` must not
//!   let a full scan survive next to a provably empty index range.

use std::collections::BTreeSet;

/// The column offsets one condition list constrains, Go's `util.Col2Len`
/// with every value at `types.UnspecifiedLength`.
///
/// Go keys the map by `Column.UniqueID` and stores each column's index prefix
/// length. This tier declares no prefix index, so every length is
/// `UnspecifiedLength`, `compareLength` is constant 0, and the map degenerates
/// to a set of columns -- which is what is stored, rather than a map whose
/// values could never differ.
pub(crate) type ColSet = BTreeSet<usize>;

/// Go `util.CompareCol2Len`, with the length comparison collapsed (see
/// [`ColSet`]).
///
/// Returns `(order, comparable)`: `order` is 1 when the left is better, -1
/// when the right is, 0 when neither; `comparable` is false when the two
/// constrain genuinely different columns, which is what stops a bigger but
/// unrelated column set from counting as a win.
pub(crate) fn compare_col_sets(left: &ColSet, right: &ColSet) -> (i32, bool) {
    match left.len().cmp(&right.len()) {
        std::cmp::Ordering::Greater => (1, left.is_superset(right)),
        std::cmp::Ordering::Less => (-1, right.is_superset(left)),
        // Same size: Go walks `c2` and bails the moment a column is missing
        // from `c1`. With equal sizes that is exactly set equality.
        std::cmp::Ordering::Equal => (0, left == right),
    }
}

/// Go `compareBool`.
const fn compare_bool(left: bool, right: bool) -> i32 {
    match (left, right) {
        (true, false) => 1,
        (false, true) => -1,
        _ => 0,
    }
}

/// One enumerated access path, with the structural facts skyline pruning
/// reads off it -- Go's `candidatePath`.
#[derive(Clone, Debug)]
pub(crate) struct Candidate<T> {
    /// The path itself, opaque here: pruning never looks at its cost.
    pub(crate) path: T,
    /// Go `accessCondsColMap`.
    pub(crate) access_columns: ColSet,
    /// Go `indexCondsColMap`: the access conditions PLUS the index filters,
    /// over the index's full column list. Empty for a table path, which is
    /// what Go leaves it as (it only fills it in `getIndexCandidate`).
    pub(crate) index_columns: ColSet,
    /// Go `AccessPath.IsSingleScan`: the scan needs no lookup back into the
    /// table. Always true for a table path.
    pub(crate) single_scan: bool,
    /// Go `candidatePath.eqOrInCount`.
    pub(crate) eq_or_in_count: usize,
    /// Go `candidatePath.isFullRange`: the ranges cover the whole scan, so
    /// the path filters nothing on the way out of the store.
    pub(crate) full_range: bool,
    /// Go `AccessPath.CountAfterAccess`, the rows the access itself yields.
    pub(crate) count_after_access: f64,
    /// Whether THIS path's statistics are pseudo -- Go `isCandidatesPseudo`,
    /// which for an index path asks `ColAndIdxExistenceMap.HasAnalyzed(index)`
    /// rather than reusing the table's own flag.
    pub(crate) pseudo: bool,
    /// How many columns the index declares, Go's `len(path.Index.Columns)`,
    /// which only `isFullIndexMatch` reads. Zero for the table path.
    pub(crate) index_width: usize,
    /// Go's `len(path.Ranges) == 0`: the conditions are contradictory, so no
    /// row can qualify and every other candidate is pointless.
    pub(crate) empty_range: bool,
    /// Go `len(path.IndexFilters)` and `len(path.TableFilters)`: the residual
    /// conditions the index can and cannot evaluate before the row lookup.
    /// The `preferRange` filter reads their relative size.
    pub(crate) index_filter_count: usize,
    /// See [`Candidate::index_filter_count`].
    pub(crate) table_filter_count: usize,
}

/// Go `compareIndexBack`.
fn compare_index_back<T>(lhs: &Candidate<T>, rhs: &Candidate<T>) -> (i32, bool) {
    let result = compare_bool(lhs.single_scan, rhs.single_scan);
    if result == 0 && !lhs.single_scan {
        // Both read the table back, so the tie-break is how much filtering
        // the index side does before the lookup.
        return compare_col_sets(&lhs.index_columns, &rhs.index_columns);
    }
    (result, true)
}

/// Go `compareEqOrIn`.
fn compare_eq_or_in<T>(lhs: &Candidate<T>, rhs: &Candidate<T>) -> i32 {
    match lhs.eq_or_in_count.cmp(&rhs.eq_or_in_count) {
        std::cmp::Ordering::Greater => 1,
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
    }
}

/// Go `isFullIndexMatch`, for the non-DNF shape this tier builds: the index
/// has an `=`/`IN` prefix and its index-condition columns reach every column
/// the index declares.
fn is_full_index_match<T>(candidate: &Candidate<T>) -> bool {
    candidate.eq_or_in_count > 0 && candidate.index_columns.len() >= candidate.index_width
}

/// Go `comparePseudo`.
#[allow(clippy::too_many_arguments)]
fn compare_pseudo(
    lhs_pseudo: bool,
    rhs_pseudo: bool,
    lhs_full_match: bool,
    rhs_full_match: bool,
    eq_or_in_result: i32,
    lhs_eq_or_in: usize,
    rhs_eq_or_in: usize,
    prefer_range: bool,
) -> i32 {
    if !lhs_pseudo && lhs_eq_or_in > 0 && eq_or_in_result >= 0 {
        return 1;
    }
    if !rhs_pseudo && rhs_eq_or_in > 0 && eq_or_in_result <= 0 {
        return -1;
    }
    if prefer_range {
        if lhs_pseudo && eq_or_in_result > 0 && (lhs_eq_or_in > 1 || lhs_full_match) {
            return 1;
        }
        if rhs_pseudo && eq_or_in_result < 0 && (rhs_eq_or_in > 1 || rhs_full_match) {
            return -1;
        }
    }
    0
}

/// The session facts `compareCandidates` reads that are not per-candidate.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PruningContext {
    /// Go `statsTbl.HistColl.Pseudo`.
    pub(crate) table_pseudo: bool,
    /// Go `ds.TableStats.RowCount`, for the `preferRange` narrowing.
    pub(crate) row_count: f64,
    /// Go `GetAllowPreferRangeScan()`, `tidb_opt_prefer_range_scan`.
    pub(crate) prefer_range: bool,
    /// Whether the required property carries a row cap, Go's
    /// `prop.ExpectedCnt != math.MaxFloat64`, which disables Fix45132's rule
    /// because a `LIMIT` changes what "rows after access" means.
    pub(crate) has_limit: bool,
}

/// Go `fixcontrol.Fix45132`'s default threshold: one path's access row count
/// being this many times another's is a win on its own.
const RISK_ROW_RATIO_THRESHOLD: f64 = 1000.0;

/// Go `compareCandidates`, reduced to the dimensions this tier carries (see
/// the module doc for the full live/excluded list).
///
/// Returns `(order, winner_is_pseudo)` exactly as Go does: 1 when the left
/// prunes the right, -1 when the right prunes the left, 0 when neither.
pub(crate) fn compare_candidates<T>(
    lhs: &Candidate<T>,
    rhs: &Candidate<T>,
    context: &PruningContext,
) -> (i32, bool) {
    let (lhs_pseudo, rhs_pseudo) = (
        context.table_pseudo || lhs.pseudo,
        context.table_pseudo || rhs.pseudo,
    );
    // EXCLUDED, held at Go's own value for this call: see the module doc.
    let match_result = 0;
    let global_result = 0;
    let risk_result = 0;

    let (access_result, access_comparable) =
        compare_col_sets(&lhs.access_columns, &rhs.access_columns);
    let (scan_result, scan_comparable) = compare_index_back(lhs, rhs);
    let eq_or_in_result = compare_eq_or_in(lhs, rhs);

    let predicate_result = access_result + risk_result + eq_or_in_result;
    let total_sum = access_result + scan_result + match_result + global_result;

    // One index has statistics and the other does not: Go lets the analyzed
    // one win on a heuristic rather than on a cost it cannot trust.
    if (lhs_pseudo || rhs_pseudo)
        && !context.table_pseudo
        && (lhs.eq_or_in_count > 0 || rhs.eq_or_in_count > 0)
    {
        let pseudo_result = compare_pseudo(
            lhs_pseudo,
            rhs_pseudo,
            is_full_index_match(lhs),
            is_full_index_match(rhs),
            eq_or_in_result,
            lhs.eq_or_in_count,
            rhs.eq_or_in_count,
            context.prefer_range,
        );
        if pseudo_result > 0 && total_sum >= 0 {
            return (pseudo_result, lhs_pseudo);
        }
        if pseudo_result < 0 && total_sum <= 0 {
            return (pseudo_result, rhs_pseudo);
        }
    }

    // Fix45132: an access row count a thousand times smaller wins outright.
    // Guarded above 100 rows so that 0.01 : 10 is not called a 1000x win.
    if lhs.count_after_access > 100.0 && rhs.count_after_access > 100.0 && !context.has_limit {
        if lhs.count_after_access / rhs.count_after_access > RISK_ROW_RATIO_THRESHOLD
            && risk_result <= 0
        {
            return (-1, rhs_pseudo);
        }
        if rhs.count_after_access / lhs.count_after_access > RISK_ROW_RATIO_THRESHOLD
            && risk_result >= 0
        {
            return (1, lhs_pseudo);
        }
    }

    let left_did_not_lose =
        predicate_result >= 0 && scan_result >= 0 && match_result >= 0 && global_result >= 0;
    let right_did_not_lose =
        predicate_result <= 0 && scan_result <= 0 && match_result <= 0 && global_result <= 0;
    if !access_comparable || !scan_comparable {
        // Go's escape hatch here is driven entirely by `riskResult`, which
        // this tier holds at 0 (module doc). With `riskResult == 0` neither
        // of Go's two branches can fire, so the honest answer is "no winner"
        // -- this port prunes strictly less than Go here.
        return (0, false);
    }
    if left_did_not_lose && total_sum > 0 {
        return (1, lhs_pseudo);
    }
    if right_did_not_lose && total_sum < 0 {
        return (-1, rhs_pseudo);
    }
    (0, false)
}

/// Go `skylinePruning`: the candidates that survive the partial order, in
/// enumeration order.
pub(crate) fn skyline_pruning<T>(
    candidates: Vec<Candidate<T>>,
    context: &PruningContext,
) -> Vec<Candidate<T>> {
    let mut survivors: Vec<Candidate<T>> = Vec::with_capacity(candidates.len());
    let mut index_missing_stats = false;
    for candidate in candidates {
        // Go returns a lone candidate the moment one path proves no row
        // qualifies; every other path would read rows only to discard them.
        if candidate.empty_range {
            return vec![candidate];
        }
        let mut pruned = false;
        for i in (0..survivors.len()).rev() {
            let (result, winner_pseudo) = compare_candidates(&survivors[i], &candidate, context);
            if winner_pseudo {
                index_missing_stats = true;
            }
            if result == 1 {
                pruned = true;
                break;
            }
            if result == -1 {
                survivors.remove(i);
            }
        }
        if !pruned {
            survivors.push(candidate);
        }
    }

    // Go narrows the master switch before letting it drop full scans: with
    // real statistics on every path there is no reason to distrust the cost
    // model, so the preference only applies when some estimate is pseudo.
    let prefer_range = context.prefer_range
        && (index_missing_stats || context.table_pseudo || context.row_count < 1.0);
    if prefer_range && survivors.len() > 1 {
        // Go also keeps forced / TiFlash / global / MV paths unconditionally
        // here; none of those exist in this tier (module doc).
        let preferred = |candidate: &Candidate<T>| {
            let index_filters = candidate.eq_or_in_count > 0
                || candidate.table_filter_count < candidate.index_filter_count;
            // `prop.IsSortItemEmpty()` is true for this tier's only
            // invocation, so the property half of Go's condition holds.
            (candidate.single_scan || index_filters) && !candidate.full_range
        };
        if survivors.iter().any(preferred) {
            survivors.retain(preferred);
        }
    }
    survivors
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(
        access: &[usize],
        index: &[usize],
        single_scan: bool,
        eq_or_in_count: usize,
        index_width: usize,
    ) -> Candidate<&'static str> {
        Candidate {
            path: "path",
            access_columns: access.iter().copied().collect(),
            index_columns: index.iter().copied().collect(),
            single_scan,
            eq_or_in_count,
            full_range: false,
            count_after_access: 1.0,
            pseudo: false,
            index_width,
            empty_range: false,
            index_filter_count: 0,
            table_filter_count: 0,
        }
    }

    /// The full-scan candidate: no access conditions, always a single scan,
    /// and its range is the whole table.
    fn table_candidate() -> Candidate<&'static str> {
        let mut table = candidate(&[], &[], true, 0, 0);
        table.full_range = true;
        table
    }

    fn context() -> PruningContext {
        PruningContext {
            table_pseudo: false,
            row_count: 2000.0,
            prefer_range: true,
            has_limit: false,
        }
    }

    #[test]
    fn a_strict_superset_of_access_columns_dominates() {
        // `idx_cover(bucket, rare)` vs `idx_rare(rare)` under
        // `WHERE bucket = 1 AND rare = 7`, the case Go prunes before costing.
        let cover = candidate(&[1, 2], &[1, 2], false, 2, 2);
        let rare = candidate(&[2], &[2], false, 1, 1);
        assert_eq!(compare_candidates(&cover, &rare, &context()).0, 1);
        assert_eq!(compare_candidates(&rare, &cover, &context()).0, -1);
    }

    #[test]
    fn equal_sized_but_different_access_columns_are_incomparable() {
        // `idx_a(a)` vs `idx_b(b)` under `WHERE a = 1 AND b = 2`: Go's
        // `CompareCol2Len` reports "not comparable" and neither is pruned.
        let a = candidate(&[0], &[0], false, 1, 1);
        let b = candidate(&[1], &[1], false, 1, 1);
        assert_eq!(compare_candidates(&a, &b, &context()), (0, false));
        assert_eq!(compare_candidates(&b, &a, &context()), (0, false));
    }

    #[test]
    fn a_covering_scan_dominates_an_identical_non_covering_one() {
        // Identical access conditions, so only `compareIndexBack` separates
        // them: `SELECT bucket, rare FROM t WHERE bucket = 1` prunes
        // `idx_bucket` in favour of the covering `idx_cover`.
        let covering = candidate(&[1], &[1, 2], true, 1, 2);
        let lookup = candidate(&[1], &[1], false, 1, 1);
        assert_eq!(compare_candidates(&covering, &lookup, &context()).0, 1);
        assert_eq!(compare_candidates(&lookup, &covering, &context()).0, -1);
    }

    #[test]
    fn the_table_path_is_never_pruned_by_a_lookup_index() {
        // Go's stated invariant: a table scan is always a single scan, so its
        // `scanResult` of +1 cancels the index's `accessResult`, and
        // `totalSum` lands on 0.
        let table = table_candidate();
        let index = candidate(&[1], &[1], false, 1, 1);
        assert_eq!(compare_candidates(&table, &index, &context()).0, 0);
        assert_eq!(compare_candidates(&index, &table, &context()).0, 0);
    }

    #[test]
    fn a_covering_index_with_access_conditions_prunes_the_table_path() {
        // Both are single scans, so `scanResult` is 0 and the index's larger
        // access-column set carries `totalSum` on its own.
        let table = table_candidate();
        let covering = candidate(&[1], &[1, 2], true, 1, 2);
        assert_eq!(compare_candidates(&covering, &table, &context()).0, 1);
    }

    #[test]
    fn pruning_keeps_enumeration_order_and_drops_the_dominated() {
        let table = table_candidate();
        let bucket = candidate(&[1], &[1], false, 1, 1);
        let rare = candidate(&[2], &[2], false, 1, 1);
        let cover = candidate(&[1, 2], &[1, 2], false, 2, 2);
        let survivors = skyline_pruning(vec![table, bucket, rare, cover], &context());
        // The table path survives (see the invariant above); both narrow
        // indexes lose to the covering superset.
        assert_eq!(survivors.len(), 2);
        assert!(survivors[0].access_columns.is_empty());
        assert_eq!(survivors[1].access_columns.len(), 2);
    }

    #[test]
    fn an_empty_range_short_circuits_to_that_candidate_alone() {
        let table = table_candidate();
        let mut contradiction = candidate(&[1], &[1], false, 1, 1);
        contradiction.empty_range = true;
        let survivors = skyline_pruning(vec![table, contradiction], &context());
        assert_eq!(survivors.len(), 1);
        assert!(survivors[0].empty_range);
    }

    #[test]
    fn prefer_range_drops_the_full_scan_only_under_pseudo_statistics() {
        let table = table_candidate();
        let rare = candidate(&[2], &[2], false, 1, 1);
        let analyzed = context();
        assert_eq!(
            skyline_pruning(vec![table.clone(), rare.clone()], &analyzed).len(),
            2
        );
        let pseudo = PruningContext {
            table_pseudo: true,
            ..analyzed
        };
        let survivors = skyline_pruning(vec![table, rare], &pseudo);
        assert_eq!(survivors.len(), 1);
        assert!(!survivors[0].full_range);
    }

    #[test]
    fn a_thousandfold_smaller_access_count_wins_on_its_own() {
        let mut broad = candidate(&[1], &[1], false, 1, 1);
        broad.count_after_access = 200_000.0;
        let mut narrow = candidate(&[2], &[2], false, 1, 1);
        narrow.count_after_access = 150.0;
        // Incomparable access columns, so only Fix45132 can separate them.
        assert_eq!(compare_candidates(&broad, &narrow, &context()).0, -1);
        assert_eq!(compare_candidates(&narrow, &broad, &context()).0, 1);
    }
}
