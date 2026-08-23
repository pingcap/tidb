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
//!   [`Candidate::access_columns`], with Go's `compareLength` DROPPED; see
//!   [`ColSet`] for what that costs and why it is never a wrong answer.
//! * `scanResult` -- [`compare_index_back`] over `single_scan` (Go
//!   `IsSingleScan`, this crate's `is_covering`) then
//!   [`Candidate::index_columns`].
//! * `eqOrInResult` -- [`Candidate::eq_or_in_count`], from the detacher's own
//!   `=`/`IN` prefix walk.
//! * `riskResult` -- [`compare_risk_ratio`] over the estimator's own
//!   `MaxCountAfterAccess`/`MinCountAfterAccess` (see that function, and
//!   "How much `riskResult` can actually change" below).
//! * the pseudo-statistics rules -- `isCandidatesPseudo` / `comparePseudo`,
//!   driven by whether THIS index has a loaded histogram.
//! * Fix45132's 1000x `CountAfterAccess` rule.
//! * the `preferRange` post-filter, whose master switch
//!   `tidb_opt_prefer_range_scan` defaults ON (`vardef.DefOptPreferRangeScan`).
//! * `matchResult` (`matchProperty`) -- `compareBool` over
//!   [`Candidate::match_property`]. The statement's own `ORDER BY` re-enters
//!   path selection (`crate::driver::access::best_single_table_access_path`
//!   marks each candidate and prices the non-matching ones under the Sort
//!   enforcer), and the dimension exists in Go precisely so a non-matching
//!   path cannot DOMINATE a matching one there -- without it the covering
//!   `IndexRangeScan` skyline-pruned the ordered table scan before the
//!   enforcer's price was ever compared. Its two other regimes are exact:
//!   under an EMPTY property Go's `matchProperty` answers `PropNotMatched`
//!   for every path, and every constructor leaves the field `false`, so
//!   `compareBool(false, false)` is 0; a join leaf's required order is
//!   applied as a FILTER over the enumeration before pruning, so every
//!   survivor compares `compareBool(true, true)` = 0 just the same.
//!
//! # How much `riskResult` can actually change
//!
//! It was carried as this module's known wrong-plan risk while it was held at
//! zero. Porting it settled how wide that risk really was, and the answer is
//! narrow -- worth writing down, because "we ported it" is not the same
//! statement as "it was dangerous".
//!
//! In the COMPARABLE branch, `riskResult` can only overturn a decision when
//! `accessResult + eqOrInResult == 0` while `totalSum > 0`. Enumerate the
//! three ways that can happen:
//!
//! * `accessResult` +1 with `eqOrInResult` -1 is IMPOSSIBLE here. The
//!   detacher's access conditions are an `=`/`IN` prefix plus at most one
//!   trailing range, so `eqOrInCount >= |accessColumns| - 1` always; a
//!   candidate with strictly more access columns can never have strictly
//!   fewer `=`/`IN` conditions.
//! * `accessResult` -1 with `eqOrInResult` +1 needs `scanResult` of +2 to keep
//!   `totalSum` positive, and `scanResult` is bounded by 1.
//! * `accessResult` 0 with `eqOrInResult` 0 and `scanResult` +1 is the ONE
//!   real shape: same access columns, same `=`/`IN` count, one path covering.
//!   It is the case
//!   [`tests::a_risky_covering_index_no_longer_prunes_the_certain_lookup_go_keeps`]
//!   pins, on numbers a real cluster produced.
//!
//! Even in that shape the CHOSEN path does not move in this tier. Go's first
//! risk branch requires the safer path's expected count to be no larger, so
//! the candidate that survives because of `riskResult` is a NON-COVERING path
//! with no fewer rows than the covering one it used to lose to -- and a double
//! read costs about 1965 per row here against a covering scan's 45, so the
//! covering path still wins on cost by a wide margin. For the survivor to win
//! the covering path would have to estimate more than 40x as many rows for the
//! SAME access conditions, which no single estimator does.
//!
//! So the honest summary: the dimension was a real hole in the partial order
//! and is now closed, the live differential confirms it fires in both branches
//! on real statistics, and no chosen path changed. It is a correctness fix
//! against future divergence -- a different cost model, a different index
//! shape, or the covering-index enumeration gap being closed -- rather than a
//! bug that was already biting.
//!
//! EXCLUDED, each with its wrong-plan direction:
//!
//! * `getPseudoRowCountWithPartialStats` -- the one estimator branch that can
//!   report a `MaxEst` above its `Est` while the index itself is UNANALYZED
//!   (`row_count_index.go`'s `IndexStatsIsInvalid` arm, when the index's
//!   COLUMNS do have histograms). This tier's [`super::access_cost`] falls
//!   back to the plain pseudo formula there, whose three estimates are equal,
//!   so such a path reports no risk. Direction: this port sees LESS risk than
//!   Go on a table with analyzed columns and an unanalyzed index, so
//!   `riskResult` reads 0 where Go reads a winner -- it prunes less, not more.
//! * `globalResult` (`compareGlobalIndex`) is held at 0. Also exact: it
//!   compares `Index.Global`, and this tier has no global index to compare:
//!   a HASH-partitioned table's indexes are keyed by the TABLE id, which is
//!   sound only because every unique key covers the partitioning columns
//!   (see `crate::ddl::table_partition`), and no index carries a
//!   `GLOBAL` flag for the planner to read.
//! * index-merge, multi-valued-index and TiFlash candidates never reach here
//!   -- `access_cost::enumerate_paths` excludes them by name -- so
//!   `isMVIndexPath`, `convergeIndexMergeCandidate` and the
//!   `StoreType == kv.TiFlash` guards have nothing to act on.
//! * `ShouldPreferIndexMerge` is held at false: there is no index-merge path
//!   here to prefer.
//! * `path.Forced` (`USE`/`FORCE INDEX`) is LIVE. It enters the candidate in
//!   `access_cost::enumerate_paths`, survives here as [`Candidate::forced`],
//!   and bypasses the `preferRange` post-filter exactly as it does in Go. A
//!   forced full-index scan must not disappear merely because another path is
//!   a range scan: the hint, not this heuristic, owns that decision.
//! * the empty-range `TableDual` short-circuit
//!   (`if len(path.Ranges) == 0 { return []*candidatePath{{path}} }`) is
//!   ported in [`skyline_pruning`], because a contradictory `WHERE` must not
//!   let a full scan survive next to a provably empty index range. Go's
//!   SECOND half of the same rule -- `findBestTask`'s `if len(path.Ranges)
//!   == 0` returning a `PhysicalTableDual` for the chosen path -- is
//!   `crate::plan_trace::PlanTrace::empty_range_table_dual`, which is where
//!   the PLAN stops printing a scan for a path that reads nothing.

use std::collections::BTreeSet;

/// The column offsets one condition list constrains: Go's `util.Col2Len`
/// with the LENGTH half dropped.
///
/// Go keys the map by `Column.UniqueID` and stores each column's index prefix
/// length, and `CompareCol2Len` prefers the candidate whose lengths are
/// longer (`compareLength`). Prefix indexes exist in this tier now, so those
/// lengths are no longer all `UnspecifiedLength` and the collapse is a real
/// approximation rather than an identity.
///
/// NOT MODELLED (documented), and bounded: it can only make two candidates
/// that constrain the SAME columns with DIFFERENT prefix lengths look
/// equally good, so one of them survives pruning where Go would have pruned
/// it. Both are still correct access paths -- every range this crate builds
/// over a prefix key part is a superset the residual `WHERE` filters, and the
/// row lookup supplies the whole value either way -- so the cost of the gap
/// is a scan that reads more entries than Go's, never a different answer.
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
    /// Go `AccessPath.MaxCountAfterAccess`: the WORST case the same estimator
    /// admits for [`Candidate::count_after_access`] -- not a second estimate
    /// of the expected count but an upper bound on it, so the two together
    /// say how much the expected count can be trusted. Zero means the
    /// estimator identified no risk, which is Go's own default.
    pub(crate) max_count_after_access: f64,
    /// Go `AccessPath.MinCountAfterAccess`: the matching LOWER bound, zero
    /// when no risk was identified.
    pub(crate) min_count_after_access: f64,
    /// Go `AccessPath.CountAfterIndex`: the rows left after the index's OWN
    /// filters and before the row lookup. Equal to
    /// [`Candidate::count_after_access`] when the index evaluates no filter.
    pub(crate) count_after_index: f64,
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
    /// Go `AccessPath.Forced`. The `preferRange` post-filter keeps these
    /// candidates unconditionally; otherwise a forced whole-index scan could
    /// disappear after hint filtering had already removed the table path.
    pub(crate) forced: bool,
    /// Go `candidatePath.matchPropResult.Matched()`: whether this path's own
    /// walk already delivers the required sort property. Under an EMPTY
    /// property Go's `matchProperty` answers `PropNotMatched` for every path,
    /// so `false` for every candidate is that call's exact value; the
    /// single-table chooser sets it per candidate when the statement's own
    /// `ORDER BY` re-enters path selection
    /// (`crate::driver::access::best_single_table_access_path`).
    pub(crate) match_property: bool,
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

/// Whether `TIDB_RUST_SKYLINE_PROBE` was set when this process started.
///
/// Pruning decides which candidates the cost model never sees, so when a plan
/// is wrong the survivors are exactly what you need and exactly what `EXPLAIN`
/// cannot show: it prints the ONE path that won. This is the only way to read
/// the partial order's inputs off a running node, and it is what the live
/// access-path differential uses to check that a dimension fires at all.
static PROBE: std::sync::LazyLock<bool> =
    std::sync::LazyLock::new(|| std::env::var_os("TIDB_RUST_SKYLINE_PROBE").is_some());

/// One `compareCandidates` call's inputs and its risk verdict, for [`PROBE`].
fn probe<T>(lhs: &Candidate<T>, rhs: &Candidate<T>, risk_result: i32) {
    let describe = |candidate: &Candidate<T>| {
        format!(
            "acc={:?} idx={:?} single={} eq={} cnt={:.4} min={:.4} max={:.4} afterIndex={:.4}",
            candidate.access_columns,
            candidate.index_columns,
            candidate.single_scan,
            candidate.eq_or_in_count,
            candidate.count_after_access,
            candidate.min_count_after_access,
            candidate.max_count_after_access,
            candidate.count_after_index,
        )
    };
    eprintln!(
        "SKYLINE lhs({}) rhs({}) risk={risk_result}",
        describe(lhs),
        describe(rhs)
    );
}

/// Go `compareRiskRatio` (`pkg/planner/core/find_best_task.go`).
///
/// The dimension asks which candidate's row estimate can be trusted more, not
/// which one is smaller. `MaxCountAfterAccess` is a MAXIMUM the same estimator
/// admits for the same ranges -- from an in-bucket skew bound or an
/// out-of-range extrapolation -- so `max / est` is how far the estimate can be
/// wrong in the direction that hurts: a path whose 3-row estimate could really
/// be 300 rows is riskier than one whose 30-row estimate could only be 33,
/// even though 3 < 30.
///
/// A ratio of 0 means the estimator flagged no risk at all, and 0 is therefore
/// the LOWEST risk -- which is why Go compares the ratios rather than the
/// counts, and why a path with no identified risk beats one with any.
///
/// The lower-risk side still has to be defensible on rows before it wins:
/// either its expected count is no larger (the first branch), or its whole
/// [min, max] interval sits lower AND its own lower bound or post-index count
/// is no larger (the second). Without that a tiny-but-certain estimate would
/// prune a large-but-correct one.
fn compare_risk_ratio<T>(lhs: &Candidate<T>, rhs: &Candidate<T>) -> i32 {
    let ratio = |candidate: &Candidate<T>| {
        if candidate.max_count_after_access > candidate.count_after_access
            && candidate.count_after_access > 0.0
        {
            candidate.max_count_after_access / candidate.count_after_access
        } else {
            0.0
        }
    };
    let (lhs_ratio, rhs_ratio) = (ratio(lhs), ratio(rhs));
    let sum =
        |candidate: &Candidate<T>| candidate.count_after_access + candidate.max_count_after_access;
    // Go writes the two directions out longhand; they are the same test with
    // the sides exchanged, so it is written once and applied twice.
    let wins = |low: &Candidate<T>, high: &Candidate<T>| {
        low.count_after_access <= high.count_after_access
            || (sum(low) < sum(high)
                && low.min_count_after_access > 0.0
                && (low.min_count_after_access <= high.min_count_after_access
                    || low.count_after_index <= high.count_after_index))
    };
    if lhs_ratio < rhs_ratio && wins(lhs, rhs) {
        return 1;
    }
    if rhs_ratio < lhs_ratio && wins(rhs, lhs) {
        return -1;
    }
    0
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
    /// Whether the required property carries a SORT, Go's
    /// `!prop.IsSortItemEmpty()`. The `preferRange` post-filter reads it:
    /// under an ordered property a range path counts as "preferred" only when
    /// it also MATCHES the order (`prop.IsSortItemEmpty() ||
    /// c.matchPropResult.Matched()`), so an unordered range scan cannot
    /// evict the ordered full scan the property will choose.
    pub(crate) has_sort_property: bool,
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
    // Go: `compareBool(lhs.matchPropResult.Matched(), rhs.matchPropResult
    // .Matched())`. Every caller that carries no required order leaves both
    // sides `false`, which keeps this at Go's own 0 for an empty property.
    let match_result = compare_bool(lhs.match_property, rhs.match_property);
    // EXCLUDED, held at Go's own value for this call: see the module doc.
    let global_result = 0;

    let risk_result = compare_risk_ratio(lhs, rhs);
    if *PROBE {
        probe(lhs, rhs, risk_result);
    }
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
        // Different combinations of access columns, so no dominance argument
        // is available -- but a clear risk difference still separates them.
        // `predicateResult` already carries `riskResult`, so it must beat 1
        // (not 0) for the access-column win to have survived the risk.
        if risk_result > 0 && left_did_not_lose && total_sum >= 0 && predicate_result > 1 {
            return (1, lhs_pseudo);
        }
        if risk_result < 0 && right_did_not_lose && total_sum <= 0 && predicate_result < -1 {
            return (-1, rhs_pseudo);
        }
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
        // Go keeps forced / TiFlash / global / MV paths unconditionally here.
        // TiFlash, global and MV paths do not exist in this tier; forced paths
        // do, and the bypass is semantically significant after hint filtering.
        let preferred = |candidate: &Candidate<T>| {
            if candidate.forced {
                return true;
            }
            let index_filters = candidate.eq_or_in_count > 0
                || candidate.table_filter_count < candidate.index_filter_count;
            // Go: `(c.path.IsSingleScan || indexFilters) &&
            // (prop.IsSortItemEmpty() || c.matchPropResult.Matched())`.
            (candidate.single_scan || index_filters)
                && (!context.has_sort_property || candidate.match_property)
                && !candidate.full_range
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
            // Go's own no-risk state: the estimator reported one number, so
            // its min and max equal it and the risk ratio is 0.
            max_count_after_access: 1.0,
            min_count_after_access: 1.0,
            count_after_index: 1.0,
            pseudo: false,
            index_width,
            empty_range: false,
            index_filter_count: 0,
            table_filter_count: 0,
            forced: false,
            match_property: false,
        }
    }

    /// Sets one candidate's whole estimate at once: the expected count and
    /// the worst case the estimator admits for it.
    ///
    /// It is written as one call on purpose. Setting `count_after_access`
    /// without moving `count_after_index` leaves the two describing different
    /// row counts, and `compareRiskRatio`'s second branch reads
    /// `count_after_index` -- so a test that moved only one of them could pass
    /// on a fixture no estimator would ever produce.
    fn with_estimate(candidate: &mut Candidate<&'static str>, count: f64, max: f64) {
        candidate.count_after_access = count;
        candidate.max_count_after_access = max;
        // The estimators this tier has report a minimum equal to the expected
        // count unless `adjustCountAfterAccess` moved it, and no index filter
        // narrows these fixtures, so the post-index count is the access count.
        candidate.min_count_after_access = count;
        candidate.count_after_index = count;
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
            has_sort_property: false,
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
    fn prefer_range_never_drops_a_forced_full_index_scan() {
        // Keep the pair skyline-incomparable before the post-filter: the
        // forced index covers while the range index needs a row lookup, so
        // Go's scan and access dimensions cancel. The post-filter is the only
        // rule under test.
        let mut forced_full_index = candidate(&[], &[], true, 0, 1);
        forced_full_index.full_range = true;
        forced_full_index.forced = true;
        let ranged_index = candidate(&[1], &[1], false, 1, 1);
        let pseudo = PruningContext {
            table_pseudo: true,
            ..context()
        };

        let survivors = skyline_pruning(vec![forced_full_index, ranged_index], &pseudo);

        assert_eq!(survivors.len(), 2);
        assert!(survivors.iter().any(|candidate| candidate.forced));
        assert!(survivors.iter().any(|candidate| !candidate.full_range));
    }

    /// A path whose estimate could be ten times larger than it says is
    /// riskier than one whose estimate is bigger but certain, and Go's
    /// `compareRiskRatio` says so.
    #[test]
    fn no_identified_risk_beats_any_identified_risk() {
        let mut certain = candidate(&[1], &[1], false, 1, 1);
        with_estimate(&mut certain, 30.0, 30.0);
        let mut risky = candidate(&[2], &[2], false, 1, 1);
        with_estimate(&mut risky, 3.0, 300.0);
        // The safer path here also has MORE rows, and Go's first branch
        // requires the safer side to be no larger -- so neither wins, even
        // though 3 rows that could be 300 is the worse bet. Go's second
        // branch does not rescue it either: it needs the safer side's own
        // lower bound or post-index count to be no larger, and 30 is not.
        // This is the shape that keeps the dimension from overruling the row
        // counts, and it is why `riskResult` is so much narrower in practice
        // than the arithmetic suggests.
        assert_eq!(compare_risk_ratio(&certain, &risky), 0);
        assert_eq!(compare_risk_ratio(&risky, &certain), 0);

        // Give the safer path the smaller expected count as well and the
        // dimension does fire, through Go's first branch.
        with_estimate(&mut certain, 3.0, 3.0);
        with_estimate(&mut risky, 30.0, 3000.0);
        assert_eq!(compare_risk_ratio(&certain, &risky), 1);
        assert_eq!(compare_risk_ratio(&risky, &certain), -1);
    }

    /// The regression this dimension exists to prevent, on the numbers a real
    /// cluster produced.
    ///
    /// `risky(id PK, v, w, KEY idx_v(v), KEY idx_vw(v, w))`, 2000 rows, `v`
    /// taking 100 distinct values, `ANALYZE TABLE risky WITH 4 BUCKETS` so
    /// each bucket is 500 rows deep. Then
    /// `SELECT v, w FROM risky WHERE v BETWEEN 40 AND 42` gives both paths the
    /// same access column `{v}`, the same `=`/`IN` count of 0, and the same
    /// expected count of 60 -- but the range lands inside ONE bucket of the
    /// `(v, w)` histogram, so `idx_vw`'s worst case is that whole 500-row
    /// bucket while `idx_v`'s estimate is exact. Read off the live node by
    /// `rust/scripts/run-realtikv-access-path.sh` with
    /// `TIDB_RUST_SKYLINE_PROBE=1`:
    ///
    /// ```text
    /// SKYLINE lhs(acc={1} idx={1} single=false eq=0 cnt=60 min=60 max=60)
    ///         rhs(acc={1} idx={1} single=true  eq=0 cnt=60 min=60 max=500) risk=1
    /// ```
    ///
    /// `accessResult` is 0 and `scanResult` is +1 for the covering `idx_vw`,
    /// so `totalSum` is -1 from the left's point of view and WITHOUT
    /// `riskResult` the covering path prunes `idx_v` outright. With it,
    /// `predicateResult` is +1, `rightDidNotLose` is false, and neither prunes
    /// the other -- which is what Go does with these same numbers, because the
    /// 500 is Go's own bucket.
    ///
    /// This is the whole shape the dimension can flip in the comparable
    /// branch; see [`compare_risk_ratio`] and the module doc for why there is
    /// no other.
    #[test]
    fn a_risky_covering_index_no_longer_prunes_the_certain_lookup_go_keeps() {
        // `index_columns` is `{v}` for both: `idx_vw`'s `w` is not in the
        // access conditions and not an index filter, so Go's
        // `indexCondsColMap` holds only `v` -- which is why `compareIndexBack`
        // falls through to `IsSingleScan` and nothing else separates them.
        let mut covering = candidate(&[1], &[1], true, 0, 2);
        with_estimate(&mut covering, 60.0, 500.0);
        let mut lookup = candidate(&[1], &[1], false, 0, 1);
        with_estimate(&mut lookup, 60.0, 60.0);

        assert_eq!(compare_risk_ratio(&lookup, &covering), 1);
        // Neither prunes the other, in either argument order.
        assert_eq!(
            compare_candidates(&covering, &lookup, &context()),
            (0, false)
        );
        assert_eq!(
            compare_candidates(&lookup, &covering, &context()),
            (0, false)
        );
        assert_eq!(
            skyline_pruning(vec![lookup.clone(), covering.clone()], &context()).len(),
            2
        );

        // Remove ONLY the risk and the covering path dominates again, exactly
        // as this port did before the dimension was live. That is what makes
        // the assertions above about RISK rather than about some unrelated
        // change to the order.
        let mut certain = covering.clone();
        with_estimate(&mut certain, 60.0, 60.0);
        assert_eq!(compare_candidates(&certain, &lookup, &context()).0, 1);
        assert_eq!(skyline_pruning(vec![lookup, certain], &context()).len(), 1);
    }

    /// Go's incomparable-branch escape hatch, which only `riskResult` can
    /// open: two candidates constraining different columns are normally left
    /// alone, but a clear risk difference plus a `predicateResult` big enough
    /// to have survived it names a winner.
    ///
    /// The numbers are the live ones again, from the same fixture as
    /// [`a_risky_covering_index_no_longer_prunes_the_certain_lookup_go_keeps`]
    /// under `SELECT * FROM risky WHERE v = 40 AND w = 740
    /// AND c BETWEEN 400 AND 402`:
    ///
    /// ```text
    /// SKYLINE lhs(acc={1,2} idx={1,2} single=false eq=2 cnt=1 min=1 max=1)
    ///         rhs(acc={3}   idx={3}   single=false eq=0 cnt=3 min=3 max=500) risk=1
    /// ```
    ///
    /// Here `riskResult` ADDS pruning this port previously did not do: with it
    /// at zero, `idx_c` survived and the cost model judged it. It cannot make
    /// the port prune something Go keeps -- the arithmetic is Go's, on Go's
    /// own bucket count.
    #[test]
    fn a_clear_risk_difference_separates_otherwise_incomparable_candidates() {
        // `idx_vw(v, w)` vs `idx_c(c)`: neither access-column set contains the
        // other, so `CompareCol2Len` reports incomparable.
        let mut safe = candidate(&[1, 2], &[1, 2], false, 2, 2);
        with_estimate(&mut safe, 1.0, 1.0);
        let mut risky = candidate(&[3], &[3], false, 0, 1);
        with_estimate(&mut risky, 3.0, 500.0);
        // riskResult +1, accessResult +1, eqOrInResult +2: predicateResult 4,
        // which clears Go's "> 1" bar.
        assert_eq!(compare_candidates(&safe, &risky, &context()).0, 1);
        assert_eq!(compare_candidates(&risky, &safe, &context()).0, -1);
        assert_eq!(
            skyline_pruning(vec![risky.clone(), safe.clone()], &context()).len(),
            1
        );
        // Without the risk the two are simply incomparable and both survive,
        // which is what this port did before.
        with_estimate(&mut risky, 3.0, 3.0);
        assert_eq!(compare_candidates(&safe, &risky, &context()), (0, false));
        assert_eq!(skyline_pruning(vec![risky, safe], &context()).len(), 2);
    }

    #[test]
    fn a_thousandfold_smaller_access_count_wins_on_its_own() {
        let mut broad = candidate(&[1], &[1], false, 1, 1);
        with_estimate(&mut broad, 200_000.0, 200_000.0);
        let mut narrow = candidate(&[2], &[2], false, 1, 1);
        with_estimate(&mut narrow, 150.0, 150.0);
        // Incomparable access columns, so only Fix45132 can separate them.
        assert_eq!(compare_candidates(&broad, &narrow, &context()).0, -1);
        assert_eq!(compare_candidates(&narrow, &broad, &context()).0, 1);
    }
}
