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

//! Ports for `pkg/planner/core/tests/redact/` — items 1170–1174 of
//! `pkg/planner.part20` (all 1278 `Test*`/`Benchmark*` declarations under
//! `pkg/planner/` on `origin/master`, sorted by file then line, chunked by
//! 60). `redact/main_test.go:25 TestMain` is bootstrap-only and is recorded
//! as skipped-reason in the batch receipt.
//!
//! The Go suite drives `explain format='plan_tree'` under
//! `set global tidb_redact_log = MARKER|ON` and pins how literal values in
//! plan rows are masked: MARKER wraps each value in `‹›` (Go
//! `errors.RedactLogMarker`), ON replaces each value with `?` (Go
//! `errors.RedactLogEnable`). One row family of `TestRedactExplain` — the
//! root and cop `Limit` operators — is pinned HERE for real, against this
//! wired [`tidb_planner::physical::PhysicalLimit::explain_info`]
//! redaction branches, which are the source `PhysicalLimit.ExplainInfo`
//! implementation (`pkg/planner/core/operator/physicalop/physical_limit.go:
//! 126-155`: Disable renders `offset:%v, count:%v`, Marker renders
//! `offset:‹%v›, count:‹%v›`, Enable renders `offset:?, count:?`). Everything
//! else (ranges, projections, selections, TiFlash windows) has no Rust
//! redaction surface yet and stays documentary.

use tidb_planner::physical::{BasePhysicalPlan, PhysicalLimit, RedactMode};

fn limit(offset: u64, count: u64) -> PhysicalLimit {
    PhysicalLimit {
        base: BasePhysicalPlan::with_id(1, "Limit", 0),
        offset,
        count,
        ..PhysicalLimit::default()
    }
}

/// Rust side of `pkg/planner/core/tests/redact/redact_test.go:23
/// TestRedactExplain` — the Limit rows of the MARKER and ON arms.
///
/// The Go test (under `set global tidb_redact_log=MARKER`, :42) explains
/// `select * from t where a > 1 limit 10 offset 10` on
/// `t(a int primary key, b int)` and expects exactly
/// `Limit root  offset:‹10›, count:‹10›` (:55) over
/// `Limit cop[tikv]  offset:‹0›, count:‹20›` (:57); under
/// `tidb_redact_log=ON` (:115) the same query expects
/// `Limit root  offset:?, count:?` (:128) over
/// `Limit cop[tikv]  offset:?, count:?` (:130). The operator-name/`root`/
/// `cop[tikv]` prefix comes from the explain driver; the value-bearing tail
/// is `PhysicalLimit.ExplainInfo` (`pkg/planner/core/operator/physicalop/
/// physical_limit.go:126-155`), which this crate models as
/// the wired `PhysicalLimit::explain_info`. The golden root limit carries
/// `offset=10, count=10` and the cop-side limit `offset=0, count=20` (the
/// pushed-down child folds the offset into its count), so both goldens are
/// pinned verbatim in both redaction modes.
#[test]
fn redact_explain_limit_rows_track_marker_and_on_modes() {
    // MARKER arm — redact_test.go:53-58 (tidb_redact_log=MARKER set at :42).
    let root = limit(10, 10);
    assert_eq!(
        root.explain_info(RedactMode::Marker),
        "offset:‹10›, count:‹10›",
        "root Limit row under MARKER (redact_test.go:55)"
    );
    let cop = limit(0, 20);
    assert_eq!(
        cop.explain_info(RedactMode::Marker),
        "offset:‹0›, count:‹20›",
        "cop Limit row under MARKER (redact_test.go:57)"
    );

    // ON arm — redact_test.go:126-131 (tidb_redact_log=ON set at :115).
    assert_eq!(
        root.explain_info(RedactMode::Enable),
        "offset:?, count:?",
        "root Limit row under ON (redact_test.go:128)"
    );
    assert_eq!(
        cop.explain_info(RedactMode::Enable),
        "offset:?, count:?",
        "cop Limit row under ON (redact_test.go:130)"
    );
}

/// Documentary twin for the REMAINDER of `pkg/planner/core/tests/redact/
/// redact_test.go:23 TestRedactExplain` (the non-Limit rows): MARKER `‹v›`
/// vs ON `?` masking over the multi-value `in` Projection/HashJoin/
/// Batch_Point_Get/Selection rows (:44-51 vs :117-124), the TableRangeScan
/// ranges `range:(‹1›,+inf]` / `range:[-inf,‹1›)` vs `(?,+inf]` / `[-inf,?)`
/// (:58, :62 vs :131, :135), Point_Get `handle:‹1›` vs `handle:?` with its
/// Sort/Projection rows (:64-68 vs :137-141), the TiFlash window partition
/// expression `plus(test.employee.deptid, ‹1›)` vs `?(...)` (:70-79 vs
/// :143-152), the list-partition prune `eq(test.tlist.a, ‹2›)` vs `?`
/// (:80-83 vs :153-156), the recursive-CTE Limit/Projection/Selection values
/// (:85-97 vs :158-169), the virtual-generated-column IndexRangeScan
/// `range:[‹1›,‹1›]` vs `[?,?]` (:99-104 vs :171-176), and the group-by
/// `group by:‹1›` vs `?` HashAgg rows (:106-111 vs :178-183). Every one of
/// those renders through range/projection/selection explain code with its
/// own redaction switch, none of which this crate models.
///
/// go-parity-gap: range/projection/selection/window explain redaction has no
/// Rust surface; only the Limit branch is modeled (see the running twin).
#[test]
#[ignore = "go-parity-gap: non-Limit explain rows need redaction-aware range/projection/selection explain surfaces"]
fn redact_explain_remaining_rows_marker_and_on_documentary() {}

/// GO PORT of `pkg/planner/core/tests/redact/redact_test.go:186
/// TestRedactForRangeInfo`.
///
/// Contract: under `tidb_redact_log=ON` (:195), the inl_join plan over
/// `t1(a int)` × `t2(a int, b int, c int, index idx(a, b))` with
/// `where t2.b in (10, 20, 30)` explains the inner IndexRangeScan as
/// `range: decided by [eq(test.t2.a, test.t1.a) in(test.t2.b, ?, ?, ?)]`
/// (:204) — the three IN-list literals are masked to `?` inside the
/// access-condition spelling, while structural identifiers stay readable.
///
/// go-parity-gap: index-join range-decided-by explain spelling with
/// redaction is unported.
#[test]
#[ignore = "go-parity-gap: range-decided-by explain redaction needs the index-join explain surface"]
fn redact_range_info_masks_in_list_in_decided_by() {}

/// GO PORT of `pkg/planner/core/tests/redact/redact_test.go:209
/// TestJoinNotSupportedByTiFlash`.
///
/// Contract: with `dayofmonth` blacklisted for TiFlash
/// (`mysql.expr_pushdown_blacklist` + `admin reload expr_pushdown_blacklist`,
/// :219-220), the self left-join with `dayofmonth(a.datetime_col) > 100`
/// cannot push to TiFlash and plans as a root MergeJoin with keep-order
/// index scans. Under `tidb_redact_log=ON` (:221) the join's left condition
/// explains as `gt(dayofmonth(test.table_1.datetime_col), ?)` (:222-227);
/// under MARKER (:228) as `gt(dayofmonth(test.table_1.datetime_col), ‹100›)`
/// (:229-234) — the blacklisted function is pinned INSIDE the redacted
/// condition, proving the unsupported-pushdown decision happens before
/// redaction.
///
/// go-parity-gap: MergeJoin explain rows with expr-pushdown-blacklist
/// decisions are unported.
#[test]
#[ignore = "go-parity-gap: MergeJoin explain + expr pushdown blacklist have no Rust surface"]
fn join_not_supported_by_tiflash_redacts_mergejoin_left_cond() {}

/// GO PORT of `pkg/planner/core/tests/redact/redact_test.go:237
/// TestRedactTiFlash`.
///
/// Contract: with `tidb_enforce_mpp=1`, isolation engines tiflash, TiFlash
/// replicas registered and `tidb_max_tiflash_threads=20` (:245-251), the
/// window query `first_value(v) over (partition by p order by o range
/// between 3 preceding and 0 following)` explains under ON (:252) with the
/// frame bounds masked: `... range between ? preceding and ? following),
/// stream_count: 20` (:253-260, frame at :256); under MARKER (:261) the
/// bounds read `range between ‹3› preceding and ‹0› following` (:262-269,
/// frame at :265). The `stream_count: 20` suffix (from the threads setting)
/// stays unmasked in both arms.
///
/// go-parity-gap: TiFlash window/Exchange explain rows are unported.
#[test]
#[ignore = "go-parity-gap: TiFlash window frame explain redaction is unported"]
fn redact_tiflash_window_frame_bounds() {}
