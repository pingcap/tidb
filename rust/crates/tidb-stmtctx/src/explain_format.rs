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

//! The `EXPLAIN FORMAT` a statement names (Go `types.ExplainFormat*`).

/// The `EXPLAIN FORMAT = '...'` this tier accepts. Go's `'row'` (the
/// default, also the explicit spelling) and `'brief'` render the identical
/// five-column tree; `'brief'` merely drops each operator's `_N` build-order
/// suffix. `'verbose'` adds costs from the statement's optimizer context.
/// `'plan_tree'` is Go's four-column tree shape (it omits `estRows`),
/// which is the format used by the TPC-DS source test.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExplainFormat {
    /// Go's default: every operator id carries its `_N` build-order suffix.
    Row,
    /// Row format with statement-local estimated operator costs.
    Verbose,
    /// Same tree, ids printed without the `_N` suffix.
    Brief,
    /// Go's four-column tree format, without estimates or id suffixes.
    PlanTree,
    /// Go's GraphViz digraph (`Explain.prepareDotInfo`), one TEXT cell
    /// carrying the whole `digraph` document.
    Dot,
    /// Go's hint report (`Explain.prepareHintInfo`): the statement's
    /// rendered hint string beside its binding status.
    Hint,
    /// Go's cost trace (`ExplainFormatCostTrace`): the row planner's own
    /// per-operator cost formulas beside the estimates. The format is
    /// ADMITTED here; its renderer needs go's `CostVer2` formula-trace
    /// port (`plan_cost_ver2.go`'s per-operator formula closures) and
    /// answers `renderer is not translated` until that lands.
    CostTrace,
    /// Go's JSON plan report (`ExplainFormatJSON`): the row planner renders
    /// the plan tree as one JSON document per root.
    Json,
    /// Go `ExplainFormatPlanCache`: the row format over the plan the
    /// non-prepared plan cache gives the statement, with the reason it
    /// skipped the cache as a warning.
    PlanCache,
    /// Go `ExplainFormatTiDBJSON`: every root's operator tree as one JSON
    /// array in a single `TiDB_JSON` cell (`explainFlatPlanInJSONFormat`).
    TidbJson,
    /// Go `ExplainFormatBinary`: the compressed `tipb.ExplainData` of the
    /// plan in plan order (`BinaryPlanStrFromFlatPlan(flat, false)`).
    Binary,
}

impl ExplainFormat {
    /// Parses Go's `ExplainStmt.Format` string, case-insensitively as Go's
    /// preprocessor does. `None` for a format this tier does not recognize
    /// -- the caller reports Go's own "Unknown EXPLAIN format name" error
    /// (captured verbatim from `explain format = 'bogus' ...`).
    pub fn parse(format: &str) -> Option<Self> {
        // Go `Explain.prepareSchema` rewrites `traditional` to `row`.
        if format.eq_ignore_ascii_case("row") || format.eq_ignore_ascii_case("traditional") {
            Some(Self::Row)
        } else if format.eq_ignore_ascii_case("verbose") {
            Some(Self::Verbose)
        } else if format.eq_ignore_ascii_case("brief") {
            Some(Self::Brief)
        } else if format.eq_ignore_ascii_case("plan_tree") {
            Some(Self::PlanTree)
        } else if format.eq_ignore_ascii_case("dot") {
            Some(Self::Dot)
        } else if format.eq_ignore_ascii_case("hint") {
            Some(Self::Hint)
        } else if format.eq_ignore_ascii_case("cost_trace") {
            Some(Self::CostTrace)
        } else if format.eq_ignore_ascii_case("json") {
            Some(Self::Json)
        } else if format.eq_ignore_ascii_case("plan_cache") {
            Some(Self::PlanCache)
        } else if format.eq_ignore_ascii_case("tidb_json") {
            Some(Self::TidbJson)
        } else if format.eq_ignore_ascii_case("binary") {
            Some(Self::Binary)
        } else {
            None
        }
    }
}

