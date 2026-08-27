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

//! Documentary gap ports for two `pkg/planner/core/rule` unit-test families
//! (`pkg/planner.part15` on `origin/master`):
//!
//! - `rule_prune_indexes_internal_test.go` items 859–860, whose helpers live
//!   in `pkg/planner/core/rule/index_double_read.go`-adjacent scoring code;
//! - `rule_generate_column_substitute_test.go:209 BenchmarkSubstituteExpression`
//!   (item 861).
//!
//! Neither `effectiveIndexColumnIDs` / `HandleColsToAppend` /
//! `scoreIndexPath` nor the expression-column substitution kernel is
//! transcreated on this crate's tree yet.
//!
//! | Go function | Rust test |
//! | --- | --- |
//! | `rule_prune_indexes_internal_test.go:35 TestEffectiveIndexColumnIDsWithUnresolvedColumn` | [`effective_index_column_ids_stop_at_unresolved_slot`] |
//! | `rule_prune_indexes_internal_test.go:67 TestScoreIndexPathPartialIndexBadOffset` | [`score_index_path_partial_index_bad_offset_scores_zero`] |
//! | `rule_generate_column_substitute_test.go:209 BenchmarkSubstituteExpression` | [`benchmark_substitute_expression_alias_groups`] |

/// GO PORT of `rule_prune_indexes_internal_test.go:35
/// TestEffectiveIndexColumnIDsWithUnresolvedColumn` (docstring :30-34).
///
/// Re-derived contract: a common-handle DataSource (`IsCommonHandle=true`,
/// `CommonHandleVersion=1`, one int handle column id=3). For an index over
/// `(a,b)` whose `FullIdxCols` are both resolved `[col1, col2]`,
/// `effectiveIndexColumnIDs` returns `[1, 2, 3]` — the clustered handle id
/// appended after the resolved prefix. For `FullIdxCols = [col1, nil]` (a
/// position unresolvable by `util.IndexInfo2FullCols`):
/// `ds.HandleColsToAppend(path, path.FullIdxCols)` returns NIL slices (no
/// append past a hole), and `effectiveIndexColumnIDs` yields `[1, -1]` —
/// crediting stops at the unresolved slot, never touching out-of-range data.
#[test]
#[ignore = "go-parity-gap: effectiveIndexColumnIDs/HandleColsToAppend on DataSource+AccessPath shapes not transcreated"]
fn effective_index_column_ids_stop_at_unresolved_slot() {}

/// GO PORT of `rule_prune_indexes_internal_test.go:67
/// TestScoreIndexPathPartialIndexBadOffset` (docstring :62-66).
///
/// Re-derived contract: an index whose single `IndexColumn` carries
/// `Offset == len(tableColumns)` (one past the end) must NOT panic inside
/// `scoreIndexPath`; the pre-check treats the constraint column as not found
/// and the returned score's `interestingCount` stays ZERO even though
/// `interestingColIDs` contains the full-index column's id=1 and
/// `ConditionExprString` says `a > 0` (:71-80).
#[test]
#[ignore = "go-parity-gap: scoreIndexPath/columnRequirements scoring internals not transcreated"]
fn score_index_path_partial_index_bad_offset_scores_zero() {}

/// GO PORT of `rule_generate_column_substitute_test.go:209
/// BenchmarkSubstituteExpression`.
///
/// Re-derived contract: repeatedly substitutes expression aliases across the
/// generated-column-substitution pass (`substituteExprCol`/`replaceExprCol`
/// harness built by the sibling tests); b.N iterations exercise the same
/// substitution depth used by rules checking whether predicates can use
/// indexed virtual columns. Benchmark-shaped port; excluded from the batch
/// gate exactly as `go test` skips Benchmarks.
#[test]
#[ignore = "go-parity-gap: expression alias substitution kernel not in this crate"]
fn benchmark_substitute_expression_alias_groups() {}
