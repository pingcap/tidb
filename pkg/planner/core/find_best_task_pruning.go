// Copyright 2017 PingCAP, Inc.
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

package core

import (
	"fmt"
	"slices"
	"strings"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/rule"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
)

// skylinePruning prunes access paths according to different factors. An access path can be pruned only if
// there exists a path that is not worse than it at all factors and there is at least one better factor.
func skylinePruning(ds *logicalop.DataSource, prop *property.PhysicalProperty) []*candidatePath {
	candidates := make([]*candidatePath, 0, 4)
	idxMissingStats := false
	// tidb_opt_prefer_range_scan is the master switch to control index preferencing
	preferRange := ds.SCtx().GetSessionVars().GetAllowPreferRangeScan()
	for _, path := range ds.PossibleAccessPaths {
		// We should check whether the possible access path is valid first.
		if path.StoreType != kv.TiFlash && prop.IsFlashProp() {
			continue
		}
		if len(path.PartialAlternativeIndexPaths) > 0 {
			// OR normal index merge path, try to determine every index partial path for this property.
			candidate := convergeIndexMergeCandidate(ds, path, prop)
			if candidate != nil {
				candidates = append(candidates, candidate)
			}
			continue
		}
		if path.PartialIndexPaths != nil {
			candidates = append(candidates, getIndexMergeCandidate(ds, path, prop))
			continue
		}
		// if we already know the range of the scan is empty, just return a TableDual
		if len(path.Ranges) == 0 {
			return []*candidatePath{{path: path}}
		}
		var currentCandidate *candidatePath
		if path.IsTablePath() {
			if prop.PartialOrderInfo != nil {
				// skyline pruning table path with partial order property is not supported yet.
				// TODO: support it in the future after we support prefix column as partial order.
				continue
			}
			currentCandidate = getTableCandidate(ds, path, prop)
		} else {
			// Check if this path can be used for partial order optimization
			var matchPartialOrderIndex bool
			if ds.SCtx().GetSessionVars().IsPartialOrderedIndexForTopNEnabled() &&
				prop.PartialOrderInfo != nil {
				if !matchPartialOrderProperty(path, prop.PartialOrderInfo).Matched {
					// skyline pruning all indexes that cannot provide partial order when we are looking for
					continue
				}
				matchPartialOrderIndex = true
				// If the index can match partial order requirement and user use "use/force index" in hint.
				// If the index can't match partial order requirement and use use "use/force index" and enable partial order optimization together,
				// the behavior will degenerate into normal index use behavior without considering partial order optimization.
				// If path is force but it use the hint /no_order_index/ which ForceNoKeepOrder is true,
				// we won't consider it for partial order optimization, and it will be treated as normal forced index.
				if path.Forced && !path.ForceNoKeepOrder {
					path.ForcePartialOrder = true
				}
			}

			// We will use index to generate physical plan if any of the following conditions is satisfied:
			// 1. This path's access cond is not nil.
			// 2. We have a non-empty prop to match.
			// 3. This index is forced to choose.
			// 4. The needed columns are all covered by index columns(and handleCol).
			// 5. Match PartialOrderInfo physical property to be considered for partial order optimization (new condition).
			keepIndex := len(path.AccessConds) > 0 || !prop.IsSortItemEmpty() || path.Forced || path.IsSingleScan || matchPartialOrderIndex
			if !keepIndex {
				// If none of the above conditions are met, this index will be directly pruned here.
				continue
			}

			// After passing the check, generate the candidate
			currentCandidate = getIndexCandidate(ds, path, prop)
		}
		pruned := false
		for i := len(candidates) - 1; i >= 0; i-- {
			if candidates[i].path.StoreType == kv.TiFlash {
				continue
			}
			result, missingStats := compareCandidates(ds.SCtx(), ds.StatisticTable, prop, candidates[i], currentCandidate, preferRange)
			if missingStats {
				idxMissingStats = true // Ensure that we track idxMissingStats across all iterations
			}
			if result == 1 {
				pruned = true
				// We can break here because the current candidate lost to another plan.
				// This means that we won't add it to the candidates below.
				break
			} else if result == -1 {
				// The current candidate is better - so remove the old one from "candidates"
				candidates = slices.Delete(candidates, i, i+1)
			}
		}
		if !pruned {
			candidates = append(candidates, currentCandidate)
		}
	}

	// If we've forced an index merge - we want to keep these plans
	preferMerge := rule.ShouldPreferIndexMerge(ds)
	if preferRange {
		// Override preferRange with the following limitations to scope
		preferRange = preferMerge || idxMissingStats || ds.TableStats.HistColl.Pseudo || ds.TableStats.RowCount < 1
	}
	if preferRange && len(candidates) > 1 {
		// If a candidate path is TiFlash-path or forced-path or MV index or global index, we just keep them. For other
		// candidate paths, if there exists any range scan path, we remove full scan paths and keep range scan paths.
		preferredPaths := make([]*candidatePath, 0, len(candidates))
		var hasRangeScanPath, hasMultiRange bool
		for _, c := range candidates {
			if len(c.path.Ranges) > 1 {
				hasMultiRange = true
			}
			if c.path.Forced || c.path.StoreType == kv.TiFlash || (c.path.Index != nil && (c.path.Index.Global || c.path.Index.MVIndex)) {
				preferredPaths = append(preferredPaths, c)
				continue
			}
			// Preference plans with equals/IN predicates or where there is more filtering in the index than against the table
			indexFilters := c.equalPredicateCount() > 0 || len(c.path.TableFilters) < len(c.path.IndexFilters)
			if preferMerge || ((c.path.IsSingleScan || indexFilters) && (prop.IsSortItemEmpty() || c.matchPropResult.Matched())) {
				if !c.path.IsFullScanRange(ds.TableInfo) {
					preferredPaths = append(preferredPaths, c)
					hasRangeScanPath = true
				}
			}
		}
		if hasMultiRange {
			// Only log the fix control if we had multiple ranges
			ds.SCtx().GetSessionVars().RecordRelevantOptFix(fixcontrol.Fix52869)
		}
		if hasRangeScanPath {
			return preferredPaths
		}
	}

	return candidates
}

// hasOnlyEqualPredicatesInDNF checks if all access conditions in DNF form contain at least one equal predicate
func (c *candidatePath) hasOnlyEqualPredicatesInDNF() bool {
	// Helper function to check if a condition is an equal/IN predicate or a LogicOr of equal/IN predicates
	var isEqualPredicateOrOr func(expr expression.Expression) bool
	isEqualPredicateOrOr = func(expr expression.Expression) bool {
		sf, ok := expr.(*expression.ScalarFunction)
		if !ok {
			return false
		}
		switch sf.FuncName.L {
		case ast.UnaryNot:
			// Reject NOT operators - they can make predicates non-equal
			return false
		case ast.LogicOr, ast.LogicAnd:
			for _, arg := range sf.GetArgs() {
				if !isEqualPredicateOrOr(arg) {
					return false
				}
			}
			return true
		case ast.EQ, ast.In:
			// Check if it's an equal predicate (eq) or IN predicate (in)
			// Also reject any other comparison operators that are not equal/IN
			return true
		default:
			// Reject all other comparison operators (LT, GT, LE, GE, NE, etc.)
			// and any other functions that are not equal/IN predicates
			return false
		}
	}

	// Check all access conditions
	for _, cond := range c.path.AccessConds {
		if !isEqualPredicateOrOr(cond) {
			return false
		}
	}
	return true
}

func (c *candidatePath) equalPredicateCount() int {
	if c.indexJoinCols > 0 { // this candidate path is for Index Join
		// Specially handle indexes under Index Join, since these indexes can't see the join key themselves, we can't
		// use path.EqOrInCondCount here.
		// For example, for "where t1.a=t2.a and t2.b=1" and index "idx(t2, a, b)", since the DataSource of t2 can't
		// see the join key "t1.a=t2.a", it only considers "t2.b=1" as access condition, so its EqOrInCondCount is 0,
		// but actually it should be 2.
		return c.indexJoinCols
	}

	// Exit if this isn't a DNF condition or has no access conditions
	if !c.path.IsDNFCond || len(c.path.AccessConds) == 0 {
		return c.path.EqOrInCondCount
	}
	if c.hasOnlyEqualPredicatesInDNF() {
		return c.path.MinAccessCondsForDNFCond
	}
	return max(0, c.path.MinAccessCondsForDNFCond-1)
}

func getPruningInfo(ds *logicalop.DataSource, candidates []*candidatePath, prop *property.PhysicalProperty) string {
	if len(candidates) == len(ds.PossibleAccessPaths) {
		return ""
	}
	if len(candidates) == 1 && len(candidates[0].path.Ranges) == 0 {
		// For TableDual, we don't need to output pruning info.
		return ""
	}
	names := make([]string, 0, len(candidates))
	var tableName string
	if ds.TableAsName.O == "" {
		tableName = ds.TableInfo.Name.O
	} else {
		tableName = ds.TableAsName.O
	}
	getSimplePathName := func(path *util.AccessPath) string {
		if path.IsTablePath() {
			if path.StoreType == kv.TiFlash {
				return tableName + "(tiflash)"
			}
			return tableName
		}
		return path.Index.Name.O
	}
	for _, cand := range candidates {
		if cand.path.PartialIndexPaths != nil {
			partialNames := make([]string, 0, len(cand.path.PartialIndexPaths))
			for _, partialPath := range cand.path.PartialIndexPaths {
				partialNames = append(partialNames, getSimplePathName(partialPath))
			}
			names = append(names, fmt.Sprintf("IndexMerge{%s}", strings.Join(partialNames, ",")))
		} else {
			names = append(names, getSimplePathName(cand.path))
		}
	}
	items := make([]string, 0, len(prop.SortItems))
	for _, item := range prop.SortItems {
		items = append(items, item.String())
	}
	return fmt.Sprintf("[%s] remain after pruning paths for %s given Prop{SortItems: [%s], TaskTp: %s}",
		strings.Join(names, ","), tableName, strings.Join(items, " "), prop.TaskTp)
}

