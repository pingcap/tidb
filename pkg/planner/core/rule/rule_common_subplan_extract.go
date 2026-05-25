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

package rule

import (
	"bytes"
	"context"
	"fmt"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
)

// CommonSubplanExtractor detects structurally identical subtrees within the
// branches of a LogicalUnionAll and extracts them as a shared CTE, so the
// common subplan is computed only once and its result is reused across branches.
//
// Only "completely identical" branches are handled: the full subtree shape,
// base tables, filter/project/aggregate expressions must all match under
// normalized column naming (OrigName), so two branches compiled from the
// same SQL fragment will fingerprint identically even with different UniqueIDs.
//
// This rule runs after JoinReOrder (index 21) and before the final ColumnPruner
// (index 24), so join shapes are stable and the CTE will be pruned correctly.
type CommonSubplanExtractor struct{}

// Optimize implements base.LogicalOptRule.
func (c *CommonSubplanExtractor) Optimize(_ context.Context, p base.LogicalPlan) (base.LogicalPlan, bool, error) {
	sctx := p.SCtx()
	if sctx == nil || sctx.GetSessionVars() == nil || !sctx.GetSessionVars().EnableCommonSubplanExtract {
		return p, false, nil
	}
	changed, err := c.optimizeRecursive(p)
	return p, changed, err
}

// Name implements base.LogicalOptRule.
func (*CommonSubplanExtractor) Name() string {
	return "common_subplan_extract"
}

// optimizeRecursive walks the plan bottom-up. At each LogicalUnionAll node it
// detects and extracts duplicate branches as shared CTEs.
func (c *CommonSubplanExtractor) optimizeRecursive(p base.LogicalPlan) (bool, error) {
	changed := false
	for _, ch := range p.Children() {
		childChanged, err := c.optimizeRecursive(ch)
		if err != nil {
			return false, err
		}
		changed = changed || childChanged
	}

	union, ok := p.(*logicalop.LogicalUnionAll)
	if !ok {
		return changed, nil
	}
	groups := detectUnionBranchDuplicates(union)
	if len(groups) == 0 {
		return changed, nil
	}
	unionChanged := extractDuplicatesInUnion(union, groups)
	return changed || unionChanged, nil
}

// detectUnionBranchDuplicates fingerprints every child of union and returns
// groups of child indices that share a fingerprint (≥ 2 members only).
func detectUnionBranchDuplicates(union *logicalop.LogicalUnionAll) map[string][]int {
	groups := make(map[string][]int, len(union.Children()))
	for i, ch := range union.Children() {
		var buf bytes.Buffer
		if !writePlanFingerprint(&buf, ch) {
			continue
		}
		fp := buf.String()
		groups[fp] = append(groups[fp], i)
	}
	// remove singleton groups
	for fp, indices := range groups {
		if len(indices) < 2 {
			delete(groups, fp)
		}
	}
	return groups
}

// extractDuplicatesInUnion rewrites the union children: for each duplicate
// group it creates one CTEClass (backed by the first identical child) and
// replaces every member of the group with a fresh LogicalCTE reference.
func extractDuplicatesInUnion(union *logicalop.LogicalUnionAll, groups map[string][]int) bool {
	if len(groups) == 0 {
		return false
	}
	sctx := union.SCtx()
	svar := sctx.GetSessionVars()
	children := union.Children()
	changed := false

	for _, indices := range groups {
		firstChild := children[indices[0]]

		// Allocate a unique storage ID by piggybacking on the column ID counter.
		storageID := int(svar.AllocPlanColumnID())
		cteName := ast.NewCIStr(fmt.Sprintf("cse_cte_%d", storageID))

		// prevSchema holds the column descriptors of the seed plan.
		// ColumnMap entries point here so predicate push-down can resolve
		// column references from CTE consumers back to the seed plan.
		prevSchema := firstChild.Schema().Clone()

		cteClass := &logicalop.CTEClass{
			SeedPartLogicalPlan: firstChild,
			IDForStorage:        storageID,
			PushDownPredicates:  make([]expression.Expression, 0),
			ColumnMap:           make(map[string]*expression.Column),
		}

		for _, idx := range indices {
			// Each CTE reference gets its own schema with fresh UniqueIDs so
			// upstream operators using the Union's schema are not confused.
			// The physical Union maps children by column position, not UniqueID,
			// so differing IDs here are harmless.
			newSchema := prevSchema.Clone()
			for _, col := range newSchema.Columns {
				col.RetType = col.RetType.Clone()
				col.UniqueID = svar.AllocPlanColumnID()
				// Remove NotNull: the CTE materialization may return fewer rows
				// than the original plan in edge cases (empty intermediate result).
				col.RetType.DelFlag(mysql.NotNullFlag)
				col.CleanHashCode()
			}
			// Register the mapping from this reference's column hash codes to the
			// seed plan's columns. Multiple references can coexist in the same map
			// because their column UniqueIDs (and thus hash codes) are distinct.
			for i, col := range newSchema.Columns {
				cteClass.ColumnMap[string(col.HashCode())] = prevSchema.Columns[i]
			}

			lCTE := logicalop.LogicalCTE{
				Cte:       cteClass,
				CteAsName: cteName,
				CteName:   cteName,
				SeedStat:  &property.StatsInfo{},
			}.Init(sctx, union.QueryBlockOffset())
			lCTE.SetSchema(newSchema)
			lCTE.SetOutputNames(firstChild.OutputNames())

			children[idx] = lCTE
			changed = true
		}
	}
	return changed
}

// ---- fingerprinting ----

// writePlanFingerprint writes a canonical, normalized representation of the
// logical plan subtree rooted at p into buf. It returns false if the subtree
// contains any node type not yet covered, signalling that the fingerprint is
// unreliable for this branch.
//
// Column references are encoded via ExplainNormalizedInfo() which uses
// col.OrigName rather than col.UniqueID. Therefore two structurally identical
// plan subtrees compiled from the same SQL (which may differ in UniqueID
// assignments) produce the same fingerprint.
func writePlanFingerprint(buf *bytes.Buffer, p base.LogicalPlan) bool {
	switch v := p.(type) {
	case *logicalop.LogicalJoin:
		fmt.Fprintf(buf, "J[%d|", int(v.JoinType))
		buf.Write(expression.SortedExplainNormalizedScalarFuncList(v.EqualConditions))
		buf.WriteByte(';')
		buf.Write(expression.SortedExplainNormalizedExpressionList(v.LeftConditions))
		buf.WriteByte(';')
		buf.Write(expression.SortedExplainNormalizedExpressionList(v.RightConditions))
		buf.WriteByte(';')
		buf.Write(expression.SortedExplainNormalizedExpressionList(v.OtherConditions))
		buf.WriteByte('|')

	case *logicalop.LogicalSelection:
		buf.WriteString("S[")
		buf.Write(expression.SortedExplainNormalizedExpressionList(v.Conditions))
		buf.WriteByte('|')

	case *logicalop.DataSource:
		if v.TableInfo == nil {
			return false
		}
		fmt.Fprintf(buf, "D[%s.%d|pid=%d|", v.DBName.L, v.TableInfo.ID, v.PhysicalTableID)
		buf.Write(expression.SortedExplainNormalizedExpressionList(v.AllConds))
		buf.WriteByte('|')

	case *logicalop.LogicalProjection:
		buf.WriteString("P[")
		// Projection order is significant: SELECT a,b ≠ SELECT b,a.
		for i, e := range v.Exprs {
			if i > 0 {
				buf.WriteByte(',')
			}
			buf.WriteString(e.ExplainNormalizedInfo())
		}
		buf.WriteByte('|')

	case *logicalop.LogicalAggregation:
		buf.WriteString("A[")
		buf.Write(expression.SortedExplainNormalizedExpressionList(v.GroupByItems))
		buf.WriteByte('|')
		for i, agg := range v.AggFuncs {
			if i > 0 {
				buf.WriteByte(',')
			}
			// ExplainAggFunc with normalized=true does not use the context.
			buf.WriteString(aggregation.ExplainAggFunc(nil, agg, true))
		}
		buf.WriteByte('|')

	case *logicalop.LogicalTopN:
		fmt.Fprintf(buf, "TN[%d+%d|", v.Count, v.Offset)
		for i, item := range v.ByItems {
			if i > 0 {
				buf.WriteByte(',')
			}
			buf.WriteString(item.Expr.ExplainNormalizedInfo())
			if item.Desc {
				buf.WriteString(":d")
			}
		}
		buf.WriteByte('|')

	case *logicalop.LogicalLimit:
		fmt.Fprintf(buf, "L[%d+%d|", v.Count, v.Offset)

	case *logicalop.LogicalUnionAll:
		buf.WriteString("U[")

	default:
		return false
	}

	// Append children count and recurse.
	children := p.Children()
	fmt.Fprintf(buf, "#%d{", len(children))
	for _, ch := range children {
		if !writePlanFingerprint(buf, ch) {
			return false
		}
	}
	buf.WriteString("}]")
	return true
}
