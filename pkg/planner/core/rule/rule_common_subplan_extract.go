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
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util/coreusage"
)

// CommonSubplanExtractor detects structurally identical Join subtrees in a
// single query so they can be shared via a CTE. This is a conservative L0
// implementation modelled after OceanBase's TEMP_TABLE_OPTIMIZATION:
// a candidate subtree must match another candidate on
//   - join shape (same post-order of LogicalJoin / DataSource / LogicalSelection)
//   - join type and all join conditions
//   - selection / data-source filter conditions
//   - underlying base tables (TableInfo.ID + PhysicalTableID)
//
// It runs after JoinReorder has stabilised the join shape and before the
// final ColumnPruner pass. Detection is the only side-effect in this
// landing; the CTE rewrite path is the follow-up.
type CommonSubplanExtractor struct{}

// Optimize implements base.LogicalOptRule.<0th> interface.
func (c *CommonSubplanExtractor) Optimize(_ context.Context, p base.LogicalPlan) (base.LogicalPlan, bool, error) {
	sctx := p.SCtx()
	if sctx == nil || sctx.GetSessionVars() == nil || !sctx.GetSessionVars().EnableCommonSubplanExtract {
		return p, false, nil
	}
	groups := c.detect(p)
	if len(groups) == 0 {
		return p, false, nil
	}
	// Phase 1 landing: detection only. Record telemetry via statement context.
	if stmtCtx := sctx.GetSessionVars().StmtCtx; stmtCtx != nil {
		for _, g := range groups {
			stmtCtx.AppendNote(fmt.Errorf(
				"common_subplan_extract: detected %d identical join subtrees fingerprint=%s",
				len(g.candidates), truncateForNote(g.fingerprint)))
		}
	}
	// TODO: extract to shared CTE. For now leave the plan unchanged so this
	// rule is always safe to enable.
	return p, false, nil
}

// Name implements base.LogicalOptRule.<1st> interface.
func (*CommonSubplanExtractor) Name() string {
	return "common_subplan_extract"
}

// subplanGroup holds a set of candidate join subtrees sharing a fingerprint.
type subplanGroup struct {
	fingerprint string
	candidates  []base.LogicalPlan
}

// detect collects every LogicalJoin that is safe to share and groups them
// by a canonical fingerprint. Only groups with at least two candidates are
// returned.
func (c *CommonSubplanExtractor) detect(root base.LogicalPlan) []*subplanGroup {
	buckets := make(map[string]*subplanGroup)
	// Top-down descent with post-order capture so outer subtrees that also
	// qualify still produce their own fingerprint.
	var walk func(p base.LogicalPlan)
	walk = func(p base.LogicalPlan) {
		if p == nil {
			return
		}
		for _, ch := range p.Children() {
			walk(ch)
		}
		join, ok := p.(*logicalop.LogicalJoin)
		if !ok {
			return
		}
		if !c.isSafeCandidate(join) {
			return
		}
		evalCtx := join.SCtx().GetExprCtx().GetEvalCtx()
		var buf bytes.Buffer
		if !writeFingerprint(&buf, join, evalCtx) {
			return
		}
		fp := buf.String()
		g, ok := buckets[fp]
		if !ok {
			g = &subplanGroup{fingerprint: fp}
			buckets[fp] = g
		}
		g.candidates = append(g.candidates, join)
	}
	walk(root)

	out := make([]*subplanGroup, 0, len(buckets))
	for _, g := range buckets {
		if len(g.candidates) >= 2 && !hasAncestorDescendant(g.candidates) {
			out = append(out, g)
		}
	}
	return out
}

// isSafeCandidate returns true iff the join subtree is eligible for L0
// extraction: inner join only, no correlated columns, no non-deterministic
// exprs, and a whitelist of inner operators.
func (c *CommonSubplanExtractor) isSafeCandidate(join *logicalop.LogicalJoin) bool {
	if join.JoinType != base.InnerJoin {
		return false
	}
	if join.FromDecorrelatedApply {
		return false
	}
	if len(coreusage.ExtractCorrelatedCols4LogicalPlan(join)) > 0 {
		return false
	}
	return subtreeAllowedForExtract(join)
}

// subtreeAllowedForExtract walks the subtree and rejects any operator that
// is not one of {LogicalJoin(inner), LogicalSelection, DataSource}. This is
// deliberately conservative; richer shapes (Aggregation, TopN, Projection,
// Apply, CTE references) can be added once the CTE wiring is in place.
func subtreeAllowedForExtract(p base.LogicalPlan) bool {
	switch v := p.(type) {
	case *logicalop.LogicalJoin:
		if v.JoinType != base.InnerJoin {
			return false
		}
		if containsNonDeterministic(v.EqualConditions) ||
			containsNonDeterministic(v.NAEQConditions) ||
			containsNonDeterministicCNF(v.LeftConditions) ||
			containsNonDeterministicCNF(v.RightConditions) ||
			containsNonDeterministicCNF(v.OtherConditions) {
			return false
		}
	case *logicalop.LogicalSelection:
		if containsNonDeterministicAny(v.Conditions) {
			return false
		}
	case *logicalop.DataSource:
		if containsNonDeterministicAny(v.AllConds) || containsNonDeterministicAny(v.PushedDownConds) {
			return false
		}
	default:
		return false
	}
	for _, ch := range p.Children() {
		if !subtreeAllowedForExtract(ch) {
			return false
		}
	}
	return true
}

func containsNonDeterministic(exprs []*expression.ScalarFunction) bool {
	for _, e := range exprs {
		if e != nil && expression.CheckNonDeterministic(e) {
			return true
		}
	}
	return false
}

func containsNonDeterministicCNF(exprs expression.CNFExprs) bool {
	return containsNonDeterministicAny(exprs)
}

func containsNonDeterministicAny(exprs []expression.Expression) bool {
	for _, e := range exprs {
		if expression.CheckNonDeterministic(e) {
			return true
		}
	}
	return false
}

// hasAncestorDescendant rejects groups where one candidate is a descendant of
// another — extracting a nested pair would double-count work.
func hasAncestorDescendant(cs []base.LogicalPlan) bool {
	set := make(map[base.LogicalPlan]struct{}, len(cs))
	for _, c := range cs {
		set[c] = struct{}{}
	}
	for _, c := range cs {
		for _, ch := range c.Children() {
			if containsAnyDescendant(ch, set) {
				return true
			}
		}
	}
	return false
}

func containsAnyDescendant(p base.LogicalPlan, set map[base.LogicalPlan]struct{}) bool {
	if p == nil {
		return false
	}
	if _, ok := set[p]; ok {
		return true
	}
	for _, ch := range p.Children() {
		if containsAnyDescendant(ch, set) {
			return true
		}
	}
	return false
}

// writeFingerprint emits a canonical string capturing the subtree shape and
// its expressions using column-OrigName (table/column name) rather than
// UniqueID, so two structurally identical subtrees with distinct column
// UniqueIDs share the same fingerprint. Returns false if anything about the
// subtree makes fingerprinting unreliable.
func writeFingerprint(buf *bytes.Buffer, p base.LogicalPlan, evalCtx expression.EvalContext) bool {
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
	default:
		// subtreeAllowedForExtract should have caught this.
		return false
	}
	children := p.Children()
	fmt.Fprintf(buf, "#%d{", len(children))
	for _, ch := range children {
		if !writeFingerprint(buf, ch, evalCtx) {
			return false
		}
	}
	buf.WriteString("}]")
	_ = evalCtx
	return true
}

func truncateForNote(s string) string {
	const maxLen = 200
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
