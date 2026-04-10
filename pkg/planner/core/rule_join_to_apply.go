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

package core

import (
	"context"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	ruleutil "github.com/pingcap/tidb/pkg/planner/core/rule/util"
	planutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/types"
)

// JoinToApplyRule rewrites a left outer join into apply when a correlated
// predicate can bypass a window barrier on the inner side.
type JoinToApplyRule struct{}

// correlatableJoinKey keeps the same join key in three coordinate systems:
// at the join boundary, at the window boundary, and at the datasource boundary.
type correlatableJoinKey struct {
	eq          *expression.ScalarFunction
	outerCol    *expression.Column
	innerCol    *expression.Column
	resolvedCol *expression.Column
	accessCol   *expression.Column
}

type windowBypassPath struct {
	window       *logicalop.LogicalWindow
	partitionCol *expression.Column
	accessCol    *expression.Column
	dataSource   *logicalop.DataSource
}

// Optimize implements base.LogicalOptRule.
func (*JoinToApplyRule) Optimize(_ context.Context, p base.LogicalPlan, _ *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	return rewriteJoinToApply(p)
}

// Name implements base.LogicalOptRule.
func (*JoinToApplyRule) Name() string {
	return "join_to_apply"
}

func rewriteJoinToApply(p base.LogicalPlan) (base.LogicalPlan, bool, error) {
	if _, ok := p.(*logicalop.LogicalCTE); ok {
		return p, false, nil
	}

	// Rewrite bottom-up so child joins are already stabilized when the parent
	// checks its own right subtree shape.
	planChanged := false
	if len(p.Children()) > 0 {
		newChildren := make([]base.LogicalPlan, 0, len(p.Children()))
		for _, child := range p.Children() {
			newChild, childChanged, err := rewriteJoinToApply(child)
			if err != nil {
				return nil, false, err
			}
			planChanged = planChanged || childChanged
			newChildren = append(newChildren, newChild)
		}
		p.SetChildren(newChildren...)
	}

	join, ok := p.(*logicalop.LogicalJoin)
	if !ok {
		return p, planChanged, nil
	}

	newPlan, changed := tryRewriteJoinToApply(join)
	return newPlan, planChanged || changed, nil
}

func tryRewriteJoinToApply(join *logicalop.LogicalJoin) (base.LogicalPlan, bool) {
	if join.JoinType != logicalop.LeftOuterJoin || !join.SCtx().GetSessionVars().StmtCtx.EnableJoinToApply {
		return join, false
	}

	if containsLogicalCTE(join.Children()[1]) {
		// Apply under CTE needs extra builder-time CTE bookkeeping that this
		// rule intentionally does not reproduce in the first rollout.
		join.SCtx().GetSessionVars().StmtCtx.SetHintWarning("JOIN_TO_APPLY() is inapplicable because the right subtree contains a CTE.")
		return join, false
	}

	var (
		window     *logicalop.LogicalWindow
		dataSource *logicalop.DataSource
	)
	correlatableKeys := make([]correlatableJoinKey, 0, len(join.EqualConditions))
	remainingKeys := make([]*expression.ScalarFunction, 0, len(join.EqualConditions))
	for _, eq := range join.EqualConditions {
		outerCol, innerCol := extractJoinKeyCols(eq, join.Children()[0].Schema(), join.Children()[1].Schema())
		if outerCol == nil || innerCol == nil {
			remainingKeys = append(remainingKeys, eq)
			continue
		}

		trace, ok := traceWindowBypassPath(join.Children()[1], innerCol, false)
		if !ok {
			remainingKeys = append(remainingKeys, eq)
			continue
		}
		if window == nil {
			window = trace.window
			dataSource = trace.dataSource
		} else if window != trace.window || dataSource != trace.dataSource {
			remainingKeys = append(remainingKeys, eq)
			continue
		}
		correlatableKeys = append(correlatableKeys, correlatableJoinKey{
			eq:          eq,
			outerCol:    outerCol,
			innerCol:    innerCol,
			resolvedCol: trace.partitionCol,
			accessCol:   trace.accessCol,
		})
	}

	if len(correlatableKeys) == 0 {
		join.SCtx().GetSessionVars().StmtCtx.SetHintWarning("JOIN_TO_APPLY() is inapplicable because no join key matches the window partition columns.")
		return join, false
	}
	if !dataSourceHasUsableTiKVAccessPath(dataSource, correlatableKeys) {
		join.SCtx().GetSessionVars().StmtCtx.SetHintWarning("JOIN_TO_APPLY() is inapplicable because the inner datasource has no usable TiKV access path on the correlated key.")
		return join, false
	}

	apply := logicalop.LogicalApply{
		LogicalJoin: *join,
		CorCols:     make([]*expression.CorrelatedColumn, 0, len(correlatableKeys)),
	}.Init(join.SCtx(), join.QueryBlockOffset())
	apply.SetChildren(join.Children()[0], join.Children()[1])
	// The correlated keys stop being regular join equalities. They will be
	// re-attached as right-side correlated filters below.
	apply.EqualConditions = remainingKeys

	correlatedConds := make([]expression.Expression, 0, len(correlatableKeys))
	for _, key := range correlatableKeys {
		corCol := &expression.CorrelatedColumn{
			Column: *key.outerCol,
			Data:   new(types.Datum),
		}
		apply.CorCols = append(apply.CorCols, corCol)
		correlatedConds = append(correlatedConds,
			expression.NewFunctionInternal(join.SCtx().GetExprCtx(), ast.EQ, types.NewFieldType(mysql.TypeTiny), key.innerCol, corCol))
	}
	apply.AttachOnConds(correlatedConds)
	ruleutil.BuildKeyInfoPortal(apply)
	return apply, true
}

// traceWindowBypassPath follows an inner-side column through the currently
// supported transparent operators and proves whether it can bypass exactly one
// Window barrier down to a datasource access column.
//
// Stage 1 intentionally keeps the operator set narrow:
//
//	Selection / passthrough Projection / Window / DataSource
//
// Future extensions can add more transparent operators here, and can introduce
// more barrier types such as Aggregation or DISTINCT with their own safety
// checks, without changing the rewrite logic above.
func traceWindowBypassPath(p base.LogicalPlan, col *expression.Column, windowSeen bool) (windowBypassPath, bool) {
	switch node := p.(type) {
	case *logicalop.LogicalSelection:
		nextCol := resolveColumnThroughTransparentNode(col, node.Children()[0])
		if nextCol == nil {
			return windowBypassPath{}, false
		}
		return traceWindowBypassPath(node.Children()[0], nextCol, windowSeen)
	case *logicalop.LogicalProjection:
		nextCol := resolveColumnThroughProjection(col, node)
		if nextCol == nil {
			return windowBypassPath{}, false
		}
		return traceWindowBypassPath(node.Children()[0], nextCol, windowSeen)
	case *logicalop.LogicalWindow:
		// Only one validated barrier is supported in the first stage.
		if windowSeen {
			return windowBypassPath{}, false
		}
		partitionSchema := expression.NewSchema(node.GetPartitionByCols()...)
		if !partitionSchema.Contains(col) {
			return windowBypassPath{}, false
		}
		nextCol := resolveColumnThroughTransparentNode(col, node.Children()[0])
		if nextCol == nil {
			return windowBypassPath{}, false
		}
		trace, ok := traceWindowBypassPath(node.Children()[0], nextCol, true)
		if !ok {
			return windowBypassPath{}, false
		}
		trace.window = node
		trace.partitionCol = col
		return trace, true
	case *logicalop.DataSource:
		if !windowSeen {
			return windowBypassPath{}, false
		}
		accessCol := resolveColumnThroughTransparentNode(col, node)
		if accessCol == nil {
			return windowBypassPath{}, false
		}
		return windowBypassPath{
			accessCol:  accessCol,
			dataSource: node,
		}, true
	default:
		// Future extension points:
		//   - schema-preserving transparent operators such as LogicalSort
		//   - additional validated barriers such as LogicalAggregation
		return windowBypassPath{}, false
	}
}

func containsLogicalCTE(p base.LogicalPlan) bool {
	if _, ok := p.(*logicalop.LogicalCTE); ok {
		return true
	}
	for _, child := range p.Children() {
		if containsLogicalCTE(child) {
			return true
		}
	}
	return false
}

func extractJoinKeyCols(eq *expression.ScalarFunction, leftSchema, rightSchema *expression.Schema) (*expression.Column, *expression.Column) {
	leftCol, leftOK := eq.GetArgs()[0].(*expression.Column)
	rightCol, rightOK := eq.GetArgs()[1].(*expression.Column)
	if !leftOK || !rightOK {
		return nil, nil
	}
	switch {
	case leftSchema.Contains(leftCol) && rightSchema.Contains(rightCol):
		return leftCol, rightCol
	case leftSchema.Contains(rightCol) && rightSchema.Contains(leftCol):
		return rightCol, leftCol
	default:
		return nil, nil
	}
}

// resolveColumnThroughTransparentNode rebinds a column to the child's schema
// for operators that preserve columns 1:1.
func resolveColumnThroughTransparentNode(col *expression.Column, child base.LogicalPlan) *expression.Column {
	return child.Schema().RetrieveColumn(col)
}

// resolveColumnThroughProjection follows a column through a pure passthrough
// projection. Any computed expression means this key cannot be correlated by
// the current rule.
func resolveColumnThroughProjection(col *expression.Column, proj *logicalop.LogicalProjection) *expression.Column {
	idx := proj.Schema().ColumnIndex(col)
	if idx < 0 {
		return nil
	}
	nextCol, ok := proj.Exprs[idx].(*expression.Column)
	if !ok {
		return nil
	}
	return nextCol
}

// dataSourceHasUsableTiKVAccessPath checks the already-pruned possible access
// paths instead of raw table metadata, so query-level index hints are respected.
func dataSourceHasUsableTiKVAccessPath(ds *logicalop.DataSource, keys []correlatableJoinKey) bool {
	candidateCols := make([]*expression.Column, 0, len(keys))
	for _, key := range keys {
		duplicated := false
		for _, col := range candidateCols {
			if col.EqualColumn(key.accessCol) {
				duplicated = true
				break
			}
		}
		if !duplicated {
			candidateCols = append(candidateCols, key.accessCol)
		}
	}
	for _, path := range ds.PossibleAccessPaths {
		if path.StoreType != kv.TiKV {
			continue
		}
		if path.IsTiKVTablePath() && hasHandleAccessPrefix(ds.HandleCols, candidateCols) {
			return true
		}
		if path.Index != nil && hasIndexAccessPrefix(path, candidateCols) {
			return true
		}
	}
	return false
}

func hasHandleAccessPrefix(handleCols planutil.HandleCols, candidateCols []*expression.Column) bool {
	if handleCols == nil {
		return false
	}
	for i := 0; i < handleCols.NumCols(); i++ {
		if !containsEqualColumn(candidateCols, handleCols.GetCol(i)) {
			return false
		}
	}
	return handleCols.NumCols() > 0
}

// hasIndexAccessPrefix requires at least one leading index column to be
// constrained by correlated equality before the rewrite is considered.
func hasIndexAccessPrefix(path *planutil.AccessPath, candidateCols []*expression.Column) bool {
	idxCols := path.IdxCols
	if len(idxCols) == 0 {
		idxCols = path.FullIdxCols
	}
	matchedPrefix := 0
	for _, idxCol := range idxCols {
		if idxCol == nil {
			break
		}
		if !containsEqualColumn(candidateCols, idxCol) {
			break
		}
		matchedPrefix++
	}
	return matchedPrefix > 0
}

func containsEqualColumn(cols []*expression.Column, target *expression.Column) bool {
	for _, col := range cols {
		if col.EqualColumn(target) {
			return true
		}
		if col.ID > 0 && target.ID > 0 && col.ID == target.ID {
			return true
		}
		// Some logical columns cloned through projection/window expansion no longer
		// share UniqueID with the datasource path columns. Restrict the fallback to
		// exact OrigName equality against already-pruned PossibleAccessPaths so it
		// cannot bypass query-level USE/IGNORE INDEX restrictions.
		if col.OrigName != "" && target.OrigName != "" && col.OrigName == target.OrigName {
			return true
		}
	}
	return false
}
