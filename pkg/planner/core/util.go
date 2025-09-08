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
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/util/set"
)

// AggregateFuncExtractor visits Expr tree.
// It collects AggregateFuncExpr from AST Node.
type AggregateFuncExtractor struct {
	// skipAggMap stores correlated aggregate functions which have been built in outer query,
	// so extractor in sub-query will skip these aggregate functions.
	skipAggMap map[*ast.AggregateFuncExpr]*expression.CorrelatedColumn
	// AggFuncs is the collected AggregateFuncExprs.
	AggFuncs []*ast.AggregateFuncExpr
}

// Enter implements Visitor interface.
func (*AggregateFuncExtractor) Enter(n ast.Node) (ast.Node, bool) {
	switch n.(type) {
	case *ast.SelectStmt, *ast.SetOprStmt:
		return n, true
	}
	return n, false
}

// Leave implements Visitor interface.
func (a *AggregateFuncExtractor) Leave(n ast.Node) (ast.Node, bool) {
	//nolint: revive
	switch v := n.(type) {
	case *ast.AggregateFuncExpr:
		if _, ok := a.skipAggMap[v]; !ok {
			a.AggFuncs = append(a.AggFuncs, v)
		}
	}
	return n, true
}

// WindowFuncExtractor visits Expr tree.
// It converts ColumnNameExpr to WindowFuncExpr and collects WindowFuncExpr.
type WindowFuncExtractor struct {
	// WindowFuncs is the collected WindowFuncExprs.
	windowFuncs []*ast.WindowFuncExpr
}

// Enter implements Visitor interface.
func (*WindowFuncExtractor) Enter(n ast.Node) (ast.Node, bool) {
	switch n.(type) {
	case *ast.SelectStmt, *ast.SetOprStmt:
		return n, true
	}
	return n, false
}

// Leave implements Visitor interface.
func (a *WindowFuncExtractor) Leave(n ast.Node) (ast.Node, bool) {
	//nolint: revive
	switch v := n.(type) {
	case *ast.WindowFuncExpr:
		a.windowFuncs = append(a.windowFuncs, v)
	}
	return n, true
}

// BuildPhysicalJoinSchema builds the schema of PhysicalJoin from it's children's schema.
func BuildPhysicalJoinSchema(joinType base.JoinType, join base.PhysicalPlan) *expression.Schema {
	leftSchema := join.Children()[0].Schema()
	switch joinType {
	case base.SemiJoin, base.AntiSemiJoin:
		return leftSchema.Clone()
	case base.LeftOuterSemiJoin, base.AntiLeftOuterSemiJoin:
		newSchema := leftSchema.Clone()
		newSchema.Append(join.Schema().Columns[join.Schema().Len()-1])
		return newSchema
	}
	newSchema := expression.MergeSchema(leftSchema, join.Children()[1].Schema())
	if joinType == base.LeftOuterJoin {
		util.ResetNotNullFlag(newSchema, leftSchema.Len(), newSchema.Len())
	} else if joinType == base.RightOuterJoin {
		util.ResetNotNullFlag(newSchema, 0, leftSchema.Len())
	}
	return newSchema
}

// GetStatsInfo gets the statistics info from a physical plan tree.
func GetStatsInfo(i any) map[string]uint64 {
	if i == nil {
		// it's a workaround for https://github.com/pingcap/tidb/issues/17419
		// To entirely fix this, uncomment the assertion in TestPreparedIssue17419
		return nil
	}
	p := i.(base.Plan)
	var physicalPlan base.PhysicalPlan
	switch x := p.(type) {
	case *Insert:
		physicalPlan = x.SelectPlan
	case *Update:
		physicalPlan = x.SelectPlan
	case *Delete:
		physicalPlan = x.SelectPlan
	case base.PhysicalPlan:
		physicalPlan = x
	}

	if physicalPlan == nil {
		return nil
	}

	statsInfos := make(map[string]uint64)
	statsInfos = CollectPlanStatsVersion(physicalPlan, statsInfos)
	return statsInfos
}

// extractStringFromStringSet helps extract string info from set.StringSet.
func extractStringFromStringSet(set set.StringSet) string {
	if len(set) < 1 {
		return ""
	}
	l := make([]string, 0, len(set))
	for k := range set {
		l = append(l, fmt.Sprintf(`"%s"`, k))
	}
	slices.Sort(l)
	return strings.Join(l, ",")
}

// extractStringFromStringSlice helps extract string info from []string.
func extractStringFromStringSlice(ss []string) string {
	if len(ss) < 1 {
		return ""
	}
	slices.Sort(ss)
	return strings.Join(ss, ",")
}

// extractStringFromUint64Slice helps extract string info from uint64 slice.
func extractStringFromUint64Slice(slice []uint64) string {
	if len(slice) < 1 {
		return ""
	}
	l := make([]string, 0, len(slice))
	for _, k := range slice {
		l = append(l, fmt.Sprintf(`%d`, k))
	}
	slices.Sort(l)
	return strings.Join(l, ",")
}

// extractStringFromBoolSlice helps extract string info from bool slice.
func extractStringFromBoolSlice(slice []bool) string {
	if len(slice) < 1 {
		return ""
	}
	l := make([]string, 0, len(slice))
	for _, k := range slice {
		l = append(l, fmt.Sprintf(`%t`, k))
	}
	slices.Sort(l)
	return strings.Join(l, ",")
}

func tableHasDirtyContent(ctx base.PlanContext, tableInfo *model.TableInfo) bool {
	pi := tableInfo.GetPartitionInfo()
	if pi == nil {
		return ctx.HasDirtyContent(tableInfo.ID)
	}
	// Currently, we add UnionScan on every partition even though only one partition's data is changed.
	// This is limited by current implementation of Partition Prune. It'll be updated once we modify that part.
	for _, partition := range pi.Definitions {
		if ctx.HasDirtyContent(partition.ID) {
			return true
		}
	}
	return false
}
