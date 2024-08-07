// Copyright 2024 PingCAP, Inc.
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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

var annIndexFnNameToMetric = map[string]tipb.VectorDistanceMetric{
	strings.ToLower(ast.VecL1Distance):     tipb.VectorDistanceMetric_L1,
	strings.ToLower(ast.VecL2Distance):     tipb.VectorDistanceMetric_L2,
	strings.ToLower(ast.VecCosineDistance): tipb.VectorDistanceMetric_COSINE,

	// Note: IP is not supported yet. Currently we will throw errors when building IP index.
	strings.ToLower(ast.VecNegativeInnerProduct): tipb.VectorDistanceMetric_INNER_PRODUCT,
}

var annIndexIsAscending = map[tipb.VectorDistanceMetric]bool{
	tipb.VectorDistanceMetric_L2:            true,
	tipb.VectorDistanceMetric_COSINE:        true,
	tipb.VectorDistanceMetric_L1:            true,
	tipb.VectorDistanceMetric_INNER_PRODUCT: false,
}

func getDistanceMetricProto(m model.DistanceMetric) (tipb.VectorDistanceMetric, error) {
	ev, ok := tipb.VectorDistanceMetric_value[string(m)]
	if !ok {
		return tipb.VectorDistanceMetric_INVALID_DISTANCE_METRIC,
			errors.Errorf("unknown distance metric '%s'", string(m))
	}
	return tipb.VectorDistanceMetric(ev), nil
}

func isVecDistanceFnUsed(expr expression.Expression) bool {
	candidates := []expression.Expression{expr}
	for len(candidates) > 0 {
		current := candidates[0]
		if currentFn, ok := current.(*expression.ScalarFunction); ok {
			if _, isIndexable := annIndexFnNameToMetric[currentFn.FuncName.L]; isIndexable {
				return true
			}
			candidates = append(candidates, currentFn.GetArgs()...)
		}
		candidates = candidates[1:]
	}
	return false
}

type annIndexHintVisitor struct {
	parents []PhysicalPlan
	sctx    sessionctx.Context

	indexIsHit bool

	needExplainMissReason bool

	lastIndexMissReason         string
	lastIndexMissReasonPriority int
}

func (v *annIndexHintVisitor) explainIndexMissReason(priority int, reason string) {
	if priority > v.lastIndexMissReasonPriority {
		v.lastIndexMissReason = reason
		v.lastIndexMissReasonPriority = priority
	}
}

func (v *annIndexHintVisitor) visit(plan PhysicalPlan) {
	v.onEnter(plan)
	v.parents = append(v.parents, plan)
	for _, child := range plan.Children() {
		v.visit(child)
	}
	switch x := plan.(type) {
	case *PhysicalTableReader:
		v.visit(x.tablePlan)
	}
	v.parents = v.parents[:len(v.parents)-1]
}

func (v *annIndexHintVisitor) getParent(n int) PhysicalPlan {
	idx := len(v.parents) - 1 - n
	if idx >= 0 {
		return v.parents[idx]
	}
	return nil
}

func (v *annIndexHintVisitor) onEnter(plan PhysicalPlan) {
	switch plan.(type) {
	case *PhysicalProjection:
		v.onEnterProjection(plan)
	case *PhysicalTableScan:
		v.onEnterTableScan(plan)
	}
}

func (v *annIndexHintVisitor) onEnterProjection(plan PhysicalPlan) {
	// Set needExplainMissReason as long as vector distance function
	// is mentioned in projection.
	if v.needExplainMissReason {
		return
	}
	if !v.sctx.GetSessionVars().StmtCtx.InExplainStmt {
		return
	}
	projection := plan.(*PhysicalProjection)
	for _, expr := range projection.Exprs {
		if isVecDistanceFnUsed(expr) {
			v.needExplainMissReason = true
			return
		}
	}
}

func (v *annIndexHintVisitor) onEnterTableScan(plan PhysicalPlan) bool {
	tableScan := plan.(*PhysicalTableScan)
	if tableScan.StoreType != kv.TiFlash {
		v.explainIndexMissReason(1, "not scanning over TiFlash")
		return false
	}

	if !tableScan.Table.HasVectorIndex {
		v.explainIndexMissReason(2, "table does not have ANN index")
		return false
	}

	parent1 := v.getParent(0)
	if parent1 == nil {
		v.explainIndexMissReason(2, "only Top N queries (like ORDER BY ... LIMIT ...) can use ANN index")
		return false
	}
	parentProjection, ok := parent1.(*PhysicalProjection)
	if !ok {
		if _, ok := parent1.(*PhysicalSelection); ok {
			// Give some more friendly message.
			v.explainIndexMissReason(3, "cannot utilize ANN index when there is a WHERE or HAVING clause")
		} else {
			v.explainIndexMissReason(2, "only Top N queries (like ORDER BY ... LIMIT ...) can use ANN index")
		}
		return false
	}
	parent2 := v.getParent(1)
	if parent2 == nil {
		v.explainIndexMissReason(2, "only Top N queries (like ORDER BY ... LIMIT ...) can use ANN index")
		return false
	}
	parentTopN, ok := parent2.(*PhysicalTopN)
	if !ok {
		v.explainIndexMissReason(2, "only Top N queries (like ORDER BY ... LIMIT ...) can use ANN index")
		return false
	}
	if len(parentTopN.ByItems) != 1 {
		v.explainIndexMissReason(4, "order by multiple expressions")
		return false
	}

	// We expect ordering by a column generated by the projection executor and
	// the column is projected from a vector distance function.
	// Example:
	// └─TopN_17                  | Column#3, offset:0, count:5                                    |
	//   └─Projection_21          | test.t2.val, vec_cosine_distance(test.t2.val, [4,5])->Column#3 |
	//     └─TableFullScan_16     | keep order:false, stats:pseudo                                 |
	orderByCol, ok := parentTopN.ByItems[0].Expr.(*expression.Column)
	if !ok {
		v.explainIndexMissReason(5, "order by a non-projected column is not supported yet")
		return false
	}

	if orderByCol.Index < 0 || orderByCol.Index >= len(parentProjection.Exprs) {
		v.explainIndexMissReason(6, "internal error, unknown order by column")
		return false
	}

	orderByExpr := parentProjection.Exprs[orderByCol.Index]
	annQueryInfo, err := v.extractANNQueryFromExpr(orderByExpr)
	if annQueryInfo == nil || err != nil {
		if err != nil {
			v.explainIndexMissReason(7, "internal error, "+err.Error())
		} else {
			v.explainIndexMissReason(7, "not ordering by a vector distance function")
		}
		return false
	}

	// Check whether the ORDER BY column has defined ANN index.
	{
		columnIsChecked := false

		for _, col := range tableScan.Columns {
			if col.ID != annQueryInfo.ColumnId {
				continue
			}
			if col.VectorIndex == nil {
				v.explainIndexMissReason(8, fmt.Sprintf("column '%s' does not have ANN index", col.Name.O))
				return false
			}
			distanceMetric, err := getDistanceMetricProto(col.VectorIndex.DistanceMetric)
			if err != nil {
				v.explainIndexMissReason(9, "internal error when parsing column index info, "+err.Error())
				return false
			}
			if distanceMetric != annQueryInfo.DistanceMetric {
				v.explainIndexMissReason(10, fmt.Sprintf(
					"not ordering by %s distance",
					distanceMetric.String()))
				return false
			}
			columnIsChecked = true
			break
		}

		if !columnIsChecked {
			v.explainIndexMissReason(11, "internal error, cannot find referenced column")
			return false
		}
	}

	orderByAsc := !parentTopN.ByItems[0].Desc
	orderByFn := orderByExpr.(*expression.ScalarFunction).FuncName.L
	if orderByAsc != annIndexIsAscending[annQueryInfo.DistanceMetric] {
		expectedOrderStr := "ASC"
		if !annIndexIsAscending[annQueryInfo.DistanceMetric] {
			expectedOrderStr = "DESC"
		}
		v.explainIndexMissReason(12, fmt.Sprintf(
			"index can be used only when ordering by %s() in %s order",
			orderByFn,
			expectedOrderStr))
		return false
	}

	logutil.BgLogger().Debug("planner: add ANN index hint to table scan",
		zap.String("table", tableScan.Table.Name.L),
		zap.String("order_by", expression.ExplainExpressionList([]expression.Expression{orderByExpr}, tableScan.Schema())),
		zap.Bool("order_by_asc", orderByAsc),
		zap.Uint64("top_n", parentTopN.Count),
	)

	annQueryInfo.QueryType = tipb.ANNQueryType_OrderBy
	annQueryInfo.TopK = uint32(parentTopN.Count)
	tableScan.annQuery = annQueryInfo
	v.indexIsHit = true
	// Update cost
	tableScan.StatsInfo().RowCount = parentTopN.StatsInfo().RowCount

	// Mark some info for this statement. They will be used to produce telemetry
	// metrics after execution.
	v.sctx.GetSessionVars().StmtCtx.VectorSearchIsANNQuery = true
	v.sctx.GetSessionVars().StmtCtx.VectorSearchTopK = uint32(parentTopN.Count)

	return true
}

// extractANNQueryFromExpr returns nil if the expression is not a vector distance function.
// We only support expressions like:
// VEC_Cosine_Distance(v, '[4.0, 5.0]')
// or
// VEC_Cosine_Distance('[4.0, 5.0]', v)
func (v *annIndexHintVisitor) extractANNQueryFromExpr(expr expression.Expression) (*tipb.ANNQueryInfo, error) {
	x, ok := expr.(*expression.ScalarFunction)
	if !ok {
		return nil, nil
	}

	distanceFnName := x.FuncName.L
	if _, isIndexable := annIndexFnNameToMetric[distanceFnName]; !isIndexable {
		return nil, nil
	}

	args := x.GetArgs()
	if len(args) != 2 {
		return nil, errors.Errorf("internal: expect 2 args for function %s, but got %d", x.FuncName.L, len(args))
	}
	// One arg must be a vector column ref, and one arg must be a vector constant.
	// Note: this must be run after constant folding.

	var vectorConstant *expression.Constant = nil
	var vectorColumn *expression.Column = nil
	nVectorColumns := 0
	nVectorConstants := 0
	for _, arg := range args {
		if v, ok := arg.(*expression.Column); ok {
			if v.RetType.GetType() != mysql.TypeTiDBVectorFloat32 {
				break
			}
			vectorColumn = v
			nVectorColumns++
		} else if v, ok := arg.(*expression.Constant); ok {
			if v.RetType.GetType() != mysql.TypeTiDBVectorFloat32 {
				break
			}
			vectorConstant = v
			nVectorConstants++
		}
	}
	if nVectorColumns != 1 || nVectorConstants != 1 {
		return nil, nil
	}

	// All check passed.
	if vectorConstant.Value.Kind() != types.KindVectorFloat32 {
		return nil, errors.Errorf("internal: expect vectorFloat32 constant, but got %s", vectorConstant.Value.String())
	}

	queryInfo := &tipb.ANNQueryInfo{}
	queryInfo.RefVecF32 = vectorConstant.Value.GetVectorFloat32().SerializeTo(nil)
	queryInfo.ColumnName = vectorColumn.OrigName
	queryInfo.ColumnId = vectorColumn.ID
	queryInfo.DistanceMetric = annIndexFnNameToMetric[distanceFnName]

	return queryInfo, nil
}

// addANNIndexHintToTableScan tries to add ANN index hint to table scan.
// When TiFlash sees the ANN index hint, it will try to lookup in the ANN index.
// It works for the following plan:
//
//	TableScan(#vec) -> Projection(distance(#vec)→#d) -> TopN(#d)
func addANNIndexHintToTableScan(sctx sessionctx.Context, plan PhysicalPlan) bool {
	v := &annIndexHintVisitor{
		sctx: sctx,
	}
	v.visit(plan)
	if !v.indexIsHit && v.needExplainMissReason {
		if v.lastIndexMissReason == "" {
			v.lastIndexMissReason = "not table scan"
		}
		logutil.BgLogger().Debug("planner: ANN index not used",
			zap.String("reason", v.lastIndexMissReason))
		if sctx.GetSessionVars().StmtCtx.InExplainStmt {
			sctx.GetSessionVars().StmtCtx.AppendWarning(errors.New("ANN index not used: " + v.lastIndexMissReason))
		}
	}
	return v.indexIsHit
}
