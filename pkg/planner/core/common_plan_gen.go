package core

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"

	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/util/hint"
)

func (e *Explain) unityPlanAll() (string, error) {
	p := parser.New()
	stmt, err := p.ParseOneStmt(e.SCtx().GetSessionVars().StmtCtx.OriginalSQL, "", "")
	if err != nil {
		return "", err
	}
	tableNames := bindinfo.CollectTableNames(stmt)
	for _, t := range tableNames {
		fmt.Println("?>>>>>>>>>> ", t.Name)
		fmt.Println(">>>>>> ", e.tableIndexNames(t))
	}
	return "", nil
}

func (e *Explain) tableIndexNames(t *ast.TableName) (idxNames []string) {
	is := domain.GetDomain(e.SCtx()).InfoSchema()
	schema := t.Schema
	if schema.L == "" {
		schema = model.NewCIStr(e.SCtx().GetSessionVars().CurrentDB)
	}
	tt, err := is.TableByName(context.Background(), schema, t.Name)
	if err != nil {
		panic(err)
	}
	for _, idx := range tt.Indices() {
		idxNames = append(idxNames, idx.Meta().Name.O)
	}
	return
}

func (e *Explain) unityPlanOne() (string, error) {
	up := new(UnityPlan)
	up.PlanDigest = planDigest(e.TargetPlan)
	rootStats, _, memTracker, _ := getRuntimeInfo(e.SCtx(), e.TargetPlan, e.RuntimeStatsColl)
	basicStats, _ := rootStats.MergeStats()
	up.TimeInMS = float64(basicStats.GetTime()) / 1e6
	up.MemInByte = memTracker.MaxConsumed()
	up.SubPlans = e.unitySubPlan()
	data, err := json.Marshal(up)
	return string(data), err
}

func (e *Explain) unitySubPlan() (subPlans []*UnityPlanNode) {
	flat := FlattenPhysicalPlan(e.TargetPlan, true)
	var iterSubPlanFunc func(op *FlatOperator)
	iterSubPlanFunc = func(op *FlatOperator) {
		if !op.IsRoot {
			return
		}
		explainNode := e.explainOpRecursivelyInJSONFormat(op, flat.Main)
		planNode := &UnityPlanNode{
			ExplainInfoForEncode: explainNode,
			Hints:                planHints(op.Origin),
		}
		subPlans = append(subPlans, planNode)
		for _, childIdx := range op.ChildrenIdx {
			iterSubPlanFunc(flat.Main[childIdx])
		}
	}
	iterSubPlanFunc(flat.Main[0])
	return
}

type UnityPlanNode struct {
	*ExplainInfoForEncode
	Hints string `json:"hints"`
}

type UnityPlan struct {
	PlanDigest string           `json:"planDigest"`
	TimeInMS   float64          `json:"TimeInMS"`
	MemInByte  int64            `json:"memInByte"`
	SubPlans   []*UnityPlanNode `json:"subPlans"`
}

func planHints(p base.Plan) string {
	flat := FlattenPhysicalPlan(p, true)
	hints := GenHintsFromFlatPlan(flat)
	return hint.RestoreOptimizerHints(hints)
}

func planDigest(p base.Plan) string {
	flat := FlattenPhysicalPlan(p, true)
	_, digest := NormalizeFlatPlan(flat)
	return digest.String()
}
