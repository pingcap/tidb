package core

import (
	"encoding/json"
	"github.com/pingcap/tidb/pkg/planner/core/base"
)

/*
explain analyze format='unity_plan' select * from t;

	{
		"time_cost": 10.1,
		""
	}
*/
func (e *Explain) unityPlan() (string, error) {
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

func (e *Explain) unitySubPlan() (subPlans []*ExplainInfoForEncode) {
	flat := FlattenPhysicalPlan(e.TargetPlan, true)
	var iterSubPlanFunc func(op *FlatOperator)
	iterSubPlanFunc = func(op *FlatOperator) {
		subPlans = append(subPlans, e.explainOpRecursivelyInJSONFormat(op, flat.Main))
		for _, childIdx := range op.ChildrenIdx {
			iterSubPlanFunc(flat.Main[childIdx])
		}
	}
	iterSubPlanFunc(flat.Main[0])
	return
}

type UnityPlan struct {
	PlanDigest string                  `json:"planDigest"`
	TimeInMS   float64                 `json:"TimeInMS"`
	MemInByte  int64                   `json:"memInByte"`
	SubPlans   []*ExplainInfoForEncode `json:"subPlans"`
}

func planDigest(p base.Plan) string {
	flat := FlattenPhysicalPlan(p, true)
	_, digest := NormalizeFlatPlan(flat)
	return digest.String()
}
