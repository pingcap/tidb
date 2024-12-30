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
	data, err := json.Marshal(up)
	return string(data), err
}

func (e *Explain) unitySubPlan() {

}

type UnityPlanNode struct {
	ID                  string                  `json:"id"`
	EstRows             string                  `json:"estRows"`
	ActRows             string                  `json:"actRows,omitempty"`
	TaskType            string                  `json:"taskType"`
	AccessObject        string                  `json:"accessObject,omitempty"`
	ExecuteInfo         string                  `json:"executeInfo,omitempty"`
	OperatorInfo        string                  `json:"operatorInfo,omitempty"`
	EstCost             string                  `json:"estCost,omitempty"`
	MemoryInfo          string                  `json:"memoryInfo,omitempty"`
	TotalMemoryConsumed string                  `json:"totalMemoryConsumed,omitempty"`
	SubOperators        []*ExplainInfoForEncode `json:"subOperators,omitempty"`
}

type UnityPlan struct {
	PlanDigest string           `json:"plan_digest"`
	TimeInMS   float64          `json:"time_in_ms"`
	MemInByte  int64            `json:"mem_in_byte"`
	SubPlans   []*UnityPlanNode `json:"sub_plans"`
}

func planDigest(p base.Plan) string {
	flat := FlattenPhysicalPlan(p, true)
	_, digest := NormalizeFlatPlan(flat)
	return digest.String()
}
