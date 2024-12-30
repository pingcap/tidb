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
	up := new(unityPlan)
	up.PlanDigest = planDigest(e.TargetPlan)
	rootStats, _, memTracker, _ := getRuntimeInfo(e.SCtx(), e.TargetPlan, e.RuntimeStatsColl)
	basicStats, _ := rootStats.MergeStats()
	up.TimeInMS = float64(basicStats.GetTime()) / 1e6
	up.MemInByte = memTracker.MaxConsumed()
	data, err := json.Marshal(up)
	return string(data), err
}

type unityPlan struct {
	PlanDigest string
	TimeInMS   float64
	MemInByte  int64
}

func planDigest(p base.Plan) string {
	flat := FlattenPhysicalPlan(p, true)
	_, digest := NormalizeFlatPlan(flat)
	return digest.String()
}
