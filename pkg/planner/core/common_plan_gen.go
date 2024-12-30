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
	runtimeStats, _ := e.RuntimeStatsColl.GetRootStats(e.TargetPlan.ID()).MergeStats()
	up.TimeInMS = float64(runtimeStats.GetTime())

	data, err := json.Marshal(up)
	return string(data), err
}

type unityPlan struct {
	PlanDigest string
	TimeInMS   float64
}

func planDigest(p base.Plan) string {
	flat := FlattenPhysicalPlan(p, true)
	_, digest := NormalizeFlatPlan(flat)
	return digest.String()
}
