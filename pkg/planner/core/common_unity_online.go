package core

import "encoding/json"

func (e *Explain) UnityOnline() string {
	up := new(UnityOnlinePlan)
	up.SubPlans = e.unityOnlineSubPlan()
	data, err := json.Marshal(up)
	must(err)
	return string(data)
}

func (e *Explain) unityOnlineSubPlan() (subPlans []*UnityOnlinePlanNode) {
	flat := FlattenPhysicalPlan(e.TargetPlan, true)
	var iterSubPlanFunc func(op *FlatOperator)
	iterSubPlanFunc = func(op *FlatOperator) {
		if !op.IsRoot {
			return
		}
		explainNode := e.explainOpRecursivelyInJSONFormat(op, flat.Main)
		planNode := &UnityOnlinePlanNode{
			ExplainInfoForEncode: explainNode,
			PreSequence:          planPreSequences(op.Origin),
		}
		subPlans = append(subPlans, planNode)
		for _, childIdx := range op.ChildrenIdx {
			iterSubPlanFunc(flat.Main[childIdx])
		}
	}
	iterSubPlanFunc(flat.Main[0])
	return
}

type UnityOnlinePlan struct {
	SubPlans []*UnityOnlinePlanNode `json:"subPlans"`
}

type UnityOnlinePlanNode struct {
	*ExplainInfoForEncode
	PreSequence *UnityPreSequence `json:"preSequence"`
}
