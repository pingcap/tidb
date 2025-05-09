package core

import (
	"encoding/json"

	"github.com/pingcap/tidb/pkg/util/texttree"
)

func (e *Explain) UnityOnline() string {
	node := e.unityOnlineSubPlan()
	data, err := json.Marshal(node)
	must(err)
	return string(data)
}

func (e *Explain) unityOnlineSubPlan() *UnityOnlinePlanNode {
	flat := FlattenPhysicalPlan(e.TargetPlan, true)
	var iterSubPlanFunc func(op *FlatOperator) *UnityOnlinePlanNode
	iterSubPlanFunc = func(flatOp *FlatOperator) *UnityOnlinePlanNode {
		if !flatOp.IsRoot {
			return nil
		}

		taskTp := ""
		if flatOp.IsRoot {
			taskTp = "root"
		} else {
			taskTp = flatOp.ReqType.Name() + "[" + flatOp.StoreType.Name() + "]"
		}
		explainID := flatOp.Origin.ExplainID().String() + flatOp.Label.String()
		textTreeExplainID := texttree.PrettyIdentifier(explainID, flatOp.TextTreeIndent, flatOp.IsLastChild)

		estRows, estCost, _, accessObject, operatorInfo := e.getOperatorInfo(flatOp.Origin, textTreeExplainID)
		preSequence := planPreSequences(flatOp.Origin)

		node := &UnityOnlinePlanNode{
			ID:           explainID,
			EstRows:      estRows,
			TaskType:     taskTp,
			AccessObject: accessObject,
			OperatorInfo: operatorInfo,
			EstCost:      estCost,
			PreSequence:  preSequence,
		}
		for _, childIdx := range flatOp.ChildrenIdx {
			if childOp := iterSubPlanFunc(flat.Main[childIdx]); childOp != nil {
				node.SubOperators = append(node.SubOperators, childOp)
			}
		}
		return node
	}
	return iterSubPlanFunc(flat.Main[0])
}

type UnityOnlinePlanNode struct {
	ID           string                 `json:"id"`
	EstRows      string                 `json:"estRows"`
	TaskType     string                 `json:"taskType"`
	AccessObject string                 `json:"accessObject,omitempty"`
	OperatorInfo string                 `json:"operatorInfo,omitempty"`
	EstCost      string                 `json:"estCost,omitempty"`
	PreSequence  *UnityPreSequence      `json:"preSequence"`
	SubOperators []*UnityOnlinePlanNode `json:"subOperators,omitempty"`
}
