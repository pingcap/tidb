package core

import (
	"encoding/json"

	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/util/texttree"
)

func (e *Explain) UnityOnline() string {
	node := e.unityOnlineSubPlan(e.TargetPlan.(base.PhysicalPlan))
	data, err := json.Marshal(node)
	must(err)
	return string(data)
}

func (e *Explain) unityOnlineSubPlan(op base.PhysicalPlan) *UnityOnlinePlanNode {
	flat := FlattenPhysicalPlan(e.TargetPlan, true)
	flatOp := flat.Main[0]
	taskTp := ""
	if flatOp.IsRoot {
		taskTp = "root"
	} else {
		taskTp = flatOp.ReqType.Name() + "[" + flatOp.StoreType.Name() + "]"
	}
	explainID := flatOp.Origin.ExplainID().String() + flatOp.Label.String()
	textTreeExplainID := texttree.PrettyIdentifier(explainID, flatOp.TextTreeIndent, flatOp.IsLastChild)

	estRows, estCost, _, accessObject, operatorInfo := e.getOperatorInfo(op, textTreeExplainID)
	preSequence := planPreSequences(op)

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
		node.SubOperators = append(node.SubOperators, e.unityOnlineSubPlan(flat.Main[childIdx].Origin.(base.PhysicalPlan)))
	}
	return node
}

type UnityOnlinePlanNode struct {
	ID           string                 `json:"id"`
	EstRows      string                 `json:"estRows"`
	TaskType     string                 `json:"taskType"`
	AccessObject string                 `json:"accessObject,omitempty"`
	OperatorInfo string                 `json:"operatorInfo,omitempty"`
	EstCost      string                 `json:"estCost,omitempty"`
	SubOperators []*UnityOnlinePlanNode `json:"subOperators,omitempty"`
	PreSequence  *UnityPreSequence      `json:"preSequence"`
}
