// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package cascades

import (
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/planner/memo"
	"github.com/pingcap/tidb/planner/property"
)

// ImplementationRule defines the interface for implementation rules.
type ImplementationRule interface {
	// Match checks if current GroupExpr matches this rule under required physical property.
	Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool)
	// OnImplement generates physical plan using this rule for current GroupExpr. Note that
	// childrenReqProps of generated physical plan should be set correspondingly in this function.
	OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) (impl memo.Implementation, err error)
}

// GetImplementationRules gets the all the candidate implementation rules based
// on the logical plan node.
func GetImplementationRules(node plannercore.LogicalPlan) []ImplementationRule {
	operand := memo.GetOperand(node)
	return implementationMap[operand]
}

var implementationMap = map[memo.Operand][]ImplementationRule{
	/**
	OperandSelection: []ImplementationRule{
		nil,
	},
	OperandProjection: []ImplementationRule{
		nil,
	},
	*/
}
