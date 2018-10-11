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
	"fmt"

	plannercore "github.com/pingcap/tidb/planner/core"
)

type GroupExpr struct {
	exprNode plannercore.LogicalPlan
	children []*Group
	explored bool

	selfFingerprint string
}

func NewGroupExpr(node plannercore.LogicalPlan) *GroupExpr {
	return &GroupExpr{
		exprNode: node,
		children: nil,
		explored: false,
	}
}

func (e *GroupExpr) FingerPrint() string {
	if e.selfFingerprint == "" {
		e.selfFingerprint = fmt.Sprintf("%v", e.exprNode.ID())
		for i := range e.children {
			e.selfFingerprint += e.children[i].FingerPrint()
		}
	}
	return e.selfFingerprint
}
