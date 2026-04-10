// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package memo

import (
	"container/list"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/cascades/pattern"
	"github.com/pingcap/tidb/pkg/planner/core/base"
)

// Exported types for testing

// ExportedGroup exports Group for testing
type ExportedGroup = Group

// ExportedGroupExpr exports GroupExpr for testing
type ExportedGroupExpr = GroupExpr

// ExportedNewGroupWithSchema exports NewGroupWithSchema for testing
func ExportedNewGroupWithSchema(e *GroupExpr, s *expression.Schema) *Group {
	return NewGroupWithSchema(e, s)
}

// ExportedNewGroupExpr exports NewGroupExpr for testing
func ExportedNewGroupExpr(node base.LogicalPlan) *GroupExpr {
	return NewGroupExpr(node)
}

// ExportedImplementation exports Implementation for testing
type ExportedImplementation = Implementation

// ExportedNewExprIterFromGroupElem exports NewExprIterFromGroupElem for testing
func ExportedNewExprIterFromGroupElem(elem *list.Element, p *pattern.Pattern) *ExprIter {
	return NewExprIterFromGroupElem(elem, p)
}

// ExportedConvert2Group exports Convert2Group for testing
func ExportedConvert2Group(logic base.LogicalPlan) *Group {
	return Convert2Group(logic)
}

// ExportedGetSelfFingerprint exports the selfFingerprint field for testing
func ExportedGetSelfFingerprint(expr *GroupExpr) string {
	return expr.selfFingerprint
}

// ExportedSetSelfFingerprint sets the selfFingerprint field for testing
func ExportedSetSelfFingerprint(expr *GroupExpr, fp string) {
	expr.selfFingerprint = fp
}

// ExportedExploreMark exports ExploreMark for testing
type ExportedExploreMark = ExploreMark
