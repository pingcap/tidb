// Copyright 2019 PingCAP, Inc.
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
	"bytes"
	"fmt"
	"github.com/pingcap/tidb/planner/memo"
	"strings"
)

// ToString stringifies a Group Tree.
func ToString(g *memo.Group) []string {
	idMap := make(map[*memo.Group]int)
	idMap[g] = 0
	return toString(g, idMap, map[*memo.Group]struct{}{}, []string{})
}

// toString recursively stringifies a Group Tree using a preorder traversal method.
func toString(g *memo.Group, idMap map[*memo.Group]int, visited map[*memo.Group]struct{}, strs []string) []string {
	if _, exists := visited[g]; exists {
		return strs
	}
	visited[g] = struct{}{}
	// Add new Groups to idMap.
	for item := g.Equivalents.Front(); item != nil; item = item.Next() {
		expr := item.Value.(*memo.GroupExpr)
		for _, childGroup := range expr.Children {
			if _, exists := idMap[childGroup]; !exists {
				idMap[childGroup] = len(idMap)
			}
		}
	}
	// Visit self first.
	strs = append(strs, groupToString(g, idMap)...)
	// Visit children then.
	for item := g.Equivalents.Front(); item != nil; item = item.Next() {
		expr := item.Value.(*memo.GroupExpr)
		for _, childGroup := range expr.Children {
			strs = toString(childGroup, idMap, visited, strs)
		}
	}
	return strs
}

// groupToString only stringifies a single Group.
// Format:
// Group#1 Columns:[a, b, c]
//     Selection_13 input:Group#2 gt(a, b)
//     Projection_15 input:Group#3 a, b, c
func groupToString(g *memo.Group, idMap map[*memo.Group]int) []string {
	result := make([]string, 0, g.Equivalents.Len()+1)
	result = append(result, fmt.Sprintf("Group#%d %s", idMap[g], g.Prop.Schema.String()))
	for item := g.Equivalents.Front(); item != nil; item = item.Next() {
		expr := item.Value.(*memo.GroupExpr)
		result = append(result, "    "+groupExprToString(expr, idMap))
	}
	return result
}

// groupExprToString stringifies a groupExpr(or a LogicalPlan).
// Format:
// Selection_13 input:Group#2 gt(Column#1, Column#4)
func groupExprToString(expr *memo.GroupExpr, idMap map[*memo.Group]int) string {
	buffer := bytes.NewBufferString(expr.ExprNode.ExplainID().String())
	if len(expr.Children) == 0 {
		fmt.Fprintf(buffer, " %s", expr.ExprNode.ExplainInfo())
	} else {
		fmt.Fprintf(buffer, " %s, %s", getChildrenGroupId(expr, idMap), expr.ExprNode.ExplainInfo())
	}
	return buffer.String()
}

func getChildrenGroupId(expr *memo.GroupExpr, idMap map[*memo.Group]int) string {
	children := make([]string, 0, len(expr.Children))
	for _, child := range expr.Children {
		children = append(children, fmt.Sprintf("Group#%d", idMap[child]))
	}
	if len(expr.Children) == 1 {
		return "input:" + children[0]
	}
	return "input:[" + strings.Join(children, ",") + "] "
}
