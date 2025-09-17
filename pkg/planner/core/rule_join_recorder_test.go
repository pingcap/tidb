// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package core_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// ---------------- Mock ----------------

// LogicalPlan
type LogicalPlan interface {
	Name() string
	SelectOffset() int
}

// mockPlan
type mockPlan struct {
	name   string
	offset int
}

func (m *mockPlan) Name() string      { return m.name }
func (m *mockPlan) SelectOffset() int { return m.offset }
func newMockPlan(name string, offset int) *mockPlan {
	return &mockPlan{name: name, offset: offset}
}

// HintTable
type HintTable struct {
	DBName       string
	TblName      string
	SelectOffset int
}

// LeadingTableOrder
type LeadingTableOrder struct {
	Table *HintTable
	Left  *LeadingTableOrder
	Right *LeadingTableOrder
}

// PlanHints
type PlanHints struct {
	LeadingOrder *LeadingTableOrder
}

// ---------------- Solver ----------------

type JoinType int

const (
	InnerJoin JoinType = iota
)

// simplified version of baseSingleGroupJoinOrderSolver
type Solver struct{}

func newSolver() *Solver {
	return &Solver{}
}

// simplified version of generateLeadingPlan
func (s *Solver) generateLeadingPlan(curJoinGroup []LogicalPlan, hintInfo *PlanHints, hasOuterJoin bool) (bool, LogicalPlan, []LogicalPlan) {
	leadingPlan, remainingGroup, err := s.buildLeadingJoinTree(hintInfo.LeadingOrder, curJoinGroup, hasOuterJoin)
	if err != nil {
		return false, nil, nil
	}
	if leadingPlan == nil {
		return false, nil, nil
	}
	return true, leadingPlan, remainingGroup
}

// simplified version of buildLeadingJoinTree
func (s *Solver) buildLeadingJoinTree(order *LeadingTableOrder, curJoinGroup []LogicalPlan, hasOuterJoin bool) (LogicalPlan, []LogicalPlan, error) {
	if order == nil {
		return nil, curJoinGroup, nil
	}

	// leaf node
	if order.Table != nil {
		hintTbl := order.Table
		var plan LogicalPlan
		var remainingGroup []LogicalPlan
		matchFound := false

		for i, p := range curJoinGroup {
			if hintTbl.TblName == p.Name() && hintTbl.SelectOffset == p.SelectOffset() {
				plan = p
				remainingGroup = append(curJoinGroup[:i], curJoinGroup[i+1:]...)
				matchFound = true
				break
			}
		}
		if !matchFound {
			return nil, nil, fmt.Errorf("table %s in leading hint is not found", hintTbl.TblName)
		}
		return plan, remainingGroup, nil
	}

	// recursively build left and right subtree
	leftPlan, leftRemaining, err := s.buildLeadingJoinTree(order.Left, curJoinGroup, hasOuterJoin)
	if err != nil {
		return nil, nil, err
	}
	rightPlan, rightRemaining, err := s.buildLeadingJoinTree(order.Right, leftRemaining, hasOuterJoin)
	if err != nil {
		return nil, nil, err
	}

	if hasOuterJoin && (leftPlan == nil || rightPlan == nil) {
		return nil, nil, fmt.Errorf("cartesian join not allowed with outer join")
	}

	// always make leftPlan the left side, rightPlan the right side
	joinPlan := &mockPlan{
		name:   fmt.Sprintf("join(%s,%s)", leftPlan.Name(), rightPlan.Name()),
		offset: 0,
	}

	return joinPlan, rightRemaining, nil
}

// ---------------- test cases ----------------

func TestGenerateLeadingPlan_Basic(t *testing.T) {
	solver := newSolver()
	curGroup := []LogicalPlan{
		newMockPlan("a", 0),
		newMockPlan("b", 0),
	}
	order := &LeadingTableOrder{
		Left:  &LeadingTableOrder{Table: &HintTable{TblName: "a", SelectOffset: 0}},
		Right: &LeadingTableOrder{Table: &HintTable{TblName: "b", SelectOffset: 0}},
	}
	h := &PlanHints{LeadingOrder: order}

	ok, plan, remaining := solver.generateLeadingPlan(curGroup, h, false)
	require.True(t, ok)
	require.NotNil(t, plan)
	require.Equal(t, "join(a,b)", plan.Name())
	require.Len(t, remaining, 0)
}

func TestGenerateLeadingPlan_TableNotFound(t *testing.T) {
	solver := newSolver()
	curGroup := []LogicalPlan{
		newMockPlan("a", 0),
	}
	order := &LeadingTableOrder{Table: &HintTable{TblName: "x", SelectOffset: 0}}
	h := &PlanHints{LeadingOrder: order}

	ok, plan, remaining := solver.generateLeadingPlan(curGroup, h, false)
	require.False(t, ok)
	require.Nil(t, plan)
	require.Nil(t, remaining)
}

func TestGenerateLeadingPlan_Nested(t *testing.T) {
	solver := newSolver()
	curGroup := []LogicalPlan{
		newMockPlan("a", 0),
		newMockPlan("b", 0),
		newMockPlan("c", 0),
		newMockPlan("d", 0),
	}
	order := &LeadingTableOrder{
		Left: &LeadingTableOrder{
			Left:  &LeadingTableOrder{Table: &HintTable{TblName: "a", SelectOffset: 0}},
			Right: &LeadingTableOrder{Table: &HintTable{TblName: "b", SelectOffset: 0}},
		},
		Right: &LeadingTableOrder{
			Left:  &LeadingTableOrder{Table: &HintTable{TblName: "c", SelectOffset: 0}},
			Right: &LeadingTableOrder{Table: &HintTable{TblName: "d", SelectOffset: 0}},
		},
	}
	h := &PlanHints{LeadingOrder: order}

	ok, plan, remaining := solver.generateLeadingPlan(curGroup, h, false)
	require.True(t, ok)
	require.NotNil(t, plan)
	require.Equal(t, "join(join(a,b),join(c,d))", plan.Name())
	require.Len(t, remaining, 0)
}

func TestGenerateLeadingPlan_EmptyOrder(t *testing.T) {
	solver := newSolver()
	curGroup := []LogicalPlan{
		newMockPlan("a", 0),
	}
	h := &PlanHints{LeadingOrder: nil}

	ok, plan, remaining := solver.generateLeadingPlan(curGroup, h, false)
	require.False(t, ok)
	require.Nil(t, plan)
	require.Nil(t, remaining)
}
