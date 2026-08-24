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
// See the License for the specific language governing permissions and
// limitations under the License.

package ast

import (
	"io"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/stretchr/testify/require"
)

type partitionLegacyVisitor struct {
	names        map[Node]string
	events       []string
	skip         Node
	stop         Node
	replacements map[Node]Node
	mutate       func(Node)
}

func (v *partitionLegacyVisitor) Enter(n Node) (Node, bool) {
	v.events = append(v.events, "enter "+v.names[n])
	if v.mutate != nil {
		v.mutate(n)
	}
	return n, n == v.skip
}

func (v *partitionLegacyVisitor) Leave(n Node) (Node, bool) {
	v.events = append(v.events, "leave "+v.names[n])
	if replacement := v.replacements[n]; replacement != nil {
		return replacement, n != v.stop
	}
	return n, n != v.stop
}

type partitionInPlaceVisitor struct {
	names  map[Node]string
	events []string
	skip   Node
	stop   Node
	mutate func(Node)
}

func (v *partitionInPlaceVisitor) Enter(n Node) bool {
	v.events = append(v.events, "enter "+v.names[n])
	if v.mutate != nil {
		v.mutate(n)
	}
	return n == v.skip
}

func (v *partitionInPlaceVisitor) Leave(n Node) bool {
	v.events = append(v.events, "leave "+v.names[n])
	return n != v.stop
}

// partitionReturningExpr exposes whether a helper assigns the Node returned by
// Accept. Its return value deliberately differs from the node seen by the
// visitor, as an unsupported external node is allowed to own its fallback.
type partitionReturningExpr struct {
	exprNode
	returned ExprNode
}

func (*partitionReturningExpr) Restore(*format.RestoreCtx) error { return nil }
func (*partitionReturningExpr) Format(io.Writer)                 {}

func (n *partitionReturningExpr) Accept(v Visitor) (Node, bool) {
	newNode, _ := v.Enter(n)
	_, ok := v.Leave(newNode)
	if !ok {
		return n, false
	}
	return n.returned, true
}

func TestPartitionVisitorHelpersPreserveChildOrder(t *testing.T) {
	exprColumn := &ColumnName{}
	expr := &ColumnNameExpr{Name: exprColumn}
	columnA := &ColumnName{}
	columnB := &ColumnName{}
	method := &PartitionMethod{Expr: expr, ColumnNames: []*ColumnName{columnA, columnB}}
	names := map[Node]string{
		expr:       "expr",
		exprColumn: "expr column",
		columnA:    "column A",
		columnB:    "column B",
	}
	expected := []string{
		"enter expr",
		"enter expr column",
		"leave expr column",
		"leave expr",
		"enter column A",
		"leave column A",
		"enter column B",
		"leave column B",
	}

	legacy := &partitionLegacyVisitor{names: names}
	require.True(t, method.acceptWithVisitor(legacy))
	require.Equal(t, expected, legacy.events)

	inPlace := &partitionInPlaceVisitor{names: names}
	require.True(t, method.walkInPlace(inPlace))
	require.Equal(t, expected, inPlace.events)
}

func TestPartitionDefinitionVisitorHelpersPreserveClauseOrder(t *testing.T) {
	lessA := &DefaultExpr{}
	lessB := &DefaultExpr{}
	inA := &DefaultExpr{}
	inB := &DefaultExpr{}
	inC := &DefaultExpr{}

	testCases := []struct {
		name       string
		definition *PartitionDefinition
		names      map[Node]string
		expected   []string
	}{
		{
			name:       "none",
			definition: &PartitionDefinition{Clause: &PartitionDefinitionClauseNone{}},
		},
		{
			name: "less_than",
			definition: &PartitionDefinition{Clause: &PartitionDefinitionClauseLessThan{
				Exprs: []ExprNode{lessA, lessB},
			}},
			names: map[Node]string{lessA: "less A", lessB: "less B"},
			expected: []string{
				"enter less A", "leave less A",
				"enter less B", "leave less B",
			},
		},
		{
			name: "in_row_major_order",
			definition: &PartitionDefinition{Clause: &PartitionDefinitionClauseIn{
				Values: [][]ExprNode{{inA, inB}, {inC}},
			}},
			names: map[Node]string{inA: "in A", inB: "in B", inC: "in C"},
			expected: []string{
				"enter in A", "leave in A",
				"enter in B", "leave in B",
				"enter in C", "leave in C",
			},
		},
		{
			name:       "history",
			definition: &PartitionDefinition{Clause: &PartitionDefinitionClauseHistory{}},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			legacy := &partitionLegacyVisitor{names: testCase.names}
			require.True(t, testCase.definition.acceptWithVisitor(legacy))
			require.Equal(t, testCase.expected, legacy.events)

			inPlace := &partitionInPlaceVisitor{names: testCase.names}
			require.True(t, testCase.definition.walkInPlace(inPlace))
			require.Equal(t, testCase.expected, inPlace.events)
		})
	}
}

func TestPartitionVisitorHelpersPreserveSkipAndStop(t *testing.T) {
	t.Run("skip_expression_children", func(t *testing.T) {
		nested := &ColumnName{}
		expr := &ColumnNameExpr{Name: nested}
		column := &ColumnName{}
		method := &PartitionMethod{Expr: expr, ColumnNames: []*ColumnName{column}}
		names := map[Node]string{expr: "expr", nested: "nested", column: "column"}
		expected := []string{"enter expr", "leave expr", "enter column", "leave column"}

		legacy := &partitionLegacyVisitor{names: names, skip: expr}
		require.True(t, method.acceptWithVisitor(legacy))
		require.Equal(t, expected, legacy.events)

		inPlace := &partitionInPlaceVisitor{names: names, skip: expr}
		require.True(t, method.walkInPlace(inPlace))
		require.Equal(t, expected, inPlace.events)
	})

	t.Run("method_stop_propagates", func(t *testing.T) {
		expr := &DefaultExpr{}
		column := &ColumnName{}
		method := &PartitionMethod{Expr: expr, ColumnNames: []*ColumnName{column}}
		names := map[Node]string{expr: "expr", column: "column"}
		expected := []string{"enter expr", "leave expr"}

		legacy := &partitionLegacyVisitor{names: names, stop: expr}
		require.False(t, method.acceptWithVisitor(legacy))
		require.Equal(t, expected, legacy.events)

		inPlace := &partitionInPlaceVisitor{names: names, stop: expr}
		require.False(t, method.walkInPlace(inPlace))
		require.Equal(t, expected, inPlace.events)
	})

	t.Run("clause_stop_propagates", func(t *testing.T) {
		first := &DefaultExpr{}
		second := &DefaultExpr{}
		third := &DefaultExpr{}
		definition := &PartitionDefinition{Clause: &PartitionDefinitionClauseIn{
			Values: [][]ExprNode{{first, second}, {third}},
		}}
		names := map[Node]string{first: "first", second: "second", third: "third"}
		expected := []string{
			"enter first", "leave first",
			"enter second", "leave second",
		}

		legacy := &partitionLegacyVisitor{names: names, stop: second}
		require.False(t, definition.acceptWithVisitor(legacy))
		require.Equal(t, expected, legacy.events)

		inPlace := &partitionInPlaceVisitor{names: names, stop: second}
		require.False(t, definition.walkInPlace(inPlace))
		require.Equal(t, expected, inPlace.events)
	})
}

func TestPartitionWalkInPlaceMutatesWithoutFrameworkWriteback(t *testing.T) {
	t.Run("direct_mutation", func(t *testing.T) {
		methodColumn := &ColumnName{Name: NewCIStr("method")}
		clauseColumn := &ColumnName{Name: NewCIStr("clause")}
		method := &PartitionMethod{ColumnNames: []*ColumnName{methodColumn}}
		definition := &PartitionDefinition{Clause: &PartitionDefinitionClauseLessThan{
			Exprs: []ExprNode{&ColumnNameExpr{Name: clauseColumn}},
		}}
		visitor := &partitionInPlaceVisitor{
			names: map[Node]string{},
			mutate: func(n Node) {
				if column, ok := n.(*ColumnName); ok {
					column.Name = NewCIStr("changed")
				}
			},
		}

		require.True(t, method.walkInPlace(visitor))
		require.True(t, definition.walkInPlace(visitor))
		require.Equal(t, NewCIStr("changed"), methodColumn.Name)
		require.Equal(t, NewCIStr("changed"), clauseColumn.Name)
	})

	t.Run("no_framework_child_writeback", func(t *testing.T) {
		methodReplacement := &partitionReturningExpr{}
		methodOriginal := &partitionReturningExpr{returned: methodReplacement}
		clauseReplacement := &partitionReturningExpr{}
		clauseOriginal := &partitionReturningExpr{returned: clauseReplacement}
		method := &PartitionMethod{Expr: methodOriginal}
		clause := &PartitionDefinitionClauseLessThan{Exprs: []ExprNode{clauseOriginal}}
		visitor := &partitionInPlaceVisitor{names: map[Node]string{
			methodOriginal: "method original",
			clauseOriginal: "clause original",
		}}

		require.True(t, method.walkInPlace(visitor))
		require.True(t, clause.walkInPlace(visitor))
		require.Same(t, methodOriginal, method.Expr)
		require.Same(t, clauseOriginal, clause.Exprs[0])
	})

	t.Run("legacy_replacement_is_preserved", func(t *testing.T) {
		methodOriginal := &DefaultExpr{}
		methodReplacement := &DefaultExpr{}
		clauseOriginal := &DefaultExpr{}
		clauseReplacement := &DefaultExpr{}
		method := &PartitionMethod{Expr: methodOriginal}
		clause := &PartitionDefinitionClauseLessThan{Exprs: []ExprNode{clauseOriginal}}
		visitor := &partitionLegacyVisitor{
			names: map[Node]string{
				methodOriginal: "method original",
				clauseOriginal: "clause original",
			},
			replacements: map[Node]Node{
				methodOriginal: methodReplacement,
				clauseOriginal: clauseReplacement,
			},
		}

		require.True(t, method.acceptWithVisitor(visitor))
		require.True(t, clause.acceptWithVisitor(visitor))
		require.Same(t, methodReplacement, method.Expr)
		require.Same(t, clauseReplacement, clause.Exprs[0])
	})
}
