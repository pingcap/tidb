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

package ast_test

import (
	"fmt"
	"testing"

	parserast "github.com/pingcap/tidb/pkg/parser/ast"
)

type benchmarkVisitor struct{}

func (*benchmarkVisitor) Enter(n parserast.Node) (parserast.Node, bool) {
	return n, false
}

func (*benchmarkVisitor) Leave(n parserast.Node) (parserast.Node, bool) {
	return n, true
}

func BenchmarkVisitorTraversal(b *testing.B) {
	for _, replaceableNodes := range []int{10, 100, 500, 1000} {
		b.Run(fmt.Sprintf("%dReplaceableNodes", replaceableNodes), func(b *testing.B) {
			b.Run("Visitor", func(b *testing.B) {
				root := newBenchmarkSelect(replaceableNodes)
				visitor := &benchmarkVisitor{}
				var ok bool
				b.ReportAllocs()
				for b.Loop() {
					_, ok = root.Accept(visitor)
				}
				if !ok {
					b.Fatal("visitor traversal stopped")
				}
			})

			b.Run("InPlaceVisitor", func(b *testing.B) {
				root := newBenchmarkSelect(replaceableNodes)
				visitor := &benchmarkVisitor{}
				var ok bool
				b.ReportAllocs()
				for b.Loop() {
					_, ok = root.Accept(visitor)
				}
				if !ok {
					b.Fatal("visitor traversal stopped")
				}
			})
		})
	}
}

// newBenchmarkSelect returns a select with exactly replaceableNodes children.
// The legacy Visitor writes every returned child back into its parent, while
// InPlaceVisitor traverses the same nodes without those framework writes.
func newBenchmarkSelect(replaceableNodes int) *parserast.SelectStmt {
	fields := make([]*parserast.SelectField, 0, replaceableNodes/3+1)
	remaining := replaceableNodes - 1 // SelectStmt.Fields accounts for one child.
	for remaining > 0 {
		switch {
		case remaining == 4:
			fields = append(fields,
				&parserast.SelectField{Expr: &parserast.DefaultExpr{}},
				&parserast.SelectField{Expr: &parserast.DefaultExpr{}},
			)
			remaining = 0
		case remaining >= 3:
			fields = append(fields, &parserast.SelectField{
				Expr: &parserast.ColumnNameExpr{Name: &parserast.ColumnName{}},
			})
			remaining -= 3
		case remaining == 2:
			fields = append(fields, &parserast.SelectField{Expr: &parserast.DefaultExpr{}})
			remaining = 0
		default:
			panic("replaceableNodes must be at least 3")
		}
	}
	return &parserast.SelectStmt{Fields: &parserast.FieldList{Fields: fields}}
}
