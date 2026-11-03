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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package driver

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

type parserDriverInPlaceVisitor struct {
	enter func(ast.Node) bool
	leave func(ast.Node) bool
}

func (v *parserDriverInPlaceVisitor) Enter(node ast.Node) bool {
	if v.enter == nil {
		return false
	}
	return v.enter(node)
}

func (v *parserDriverInPlaceVisitor) Leave(node ast.Node) bool {
	if v.leave == nil {
		return true
	}
	return v.leave(node)
}

func TestValueExprRestore(t *testing.T) {
	tests := []struct {
		datum  types.Datum
		expect string
	}{
		{types.NewDatum(nil), "NULL"},
		{types.NewIntDatum(1), "1"},
		{types.NewIntDatum(-1), "-1"},
		{types.NewUintDatum(1), "1"},
		{types.NewFloat32Datum(1.1), "1.1e+00"},
		{types.NewFloat64Datum(1.1), "1.1e+00"},
		{types.NewStringDatum("test `s't\"r."), "'test `s''t\"r.'"},
		{types.NewBytesDatum([]byte("test `s't\"r.")), "'test `s''t\"r.'"},
		{types.NewBinaryLiteralDatum([]byte("test `s't\"r.")), "b'11101000110010101110011011101000010000001100000011100110010011101110100001000100111001000101110'"},
		{types.NewDecimalDatum(types.NewDecFromInt(321)), "321"},
		{types.NewDurationDatum(types.ZeroDuration), "'00:00:00'"},
		{types.NewTimeDatum(types.ZeroDatetime), "'0000-00-00 00:00:00'"},
		{types.NewStringDatum("\\"), "'\\\\'"},
	}

	for _, test := range tests {
		t.Run(test.expect, func(t *testing.T) {
			var sb strings.Builder
			expr := &ValueExpr{Datum: test.datum}
			err := expr.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
			require.NoError(t, err)
			require.Equalf(t, test.expect, sb.String(), "datum: %#v", test.datum)
		})
	}

	t.Run("walk", func(t *testing.T) {
		testCases := []struct {
			name string
			node ast.Node
		}{
			{name: "value_expr", node: &ValueExpr{}},
			{name: "param_marker_expr", node: &ParamMarkerExpr{}},
		}

		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				var events []string
				visitor := &parserDriverInPlaceVisitor{
					enter: func(node ast.Node) bool {
						require.Same(t, testCase.node, node)
						events = append(events, "enter")
						return true
					},
					leave: func(node ast.Node) bool {
						require.Same(t, testCase.node, node)
						events = append(events, "leave")
						return true
					},
				}

				require.True(t, ast.Walk(testCase.node, visitor))
				require.Equal(t, []string{"enter", "leave"}, events)

				allocationVisitor := &parserDriverInPlaceVisitor{}
				var ok bool
				allocations := testing.AllocsPerRun(100, func() {
					ok = ast.Walk(testCase.node, allocationVisitor)
				})
				require.True(t, ok)
				require.Zero(t, allocations)
			})
		}
	})
}

func TestValueExprFormat(t *testing.T) {
	tests := []struct {
		datum  types.Datum
		expect string
	}{
		{types.NewDatum(nil), "NULL"},
		{types.NewIntDatum(1), "1"},
		{types.NewIntDatum(-1), "-1"},
		{types.NewUintDatum(1), "1"},
		{types.NewFloat32Datum(1.1), "1.1e+00"},
		{types.NewFloat64Datum(1.1), "1.1e+00"},
		{types.NewStringDatum("test `s't\"r."), "'test `s''t\"r.'"},
		{types.NewBytesDatum([]byte("test `s't\"r.")), "'test `s''t\"r.'"},
		{types.NewBinaryLiteralDatum([]byte("test `s't\"r.")), "b'11101000110010101110011011101000010000001100000011100110010011101110100001000100111001000101110'"},
		{types.NewDecimalDatum(types.NewDecFromInt(321)), "321"},
		{types.NewStringDatum("\\"), "'\\\\'"},
		{types.NewStringDatum("''"), "''''''"},
		{types.NewStringDatum("\\''\t\n"), "'\\\\''''\t\n'"},
	}

	for _, test := range tests {
		t.Run(test.expect, func(t *testing.T) {
			var sb strings.Builder
			expr := &ValueExpr{Datum: test.datum}
			expr.Format(&sb)
			require.Equalf(t, test.expect, sb.String(), "datum: %#v", test.datum)
		})
	}
}
