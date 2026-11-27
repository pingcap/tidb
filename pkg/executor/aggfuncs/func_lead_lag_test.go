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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aggfuncs_test

import (
	"math"
	"testing"

	"github.com/pingcap/tidb/pkg/executor/aggfuncs"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
)

func TestLeadLag(t *testing.T) {
	zero := expression.NewZero()
	one := expression.NewOne()
	two := &expression.Constant{
		Value:   types.NewDatum(2),
		RetType: types.NewFieldType(mysql.TypeTiny),
	}
	three := &expression.Constant{
		Value:   types.NewDatum(3),
		RetType: types.NewFieldType(mysql.TypeTiny),
	}
	million := &expression.Constant{
		Value:   types.NewDatum(1000000),
		RetType: types.NewFieldType(mysql.TypeLong),
	}
	defaultArg := &expression.Column{RetType: types.NewFieldType(mysql.TypeLonglong), Index: 0}

	numRows := 3
	tests := []windowTest{
		// lag(field0, N)
		buildWindowTesterWithArgs(ast.WindowFuncLag, mysql.TypeLonglong,
			[]expression.Expression{zero}, 0, numRows, 0, 1, 2),
		buildWindowTesterWithArgs(ast.WindowFuncLag, mysql.TypeLonglong,
			[]expression.Expression{one}, 0, numRows, nil, 0, 1),
		buildWindowTesterWithArgs(ast.WindowFuncLag, mysql.TypeLonglong,
			[]expression.Expression{two}, 0, numRows, nil, nil, 0),
		buildWindowTesterWithArgs(ast.WindowFuncLag, mysql.TypeLonglong,
			[]expression.Expression{three}, 0, numRows, nil, nil, nil),
		buildWindowTesterWithArgs(ast.WindowFuncLag, mysql.TypeLonglong,
			[]expression.Expression{million}, 0, numRows, nil, nil, nil),
		// lag(field0, N, 1000000)
		buildWindowTesterWithArgs(ast.WindowFuncLag, mysql.TypeLonglong,
			[]expression.Expression{zero, million}, 0, numRows, 0, 1, 2),
		buildWindowTesterWithArgs(ast.WindowFuncLag, mysql.TypeLonglong,
			[]expression.Expression{one, million}, 0, numRows, 1000000, 0, 1),
		buildWindowTesterWithArgs(ast.WindowFuncLag, mysql.TypeLonglong,
			[]expression.Expression{two, million}, 0, numRows, 1000000, 1000000, 0),
		buildWindowTesterWithArgs(ast.WindowFuncLag, mysql.TypeLonglong,
			[]expression.Expression{three, million}, 0, numRows, 1000000, 1000000, 1000000),
		buildWindowTesterWithArgs(ast.WindowFuncLag, mysql.TypeLonglong,
			[]expression.Expression{million, million}, 0, numRows, 1000000, 1000000, 1000000),
		// lag(field0, N, field0)
		buildWindowTesterWithArgs(ast.WindowFuncLag, mysql.TypeLonglong,
			[]expression.Expression{zero, defaultArg}, 0, numRows, 0, 1, 2),
		buildWindowTesterWithArgs(ast.WindowFuncLag, mysql.TypeLonglong,
			[]expression.Expression{one, defaultArg}, 0, numRows, 0, 0, 1),
		buildWindowTesterWithArgs(ast.WindowFuncLag, mysql.TypeLonglong,
			[]expression.Expression{two, defaultArg}, 0, numRows, 0, 1, 0),
		buildWindowTesterWithArgs(ast.WindowFuncLag, mysql.TypeLonglong,
			[]expression.Expression{three, defaultArg}, 0, numRows, 0, 1, 2),
		buildWindowTesterWithArgs(ast.WindowFuncLag, mysql.TypeLonglong,
			[]expression.Expression{million, defaultArg}, 0, numRows, 0, 1, 2),

		// lead(field0, N)
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{zero}, 0, numRows, 0, 1, 2),
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{one}, 0, numRows, 1, 2, nil),
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{two}, 0, numRows, 2, nil, nil),
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{three}, 0, numRows, nil, nil, nil),
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{million}, 0, numRows, nil, nil, nil),
		// lead(field0, N, 1000000)
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{zero, million}, 0, numRows, 0, 1, 2),
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{one, million}, 0, numRows, 1, 2, 1000000),
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{two, million}, 0, numRows, 2, 1000000, 1000000),
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{three, million}, 0, numRows, 1000000, 1000000, 1000000),
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{million, million}, 0, numRows, 1000000, 1000000, 1000000),
		// lead(field0, N, field0)
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{zero, defaultArg}, 0, numRows, 0, 1, 2),
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{one, defaultArg}, 0, numRows, 1, 2, 2),
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{two, defaultArg}, 0, numRows, 2, 1, 2),
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{three, defaultArg}, 0, numRows, 0, 1, 2),
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{million, defaultArg}, 0, numRows, 0, 1, 2),
	}
	for _, test := range tests {
		testWindowFunc(t, test)
	}
}

func TestLeadLagLargeOffset(t *testing.T) {
	newUint64Const := func(v uint64) *expression.Constant {
		ft := types.NewFieldType(mysql.TypeLonglong)
		ft.AddFlag(mysql.UnsignedFlag)
		return &expression.Constant{
			Value:   types.NewUintDatum(v),
			RetType: ft,
		}
	}

	two := newUint64Const(2)
	four := newUint64Const(4)
	maxInt64 := newUint64Const(math.MaxInt64)
	maxUint64 := newUint64Const(math.MaxUint64)
	maxUint64Minus1 := newUint64Const(math.MaxUint64 - 1)
	maxUint64Minus2 := newUint64Const(math.MaxUint64 - 2)
	defaultConst := &expression.Constant{
		Value:   types.NewDatum(1000000),
		RetType: types.NewFieldType(mysql.TypeLong),
	}
	defaultArg := &expression.Column{RetType: types.NewFieldType(mysql.TypeLonglong), Index: 0}

	numRows := 5
	tests := []windowTest{
		// Normal offset and boundary offset == len(rows)-1 for LEAD.
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{two}, 0, numRows, 2, 3, 4, nil, nil),
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{four}, 0, numRows, 4, nil, nil, nil, nil),
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{four, defaultConst}, 0, numRows, 4, 1000000, 1000000, 1000000, 1000000),

		// Large offsets near 2^64 must not overflow uint64 addition in LEAD.
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{maxInt64}, 0, numRows, nil, nil, nil, nil, nil),
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{maxUint64}, 0, numRows, nil, nil, nil, nil, nil),
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{maxUint64Minus1}, 0, numRows, nil, nil, nil, nil, nil),
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{maxUint64Minus2}, 0, numRows, nil, nil, nil, nil, nil),
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{maxUint64, defaultConst}, 0, numRows, 1000000, 1000000, 1000000, 1000000, 1000000),
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{maxUint64Minus1, defaultConst}, 0, numRows, 1000000, 1000000, 1000000, 1000000, 1000000),
		buildWindowTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{maxUint64Minus2, defaultArg}, 0, numRows, 0, 1, 2, 3, 4),

		// Corresponding large offsets for LAG.
		buildWindowTesterWithArgs(ast.WindowFuncLag, mysql.TypeLonglong,
			[]expression.Expression{maxUint64}, 0, numRows, nil, nil, nil, nil, nil),
		buildWindowTesterWithArgs(ast.WindowFuncLag, mysql.TypeLonglong,
			[]expression.Expression{maxUint64Minus1}, 0, numRows, nil, nil, nil, nil, nil),
		buildWindowTesterWithArgs(ast.WindowFuncLag, mysql.TypeLonglong,
			[]expression.Expression{maxUint64Minus2, defaultConst}, 0, numRows, 1000000, 1000000, 1000000, 1000000, 1000000),
	}
	for _, test := range tests {
		testWindowFunc(t, test)
	}
}

func TestMemLeadLag(t *testing.T) {
	zero := expression.NewZero()
	one := expression.NewOne()
	two := &expression.Constant{
		Value:   types.NewDatum(2),
		RetType: types.NewFieldType(mysql.TypeTiny),
	}
	three := &expression.Constant{
		Value:   types.NewDatum(3),
		RetType: types.NewFieldType(mysql.TypeTiny),
	}
	million := &expression.Constant{
		Value:   types.NewDatum(1000000),
		RetType: types.NewFieldType(mysql.TypeLong),
	}

	numRows := 3
	tests := []windowMemTest{
		// lag(field0, N)
		buildWindowMemTesterWithArgs(ast.WindowFuncLag, mysql.TypeLonglong,
			[]expression.Expression{zero}, 0, numRows, aggfuncs.DefPartialResult4LeadLagSize, rowMemDeltaGens),
		buildWindowMemTesterWithArgs(ast.WindowFuncLag, mysql.TypeLonglong,
			[]expression.Expression{one}, 0, numRows, aggfuncs.DefPartialResult4LeadLagSize, rowMemDeltaGens),
		buildWindowMemTesterWithArgs(ast.WindowFuncLag, mysql.TypeLonglong,
			[]expression.Expression{two}, 0, numRows, aggfuncs.DefPartialResult4LeadLagSize, rowMemDeltaGens),
		buildWindowMemTesterWithArgs(ast.WindowFuncLag, mysql.TypeLonglong,
			[]expression.Expression{three}, 0, numRows, aggfuncs.DefPartialResult4LeadLagSize, rowMemDeltaGens),
		buildWindowMemTesterWithArgs(ast.WindowFuncLag, mysql.TypeLonglong,
			[]expression.Expression{million}, 0, numRows, aggfuncs.DefPartialResult4LeadLagSize, rowMemDeltaGens),

		// lead(field0, N)
		buildWindowMemTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{zero}, 0, numRows, aggfuncs.DefPartialResult4LeadLagSize, rowMemDeltaGens),
		buildWindowMemTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{one}, 0, numRows, aggfuncs.DefPartialResult4LeadLagSize, rowMemDeltaGens),
		buildWindowMemTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{two}, 0, numRows, aggfuncs.DefPartialResult4LeadLagSize, rowMemDeltaGens),
		buildWindowMemTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{three}, 0, numRows, aggfuncs.DefPartialResult4LeadLagSize, rowMemDeltaGens),
		buildWindowMemTesterWithArgs(ast.WindowFuncLead, mysql.TypeLonglong,
			[]expression.Expression{million}, 0, numRows, aggfuncs.DefPartialResult4LeadLagSize, rowMemDeltaGens),
	}

	for _, test := range tests {
		testWindowAggMemFunc(t, test)
	}
}
