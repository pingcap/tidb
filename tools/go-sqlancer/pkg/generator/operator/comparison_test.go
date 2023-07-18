// Copyright 2022 PingCAP, Inc.
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

package operator

import (
	"fmt"
	"math"
	"testing"

	parser_driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/stretchr/testify/assert"
)

func TestNullEq(t *testing.T) {
	cases := []interface{}{math.MaxFloat32, math.MaxFloat64,
		math.MinInt32, math.MinInt64,
		math.SmallestNonzeroFloat32,
		math.SmallestNonzeroFloat64,
		"str", "˜ƒf∫∫ß", "", nil,
		[]byte{'h', 'i'},
	}
	for i, kase := range cases {
		a := parser_driver.ValueExpr{}
		a.SetValue(kase)
		b := parser_driver.ValueExpr{}
		b.SetValue(kase)
		expected := parser_driver.ValueExpr{}
		expected.SetValue(1)
		actual, err := nullEq.Eval(a, b)
		assert.NoError(t, err, "should not return error")
		assert.Equal(t, expected, actual)

		// with not same kase
		b.SetValue(cases[(i+1)%len(cases)])
		expected.SetValue(0)
		actual, err = nullEq.Eval(a, b)
		assert.NoError(t, err, "should not return error")
		assert.Equal(t, expected, actual)
	}
}

func TestIsNull(t *testing.T) {
	cases := []interface{}{math.MaxFloat32, math.MaxFloat64,
		math.MinInt32, math.MinInt64,
		math.SmallestNonzeroFloat32,
		math.SmallestNonzeroFloat64,
		"str", "˜ƒf∫∫ß", "", nil,
		[]byte{'h', 'i'},
	}
	for _, kase := range cases {
		a := parser_driver.ValueExpr{}
		a.SetValue(kase)
		expected := parser_driver.ValueExpr{}
		expected.SetValue(0)
		if kase == nil {
			expected.SetValue(1)
		}
		actual, err := isNull.Eval(a)
		assert.NoError(t, err, "should not return error")
		assert.Equal(t, expected, actual)
	}
}

func TestIn(t *testing.T) {
	type kase struct {
		expr   interface{}
		values []interface{}
		expect parser_driver.ValueExpr
	}
	truly := parser_driver.ValueExpr{}
	truly.SetValue(1)
	falsely := parser_driver.ValueExpr{}
	falsely.SetValue(0)
	null := parser_driver.ValueExpr{}
	null.SetNull()

	cases := []kase{
		{math.MaxFloat32, []interface{}{math.MaxFloat32, -1}, truly},
		{math.MaxFloat32, []interface{}{-1}, falsely},
		{math.MinInt64, []interface{}{math.MinInt64, nil}, truly},
		{math.MinInt64, []interface{}{nil}, null},
		{"str", []interface{}{"", "˜ƒf∫∫ß"}, falsely},
	}

	for _, kase := range cases {
		value := parser_driver.ValueExpr{}
		value.SetValue(kase.expr)
		var values = []parser_driver.ValueExpr{value}
		for _, value := range kase.values {
			v := parser_driver.ValueExpr{}
			v.SetValue(value)
			values = append(values, v)
		}
		actual, err := in.Eval(values...)
		assert.NoError(t, err, "should not return error")
		assert.Equal(t, kase.expect, actual)
	}
}

func TestBetween(t *testing.T) {
	type kase struct {
		expr   interface{}
		values []interface{}
		expect parser_driver.ValueExpr
	}
	truly := parser_driver.ValueExpr{}
	truly.SetValue(1)
	falsely := parser_driver.ValueExpr{}
	falsely.SetValue(0)
	null := parser_driver.ValueExpr{}
	null.SetNull()
	cases := []kase{
		{1.0, []interface{}{math.SmallestNonzeroFloat64, 2.0}, truly},
		{-2.0, []interface{}{-1000000.0, math.MaxFloat64}, truly},
		{math.MinInt64, []interface{}{math.MinInt64, nil}, null},
		{"str", []interface{}{"st", "str2"}, truly},
		{0, []interface{}{nil, -2034522382449724946}, falsely},
		{0, []interface{}{nil, 0}, null},
	}

	for i, kase := range cases {
		value := parser_driver.ValueExpr{}
		value.SetValue(kase.expr)
		var values = []parser_driver.ValueExpr{value}
		for _, value := range kase.values {
			v := parser_driver.ValueExpr{}
			v.SetValue(value)
			values = append(values, v)
		}
		actual, err := between.Eval(values...)
		assert.NoError(t, err, "should not return error")
		assert.Equal(t, kase.expect, actual, fmt.Sprintf("case %d failed", i))
	}
}

func TestStrCmp(t *testing.T) {
	type kase struct {
		expr1  interface{}
		expr2  interface{}
		expect parser_driver.ValueExpr
	}
	gt := parser_driver.ValueExpr{}
	gt.SetValue(1)
	eq := parser_driver.ValueExpr{}
	eq.SetValue(0)
	lt := parser_driver.ValueExpr{}
	lt.SetValue(-1)
	null := parser_driver.ValueExpr{}
	null.SetNull()
	cases := []kase{
		{"text", "text2", lt},
		{"text2", "text", gt},
		{"text", "text", eq},
		{nil, "text", null},
		{"text", nil, null},
		{"J_%AEQ", 0, gt},
	}

	for i, kase := range cases {
		expr1 := parser_driver.ValueExpr{}
		expr1.SetValue(kase.expr1)
		expr2 := parser_driver.ValueExpr{}
		expr2.SetValue(kase.expr2)
		actual, err := strCmp.Eval(expr1, expr2)
		assert.NoError(t, err, "should not return error")
		assert.Equal(t, kase.expect, actual, fmt.Sprintf("case %d failed", i))
	}
}
