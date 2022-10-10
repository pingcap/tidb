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

func TestCase(t *testing.T) {
	type kase struct {
		values []interface{}
		expect interface{}
	}

	cases := []kase{
		{[]interface{}{math.SmallestNonzeroFloat64, 1.0}, 1.0},
		{[]interface{}{0, 1.0, math.SmallestNonzeroFloat64, 2.0}, 2.0},
		{[]interface{}{nil, math.MaxFloat64}, nil},
		{[]interface{}{nil, 1.0, "true", 2.0}, nil},
		{[]interface{}{nil, 1.0, "1.true", 2.0}, 2.0},
	}

	for i, kase := range cases {
		var values []parser_driver.ValueExpr
		for _, value := range kase.values {
			v := parser_driver.ValueExpr{}
			v.SetValue(value)
			values = append(values, v)
		}
		actual, err := Case.Eval(values...)
		expect := parser_driver.ValueExpr{}
		expect.SetValue(kase.expect)
		assert.NoError(t, err, "should not return error")
		assert.Equal(t, expect, actual, fmt.Sprintf("case %d failed", i))
	}
}

func TestIf(t *testing.T) {
	type kase struct {
		values []interface{}
		expect interface{}
	}

	cases := []kase{
		{[]interface{}{math.SmallestNonzeroFloat64, 1.0, 2.0}, 1.0},
		{[]interface{}{0, 1.0, math.SmallestNonzeroFloat64}, math.SmallestNonzeroFloat64},
		{[]interface{}{nil, math.MaxFloat64, -1}, -1},
		{[]interface{}{"test", 1.0, 2.0}, 2.0},
		{[]interface{}{"", 1.0, 2.0}, 2.0},
		{[]interface{}{"", 1.0, nil}, nil},
	}

	for i, kase := range cases {
		var values []parser_driver.ValueExpr
		for _, value := range kase.values {
			v := parser_driver.ValueExpr{}
			v.SetValue(value)
			values = append(values, v)
		}
		actual, err := If.Eval(values...)
		expect := parser_driver.ValueExpr{}
		expect.SetValue(kase.expect)
		assert.NoError(t, err, "should not return error")
		assert.Equal(t, expect, actual, fmt.Sprintf("case %d failed", i))
	}
}

func TestNullIf(t *testing.T) {
	type kase struct {
		values []interface{}
		expect interface{}
	}

	cases := []kase{
		{[]interface{}{math.SmallestNonzeroFloat64, 1.0}, math.SmallestNonzeroFloat64},
		{[]interface{}{"1.0", 1.0}, nil},
		{[]interface{}{1.0, "1.000"}, nil},
		{[]interface{}{0, 0}, nil},
		{[]interface{}{0, 1.0}, 0},
		{[]interface{}{-1, nil}, -1},
		{[]interface{}{nil, nil}, nil},
		{[]interface{}{nil, -1}, nil},
	}

	for i, kase := range cases {
		var values []parser_driver.ValueExpr
		for _, value := range kase.values {
			v := parser_driver.ValueExpr{}
			v.SetValue(value)
			values = append(values, v)
		}
		actual, err := NullIf.Eval(values...)
		expect := parser_driver.ValueExpr{}
		expect.SetValue(kase.expect)
		assert.NoError(t, err, "should not return error")
		assert.Equal(t, expect, actual, fmt.Sprintf("case %d failed", i))
	}
}
