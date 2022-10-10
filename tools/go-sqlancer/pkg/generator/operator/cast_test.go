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
	"testing"

	parser_driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/stretchr/testify/assert"
)

func TestCastSigned(t *testing.T) {
	type kase struct {
		values []interface{}
		expect interface{}
	}

	cases := []kase{
		{[]interface{}{1.0}, 1},
		{[]interface{}{-1}, int64(-1)},
		{[]interface{}{"-1"}, int64(-1)},
		{[]interface{}{"-1.1"}, int64(-1)},
		{[]interface{}{"0.652053"}, int64(1)},
		{[]interface{}{"0.7"}, int64(1)},
		{[]interface{}{nil}, nil},
		{[]interface{}{"str"}, int64(0)},
	}

	for i, kase := range cases {
		var values []parser_driver.ValueExpr
		for _, value := range kase.values {
			v := parser_driver.ValueExpr{}
			v.SetValue(value)
			values = append(values, v)
		}
		actual, err := castSigned.Eval(values...)
		expect := parser_driver.ValueExpr{}
		expect.SetValue(kase.expect)
		assert.NoError(t, err, "should not return error")
		assert.Equal(t, expect, actual, fmt.Sprintf("case %d failed", i))
	}
}
