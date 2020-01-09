// Copyright 2020 PingCAP, Inc.
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

package expression

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
)

type nullableCheckCase struct {
	childrenNullable []bool
	selfNullable     bool
}

type exprNullableTestCase struct {
	// retEvalType is the EvalType of the expression result.
	// This field is required.
	retEvalType types.EvalType
	// childrenTypes is the EvalTypes of the expression children(arguments).
	// Note that the length of this slice should be less than 32.
	// This field is required.
	childrenTypes []types.EvalType
	// alwaysNullable means this expression can generate null whether its children
	// are nullable or not.
	// This field is optional.
	alwaysNullable bool
	// alwaysNotNull means this expression will never generate null value.
	// Note: `alwaysNullable` and `alwaysNotNull` cannot be set at the same time.
	// This field is optional.
	alwaysNotNull bool
	// specialCases is the special test cases for nullable test.
	// This field is optional.
	specialCases []nullableCheckCase
}

func testExpressionNullable(c *C, testCases map[string][]exprNullableTestCase) {
	ctx := mock.NewContext()
	for funcName, cases := range testCases {
		for _, testCase := range cases {
			c.Assert(testCase.alwaysNullable && testCase.alwaysNotNull, IsFalse)

			specialCases := make(map[int]bool)
			for _, specialCase := range testCase.specialCases {
				nullableFlags := 0
				for j, childNullable := range specialCase.childrenNullable {
					if childNullable {
						nullableFlags += 1 << j
					}
				}
				_, ok := specialCases[nullableFlags]
				c.Assert(ok, IsFalse)
				specialCases[nullableFlags] = specialCase.selfNullable
			}

			cols := make([]*Column, len(testCase.childrenTypes))
			for i, childType := range testCase.childrenTypes {
				cols[i] = &Column{UniqueID: int64(i), Index: i, RetType: eType2FieldType(childType)}
			}
			expr, err := NewFunction(ctx, funcName, eType2FieldType(testCase.retEvalType), Column2Exprs(cols)...)
			c.Assert(err, IsNil)

			// Totally 2^(len(children)) check cases.
			n := 1 << len(testCase.childrenTypes)
			for i := 0; i < n; i++ {
				notNullCols := make([]*Column, 0, len(testCase.childrenTypes))
				for j := range testCase.childrenTypes {
					if (1<<j)&i == 0 {
						notNullCols = append(notNullCols, cols[j])
					}
				}
				result := expr.Nullable(notNullCols)
				if testCase.alwaysNullable {
					c.Assert(result, IsTrue)
				} else {
					// check special cases.
					if answer, ok := specialCases[i]; ok {
						c.Assert(result, Equals, answer)
					} else {
						// If i == 0, all of the children are `NotNull`, so `Nullable` usually returns `false`.
						// If i != 0, at least one child is `Nullable`, so `Nullable` usually returns `true`.
						c.Assert(result, Equals, i != 0)
					}
				}
			}
		}
	}
}
