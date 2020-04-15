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

package core

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/v4/expression"
	"github.com/pingcap/tidb/v4/expression/aggregation"
	"github.com/pingcap/tidb/v4/types"
	"github.com/pingcap/tidb/v4/util/mock"
)

var _ = Suite(&testInjectProjSuite{})

type testInjectProjSuite struct {
}

func (s *testInjectProjSuite) TestWrapCastForAggFuncs(c *C) {
	aggNames := []string{ast.AggFuncSum}
	modes := []aggregation.AggFunctionMode{aggregation.CompleteMode,
		aggregation.FinalMode, aggregation.Partial1Mode, aggregation.Partial1Mode}
	retTypes := []byte{mysql.TypeLong, mysql.TypeNewDecimal, mysql.TypeDouble, mysql.TypeLonglong, mysql.TypeInt24}
	hasDistincts := []bool{true, false}

	aggFuncs := make([]*aggregation.AggFuncDesc, 0, 32)
	for _, hasDistinct := range hasDistincts {
		for _, name := range aggNames {
			for _, mode := range modes {
				for _, retType := range retTypes {
					sctx := mock.NewContext()
					aggFunc, err := aggregation.NewAggFuncDesc(sctx, name,
						[]expression.Expression{&expression.Constant{Value: types.Datum{}, RetType: types.NewFieldType(retType)}},
						hasDistinct)
					c.Assert(err, IsNil)
					aggFunc.Mode = mode
					aggFuncs = append(aggFuncs, aggFunc)
				}
			}
		}
	}

	orgAggFuncs := make([]*aggregation.AggFuncDesc, 0, len(aggFuncs))
	for _, agg := range aggFuncs {
		orgAggFuncs = append(orgAggFuncs, agg.Clone())
	}

	wrapCastForAggFuncs(mock.NewContext(), aggFuncs)
	for i := range aggFuncs {
		if aggFuncs[i].Mode != aggregation.FinalMode && aggFuncs[i].Mode != aggregation.Partial2Mode {
			c.Assert(aggFuncs[i].RetTp.Tp, Equals, aggFuncs[i].Args[0].GetType().Tp)
		} else {
			c.Assert(aggFuncs[i].Args[0].GetType().Tp, Equals, orgAggFuncs[i].Args[0].GetType().Tp)
		}
	}
}
