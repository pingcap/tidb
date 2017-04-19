// Copyright 2016 PingCAP, Inc.
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

package xeval

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

func (s *testEvalSuite) TestEvalCoalesce(c *C) {
	colID := int64(1)
	row := make(map[int64]types.Datum)
	row[colID] = types.NewIntDatum(100)
	xevaluator := &Evaluator{Row: row}
	nullDatum := types.Datum{}
	nullDatum.SetNull()
	notNullDatum := types.NewStringDatum("not-null")
	tests := []struct {
		expr   *tipb.Expr
		result types.Datum
	}{
		{
			expr:   buildExpr(tipb.ExprType_Coalesce, nullDatum, nullDatum, nullDatum),
			result: nullDatum,
		},
		{
			expr:   buildExpr(tipb.ExprType_Coalesce, nullDatum, notNullDatum, nullDatum),
			result: notNullDatum,
		},
		{
			expr:   buildExpr(tipb.ExprType_Coalesce, nullDatum, notNullDatum, types.NewStringDatum("not-null-2"), nullDatum),
			result: notNullDatum,
		},
	}
	for _, tt := range tests {
		result, err := xevaluator.Eval(tt.expr)
		c.Assert(err, IsNil)
		c.Assert(result.Kind(), Equals, tt.result.Kind())
		cmp, err := result.CompareDatum(xevaluator.StatementCtx, tt.result)
		c.Assert(err, IsNil)
		c.Assert(cmp, Equals, 0)
	}
}
