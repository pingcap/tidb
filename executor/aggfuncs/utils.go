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

package aggfuncs

import (
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/hack"
)

func evalDecimalWithFrac(sctx sessionctx.Context, arg expression.Expression, row chunk.Row) (res *types.MyDecimal, isNull bool, err error) {
	res, isNull, err = arg.EvalDecimal(sctx, row)
	if err != nil {
		return
	}
	if isNull {
		return
	}
	if frac := arg.GetType().Decimal; frac >= 0 {
		err = res.Round(res, frac, types.ModeHalfEven)
		if err != nil {
			return
		}
	}
	return
}

func evalStringWithFrac(sctx sessionctx.Context, arg expression.Expression, row chunk.Row) (res string, isNull bool, err error) {
	if arg.GetType().EvalType() == types.ETDecimal {
		var dec *types.MyDecimal
		dec, isNull, err = arg.EvalDecimal(sctx, row)
		if err != nil {
			return
		}
		if isNull {
			return
		}
		if frac := arg.GetType().Decimal; frac >= 0 {
			err = dec.Round(dec, frac, types.ModeHalfEven)
			if err != nil {
				return
			}
		}
		res = string(hack.String(dec.ToString()))
		return
	}
	res, isNull, err = arg.EvalString(sctx, row)
	return
}
