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
// See the License for the specific language governing permissions and
// limitations under the License.

package aggfuncs

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

// All the following sum function implementations return the decimal result,
// which store the partial results in "partialResult4SumDecimal".
//
// "baseSumDecimal" is wrapped by:
// - "sumOriginal4Decimal"
// - "sumPartial4Decimal"
type baseSumDecimal struct {
	baseAggFunc
}

type partialResult4SumDecimal struct {
	sum   types.MyDecimal
	count int64
}

func (e *baseSumDecimal) AllocPartialResult() PartialResult {
	return PartialResult(&partialResult4SumDecimal{})

}

func (e *baseSumDecimal) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4SumDecimal)(pr)
	p.sum = *types.NewDecFromInt(0)
	p.count = int64(0)
}

func (e *baseSumDecimal) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4SumDecimal)(pr)
	if p.count == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	finalResult := &p.sum

	chk.AppendMyDecimal(e.ordinal, finalResult)
	return nil
}

type sumOriginal4Decimal struct {
	baseSumDecimal
}

func (e *sumOriginal4Decimal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4SumDecimal)(pr)
	newSum := new(types.MyDecimal)
	for _, row := range rowsInGroup {
		inputSum, isNull, err := e.args[0].EvalDecimal(sctx, row)
		if err != nil {
			return errors.Trace(err)
		}
		if isNull {
			continue
		}

		err = types.DecimalAdd(&p.sum, inputSum, newSum)
		if err != nil {
			return errors.Trace(err)
		}
		p.sum = *newSum
		p.count++
	}
	return nil
}

type sumPartial4Decimal struct {
	baseSumDecimal
}

func (e *sumPartial4Decimal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4SumDecimal)(pr)
	newSum := new(types.MyDecimal)
	for _, row := range rowsInGroup {
		inputSum, isNull, err := e.args[1].EvalDecimal(sctx, row)
		if err != nil {
			return errors.Trace(err)
		}
		if isNull {
			continue
		}

		inputCount, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return errors.Trace(err)
		}
		if isNull {
			continue
		}

		err = types.DecimalAdd(&p.sum, inputSum, newSum)
		if err != nil {
			return errors.Trace(err)
		}
		p.sum = *newSum
		p.count += inputCount
	}
	return nil
}
