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

type partialResult4SumInt struct {
	val int64
	// isNull is used to indicates:
	// 1. whether the partial result is the initialization value which should not be sum during evaluation;
	// 2. whether all the values of arg are all null, if so, we should return null as the default value for SUM.
	isNull bool
}

type partialResult4SumUint struct {
	val    uint64
	isNull bool
}

type partialResult4SumDecimal struct {
	val    types.MyDecimal
	isNull bool
}

type baseSumAggFunc struct {
	baseAggFunc
}

type sumAggFunc4Int struct {
	baseSumAggFunc
}

func (e *sumAggFunc4Int) AllocPartialResult() PartialResult {
	p := new(partialResult4SumInt)
	p.isNull = true
	return PartialResult(p)
}

func (e *sumAggFunc4Int) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4SumInt)(pr)
	p.val = 0
	p.isNull = true
}

func (e *sumAggFunc4Int) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4SumInt)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendInt64(e.ordinal, p.val)
	return nil
}

func (e *sumAggFunc4Int) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4SumInt)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return errors.Trace(err)
		}
		if isNull {
			continue
		}
		if p.isNull {
			p.val = input
			p.isNull = false
			continue
		}
		p.val += input
	}
	return nil
}

type sumAggFunc4Uint struct {
	baseSumAggFunc
}

func (e *sumAggFunc4Uint) AllocPartialResult() PartialResult {
	p := new(partialResult4SumUint)
	p.isNull = true
	return PartialResult(p)
}

func (e *sumAggFunc4Uint) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4SumUint)(pr)
	p.val = 0
	p.isNull = true
}

func (e *sumAggFunc4Uint) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4SumUint)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendUint64(e.ordinal, p.val)
	return nil
}

func (e *sumAggFunc4Uint) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4SumUint)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return errors.Trace(err)
		}
		if isNull {
			continue
		}
		uintVal := uint64(input)
		if p.isNull {
			p.val = uintVal
			p.isNull = false
			continue
		}
		p.val += uintVal
	}
	return nil
}

type sumAggFunc4Decimal struct {
	baseSumAggFunc
}

func (e *sumAggFunc4Decimal) AllocPartialResult() PartialResult {
	p := new(partialResult4SumDecimal)
	//p.val = *types.NewDecFromInt(0) should the line un-commented?
	p.isNull = true
	return PartialResult(p)
}

func (e *sumAggFunc4Decimal) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4SumDecimal)(pr)
	//p.val = *types.NewDecFromInt(0)   should the line un-commented?
	p.isNull = true
}

func (e *sumAggFunc4Decimal) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4SumDecimal)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendMyDecimal(e.ordinal, &p.val)
	return nil
}

func (e *sumAggFunc4Decimal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4SumDecimal)(pr)
	newSum := new(types.MyDecimal)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalDecimal(sctx, row)
		if err != nil {
			return errors.Trace(err)
		}
		if isNull {
			continue
		}
		if p.isNull {
			p.val = *input
			p.isNull = false
			continue
		}

		err = types.DecimalAdd(&p.val, input, newSum)
		if err != nil {
			return errors.Trace(err)
		}
		p.val = *newSum
	}
	return nil
}
