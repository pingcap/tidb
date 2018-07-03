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

type partialResult4MaxMinInt int64
type partialResult4MaxMinDecimal *types.MyDecimal
type partialResult4MaxMinFloat32 float32
type partialResult4MaxMinFloat64 float64
// Todo
type partialResult4MaxMinString string
type partialResult4MaxMinDatetime types.Time
type partialResult4MaxMinTimestamp types.Time
type partialResult4MaxMinDuration types.Duration

type maxMin4Int struct {
	baseAggFunc

	isMax      bool
	isUnsigned bool
}

func (e *maxMin4Int) AllocPartialResult() PartialResult {
	return PartialResult(new(partialResult4MaxMinInt))
}

func (e *maxMin4Int) ResetPartialResult(pr PartialResult) {
	*(*partialResult4MaxMinInt)(pr) = 0
}

func (e *maxMin4Int) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	if !e.isUnsigned {
		chk.AppendInt64(e.ordinal, int64(*(*partialResult4MaxMinInt)(pr)))
	} else {
		chk.AppendUint64(e.ordinal, uint64(*(*partialResult4MaxMinInt)(pr)))
	}

	return nil
}

func (e *maxMin4Int) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	tmp := (*partialResult4MaxMinInt)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return errors.Trace(err)
		}
		if isNull {
			continue
		}
		if !e.isUnsigned {
			if f0, f1 := int64(*tmp), int64(input); e.isMax && f1 > f0 || !e.isMax && f1 < f0 {
				*tmp = partialResult4MaxMinInt(f1)
			}
		} else {
			if f0, f1 := uint64(*tmp), uint64(input); e.isMax && f1 > f0 || !e.isMax && f1 < f0 {
				*tmp = partialResult4MaxMinInt(f1)
			}
		}
	}
	return nil
}

// maxMin4Float32 gets a float32 input and returns a float32 result.
type maxMin4Float32 struct {
	baseAggFunc

	isMax bool
}

func (e *maxMin4Float32) AllocPartialResult() PartialResult {
	return PartialResult(new(partialResult4MaxMinFloat32))
}

func (e *maxMin4Float32) ResetPartialResult(pr PartialResult) {
	*(*partialResult4MaxMinFloat32)(pr) = 0
}

func (e *maxMin4Float32) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	chk.AppendFloat32(e.ordinal, float32(*(*partialResult4MaxMinFloat32)(pr)))
	return nil
}

func (e *maxMin4Float32) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	tmp := (*partialResult4MaxMinFloat32)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return errors.Trace(err)
		}
		if isNull {
			continue
		}
		if f0, f1 := float32(*tmp), float32(input); e.isMax && f1 > f0 || !e.isMax && f1 < f0 {
			*tmp = partialResult4MaxMinFloat32(f1)
		}
	}
	return nil
}

type maxMin4Float64 struct {
	baseAggFunc

	isMax bool
}

func (e *maxMin4Float64) AllocPartialResult() PartialResult {
	return PartialResult(new(partialResult4MaxMinFloat64))
}

func (e *maxMin4Float64) ResetPartialResult(pr PartialResult) {
	*(*partialResult4MaxMinFloat64)(pr) = 0
}

func (e *maxMin4Float64) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	chk.AppendFloat64(e.ordinal, float64(*(*partialResult4MaxMinFloat64)(pr)))
	return nil
}

func (e *maxMin4Float64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	tmp := (*partialResult4MaxMinFloat64)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return errors.Trace(err)
		}
		if isNull {
			continue
		}
		if f0, f1 := float64(*tmp), float64(input); e.isMax && f1 > f0 || !e.isMax && f1 < f0 {
			*tmp = partialResult4MaxMinFloat64(f1)
		}
	}
	return nil
}

type maxMin4Decimal struct {
	baseAggFunc

	isMax bool
}

func (e *maxMin4Decimal) AllocPartialResult() PartialResult {
	return PartialResult(new(partialResult4MaxMinDecimal))
}

func (e *maxMin4Decimal) ResetPartialResult(pr PartialResult) {
	*(*partialResult4MaxMinDecimal)(pr) = partialResult4MaxMinDecimal(*new(partialResult4MaxMinDecimal))
}

func (e *maxMin4Decimal) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	chk.AppendMyDecimal(e.ordinal, (*types.MyDecimal)(*(*partialResult4MaxMinDecimal)(pr)))
	return nil
}

func (e *maxMin4Decimal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	tmp := (*partialResult4MaxMinDecimal)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalDecimal(sctx, row)
		if err != nil {
			return errors.Trace(err)
		}
		if isNull {
			continue
		}
		f0, f1 := (*types.MyDecimal)(*tmp), (*types.MyDecimal)(input)
		cmp := f1.Compare(f0)
		if e.isMax && cmp == 1 || !e.isMax && cmp == -1 {
			*tmp = partialResult4MaxMinDecimal(f1)
		}
	}
	return nil
}
