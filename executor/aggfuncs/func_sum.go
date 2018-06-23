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
	"unsafe"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

// All the AggFunc implementations for "SUM" are listed here.
var (
	_ AggFunc = (*sum4Decimal)(nil)
)

type sum4Decimal struct {
	baseAggFunc
	deDuper map[types.MyDecimal]bool
}

type partialResult4SumDecimal struct {
	sum types.MyDecimal
}

func (e *sum4Decimal) toPartialResult(partialBytes []byte) *partialResult4SumDecimal {
	return (*partialResult4SumDecimal)(unsafe.Pointer(&partialBytes[0]))
}

func (e *sum4Decimal) AllocPartialResult() []byte {
	partialBytes := make([]byte, unsafe.Sizeof(partialResult4SumDecimal{}))
	e.ResetPartialResult(partialBytes)
	return partialBytes
}

func (e *sum4Decimal) ResetPartialResult(partialBytes []byte) {
	*e.toPartialResult(partialBytes) = partialResult4SumDecimal{}
}

func (e *sum4Decimal) AppendPartialResult2Chunk(sctx sessionctx.Context, partialBytes []byte, chk *chunk.Chunk) error {
	partialResult := e.toPartialResult(partialBytes)
	chk.AppendMyDecimal(e.output[0], &partialResult.sum)
	return nil
}

func (e *sum4Decimal) AppendFinalResult2Chunk(sctx sessionctx.Context, partialBytes []byte, chk *chunk.Chunk) error {
	partialResult := e.toPartialResult(partialBytes)
	chk.AppendMyDecimal(e.output[0], &partialResult.sum)
	return nil
}

func (e *sum4Decimal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, partialBytes []byte) error {
	partialResult := e.toPartialResult(partialBytes)
	newSum := new(types.MyDecimal)
	for _, row := range rowsInGroup {
		input, isNull, err := e.input[0].EvalDecimal(sctx, row)
		if err != nil {
			return errors.Trace(err)
		} else if isNull || (e.deDuper != nil && e.deDuper[*input]) {
			continue
		}
		err = types.DecimalAdd(&partialResult.sum, input, newSum)
		if err != nil {
			return errors.Trace(err)
		}
		partialResult.sum = *newSum
		if e.deDuper != nil {
			e.deDuper[*input] = true
		}
	}
	return nil
}
