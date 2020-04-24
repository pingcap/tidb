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
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
)

type baseCount struct {
	baseAggFunc
}

type partialResult4Count = int64

func (e *baseCount) AllocPartialResult() PartialResult {
	return PartialResult(new(partialResult4Count))
}

func (e *baseCount) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4Count)(pr)
	*p = 0
}

func (e *baseCount) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4Count)(pr)
	chk.AppendInt64(e.ordinal, *p)
	return nil
}

type countOriginal4Int struct {
	baseCount
}

func (e *countOriginal4Int) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4Count)(pr)

	for _, row := range rowsInGroup {
		_, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}

		*p++
	}

	return nil
}

type countOriginal4Real struct {
	baseCount
}

func (e *countOriginal4Real) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4Count)(pr)

	for _, row := range rowsInGroup {
		_, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}

		*p++
	}

	return nil
}

type countOriginal4Decimal struct {
	baseCount
}

func (e *countOriginal4Decimal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4Count)(pr)

	for _, row := range rowsInGroup {
		_, isNull, err := e.args[0].EvalDecimal(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}

		*p++
	}

	return nil
}

type countOriginal4Time struct {
	baseCount
}

func (e *countOriginal4Time) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4Count)(pr)

	for _, row := range rowsInGroup {
		_, isNull, err := e.args[0].EvalTime(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}

		*p++
	}

	return nil
}

type countOriginal4Duration struct {
	baseCount
}

func (e *countOriginal4Duration) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4Count)(pr)

	for _, row := range rowsInGroup {
		_, isNull, err := e.args[0].EvalDuration(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}

		*p++
	}

	return nil
}

type countOriginal4JSON struct {
	baseCount
}

func (e *countOriginal4JSON) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4Count)(pr)

	for _, row := range rowsInGroup {
		_, isNull, err := e.args[0].EvalJSON(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}

		*p++
	}

	return nil
}

type countOriginal4String struct {
	baseCount
}

func (e *countOriginal4String) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4Count)(pr)

	for _, row := range rowsInGroup {
		_, isNull, err := e.args[0].EvalString(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}

		*p++
	}

	return nil
}

type countPartial struct {
	baseCount
}

func (e *countPartial) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4Count)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}

		*p += input
	}
	return nil
}

func (*countPartial) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	p1, p2 := (*partialResult4Count)(src), (*partialResult4Count)(dst)
	*p2 += *p1
	return nil
}
