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
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pkg/errors"
)

type baseJsonArrayAgg struct {
	baseAggFunc
}

type partialResult4JsonArrayAgg struct {
	array []interface{}
}

func (e *baseJsonArrayAgg) AllocPartialResult() PartialResult {
	return PartialResult(&partialResult4JsonArrayAgg{})
}

func (e *baseJsonArrayAgg) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4JsonArrayAgg)(pr)
	p.array = make([]interface{}, 0, 0)
}

func (e *baseJsonArrayAgg) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4JsonArrayAgg)(pr)
	if len(p.array) == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	result := json.CreateBinary(p.array)
	chk.AppendJSON(e.ordinal, result)
	return nil
}

func (e *baseJsonArrayAgg) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4JsonArrayAgg)(pr)
	result := make([]interface{}, 0, len(rowsInGroup))
	for _, row := range rowsInGroup {
		res, err := e.args[0].Eval(row)
		if err != nil {
			return errors.Trace(err)
		}

		var finalResult string
		finalResult, err = res.ToString()
		if err != nil {
			return errors.Trace(err)
		}

		result = append(result, finalResult)
	}
	p.array = append(p.array, result...)
	return nil
}

type original4JsonArrayAgg struct {
	baseJsonArrayAgg
}

type partial4JsonArrayAgg struct {
	baseJsonArrayAgg
}

func (e *partial4JsonArrayAgg) MergePartialResult(sctx sessionctx.Context, src PartialResult, dst PartialResult) error {
	p1, p2 := (*partialResult4JsonArrayAgg)(src), (*partialResult4JsonArrayAgg)(dst)
	p2.array = append(p2.array, p1.array...)
	return nil
}
