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
	"github.com/pingcap/tidb/types"
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

	chk.AppendJSON(e.ordinal, json.CreateBinary(p.array))
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

		result = append(result, res.GetValue())
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

type baseJsonObjectAgg struct {
	baseAggFunc
}

type partialResult4JsonObjectAgg struct {
	entries map[string]interface{}
}

func (e *baseJsonObjectAgg) AllocPartialResult() PartialResult {
	res := partialResult4JsonObjectAgg{}
	res.entries = make(map[string]interface{})
	return PartialResult(&res)
}

func (e *baseJsonObjectAgg) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4JsonObjectAgg)(pr)
	p.entries = make(map[string]interface{})
}

func (e *baseJsonObjectAgg) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4JsonObjectAgg)(pr)
	if len(p.entries) == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}

	chk.AppendJSON(e.ordinal, json.CreateBinary(p.entries))
	return nil
}

func (e *baseJsonObjectAgg) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4JsonObjectAgg)(pr)
	for _, row := range rowsInGroup {
		key, err := e.args[0].Eval(row)
		if err != nil {
			return errors.Trace(err)
		}

		var value types.Datum
		value, err = e.args[1].Eval(row)
		if err != nil {
			return errors.Trace(err)
		}

		p.entries[key.GetString()] = value.GetValue()
	}
	return nil
}

type original4JsonObjectAgg struct {
	baseJsonObjectAgg
}

type partial4JsonObjectAgg struct {
	baseJsonObjectAgg
}

func (e *partial4JsonObjectAgg) MergePartialResult(sctx sessionctx.Context, src PartialResult, dst PartialResult) error {
	p1, p2 := (*partialResult4JsonObjectAgg)(src), (*partialResult4JsonObjectAgg)(dst)
	for k, v := range p1.entries {
		p2.entries[k] = v
	}
	return nil
}
