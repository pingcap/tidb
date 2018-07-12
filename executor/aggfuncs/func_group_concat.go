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
	"bytes"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"strings"
)

type baseGroupConcat4String struct {
	baseAggFunc
}

func (e *baseGroupConcat4String) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4ConcatString)(pr)
	if p.buffer == nil {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendString(e.ordinal, p.buffer.String())
	return nil
}

type basePartialResult4GroupConcat struct {
	sep       string
	sepInited bool
	buffer    *bytes.Buffer
}

type partialResult4ConcatString struct {
	basePartialResult4GroupConcat
}

type groupConcat4String struct {
	baseGroupConcat4String
}

func (e *groupConcat4String) AllocPartialResult() PartialResult {
	return PartialResult(new(partialResult4ConcatString))
}

func (e *groupConcat4String) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4ConcatString)(pr)
	p.sep, p.sepInited, p.buffer = "", false, nil
}

func (e *groupConcat4String) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (err error) {
	p := (*partialResult4ConcatString)(pr)
	if !p.sepInited {
		// Separator can only be a string constant value, so it would never be null.
		p.sep, _, err = e.args[len(e.args)-1].EvalString(sctx, nil)
		if err != nil {
			return errors.Trace(err)
		}
	}
	v, isNull, valsBuf := "", false, make([]string, len(e.args)-1)
	for _, row := range rowsInGroup {
		for i, l := 0, len(e.args)-1; i < l; i++ {
			v, isNull, err = e.args[i].EvalString(sctx, row)
			if err != nil {
				return errors.Trace(err)
			}
			if isNull {
				continue
			}
			valsBuf[i] = v
		}
		// write separator
		if p.buffer == nil {
			p.buffer = &bytes.Buffer{}
		} else {
			p.buffer.WriteString(p.sep)
		}
		// write values
		for _, s := range valsBuf {
			p.buffer.WriteString(s)
		}
	}
	// TODO: if total length is greater than global var group_concat_max_len, truncate it.
	return nil
}

type partialResult4ConcatDistinctString struct {
	basePartialResult4GroupConcat
	valSet stringSet
}

type groupConcat4DistinctString struct {
	baseGroupConcat4String
}

func (e *groupConcat4DistinctString) AllocPartialResult() PartialResult {
	p := new(partialResult4ConcatDistinctString)
	p.valSet = newStringSet()
	return PartialResult(p)
}

func (e *groupConcat4DistinctString) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4ConcatDistinctString)(pr)
	p.sep, p.sepInited, p.buffer, p.valSet = "", false, nil, newStringSet()
}

func (e *groupConcat4DistinctString) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (err error) {
	p := (*partialResult4ConcatDistinctString)(pr)
	if !p.sepInited {
		// Separator can only be a string constant value, so it would never be null.
		p.sep, _, err = e.args[len(e.args)-1].EvalString(sctx, nil)
		if err != nil {
			return errors.Trace(err)
		}
	}
	v, isNull, valsBuf, joinedVals := "", false, make([]string, len(e.args)-1), ""
	for _, row := range rowsInGroup {
		for i, l := 0, len(e.args)-1; i < l; i++ {
			v, isNull, err = e.args[i].EvalString(sctx, row)
			if err != nil {
				return errors.Trace(err)
			}
			if isNull {
				continue
			}
			valsBuf[i] = v
		}
		joinedVals = strings.Join(valsBuf, "")
		if p.valSet.exist(joinedVals) {
			continue
		}
		p.valSet.insert(joinedVals)
		// write separator
		if p.buffer == nil {
			p.buffer = &bytes.Buffer{}
		} else {
			p.buffer.WriteString(p.sep)
		}
		// write values
		for _, s := range valsBuf {
			p.buffer.WriteString(s)
		}
	}
	// TODO: if total length is greater than global var group_concat_max_len, truncate it.
	return nil
}
