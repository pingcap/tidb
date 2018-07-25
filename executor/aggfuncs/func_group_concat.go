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

	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pkg/errors"
)

type baseGroupConcat4String struct {
	baseAggFunc

	sep string
}

func (e *baseGroupConcat4String) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4GroupConcat)(pr)
	if p.buffer == nil {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendString(e.ordinal, p.buffer.String())
	return nil
}

type basePartialResult4GroupConcat struct {
	buffer *bytes.Buffer
}

type partialResult4GroupConcat struct {
	basePartialResult4GroupConcat
}

type groupConcat struct {
	baseGroupConcat4String
}

func (e *groupConcat) AllocPartialResult() PartialResult {
	return PartialResult(new(partialResult4GroupConcat))
}

func (e *groupConcat) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4GroupConcat)(pr)
	p.buffer = nil
}

func (e *groupConcat) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (err error) {
	p := (*partialResult4GroupConcat)(pr)
	v, isNull, isWriteSep := "", false, false
	for _, row := range rowsInGroup {
		isWriteSep = false
		for _, arg := range e.args {
			v, isNull, err = arg.EvalString(sctx, &row)
			if err != nil {
				return errors.WithStack(err)
			}
			if isNull {
				continue
			}
			isWriteSep = true
			if p.buffer == nil {
				p.buffer = &bytes.Buffer{}
			}
			p.buffer.WriteString(v)
		}
		if isWriteSep {
			p.buffer.WriteString(e.sep)
		}
	}
	p.buffer.Truncate(p.buffer.Len() - len(e.sep))
	// TODO: if total length is greater than global var group_concat_max_len, truncate it.
	// issue: #7034
	return nil
}

type partialResult4GroupConcatDistinct struct {
	basePartialResult4GroupConcat
	valsBuf *bytes.Buffer
	valSet  stringSet
}

type groupConcatDistinct struct {
	baseGroupConcat4String
}

func (e *groupConcatDistinct) AllocPartialResult() PartialResult {
	p := new(partialResult4GroupConcatDistinct)
	p.valsBuf = &bytes.Buffer{}
	p.valSet = newStringSet()
	return PartialResult(p)
}

func (e *groupConcatDistinct) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4GroupConcatDistinct)(pr)
	p.buffer, p.valSet = nil, newStringSet()
}

func (e *groupConcatDistinct) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (err error) {
	p := (*partialResult4GroupConcatDistinct)(pr)
	v, isNull := "", false
	for _, row := range rowsInGroup {
		p.valsBuf.Reset()
		for _, arg := range e.args {
			v, isNull, err = arg.EvalString(sctx, &row)
			if err != nil {
				return errors.WithStack(err)
			}
			if isNull {
				continue
			}
			p.valsBuf.WriteString(v)
		}
		joinedVals := p.valsBuf.String()
		if p.valSet.exist(joinedVals) {
			continue
		}
		p.valSet.insert(joinedVals)
		// write separator
		if p.buffer == nil {
			p.buffer = &bytes.Buffer{}
		} else {
			p.buffer.WriteString(e.sep)
		}
		// write values
		p.buffer.WriteString(joinedVals)
	}
	// TODO: if total length is greater than global var group_concat_max_len, truncate it.
	// issue: #7034
	return nil
}
