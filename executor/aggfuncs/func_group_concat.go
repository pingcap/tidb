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

	"github.com/cznic/mathutil"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
)

type baseGroupConcat4String struct {
	baseAggFunc

	sep    string
	maxLen uint64
	// According to MySQL, a 'group_concat' function generates exactly one 'truncated' warning during its life time, no matter
	// how many group actually truncated. 'truncated' acts as a sentinel to indicate whether this warning has already been
	// generated.
	truncated bool
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

func (e *baseGroupConcat4String) truncatePartialResultIfNeed(sctx sessionctx.Context, buffer *bytes.Buffer) (err error) {
	if e.maxLen > 0 && uint64(buffer.Len()) > e.maxLen {
		i := mathutil.MaxInt
		if uint64(i) > e.maxLen {
			i = int(e.maxLen)
		}
		buffer.Truncate(i)
		if !e.truncated {
			sctx.GetSessionVars().StmtCtx.AppendWarning(expression.ErrCutValueGroupConcat)
		}
		e.truncated = true
	}
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
	v, isNull := "", false
	for _, row := range rowsInGroup {
		if p.buffer != nil {
			p.buffer.WriteString(e.sep)
		}
		isAllNull := true
		for _, arg := range e.args {
			v, isNull, err = arg.EvalString(sctx, row)
			if err != nil {
				return errors.Trace(err)
			}
			if isNull {
				continue
			}
			isAllNull = false
			if p.buffer == nil {
				p.buffer = &bytes.Buffer{}
			}
			p.buffer.WriteString(v)
		}
		if isAllNull {
			if p.buffer != nil {
				p.buffer.Truncate(p.buffer.Len() - len(e.sep))
			}
			continue
		}
	}
	if p.buffer != nil {
		return e.truncatePartialResultIfNeed(sctx, p.buffer)
	}
	return nil
}

func (e *groupConcat) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	p1, p2 := (*partialResult4GroupConcat)(src), (*partialResult4GroupConcat)(dst)
	if p1.buffer == nil {
		return nil
	}
	if p2.buffer == nil {
		p2.buffer = p1.buffer
		return nil
	}
	p2.buffer.WriteString(e.sep)
	p2.buffer.WriteString(p1.buffer.String())
	e.truncatePartialResultIfNeed(sctx, p2.buffer)
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
		allIsNull := true
		p.valsBuf.Reset()
		for _, arg := range e.args {
			v, isNull, err = arg.EvalString(sctx, row)
			if err != nil {
				return errors.Trace(err)
			}
			if isNull {
				continue
			}
			allIsNull = false
			p.valsBuf.WriteString(v)
		}
		if allIsNull {
			continue
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
	if p.buffer != nil {
		return e.truncatePartialResultIfNeed(sctx, p.buffer)
	}
	return nil
}
