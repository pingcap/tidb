// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"regexp"
	"sync"

	"github.com/pingcap/tidb/util/chunk"
)

// Parameters may be const or ignored by the user, so different situations should be considered
// We can handle parameters more easily with this struct.
//
// notProvided field shows if user ignores this param in sql
// for example:
//
//	select regexp_like("123", "123", "m"), here notProvided field is false for the third parameter
//	select regexp_like("123", "123"), here notProvided field is true for the third parameter
type regexpParam struct {
	defaultStrVal string
	defaultIntVal int64 // default value when notProvided is true
	isConst       bool
	notProvided   bool
	col           *chunk.Column
}

func (re *regexpParam) getCol() *chunk.Column {
	return re.col
}

func (re *regexpParam) getStringVal(id int) string {
	if re.notProvided || re.isConst {
		return re.defaultStrVal
	}

	return re.getCol().GetString(id)
}

func (re *regexpParam) getIntVal(id int) int64 {
	if re.notProvided || re.isConst {
		return re.defaultIntVal
	}

	return re.getCol().GetInt64(id)
}

// bool return value: return true when we get a const null parameter
func buildStringParam(bf *baseBuiltinFunc, idx int, input *chunk.Chunk, notProvided bool) (*regexpParam, bool, error) {
	var pa regexpParam
	var err error

	pa.notProvided = notProvided
	if pa.notProvided {
		pa.defaultStrVal = ""
		return &pa, false, nil
	}

	pa.col, err = bf.bufAllocator.get()
	if err != nil {
		return nil, false, err
	}

	// Get values from input
	if err := bf.args[idx].VecEvalString(bf.ctx, input, pa.getCol()); err != nil {
		return nil, false, err
	}

	// Check if this is a const value
	pa.isConst = bf.args[idx].ConstItem(bf.ctx.GetSessionVars().StmtCtx)
	if pa.isConst {
		// Initialize the const
		var isConstNull bool
		pa.defaultStrVal, isConstNull, err = bf.args[idx].EvalString(bf.ctx, chunk.Row{})
		if isConstNull {
			return nil, isConstNull, nil
		}
	}

	return &pa, false, nil
}

// bool return value: return true when we get a const null parameter
func buildIntParam(bf *baseBuiltinFunc, idx int, input *chunk.Chunk, notProvided bool, defaultIntVal int64) (*regexpParam, bool, error) {
	var pa regexpParam
	var err error

	pa.notProvided = notProvided
	if pa.notProvided {
		pa.defaultIntVal = defaultIntVal
		return &pa, false, nil
	}

	pa.col, err = bf.bufAllocator.get()
	if err != nil {
		return nil, false, err
	}

	// Get values from input
	if err := bf.args[idx].VecEvalInt(bf.ctx, input, pa.getCol()); err != nil {
		return nil, false, err
	}

	// Check if this is a const value
	pa.isConst = bf.args[idx].ConstItem(bf.ctx.GetSessionVars().StmtCtx)
	if pa.isConst {
		// Initialize the const
		var isConstNull bool
		pa.defaultIntVal, isConstNull, err = bf.args[idx].EvalInt(bf.ctx, chunk.Row{})
		// pa.defaultIntVal, isConstNull = getColumnConstValInt(pa.getCol(), input.NumRows())
		if isConstNull {
			return nil, isConstNull, nil
		}
	}

	return &pa, false, nil
}

// memorized regexp means the constant pattern.
// Sometimes user may input a constant pattern, and it's unnecessary to compile
// the regexp.Regexp each time.
type regexpMemorizedSig struct {
	memorizedRegexp *regexp.Regexp
	memorizedErr    error
	lock            sync.Mutex
}

func (reg *regexpMemorizedSig) isMemorizedRegexpInitialized() bool {
	return !(reg.memorizedRegexp == nil && reg.memorizedErr == nil)
}

func (reg *regexpMemorizedSig) memorize(compile func(string) (*regexp.Regexp, error), pattern string) {
	re, err := compile(pattern)
	reg.memorizedRegexp = re
	reg.memorizedErr = err
}

func releaseBuffers(bf *baseBuiltinFunc, params []*regexpParam) {
	for _, pa := range params {
		if pa.getCol() != nil {
			bf.bufAllocator.put(pa.getCol())
		}
	}
}

func getBuffers(params []*regexpParam) []*chunk.Column {
	buffers := make([]*chunk.Column, 0, 6)
	for _, pa := range params {
		if pa.getCol() != nil {
			buffers = append(buffers, pa.getCol())
		}
	}
	return buffers
}

func isResultNull(columns []*chunk.Column, i int) bool {
	for _, col := range columns {
		if col.IsNull(i) {
			return true
		}
	}
	return false
}

func fillNullStringIntoResult(result *chunk.Column, num int) {
	result.ReserveString(num)
	for i := 0; i < num; i++ {
		result.AppendNull()
	}
}
