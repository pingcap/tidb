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

	"github.com/pingcap/errors"
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
	constStrVal   string
	constIntVal   int64
	defaultIntVal int64 // default value when notProvided is true
	isConst       bool
	notProvided   bool
	col           *chunk.Column
}

func (re *regexpParam) getCol() *chunk.Column {
	return re.col
}

func (re *regexpParam) getStringVal(id int) string {
	if re.notProvided {
		return ""
	}

	if re.isConst {
		return re.constStrVal
	}

	return re.getCol().GetString(id)
}

func (re *regexpParam) getIntVal(id int) int64 {
	if re.notProvided {
		return re.defaultIntVal
	}

	if re.isConst {
		return re.constIntVal
	}

	return re.getCol().GetInt64(id)
}

// bool return value: return true when we get a const null parameter
func buildStringParam(bf *baseBuiltinFunc, id int, input *chunk.Chunk, notProvided bool) (*regexpParam, bool, error) {
	var pa regexpParam
	var err error

	pa.notProvided = notProvided
	if pa.notProvided {
		return &pa, false, nil
	}

	pa.col, err = bf.bufAllocator.get()
	if err != nil {
		return nil, false, err
	}

	// Get values from input
	if err := bf.args[id].VecEvalString(bf.ctx, input, pa.getCol()); err != nil {
		return nil, false, err
	}

	// Check if this is a const value
	pa.isConst = bf.args[id].ConstItem(bf.ctx.GetSessionVars().StmtCtx)
	if pa.isConst {
		// Initialize the const
		var isConstNull bool
		pa.constStrVal, isConstNull = getColumnConstValString(pa.getCol(), input.NumRows())
		if isConstNull {
			return nil, isConstNull, nil
		}
	}

	return &pa, false, nil
}

// bool return value: return true when we get a const null parameter
func buildIntParam(bf *baseBuiltinFunc, id int, input *chunk.Chunk, notProvided bool, defaultIntVal int64) (*regexpParam, bool, error) {
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
	if err := bf.args[id].VecEvalInt(bf.ctx, input, pa.getCol()); err != nil {
		return nil, false, err
	}

	// Check if this is a const value
	pa.isConst = bf.args[id].ConstItem(bf.ctx.GetSessionVars().StmtCtx)
	if pa.isConst {
		// Initialize the const
		var isConstNull bool
		pa.constIntVal, isConstNull = getColumnConstValInt(pa.getCol(), input.NumRows())
		if isConstNull {
			return nil, isConstNull, nil
		}
	}

	return &pa, false, nil
}

// bool return value: return true when we get a const null parameter
func getColumnConstValString(col *chunk.Column, n int) (string, bool) {
	// Precondition: col is generated from a constant expression
	for i := 0; i < n; i++ {
		if col.IsNull(i) {
			return "", true
		}
		return col.GetString(i), false
	}
	return "", true
}

// bool return value: return true when we get a const null parameter
func getColumnConstValInt(col *chunk.Column, n int) (int64, bool) {
	// Precondition: col is generated from a constant expression
	for i := 0; i < n; i++ {
		if col.IsNull(i) {
			return 0, true
		}
		return col.GetInt64(i), false
	}
	return 0, true
}

// memorized regexp means the constant pattern.
// Sometimes user may input a constant pattern, and it's unnecessary to compile
// the regexp.Regexp each time.
type regexpMemorizedSig struct {
	memorizedRegexp *regexp.Regexp
	memorizedErr    error
}

func (reg *regexpMemorizedSig) isMemorizedRegexpInitialized() bool {
	return !(reg.memorizedRegexp == nil && reg.memorizedErr == nil)
}

func (reg *regexpMemorizedSig) memorize(compile func(string) (*regexp.Regexp, error), pattern string) {
	re, err := compile(pattern)
	reg.memorizedRegexp = re
	reg.memorizedErr = err
}

func (reg *regexpMemorizedSig) initMemoizedRegexp(compile func(string) (*regexp.Regexp, error), patterns *chunk.Column, n int) {
	// Precondition: patterns is generated from a constant expression
	if n == 0 {
		// If the input rownum is zero, the Regexp error shouldn't be generated.
		return
	}
	for i := 0; i < n; i++ {
		if patterns.IsNull(i) {
			continue
		}
		// Compile this constant pattern, so that we can avoid this repeatable work
		reg.memorize(compile, patterns.GetString(i))
		break
	}
	if !reg.isMemorizedRegexpInitialized() {
		reg.memorizedErr = errors.New("No valid regexp pattern found")
	}
	if reg.memorizedErr != nil {
		reg.memorizedRegexp = nil
	}
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
