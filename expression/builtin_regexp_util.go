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
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/chunk"
)

// Parameters may be const or ignored by the user, so different situations should be considered
// We can handle parameters more easily with this struct.
//
// isNull field shows if user ignores this param in sql
// for example:
//
//	select regexp_like("123", "123", "m"), here isNull field is false for the third parameter
//	select regexp_like("123", "123"), here isNull field is true for the third parameter
type regexpParam struct {
	constStrVal   string
	constIntVal   int64
	defaultIntVal int64 // default value when isNull is true
	isConst       bool
	isNull        bool
	col           *chunk.Column
}

func (re *regexpParam) getCol() *chunk.Column {
	return re.col
}

func (re *regexpParam) getStringVal(id int) string {
	if re.isNull {
		return ""
	}

	if re.isConst {
		return re.constStrVal
	}

	return re.col.GetString(id)
}

func (re *regexpParam) getIntVal(id int) int64 {
	if re.isNull {
		return re.defaultIntVal
	}

	if re.isConst {
		return re.constIntVal
	}

	return re.col.GetInt64(id)
}

func buildStringParam(bf *baseBuiltinFunc, id int, input *chunk.Chunk, isNull bool) (*regexpParam, error) {
	var pa regexpParam
	var err error

	pa.isNull = isNull
	if pa.isNull {
		return &pa, nil
	}

	pa.col, err = bf.bufAllocator.get()
	if err != nil {
		return nil, err
	}

	// Get values from input
	if err := bf.args[id].VecEvalString(bf.ctx, input, pa.col); err != nil {
		return nil, err
	}

	// Check if this is a const value
	pa.isConst = bf.args[id].ConstItem(bf.ctx.GetSessionVars().StmtCtx)
	if pa.isConst {
		// Initialize the const
		pa.constStrVal, err = getColumnConstValString(pa.col, input.NumRows())
		if err != nil {
			return nil, err
		}
	}

	return &pa, nil
}

func buildIntParam(bf *baseBuiltinFunc, id int, input *chunk.Chunk, isNull bool, defaultIntVal int64) (*regexpParam, error) {
	var pa regexpParam
	var err error

	pa.isNull = isNull
	if pa.isNull {
		pa.defaultIntVal = defaultIntVal
		return &pa, nil
	}

	pa.col, err = bf.bufAllocator.get()
	if err != nil {
		return nil, err
	}

	// Get values from input
	if err := bf.args[id].VecEvalInt(bf.ctx, input, pa.col); err != nil {
		return nil, err
	}

	// Check if this is a const value
	pa.isConst = bf.args[id].ConstItem(bf.ctx.GetSessionVars().StmtCtx)
	if pa.isConst {
		// Initialize the const
		pa.constIntVal, err = getColumnConstValInt(pa.col, input.NumRows())
		if err != nil {
			return nil, err
		}
	}

	return &pa, nil
}

func getColumnConstValString(col *chunk.Column, n int) (string, error) {
	// Precondition: col is generated from a constant expression
	for i := 0; i < n; i++ {
		if col.IsNull(i) {
			continue
		}
		return col.GetString(i), nil
	}
	return "", errors.New("Can't get const string")
}

func getColumnConstValInt(col *chunk.Column, n int) (int64, error) {
	// Precondition: col is generated from a constant expression
	for i := 0; i < n; i++ {
		if col.IsNull(i) {
			continue
		}
		return col.GetInt64(i), nil
	}
	return 0, errors.New("Can't get const int")
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
		re, err := compile(patterns.GetString(i))
		reg.memorizedRegexp = re
		reg.memorizedErr = err
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
		if pa.col != nil {
			bf.bufAllocator.put(pa.col)
		}
	}
}

func getBuffers(params []*regexpParam) []*chunk.Column {
	buffers := make([]*chunk.Column, 0, 5)
	for _, pa := range params {
		if pa.col != nil {
			buffers = append(buffers, pa.col)
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

func utf8Len(b byte) int {
	flag := uint8(128)
	if (flag & b) == 0 {
		return 1
	}

	length := 0

	for ; (flag & b) != 0; flag >>= 1 {
		length++
	}

	return length
}

// This string should always be valid which means that it should always return true in ValidString(str)
func trimUtf8String(str *string, trimmedNum int64) int64 {
	totalLenTrimmed := int64(0)
	for ; trimmedNum > 0; trimmedNum-- {
		length := utf8Len((*str)[0]) // character length
		(*str) = (*str)[length:]
		totalLenTrimmed += int64(length)
	}
	return totalLenTrimmed
}

// Convert a binary index to index referring to utf8
func convertPosInUtf8(str *string, pos int64) int64 {
	preStr := (*str)[:pos]
	preStrNum := utf8.RuneCountInString(preStr)
	return int64(preStrNum + 1)
}
