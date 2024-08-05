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
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// Parameters may be const or ignored by the user, so different situations should be considered
// We can handle parameters more easily with this struct.
//
// When a parameter is not provided by user or is const, col field will be nil and we should
// provide this parameter with defaultxxx field.
//
// for example:
//
//	select regexp_like(t.a, "123", "m") from t, here col == nil for the second and third parameter
//	select regexp_like(t.a, "123", "123"), here col != nil for the second and third parameter
//
// defaultxxx: When a parameter is not provided or const, defaultxxx field should be it's value.
type funcParam struct {
	defaultStrVal string
	defaultIntVal int64
	col           *chunk.Column
}

func (re *funcParam) setStrVal(val string) {
	re.defaultStrVal = val
}

func (re *funcParam) setIntVal(val int64) {
	re.defaultIntVal = val
}

func (re *funcParam) setCol(newCol *chunk.Column) {
	re.col = newCol
}

func (re *funcParam) getCol() *chunk.Column {
	return re.col
}

func (re *funcParam) getStringVal(id int) string {
	if re.col == nil {
		return re.defaultStrVal
	}

	return re.getCol().GetString(id)
}

func (re *funcParam) getIntVal(id int) int64 {
	if re.col == nil {
		return re.defaultIntVal
	}

	return re.getCol().GetInt64(id)
}

// bool return value: return true when we get a const null parameter
func buildStringParam(ctx EvalContext, bf *baseBuiltinFunc, idx int, input *chunk.Chunk, notProvided bool) (*funcParam, bool, error) {
	var pa funcParam
	var err error

	if notProvided {
		pa.defaultStrVal = ""
		return &pa, false, nil
	}

	// Check if this is a const value.
	// funcParam will not be shared between evaluations, so we just need it to be const in one ctx.
	if bf.args[idx].ConstLevel() >= ConstOnlyInContext {
		// Initialize the const
		var isConstNull bool
		pa.defaultStrVal, isConstNull, err = bf.args[idx].EvalString(ctx, chunk.Row{})
		if isConstNull || err != nil {
			return nil, isConstNull, err
		}
		return &pa, false, nil
	}

	pa.col, err = bf.bufAllocator.get()
	if err != nil {
		return nil, false, err
	}

	// Get values from input
	err = bf.args[idx].VecEvalString(ctx, input, pa.getCol())

	return &pa, false, err
}

// bool return value: return true when we get a const null parameter
func buildIntParam(ctx EvalContext, bf *baseBuiltinFunc, idx int, input *chunk.Chunk, notProvided bool, defaultIntVal int64) (*funcParam, bool, error) {
	var pa funcParam
	var err error

	if notProvided {
		pa.defaultIntVal = defaultIntVal
		return &pa, false, nil
	}

	// Check if this is a const value
	// funcParam will not be shared between evaluations, so we just need it to be const in one ctx.
	if bf.args[idx].ConstLevel() >= ConstOnlyInContext {
		// Initialize the const
		var isConstNull bool
		pa.defaultIntVal, isConstNull, err = bf.args[idx].EvalInt(ctx, chunk.Row{})
		if isConstNull || err != nil {
			return nil, isConstNull, err
		}
		return &pa, false, nil
	}

	pa.col, err = bf.bufAllocator.get()
	if err != nil {
		return nil, false, err
	}

	// Get values from input
	err = bf.args[idx].VecEvalInt(ctx, input, pa.getCol())

	return &pa, false, err
}
