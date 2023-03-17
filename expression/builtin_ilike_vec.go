// Copyright 2023 PingCAP, Inc.
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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/stringutil"
)

// LowerAlphaASCII only lowers alpha ascii
func LowerAlphaASCII(loweredCol *chunk.Column, rowNum int) {
	for i := 0; i < rowNum; i++ {
		str := loweredCol.GetString(i)
		strBytes := hack.Slice(str)

		stringutil.LowerOneString(strBytes)
	}
}

// LowerAlphaASCIIExcludeEscapeChar lower alpha ascii exclude escape char
func LowerAlphaASCIIExcludeEscapeChar(loweredCol *chunk.Column, rowNum int, excludedChar int64) int64 {
	actualEscapeChar := excludedChar
	for i := 0; i < rowNum; i++ {
		str := loweredCol.GetString(i)
		strBytes := hack.Slice(str)

		actualEscapeChar = int64(stringutil.LowerOneStringExcludeEscapeChar(strBytes, byte(excludedChar)))
	}
	return actualEscapeChar
}

func (b *builtinIlikeSig) vectorized() bool {
	return true
}

func (b *builtinIlikeSig) canMemorize(param *funcParam) bool {
	return param.getCol() == nil
}

func (b *builtinIlikeSig) tryToMemorize(param *funcParam, escape int64) {
	if !b.canMemorize(param) {
		return
	}

	memorization := func() {
		if b.pattern == nil {
			b.pattern = collate.ConvertAndGetBinCollation(b.collation).Pattern()
			b.pattern.Compile(param.getStringVal(0), byte(escape))
			b.isMemorizedPattern = true
		}
	}

	// Only be executed once to achieve thread-safe
	b.once.Do(memorization)
}

func (b *builtinIlikeSig) getEscape(input *chunk.Chunk, result *chunk.Column) (int64, bool, error) {
	rowNum := input.NumRows()
	escape := int64('\\')

	if !b.args[2].ConstItem(b.ctx.GetSessionVars().StmtCtx) {
		return escape, true, errors.Errorf("escape should be const")
	}

	escape, isConstNull, err := b.args[2].EvalInt(b.ctx, chunk.Row{})
	if isConstNull {
		fillNullStringIntoResult(result, rowNum)
		return escape, true, nil
	} else if err != nil {
		return escape, true, err
	}
	return escape, false, nil
}

func (b *builtinIlikeSig) lowerExpr(param *funcParam, rowNum int) {
	col := param.getCol()
	if col == nil {
		str := param.getStringVal(0)
		strBytes := hack.Slice(str)
		stringutil.LowerOneString(strBytes)
		param.setStrVal(str)
		return
	}

	tmpExprCol := param.getCol().CopyConstruct(nil)
	LowerAlphaASCII(tmpExprCol, rowNum)
	param.setCol(tmpExprCol)
}

func (b *builtinIlikeSig) lowerPattern(param *funcParam, rowNum int, escape int64) int64 {
	col := param.getCol()
	if col == nil {
		str := param.getStringVal(0)
		strBytes := hack.Slice(str)
		escape = int64(stringutil.LowerOneStringExcludeEscapeChar(strBytes, byte(escape)))
		param.setStrVal(str)
		return escape
	}

	tmpPatternCol := param.getCol().CopyConstruct(nil)
	escape = LowerAlphaASCIIExcludeEscapeChar(tmpPatternCol, rowNum, escape)
	param.setCol(tmpPatternCol)

	return escape
}

func (b *builtinIlikeSig) vecVec(params []*funcParam, rowNum int, escape int64, result *chunk.Column) error {
	result.ResizeInt64(rowNum, false)
	result.MergeNulls(params[0].getCol(), params[1].getCol())
	i64s := result.Int64s()
	for i := 0; i < rowNum; i++ {
		if result.IsNull(i) {
			continue
		}
		b.pattern.Compile(params[1].getStringVal(i), byte(escape))
		match := b.pattern.DoMatch(params[0].getStringVal(i))
		i64s[i] = boolToInt64(match)
	}
	return nil
}

func (b *builtinIlikeSig) constVec(expr string, param *funcParam, rowNum int, escape int64, result *chunk.Column) error {
	result.ResizeInt64(rowNum, false)
	result.MergeNulls(param.getCol())
	i64s := result.Int64s()
	for i := 0; i < rowNum; i++ {
		if result.IsNull(i) {
			continue
		}
		b.pattern.Compile(param.getStringVal(i), byte(escape))
		match := b.pattern.DoMatch(expr)
		i64s[i] = boolToInt64(match)
	}
	return nil
}

func (b *builtinIlikeSig) ilikeWithMemorization(exprParam *funcParam, rowNum int, result *chunk.Column) error {
	result.ResizeInt64(rowNum, false)
	result.MergeNulls(exprParam.getCol())
	i64s := result.Int64s()
	for i := 0; i < rowNum; i++ {
		if result.IsNull(i) {
			continue
		}
		match := b.pattern.DoMatch(exprParam.getStringVal(i))
		i64s[i] = boolToInt64(match)
	}
	return nil
}

func (b *builtinIlikeSig) ilikeWithoutMemorization(params []*funcParam, rowNum int, escape int64, result *chunk.Column) error {
	if params[0].getCol() == nil {
		return b.constVec(params[0].getStringVal(0), params[1], rowNum, escape, result)
	}

	return b.vecVec(params, rowNum, escape, result)
}

func (b *builtinIlikeSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	rowNum := input.NumRows()
	params := make([]*funcParam, 0, 3)
	defer releaseBuffers(&b.baseBuiltinFunc, params)

	for i := 0; i < 2; i++ {
		param, isConstNull, err := buildStringParam(&b.baseBuiltinFunc, i, input, false)
		if err != nil {
			return ErrRegexp.GenWithStackByArgs(err)
		}
		if isConstNull {
			fillNullStringIntoResult(result, rowNum)
			return nil
		}
		params = append(params, param)
	}

	escape, ret, err := b.getEscape(input, result)
	if err != nil || ret {
		return err
	}

	b.lowerExpr(params[0], rowNum)
	escape = b.lowerPattern(params[1], rowNum, escape)

	b.tryToMemorize(params[1], escape)
	if !b.isMemorizedPattern {
		b.pattern = collate.ConvertAndGetBinCollation(b.collation).Pattern()
		return b.ilikeWithoutMemorization(params, rowNum, escape, result)
	}

	return b.ilikeWithMemorization(params[0], rowNum, result)
}
