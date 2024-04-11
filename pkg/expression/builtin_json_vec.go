// Copyright 2019 PingCAP, Inc.
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
	"bytes"
	goJSON "encoding/json"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

//revive:disable:defer
func vecJSONModify(ctx EvalContext, args []Expression, bufAllocator columnBufferAllocator, input *chunk.Chunk, result *chunk.Column, mt types.JSONModifyType) error {
	nr := input.NumRows()
	jsonBuf, err := bufAllocator.get()
	if err != nil {
		return err
	}
	defer bufAllocator.put(jsonBuf)
	if err := args[0].VecEvalJSON(ctx, input, jsonBuf); err != nil {
		return err
	}

	strBufs := make([]*chunk.Column, (len(args)-1)/2)
	for i := 1; i < len(args); i += 2 {
		strBufs[(i-1)/2], err = bufAllocator.get()
		if err != nil {
			return err
		}
		defer bufAllocator.put(strBufs[(i-1)/2])
		if err := args[i].VecEvalString(ctx, input, strBufs[(i-1)/2]); err != nil {
			return err
		}
	}
	valueBufs := make([]*chunk.Column, (len(args)-1)/2+1)
	for i := 2; i < len(args); i += 2 {
		valueBufs[i/2-1], err = bufAllocator.get()
		if err != nil {
			return err
		}
		defer bufAllocator.put(valueBufs[i/2-1])
		if err := args[i].VecEvalJSON(ctx, input, valueBufs[i/2-1]); err != nil {
			return err
		}
	}
	result.ReserveJSON(nr)
	for i := 0; i < nr; i++ {
		if jsonBuf.IsNull(i) {
			result.AppendNull()
			continue
		}
		pathExprs := make([]types.JSONPathExpression, 0, (len(args)-1)/2+1)
		values := make([]types.BinaryJSON, 0, (len(args)-1)/2+1)
		var pathExpr types.JSONPathExpression
		isNull := false
		for j := 1; j < len(args); j += 2 {
			if strBufs[(j-1)/2].IsNull(i) {
				isNull = true
				break
			}
			pathExpr, err = types.ParseJSONPathExpr(strBufs[(j-1)/2].GetString(i))
			if err != nil {
				return err
			}
			pathExprs = append(pathExprs, pathExpr)
		}
		for j := 2; j < len(args); j += 2 {
			if valueBufs[j/2-1].IsNull(i) {
				values = append(values, types.CreateBinaryJSON(nil))
			} else {
				values = append(values, valueBufs[j/2-1].GetJSON(i))
			}
		}
		if isNull {
			result.AppendNull()
		} else {
			res, err := jsonBuf.GetJSON(i).Modify(pathExprs, values, mt)
			if err != nil {
				return err
			}
			result.AppendJSON(res)
		}
	}
	return nil
}

func (b *builtinJSONStorageFreeSig) vectorized() bool {
	return true
}

func (b *builtinJSONStorageFreeSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalJSON(ctx, input, buf); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	int64s := result.Int64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}

		int64s[i] = 0
	}
	return nil
}

func (b *builtinJSONStorageSizeSig) vectorized() bool {
	return true
}

func (b *builtinJSONStorageSizeSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalJSON(ctx, input, buf); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	int64s := result.Int64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		j := buf.GetJSON(i)

		int64s[i] = int64(len(j.Value)) + 1
	}
	return nil
}

func (b *builtinJSONDepthSig) vectorized() bool {
	return true
}

func (b *builtinJSONDepthSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalJSON(ctx, input, buf); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	int64s := result.Int64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		j := buf.GetJSON(i)
		int64s[i] = int64(j.GetElemDepth())
	}
	return nil
}

func (b *builtinJSONKeysSig) vectorized() bool {
	return true
}

func (b *builtinJSONKeysSig) vecEvalJSON(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalJSON(ctx, input, buf); err != nil {
		return err
	}

	result.ReserveJSON(n)
	var j types.BinaryJSON
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}

		j = buf.GetJSON(i)
		if j.TypeCode != types.JSONTypeCodeObject {
			result.AppendNull()
			continue
		}
		result.AppendJSON(j.GetKeys())
	}
	return nil
}

func (b *builtinJSONInsertSig) vectorized() bool {
	return true
}

func (b *builtinJSONInsertSig) vecEvalJSON(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	err := vecJSONModify(ctx, b.args, b.bufAllocator, input, result, types.JSONModifyInsert)
	return err
}

func (b *builtinJSONReplaceSig) vectorized() bool {
	return true
}

func (b *builtinJSONReplaceSig) vecEvalJSON(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	err := vecJSONModify(ctx, b.args, b.bufAllocator, input, result, types.JSONModifyReplace)
	return err
}

func (b *builtinJSONArraySig) vectorized() bool {
	return true
}

func (b *builtinJSONArraySig) vecEvalJSON(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	nr := input.NumRows()
	jsons := make([][]any, nr)
	for i := 0; i < nr; i++ {
		jsons[i] = make([]any, 0, len(b.args))
	}
	for _, arg := range b.args {
		j, err := b.bufAllocator.get()
		if err != nil {
			return err
		}
		defer b.bufAllocator.put(j)
		if err = arg.VecEvalJSON(ctx, input, j); err != nil {
			return err
		}
		for i := 0; i < nr; i++ {
			if j.IsNull(i) {
				jsons[i] = append(jsons[i], types.CreateBinaryJSON(nil))
			} else {
				jsons[i] = append(jsons[i], j.GetJSON(i))
			}
		}
	}
	result.ReserveJSON(nr)
	for i := 0; i < nr; i++ {
		bj, err := types.CreateBinaryJSONWithCheck(jsons[i])
		if err != nil {
			return err
		}
		result.AppendJSON(bj)
	}
	return nil
}

func (b *builtinJSONMemberOfSig) vectorized() bool {
	return true
}

func (b *builtinJSONMemberOfSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	nr := input.NumRows()

	targetCol, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(targetCol)

	if err := b.args[0].VecEvalJSON(ctx, input, targetCol); err != nil {
		return err
	}

	objCol, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(objCol)

	if err := b.args[1].VecEvalJSON(ctx, input, objCol); err != nil {
		return err
	}

	result.ResizeInt64(nr, false)
	resI64s := result.Int64s()

	result.MergeNulls(targetCol, objCol)
	for i := 0; i < nr; i++ {
		if result.IsNull(i) {
			continue
		}
		obj := objCol.GetJSON(i)
		target := targetCol.GetJSON(i)
		if obj.TypeCode != types.JSONTypeCodeArray {
			resI64s[i] = boolToInt64(types.CompareBinaryJSON(obj, target) == 0)
		} else {
			elemCount := obj.GetElemCount()
			for j := 0; j < elemCount; j++ {
				if types.CompareBinaryJSON(obj.ArrayGetElem(j), target) == 0 {
					resI64s[i] = 1
					break
				}
			}
		}
	}

	return nil
}

func (b *builtinJSONContainsSig) vectorized() bool {
	return true
}

func (b *builtinJSONContainsSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	nr := input.NumRows()

	objCol, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(objCol)

	if err := b.args[0].VecEvalJSON(ctx, input, objCol); err != nil {
		return err
	}

	targetCol, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(targetCol)

	if err := b.args[1].VecEvalJSON(ctx, input, targetCol); err != nil {
		return err
	}

	result.ResizeInt64(nr, false)
	resI64s := result.Int64s()

	if len(b.args) == 3 {
		pathCol, err := b.bufAllocator.get()
		if err != nil {
			return err
		}
		defer b.bufAllocator.put(pathCol)

		if err := b.args[2].VecEvalString(ctx, input, pathCol); err != nil {
			return err
		}

		result.MergeNulls(objCol, targetCol, pathCol)

		var pathExpr types.JSONPathExpression
		for i := 0; i < nr; i++ {
			if result.IsNull(i) {
				continue
			}
			pathExpr, err = types.ParseJSONPathExpr(pathCol.GetString(i))
			if err != nil {
				return err
			}
			if pathExpr.CouldMatchMultipleValues() {
				return types.ErrInvalidJSONPathMultipleSelection
			}

			obj, exists := objCol.GetJSON(i).Extract([]types.JSONPathExpression{pathExpr})
			if !exists {
				result.SetNull(i, true)
				continue
			}

			if types.ContainsBinaryJSON(obj, targetCol.GetJSON(i)) {
				resI64s[i] = 1
			} else {
				resI64s[i] = 0
			}
		}
	} else {
		result.MergeNulls(objCol, targetCol)
		for i := 0; i < nr; i++ {
			if result.IsNull(i) {
				continue
			}
			if types.ContainsBinaryJSON(objCol.GetJSON(i), targetCol.GetJSON(i)) {
				resI64s[i] = 1
			} else {
				resI64s[i] = 0
			}
		}
	}

	return nil
}

func (b *builtinJSONOverlapsSig) vectorized() bool {
	return true
}

func (b *builtinJSONOverlapsSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	nr := input.NumRows()

	objCol, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(objCol)

	if err := b.args[0].VecEvalJSON(ctx, input, objCol); err != nil {
		return err
	}

	targetCol, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(targetCol)

	if err := b.args[1].VecEvalJSON(ctx, input, targetCol); err != nil {
		return err
	}

	result.ResizeInt64(nr, false)
	resI64s := result.Int64s()

	result.MergeNulls(objCol, targetCol)
	for i := 0; i < nr; i++ {
		if result.IsNull(i) {
			continue
		}
		if types.OverlapsBinaryJSON(objCol.GetJSON(i), targetCol.GetJSON(i)) {
			resI64s[i] = 1
		} else {
			resI64s[i] = 0
		}
	}

	return nil
}

func (b *builtinJSONQuoteSig) vectorized() bool {
	return true
}

func (b *builtinJSONQuoteSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		result.AppendString(strconv.Quote(buf.GetString(i)))
	}
	return nil
}

func (b *builtinJSONSearchSig) vectorized() bool {
	return true
}

func (b *builtinJSONSearchSig) vecEvalJSON(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	nr := input.NumRows()
	jsonBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(jsonBuf)
	if err := b.args[0].VecEvalJSON(ctx, input, jsonBuf); err != nil {
		return err
	}
	typeBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(typeBuf)
	if err := b.args[1].VecEvalString(ctx, input, typeBuf); err != nil {
		return err
	}
	searchBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(searchBuf)
	if err := b.args[2].VecEvalString(ctx, input, searchBuf); err != nil {
		return err
	}

	var escapeBuf *chunk.Column
	if len(b.args) >= 4 {
		escapeBuf, err = b.bufAllocator.get()
		if err != nil {
			return err
		}
		defer b.bufAllocator.put(escapeBuf)
		if err := b.args[3].VecEvalString(ctx, input, escapeBuf); err != nil {
			return err
		}
	}

	var pathBufs []*chunk.Column
	if len(b.args) >= 5 {
		pathBufs = make([]*chunk.Column, (len(b.args) - 4))
		for i := 4; i < len(b.args); i++ {
			index := i - 4
			pathBufs[index], err = b.bufAllocator.get()
			if err != nil {
				return err
			}
			defer b.bufAllocator.put(pathBufs[index])
			if err := b.args[i].VecEvalString(ctx, input, pathBufs[index]); err != nil {
				return err
			}
		}
	}

	result.ReserveJSON(nr)

	for i := 0; i < nr; i++ {
		if jsonBuf.IsNull(i) || searchBuf.IsNull(i) || typeBuf.IsNull(i) {
			result.AppendNull()
			continue
		}
		containType := strings.ToLower(typeBuf.GetString(i))
		escape := byte('\\')
		if escapeBuf != nil && !escapeBuf.IsNull(i) {
			escapeStr := escapeBuf.GetString(i)
			if len(escapeStr) == 0 {
				escape = byte('\\')
			} else if len(escapeStr) == 1 {
				escape = escapeStr[0]
			} else {
				return errIncorrectArgs.GenWithStackByArgs("ESCAPE")
			}
		}
		var pathExprs []types.JSONPathExpression
		if pathBufs != nil {
			pathExprs = make([]types.JSONPathExpression, 0, len(b.args)-4)
			for j := 0; j < len(b.args)-4; j++ {
				if pathBufs[j].IsNull(i) {
					break
				}
				pathExpr, err := types.ParseJSONPathExpr(pathBufs[j].GetString(i))
				if err != nil {
					return types.ErrInvalidJSONPath.GenWithStackByArgs(pathBufs[j].GetString(i))
				}
				pathExprs = append(pathExprs, pathExpr)
			}
		}
		bj, isNull, err := jsonBuf.GetJSON(i).Search(containType, searchBuf.GetString(i), escape, pathExprs)
		if err != nil {
			return err
		}
		if isNull {
			result.AppendNull()
			continue
		}
		result.AppendJSON(bj)
	}
	return nil
}

func (b *builtinJSONSetSig) vectorized() bool {
	return true
}

func (b *builtinJSONSetSig) vecEvalJSON(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	err := vecJSONModify(ctx, b.args, b.bufAllocator, input, result, types.JSONModifySet)
	return err
}

func (b *builtinJSONObjectSig) vectorized() bool {
	return true
}

func (b *builtinJSONObjectSig) vecEvalJSON(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	nr := input.NumRows()
	if len(b.args)&1 == 1 {
		err := ErrIncorrectParameterCount.GenWithStackByArgs(ast.JSONObject)
		return err
	}

	jsons := make([]map[string]any, nr)
	for i := 0; i < nr; i++ {
		jsons[i] = make(map[string]any, len(b.args)>>1)
	}

	argBuffers := make([]*chunk.Column, len(b.args))
	var err error
	for i := 0; i < len(b.args); i++ {
		if i&1 == 0 {
			if argBuffers[i], err = b.bufAllocator.get(); err != nil {
				return err
			}
			defer func(buf *chunk.Column) {
				b.bufAllocator.put(buf)
			}(argBuffers[i])

			if err = b.args[i].VecEvalString(ctx, input, argBuffers[i]); err != nil {
				return err
			}
		} else {
			if argBuffers[i], err = b.bufAllocator.get(); err != nil {
				return err
			}
			defer func(buf *chunk.Column) {
				b.bufAllocator.put(buf)
			}(argBuffers[i])

			if err = b.args[i].VecEvalJSON(ctx, input, argBuffers[i]); err != nil {
				return err
			}
		}
	}

	result.ReserveJSON(nr)
	for i := 0; i < len(b.args); i++ {
		if i&1 == 1 {
			keyCol := argBuffers[i-1]
			valueCol := argBuffers[i]

			var key string
			var value types.BinaryJSON
			for j := 0; j < nr; j++ {
				if keyCol.IsNull(j) {
					err := errors.New("JSON documents may not contain NULL member names")
					return err
				}
				key = keyCol.GetString(j)
				if valueCol.IsNull(j) {
					value = types.CreateBinaryJSON(nil)
				} else {
					value = valueCol.GetJSON(j)
				}
				jsons[j][key] = value
			}
		}
	}

	for i := 0; i < nr; i++ {
		bj, err := types.CreateBinaryJSONWithCheck(jsons[i])
		if err != nil {
			return err
		}
		result.AppendJSON(bj)
	}
	return nil
}

func (b *builtinJSONArrayInsertSig) vectorized() bool {
	return true
}

func (b *builtinJSONArrayInsertSig) vecEvalJSON(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	nr := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalJSON(ctx, input, buf); err != nil {
		return err
	}
	pathBufs := make([]*chunk.Column, (len(b.args)-1)/2)
	valueBufs := make([]*chunk.Column, (len(b.args)-1)/2)
	for i := 1; i < len(b.args); i++ {
		if i&1 == 0 {
			valueBufs[i/2-1], err = b.bufAllocator.get()
			if err != nil {
				return err
			}
			defer b.bufAllocator.put(valueBufs[i/2-1])
			if err := b.args[i].VecEvalJSON(ctx, input, valueBufs[i/2-1]); err != nil {
				return err
			}
		} else {
			pathBufs[(i-1)/2], err = b.bufAllocator.get()
			if err != nil {
				return err
			}
			defer b.bufAllocator.put(pathBufs[(i-1)/2])
			if err := b.args[i].VecEvalString(ctx, input, pathBufs[(i-1)/2]); err != nil {
				return err
			}
		}
	}
	var pathExpr types.JSONPathExpression
	var value types.BinaryJSON
	result.ReserveJSON(nr)
	for i := 0; i < nr; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}

		res := buf.GetJSON(i)
		isnull := false
		for j := 0; j < (len(b.args)-1)/2; j++ {
			if pathBufs[j].IsNull(i) {
				isnull = true
				break
			}
			pathExpr, err = types.ParseJSONPathExpr(pathBufs[j].GetString(i))
			if err != nil {
				return types.ErrInvalidJSONPath.GenWithStackByArgs(pathBufs[j].GetString(i))
			}
			if pathExpr.CouldMatchMultipleValues() {
				return types.ErrInvalidJSONPathMultipleSelection
			}
			if valueBufs[j].IsNull(i) {
				value = types.CreateBinaryJSON(nil)
			} else {
				value = valueBufs[j].GetJSON(i)
			}
			res, err = res.ArrayInsert(pathExpr, value)
			if err != nil {
				return err
			}
		}
		if isnull {
			result.AppendNull()
			continue
		}
		result.AppendJSON(res)
	}
	return nil
}

func (b *builtinJSONKeys2ArgsSig) vectorized() bool {
	return true
}

func (b *builtinJSONKeys2ArgsSig) vecEvalJSON(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	nr := input.NumRows()
	jsonBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(jsonBuf)
	if err := b.args[0].VecEvalJSON(ctx, input, jsonBuf); err != nil {
		return err
	}
	pathBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(pathBuf)
	if err := b.args[1].VecEvalString(ctx, input, pathBuf); err != nil {
		return err
	}

	result.ReserveJSON(nr)
	for i := 0; i < nr; i++ {
		if jsonBuf.IsNull(i) || pathBuf.IsNull(i) {
			result.AppendNull()
			continue
		}

		pathExpr, err := types.ParseJSONPathExpr(pathBuf.GetString(i))
		if err != nil {
			return err
		}
		if pathExpr.CouldMatchMultipleValues() {
			return types.ErrInvalidJSONPathMultipleSelection
		}

		jsonItem := jsonBuf.GetJSON(i)
		if jsonItem.TypeCode != types.JSONTypeCodeObject {
			result.AppendNull()
			continue
		}

		res, exists := jsonItem.Extract([]types.JSONPathExpression{pathExpr})
		if !exists || res.TypeCode != types.JSONTypeCodeObject {
			result.AppendNull()
			continue
		}
		result.AppendJSON(res.GetKeys())
	}
	return nil
}

func (b *builtinJSONLengthSig) vectorized() bool {
	return true
}

func (b *builtinJSONLengthSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	nr := input.NumRows()

	result.ResizeInt64(nr, false)
	resI64s := result.Int64s()

	jsonBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(jsonBuf)
	if err := b.args[0].VecEvalJSON(ctx, input, jsonBuf); err != nil {
		return err
	}
	result.MergeNulls(jsonBuf)

	if len(b.args) == 2 {
		pathBuf, err := b.bufAllocator.get()
		if err != nil {
			return err
		}
		defer b.bufAllocator.put(pathBuf)
		if err := b.args[1].VecEvalString(ctx, input, pathBuf); err != nil {
			return err
		}
		result.MergeNulls(pathBuf)

		for i := 0; i < nr; i++ {
			if result.IsNull(i) {
				continue
			}

			pathExpr, err := types.ParseJSONPathExpr(pathBuf.GetString(i))
			if err != nil {
				return err
			}
			if pathExpr.CouldMatchMultipleValues() {
				return types.ErrInvalidJSONPathMultipleSelection
			}

			jsonItem := jsonBuf.GetJSON(i)
			obj, exists := jsonItem.Extract([]types.JSONPathExpression{pathExpr})
			if !exists {
				result.SetNull(i, true)
				continue
			}
			if obj.TypeCode != types.JSONTypeCodeObject && obj.TypeCode != types.JSONTypeCodeArray {
				resI64s[i] = 1
				continue
			}
			resI64s[i] = int64(obj.GetElemCount())
		}
	} else {
		for i := 0; i < nr; i++ {
			if result.IsNull(i) {
				continue
			}

			jsonItem := jsonBuf.GetJSON(i)
			if jsonItem.TypeCode != types.JSONTypeCodeObject && jsonItem.TypeCode != types.JSONTypeCodeArray {
				resI64s[i] = 1
				continue
			}
			resI64s[i] = int64(jsonItem.GetElemCount())
		}
	}

	return nil
}

func (b *builtinJSONTypeSig) vectorized() bool {
	return true
}

func (b *builtinJSONTypeSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalJSON(ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		result.AppendString(buf.GetJSON(i).Type())
	}
	return nil
}

func (b *builtinJSONExtractSig) vectorized() bool {
	return true
}

func (b *builtinJSONExtractSig) vecEvalJSON(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	var err error

	nr := input.NumRows()
	jsonBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(jsonBuf)
	if err = b.args[0].VecEvalJSON(ctx, input, jsonBuf); err != nil {
		return err
	}

	pathArgs := b.args[1:]
	pathBuffers := make([]*chunk.Column, len(pathArgs))
	for k := 0; k < len(pathArgs); k++ {
		if pathBuffers[k], err = b.bufAllocator.get(); err != nil {
			return err
		}
		defer func(buf *chunk.Column) {
			b.bufAllocator.put(buf)
		}(pathBuffers[k])

		if err := pathArgs[k].VecEvalString(ctx, input, pathBuffers[k]); err != nil {
			return err
		}
	}

	result.ReserveJSON(nr)
	for i := 0; i < nr; i++ {
		if jsonBuf.IsNull(i) {
			result.AppendNull()
			continue
		}
		jsonItem := jsonBuf.GetJSON(i)

		pathExprs := make([]types.JSONPathExpression, len(pathBuffers))
		hasNullPath := false
		for k, pathBuf := range pathBuffers {
			if pathBuf.IsNull(i) {
				hasNullPath = true
				break
			}
			if pathExprs[k], err = types.ParseJSONPathExpr(pathBuf.GetString(i)); err != nil {
				return err
			}
		}
		if hasNullPath {
			result.AppendNull()
			continue
		}

		var found bool
		if jsonItem, found = jsonItem.Extract(pathExprs); !found {
			result.AppendNull()
			continue
		}
		result.AppendJSON(jsonItem)
	}

	return nil
}

func (b *builtinJSONRemoveSig) vectorized() bool {
	return true
}

func (b *builtinJSONRemoveSig) vecEvalJSON(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	nr := input.NumRows()
	jsonBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(jsonBuf)
	if err := b.args[0].VecEvalJSON(ctx, input, jsonBuf); err != nil {
		return err
	}

	strBufs := make([]*chunk.Column, len(b.args)-1)
	for i := 1; i < len(b.args); i++ {
		strBufs[i-1], err = b.bufAllocator.get()
		if err != nil {
			return err
		}
		defer b.bufAllocator.put(strBufs[i-1])
		if err := b.args[i].VecEvalString(ctx, input, strBufs[i-1]); err != nil {
			return err
		}
	}

	result.ReserveJSON(nr)
	for i := 0; i < nr; i++ {
		if jsonBuf.IsNull(i) {
			result.AppendNull()
			continue
		}

		pathExprs := make([]types.JSONPathExpression, 0, len(b.args)-1)
		var pathExpr types.JSONPathExpression
		isNull := false

		for j := 1; j < len(b.args); j++ {
			if strBufs[j-1].IsNull(i) {
				isNull = true
				break
			}
			pathExpr, err = types.ParseJSONPathExpr(strBufs[j-1].GetString(i))
			if err != nil {
				return err
			}
			pathExprs = append(pathExprs, pathExpr)
		}

		if isNull {
			result.AppendNull()
		} else {
			res, err := jsonBuf.GetJSON(i).Remove(pathExprs)
			if err != nil {
				return err
			}
			result.AppendJSON(res)
		}
	}
	return nil
}

func (b *builtinJSONMergeSig) vectorized() bool {
	return true
}

func (b *builtinJSONMergeSig) vecEvalJSON(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	nr := input.NumRows()
	argBuffers := make([]*chunk.Column, len(b.args))
	var err error
	for i, arg := range b.args {
		if argBuffers[i], err = b.bufAllocator.get(); err != nil {
			return err
		}
		defer func(buf *chunk.Column) {
			b.bufAllocator.put(buf)
		}(argBuffers[i])

		if err := arg.VecEvalJSON(ctx, input, argBuffers[i]); err != nil {
			return err
		}
	}

	jsonValues := make([][]types.BinaryJSON, nr)

	for i := 0; i < nr; i++ {
		isNullFlag := false
		for j := 0; j < len(b.args); j++ {
			isNullFlag = isNullFlag || argBuffers[j].IsNull(i)
		}
		if isNullFlag {
			jsonValues[i] = nil
		} else {
			jsonValues[i] = make([]types.BinaryJSON, 0, len(b.args))
		}
	}
	for i := 0; i < len(b.args); i++ {
		for j := 0; j < nr; j++ {
			if jsonValues[j] == nil {
				continue
			}
			jsonValues[j] = append(jsonValues[j], argBuffers[i].GetJSON(j))
		}
	}

	result.ReserveJSON(nr)
	for i := 0; i < nr; i++ {
		if jsonValues[i] == nil {
			result.AppendNull()
			continue
		}
		result.AppendJSON(types.MergeBinaryJSON(jsonValues[i]))
	}

	if b.pbCode == tipb.ScalarFuncSig_JsonMergeSig {
		for i := 0; i < nr; i++ {
			if result.IsNull(i) {
				continue
			}
			tc := typeCtx(ctx)
			tc.AppendWarning(errDeprecatedSyntaxNoReplacement.FastGenByArgs("JSON_MERGE", ""))
		}
	}

	return nil
}

func (b *builtinJSONContainsPathSig) vectorized() bool {
	return true
}

func (b *builtinJSONContainsPathSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	jsonBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(jsonBuf)
	if err := b.args[0].VecEvalJSON(ctx, input, jsonBuf); err != nil {
		return err
	}
	typeBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(typeBuf)
	if err := b.args[1].VecEvalString(ctx, input, typeBuf); err != nil {
		return err
	}
	pathBufs := make([]*chunk.Column, len(b.args)-2)
	defer func() {
		for i := 0; i < len(pathBufs); i++ {
			if pathBufs[i] != nil {
				b.bufAllocator.put(pathBufs[i])
			}
		}
	}()
	for i := 0; i < len(pathBufs); i++ {
		pathBuf, err := b.bufAllocator.get()
		if err != nil {
			return err
		}
		pathBufs[i] = pathBuf
		if err := b.args[2+i].VecEvalString(ctx, input, pathBuf); err != nil {
			return err
		}
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(jsonBuf, typeBuf)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		containType := strings.ToLower(typeBuf.GetString(i))
		if containType != types.JSONContainsPathAll && containType != types.JSONContainsPathOne {
			return types.ErrInvalidJSONContainsPathType
		}
		obj := jsonBuf.GetJSON(i)
		contains := int64(1)
		var pathExpr types.JSONPathExpression
		for j := 0; j < len(pathBufs); j++ {
			if pathBufs[j].IsNull(i) {
				result.SetNull(i, true)
				contains = -1
				break
			}
			path := pathBufs[j].GetString(i)
			if pathExpr, err = types.ParseJSONPathExpr(path); err != nil {
				return err
			}
			_, exists := obj.Extract([]types.JSONPathExpression{pathExpr})
			if exists && containType == types.JSONContainsPathOne {
				contains = 1
				break
			} else if !exists && containType == types.JSONContainsPathOne {
				contains = 0
			} else if !exists && containType == types.JSONContainsPathAll {
				contains = 0
				break
			}
		}
		if contains >= 0 {
			i64s[i] = contains
		}
	}
	return nil
}

func (b *builtinJSONArrayAppendSig) vectorized() bool {
	return true
}

func (b *builtinJSONArrayAppendSig) vecEvalJSON(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	m := (len(b.args) - 1) / 2

	jsonBufs, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(jsonBufs)
	if err := b.args[0].VecEvalJSON(ctx, input, jsonBufs); err != nil {
		return err
	}

	pathBufs := make([]*chunk.Column, 0, m)
	valBufs := make([]*chunk.Column, 0, m)
	defer func() {
		for _, buf := range pathBufs {
			b.bufAllocator.put(buf)
		}
		for _, buf := range valBufs {
			b.bufAllocator.put(buf)
		}
	}()
	for i := 1; i < len(b.args)-1; i += 2 {
		pathBuf, err := b.bufAllocator.get()
		if err != nil {
			return err
		}
		pathBufs = append(pathBufs, pathBuf)
		if err := b.args[i].VecEvalString(ctx, input, pathBuf); err != nil {
			return err
		}
		valBuf, err := b.bufAllocator.get()
		if err != nil {
			return err
		}
		if err := b.args[i+1].VecEvalJSON(ctx, input, valBuf); err != nil {
			return err
		}
		valBufs = append(valBufs, valBuf)
	}

	result.ReserveJSON(n)
	for i := 0; i < n; i++ {
		if jsonBufs.IsNull(i) {
			result.AppendNull()
			continue
		}
		res := jsonBufs.GetJSON(i)
		isNull := false
		for j := 0; j < m; j++ {
			if pathBufs[j].IsNull(i) {
				isNull = true
				break
			}
			s := pathBufs[j].GetString(i)
			v, vNull := types.BinaryJSON{}, valBufs[j].IsNull(i)
			if !vNull {
				v = valBufs[j].GetJSON(i)
			}
			if vNull {
				v = types.CreateBinaryJSON(nil)
			}
			res, isNull, err = b.appendJSONArray(res, s, v)
			if err != nil {
				return err
			}
			if isNull {
				break
			}
		}
		if isNull {
			result.AppendNull()
		} else {
			result.AppendJSON(res)
		}
	}
	return nil
}

func (b *builtinJSONUnquoteSig) vectorized() bool {
	return true
}

func (b *builtinJSONUnquoteSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		str := buf.GetString(i)
		if len(str) >= 2 && str[0] == '"' && str[len(str)-1] == '"' && !goJSON.Valid([]byte(str)) {
			return types.ErrInvalidJSONText.GenWithStackByArgs("The document root must not be followed by other values.")
		}
		str, err := types.UnquoteString(str)
		if err != nil {
			return err
		}
		result.AppendString(str)
	}
	return nil
}

func (b *builtinJSONSPrettySig) vectorized() bool {
	return true
}

func (b *builtinJSONSPrettySig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalJSON(ctx, input, buf); err != nil {
		return err
	}
	result.ReserveString(n)
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}

		jb, err := buf.GetJSON(i).MarshalJSON()
		if err != nil {
			return err
		}

		var resBuf bytes.Buffer
		if err = goJSON.Indent(&resBuf, jb, "", "  "); err != nil {
			return err
		}

		result.AppendString(resBuf.String())
	}
	return nil
}

func (b *builtinJSONMergePatchSig) vectorized() bool {
	return true
}

func (b *builtinJSONMergePatchSig) vecEvalJSON(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	nr := input.NumRows()
	argBuffers := make([]*chunk.Column, len(b.args))
	var err error
	for i, arg := range b.args {
		if argBuffers[i], err = b.bufAllocator.get(); err != nil {
			return err
		}
		defer func(buf *chunk.Column) {
			b.bufAllocator.put(buf)
		}(argBuffers[i])

		if err := arg.VecEvalJSON(ctx, input, argBuffers[i]); err != nil {
			return err
		}
	}

	result.ReserveJSON(nr)
	jsonValue := make([]*types.BinaryJSON, 0, len(b.args))
	for i := 0; i < nr; i++ {
		jsonValue = jsonValue[:0]
		for j := 0; j < len(b.args); j++ {
			if argBuffers[j].IsNull(i) {
				jsonValue = append(jsonValue, nil)
			} else {
				v := argBuffers[j].GetJSON(i)
				jsonValue = append(jsonValue, &v)
			}
		}

		tmpJSON, e := types.MergePatchBinaryJSON(jsonValue)
		if e != nil {
			return e
		}
		if tmpJSON == nil {
			result.AppendNull()
		} else {
			result.AppendJSON(*tmpJSON)
		}
	}

	return nil
}
