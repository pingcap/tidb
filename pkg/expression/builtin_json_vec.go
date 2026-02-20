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
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
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
	for i := range nr {
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
	for i := range n {
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
	for i := range n {
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
	for i := range n {
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
	for i := range n {
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
	for i := range nr {
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
		for i := range nr {
			if j.IsNull(i) {
				jsons[i] = append(jsons[i], types.CreateBinaryJSON(nil))
			} else {
				jsons[i] = append(jsons[i], j.GetJSON(i))
			}
		}
	}
	result.ReserveJSON(nr)
	for i := range nr {
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
	for i := range nr {
		if result.IsNull(i) {
			continue
		}
		obj := objCol.GetJSON(i)
		target := targetCol.GetJSON(i)
		if obj.TypeCode != types.JSONTypeCodeArray {
			resI64s[i] = boolToInt64(types.CompareBinaryJSON(obj, target) == 0)
		} else {
			elemCount := obj.GetElemCount()
			for j := range elemCount {
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
		for i := range nr {
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
		for i := range nr {
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
	for i := range nr {
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
	for i := range n {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		buffer := &bytes.Buffer{}
		encoder := goJSON.NewEncoder(buffer)
		encoder.SetEscapeHTML(false)
		err = encoder.Encode(buf.GetString(i))
		if err != nil {
			return err
		}
		result.AppendString(string(bytes.TrimSuffix(buffer.Bytes(), []byte("\n"))))
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

	for i := range nr {
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

		var isNull bool
		var pathExprs []types.JSONPathExpression
		if pathBufs != nil {
			pathExprs = make([]types.JSONPathExpression, 0, len(b.args)-4)
			for j := range len(b.args) - 4 {
				if pathBufs[j].IsNull(i) {
					isNull = true
					break
				}
				pathExpr, err := types.ParseJSONPathExpr(pathBufs[j].GetString(i))
				if err != nil {
					return err
				}
				pathExprs = append(pathExprs, pathExpr)
			}
		}
		if isNull {
			result.AppendNull()
			continue
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
	for i := range nr {
		jsons[i] = make(map[string]any, len(b.args)>>1)
	}

	argBuffers := make([]*chunk.Column, len(b.args))
	var err error
	for i := range b.args {
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
	for i := range b.args {
		if i&1 == 1 {
			keyCol := argBuffers[i-1]
			valueCol := argBuffers[i]

			var key string
			var value types.BinaryJSON
			for j := range nr {
				if keyCol.IsNull(j) {
					return types.ErrJSONDocumentNULLKey
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

	for i := range nr {
		bj, err := types.CreateBinaryJSONWithCheck(jsons[i])
		if err != nil {
			return err
		}
		result.AppendJSON(bj)
	}
	return nil
}

