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
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

func vecJSONModify(ctx sessionctx.Context, args []Expression, bufAllocator columnBufferAllocator, input *chunk.Chunk, result *chunk.Column, mt json.ModifyType) error {
	nr := input.NumRows()
	jsonBuf, err := bufAllocator.get(types.ETJson, nr)
	if err != nil {
		return err
	}
	defer bufAllocator.put(jsonBuf)
	if err := args[0].VecEvalJSON(ctx, input, jsonBuf); err != nil {
		return err
	}

	strBufs := make([]*chunk.Column, (len(args)-1)/2)
	for i := 1; i < len(args); i += 2 {
		strBufs[(i-1)/2], err = bufAllocator.get(types.ETString, nr)
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
		valueBufs[i/2-1], err = bufAllocator.get(types.ETJson, nr)
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
		pathExprs := make([]json.PathExpression, 0, (len(args)-1)/2+1)
		values := make([]json.BinaryJSON, 0, (len(args)-1)/2+1)
		var pathExpr json.PathExpression
		isNull := false
		for j := 1; j < len(args); j += 2 {
			if strBufs[(j-1)/2].IsNull(i) {
				isNull = true
				break
			}
			pathExpr, err = json.ParseJSONPathExpr(strBufs[(j-1)/2].GetString(i))
			if err != nil {
				return err
			}
			pathExprs = append(pathExprs, pathExpr)
		}
		for j := 2; j < len(args); j += 2 {
			if valueBufs[j/2-1].IsNull(i) {
				values = append(values, json.CreateBinary(nil))
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

func (b *builtinJSONStorageSizeSig) vectorized() bool {
	return true
}

func (b *builtinJSONStorageSizeSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETJson, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalJSON(b.ctx, input, buf); err != nil {
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

		jb, err := j.MarshalJSON()
		if err != nil {
			continue
		}

		int64s[i] = int64(len(jb))
	}
	return nil
}

func (b *builtinJSONDepthSig) vectorized() bool {
	return true
}

func (b *builtinJSONDepthSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETJson, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalJSON(b.ctx, input, buf); err != nil {
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

func (b *builtinJSONKeysSig) vecEvalJSON(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETJson, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalJSON(b.ctx, input, buf); err != nil {
		return err
	}

	result.ReserveJSON(n)
	var j json.BinaryJSON
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}

		j = buf.GetJSON(i)
		if j.TypeCode != json.TypeCodeObject {
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

func (b *builtinJSONInsertSig) vecEvalJSON(input *chunk.Chunk, result *chunk.Column) error {
	err := vecJSONModify(b.ctx, b.args, b.bufAllocator, input, result, json.ModifyInsert)
	return err
}

func (b *builtinJSONReplaceSig) vectorized() bool {
	return true
}

func (b *builtinJSONReplaceSig) vecEvalJSON(input *chunk.Chunk, result *chunk.Column) error {
	err := vecJSONModify(b.ctx, b.args, b.bufAllocator, input, result, json.ModifyReplace)
	return err
}

func (b *builtinJSONArraySig) vectorized() bool {
	return true
}

func (b *builtinJSONArraySig) vecEvalJSON(input *chunk.Chunk, result *chunk.Column) error {
	nr := input.NumRows()
	jsons := make([][]interface{}, nr)
	for i := 0; i < nr; i++ {
		jsons[i] = make([]interface{}, 0, len(b.args))
	}
	for _, arg := range b.args {
		j, err := b.bufAllocator.get(types.ETJson, nr)
		if err != nil {
			return err
		}
		defer b.bufAllocator.put(j)
		if err = arg.VecEvalJSON(b.ctx, input, j); err != nil {
			return err
		}
		for i := 0; i < nr; i++ {
			if j.IsNull(i) {
				jsons[i] = append(jsons[i], json.CreateBinary(nil))
			} else {
				jsons[i] = append(jsons[i], j.GetJSON(i))
			}
		}
	}
	result.ReserveJSON(nr)
	for i := 0; i < nr; i++ {
		result.AppendJSON(json.CreateBinary(jsons[i]))
	}
	return nil
}

func (b *builtinJSONContainsSig) vectorized() bool {
	return true
}

func (b *builtinJSONContainsSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	nr := input.NumRows()

	objCol, err := b.bufAllocator.get(types.ETJson, nr)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(objCol)

	if err := b.args[0].VecEvalJSON(b.ctx, input, objCol); err != nil {
		return err
	}

	targetCol, err := b.bufAllocator.get(types.ETJson, nr)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(targetCol)

	if err := b.args[1].VecEvalJSON(b.ctx, input, targetCol); err != nil {
		return err
	}

	result.ResizeInt64(nr, false)
	resI64s := result.Int64s()

	if len(b.args) == 3 {
		pathCol, err := b.bufAllocator.get(types.ETString, nr)
		if err != nil {
			return err
		}
		defer b.bufAllocator.put(pathCol)

		if err := b.args[2].VecEvalString(b.ctx, input, pathCol); err != nil {
			return err
		}

		result.MergeNulls(objCol, targetCol, pathCol)

		var pathExpr json.PathExpression
		for i := 0; i < nr; i++ {
			if result.IsNull(i) {
				continue
			}
			pathExpr, err = json.ParseJSONPathExpr(pathCol.GetString(i))
			if err != nil {
				return err
			}
			if pathExpr.ContainsAnyAsterisk() {
				return json.ErrInvalidJSONPathWildcard
			}

			obj, exists := objCol.GetJSON(i).Extract([]json.PathExpression{pathExpr})
			if !exists {
				result.SetNull(i, true)
				continue
			}

			if json.ContainsBinary(obj, targetCol.GetJSON(i)) {
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
			if json.ContainsBinary(objCol.GetJSON(i), targetCol.GetJSON(i)) {
				resI64s[i] = 1
			} else {
				resI64s[i] = 0
			}
		}
	}

	return nil
}

func (b *builtinJSONQuoteSig) vectorized() bool {
	return true
}

func (b *builtinJSONQuoteSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
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

func (b *builtinJSONSearchSig) vecEvalJSON(input *chunk.Chunk, result *chunk.Column) error {
	nr := input.NumRows()
	jsonBuf, err := b.bufAllocator.get(types.ETJson, nr)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(jsonBuf)
	if err := b.args[0].VecEvalJSON(b.ctx, input, jsonBuf); err != nil {
		return err
	}
	typeBuf, err := b.bufAllocator.get(types.ETString, nr)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(typeBuf)
	if err := b.args[1].VecEvalString(b.ctx, input, typeBuf); err != nil {
		return err
	}
	searchBuf, err := b.bufAllocator.get(types.ETString, nr)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(searchBuf)
	if err := b.args[2].VecEvalString(b.ctx, input, searchBuf); err != nil {
		return err
	}

	result.ReserveJSON(nr)

	if len(b.args) >= 4 {
		escapeBuf, err := b.bufAllocator.get(types.ETString, nr)
		if err != nil {
			return err
		}
		defer b.bufAllocator.put(escapeBuf)
		if err := b.args[3].VecEvalString(b.ctx, input, escapeBuf); err != nil {
			return nil
		}
		if len(b.args) >= 5 {
			pathBufs := make([]*chunk.Column, (len(b.args) - 4))
			for i := 4; i < len(b.args); i++ {
				index := i - 4
				pathBufs[index], err = b.bufAllocator.get(types.ETString, nr)
				if err != nil {
					return err
				}
				defer b.bufAllocator.put(pathBufs[index])
				if err := b.args[i].VecEvalString(b.ctx, input, pathBufs[index]); err != nil {
					return err
				}
			}
			for i := 0; i < nr; i++ {
				if jsonBuf.IsNull(i) {
					result.AppendNull()
					continue
				}
				containType := strings.ToLower(typeBuf.GetString(i))
				escape := byte('\\')
				if !escapeBuf.IsNull(i) {
					escapeStr := escapeBuf.GetString(i)
					if len(escapeStr) == 0 {
						escape = byte('\\')
					} else if len(escapeStr) == 1 {
						escape = byte(escapeStr[0])
					} else {
						return errIncorrectArgs.GenWithStackByArgs("ESCAPE")
					}
				}
				pathExprs := make([]json.PathExpression, 0, len(b.args)-4)
				for j := 0; j < len(b.args)-4; j++ {
					if pathBufs[j].IsNull(i) {
						break
					}
					pathExpr, err := json.ParseJSONPathExpr(pathBufs[j].GetString(i))
					if err != nil {
						return json.ErrInvalidJSONPath.GenWithStackByArgs(pathBufs[j].GetString(i))
					}
					pathExprs = append(pathExprs, pathExpr)
				}
				bj, err := jsonBuf.GetJSON(i).Search(containType, searchBuf.GetString(i), escape, pathExprs)
				if err != nil {
					return err
				}
				result.AppendJSON(bj)
			}
		} else {
			for i := 0; i < nr; i++ {
				if jsonBuf.IsNull(i) {
					result.AppendNull()
					continue
				}
				containType := strings.ToLower(typeBuf.GetString(i))
				escape := byte('\\')
				isNull := escapeBuf.IsNull(i)
				if !isNull {
					escapeStr := escapeBuf.GetString(i)
					if len(escapeStr) == 0 {
						escape = byte('\\')
					} else if len(escapeStr) == 1 {
						escape = byte(escapeStr[0])
					} else {
						return errIncorrectArgs.GenWithStackByArgs("ESCAPE")
					}
				}
				bj, err := jsonBuf.GetJSON(i).Search(containType, searchBuf.GetString(i), escape, nil)
				if err != nil {
					return err
				}
				result.AppendJSON(bj)
			}
		}
	} else {
		for i := 0; i < nr; i++ {
			if jsonBuf.IsNull(i) {
				result.AppendNull()
				continue
			}
			containType := strings.ToLower(typeBuf.GetString(i))
			escape := byte('\\')
			resultItem, err := jsonBuf.GetJSON(i).Search(containType, searchBuf.GetString(i), escape, nil)
			if err != nil {
				return err
			}
			result.AppendJSON(resultItem)
		}
	}
	return nil
}

func (b *builtinJSONSetSig) vectorized() bool {
	return true
}

func (b *builtinJSONSetSig) vecEvalJSON(input *chunk.Chunk, result *chunk.Column) error {
	err := vecJSONModify(b.ctx, b.args, b.bufAllocator, input, result, json.ModifySet)
	return err
}

func (b *builtinJSONObjectSig) vectorized() bool {
	return true
}

func (b *builtinJSONObjectSig) vecEvalJSON(input *chunk.Chunk, result *chunk.Column) error {
	nr := input.NumRows()
	if len(b.args)&1 == 1 {
		err := ErrIncorrectParameterCount.GenWithStackByArgs(ast.JSONObject)
		return err
	}

	jsons := make([]map[string]interface{}, nr)
	for i := 0; i < nr; i++ {
		jsons[i] = make(map[string]interface{}, len(b.args)>>1)
	}

	argBuffers := make([]*chunk.Column, len(b.args))
	var err error
	for i := 0; i < len(b.args); i++ {
		if i&1 == 0 {
			if argBuffers[i], err = b.bufAllocator.get(types.ETString, nr); err != nil {
				return nil
			}
			defer func(buf *chunk.Column) {
				b.bufAllocator.put(buf)
			}(argBuffers[i])

			if err = b.args[i].VecEvalString(b.ctx, input, argBuffers[i]); err != nil {
				return err
			}
		} else {
			if argBuffers[i], err = b.bufAllocator.get(types.ETJson, nr); err != nil {
				return err
			}
			defer func(buf *chunk.Column) {
				b.bufAllocator.put(buf)
			}(argBuffers[i])

			if err = b.args[i].VecEvalJSON(b.ctx, input, argBuffers[i]); err != nil {
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
			var value json.BinaryJSON
			for j := 0; j < nr; j++ {
				if keyCol.IsNull(j) {
					err := errors.New("JSON documents may not contain NULL member names")
					return err
				}
				key = keyCol.GetString(j)
				if valueCol.IsNull(j) {
					value = json.CreateBinary(nil)
				} else {
					value = valueCol.GetJSON(j)
				}
				jsons[j][key] = value
			}
		}
	}

	for i := 0; i < nr; i++ {
		result.AppendJSON(json.CreateBinary(jsons[i]))
	}
	return nil
}

func (b *builtinJSONArrayInsertSig) vectorized() bool {
	return true
}

func (b *builtinJSONArrayInsertSig) vecEvalJSON(input *chunk.Chunk, result *chunk.Column) error {
	nr := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETJson, nr)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalJSON(b.ctx, input, buf); err != nil {
		return err
	}
	pathBufs := make([]*chunk.Column, (len(b.args)-1)/2)
	valueBufs := make([]*chunk.Column, (len(b.args)-1)/2)
	for i := 1; i < len(b.args); i++ {
		if i&1 == 0 {
			valueBufs[i/2-1], err = b.bufAllocator.get(types.ETJson, nr)
			if err != nil {
				return err
			}
			defer b.bufAllocator.put(valueBufs[i/2-1])
			if err := b.args[i].VecEvalJSON(b.ctx, input, valueBufs[i/2-1]); err != nil {
				return err
			}
		} else {
			pathBufs[(i-1)/2], err = b.bufAllocator.get(types.ETString, nr)
			if err != nil {
				return err
			}
			defer b.bufAllocator.put(pathBufs[(i-1)/2])
			if err := b.args[i].VecEvalString(b.ctx, input, pathBufs[(i-1)/2]); err != nil {
				return err
			}
		}
	}
	var pathExpr json.PathExpression
	var value json.BinaryJSON
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
			pathExpr, err = json.ParseJSONPathExpr(pathBufs[j].GetString(i))
			if err != nil {
				return json.ErrInvalidJSONPath.GenWithStackByArgs(pathBufs[j].GetString(i))
			}
			if pathExpr.ContainsAnyAsterisk() {
				return json.ErrInvalidJSONPathWildcard.GenWithStackByArgs(pathBufs[j].GetString(i))
			}
			if valueBufs[j].IsNull(i) {
				value = json.CreateBinary(nil)
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

func (b *builtinJSONKeys2ArgsSig) vecEvalJSON(input *chunk.Chunk, result *chunk.Column) error {
	nr := input.NumRows()
	jsonBuf, err := b.bufAllocator.get(types.ETJson, nr)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(jsonBuf)
	if err := b.args[0].VecEvalJSON(b.ctx, input, jsonBuf); err != nil {
		return err
	}
	pathBuf, err := b.bufAllocator.get(types.ETString, nr)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(pathBuf)
	if err := b.args[1].VecEvalString(b.ctx, input, pathBuf); err != nil {
		return err
	}

	result.ReserveJSON(nr)
	for i := 0; i < nr; i++ {
		if jsonBuf.IsNull(i) || pathBuf.IsNull(i) {
			result.AppendNull()
			continue
		}

		pathExpr, err := json.ParseJSONPathExpr(pathBuf.GetString(i))
		if err != nil {
			return err
		}
		if pathExpr.ContainsAnyAsterisk() {
			return json.ErrInvalidJSONPathWildcard
		}

		jsonItem := jsonBuf.GetJSON(i)
		if jsonItem.TypeCode != json.TypeCodeObject {
			result.AppendNull()
			continue
		}

		res, exists := jsonItem.Extract([]json.PathExpression{pathExpr})
		if !exists || res.TypeCode != json.TypeCodeObject {
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

func (b *builtinJSONLengthSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	nr := input.NumRows()

	jsonBuf, err := b.bufAllocator.get(types.ETJson, nr)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(jsonBuf)
	if err := b.args[0].VecEvalJSON(b.ctx, input, jsonBuf); err != nil {
		return err
	}
	result.ResizeInt64(nr, false)
	resI64s := result.Int64s()

	if len(b.args) == 2 {
		pathBuf, err := b.bufAllocator.get(types.ETString, nr)
		if err != nil {
			return err
		}
		defer b.bufAllocator.put(pathBuf)
		if err := b.args[1].VecEvalString(b.ctx, input, pathBuf); err != nil {
			return err
		}

		result.MergeNulls(jsonBuf)
		for i := 0; i < nr; i++ {
			if result.IsNull(i) {
				continue
			}
			jsonItem := jsonBuf.GetJSON(i)

			if jsonItem.TypeCode != json.TypeCodeObject && jsonItem.TypeCode != json.TypeCodeArray {
				resI64s[i] = 1
				continue
			}

			if pathBuf.IsNull(i) {
				result.SetNull(i, true)
				continue
			}

			pathExpr, err := json.ParseJSONPathExpr(pathBuf.GetString(i))
			if err != nil {
				return err
			}
			if pathExpr.ContainsAnyAsterisk() {
				return json.ErrInvalidJSONPathWildcard
			}

			obj, exists := jsonItem.Extract([]json.PathExpression{pathExpr})
			if !exists {
				result.SetNull(i, true)
				continue
			}
			if obj.TypeCode != json.TypeCodeObject && obj.TypeCode != json.TypeCodeArray {
				resI64s[i] = 1
				continue
			}
			resI64s[i] = int64(obj.GetElemCount())
		}
	} else {
		result.MergeNulls(jsonBuf)
		for i := 0; i < nr; i++ {
			if result.IsNull(i) {
				continue
			}

			jsonItem := jsonBuf.GetJSON(i)
			if jsonItem.TypeCode != json.TypeCodeObject && jsonItem.TypeCode != json.TypeCodeArray {
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

func (b *builtinJSONTypeSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETJson, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalJSON(b.ctx, input, buf); err != nil {
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

func (b *builtinJSONExtractSig) vecEvalJSON(input *chunk.Chunk, result *chunk.Column) error {
	var err error

	nr := input.NumRows()
	jsonBuf, err := b.bufAllocator.get(types.ETJson, nr)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(jsonBuf)
	if err = b.args[0].VecEvalJSON(b.ctx, input, jsonBuf); err != nil {
		return err
	}

	pathArgs := b.args[1:]
	pathBuffers := make([]*chunk.Column, len(pathArgs))
	for k := 0; k < len(pathArgs); k++ {
		if pathBuffers[k], err = b.bufAllocator.get(types.ETString, nr); err != nil {
			return err
		}
		defer func(buf *chunk.Column) {
			b.bufAllocator.put(buf)
		}(pathBuffers[k])

		if err := pathArgs[k].VecEvalString(b.ctx, input, pathBuffers[k]); err != nil {
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

		pathExprs := make([]json.PathExpression, len(pathBuffers))
		hasNullPath := false
		for k, pathBuf := range pathBuffers {
			if pathBuf.IsNull(i) {
				hasNullPath = true
				break
			}
			if pathExprs[k], err = json.ParseJSONPathExpr(pathBuf.GetString(i)); err != nil {
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

func (b *builtinJSONRemoveSig) vecEvalJSON(input *chunk.Chunk, result *chunk.Column) error {
	nr := input.NumRows()
	jsonBuf, err := b.bufAllocator.get(types.ETJson, nr)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(jsonBuf)
	if err := b.args[0].VecEvalJSON(b.ctx, input, jsonBuf); err != nil {
		return err
	}

	strBufs := make([]*chunk.Column, len(b.args)-1)
	for i := 1; i < len(b.args); i++ {
		strBufs[i-1], err = b.bufAllocator.get(types.ETString, nr)
		if err != nil {
			return err
		}
		defer b.bufAllocator.put(strBufs[i-1])
		if err := b.args[i].VecEvalString(b.ctx, input, strBufs[i-1]); err != nil {
			return err
		}
	}

	result.ReserveJSON(nr)
	for i := 0; i < nr; i++ {
		if jsonBuf.IsNull(i) {
			result.AppendNull()
			continue
		}

		pathExprs := make([]json.PathExpression, 0, len(b.args)-1)
		var pathExpr json.PathExpression
		isNull := false

		for j := 1; j < len(b.args); j++ {
			if strBufs[j-1].IsNull(i) {
				isNull = true
				break
			}
			pathExpr, err = json.ParseJSONPathExpr(strBufs[j-1].GetString(i))
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

func (b *builtinJSONMergeSig) vecEvalJSON(input *chunk.Chunk, result *chunk.Column) error {
	nr := input.NumRows()
	argBuffers := make([]*chunk.Column, len(b.args))
	var err error
	for i, arg := range b.args {
		if argBuffers[i], err = b.bufAllocator.get(types.ETJson, nr); err != nil {
			return err
		}
		defer func(buf *chunk.Column) {
			b.bufAllocator.put(buf)
		}(argBuffers[i])

		if err := arg.VecEvalJSON(b.ctx, input, argBuffers[i]); err != nil {
			return err
		}
	}

	jsonValues := make([][]json.BinaryJSON, nr)

	for i := 0; i < nr; i++ {
		isNullFlag := false
		for j := 0; j < len(b.args); j++ {
			isNullFlag = isNullFlag || argBuffers[j].IsNull(i)
		}
		if isNullFlag {
			jsonValues[i] = nil
		} else {
			jsonValues[i] = make([]json.BinaryJSON, 0, len(b.args))
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
		result.AppendJSON(json.MergeBinary(jsonValues[i]))
	}

	if b.pbCode == tipb.ScalarFuncSig_JsonMergeSig {
		for i := 0; i < nr; i++ {
			if result.IsNull(i) {
				continue
			}
			b.ctx.GetSessionVars().StmtCtx.AppendWarning(errDeprecatedSyntaxNoReplacement.GenWithStackByArgs("JSON_MERGE"))
		}
	}

	return nil
}

func (b *builtinJSONContainsPathSig) vectorized() bool {
	return true
}

func (b *builtinJSONContainsPathSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	jsonBuf, err := b.bufAllocator.get(types.ETJson, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(jsonBuf)
	if err := b.args[0].VecEvalJSON(b.ctx, input, jsonBuf); err != nil {
		return err
	}
	typeBuf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(typeBuf)
	if err := b.args[1].VecEvalString(b.ctx, input, typeBuf); err != nil {
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
		pathBuf, err := b.bufAllocator.get(types.ETString, n)
		if err != nil {
			return err
		}
		pathBufs[i] = pathBuf
		if err := b.args[2+i].VecEvalString(b.ctx, input, pathBuf); err != nil {
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
		if containType != json.ContainsPathAll && containType != json.ContainsPathOne {
			return json.ErrInvalidJSONContainsPathType
		}
		obj := jsonBuf.GetJSON(i)
		contains := int64(1)
		var pathExpr json.PathExpression
		for j := 0; j < len(pathBufs); j++ {
			if pathBufs[j].IsNull(i) {
				result.SetNull(i, true)
				contains = -1
				break
			}
			path := pathBufs[j].GetString(i)
			if pathExpr, err = json.ParseJSONPathExpr(path); err != nil {
				return err
			}
			_, exists := obj.Extract([]json.PathExpression{pathExpr})
			if exists && containType == json.ContainsPathOne {
				contains = 1
				break
			} else if !exists && containType == json.ContainsPathOne {
				contains = 0
			} else if !exists && containType == json.ContainsPathAll {
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
	return false
}

func (b *builtinJSONArrayAppendSig) vecEvalJSON(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinJSONUnquoteSig) vectorized() bool {
	return true
}

func (b *builtinJSONUnquoteSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		str, err := json.UnquoteString(buf.GetString(i))
		if err != nil {
			return err
		}
		result.AppendString(str)
	}
	return nil
}
