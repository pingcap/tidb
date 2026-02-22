// Copyright 2017 PingCAP, Inc.
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

	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

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

	for i := range nr {
		isNullFlag := false
		for j := range b.args {
			isNullFlag = isNullFlag || argBuffers[j].IsNull(i)
		}
		if isNullFlag {
			jsonValues[i] = nil
		} else {
			jsonValues[i] = make([]types.BinaryJSON, 0, len(b.args))
		}
	}
	for i := range b.args {
		for j := range nr {
			if jsonValues[j] == nil {
				continue
			}
			jsonValues[j] = append(jsonValues[j], argBuffers[i].GetJSON(j))
		}
	}

	result.ReserveJSON(nr)
	for i := range nr {
		if jsonValues[i] == nil {
			result.AppendNull()
			continue
		}
		result.AppendJSON(types.MergeBinaryJSON(jsonValues[i]))
	}

	if b.pbCode == tipb.ScalarFuncSig_JsonMergeSig {
		for i := range nr {
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
		for i := range pathBufs {
			if pathBufs[i] != nil {
				b.bufAllocator.put(pathBufs[i])
			}
		}
	}()
	for i := range pathBufs {
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
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		containType := strings.ToLower(typeBuf.GetString(i))
		if containType != types.JSONContainsPathAll && containType != types.JSONContainsPathOne {
			return types.ErrJSONBadOneOrAllArg.GenWithStackByArgs("json_contains_path")
		}
		obj := jsonBuf.GetJSON(i)
		contains := int64(1)
		var pathExpr types.JSONPathExpression
		for j := range pathBufs {
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
	for i := range n {
		if jsonBufs.IsNull(i) {
			result.AppendNull()
			continue
		}
		res := jsonBufs.GetJSON(i)
		isNull := false
		for j := range m {
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
	for i := range n {
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
	for i := range n {
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
	for i := range nr {
		jsonValue = jsonValue[:0]
		for j := range b.args {
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
