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
	"fmt"
	"hash/crc32"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

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
	for i := range nr {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}

		res := buf.GetJSON(i)
		isnull := false
		for j := range (len(b.args) - 1) / 2 {
			if pathBufs[j].IsNull(i) {
				isnull = true
				break
			}
			pathExpr, err = types.ParseJSONPathExpr(pathBufs[j].GetString(i))
			if err != nil {
				return err
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
	for i := range nr {
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

		for i := range nr {
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
		for i := range nr {
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
	for i := range n {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		result.AppendString(buf.GetJSON(i).Type())
	}
	return nil
}

func (b *builtinJSONSumCRC32Sig) vectorized() bool {
	return true
}

func (b *builtinJSONSumCRC32Sig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	ft := b.tp.ArrayType()
	f := convertJSON2Tp(ft.EvalType())
	if f == nil {
		return ErrNotSupportedYet.GenWithStackByArgs(fmt.Sprintf("calculating sum of %s", ft.String()))
	}

	nr := input.NumRows()
	jsonBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}

	defer b.bufAllocator.put(jsonBuf)
	if err = b.args[0].VecEvalJSON(ctx, input, jsonBuf); err != nil {
		return err
	}

	result.ResizeInt64(nr, false)
	result.MergeNulls(jsonBuf)
	i64s := result.Int64s()

	for i := range nr {
		if result.IsNull(i) {
			continue
		}

		jsonItem := jsonBuf.GetJSON(i)
		if jsonItem.TypeCode != types.JSONTypeCodeArray {
			return ErrInvalidTypeForJSON.GenWithStackByArgs(1, "JSON_SUM_CRC32")
		}

		var sum int64
		for j := range jsonItem.GetElemCount() {
			item, err := f(fakeSctx, jsonItem.ArrayGetElem(j), ft)
			if err != nil {
				if ErrInvalidJSONForFuncIndex.Equal(err) {
					err = errors.Errorf("Invalid JSON value for CAST to type %s", ft.CompactStr())
				}
				return err
			}
			sum += int64(crc32.ChecksumIEEE(fmt.Appendf(nil, "%v", item)))
		}
		i64s[i] = sum
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
	for k := range pathArgs {
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
	for i := range nr {
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
	for i := range nr {
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
