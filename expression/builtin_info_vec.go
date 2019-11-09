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
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/printer"
)

func (b *builtinDatabaseSig) vectorized() bool {
	return true
}

// evalString evals a builtinDatabaseSig.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html
func (b *builtinDatabaseSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()

	currentDB := b.ctx.GetSessionVars().CurrentDB
	result.ReserveString(n)
	if currentDB == "" {
		for i := 0; i < n; i++ {
			result.AppendNull()
		}
	} else {
		for i := 0; i < n; i++ {
			result.AppendString(currentDB)
		}
	}
	return nil
}

func (b *builtinConnectionIDSig) vectorized() bool {
	return false
}

func (b *builtinConnectionIDSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinTiDBVersionSig) vectorized() bool {
	return true
}

func (b *builtinTiDBVersionSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ReserveString(n)
	info := printer.GetTiDBInfo()
	for i := 0; i < n; i++ {
		result.AppendString(info)
	}
	return nil
}

func (b *builtinRowCountSig) vectorized() bool {
	return true
}

// evalInt evals ROW_COUNT().
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_row-count
func (b *builtinRowCountSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	res := int64(b.ctx.GetSessionVars().StmtCtx.PrevAffectedRows)
	for i := 0; i < n; i++ {
		i64s[i] = res
	}
	return nil
}

func (b *builtinCurrentUserSig) vectorized() bool {
	return false
}

func (b *builtinCurrentUserSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinCurrentRoleSig) vectorized() bool {
	return false
}

func (b *builtinCurrentRoleSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinUserSig) vectorized() bool {
	return false
}

func (b *builtinUserSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinTiDBIsDDLOwnerSig) vectorized() bool {
	return true
}

func (b *builtinTiDBIsDDLOwnerSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	ddlOwnerChecker := b.ctx.DDLOwnerChecker()
	var res int64
	if ddlOwnerChecker.IsOwner() {
		res = 1
	}
	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		i64s[i] = res
	}
	return nil
}

func (b *builtinFoundRowsSig) vectorized() bool {
	return false
}

func (b *builtinFoundRowsSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinBenchmarkSig) vectorized() bool {
	return false
}

func (b *builtinBenchmarkSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinLastInsertIDSig) vectorized() bool {
	return true
}

func (b *builtinLastInsertIDSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	res := int64(b.ctx.GetSessionVars().StmtCtx.PrevLastInsertID)
	for i := 0; i < n; i++ {
		i64s[i] = res
	}
	return nil
}

func (b *builtinLastInsertIDWithIDSig) vectorized() bool {
	return true
}

func (b *builtinLastInsertIDWithIDSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalInt(b.ctx, input, result); err != nil {
		return err
	}
	i64s := result.Int64s()
	for i := len(i64s) - 1; i >= 0; i-- {
		if !result.IsNull(i) {
			b.ctx.GetSessionVars().SetLastInsertID(uint64(i64s[i]))
			break
		}
	}
	return nil
}

func (b *builtinVersionSig) vectorized() bool {
	return true
}

func (b *builtinVersionSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ReserveString(n)
	for i := 0; i < n; i++ {
		result.AppendString(mysql.ServerVersion)
	}
	return nil
}

func (b *builtinTiDBDecodeKeySig) vectorized() bool {
	return true
}

func (b *builtinTiDBDecodeKeySig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
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
		result.AppendString(decodeKey(b.ctx, buf.GetString(i)))
	}
	return nil
}
