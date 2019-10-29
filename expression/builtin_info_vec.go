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
	"github.com/pingcap/tidb/util/chunk"
)

func (b *builtinDatabaseSig) vectorized() bool {
	return false
}

func (b *builtinDatabaseSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinConnectionIDSig) vectorized() bool {
	return false
}

func (b *builtinConnectionIDSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinTiDBVersionSig) vectorized() bool {
	return false
}

func (b *builtinTiDBVersionSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinRowCountSig) vectorized() bool {
	return false
}

func (b *builtinRowCountSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
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
	return false
}

func (b *builtinTiDBIsDDLOwnerSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
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
	return false
}

func (b *builtinLastInsertIDWithIDSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinVersionSig) vectorized() bool {
	return false
}

func (b *builtinVersionSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinTiDBDecodeKeySig) vectorized() bool {
	return false
}

func (b *builtinTiDBDecodeKeySig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}
