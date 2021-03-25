// Copyright 2021 PingCAP, Inc.
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

// +build hyperscan

package expression

import (
	"strings"

	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

func (b *baseBuiltinHsSig) vecUpdateSourceType(input *chunk.Chunk, n int) error {
	if len(b.args) == 3 {
		bufStp, err := b.bufAllocator.get(types.ETString, n)
		if err != nil {
			return err
		}
		defer b.bufAllocator.put(bufStp)

		if err := b.args[2].VecEvalString(b.ctx, input, bufStp); err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if bufStp.IsNull(i) {
				continue
			}
			switch strings.ToLower(bufStp.GetString(i)) {
			case "hex":
				if !b.supportDBSource {
					return errInvalidHSSourceTypeWithoutDBSource
				}
				b.sourceType = hsSourceTypeHex
			case "base64":
				if !b.supportDBSource {
					return errInvalidHSSourceTypeWithoutDBSource
				}
				b.sourceType = hsSourceTypeBase64
			case "json":
				b.sourceType = hsSourceTypeJSON
			case "lines":
				b.sourceType = hsSourceTypeLines
			default:
				return errInvalidHSSourceType
			}
			break
		}
	}
	return nil
}

func (b *baseBuiltinHsSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	bufVal, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bufVal)
	if err := b.args[0].VecEvalString(b.ctx, input, bufVal); err != nil {
		return err
	}

	bufPat, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bufPat)

	if err := b.args[1].VecEvalString(b.ctx, input, bufPat); err != nil {
		return err
	}

	err = b.vecUpdateSourceType(input, n)
	if err != nil {
		return err
	}

	if b.db == nil && n > 0 {
		for i := 0; i < n; i++ {
			if bufPat.IsNull(i) {
				continue
			}
			db, err := b.buildBlockDB(bufPat.GetString(i))
			if err != nil {
				return err
			}
			b.db = db
			break
		}
	}

	if err := b.initScratch(); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(bufVal, bufPat)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		matched := b.hsMatch(bufVal.GetString(i))
		i64s[i] = boolToInt64(matched)
	}
	return nil
}

func (b *baseBuiltinHsSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	bufVal, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bufVal)
	if err := b.args[0].VecEvalString(b.ctx, input, bufVal); err != nil {
		return err
	}

	bufPat, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bufPat)

	if err := b.args[1].VecEvalString(b.ctx, input, bufPat); err != nil {
		return err
	}

	err = b.vecUpdateSourceType(input, n)
	if err != nil {
		return err
	}

	if b.db == nil && n > 0 {
		for i := 0; i < n; i++ {
			if bufPat.IsNull(i) {
				continue
			}
			db, err := b.buildBlockDB(bufPat.GetString(i))
			if err != nil {
				return err
			}
			b.db = db
			break
		}
	}

	if err := b.initScratch(); err != nil {
		return err
	}

	result.ReserveString(n)
	for i := 0; i < n; i++ {
		if bufVal.IsNull(i) {
			result.AppendNull()
			continue
		}
		ids := b.hsMatchIds(bufVal.GetString(i))
		mids := ""
		if len(ids) > 0 {
			mids = strings.Join(ids, ",")
		}
		result.AppendString(mids)
	}
	return nil
}

func (b *builtinHsMatchSig) vectorized() bool {
	return true
}
