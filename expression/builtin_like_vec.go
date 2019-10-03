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
	"regexp"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

func (b *builtinLikeSig) vectorized() bool {
	return false
}

func (b *builtinLikeSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinRegexpBinarySig) vectorized() bool {
	return true
}

func (b *builtinRegexpBinarySig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return vecEvalRegexp(&b.baseBuiltinFunc, b, input, result)
}

func (b *builtinRegexpSig) vectorized() bool {
	return true
}

func (b *builtinRegexpSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return vecEvalRegexp(&b.baseBuiltinFunc, b, input, result)
}

func vecEvalRegexp(b *baseBuiltinFunc, rc regexpCompiler, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	bufExpr, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bufExpr)
	if err := b.args[0].VecEvalString(b.ctx, input, bufExpr); err != nil {
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

	pat2re := make(map[string]*regexp.Regexp)
	isMemoizable := b.args[1].ConstItem()
	if isMemoizable {
		// If args[1] is a constant item, then all the items in bufPat are
		// the same. So the pattern can be compiled once and memoized.
		for i := 0; i < n; i++ {
			if bufPat.IsNull(i) {
				continue
			}
			pat := bufPat.GetString(i)
			if _, ok := pat2re[pat]; ok {
				continue
			}
			re, err := rc.compile(pat)
			if err != nil {
				return err
			}
			pat2re[pat] = re
		}
	}
	getRegexp := func(pat string) (*regexp.Regexp, error) {
		if isMemoizable {
			return pat2re[pat], nil
		}
		return rc.compile(pat)
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(bufExpr, bufPat)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		re, err := getRegexp(bufPat.GetString(i))
		if err != nil {
			return err
		}
		i64s[i] = boolToInt64(re.MatchString(bufExpr.GetString(i)))
	}
	return nil
}
