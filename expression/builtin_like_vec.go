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
	return b.builtinRegexpSharedSig.vecEvalInt(input, result, b)
}

func (b *builtinRegexpSig) vectorized() bool {
	return true
}

func (b *builtinRegexpSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return b.builtinRegexpSharedSig.vecEvalInt(input, result, b)
}

func (b *builtinRegexpSharedSig) isMemoizedRegexpInitialized() bool {
	return !(b.memoizedRegexp == nil && b.memoizedErr == nil)
}

func (b *builtinRegexpSharedSig) initMemoizedRegexp(patterns *chunk.Column, n int, rc regexpCompiler) {
	// Precondition: patterns is generated from a constant expression
	for i := 0; i < n; i++ {
		if patterns.IsNull(i) {
			continue
		}
		re, err := rc.compile(patterns.GetString(i))
		b.memoizedRegexp = re
		b.memoizedErr = err
		break
	}
	if !b.isMemoizedRegexpInitialized() {
		b.memoizedErr = errors.New("No valid regexp pattern found")
	}
	if b.memoizedErr != nil {
		b.memoizedRegexp = nil
	}
}

func (b *builtinRegexpSharedSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column, rc regexpCompiler) error {
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

	if b.args[1].ConstItem() && !b.isMemoizedRegexpInitialized() {
		b.initMemoizedRegexp(bufPat, n, rc)
	}
	getRegexp := func(pat string) (*regexp.Regexp, error) {
		if b.isMemoizedRegexpInitialized() {
			return b.memoizedRegexp, b.memoizedErr
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
