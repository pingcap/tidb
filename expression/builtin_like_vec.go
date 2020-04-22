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
	return true
}

func (b *builtinLikeSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	bufVal, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bufVal)
	if err = b.args[0].VecEvalString(b.ctx, input, bufVal); err != nil {
		return err
	}
	bufPattern, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bufPattern)
	if err = b.args[1].VecEvalString(b.ctx, input, bufPattern); err != nil {
		return err
	}

	bufEscape, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bufEscape)
	if err = b.args[2].VecEvalInt(b.ctx, input, bufEscape); err != nil {
		return err
	}
	escapes := bufEscape.Int64s()

	if b.pattern == nil {
		b.pattern = b.collator().Pattern()
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(bufVal, bufPattern, bufEscape)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		b.pattern.Compile(bufPattern.GetString(i), byte(escapes[i]))
		match := b.pattern.DoMatch(bufVal.GetString(i))
		i64s[i] = boolToInt64(match)
	}

	return nil
}

func (b *builtinRegexpSig) vectorized() bool {
	return true
}

func (b *builtinRegexpUTF8Sig) vectorized() bool {
	return true
}

func (b *builtinRegexpSharedSig) isMemorizedRegexpInitialized() bool {
	return !(b.memorizedRegexp == nil && b.memorizedErr == nil)
}

func (b *builtinRegexpSharedSig) initMemoizedRegexp(patterns *chunk.Column, n int) {
	// Precondition: patterns is generated from a constant expression
	if n == 0 {
		// If the input rownum is zero, the Regexp error shouldn't be generated.
		return
	}
	for i := 0; i < n; i++ {
		if patterns.IsNull(i) {
			continue
		}
		re, err := b.compile(patterns.GetString(i))
		b.memorizedRegexp = re
		b.memorizedErr = err
		break
	}
	if !b.isMemorizedRegexpInitialized() {
		b.memorizedErr = errors.New("No valid regexp pattern found")
	}
	if b.memorizedErr != nil {
		b.memorizedRegexp = nil
	}
}

func (b *builtinRegexpSharedSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
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

	memorization := func() {
		if b.args[1].ConstItem(b.ctx.GetSessionVars().StmtCtx) && !b.isMemorizedRegexpInitialized() {
			b.initMemoizedRegexp(bufPat, n)
		}
	}
	// Only be executed once to achieve thread-safe
	b.once.Do(memorization)

	getRegexp := func(pat string) (*regexp.Regexp, error) {
		if b.isMemorizedRegexpInitialized() {
			return b.memorizedRegexp, b.memorizedErr
		}
		return b.compile(pat)
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
