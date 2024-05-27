// Copyright 2024 PingCAP, Inc.
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

package contextstatic

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/expression/context"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/stretchr/testify/require"
)

func TestNewStaticExprCtx(t *testing.T) {
	prevID := contextutil.GenContextID()
	ctx := NewStaticExprContext()
	require.Equal(t, ctx.GetEvalCtx().CtxID(), prevID+1)
	checkDefaultStaticExprCtx(t, ctx)

	opts, s := getExprCtxOptionsForTest()
	ctx = NewStaticExprContext(opts...)
	checkOptionsStaticExprCtx(t, ctx, s)
}

func TestStaticExprCtxApplyOptions(t *testing.T) {
	ctx := NewStaticExprContext()
	oldCanUseCache := ctx.canUseCache
	oldEvalCtx := ctx.evalCtx
	oldColumnIDAllocator := ctx.columnIDAllocator

	// apply with options
	opts, s := getExprCtxOptionsForTest()
	ctx2 := ctx.Apply(opts...)
	require.NotSame(t, oldCanUseCache, ctx2.canUseCache)
	require.Equal(t, oldEvalCtx, ctx.evalCtx)
	require.Same(t, oldCanUseCache, ctx.canUseCache)
	require.Same(t, oldColumnIDAllocator, ctx.columnIDAllocator)
	checkDefaultStaticExprCtx(t, ctx)
	checkOptionsStaticExprCtx(t, ctx2, s)

	// apply with empty options
	ctx3 := ctx2.Apply()
	s.skipCacheArgs = nil
	checkOptionsStaticExprCtx(t, ctx3, s)
	require.NotSame(t, ctx2.canUseCache, ctx3.canUseCache)
}

func checkDefaultStaticExprCtx(t *testing.T, ctx *StaticExprContext) {
	checkDefaultStaticEvalCtx(t, ctx.GetEvalCtx().(*StaticEvalContext))
	charsetName, collation := ctx.GetCharsetInfo()
	require.Equal(t, mysql.DefaultCharset, charsetName)
	cs, err := charset.GetCharsetInfo(charsetName)
	require.NoError(t, err)
	require.Equal(t, charsetName, cs.Name)
	require.Equal(t, cs.DefaultCollation, collation)
	require.Equal(t, mysql.DefaultCollationName, ctx.GetDefaultCollationForUTF8MB4())
	require.Equal(t, variable.DefBlockEncryptionMode, ctx.GetBlockEncryptionMode())
	require.Equal(t, variable.DefSysdateIsNow, ctx.GetSysdateIsNow())
	require.Equal(t, variable.TiDBOptOnOffWarn(variable.DefTiDBEnableNoopFuncs), ctx.GetNoopFuncsMode())
	require.NotNil(t, ctx.Rng())
	require.True(t, ctx.IsUseCache())
	require.Nil(t, ctx.skipCacheHandleFunc)
	require.NotNil(t, ctx.columnIDAllocator)
	_, ok := ctx.columnIDAllocator.(*context.SimplePlanColumnIDAllocator)
	require.True(t, ok)
	require.Equal(t, uint64(0), ctx.ConnectionID())
	require.Equal(t, true, ctx.GetWindowingUseHighPrecision())
	require.Equal(t, variable.DefGroupConcatMaxLen, ctx.GetGroupConcatMaxLen())
}

type exprCtxOptionsTestState struct {
	evalCtx       *StaticEvalContext
	colIDAlloc    context.PlanColumnIDAllocator
	rng           *mathutil.MysqlRng
	skipCacheArgs []any
}

func getExprCtxOptionsForTest() ([]StaticExprCtxOption, *exprCtxOptionsTestState) {
	s := &exprCtxOptionsTestState{
		evalCtx:    NewStaticEvalContext(WithLocation(time.FixedZone("UTC+11", 11*3600))),
		colIDAlloc: context.NewSimplePlanColumnIDAllocator(1024),
		rng:        mathutil.NewWithSeed(12345678),
	}

	return []StaticExprCtxOption{
		WithEvalCtx(s.evalCtx),
		WithCharset("gbk", "gbk_bin"),
		WithDefaultCollationForUTF8MB4("utf8mb4_0900_ai_ci"),
		WithBlockEncryptionMode("aes-256-cbc"),
		WithSysDateIsNow(true),
		WithNoopFuncsMode(variable.WarnInt),
		WithRng(s.rng),
		WithUseCache(false),
		WithSkipCacheHandleFunc(func(useCache *atomic.Bool, skipReason string) {
			s.skipCacheArgs = []any{useCache, skipReason}
		}),
		WithColumnIDAllocator(s.colIDAlloc),
		WithConnectionID(778899),
		WithWindowingUseHighPrecision(false),
		WithGroupConcatMaxLen(2233445566),
	}, s
}

func checkOptionsStaticExprCtx(t *testing.T, ctx *StaticExprContext, s *exprCtxOptionsTestState) {
	require.Same(t, s.evalCtx, ctx.GetEvalCtx())
	cs, collation := ctx.GetCharsetInfo()
	require.Equal(t, "gbk", cs)
	require.Equal(t, "gbk_bin", collation)
	require.Equal(t, "utf8mb4_0900_ai_ci", ctx.GetDefaultCollationForUTF8MB4())
	require.Equal(t, "aes-256-cbc", ctx.GetBlockEncryptionMode())
	require.Equal(t, true, ctx.GetSysdateIsNow())
	require.Equal(t, variable.WarnInt, ctx.GetNoopFuncsMode())
	require.Same(t, s.rng, ctx.Rng())
	require.False(t, ctx.IsUseCache())
	require.Nil(t, s.skipCacheArgs)
	ctx.SetSkipPlanCache("reason")
	require.Equal(t, []any{ctx.canUseCache, "reason"}, s.skipCacheArgs)
	require.Same(t, s.colIDAlloc, ctx.columnIDAllocator)
	require.Equal(t, uint64(778899), ctx.ConnectionID())
	require.False(t, ctx.GetWindowingUseHighPrecision())
	require.Equal(t, uint64(2233445566), ctx.GetGroupConcatMaxLen())
}

func TestStaticExprCtxUseCache(t *testing.T) {
	// default implement
	ctx := NewStaticExprContext()
	require.True(t, ctx.IsUseCache())
	require.Nil(t, ctx.skipCacheHandleFunc)
	ctx.SetSkipPlanCache("reason")
	require.False(t, ctx.IsUseCache())
	require.Empty(t, ctx.GetEvalCtx().TruncateWarnings(0))

	ctx = NewStaticExprContext(WithUseCache(false))
	require.False(t, ctx.IsUseCache())
	require.Nil(t, ctx.skipCacheHandleFunc)
	ctx.SetSkipPlanCache("reason")
	require.False(t, ctx.IsUseCache())
	require.Empty(t, ctx.GetEvalCtx().TruncateWarnings(0))

	ctx = NewStaticExprContext(WithUseCache(true))
	require.True(t, ctx.IsUseCache())
	require.Nil(t, ctx.skipCacheHandleFunc)
	ctx.SetSkipPlanCache("reason")
	require.False(t, ctx.IsUseCache())
	require.Empty(t, ctx.GetEvalCtx().TruncateWarnings(0))

	// custom skip func
	var args []any
	calls := 0
	ctx = NewStaticExprContext(WithSkipCacheHandleFunc(func(useCache *atomic.Bool, skipReason string) {
		args = []any{useCache, skipReason}
		calls++
		if calls > 1 {
			useCache.Store(false)
		}
	}))
	ctx.SetSkipPlanCache("reason1")
	// If we use `WithSkipCacheHandleFunc`, useCache will be set in function
	require.Equal(t, 1, calls)
	require.True(t, ctx.IsUseCache())
	require.Equal(t, []any{ctx.canUseCache, "reason1"}, args)

	args = nil
	ctx.SetSkipPlanCache("reason2")
	require.Equal(t, 2, calls)
	require.False(t, ctx.IsUseCache())
	require.Equal(t, []any{ctx.canUseCache, "reason2"}, args)

	// apply
	ctx = NewStaticExprContext()
	require.True(t, ctx.IsUseCache())
	ctx2 := ctx.Apply(WithUseCache(false))
	require.False(t, ctx2.IsUseCache())
	require.True(t, ctx.IsUseCache())
	require.NotSame(t, ctx.canUseCache, ctx2.canUseCache)
	require.Nil(t, ctx.skipCacheHandleFunc)
	require.Nil(t, ctx2.skipCacheHandleFunc)

	var args2 []any
	fn1 := func(useCache *atomic.Bool, skipReason string) { args = []any{useCache, skipReason} }
	fn2 := func(useCache *atomic.Bool, skipReason string) { args2 = []any{useCache, skipReason} }
	ctx = NewStaticExprContext(WithUseCache(false), WithSkipCacheHandleFunc(fn1))
	require.False(t, ctx.IsUseCache())
	ctx2 = ctx.Apply(WithUseCache(true), WithSkipCacheHandleFunc(fn2))
	require.NotSame(t, ctx.canUseCache, ctx2.canUseCache)
	require.False(t, ctx.IsUseCache())
	require.True(t, ctx2.IsUseCache())

	args = nil
	args2 = nil
	ctx.SetSkipPlanCache("reasonA")
	require.Equal(t, []any{ctx.canUseCache, "reasonA"}, args)
	require.Nil(t, args2)

	args = nil
	args2 = nil
	ctx2.SetSkipPlanCache("reasonB")
	require.Nil(t, args)
	require.Equal(t, []any{ctx2.canUseCache, "reasonB"}, args2)
}

func TestExprCtxColumnIDAllocator(t *testing.T) {
	// default
	ctx := NewStaticExprContext()
	alloc := ctx.columnIDAllocator
	require.NotNil(t, alloc)
	_, ok := ctx.columnIDAllocator.(*context.SimplePlanColumnIDAllocator)
	require.True(t, ok)
	require.Equal(t, int64(1), ctx.AllocPlanColumnID())

	// Apply without an allocator
	ctx2 := ctx.Apply()
	require.Same(t, ctx2.columnIDAllocator, ctx.columnIDAllocator)
	require.Equal(t, int64(2), ctx2.AllocPlanColumnID())
	require.Equal(t, int64(3), ctx.AllocPlanColumnID())

	// Apply with new allocator
	alloc = context.NewSimplePlanColumnIDAllocator(1024)
	ctx3 := ctx.Apply(WithColumnIDAllocator(alloc))
	require.Same(t, alloc, ctx3.columnIDAllocator)
	require.NotSame(t, ctx.columnIDAllocator, ctx3.columnIDAllocator)
	require.Equal(t, int64(1025), ctx3.AllocPlanColumnID())
	require.Equal(t, int64(4), ctx.AllocPlanColumnID())

	// New context with allocator
	alloc = context.NewSimplePlanColumnIDAllocator(2048)
	ctx4 := NewStaticExprContext(WithColumnIDAllocator(alloc))
	require.Same(t, alloc, ctx4.columnIDAllocator)
	require.Equal(t, int64(2049), ctx4.AllocPlanColumnID())
}
