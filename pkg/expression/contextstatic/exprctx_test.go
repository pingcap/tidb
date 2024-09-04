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
	"testing"
	"time"

	exprctx "github.com/pingcap/tidb/pkg/expression/context"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/deeptest"
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
	oldEvalCtx := ctx.evalCtx
	oldColumnIDAllocator := ctx.columnIDAllocator

	// apply with options
	opts, s := getExprCtxOptionsForTest()
	ctx2 := ctx.Apply(opts...)
	require.Equal(t, oldEvalCtx, ctx.evalCtx)
	require.Same(t, oldColumnIDAllocator, ctx.columnIDAllocator)
	checkDefaultStaticExprCtx(t, ctx)
	checkOptionsStaticExprCtx(t, ctx2, s)

	// apply with empty options
	ctx3 := ctx2.Apply()
	s.skipCacheArgs = nil
	checkOptionsStaticExprCtx(t, ctx3, s)
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
	require.NotNil(t, ctx.columnIDAllocator)
	_, ok := ctx.columnIDAllocator.(*exprctx.SimplePlanColumnIDAllocator)
	require.True(t, ok)
	require.Equal(t, uint64(0), ctx.ConnectionID())
	require.Equal(t, true, ctx.GetWindowingUseHighPrecision())
	require.Equal(t, variable.DefGroupConcatMaxLen, ctx.GetGroupConcatMaxLen())
}

type exprCtxOptionsTestState struct {
	evalCtx       *StaticEvalContext
	colIDAlloc    exprctx.PlanColumnIDAllocator
	rng           *mathutil.MysqlRng
	skipCacheArgs []any
}

func getExprCtxOptionsForTest() ([]StaticExprCtxOption, *exprCtxOptionsTestState) {
	s := &exprCtxOptionsTestState{
		evalCtx:    NewStaticEvalContext(WithLocation(time.FixedZone("UTC+11", 11*3600))),
		colIDAlloc: exprctx.NewSimplePlanColumnIDAllocator(1024),
		rng:        mathutil.NewWithSeed(12345678),
	}
	planCacheTracker := contextutil.NewPlanCacheTracker(s.evalCtx)

	return []StaticExprCtxOption{
		WithEvalCtx(s.evalCtx),
		WithCharset("gbk", "gbk_bin"),
		WithDefaultCollationForUTF8MB4("utf8mb4_0900_ai_ci"),
		WithBlockEncryptionMode("aes-256-cbc"),
		WithSysDateIsNow(true),
		WithNoopFuncsMode(variable.WarnInt),
		WithRng(s.rng),
		WithPlanCacheTracker(&planCacheTracker),
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
	require.Same(t, s.colIDAlloc, ctx.columnIDAllocator)
	require.Equal(t, uint64(778899), ctx.ConnectionID())
	require.False(t, ctx.GetWindowingUseHighPrecision())
	require.Equal(t, uint64(2233445566), ctx.GetGroupConcatMaxLen())
}

func TestExprCtxColumnIDAllocator(t *testing.T) {
	// default
	ctx := NewStaticExprContext()
	alloc := ctx.columnIDAllocator
	require.NotNil(t, alloc)
	_, ok := ctx.columnIDAllocator.(*exprctx.SimplePlanColumnIDAllocator)
	require.True(t, ok)
	require.Equal(t, int64(1), ctx.AllocPlanColumnID())

	// Apply without an allocator
	ctx2 := ctx.Apply()
	require.Same(t, ctx2.columnIDAllocator, ctx.columnIDAllocator)
	require.Equal(t, int64(2), ctx2.AllocPlanColumnID())
	require.Equal(t, int64(3), ctx.AllocPlanColumnID())

	// Apply with new allocator
	alloc = exprctx.NewSimplePlanColumnIDAllocator(1024)
	ctx3 := ctx.Apply(WithColumnIDAllocator(alloc))
	require.Same(t, alloc, ctx3.columnIDAllocator)
	require.NotSame(t, ctx.columnIDAllocator, ctx3.columnIDAllocator)
	require.Equal(t, int64(1025), ctx3.AllocPlanColumnID())
	require.Equal(t, int64(4), ctx.AllocPlanColumnID())

	// New context with allocator
	alloc = exprctx.NewSimplePlanColumnIDAllocator(2048)
	ctx4 := NewStaticExprContext(WithColumnIDAllocator(alloc))
	require.Same(t, alloc, ctx4.columnIDAllocator)
	require.Equal(t, int64(2049), ctx4.AllocPlanColumnID())
}

func TestMakeExprContextStatic(t *testing.T) {
	evalCtx := NewStaticEvalContext()
	planCacheTracker := contextutil.NewPlanCacheTracker(evalCtx.warnHandler)
	obj := NewStaticExprContext(
		WithEvalCtx(evalCtx),
		WithCharset("a", "b"),
		WithDefaultCollationForUTF8MB4("c"),
		WithBlockEncryptionMode("d"),
		WithSysDateIsNow(true),
		WithNoopFuncsMode(1),
		WithRng(mathutil.NewWithSeed(12345678)),
		WithPlanCacheTracker(&planCacheTracker),
		WithColumnIDAllocator(exprctx.NewSimplePlanColumnIDAllocator(1)),
		WithConnectionID(1),
		WithWindowingUseHighPrecision(false),
		WithGroupConcatMaxLen(1),
	)

	ignorePath := []string{
		"$.staticExprCtxState.evalCtx**",
	}
	deeptest.AssertRecursivelyNotEqual(t, obj, NewStaticExprContext(),
		deeptest.WithIgnorePath(ignorePath),
		deeptest.WithPointerComparePath([]string{
			"$.staticExprCtxState.rng",
			"$.staticExprCtxState.planCacheTracker",
		}),
	)

	staticObj := MakeExprContextStatic(obj)
	deeptest.AssertDeepClonedEqual(t, obj, staticObj,
		deeptest.WithIgnorePath(ignorePath),
		deeptest.WithPointerComparePath([]string{
			"$.staticExprCtxState.rng",
			"$.staticExprCtxState.planCacheTracker",
		}))

	require.NotSame(t, obj.GetEvalCtx(), staticObj.GetEvalCtx())
}
