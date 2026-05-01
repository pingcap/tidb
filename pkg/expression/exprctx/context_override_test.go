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

package exprctx_test

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	"github.com/pingcap/tidb/pkg/types"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/stretchr/testify/require"
)

type splitStaticExprContext struct {
	*exprstatic.ExprContext
	staticEvalCtx exprctx.StaticConvertibleEvalContext
}

func (ctx *splitStaticExprContext) GetStaticConvertibleEvalContext() exprctx.StaticConvertibleEvalContext {
	return ctx.staticEvalCtx
}

func (ctx *splitStaticExprContext) GetPlanCacheTracker() *contextutil.PlanCacheTracker {
	return ctx.ExprContext.GetPlanCacheTracker()
}

func (ctx *splitStaticExprContext) GetLastPlanColumnID() int64 {
	return ctx.ExprContext.GetLastPlanColumnID()
}

func TestCtxWithHandleTruncateErrLevel(t *testing.T) {
	for _, level := range []errctx.Level{errctx.LevelWarn, errctx.LevelIgnore, errctx.LevelError} {
		originalLevelMap := errctx.LevelMap{errctx.ErrGroupDividedByZero: errctx.LevelError}
		expectedLevelMap := originalLevelMap
		expectedLevelMap[errctx.ErrGroupTruncate] = level

		originalFlags := types.DefaultStmtFlags
		originalLoc := time.FixedZone("tz1", 3600*2)
		var expectedFlags types.Flags
		switch level {
		case errctx.LevelError:
			originalFlags = originalFlags.WithTruncateAsWarning(true)
			originalLevelMap[errctx.ErrGroupTruncate] = errctx.LevelWarn
			expectedFlags = originalFlags.WithTruncateAsWarning(false)
		case errctx.LevelWarn:
			expectedFlags = originalFlags.WithTruncateAsWarning(true)
		case errctx.LevelIgnore:
			expectedFlags = originalFlags.WithIgnoreTruncateErr(true)
		default:
			require.FailNow(t, "unexpected level")
		}

		evalCtx := exprstatic.NewEvalContext(
			exprstatic.WithTypeFlags(originalFlags),
			exprstatic.WithLocation(originalLoc),
			exprstatic.WithErrLevelMap(errctx.LevelMap{errctx.ErrGroupTruncate: level}),
		)

		tc, ec := evalCtx.TypeCtx(), evalCtx.ErrCtx()
		ctx := exprstatic.NewExprContext(
			exprstatic.WithEvalCtx(evalCtx),
			exprstatic.WithConnectionID(1234),
		)

		// override should take effect
		newCtx := exprctx.CtxWithHandleTruncateErrLevel(ctx, level)
		newEvalCtx := newCtx.GetEvalCtx()
		newTypeCtx, newErrCtx := newEvalCtx.TypeCtx(), newEvalCtx.ErrCtx()
		require.Equal(t, expectedFlags, newTypeCtx.Flags())
		require.Equal(t, expectedLevelMap, newErrCtx.LevelMap())

		newExprCtx := exprctx.WithHandleTruncateErrLevel(ctx, level)
		newExprEvalCtx := newExprCtx.GetEvalCtx()
		newExprTypeCtx, newExprErrCtx := newExprEvalCtx.TypeCtx(), newExprEvalCtx.ErrCtx()
		require.Equal(t, expectedFlags, newExprTypeCtx.Flags())
		require.Equal(t, expectedLevelMap, newExprErrCtx.LevelMap())
		require.Equal(t, uint64(1234), newExprCtx.ConnectionID())
		_, ok := newExprCtx.(exprctx.StaticConvertibleExprContext)
		require.True(t, ok)

		// other fields should not change
		require.Equal(t, originalLoc, newTypeCtx.Location())
		require.Equal(t, originalLoc, newEvalCtx.Location())
		require.Equal(t, uint64(1234), newCtx.ConnectionID())

		// old ctx should not change
		require.Same(t, evalCtx, ctx.GetEvalCtx())
		require.Equal(t, tc, evalCtx.TypeCtx())
		require.Equal(t, ec, evalCtx.ErrCtx())
		require.Same(t, originalLoc, evalCtx.Location())
		require.Equal(t, uint64(1234), ctx.ConnectionID())

		// not create new ctx case
		newCtx2 := exprctx.CtxWithHandleTruncateErrLevel(newCtx, level)
		require.Same(t, newCtx, newCtx2)
	}
}

func TestWithHandleTruncateErrLevelKeepsLiveEvalCtxForStaticConvertibleExprCtx(t *testing.T) {
	liveTime := time.Date(2026, 4, 20, 10, 0, 0, 0, time.FixedZone("live", 8*3600))
	staticTime := time.Date(2026, 4, 20, 11, 0, 0, 0, time.FixedZone("static", 9*3600))

	liveEvalCtx := exprstatic.NewEvalContext(exprstatic.WithCurrentTime(func() (time.Time, error) {
		return liveTime, nil
	}), exprstatic.WithTypeFlags(types.DefaultStmtFlags.WithTruncateAsWarning(true)),
		exprstatic.WithErrLevelMap(errctx.LevelMap{errctx.ErrGroupTruncate: errctx.LevelWarn}))
	staticEvalCtx := exprstatic.NewEvalContext(exprstatic.WithCurrentTime(func() (time.Time, error) {
		return staticTime, nil
	}), exprstatic.WithTypeFlags(types.DefaultStmtFlags.WithTruncateAsWarning(true)),
		exprstatic.WithErrLevelMap(errctx.LevelMap{errctx.ErrGroupTruncate: errctx.LevelWarn}))
	ctx := &splitStaticExprContext{
		ExprContext:   exprstatic.NewExprContext(exprstatic.WithEvalCtx(liveEvalCtx)),
		staticEvalCtx: staticEvalCtx,
	}

	overrideCtx := exprctx.WithHandleTruncateErrLevel(ctx, errctx.LevelError)

	require.Equal(t, liveEvalCtx.CtxID(), overrideCtx.GetEvalCtx().CtxID())
	_, ok := overrideCtx.GetEvalCtx().(exprctx.StaticConvertibleEvalContext)
	require.True(t, ok)

	staticConvertibleCtx, ok := overrideCtx.(exprctx.StaticConvertibleExprContext)
	require.True(t, ok)
	require.Equal(t, staticEvalCtx.CtxID(), staticConvertibleCtx.GetStaticConvertibleEvalContext().CtxID())

	liveErrCtx := overrideCtx.GetEvalCtx().ErrCtx()
	staticErrCtx := staticConvertibleCtx.GetStaticConvertibleEvalContext().ErrCtx()
	require.Equal(t, errctx.LevelError, liveErrCtx.LevelForGroup(errctx.ErrGroupTruncate))
	require.Equal(t, errctx.LevelError, staticErrCtx.LevelForGroup(errctx.ErrGroupTruncate))
}
