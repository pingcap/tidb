// Copyright 2026 PingCAP, Inc.
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

package sessionctx_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/meta/model"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/resourcegroup"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/mock"
	rangerctx "github.com/pingcap/tidb/pkg/util/ranger/context"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikvrpc"
	tikvutil "github.com/tikv/client-go/v2/util"
)

type countingDistSQLCtxContext struct {
	*mock.Context
	distSQLCalls   atomic.Int32
	runawayChecker resourcegroup.RunawayChecker
}

func (c *countingDistSQLCtxContext) GetDistSQLCtx() *distsqlctx.DistSQLContext {
	c.distSQLCalls.Add(1)
	dctx := c.Context.GetDistSQLCtx()
	dctx.RunawayChecker = c.runawayChecker
	return dctx
}

type countingPlanCtxContext struct {
	*mock.Context
	planCtxCalls    atomic.Int32
	buildPBCtxCalls atomic.Int32
	rangerCtxCalls  atomic.Int32
}

func (c *countingPlanCtxContext) GetPlanCtx() planctx.PlanContext {
	c.planCtxCalls.Add(1)
	return c
}

func (c *countingPlanCtxContext) GetBuildPBCtx() *planctx.BuildPBContext {
	c.buildPBCtxCalls.Add(1)
	return c.Context.GetBuildPBCtx()
}

func (c *countingPlanCtxContext) GetRangerCtx() *rangerctx.RangerContext {
	c.rangerCtxCalls.Add(1)
	return c.Context.GetRangerCtx()
}

type stubRunawayChecker struct{}

func (*stubRunawayChecker) BeforeExecutor() (string, error) { return "", nil }

func (*stubRunawayChecker) BeforeCopRequest(*tikvrpc.Request) error { return nil }

func (*stubRunawayChecker) CheckThresholds(*tikvutil.RUDetails, int64, error) error { return nil }

func (*stubRunawayChecker) ResetTotalProcessedKeys() {}

func (*stubRunawayChecker) CheckAction() rmpb.RunawayAction { return rmpb.RunawayAction_NoneAction }

func (*stubRunawayChecker) CheckRuleKillAction() (string, bool) { return "", false }

type locationOverrideEvalCtx struct {
	exprctx.EvalContext
	location *time.Location
}

func (ctx *locationOverrideEvalCtx) Location() *time.Location {
	return ctx.location
}

type locationOverrideExprCtx struct {
	exprctx.ExprContext
	evalCtx exprctx.EvalContext
}

func (ctx *locationOverrideExprCtx) GetEvalCtx() exprctx.EvalContext {
	return ctx.evalCtx
}

func TestWithExprCtxScopedContexts(t *testing.T) {
	t.Run("dist sql ctx preserves shared detached snapshot", func(t *testing.T) {
		baseCtx := &countingDistSQLCtxContext{Context: mock.NewContext()}
		originalDistSQLCtx := baseCtx.Context.GetDistSQLCtx()
		originalLevel := originalDistSQLCtx.ErrCtx.LevelForGroup(errctx.ErrGroupTruncate)

		overrideExprCtx := exprctx.WithHandleTruncateErrLevel(baseCtx.GetExprCtx(), errctx.LevelIgnore)
		scopedCtx := sessionctx.WithExprCtx(baseCtx, overrideExprCtx)
		shared := scopedCtx.GetDistSQLCtx()
		require.NotNil(t, shared)
		shared.TryCopLiteWorker.Store(1)

		const goroutines = 32
		results := make(chan *distsqlctx.DistSQLContext, goroutines)
		var wg sync.WaitGroup
		for range goroutines {
			wg.Add(1)
			go func() {
				defer wg.Done()
				results <- scopedCtx.GetDistSQLCtx()
			}()
		}
		wg.Wait()
		close(results)

		for dctx := range results {
			require.NotNil(t, dctx)
			require.Same(t, shared, dctx)
			require.Equal(t, errctx.LevelIgnore, dctx.ErrCtx.LevelForGroup(errctx.ErrGroupTruncate))
			require.Equal(t, uint32(1), dctx.TryCopLiteWorker.Load())
		}

		require.Equal(t, int32(goroutines+1), baseCtx.distSQLCalls.Load())
		require.NotSame(t, originalDistSQLCtx, shared)
		require.Equal(t, errctx.LevelIgnore, shared.ErrCtx.LevelForGroup(errctx.ErrGroupTruncate))
		require.Equal(t, originalLevel, originalDistSQLCtx.ErrCtx.LevelForGroup(errctx.ErrGroupTruncate))
		require.Equal(t, uint32(0), originalDistSQLCtx.TryCopLiteWorker.Load())
	})

	t.Run("dist sql ctx refreshes late bound fields", func(t *testing.T) {
		baseCtx := &countingDistSQLCtxContext{Context: mock.NewContext()}
		overrideExprCtx := exprctx.WithHandleTruncateErrLevel(baseCtx.GetExprCtx(), errctx.LevelIgnore)
		scopedCtx := sessionctx.WithExprCtx(baseCtx, overrideExprCtx)

		metrics1 := execdetails.NewRUV2Metrics()
		checker1 := &stubRunawayChecker{}
		baseCtx.GetSessionVars().RUV2Metrics = metrics1
		baseCtx.runawayChecker = checker1

		first := scopedCtx.GetDistSQLCtx()
		require.NotNil(t, first)
		require.Same(t, metrics1, first.RUV2Metrics)
		require.Same(t, checker1, first.RunawayChecker)
		require.Equal(t, errctx.LevelIgnore, first.ErrCtx.LevelForGroup(errctx.ErrGroupTruncate))

		metrics2 := execdetails.NewRUV2Metrics()
		checker2 := &stubRunawayChecker{}
		baseCtx.GetSessionVars().RUV2Metrics = metrics2
		baseCtx.runawayChecker = checker2

		second := scopedCtx.GetDistSQLCtx()
		require.NotNil(t, second)
		require.Same(t, first, second)
		require.Same(t, metrics2, second.RUV2Metrics)
		require.Same(t, checker2, second.RunawayChecker)
		require.Equal(t, errctx.LevelIgnore, second.ErrCtx.LevelForGroup(errctx.ErrGroupTruncate))
		require.Same(t, metrics2, first.RUV2Metrics)
		require.Same(t, checker2, first.RunawayChecker)
	})

	t.Run("dist sql ctx follows scoped timezone", func(t *testing.T) {
		baseCtx := mock.NewContext()
		original := baseCtx.GetDistSQLCtx()
		overrideLoc := time.FixedZone("override", 9*3600)
		overrideExprCtx := &locationOverrideExprCtx{
			ExprContext: baseCtx.GetExprCtx(),
			evalCtx: &locationOverrideEvalCtx{
				EvalContext: baseCtx.GetExprCtx().GetEvalCtx(),
				location:    overrideLoc,
			},
		}
		scopedCtx := sessionctx.WithExprCtx(baseCtx, overrideExprCtx)

		dctx := scopedCtx.GetDistSQLCtx()
		require.NotNil(t, dctx)
		require.Same(t, overrideLoc, dctx.Location)
		require.Same(t, original.Location, baseCtx.GetDistSQLCtx().Location)
	})

	t.Run("plan ctx and detached caches init once", func(t *testing.T) {
		baseCtx := &countingPlanCtxContext{Context: mock.NewContext()}
		overrideExprCtx := exprctx.WithHandleTruncateErrLevel(baseCtx.GetExprCtx(), errctx.LevelIgnore)
		scopedCtx := sessionctx.WithExprCtx(baseCtx, overrideExprCtx)

		const goroutines = 32
		planResults := make(chan planctx.PlanContext, goroutines)
		buildPBResults := make(chan *planctx.BuildPBContext, goroutines)
		rangerResults := make(chan *rangerctx.RangerContext, goroutines)

		var wg sync.WaitGroup
		for range goroutines {
			wg.Add(1)
			go func() {
				defer wg.Done()
				planResults <- scopedCtx.GetPlanCtx()
				buildPBResults <- scopedCtx.GetBuildPBCtx()
				rangerResults <- scopedCtx.GetRangerCtx()
			}()
		}
		wg.Wait()
		close(planResults)
		close(buildPBResults)
		close(rangerResults)

		var detachedPlanCtx planctx.PlanContext
		for pctx := range planResults {
			require.NotNil(t, pctx)
			if detachedPlanCtx == nil {
				detachedPlanCtx = pctx
				continue
			}
			require.Same(t, detachedPlanCtx, pctx)
		}

		var detachedBuildPBCtx *planctx.BuildPBContext
		for bpctx := range buildPBResults {
			require.NotNil(t, bpctx)
			if detachedBuildPBCtx == nil {
				detachedBuildPBCtx = bpctx
				continue
			}
			require.Same(t, detachedBuildPBCtx, bpctx)
		}

		var detachedRangerCtx *rangerctx.RangerContext
		for rctx := range rangerResults {
			require.NotNil(t, rctx)
			if detachedRangerCtx == nil {
				detachedRangerCtx = rctx
				continue
			}
			require.Same(t, detachedRangerCtx, rctx)
		}

		require.Equal(t, int32(1), baseCtx.planCtxCalls.Load())
		require.Equal(t, int32(1), baseCtx.buildPBCtxCalls.Load())
		require.Equal(t, int32(1), baseCtx.rangerCtxCalls.Load())
		planErrCtx := detachedPlanCtx.GetExprCtx().GetEvalCtx().ErrCtx()
		buildPBErrCtx := detachedBuildPBCtx.GetExprCtx().GetEvalCtx().ErrCtx()
		require.Equal(t, errctx.LevelIgnore, detachedRangerCtx.ErrCtx.LevelForGroup(errctx.ErrGroupTruncate))
		require.Equal(t, errctx.LevelIgnore, planErrCtx.LevelForGroup(errctx.ErrGroupTruncate))
		require.Equal(t, errctx.LevelIgnore, buildPBErrCtx.LevelForGroup(errctx.ErrGroupTruncate))
		baseExprErrCtx := baseCtx.GetExprCtx().GetEvalCtx().ErrCtx()
		baseBuildPBErrCtx := baseCtx.Context.GetBuildPBCtx().GetExprCtx().GetEvalCtx().ErrCtx()
		require.NotEqual(t, errctx.LevelIgnore, baseExprErrCtx.LevelForGroup(errctx.ErrGroupTruncate))
		require.NotEqual(t, errctx.LevelIgnore, baseBuildPBErrCtx.LevelForGroup(errctx.ErrGroupTruncate))
		require.NotEqual(t, errctx.LevelIgnore, baseCtx.Context.GetRangerCtx().ErrCtx.LevelForGroup(errctx.ErrGroupTruncate))
	})

	t.Run("AsSctx recovers underlying session", func(t *testing.T) {
		baseCtx := mock.NewContext()
		overrideExprCtx := exprctx.WithHandleTruncateErrLevel(baseCtx.GetExprCtx(), errctx.LevelError)
		scopedCtx := sessionctx.WithExprCtx(baseCtx, overrideExprCtx)

		recovered, err := plannercore.AsSctx(scopedCtx.GetPlanCtx())
		require.NoError(t, err)
		require.Same(t, scopedCtx, recovered)
		recoveredErrCtx := recovered.GetExprCtx().GetEvalCtx().ErrCtx()
		require.Equal(t, errctx.LevelError, recoveredErrCtx.LevelForGroup(errctx.ErrGroupTruncate))
	})

	t.Run("push down flags follow scoped expr ctx", func(t *testing.T) {
		baseCtx := mock.NewContext()
		baseCtx.GetSessionVars().StmtCtx.InSelectStmt = true
		baseCtx.GetSessionVars().StmtCtx.SetTypeFlags(baseCtx.GetSessionVars().StmtCtx.TypeFlags().WithTruncateAsWarning(true))
		baseFlags := baseCtx.GetSessionVars().StmtCtx.PushDownFlags()

		scopedCtx := sessionctx.WithTruncateErrLevel(baseCtx, errctx.LevelError)

		require.NotZero(t, baseCtx.GetSessionVars().StmtCtx.PushDownFlags()&model.FlagTruncateAsWarning)
		require.NotZero(t, baseCtx.GetSessionVars().StmtCtx.PushDownFlags()&model.FlagOverflowAsWarning)

		pushDownFlags := sessionctx.GetPushDownFlags(scopedCtx)
		require.NotZero(t, pushDownFlags&model.FlagInSelectStmt)
		require.Zero(t, pushDownFlags&model.FlagTruncateAsWarning)
		require.Zero(t, pushDownFlags&model.FlagOverflowAsWarning)
		require.Zero(t, pushDownFlags&model.FlagIgnoreTruncate)
		require.Equal(t, baseFlags, baseCtx.GetSessionVars().StmtCtx.PushDownFlags())
	})
}
