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

package planner

import (
	"context"
	"testing"
	"time"

	perrors "github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/stretchr/testify/require"
)

func TestLogicalPlanBuildRoundStateResetAndRestore(t *testing.T) {
	sessVars := variable.NewSessionVars(nil)
	sessVars.EnableAlternativeLogicalPlans = true
	sessVars.PlanID.Store(11)
	sessVars.PlanColumnID.Store(22)
	sessVars.MapScalarSubQ = []any{"scalar-subquery"}
	sessVars.MapHashCode2UniqueID4ExtendedCol = map[string]int{"column": 1}
	sessVars.RewritePhaseInfo.DurationRewrite = time.Second
	sessVars.RewritePhaseInfo.DurationPreprocessSubQuery = 2 * time.Second
	sessVars.RewritePhaseInfo.PreprocessSubQueries = 3
	sessVars.StmtCtx.GetUsedStatsInfo(true).RecordUsedInfo(33, &stmtctx.UsedStatsInfoForTable{Name: "initial"})
	sessVars.StmtCtx.SetIndexForce()
	saved := saveLogicalPlanBuildCtx(sessVars)

	resetLogicalPlanBuildRoundState(sessVars)

	require.Zero(t, sessVars.PlanID.Load())
	require.Zero(t, sessVars.PlanColumnID.Load())
	require.Nil(t, sessVars.MapScalarSubQ)
	require.Nil(t, sessVars.MapHashCode2UniqueID4ExtendedCol)
	require.Zero(t, sessVars.RewritePhaseInfo)
	require.Nil(t, sessVars.StmtCtx.GetUsedStatsInfo(false))
	require.False(t, sessVars.StmtCtx.GetIndexForce())

	restoreLogicalPlanBuildCtx(sessVars, saved)
	require.Equal(t, int32(11), sessVars.PlanID.Load())
	require.Equal(t, int64(22), sessVars.PlanColumnID.Load())
	require.Equal(t, []any{"scalar-subquery"}, sessVars.MapScalarSubQ)
	require.Equal(t, map[string]int{"column": 1}, sessVars.MapHashCode2UniqueID4ExtendedCol)
	require.Equal(t, time.Second, sessVars.RewritePhaseInfo.DurationRewrite)
	require.Equal(t, 2*time.Second, sessVars.RewritePhaseInfo.DurationPreprocessSubQuery)
	require.Equal(t, 3, sessVars.RewritePhaseInfo.PreprocessSubQueries)
	require.Equal(t, "initial", sessVars.StmtCtx.GetUsedStatsInfo(false).GetUsedInfo(33).Name)
	require.True(t, sessVars.StmtCtx.GetIndexForce())
}

func TestLogicalPlanBuildRoundStatePreservesFeatureOffState(t *testing.T) {
	sessVars := variable.NewSessionVars(nil)
	sessVars.PlanID.Store(11)
	sessVars.StmtCtx.GetUsedStatsInfo(true).RecordUsedInfo(33, &stmtctx.UsedStatsInfoForTable{Name: "initial"})
	sessVars.StmtCtx.SetIndexForce()

	resetLogicalPlanBuildRoundState(sessVars)

	require.Zero(t, sessVars.PlanID.Load())
	require.Equal(t, "initial", sessVars.StmtCtx.GetUsedStatsInfo(false).GetUsedInfo(33).Name)
	require.True(t, sessVars.StmtCtx.GetIndexForce())
}

func TestLogicalPlanBuildCtxClonesExtendedColumnMap(t *testing.T) {
	sessVars := variable.NewSessionVars(nil)
	sessVars.MapHashCode2UniqueID4ExtendedCol = map[string]int{"before": 1}

	buildCtx := saveLogicalPlanBuildCtx(sessVars)
	sessVars.MapHashCode2UniqueID4ExtendedCol["before"] = 2
	sessVars.MapHashCode2UniqueID4ExtendedCol["after"] = 3

	restoreLogicalPlanBuildCtx(sessVars, buildCtx)
	require.Equal(t, map[string]int{"before": 1}, sessVars.MapHashCode2UniqueID4ExtendedCol)

	sessVars.MapHashCode2UniqueID4ExtendedCol["mutated"] = 4
	require.Equal(t, map[string]int{"before": 1}, buildCtx.mapHashCode2UniqueID)
}

func TestLogicalPlanRoundStateCapturesBuildCtxOnlyWhenRequested(t *testing.T) {
	sessVars := variable.NewSessionVars(nil)
	roundState := logicalPlanRoundState{}

	sessVars.PlanID.Store(11)
	roundState.considerCandidate(sessVars, &core.PhysicalTableDual{}, nil, 2)
	require.Zero(t, roundState.best.logicalPlanCtx.planID)

	roundState.captureBestLogicalPlanCtx = true
	sessVars.PlanID.Store(22)
	roundState.considerCandidate(sessVars, &core.PhysicalTableDual{}, nil, 1)
	require.Equal(t, int32(22), roundState.best.logicalPlanCtx.planID)
}

func TestFallbackToBestPlanAfterAlternativeLogicalPlanError(t *testing.T) {
	sessVars := variable.NewSessionVars(nil)
	sessVars.StmtCtx.AppendWarning(perrors.New("first round warning"))
	bestCtx := saveLogicalPlanBuildCtx(sessVars)
	sessVars.StmtCtx.AppendWarning(perrors.New("second round warning"))

	bestPlan := &core.PhysicalTableDual{}
	plan, names, cost, ok := fallbackToBestPlanAfterAlternativeLogicalPlanError(
		context.Background(),
		sessVars,
		perrors.New("alternative round failed"),
		logicalPlanCandidate{plan: bestPlan, cost: 12.5, logicalPlanCtx: bestCtx},
	)

	require.True(t, ok)
	require.Same(t, bestPlan, plan)
	require.Nil(t, names)
	require.Equal(t, 12.5, cost)
	warnings := sessVars.StmtCtx.GetWarnings()
	require.Len(t, warnings, 1)
	require.Equal(t, "first round warning", warnings[0].Err.Error())
}

func TestFallbackToBestPlanAfterAlternativeLogicalPlanErrorKeepsNonFallbackError(t *testing.T) {
	cases := []struct {
		name string
		ctx  context.Context
		err  error
	}{
		{name: "context canceled error", ctx: context.Background(), err: perrors.Trace(context.Canceled)},
		{name: "context deadline exceeded error", ctx: context.Background(), err: perrors.Trace(context.DeadlineExceeded)},
		{name: "context canceled state", ctx: canceledContext(), err: perrors.New("transport error")},
		{name: "query interrupted", ctx: context.Background(), err: exeerrors.ErrQueryInterrupted.GenWithStackByArgs()},
		{name: "max execution time exceeded", ctx: context.Background(), err: exeerrors.ErrMaxExecTimeExceeded.GenWithStackByArgs()},
		{name: "query memory exceeded", ctx: context.Background(), err: exeerrors.ErrMemoryExceedForQuery.GenWithStackByArgs(123)},
		{name: "server memory exceeded", ctx: context.Background(), err: exeerrors.ErrMemoryExceedForInstance.GenWithStackByArgs(123)},
		{name: "runaway query exceeded", ctx: context.Background(), err: exeerrors.ErrResourceGroupQueryRunawayInterrupted.FastGenByArgs("runaway exceed tidb side")},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			sessVars := variable.NewSessionVars(nil)
			sessVars.StmtCtx.AppendWarning(perrors.New("first round warning"))
			bestCtx := saveLogicalPlanBuildCtx(sessVars)
			sessVars.StmtCtx.AppendWarning(perrors.New("second round warning"))

			plan, names, cost, ok := fallbackToBestPlanAfterAlternativeLogicalPlanError(
				tt.ctx,
				sessVars,
				tt.err,
				logicalPlanCandidate{plan: &core.PhysicalTableDual{}, cost: 12.5, logicalPlanCtx: bestCtx},
			)

			require.False(t, ok)
			require.Nil(t, plan)
			require.Nil(t, names)
			require.Zero(t, cost)
			warnings := sessVars.StmtCtx.GetWarnings()
			require.Len(t, warnings, 2)
			require.Equal(t, "second round warning", warnings[1].Err.Error())
		})
	}
}

func canceledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}
