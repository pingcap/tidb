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

package stmtctx_test

import (
	"errors"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/stretchr/testify/require"
)

func TestModelInferenceStatsAggregation(t *testing.T) {
	sc := stmtctx.NewStmtCtx()
	stats := sc.ModelInferenceStats()

	stats.RecordInference(1, 10, 2, stmtctx.ModelInferenceRolePredicate, 3, 100*time.Millisecond, nil)
	stats.RecordInference(1, 10, 2, stmtctx.ModelInferenceRolePredicate, 2, 200*time.Millisecond, errors.New("boom"))
	stats.RecordLoad(1, 10, 2, stmtctx.ModelInferenceRolePredicate, 50*time.Millisecond, nil)
	stats.RecordLoad(1, 10, 2, stmtctx.ModelInferenceRolePredicate, 40*time.Millisecond, errors.New("load"))

	planSummaries := stats.PlanSummaries()
	require.Len(t, planSummaries, 1)
	entries := planSummaries[1]
	require.Len(t, entries, 1)
	entry := entries[0]
	require.Equal(t, int64(10), entry.ModelID)
	require.Equal(t, int64(2), entry.VersionID)
	require.Equal(t, stmtctx.ModelInferenceRolePredicate, entry.Role)
	require.Equal(t, int64(2), entry.Calls)
	require.Equal(t, int64(1), entry.Errors)
	require.Equal(t, 300*time.Millisecond, entry.TotalInferenceTime)
	require.Equal(t, int64(5), entry.TotalBatchSize)
	require.Equal(t, int64(3), entry.MaxBatchSize)
	require.Equal(t, 90*time.Millisecond, entry.TotalLoadTime)
	require.Equal(t, int64(1), entry.LoadErrors)
	require.InDelta(t, 2.5, entry.AvgBatchSize(), 0.01)
}

func TestModelInferenceStatsSlowLogSummaries(t *testing.T) {
	sc := stmtctx.NewStmtCtx()
	stats := sc.ModelInferenceStats()

	stats.RecordInference(1, 10, 2, stmtctx.ModelInferenceRoleProjection, 1, 10*time.Millisecond, nil)
	stats.RecordInference(2, 10, 2, stmtctx.ModelInferenceRoleProjection, 2, 20*time.Millisecond, nil)
	stats.RecordLoad(1, 10, 2, stmtctx.ModelInferenceRoleProjection, 5*time.Millisecond, nil)

	summaries := stats.SlowLogSummaries()
	require.Len(t, summaries, 1)
	entry := summaries[0]
	require.Equal(t, int64(10), entry.ModelID)
	require.Equal(t, int64(2), entry.VersionID)
	require.Equal(t, stmtctx.ModelInferenceRoleProjection, entry.Role)
	require.Equal(t, int64(2), entry.Calls)
	require.Equal(t, int64(0), entry.Errors)
	require.Equal(t, 30*time.Millisecond, entry.TotalInferenceTime)
	require.Equal(t, int64(3), entry.TotalBatchSize)
	require.Equal(t, int64(2), entry.MaxBatchSize)
	require.Equal(t, 5*time.Millisecond, entry.TotalLoadTime)
	require.Equal(t, int64(0), entry.LoadErrors)
}
