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

package ddl

import (
	"context"
	"testing"
	"time"

	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

// NewJobElapsedTimeUpdaterForTest exercises snapshot reads without starting
// workers that would consume the synthetic job records.
func NewJobElapsedTimeUpdaterForTest(pool *sess.Pool) func(context.Context) error {
	return (&jobScheduler{sessPool: pool}).updateJobElapsedTimeMetrics
}

func TestDDLJobElapsedTimeAggregation(t *testing.T) {
	now := time.Unix(1000, 0)
	maxima := make(map[string]float64)
	job := &model.Job{
		Type: model.ActionAddIndex, State: model.JobStatePausing,
		StartTS: oracle.GoTimeToTS(now.Add(-2 * time.Minute)), ElapsedTime: &model.JobElapsedTime{},
	}
	updateMaxJobElapsedTime(maxima, job, now)
	require.Equal(t, 120.0, maxima[job.Type.String()])
	job.StartTS = oracle.GoTimeToTS(now.Add(-3 * time.Minute))
	job.ElapsedTime.PausedDurationMS = time.Minute.Milliseconds()
	updateMaxJobElapsedTime(maxima, job, now)
	require.Equal(t, 120.0, maxima[job.Type.String()])
	for _, state := range []model.JobState{model.JobStatePaused, model.JobStateSynced, model.JobStateCancelled, model.JobStateRollbackDone} {
		job.State = state
		job.StartTS = oracle.GoTimeToTS(now.Add(-time.Hour))
		updateMaxJobElapsedTime(maxima, job, now)
		require.Equal(t, 120.0, maxima[job.Type.String()])
	}
	job.State = model.JobStateDone
	job.StartTS = oracle.GoTimeToTS(now.Add(-4 * time.Minute))
	updateMaxJobElapsedTime(maxima, job, now)
	require.Equal(t, 180.0, maxima[job.Type.String()])
	job.ElapsedTime = nil
	job.StartTS = oracle.GoTimeToTS(now.Add(-time.Hour))
	updateMaxJobElapsedTime(maxima, job, now)
	require.Equal(t, 180.0, maxima[job.Type.String()])
	job.ElapsedTime = &model.JobElapsedTime{}
	job.Type = model.ActionCreateTable
	job.StartTS = oracle.GoTimeToTS(now.Add(time.Minute))
	updateMaxJobElapsedTime(maxima, job, now)
	require.Zero(t, maxima[job.Type.String()])
}

func TestDDLJobElapsedTimePauseTransition(t *testing.T) {
	ts := oracle.GoTimeToTS(time.Unix(100, 0))
	ctx := &jobContext{logger: zap.NewNop(), metaMut: &meta.Mutator{StartTS: ts}}
	for _, tracked := range []bool{false, true} {
		job := &model.Job{State: model.JobStatePausing}
		if tracked {
			job.ElapsedTime = &model.JobElapsedTime{}
		}
		runnable, err := (&worker{}).processJobPausingRequest(ctx, job)
		require.False(t, runnable)
		require.Error(t, err)
		require.Equal(t, model.JobStatePaused, job.State)
		if tracked {
			require.Equal(t, ts, job.ElapsedTime.PauseStartTS)
		} else {
			require.Nil(t, job.ElapsedTime)
		}
	}
}

func TestDDLJobElapsedTimeOwnerCleanup(t *testing.T) {
	metrics.DDLUnfinishedJobMaxElapsed.WithLabelValues("add index").Set(1000)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	(&jobScheduler{schCtx: ctx}).jobElapsedTimeLoop()
	registry := prometheus.NewRegistry()
	require.NoError(t, registry.Register(metrics.DDLUnfinishedJobMaxElapsed))
	families, err := registry.Gather()
	require.NoError(t, err)
	require.Empty(t, families)
}
