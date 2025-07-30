// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package refresher_test

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sysproctrack"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/priorityqueue"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/refresher"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// mockAnalysisJob implements the priorityqueue.AnalysisJob interface for testing
type mockAnalysisJob struct {
	tableID int64
	weight  float64
	analyze func(statstypes.StatsHandle, sysproctrack.Tracker) error
}

func (m *mockAnalysisJob) GetTableID() int64 { return m.tableID }
func (m *mockAnalysisJob) Analyze(h statstypes.StatsHandle, t sysproctrack.Tracker) error {
	if m.analyze != nil {
		return m.analyze(h, t)
	}
	time.Sleep(50 * time.Millisecond) // Simulate some work
	return nil
}
func (m *mockAnalysisJob) RegisterSuccessHook(priorityqueue.SuccessJobHook) {
	panic("not implemented")
}
func (m *mockAnalysisJob) RegisterFailureHook(priorityqueue.FailureJobHook) {
	panic("not implemented")
}
func (m *mockAnalysisJob) String() string { return "mockAnalysisJob" }
func (m *mockAnalysisJob) ValidateAndPrepare(sessionctx.Context) (bool, string) {
	panic("not implemented")
}
func (m *mockAnalysisJob) SetWeight(weight float64) {
	panic("not implemented")
}
func (m *mockAnalysisJob) GetWeight() float64 {
	panic("not implemented")
}
func (m *mockAnalysisJob) HasNewlyAddedIndex() bool {
	panic("not implemented")
}
func (m *mockAnalysisJob) GetIndicators() priorityqueue.Indicators {
	panic("not implemented")
}
func (m *mockAnalysisJob) SetIndicators(indicators priorityqueue.Indicators) {
	panic("not implemented")
}
func (m *mockAnalysisJob) AsJSON() statstypes.AnalysisJobJSON {
	panic("not implemented")
}

func TestWorker(t *testing.T) {
	_, dom := testkit.CreateMockStoreAndDomain(t)
	handle := dom.StatsHandle()
	sysProcTracker := dom.SysProcTracker()

	t.Run("NewWorker", func(t *testing.T) {
		w := refresher.NewWorker(handle, sysProcTracker, 5)
		require.NotNil(t, w)
		require.Equal(t, 5, w.GetMaxConcurrency())
		w.Stop()
	})

	t.Run("UpdateConcurrency", func(t *testing.T) {
		w := refresher.NewWorker(handle, sysProcTracker, 5)
		w.UpdateConcurrency(10)
		require.Equal(t, 10, w.GetMaxConcurrency())
		w.Stop()
	})

	t.Run("SubmitJob", func(t *testing.T) {
		w := refresher.NewWorker(handle, sysProcTracker, 2)
		job1 := &mockAnalysisJob{tableID: 1}
		job2 := &mockAnalysisJob{tableID: 2}
		job3 := &mockAnalysisJob{tableID: 3}

		require.True(t, w.SubmitJob(job1))
		require.True(t, w.SubmitJob(job2))
		require.False(t, w.SubmitJob(job3)) // Should be rejected due to concurrency limit

		w.WaitAutoAnalyzeFinishedForTest()
		require.Empty(t, w.GetRunningJobs())
		w.Stop()
	})

	t.Run("GetRunningJobs", func(t *testing.T) {
		w := refresher.NewWorker(handle, sysProcTracker, 2)
		jobStarted := make(chan struct{}, 2)

		// Create a custom job that signals when it starts
		customJob := func(id int64) *mockAnalysisJob {
			return &mockAnalysisJob{
				tableID: id,
				analyze: func(statstypes.StatsHandle, sysproctrack.Tracker) error {
					jobStarted <- struct{}{}
					time.Sleep(50 * time.Millisecond) // Simulate some work
					return nil
				},
			}
		}

		job1 := customJob(1)
		job2 := customJob(2)

		w.SubmitJob(job1)
		w.SubmitJob(job2)

		// Wait for both jobs to start
		<-jobStarted
		<-jobStarted

		runningJobs := w.GetRunningJobs()
		require.Len(t, runningJobs, 2)
		require.Contains(t, runningJobs, int64(1))
		require.Contains(t, runningJobs, int64(2))

		w.WaitAutoAnalyzeFinishedForTest()
		require.Empty(t, w.GetRunningJobs())
		w.Stop()
	})

	t.Run("PanicInJob", func(t *testing.T) {
		w := refresher.NewWorker(handle, sysProcTracker, 1)
		panicJob := &mockAnalysisJob{
			tableID: 1,
			analyze: func(statstypes.StatsHandle, sysproctrack.Tracker) error {
				panic("simulated panic")
			},
		}

		require.True(t, w.SubmitJob(panicJob))
		w.WaitAutoAnalyzeFinishedForTest()
		require.Empty(t, w.GetRunningJobs())
		w.Stop()
	})

	t.Run("PanicInMultipleJobs", func(t *testing.T) {
		w := refresher.NewWorker(handle, sysProcTracker, 2)
		panicJob1 := &mockAnalysisJob{
			tableID: 1,
			analyze: func(statstypes.StatsHandle, sysproctrack.Tracker) error {
				panic("simulated panic 1")
			},
		}
		panicJob2 := &mockAnalysisJob{
			tableID: 2,
			analyze: func(statstypes.StatsHandle, sysproctrack.Tracker) error {
				panic("simulated panic 2")
			},
		}

		require.True(t, w.SubmitJob(panicJob1))
		require.True(t, w.SubmitJob(panicJob2))
		w.WaitAutoAnalyzeFinishedForTest()
		require.Empty(t, w.GetRunningJobs())
		w.Stop()
	})
}
