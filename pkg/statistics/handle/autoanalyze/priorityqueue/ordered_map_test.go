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

package priorityqueue_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sysproctrack"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/priorityqueue"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/stretchr/testify/require"
)

type mockJob struct {
	tableID int64
	weight  float64
}

func (mj mockJob) IsValidToAnalyze(sctx sessionctx.Context) (bool, string) {
	panic("implement me")
}
func (mj mockJob) Analyze(statsHandle statstypes.StatsHandle, sysProcTracker sysproctrack.Tracker) error {
	panic("implement me")
}
func (mj mockJob) SetWeight(weight float64) {
	panic("implement me")
}
func (mj mockJob) GetWeight() float64 {
	return mj.weight
}
func (mj mockJob) HasNewlyAddedIndex() bool {
	panic("implement me")
}
func (mj mockJob) GetIndicators() priorityqueue.Indicators {
	panic("implement me")
}
func (mj mockJob) SetIndicators(indicators priorityqueue.Indicators) {
	panic("implement me")
}
func (mj mockJob) RegisterJobCompletionHook(hook priorityqueue.JobCompletionHook) {
	panic("implement me")
}
func (mj mockJob) String() string {
	panic("implement me")
}
func (mj mockJob) GetTableID() int64 {
	return mj.tableID
}

func createMockJob(tableID int64, weight float64) priorityqueue.AnalysisJob {
	return mockJob{tableID: tableID, weight: weight}
}

func TestOrderedMap_Add(t *testing.T) {
	om := priorityqueue.NewOrderedMap()

	job := createMockJob(1, 10)
	err := om.Add(job)
	require.NoError(t, err)

	retJob, err := om.Get(1)
	require.NoError(t, err)
	require.Equal(t, job, retJob)
}

func TestOrderedMap_BulkAdd(t *testing.T) {
	om := priorityqueue.NewOrderedMap()

	jobs := []priorityqueue.AnalysisJob{
		createMockJob(1, 10),
		createMockJob(2, 20),
		createMockJob(3, 5),
	}

	err := om.BulkAdd(jobs)
	require.NoError(t, err)

	retJob, err := om.Get(1)
	require.NoError(t, err)
	require.Equal(t, jobs[0], retJob)

	retJob, err = om.Get(2)
	require.NoError(t, err)
	require.Equal(t, jobs[1], retJob)

	retJob, err = om.Get(3)
	require.NoError(t, err)
	require.Equal(t, jobs[2], retJob)
}

func TestOrderedMap_Get(t *testing.T) {
	om := priorityqueue.NewOrderedMap()

	job := createMockJob(1, 10)
	err := om.Add(job)
	require.NoError(t, err)

	retJob, err := om.Get(1)
	require.NoError(t, err)
	require.Equal(t, job, retJob)

	_, err = om.Get(2)
	require.Error(t, err)
}

func TestOrderedMap_Delete(t *testing.T) {
	om := priorityqueue.NewOrderedMap()

	job := createMockJob(1, 10)
	err := om.Add(job)
	require.NoError(t, err)

	err = om.Delete(job)
	require.NoError(t, err)

	_, err = om.Get(1)
	require.Error(t, err)
}

func TestOrderedMap_Peek(t *testing.T) {
	om := priorityqueue.NewOrderedMap()

	job1 := createMockJob(1, 10)
	job2 := createMockJob(2, 20)

	err := om.Add(job1)
	require.NoError(t, err)

	err = om.Add(job2)
	require.NoError(t, err)

	topJob, err := om.Peek()
	require.NoError(t, err)
	require.Equal(t, job2, topJob)
}

func TestOrderedMap_List(t *testing.T) {
	om := priorityqueue.NewOrderedMap()

	jobs := []priorityqueue.AnalysisJob{
		createMockJob(1, 10),
		createMockJob(2, 20),
		createMockJob(3, 5),
	}

	err := om.BulkAdd(jobs)
	require.NoError(t, err)

	list := om.List()
	require.Len(t, list, 3)
	require.Equal(t, jobs[2], list[0])
	require.Equal(t, jobs[0], list[1])
	require.Equal(t, jobs[1], list[2])
}

func TestOrderedMap_TopN(t *testing.T) {
	om := priorityqueue.NewOrderedMap()

	jobs := []priorityqueue.AnalysisJob{
		createMockJob(1, 10),
		createMockJob(2, 20),
		createMockJob(3, 5),
	}

	err := om.BulkAdd(jobs)
	require.NoError(t, err)

	top2 := om.TopN(2)
	require.Len(t, top2, 2)
	require.Equal(t, jobs[2], top2[0])
	require.Equal(t, jobs[0], top2[1])
}

func TestOrderedMap_IsEmpty(t *testing.T) {
	om := priorityqueue.NewOrderedMap()

	require.True(t, om.IsEmpty())

	job := createMockJob(1, 10)
	err := om.Add(job)
	require.NoError(t, err)

	require.False(t, om.IsEmpty())
}

func TestOrderedMap_Len(t *testing.T) {
	om := priorityqueue.NewOrderedMap()

	require.Equal(t, 0, om.Len())

	job := createMockJob(1, 10)
	err := om.Add(job)
	require.NoError(t, err)

	require.Equal(t, 1, om.Len())
}
