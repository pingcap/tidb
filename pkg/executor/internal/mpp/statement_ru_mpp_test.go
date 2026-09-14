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

package mpp

import (
	"context"
	"sync"
	"testing"

	mpppb "github.com/pingcap/kvproto/pkg/mpp"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestStatementRUMPPPartialReports(t *testing.T) {
	for _, all := range []bool{false, true} {
		t.Run(map[bool]string{false: "timeout", true: "complete"}[all], func(t *testing.T) {
			ctx := mock.NewContext()
			stats := execdetails.NewRuntimeStatsColl(nil)
			ctx.GetSessionVars().StmtCtx.RuntimeStatsColl = stats
			c := &localMppCoordinator{
				ctx: context.Background(), sessionCtx: ctx, reportExecutionInfo: true,
				reportStatusCh: make(chan struct{}), planIDs: []int{1},
				reqMap:  map[int64]*mppRequestReport{1: {}, 2: {}},
				mppReqs: []*kv.MPPDispatchRequest{{ID: 1}, {ID: 2}},
			}
			report := func(task int64) error {
				id, rows := "TableScan_1", uint64(7)
				data, err := (&tipb.TiFlashExecutionInfo{ExecutionSummaries: []*tipb.ExecutorExecutionSummary{{ExecutorId: &id, NumProducedRows: &rows}}}).Marshal()
				require.NoError(t, err)
				return c.ReportStatus(kv.ReportStatusRequest{Request: &mpppb.ReportTaskStatusRequest{Meta: &mpppb.TaskMeta{TaskId: task}, Data: data}})
			}
			require.NoError(t, report(1))
			require.Error(t, report(1))
			if all {
				require.NoError(t, report(2))
			}
			require.NoError(t, c.handleAllReports())
			units, found := stats.GetTiFlashExecutionUnits(1)
			require.True(t, found)
			expected := uint64(7)
			if all {
				expected = 14
			}
			require.Equal(t, expected, units.Rows)
			if !all {
				require.NoError(t, report(2))
			}
			require.NoError(t, c.handleAllReports())
			after, _ := stats.GetTiFlashExecutionUnits(1)
			require.Equal(t, units, after)
		})
	}
}

func TestStatementRUMPPReportRoute(t *testing.T) {
	r := &ExecutorWithRetry{coord: &localMppCoordinator{}}
	require.False(t, r.ReportsExecutionSummariesDirectly())
	r.coord = &localMppCoordinator{reportExecutionInfo: true}
	require.True(t, r.ReportsExecutionSummariesDirectly())
	r.coord = &localMppCoordinator{}
	require.False(t, r.ReportsExecutionSummariesDirectly())
}

func TestStatementRUMPPConcurrentReports(t *testing.T) {
	ctx := mock.NewContext()
	stats := execdetails.NewRuntimeStatsColl(nil)
	ctx.GetSessionVars().StmtCtx.RuntimeStatsColl = stats
	c := &localMppCoordinator{ctx: context.Background(), sessionCtx: ctx, reportExecutionInfo: true,
		reportStatusCh: make(chan struct{}), planIDs: []int{1}, reqMap: make(map[int64]*mppRequestReport)}
	const tasks = 16
	for task := int64(1); task <= tasks; task++ {
		c.reqMap[task] = &mppRequestReport{}
		c.mppReqs = append(c.mppReqs, &kv.MPPDispatchRequest{ID: task})
	}
	id, rows := "TableScan_1", uint64(7)
	data, err := (&tipb.TiFlashExecutionInfo{ExecutionSummaries: []*tipb.ExecutorExecutionSummary{{ExecutorId: &id, NumProducedRows: &rows}}}).Marshal()
	require.NoError(t, err)
	start := make(chan struct{})
	errs := make(chan error, tasks+1)
	var wg sync.WaitGroup
	for task := int64(1); task <= tasks; task++ {
		wg.Go(func() {
			<-start
			errs <- c.ReportStatus(kv.ReportStatusRequest{Request: &mpppb.ReportTaskStatusRequest{Meta: &mpppb.TaskMeta{TaskId: task}, Data: data}})
		})
	}
	wg.Go(func() { <-start; errs <- c.handleAllReports() })
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	frozen, found := stats.GetTiFlashExecutionUnits(1)
	// Scheduling can exceed the existing 100ms deadline: either complete or partial
	// evidence is valid, but the terminal snapshot must never change afterward.
	if found {
		require.LessOrEqual(t, frozen.Rows, uint64(tasks*7))
		require.False(t, frozen.Invalid)
	}
	require.NoError(t, c.handleAllReports())
	after, _ := stats.GetTiFlashExecutionUnits(1)
	require.Equal(t, frozen, after)
}
