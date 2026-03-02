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

package reporter

import (
	"sync/atomic"
	"testing"
	"time"

	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"github.com/pingcap/tidb/pkg/util/topsql/stmtstats"
	"github.com/stretchr/testify/require"
)

type reporterRUSignalSink struct {
	ch chan *ReportData
}

func (s *reporterRUSignalSink) TrySend(data *ReportData, _ time.Time) error {
	s.ch <- data
	return nil
}

func (s *reporterRUSignalSink) OnReporterClosing() {}

func TestRemoteTopSQLReporter_TopRU_CollectRUIncrements_FlowsToReport(t *testing.T) {
	origNowFunc := nowFunc
	var currentUnix int64 = 1
	nowFunc = func() time.Time {
		return time.Unix(atomic.LoadInt64(&currentUnix), 0)
	}
	defer func() {
		nowFunc = origNowFunc
	}()

	topsqlstate.ResetTopRUItemInterval()
	t.Cleanup(func() {
		topsqlstate.ResetTopRUItemInterval()
	})

	tsr := NewRemoteTopSQLReporter(mockPlanBinaryDecoderFunc, mockPlanBinaryCompressFunc)
	t.Cleanup(tsr.Close)
	tsr.BindKeyspaceName([]byte("ks-flow"))

	sink := &reporterRUSignalSink{ch: make(chan *ReportData, 1)}
	require.NoError(t, tsr.Register(sink))

	go tsr.collectRUWorker()
	go tsr.reportWorker()

	tsr.CollectRUIncrements(stmtstats.RUIncrementMap{
		{
			User:       "root",
			SQLDigest:  stmtstats.BinaryDigest("sql-flow"),
			PlanDigest: stmtstats.BinaryDigest("plan-flow"),
		}: {
			TotalRU:      123,
			ExecCount:    2,
			ExecDuration: 200,
		},
	})

	require.Eventually(t, func() bool {
		tsr.ruAggregator.mu.Lock()
		defer tsr.ruAggregator.mu.Unlock()
		return len(tsr.ruAggregator.buckets) > 0
	}, time.Second, 10*time.Millisecond)

	atomic.StoreInt64(&currentUnix, 60)
	tsr.takeDataAndSendToReportChan(60)

	select {
	case payload := <-sink.ch:
		require.NotEmpty(t, payload.RURecords)
		rec := findRURecordByDigest(payload.RURecords, "root", "sql-flow", "plan-flow")
		require.NotNil(t, rec)
		require.Len(t, rec.Items, 1)
		require.InDelta(t, 123.0, rec.Items[0].TotalRu, 1e-9)
		require.Equal(t, uint64(2), rec.Items[0].ExecCount)
		require.Equal(t, uint64(200), rec.Items[0].ExecDuration)
		require.Equal(t, []byte("ks-flow"), rec.KeyspaceName)
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for TopRU report payload")
	}
}
