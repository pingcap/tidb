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
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/util/topsql/stmtstats"
	"github.com/stretchr/testify/require"
)

type structuredSignalSink struct {
	ch chan *ReportData
}

func (s *structuredSignalSink) TrySend(data *ReportData, _ time.Time) error {
	s.ch <- data
	return nil
}

func (s *structuredSignalSink) OnReporterClosing() {}

func TestTopRUReporter_MockDataSinkStructured(t *testing.T) {
	tsr := NewRemoteTopSQLReporter(mockPlanBinaryDecoderFunc, mockPlanBinaryCompressFunc)
	t.Cleanup(tsr.Close)

	sink := &structuredSignalSink{ch: make(chan *ReportData, 1)}
	require.NoError(t, tsr.Register(sink))

	keyspace := []byte("topru-test")
	sqlDigest := []byte("S1")
	planDigest := []byte("P1")
	tsr.RegisterSQL(sqlDigest, "select * from t where a = ?", false)
	tsr.RegisterPlan(planDigest, "point_get", false)

	ts := uint64(1700000000)
	tsr.ruAggregator.addBatchToBucket(ts, stmtstats.RUIncrementMap{
		{
			User:       "root",
			SQLDigest:  stmtstats.BinaryDigest(sqlDigest),
			PlanDigest: stmtstats.BinaryDigest(planDigest),
		}: &stmtstats.RUIncrement{
			TotalRU:      3.5,
			ExecCount:    1,
			ExecDuration: 2000,
		},
	})

	reportTs := alignToInterval(ts, ruReportWindowSeconds) + ruReportWindowSeconds
	tsr.doReport(&ReportData{
		RURecords: tsr.ruAggregator.takeReportRecords(reportTs, 60, keyspace),
		SQLMetas:  tsr.normalizedSQLMap.take().toProto(keyspace),
		PlanMetas: tsr.normalizedPlanMap.take().toProto(
			keyspace, tsr.decodePlan, tsr.compressPlan,
		),
	})

	select {
	case payload := <-sink.ch:
		require.NotEmpty(t, payload.RURecords)
		require.NotEmpty(t, payload.SQLMetas)
		require.NotEmpty(t, payload.PlanMetas)

		var matched bool
		for _, rec := range payload.RURecords {
			if rec.User != "root" || string(rec.SqlDigest) != "S1" || string(rec.PlanDigest) != "P1" {
				continue
			}
			require.NotEmpty(t, rec.Items)
			require.Greater(t, rec.Items[0].TotalRu, 0.0)
			require.GreaterOrEqual(t, rec.Items[0].ExecCount, uint64(1))
			matched = true
		}
		require.True(t, matched, "missing expected TopRU record")

		_, ok := findSQLMeta(payload.SQLMetas, sqlDigest)
		require.True(t, ok, "missing SQLMeta for TopRU digest")
		_, ok = findPlanMeta(payload.PlanMetas, planDigest)
		require.True(t, ok, "missing PlanMeta for TopRU digest")
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for reporter payload")
	}
}
