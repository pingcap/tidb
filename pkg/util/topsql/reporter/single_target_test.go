// Copyright 2022 PingCAP, Inc.
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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	reporter_metrics "github.com/pingcap/tidb/pkg/util/topsql/reporter/metrics"
	"github.com/pingcap/tidb/pkg/util/topsql/reporter/mock"
	"github.com/pingcap/tipb/go-tipb"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
)

type mockSingleTargetDataSinkRegisterer struct{}

func (r *mockSingleTargetDataSinkRegisterer) Register(dataSink DataSink) error { return nil }

func (r *mockSingleTargetDataSinkRegisterer) Deregister(dataSink DataSink) {}

func TestSingleTargetDataSink(t *testing.T) {
	server, err := mock.StartMockAgentServer()
	assert.NoError(t, err)
	defer server.Stop()

	config.UpdateGlobal(func(conf *config.Config) {
		conf.TopSQL.ReceiverAddress = server.Address()
	})

	ds := NewSingleTargetDataSink(&mockSingleTargetDataSinkRegisterer{})
	ds.Start()
	defer ds.Close()

	recordsCnt := server.RecordsCnt()
	sqlMetaCnt := server.SQLMetaCnt()

	err = ds.TrySend(&ReportData{
		DataRecords: []tipb.TopSQLRecord{{
			SqlDigest:  []byte("S1"),
			PlanDigest: []byte("P1"),
			Items: []*tipb.TopSQLRecordItem{{
				TimestampSec:      1,
				CpuTimeMs:         1,
				StmtExecCount:     1,
				StmtKvExecCount:   map[string]uint64{"": 1},
				StmtDurationSumNs: 1,
			}},
		}},
		SQLMetas: []tipb.SQLMeta{{
			SqlDigest:     []byte("S1"),
			NormalizedSql: "SQL-1",
		}},
		PlanMetas: []tipb.PlanMeta{{
			PlanDigest:     []byte("P1"),
			NormalizedPlan: "PLAN-1",
		}},
	}, time.Now().Add(10*time.Second))
	assert.NoError(t, err)

	server.WaitCollectCnt(recordsCnt, 1, 5*time.Second)
	server.WaitCollectCntOfSQLMeta(sqlMetaCnt, 1, 5*time.Second)

	assert.Len(t, server.GetLatestRecords(), 1)
	assert.Len(t, server.GetTotalSQLMetas(), 1)
	sqlMeta, exist := server.GetSQLMetaByDigestBlocking([]byte("S1"), 5*time.Second)
	assert.True(t, exist)
	assert.Equal(t, sqlMeta.NormalizedSql, "SQL-1")
	normalizedPlan, exist := server.GetPlanMetaByDigestBlocking([]byte("P1"), 5*time.Second)
	assert.True(t, exist)
	assert.Equal(t, normalizedPlan, "PLAN-1")

	ds.Close()
	t.Run("recovers send panics", func(t *testing.T) {
		ds := NewSingleTargetDataSink(&mockSingleTargetDataSinkRegisterer{})
		t.Cleanup(func() {
			ds.Close()
			if ds.conn != nil {
				require.NoError(t, ds.conn.Close())
			}
		})
		task := sendTask{data: &ReportData{}, deadline: time.Now().Add(5 * time.Second)}
		runDoSend := func() {
			done := make(chan struct{})
			go func() {
				ds.doSend(server.Address(), task)
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("single-target send did not finish")
			}
		}

		panicPath := "github.com/pingcap/tidb/pkg/util/topsql/reporter/mockSingleTargetSendPanic"
		t.Cleanup(func() { _ = failpoint.Disable(panicPath) })
		failedBefore := histogramSampleCount(t, reporter_metrics.ReportAllDurationFailedHistogram)
		succeededBefore := histogramSampleCount(t, reporter_metrics.ReportAllDurationSuccHistogram)
		require.NoError(t, failpoint.Enable(panicPath, "panic"))
		runDoSend()
		require.Equal(t, failedBefore+1, histogramSampleCount(t, reporter_metrics.ReportAllDurationFailedHistogram))
		require.Equal(t, succeededBefore, histogramSampleCount(t, reporter_metrics.ReportAllDurationSuccHistogram))
		require.NoError(t, failpoint.Disable(panicPath))
		runDoSend()
		require.Equal(t, failedBefore+1, histogramSampleCount(t, reporter_metrics.ReportAllDurationFailedHistogram))
		require.Equal(t, succeededBefore+1, histogramSampleCount(t, reporter_metrics.ReportAllDurationSuccHistogram))
	})
}

func histogramSampleCount(t *testing.T, observer any) uint64 {
	t.Helper()
	metric, ok := observer.(interface{ Write(*dto.Metric) error })
	require.True(t, ok)
	pb := &dto.Metric{}
	require.NoError(t, metric.Write(pb))
	return pb.GetHistogram().GetSampleCount()
}
