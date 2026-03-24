// Copyright 2021 PingCAP, Inc.
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
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/util/topsql/collector"
	reporter_metrics "github.com/pingcap/tidb/pkg/util/topsql/reporter/metrics"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"github.com/pingcap/tidb/pkg/util/topsql/stmtstats"
	"github.com/pingcap/tipb/go-tipb"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	maxSQLNum             = 5000
	defaultReportInterval = int(topsqlstate.DefTiDBTopSQLReportIntervalSeconds)
)

var keyspaceName = []byte("123")

func populateCache(tsr *RemoteTopSQLReporter, begin, end int, timestamp uint64) {
	// register normalized sql
	for i := begin; i < end; i++ {
		key := []byte("sqlDigest" + strconv.Itoa(i+1))
		value := "sqlNormalized" + strconv.Itoa(i+1)
		tsr.RegisterSQL(key, value, false)
	}
	// register normalized plan
	for i := begin; i < end; i++ {
		key := []byte("planDigest" + strconv.Itoa(i+1))
		value := "planNormalized" + strconv.Itoa(i+1)
		tsr.RegisterPlan(key, value, false)
	}
	// collect
	var records []collector.SQLCPUTimeRecord
	for i := begin; i < end; i++ {
		records = append(records, collector.SQLCPUTimeRecord{
			SQLDigest:  []byte("sqlDigest" + strconv.Itoa(i+1)),
			PlanDigest: []byte("planDigest" + strconv.Itoa(i+1)),
			CPUTimeMs:  uint32(i + 1),
		})
	}
	tsr.processCPUTimeData(timestamp, records)
	reportCache(tsr)
}

func reportCache(tsr *RemoteTopSQLReporter) {
	tsr.doReport(&ReportData{
		DataRecords: tsr.collecting.take().getReportRecords().toProto(keyspaceName),
		SQLMetas:    tsr.normalizedSQLMap.take().toProto(keyspaceName),
		PlanMetas:   tsr.normalizedPlanMap.take().toProto(keyspaceName, tsr.decodePlan, tsr.compressPlan),
	})
}

func reportCacheWithRU(tsr *RemoteTopSQLReporter, ruRecords []tipb.TopRURecord) {
	tsr.doReport(&ReportData{
		DataRecords: tsr.collecting.take().getReportRecords().toProto(keyspaceName),
		RURecords:   ruRecords,
		SQLMetas:    tsr.normalizedSQLMap.take().toProto(keyspaceName),
		PlanMetas:   tsr.normalizedPlanMap.take().toProto(keyspaceName, tsr.decodePlan, tsr.compressPlan),
	})
}

func mockPlanBinaryDecoderFunc(plan string) (string, error) {
	return plan, nil
}

func mockPlanBinaryCompressFunc(plan []byte) string {
	return string(plan)
}

type mockDataSink struct {
	ch chan *ReportData
}

func newMockDataSink(ch chan *ReportData) DataSink {
	return &mockDataSink{ch: ch}
}

var _ DataSink = &mockDataSink{}

func (ds *mockDataSink) TrySend(data *ReportData, _ time.Time) error {
	ds.ch <- data
	return nil
}

func (ds *mockDataSink) OnReporterClosing() {
}

func setupRemoteTopSQLReporter(tb testing.TB, maxStatementsNum, interval int) (*RemoteTopSQLReporter, *mockDataSink2) {
	tb.Helper()

	topsqlstate.GlobalState.MaxStatementCount.Store(int64(maxStatementsNum))
	topsqlstate.GlobalState.MaxCollect.Store(10000)
	restoreTicker := SetReportTickerIntervalSecondsForTest(interval)
	tb.Cleanup(restoreTicker)
	topsqlstate.EnableTopSQL()
	ts := NewRemoteTopSQLReporter(mockPlanBinaryDecoderFunc, mockPlanBinaryCompressFunc)
	ds := newMockDataSink2()
	if err := ts.Register(ds); err != nil {
		panic(err)
	}
	return ts, ds
}

func findSQLMeta(metas []tipb.SQLMeta, sqlDigest []byte) (*tipb.SQLMeta, bool) {
	for _, m := range metas {
		if string(m.SqlDigest) == string(sqlDigest) {
			return &m, true
		}
	}
	return nil, false
}

func findPlanMeta(metas []tipb.PlanMeta, planDigest []byte) (*tipb.PlanMeta, bool) {
	for _, m := range metas {
		if string(m.PlanDigest) == string(planDigest) {
			return &m, true
		}
	}
	return nil, false
}

func TestDoReportSendsMetaWhenRURecordsEmpty(t *testing.T) {
	tsr := NewRemoteTopSQLReporter(mockPlanBinaryDecoderFunc, mockPlanBinaryCompressFunc)
	t.Cleanup(tsr.Close)

	ch := make(chan *ReportData, 1)
	require.NoError(t, tsr.Register(newMockDataSink(ch)))

	input := &ReportData{
		SQLMetas: []tipb.SQLMeta{{
			SqlDigest:     []byte("S_meta_only"),
			NormalizedSql: "select /* meta only */ 1",
		}},
		PlanMetas: []tipb.PlanMeta{{
			PlanDigest:     []byte("P_meta_only"),
			NormalizedPlan: "Point_Get",
		}},
	}
	tsr.doReport(input)

	select {
	case payload := <-ch:
		require.Empty(t, payload.RURecords)
		require.Empty(t, payload.DataRecords)
		require.Len(t, payload.SQLMetas, 1)
		require.Len(t, payload.PlanMetas, 1)
		require.Equal(t, []byte("S_meta_only"), payload.SQLMetas[0].SqlDigest)
		require.Equal(t, "select /* meta only */ 1", payload.SQLMetas[0].NormalizedSql)
		require.Equal(t, []byte("P_meta_only"), payload.PlanMetas[0].PlanDigest)
		require.Equal(t, "Point_Get", payload.PlanMetas[0].NormalizedPlan)
	case <-time.After(time.Second):
		t.Fatal("meta-only payload should still be reported")
	}
}

func TestCollectAndSendBatch(t *testing.T) {
	tsr, ds := setupRemoteTopSQLReporter(t, maxSQLNum, 1)
	populateCache(tsr, 0, maxSQLNum, 1)
	require.Len(t, ds.data, 1)
	data := ds.data[0]
	require.Len(t, data.DataRecords, maxSQLNum)

	// check for equality of server received batch and the original data
	for _, req := range data.DataRecords {
		id := 0
		prefix := "sqlDigest"
		if strings.HasPrefix(string(req.SqlDigest), prefix) {
			n, err := strconv.Atoi(string(req.SqlDigest)[len(prefix):])
			require.NoError(t, err)
			id = n
		}
		require.Len(t, req.Items, 1)
		for i := range req.Items {
			require.Equal(t, uint64(1), req.Items[i].TimestampSec)
			require.Equal(t, uint32(id), req.Items[i].CpuTimeMs)
		}
		sqlMeta, exist := findSQLMeta(data.SQLMetas, req.SqlDigest)
		require.True(t, exist)
		require.Equal(t, keyspaceName, sqlMeta.KeyspaceName)
		require.Equal(t, "sqlNormalized"+strconv.Itoa(id), sqlMeta.NormalizedSql)
		planMeta, exist := findPlanMeta(data.PlanMetas, req.PlanDigest)
		require.True(t, exist)
		require.Equal(t, keyspaceName, sqlMeta.KeyspaceName)
		require.Equal(t, "planNormalized"+strconv.Itoa(id), planMeta.NormalizedPlan)
	}
}

func TestCollectAndEvicted(t *testing.T) {
	tsr, ds := setupRemoteTopSQLReporter(t, maxSQLNum, 1)
	populateCache(tsr, 0, maxSQLNum*2, 2)
	require.Len(t, ds.data, 1)
	data := ds.data[0]
	require.Len(t, data.DataRecords, maxSQLNum+1)

	// check for equality of server received batch and the original data
	for _, req := range data.DataRecords {
		id := 0
		prefix := "sqlDigest"
		if strings.HasPrefix(string(req.SqlDigest), prefix) {
			n, err := strconv.Atoi(string(req.SqlDigest)[len(prefix):])
			require.NoError(t, err)
			id = n
		}
		require.Len(t, req.Items, 1)
		require.Equal(t, uint64(2), req.Items[0].TimestampSec)
		if id == 0 {
			// test for others
			require.Nil(t, req.SqlDigest)
			require.Nil(t, req.PlanDigest)
			// 12502500 is the sum of all evicted item's cpu time. 1 + 2 + 3 + ... + 5000 = (1 + 5000) * 2500 = 12502500
			require.Equal(t, 12502500, int(req.Items[0].CpuTimeMs))
			continue
		}
		require.Greater(t, id, maxSQLNum)
		require.Equal(t, uint32(id), req.Items[0].CpuTimeMs)
		sqlMeta, exist := findSQLMeta(data.SQLMetas, req.SqlDigest)
		require.True(t, exist)
		require.Equal(t, keyspaceName, sqlMeta.KeyspaceName)
		require.Equal(t, "sqlNormalized"+strconv.Itoa(id), sqlMeta.NormalizedSql)
		planMeta, exist := findPlanMeta(data.PlanMetas, req.PlanDigest)
		require.True(t, exist)
		require.Equal(t, keyspaceName, sqlMeta.KeyspaceName)
		require.Equal(t, "planNormalized"+strconv.Itoa(id), planMeta.NormalizedPlan)
	}
}

func TestTopRUItemIntervalLifecycleIndependentFromFixedReportTicker(t *testing.T) {
	// Contract: TopRU interval lifecycle is independent from fixed TopSQL report ticker.
	topsqlstate.DisableTopSQL()
	for topsqlstate.TopRUEnabled() {
		topsqlstate.DisableTopRU()
	}
	topsqlstate.ResetTopRUItemInterval()
	t.Cleanup(func() {
		for topsqlstate.TopRUEnabled() {
			topsqlstate.DisableTopRU()
		}
		topsqlstate.ResetTopRUItemInterval()
	})

	require.Equal(t, int64(topsqlstate.DefTiDBTopRUItemIntervalSeconds), topsqlstate.GetTopRUItemInterval())
	require.NoError(t, topsqlstate.SetTopRUItemInterval(tipb.ItemInterval_ITEM_INTERVAL_15S))
	require.Equal(t, int64(tipb.ItemInterval_ITEM_INTERVAL_15S), topsqlstate.GetTopRUItemInterval())

	topsqlstate.EnableTopRU()
	require.NoError(t, topsqlstate.SetTopRUItemInterval(tipb.ItemInterval_ITEM_INTERVAL_30S))
	require.Equal(t, int64(tipb.ItemInterval_ITEM_INTERVAL_30S), topsqlstate.GetTopRUItemInterval())

	require.Error(t, topsqlstate.SetTopRUItemInterval(tipb.ItemInterval(1)))
	require.Equal(t, int64(tipb.ItemInterval_ITEM_INTERVAL_30S), topsqlstate.GetTopRUItemInterval())

	require.NoError(t, topsqlstate.SetTopRUItemInterval(tipb.ItemInterval_ITEM_INTERVAL_UNSPECIFIED))
	require.Equal(t, int64(topsqlstate.DefTiDBTopRUItemIntervalSeconds), topsqlstate.GetTopRUItemInterval())

	for topsqlstate.TopRUEnabled() {
		topsqlstate.DisableTopRU()
	}
}

func newSQLCPUTimeRecord(tsr *RemoteTopSQLReporter, sqlID int, cpuTimeMs uint32) collector.SQLCPUTimeRecord {
	key := []byte("sqlDigest" + strconv.Itoa(sqlID))
	value := "sqlNormalized" + strconv.Itoa(sqlID)
	tsr.RegisterSQL(key, value, sqlID%2 == 0)

	key = []byte("planDigest" + strconv.Itoa(sqlID))
	value = "planNormalized" + strconv.Itoa(sqlID)
	tsr.RegisterPlan(key, value, false)

	return collector.SQLCPUTimeRecord{
		SQLDigest:  []byte("sqlDigest" + strconv.Itoa(sqlID)),
		PlanDigest: []byte("planDigest" + strconv.Itoa(sqlID)),
		CPUTimeMs:  cpuTimeMs,
	}
}

func TestCollectAndTopN(t *testing.T) {
	tsr, ds := setupRemoteTopSQLReporter(t, 2, 1)

	records := []collector.SQLCPUTimeRecord{
		newSQLCPUTimeRecord(tsr, 1, 1),
		newSQLCPUTimeRecord(tsr, 2, 2),
		newSQLCPUTimeRecord(tsr, 3, 3),
	}
	// SQL-2:  2ms
	// SQL-3:  3ms
	// Others: 1ms
	tsr.processCPUTimeData(1, records)

	records = []collector.SQLCPUTimeRecord{
		newSQLCPUTimeRecord(tsr, 1, 1),
		newSQLCPUTimeRecord(tsr, 3, 3),
	}
	// SQL-1:  1ms
	// SQL-3:  3ms
	tsr.processCPUTimeData(2, records)

	records = []collector.SQLCPUTimeRecord{
		newSQLCPUTimeRecord(tsr, 4, 4),
		newSQLCPUTimeRecord(tsr, 1, 10),
		newSQLCPUTimeRecord(tsr, 3, 1),
	}
	// SQL-1:  10ms
	// SQL-4:  4ms
	// Others: 1ms
	tsr.processCPUTimeData(3, records)

	records = []collector.SQLCPUTimeRecord{
		newSQLCPUTimeRecord(tsr, 5, 5),
		newSQLCPUTimeRecord(tsr, 4, 4),
		newSQLCPUTimeRecord(tsr, 1, 10),
		newSQLCPUTimeRecord(tsr, 2, 20),
	}
	// SQL-2:  20ms
	// SQL-1:  1ms
	// Others: 9ms
	tsr.processCPUTimeData(4, records)

	// Test for time jump back.
	records = []collector.SQLCPUTimeRecord{
		newSQLCPUTimeRecord(tsr, 6, 6),
		newSQLCPUTimeRecord(tsr, 1, 1),
		newSQLCPUTimeRecord(tsr, 2, 2),
		newSQLCPUTimeRecord(tsr, 3, 3),
	}
	// SQL-6:  6ms
	// SQL-3:  3ms
	// Others: 3ms
	tsr.processCPUTimeData(0, records)

	reportCache(tsr)

	// check for equality of server received batch and the original data
	require.Len(t, ds.data, 1)
	results := ds.data[0].DataRecords
	// Digest  total
	// "":     14ms    (others)
	// SQL-1:  21ms
	// SQL-2:  22ms
	// SQL-3:  9ms
	// SQL-4:  4ms
	// SQL-6:  6ms
	require.Len(t, results, 6)
	sort.Slice(results, func(i, j int) bool {
		return string(results[i].SqlDigest) < string(results[j].SqlDigest)
	})
	getTotalCPUTime := func(record tipb.TopSQLRecord) int {
		total := uint32(0)
		for _, i := range record.Items {
			total += i.CpuTimeMs
		}
		return int(total)
	}
	require.Nil(t, results[0].SqlDigest)
	require.Equal(t, []byte(nil), results[0].SqlDigest)
	require.Equal(t, 14, getTotalCPUTime(results[0]))
	require.Equal(t, uint64(1), results[0].Items[0].TimestampSec)
	require.Equal(t, uint64(3), results[0].Items[1].TimestampSec)
	require.Equal(t, uint64(4), results[0].Items[2].TimestampSec)
	require.Equal(t, uint64(0), results[0].Items[3].TimestampSec)
	require.Equal(t, uint32(1), results[0].Items[0].CpuTimeMs)
	require.Equal(t, uint32(1), results[0].Items[1].CpuTimeMs)
	require.Equal(t, uint32(9), results[0].Items[2].CpuTimeMs)
	require.Equal(t, uint32(3), results[0].Items[3].CpuTimeMs)
	require.Equal(t, []byte("sqlDigest1"), results[1].SqlDigest)
	require.Equal(t, 21, getTotalCPUTime(results[1]))
	require.Equal(t, []byte("sqlDigest2"), results[2].SqlDigest)
	require.Equal(t, 22, getTotalCPUTime(results[2]))
	require.Equal(t, []byte("sqlDigest3"), results[3].SqlDigest)
	require.Equal(t, 9, getTotalCPUTime(results[3]))
	require.Equal(t, []byte("sqlDigest4"), results[4].SqlDigest)
	require.Equal(t, 4, getTotalCPUTime(results[4]))
	require.Equal(t, []byte("sqlDigest6"), results[5].SqlDigest)
	require.Equal(t, 6, getTotalCPUTime(results[5]))
	require.Equal(t, 6, len(ds.data[0].SQLMetas))
	require.Equal(t, keyspaceName, results[0].KeyspaceName)
	require.Equal(t, keyspaceName, results[1].KeyspaceName)
	require.Equal(t, keyspaceName, results[2].KeyspaceName)
	require.Equal(t, keyspaceName, results[3].KeyspaceName)
	require.Equal(t, keyspaceName, results[4].KeyspaceName)
	require.Equal(t, keyspaceName, results[5].KeyspaceName)
}

func TestCollectCapacity(t *testing.T) {
	tsr, _ := setupRemoteTopSQLReporter(t, maxSQLNum, defaultReportInterval)
	registerSQL := func(n int) {
		for i := range n {
			key := []byte("sqlDigest" + strconv.Itoa(i))
			value := "sqlNormalized" + strconv.Itoa(i)
			tsr.RegisterSQL(key, value, false)
		}
	}
	registerPlan := func(n int) {
		for i := range n {
			key := []byte("planDigest" + strconv.Itoa(i))
			value := "planNormalized" + strconv.Itoa(i)
			tsr.RegisterPlan(key, value, false)
		}
	}
	genRecord := func(n int) []collector.SQLCPUTimeRecord {
		records := make([]collector.SQLCPUTimeRecord, 0, n)
		for i := range n {
			records = append(records, collector.SQLCPUTimeRecord{
				SQLDigest:  []byte("sqlDigest" + strconv.Itoa(i+1)),
				PlanDigest: []byte("planDigest" + strconv.Itoa(i+1)),
				CPUTimeMs:  uint32(i + 1),
			})
		}
		return records
	}

	topsqlstate.GlobalState.MaxCollect.Store(10000)
	registerSQL(5000)
	require.Equal(t, int64(5000), tsr.normalizedSQLMap.length.Load())
	registerPlan(1000)
	require.Equal(t, int64(1000), tsr.normalizedPlanMap.length.Load())

	registerSQL(20000)
	require.Equal(t, int64(10000), tsr.normalizedSQLMap.length.Load())
	registerPlan(20000)
	require.Equal(t, int64(10000), tsr.normalizedPlanMap.length.Load())

	topsqlstate.GlobalState.MaxCollect.Store(20000)
	registerSQL(50000)
	require.Equal(t, int64(20000), tsr.normalizedSQLMap.length.Load())
	registerPlan(50000)
	require.Equal(t, int64(20000), tsr.normalizedPlanMap.length.Load())

	topsqlstate.GlobalState.MaxStatementCount.Store(5000)
	tsr.processCPUTimeData(1, genRecord(20000))
	require.Equal(t, 5001, len(tsr.collecting.records))
	require.Equal(t, int64(20000), tsr.normalizedSQLMap.length.Load())
	require.Equal(t, int64(20000), tsr.normalizedPlanMap.length.Load())
}

func TestCollectInternal(t *testing.T) {
	tsr, ds := setupRemoteTopSQLReporter(t, 3000, 1)

	records := []collector.SQLCPUTimeRecord{
		newSQLCPUTimeRecord(tsr, 1, 1),
		newSQLCPUTimeRecord(tsr, 2, 2),
	}
	tsr.processCPUTimeData(1, records)

	reportCache(tsr)

	// check for equality of server received batch and the original data
	require.Len(t, ds.data, 1)
	data := ds.data[0]
	results := data.DataRecords
	require.Len(t, results, 2)
	for _, req := range results {
		id := 0
		prefix := "sqlDigest"
		if strings.HasPrefix(string(req.SqlDigest), prefix) {
			n, err := strconv.Atoi(string(req.SqlDigest)[len(prefix):])
			require.NoError(t, err)
			id = n
		}
		require.NotEqualf(t, 0, id, "the id should not be 0")
		sqlMeta, exist := findSQLMeta(data.SQLMetas, req.SqlDigest)
		require.True(t, exist)
		require.Equal(t, id%2 == 0, sqlMeta.IsInternalSql)
		require.Equal(t, keyspaceName, sqlMeta.KeyspaceName)
	}
}

func TestMultipleDataSinks(t *testing.T) {
	restoreTicker := SetReportTickerIntervalSecondsForTest(1)
	t.Cleanup(restoreTicker)
	topsqlstate.EnableTopSQL()

	tsr := NewRemoteTopSQLReporter(mockPlanBinaryDecoderFunc, mockPlanBinaryCompressFunc)

	chs := make([]chan *ReportData, 0, 7)
	for range 7 {
		chs = append(chs, make(chan *ReportData, 1))
	}
	dss := make([]DataSink, 0, len(chs))
	for _, ch := range chs {
		dss = append(dss, newMockDataSink(ch))
	}
	for _, ds := range dss {
		require.NoError(t, tsr.Register(ds))
	}

	records := []collector.SQLCPUTimeRecord{
		newSQLCPUTimeRecord(tsr, 1, 2),
	}
	tsr.processCPUTimeData(3, records)
	reportCache(tsr)

	for _, ch := range chs {
		d := <-ch
		require.NotNil(t, d)
		require.Len(t, d.DataRecords, 1)
		require.Equal(t, keyspaceName, d.DataRecords[0].KeyspaceName)
		require.Equal(t, []byte("sqlDigest1"), d.DataRecords[0].SqlDigest)
		require.Equal(t, []byte("planDigest1"), d.DataRecords[0].PlanDigest)
		require.Len(t, d.DataRecords[0].Items, 1)
		require.Equal(t, uint64(3), d.DataRecords[0].Items[0].TimestampSec)
		require.Equal(t, uint32(2), d.DataRecords[0].Items[0].CpuTimeMs)

		require.Equal(t, []tipb.SQLMeta{{
			KeyspaceName:  keyspaceName,
			SqlDigest:     []byte("sqlDigest1"),
			NormalizedSql: "sqlNormalized1",
		}}, d.SQLMetas)

		require.Equal(t, []tipb.PlanMeta{{
			KeyspaceName:   keyspaceName,
			PlanDigest:     []byte("planDigest1"),
			NormalizedPlan: "planNormalized1",
		}}, d.PlanMetas)
	}

	// deregister half of dataSinks
	for i := 0; i < 7; i += 2 {
		tsr.Deregister(dss[i])
	}

	records = []collector.SQLCPUTimeRecord{
		newSQLCPUTimeRecord(tsr, 4, 5),
	}
	tsr.processCPUTimeData(6, records)
	reportCache(tsr)

	for i := 1; i < 7; i += 2 {
		d := <-chs[i]
		require.NotNil(t, d)
		require.Len(t, d.DataRecords, 1)
		require.Equal(t, keyspaceName, d.DataRecords[0].KeyspaceName)
		require.Equal(t, []byte("sqlDigest4"), d.DataRecords[0].SqlDigest)
		require.Equal(t, []byte("planDigest4"), d.DataRecords[0].PlanDigest)
		require.Len(t, d.DataRecords[0].Items, 1)
		require.Equal(t, uint64(6), d.DataRecords[0].Items[0].TimestampSec)
		require.Equal(t, uint32(5), d.DataRecords[0].Items[0].CpuTimeMs)

		require.Equal(t, []tipb.SQLMeta{{
			KeyspaceName:  keyspaceName,
			SqlDigest:     []byte("sqlDigest4"),
			NormalizedSql: "sqlNormalized4",
			IsInternalSql: true,
		}}, d.SQLMetas)

		require.Equal(t, []tipb.PlanMeta{{
			KeyspaceName:   keyspaceName,
			PlanDigest:     []byte("planDigest4"),
			NormalizedPlan: "planNormalized4",
		}}, d.PlanMetas)
	}

	for i := 0; i < 7; i += 2 {
		select {
		case <-chs[i]:
			require.Fail(t, "unexpected to receive messages")
		default:
		}
	}
}

func TestReporterWorker(t *testing.T) {
	restoreTicker := SetReportTickerIntervalSecondsForTest(3)
	t.Cleanup(restoreTicker)

	r := NewRemoteTopSQLReporter(mockPlanBinaryDecoderFunc, mockPlanBinaryCompressFunc)
	r.Start()
	defer r.Close()

	ch := make(chan *ReportData, 1)
	ds := newMockDataSink(ch)
	err := r.Register(ds)
	assert.NoError(t, err)

	r.Collect(nil)
	r.Collect([]collector.SQLCPUTimeRecord{{
		SQLDigest:  []byte("S1"),
		PlanDigest: []byte("P1"),
		CPUTimeMs:  1,
	}})
	r.CollectStmtStatsMap(nil)
	r.CollectStmtStatsMap(stmtstats.StatementStatsMap{
		stmtstats.SQLPlanDigest{
			SQLDigest:  "S1",
			PlanDigest: "P1",
		}: &stmtstats.StatementStatsItem{
			ExecCount:     1,
			SumDurationNs: 1,
			KvStatsItem:   stmtstats.KvStatementStatsItem{KvExecCount: map[string]uint64{"": 1}},
		},
	})

	var data *ReportData
	select {
	case data = <-ch:
	case <-time.After(5 * time.Second):
		require.Fail(t, "no data in ch")
	}

	assert.Len(t, data.DataRecords, 1)
	assert.Equal(t, []byte("S1"), data.DataRecords[0].SqlDigest)
	assert.Equal(t, []byte("P1"), data.DataRecords[0].PlanDigest)
}

// TestReporterChannelsFullDropsAndMetrics covers the backpressure risk path:
// when internal channels are full, reporter should drop fast (no panic/hang)
// and account drops via the corresponding metrics counters.
func TestReporterChannelsFullDropsAndMetrics(t *testing.T) {
	tsr := NewRemoteTopSQLReporter(mockPlanBinaryDecoderFunc, mockPlanBinaryCompressFunc)
	t.Cleanup(tsr.Close)

	ruData := stmtstats.RUIncrementMap{
		stmtstats.RUKey{
			User:       "u1",
			SQLDigest:  stmtstats.BinaryDigest("sql1"),
			PlanDigest: stmtstats.BinaryDigest("plan1"),
		}: &stmtstats.RUIncrement{
			TotalRU:      1,
			ExecCount:    1,
			ExecDuration: 1,
		},
	}

	beforeCollectRUDrop := readCounter(t, reporter_metrics.IgnoreCollectRUChannelFullCounter)
	tsr.collectRUIncrementsChan <- ruBatch{timestamp: uint64(nowFunc().Unix()), data: ruData}
	tsr.collectRUIncrementsChan <- ruBatch{timestamp: uint64(nowFunc().Unix()), data: ruData} // fill collectRUIncrementsChan (buffer=2)
	doneCollect := make(chan struct{})
	go func() {
		tsr.CollectRUIncrements(ruData) // should drop immediately when channel is full
		close(doneCollect)
	}()
	select {
	case <-doneCollect:
	case <-time.After(time.Second):
		t.Fatal("CollectRUIncrements should not block when collectRUIncrementsChan is full")
	}
	require.Equal(t, 2, len(tsr.collectRUIncrementsChan))
	require.InDelta(t, 1.0, readCounter(t, reporter_metrics.IgnoreCollectRUChannelFullCounter)-beforeCollectRUDrop, 1e-9)

	beforeReportDrop := readCounter(t, reporter_metrics.IgnoreReportChannelFullCounter)
	origInterval := topsqlstate.GetTopRUItemInterval()
	topsqlstate.SetTopRUItemInterval(tipb.ItemInterval_ITEM_INTERVAL_60S)
	t.Cleanup(func() {
		topsqlstate.SetTopRUItemInterval(tipb.ItemInterval(origInterval))
	})

	tsr.reportCollectedDataChan <- collectedData{} // fill reportCollectedDataChan (buffer=1)
	doneReport := make(chan struct{})
	go func() {
		tsr.takeDataAndSendToReportChan(60) // should drop immediately when report channel is full
		close(doneReport)
	}()
	select {
	case <-doneReport:
	case <-time.After(time.Second):
		t.Fatal("takeDataAndSendToReportChan should not block when reportCollectedDataChan is full")
	}
	require.Equal(t, 1, len(tsr.reportCollectedDataChan))
	require.InDelta(t, 1.0, readCounter(t, reporter_metrics.IgnoreReportChannelFullCounter)-beforeReportDrop, 1e-9)
}

// TestReporterBackpressureAndDropScenario covers the reporter backpressure
// scenario end-to-end: bounded drop under channel pressure, metrics observability,
// and recovery once consumer resumes.
func TestReporterBackpressureAndDropScenario(t *testing.T) {
	origNowFunc := nowFunc
	var currentUnix int64 = 1
	nowFunc = func() time.Time {
		return time.Unix(atomic.LoadInt64(&currentUnix), 0)
	}
	t.Cleanup(func() { nowFunc = origNowFunc })

	origInterval := topsqlstate.GetTopRUItemInterval()
	topsqlstate.SetTopRUItemInterval(tipb.ItemInterval_ITEM_INTERVAL_60S)
	t.Cleanup(func() {
		topsqlstate.SetTopRUItemInterval(tipb.ItemInterval(origInterval))
	})

	tsr := NewRemoteTopSQLReporter(mockPlanBinaryDecoderFunc, mockPlanBinaryCompressFunc)
	defer tsr.Close()
	sinkCh := make(chan *ReportData, 4)
	require.NoError(t, tsr.Register(newMockDataSink(sinkCh)))

	go tsr.collectWorker()

	sendBatch := func(ts int64, reportEnd uint64) {
		atomic.StoreInt64(&currentUnix, ts)
		tsr.CollectRUIncrements(stmtstats.RUIncrementMap{
			{
				User:       "bp-user",
				SQLDigest:  stmtstats.BinaryDigest("bp-sql"),
				PlanDigest: stmtstats.BinaryDigest("bp-plan"),
			}: {
				TotalRU:      float64(ts),
				ExecCount:    1,
				ExecDuration: 1,
			},
		})
		bucketStart := uint64(ts) - uint64(ts)%15
		require.Eventually(t, func() bool {
			tsr.ruAggregator.mu.Lock()
			defer tsr.ruAggregator.mu.Unlock()
			_, ok := tsr.ruAggregator.buckets[bucketStart]
			return ok
		}, time.Second, 10*time.Millisecond)
		done := make(chan struct{})
		go func() {
			tsr.takeDataAndSendToReportChan(reportEnd)
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("takeDataAndSendToReportChan should not block under backpressure")
		}
	}

	beforeDrop := readCounter(t, reporter_metrics.IgnoreReportChannelFullCounter)
	sendBatch(1, 60)   // fill reportCollectedDataChan (buffer=1)
	sendBatch(61, 120) // drop under backpressure (no report worker consuming yet)
	require.Equal(t, 1, len(tsr.reportCollectedDataChan))
	require.InDelta(t, 1.0, readCounter(t, reporter_metrics.IgnoreReportChannelFullCounter)-beforeDrop, 1e-9)

	reportWorkerDone := make(chan struct{})
	go func() {
		tsr.reportWorker()
		close(reportWorkerDone)
	}()

	var firstPayload *ReportData
	select {
	case firstPayload = <-sinkCh:
	case <-time.After(time.Second):
		t.Fatal("reporter should recover and send payload after consumer resumes")
	}
	require.NotNil(t, firstPayload)
	require.NotEmpty(t, firstPayload.RURecords)
	require.Empty(t, firstPayload.DataRecords, "TopSQL shared path should remain untouched in RU-only scenario")
	require.Eventually(t, func() bool {
		return len(tsr.reportCollectedDataChan) == 0
	}, time.Second, 10*time.Millisecond)

	dropAfterRecover := readCounter(t, reporter_metrics.IgnoreReportChannelFullCounter)
	sendBatch(121, 180)
	var secondPayload *ReportData
	select {
	case secondPayload = <-sinkCh:
	case <-time.After(time.Second):
		t.Fatal("reporter should continue working after backpressure recovery")
	}
	require.NotNil(t, secondPayload)
	require.NotEmpty(t, secondPayload.RURecords)
	require.InDelta(t, 0.0, readCounter(t, reporter_metrics.IgnoreReportChannelFullCounter)-dropAfterRecover, 1e-9)

	tsr.Close()
	select {
	case <-reportWorkerDone:
	case <-time.After(time.Second):
		t.Fatal("reportWorker should exit on reporter close")
	}
}

// TestTopRUPipelineInProcessIntegration covers the core in-process TopRU
// pipeline across modules: CollectRUIncrements -> worker/aggregation -> report
// payload, including same-key accumulation and no duplicate window emission.
func TestTopRUPipelineInProcessIntegration(t *testing.T) {
	origNowFunc := nowFunc
	var currentUnix int64 = 1
	nowFunc = func() time.Time {
		return time.Unix(atomic.LoadInt64(&currentUnix), 0)
	}
	t.Cleanup(func() { nowFunc = origNowFunc })

	tsr := NewRemoteTopSQLReporter(mockPlanBinaryDecoderFunc, mockPlanBinaryCompressFunc)
	t.Cleanup(tsr.Close)
	tsr.BindKeyspaceName([]byte("ks-pipeline"))

	ch := make(chan *ReportData, 2)
	require.NoError(t, tsr.Register(newMockDataSink(ch)))
	go tsr.collectWorker()
	go tsr.reportWorker()

	hotSQLDigest, hotPlanDigest := []byte("sql-hot"), []byte("plan-hot")
	coldSQLDigest, coldPlanDigest := []byte("sql-cold"), []byte("plan-cold")
	tsr.RegisterSQL(hotSQLDigest, "select /* hot */ 1", false)
	tsr.RegisterPlan(hotPlanDigest, "hot-plan", false)
	tsr.RegisterSQL(coldSQLDigest, "select /* cold */ 1", false)
	tsr.RegisterPlan(coldPlanDigest, "cold-plan", false)

	hotKey := stmtstats.RUKey{
		User:       "user-hot",
		SQLDigest:  stmtstats.BinaryDigest("sql-hot"),
		PlanDigest: stmtstats.BinaryDigest("plan-hot"),
	}
	coldKey := stmtstats.RUKey{
		User:       "user-cold",
		SQLDigest:  stmtstats.BinaryDigest("sql-cold"),
		PlanDigest: stmtstats.BinaryDigest("plan-cold"),
	}

	tsr.CollectRUIncrements(stmtstats.RUIncrementMap{
		hotKey: &stmtstats.RUIncrement{
			TotalRU:      10,
			ExecCount:    1,
			ExecDuration: 100,
		},
		coldKey: &stmtstats.RUIncrement{
			TotalRU:      3,
			ExecCount:    1,
			ExecDuration: 30,
		},
	})
	tsr.CollectRUIncrements(stmtstats.RUIncrementMap{
		hotKey: &stmtstats.RUIncrement{
			TotalRU:      7,
			ExecCount:    2,
			ExecDuration: 70,
		},
	})

	require.Eventually(t, func() bool {
		tsr.ruAggregator.mu.Lock()
		defer tsr.ruAggregator.mu.Unlock()
		return len(tsr.ruAggregator.buckets) > 0
	}, time.Second, 10*time.Millisecond)

	tsr.takeDataAndSendToReportChan(60)

	var payload *ReportData
	select {
	case payload = <-ch:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for TopRU pipeline report payload")
	}

	require.NotEmpty(t, payload.RURecords)
	hotRec := findRURecordByDigest(payload.RURecords, "user-hot", "sql-hot", "plan-hot")
	require.NotNil(t, hotRec)
	coldRec := findRURecordByDigest(payload.RURecords, "user-cold", "sql-cold", "plan-cold")
	require.NotNil(t, coldRec)

	hotTotalRU, hotExecCount := 0.0, uint64(0)
	for _, item := range hotRec.Items {
		hotTotalRU += item.TotalRu
		hotExecCount += item.ExecCount
	}
	require.InDelta(t, 17.0, hotTotalRU, 1e-9)
	require.Equal(t, uint64(3), hotExecCount)

	coldTotalRU := 0.0
	for _, item := range coldRec.Items {
		coldTotalRU += item.TotalRu
	}
	require.InDelta(t, 3.0, coldTotalRU, 1e-9)

	_, ok := findSQLMeta(payload.SQLMetas, hotSQLDigest)
	require.True(t, ok, "missing SQL meta for hot key")
	_, ok = findSQLMeta(payload.SQLMetas, coldSQLDigest)
	require.True(t, ok, "missing SQL meta for cold key")
	_, ok = findPlanMeta(payload.PlanMetas, hotPlanDigest)
	require.True(t, ok, "missing plan meta for hot key")
	_, ok = findPlanMeta(payload.PlanMetas, coldPlanDigest)
	require.True(t, ok, "missing plan meta for cold key")

	tsr.takeDataAndSendToReportChan(61)
	require.Never(t, func() bool {
		select {
		case <-ch:
			return true
		default:
			return false
		}
	}, 300*time.Millisecond, 10*time.Millisecond)
}

// TestTopRUPipelineGracefulShutdown covers shutdown risk: workers should exit
// cleanly without panic/hang, and unclosed RU tail data is not force-flushed.
func TestTopRUPipelineGracefulShutdown(t *testing.T) {
	origNowFunc := nowFunc
	var currentUnix int64 = 1
	nowFunc = func() time.Time {
		return time.Unix(atomic.LoadInt64(&currentUnix), 0)
	}
	t.Cleanup(func() { nowFunc = origNowFunc })

	tsr := NewRemoteTopSQLReporter(mockPlanBinaryDecoderFunc, mockPlanBinaryCompressFunc)
	ch := make(chan *ReportData, 1)
	require.NoError(t, tsr.Register(newMockDataSink(ch)))

	collectDone := make(chan struct{})
	reportDone := make(chan struct{})
	go func() {
		tsr.collectWorker()
		close(collectDone)
	}()
	go func() {
		tsr.reportWorker()
		close(reportDone)
	}()

	tsr.CollectRUIncrements(stmtstats.RUIncrementMap{
		{
			User:       "shutdown-user",
			SQLDigest:  stmtstats.BinaryDigest("sql-shutdown"),
			PlanDigest: stmtstats.BinaryDigest("plan-shutdown"),
		}: &stmtstats.RUIncrement{
			TotalRU:      5,
			ExecCount:    1,
			ExecDuration: 50,
		},
	})
	require.Eventually(t, func() bool {
		tsr.ruAggregator.mu.Lock()
		defer tsr.ruAggregator.mu.Unlock()
		return len(tsr.ruAggregator.buckets) > 0
	}, time.Second, 10*time.Millisecond)

	closeDone := make(chan struct{})
	go func() {
		tsr.Close()
		close(closeDone)
	}()

	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("reporter close should not hang")
	}
	select {
	case <-collectDone:
	case <-time.After(time.Second):
		t.Fatal("collectWorker should exit on close")
	}
	select {
	case <-reportDone:
	case <-time.After(time.Second):
		t.Fatal("reportWorker should exit on close")
	}

	select {
	case <-ch:
		t.Fatal("unexpected payload: shutdown should not flush unclosed TopRU tail window")
	default:
	}

	// Idempotent close should return promptly as well.
	closeDoneAgain := make(chan struct{})
	go func() {
		tsr.Close()
		close(closeDoneAgain)
	}()
	select {
	case <-closeDoneAgain:
	case <-time.After(time.Second):
		t.Fatal("second close should be non-blocking")
	}
}

func TestTopRUBestEffortBoundaryShift(t *testing.T) {
	origNowFunc := nowFunc
	var currentUnix int64 = 61
	nowFunc = func() time.Time {
		return time.Unix(atomic.LoadInt64(&currentUnix), 0)
	}
	t.Cleanup(func() { nowFunc = origNowFunc })

	tsr := NewRemoteTopSQLReporter(mockPlanBinaryDecoderFunc, mockPlanBinaryCompressFunc)
	t.Cleanup(tsr.Close)

	key := stmtstats.RUKey{
		User:       "u-best",
		SQLDigest:  stmtstats.BinaryDigest("sql-best"),
		PlanDigest: stmtstats.BinaryDigest("plan-best"),
	}
	tsr.CollectRUIncrements(stmtstats.RUIncrementMap{
		key: {
			TotalRU:      7,
			ExecCount:    1,
			ExecDuration: 1,
		},
	})
	// Drain queued RU into aggregator to make the attribution deterministic in UT.
	drainRUBatchesForTest(tsr)

	// Timeline:
	//   - report tick at 60 closes [0,60)
	//   - this RU batch is collected at t=61 (boundary-late arrival)
	// Contract:
	//   - best-effort only, no backfill into an already closed window.
	// So [0,60) must not contain the RU.
	first := tsr.ruAggregator.takeReportRecords(60, 60, []byte("ks"))
	require.Nil(t, findRURecordByDigest(first, "u-best", "sql-best", "plan-best"))

	// The same RU should appear in the next aligned closed window [60,120).
	second := tsr.ruAggregator.takeReportRecords(120, 60, []byte("ks"))
	rec := findRURecordByDigest(second, "u-best", "sql-best", "plan-best")
	require.NotNil(t, rec)
}

func drainRUBatchesForTest(tsr *RemoteTopSQLReporter) {
	for {
		select {
		case batch := <-tsr.collectRUIncrementsChan:
			if len(batch.data) == 0 {
				continue
			}
			tsr.ruAggregator.addBatchToBucket(batch.timestamp, batch.data)
		default:
			return
		}
	}
}

func initializeCache(tb testing.TB, maxStatementsNum, interval int) (*RemoteTopSQLReporter, *mockDataSink2) {
	tb.Helper()
	ts, ds := setupRemoteTopSQLReporter(tb, maxStatementsNum, interval)
	populateCache(ts, 0, maxStatementsNum, 1)
	return ts, ds
}

func populateCacheWithRU(tsr *RemoteTopSQLReporter, begin, end int, timestamp uint64, ruRecords []tipb.TopRURecord) {
	// register normalized sql
	for i := begin; i < end; i++ {
		key := []byte("sqlDigest" + strconv.Itoa(i+1))
		value := "sqlNormalized" + strconv.Itoa(i+1)
		tsr.RegisterSQL(key, value, false)
	}
	// register normalized plan
	for i := begin; i < end; i++ {
		key := []byte("planDigest" + strconv.Itoa(i+1))
		value := "planNormalized" + strconv.Itoa(i+1)
		tsr.RegisterPlan(key, value, false)
	}
	// collect
	var records []collector.SQLCPUTimeRecord
	for i := begin; i < end; i++ {
		records = append(records, collector.SQLCPUTimeRecord{
			SQLDigest:  []byte("sqlDigest" + strconv.Itoa(i+1)),
			PlanDigest: []byte("planDigest" + strconv.Itoa(i+1)),
			CPUTimeMs:  uint32(i + 1),
		})
	}
	tsr.processCPUTimeData(timestamp, records)
	reportCacheWithRU(tsr, ruRecords)
}

func makeTopRURecordsForBench(numUsers, numSQLsPerUser int) []tipb.TopRURecord {
	agg := newRUWindowAggregator()
	batch := makeRUBatch(numUsers, numSQLsPerUser)
	agg.addBatchToBucket(1, batch)
	agg.addBatchToBucket(16, batch)
	agg.addBatchToBucket(31, batch)
	agg.addBatchToBucket(46, batch)
	return agg.takeReportRecords(uint64(defaultReportInterval), uint64(defaultReportInterval), keyspaceName)
}

// BenchmarkReporterScenarios provides a unified benchmark suite for reporter paths,
// grouped by scenario category: collect/increment frequency, collect/evict, and backpressure.
// Use -bench=BenchmarkReporterScenarios -benchmem to compare ns/op and B/op.
// Use -bench=BenchmarkReporterScenarios/<category>/<case> for targeted runs.
func BenchmarkReporterScenarios(b *testing.B) {
	b.Run("collect_frequency", func(b *testing.B) {
		ruRecords := makeTopRURecordsForBench(100, 100)

		b.Run("TopSQLOnly", func(b *testing.B) {
			tsr, _ := initializeCache(b, maxSQLNum, defaultReportInterval)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				populateCache(tsr, 0, maxSQLNum, uint64(i))
			}
		})
		b.Run("TopRUOnly", func(b *testing.B) {
			tsr, _ := initializeCache(b, maxSQLNum, defaultReportInterval)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tsr.doReport(&ReportData{RURecords: ruRecords})
			}
		})
		b.Run("TopSQLAndTopRU", func(b *testing.B) {
			tsr, _ := initializeCache(b, maxSQLNum, defaultReportInterval)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				populateCacheWithRU(tsr, 0, maxSQLNum, uint64(i), ruRecords)
			}
		})
	})

	b.Run("collect_evict", func(b *testing.B) {
		tsr, _ := initializeCache(b, maxSQLNum, defaultReportInterval)
		begin := 0
		end := maxSQLNum
		for i := range b.N {
			begin += maxSQLNum
			end += maxSQLNum
			populateCache(tsr, begin, end, uint64(i))
		}
	})

	b.Run("backpressure", func(b *testing.B) {
		origInterval := topsqlstate.GetTopRUItemInterval()
		topsqlstate.SetTopRUItemInterval(tipb.ItemInterval_ITEM_INTERVAL_60S)
		b.Cleanup(func() {
			topsqlstate.SetTopRUItemInterval(tipb.ItemInterval(origInterval))
		})

		b.Run("normal", func(b *testing.B) {
			tsr := NewRemoteTopSQLReporter(mockPlanBinaryDecoderFunc, mockPlanBinaryCompressFunc)
			b.Cleanup(tsr.Close)

			drainDone := make(chan struct{})
			go func() {
				for {
					select {
					case <-drainDone:
						return
					case <-tsr.reportCollectedDataChan:
					}
				}
			}()
			b.Cleanup(func() { close(drainDone) })

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tsr.takeDataAndSendToReportChan(60)
			}
		})

		b.Run("drop_path", func(b *testing.B) {
			tsr := NewRemoteTopSQLReporter(mockPlanBinaryDecoderFunc, mockPlanBinaryCompressFunc)
			b.Cleanup(tsr.Close)
			tsr.reportCollectedDataChan <- collectedData{} // keep channel full to trigger drop path

			beforeDrop := readCounterValue(reporter_metrics.IgnoreReportChannelFullCounter)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tsr.takeDataAndSendToReportChan(60)
			}
			b.StopTimer()

			dropped := readCounterValue(reporter_metrics.IgnoreReportChannelFullCounter) - beforeDrop
			b.ReportMetric(dropped, "drop_total")
			if b.N > 0 {
				b.ReportMetric(dropped/float64(b.N), "drop/op")
			}
		})
	})
}

func readCounterValue(c interface{ Write(*dto.Metric) error }) float64 {
	pb := &dto.Metric{}
	if err := c.Write(pb); err != nil {
		return 0
	}
	return pb.GetCounter().GetValue()
}

func readCounter(t *testing.T, c interface{ Write(*dto.Metric) error }) float64 {
	t.Helper()
	pb := &dto.Metric{}
	require.NoError(t, c.Write(pb))
	return pb.GetCounter().GetValue()
}
