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
	"testing"
	"time"

	"github.com/pingcap/tidb/util/topsql/collector"
	topsqlstate "github.com/pingcap/tidb/util/topsql/state"
	"github.com/pingcap/tidb/util/topsql/stmtstats"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	maxSQLNum = 5000
)

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
		tsr.RegisterPlan(key, value)
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
		DataRecords: tsr.collecting.take().getReportRecords().toProto(),
		SQLMetas:    tsr.normalizedSQLMap.take().toProto(),
		PlanMetas:   tsr.normalizedPlanMap.take().toProto(tsr.decodePlan),
	})
}

func mockPlanBinaryDecoderFunc(plan string) (string, error) {
	return plan, nil
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

func setupRemoteTopSQLReporter(maxStatementsNum, interval int) (*RemoteTopSQLReporter, *mockDataSink2) {
	topsqlstate.GlobalState.MaxStatementCount.Store(int64(maxStatementsNum))
	topsqlstate.GlobalState.MaxCollect.Store(10000)
	topsqlstate.GlobalState.ReportIntervalSeconds.Store(int64(interval))
	topsqlstate.EnableTopSQL()
	ts := NewRemoteTopSQLReporter(mockPlanBinaryDecoderFunc)
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

func TestCollectAndSendBatch(t *testing.T) {
	tsr, ds := setupRemoteTopSQLReporter(maxSQLNum, 1)
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
		require.Equal(t, "sqlNormalized"+strconv.Itoa(id), sqlMeta.NormalizedSql)
		planMeta, exist := findPlanMeta(data.PlanMetas, req.PlanDigest)
		require.True(t, exist)
		require.Equal(t, "planNormalized"+strconv.Itoa(id), planMeta.NormalizedPlan)
	}
}

func TestCollectAndEvicted(t *testing.T) {
	tsr, ds := setupRemoteTopSQLReporter(maxSQLNum, 1)
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
		require.Equal(t, "sqlNormalized"+strconv.Itoa(id), sqlMeta.NormalizedSql)
		planMeta, exist := findPlanMeta(data.PlanMetas, req.PlanDigest)
		require.True(t, exist)
		require.Equal(t, "planNormalized"+strconv.Itoa(id), planMeta.NormalizedPlan)
	}
}

func newSQLCPUTimeRecord(tsr *RemoteTopSQLReporter, sqlID int, cpuTimeMs uint32) collector.SQLCPUTimeRecord {
	key := []byte("sqlDigest" + strconv.Itoa(sqlID))
	value := "sqlNormalized" + strconv.Itoa(sqlID)
	tsr.RegisterSQL(key, value, sqlID%2 == 0)

	key = []byte("planDigest" + strconv.Itoa(sqlID))
	value = "planNormalized" + strconv.Itoa(sqlID)
	tsr.RegisterPlan(key, value)

	return collector.SQLCPUTimeRecord{
		SQLDigest:  []byte("sqlDigest" + strconv.Itoa(sqlID)),
		PlanDigest: []byte("planDigest" + strconv.Itoa(sqlID)),
		CPUTimeMs:  cpuTimeMs,
	}
}

func TestCollectAndTopN(t *testing.T) {
	tsr, ds := setupRemoteTopSQLReporter(2, 1)

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
}

func TestCollectCapacity(t *testing.T) {
	tsr, _ := setupRemoteTopSQLReporter(maxSQLNum, 60)
	registerSQL := func(n int) {
		for i := 0; i < n; i++ {
			key := []byte("sqlDigest" + strconv.Itoa(i))
			value := "sqlNormalized" + strconv.Itoa(i)
			tsr.RegisterSQL(key, value, false)
		}
	}
	registerPlan := func(n int) {
		for i := 0; i < n; i++ {
			key := []byte("planDigest" + strconv.Itoa(i))
			value := "planNormalized" + strconv.Itoa(i)
			tsr.RegisterPlan(key, value)
		}
	}
	genRecord := func(n int) []collector.SQLCPUTimeRecord {
		records := make([]collector.SQLCPUTimeRecord, 0, n)
		for i := 0; i < n; i++ {
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
	tsr, ds := setupRemoteTopSQLReporter(3000, 1)

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
	}
}

func TestMultipleDataSinks(t *testing.T) {
	topsqlstate.GlobalState.ReportIntervalSeconds.Store(1)
	topsqlstate.EnableTopSQL()

	tsr := NewRemoteTopSQLReporter(mockPlanBinaryDecoderFunc)

	var chs []chan *ReportData
	for i := 0; i < 7; i++ {
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
		require.Equal(t, []byte("sqlDigest1"), d.DataRecords[0].SqlDigest)
		require.Equal(t, []byte("planDigest1"), d.DataRecords[0].PlanDigest)
		require.Len(t, d.DataRecords[0].Items, 1)
		require.Equal(t, uint64(3), d.DataRecords[0].Items[0].TimestampSec)
		require.Equal(t, uint32(2), d.DataRecords[0].Items[0].CpuTimeMs)

		require.Equal(t, []tipb.SQLMeta{{
			SqlDigest:     []byte("sqlDigest1"),
			NormalizedSql: "sqlNormalized1",
		}}, d.SQLMetas)

		require.Equal(t, []tipb.PlanMeta{{
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
		require.Equal(t, []byte("sqlDigest4"), d.DataRecords[0].SqlDigest)
		require.Equal(t, []byte("planDigest4"), d.DataRecords[0].PlanDigest)
		require.Len(t, d.DataRecords[0].Items, 1)
		require.Equal(t, uint64(6), d.DataRecords[0].Items[0].TimestampSec)
		require.Equal(t, uint32(5), d.DataRecords[0].Items[0].CpuTimeMs)

		require.Equal(t, []tipb.SQLMeta{{
			SqlDigest:     []byte("sqlDigest4"),
			NormalizedSql: "sqlNormalized4",
			IsInternalSql: true,
		}}, d.SQLMetas)

		require.Equal(t, []tipb.PlanMeta{{
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
	topsqlstate.GlobalState.ReportIntervalSeconds.Store(3)

	r := NewRemoteTopSQLReporter(mockPlanBinaryDecoderFunc)
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

func initializeCache(maxStatementsNum, interval int) (*RemoteTopSQLReporter, *mockDataSink2) {
	ts, ds := setupRemoteTopSQLReporter(maxStatementsNum, interval)
	populateCache(ts, 0, maxStatementsNum, 1)
	return ts, ds
}

func BenchmarkTopSQL_CollectAndIncrementFrequency(b *testing.B) {
	tsr, _ := initializeCache(maxSQLNum, 120)
	for i := 0; i < b.N; i++ {
		populateCache(tsr, 0, maxSQLNum, uint64(i))
	}
}

func BenchmarkTopSQL_CollectAndEvict(b *testing.B) {
	tsr, _ := initializeCache(maxSQLNum, 120)
	begin := 0
	end := maxSQLNum
	for i := 0; i < b.N; i++ {
		begin += maxSQLNum
		end += maxSQLNum
		populateCache(tsr, begin, end, uint64(i))
	}
}
