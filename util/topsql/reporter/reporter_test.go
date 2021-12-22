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
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/topsql/reporter/mock"
	"github.com/pingcap/tidb/util/topsql/stmtstats"
	"github.com/pingcap/tidb/util/topsql/tracecpu"
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
	var records []tracecpu.SQLCPUTimeRecord
	for i := begin; i < end; i++ {
		records = append(records, tracecpu.SQLCPUTimeRecord{
			SQLDigest:  []byte("sqlDigest" + strconv.Itoa(i+1)),
			PlanDigest: []byte("planDigest" + strconv.Itoa(i+1)),
			CPUTimeMs:  uint32(i + 1),
		})
	}
	tsr.Collect(timestamp, records)
	// sleep a while for the asynchronous collect
	time.Sleep(100 * time.Millisecond)
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

func setupRemoteTopSQLReporter(maxStatementsNum, interval int, addr string) (*RemoteTopSQLReporter, *SingleTargetDataSink) {
	variable.TopSQLVariable.MaxStatementCount.Store(int64(maxStatementsNum))
	variable.TopSQLVariable.MaxCollect.Store(10000)
	variable.TopSQLVariable.ReportIntervalSeconds.Store(int64(interval))
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TopSQL.ReceiverAddress = addr
	})

	ts := NewRemoteTopSQLReporter(mockPlanBinaryDecoderFunc)
	ds := NewSingleTargetDataSink(ts)
	return ts, ds
}

func initializeCache(maxStatementsNum, interval int, addr string) (*RemoteTopSQLReporter, *SingleTargetDataSink) {
	ts, ds := setupRemoteTopSQLReporter(maxStatementsNum, interval, addr)
	populateCache(ts, 0, maxStatementsNum, 1)
	return ts, ds
}

func TestCollectAndSendBatch(t *testing.T) {
	agentServer, err := mock.StartMockAgentServer()
	require.NoError(t, err)
	defer agentServer.Stop()

	tsr, ds := setupRemoteTopSQLReporter(maxSQLNum, 1, agentServer.Address())
	defer func() {
		ds.Close()
		tsr.Close()
	}()
	populateCache(tsr, 0, maxSQLNum, 1)

	agentServer.WaitCollectCnt(1, time.Second*5)
	require.Len(t, agentServer.GetLatestRecords(), maxSQLNum)

	// check for equality of server received batch and the original data
	records := agentServer.GetLatestRecords()
	for _, req := range records {
		id := 0
		prefix := "sqlDigest"
		if strings.HasPrefix(string(req.SqlDigest), prefix) {
			n, err := strconv.Atoi(string(req.SqlDigest)[len(prefix):])
			require.NoError(t, err)
			id = n
		}
		require.Len(t, req.RecordListCpuTimeMs, 1)
		for i := range req.RecordListCpuTimeMs {
			require.Equal(t, uint32(id), req.RecordListCpuTimeMs[i])
		}
		require.Len(t, req.RecordListTimestampSec, 1)
		for i := range req.RecordListTimestampSec {
			require.Equal(t, uint64(1), req.RecordListTimestampSec[i])
		}
		sqlMeta, exist := agentServer.GetSQLMetaByDigestBlocking(req.SqlDigest, time.Second)
		require.True(t, exist)
		require.Equal(t, "sqlNormalized"+strconv.Itoa(id), sqlMeta.NormalizedSql)
		normalizedPlan, exist := agentServer.GetPlanMetaByDigestBlocking(req.PlanDigest, time.Second)
		require.True(t, exist)
		require.Equal(t, "planNormalized"+strconv.Itoa(id), normalizedPlan)
	}
}

func TestCollectAndEvicted(t *testing.T) {
	agentServer, err := mock.StartMockAgentServer()
	require.NoError(t, err)
	defer agentServer.Stop()

	tsr, ds := setupRemoteTopSQLReporter(maxSQLNum, 1, agentServer.Address())
	defer func() {
		ds.Close()
		tsr.Close()
	}()
	populateCache(tsr, 0, maxSQLNum*2, 2)

	agentServer.WaitCollectCnt(1, time.Second*10)

	// check for equality of server received batch and the original data
	records := agentServer.GetLatestRecords()
	require.Len(t, records, maxSQLNum+1)
	for _, req := range records {
		id := 0
		prefix := "sqlDigest"
		if strings.HasPrefix(string(req.SqlDigest), prefix) {
			n, err := strconv.Atoi(string(req.SqlDigest)[len(prefix):])
			require.NoError(t, err)
			id = n
		}
		require.Len(t, req.RecordListTimestampSec, 1)
		require.Equal(t, uint64(2), req.RecordListTimestampSec[0])
		require.Len(t, req.RecordListCpuTimeMs, 1)
		if id == 0 {
			// test for others
			require.Nil(t, req.SqlDigest)
			require.Nil(t, req.PlanDigest)
			// 12502500 is the sum of all evicted item's cpu time. 1 + 2 + 3 + ... + 5000 = (1 + 5000) * 2500 = 12502500
			require.Equal(t, 12502500, int(req.RecordListCpuTimeMs[0]))
			continue
		}
		require.Greater(t, id, maxSQLNum)
		require.Equal(t, uint32(id), req.RecordListCpuTimeMs[0])
		sqlMeta, exist := agentServer.GetSQLMetaByDigestBlocking(req.SqlDigest, time.Second)
		require.True(t, exist)
		require.Equal(t, "sqlNormalized"+strconv.Itoa(id), sqlMeta.NormalizedSql)
		normalizedPlan, exist := agentServer.GetPlanMetaByDigestBlocking(req.PlanDigest, time.Second)
		require.True(t, exist)
		require.Equal(t, "planNormalized"+strconv.Itoa(id), normalizedPlan)
	}
}

func newSQLCPUTimeRecord(tsr *RemoteTopSQLReporter, sqlID int, cpuTimeMs uint32) tracecpu.SQLCPUTimeRecord {
	key := []byte("sqlDigest" + strconv.Itoa(sqlID))
	value := "sqlNormalized" + strconv.Itoa(sqlID)
	tsr.RegisterSQL(key, value, sqlID%2 == 0)

	key = []byte("planDigest" + strconv.Itoa(sqlID))
	value = "planNormalized" + strconv.Itoa(sqlID)
	tsr.RegisterPlan(key, value)

	return tracecpu.SQLCPUTimeRecord{
		SQLDigest:  []byte("sqlDigest" + strconv.Itoa(sqlID)),
		PlanDigest: []byte("planDigest" + strconv.Itoa(sqlID)),
		CPUTimeMs:  cpuTimeMs,
	}
}

func collectAndWait(tsr *RemoteTopSQLReporter, timestamp uint64, records []tracecpu.SQLCPUTimeRecord) {
	tsr.Collect(timestamp, records)
	time.Sleep(time.Millisecond * 100)
}

func TestCollectAndTopN(t *testing.T) {
	agentServer, err := mock.StartMockAgentServer()
	require.NoError(t, err)
	defer agentServer.Stop()

	tsr, ds := setupRemoteTopSQLReporter(2, 1, agentServer.Address())
	defer func() {
		ds.Close()
		tsr.Close()
	}()

	records := []tracecpu.SQLCPUTimeRecord{
		newSQLCPUTimeRecord(tsr, 1, 1),
		newSQLCPUTimeRecord(tsr, 2, 2),
	}
	collectAndWait(tsr, 1, records)

	records = []tracecpu.SQLCPUTimeRecord{
		newSQLCPUTimeRecord(tsr, 3, 3),
		newSQLCPUTimeRecord(tsr, 1, 1),
	}
	collectAndWait(tsr, 2, records)

	records = []tracecpu.SQLCPUTimeRecord{
		newSQLCPUTimeRecord(tsr, 4, 1),
		newSQLCPUTimeRecord(tsr, 1, 1),
	}
	collectAndWait(tsr, 3, records)

	records = []tracecpu.SQLCPUTimeRecord{
		newSQLCPUTimeRecord(tsr, 5, 1),
		newSQLCPUTimeRecord(tsr, 1, 1),
	}
	collectAndWait(tsr, 4, records)

	// Test for time jump back.
	records = []tracecpu.SQLCPUTimeRecord{
		newSQLCPUTimeRecord(tsr, 6, 1),
		newSQLCPUTimeRecord(tsr, 1, 1),
	}
	collectAndWait(tsr, 0, records)

	// Wait agent server collect finish.
	agentServer.WaitCollectCnt(1, time.Second*10)

	// check for equality of server received batch and the original data
	results := agentServer.GetLatestRecords()
	require.Len(t, results, 3)
	sort.Slice(results, func(i, j int) bool {
		return string(results[i].SqlDigest) < string(results[j].SqlDigest)
	})
	getTotalCPUTime := func(record *tipb.TopSQLRecord) int {
		total := uint32(0)
		for _, v := range record.RecordListCpuTimeMs {
			total += v
		}
		return int(total)
	}
	require.Nil(t, results[0].SqlDigest)
	require.Equal(t, 5, getTotalCPUTime(results[0]))
	require.Equal(t, []uint64{0, 1, 3, 4}, results[0].RecordListTimestampSec)
	require.Equal(t, []uint32{1, 2, 1, 1}, results[0].RecordListCpuTimeMs)
	require.Equal(t, []byte("sqlDigest1"), results[1].SqlDigest)
	require.Equal(t, 5, getTotalCPUTime(results[1]))
	require.Equal(t, []byte("sqlDigest3"), results[2].SqlDigest)
	require.Equal(t, 3, getTotalCPUTime(results[2]))
	// sleep to wait for all SQL meta received.
	time.Sleep(50 * time.Millisecond)
	totalMetas := agentServer.GetTotalSQLMetas()
	require.Equal(t, 6, len(totalMetas))
}

func TestCollectCapacity(t *testing.T) {
	tsr, ds := setupRemoteTopSQLReporter(maxSQLNum, 60, "")
	defer func() {
		ds.Close()
		tsr.Close()
	}()

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
	genRecord := func(n int) []tracecpu.SQLCPUTimeRecord {
		records := make([]tracecpu.SQLCPUTimeRecord, 0, n)
		for i := 0; i < n; i++ {
			records = append(records, tracecpu.SQLCPUTimeRecord{
				SQLDigest:  []byte("sqlDigest" + strconv.Itoa(i+1)),
				PlanDigest: []byte("planDigest" + strconv.Itoa(i+1)),
				CPUTimeMs:  uint32(i + 1),
			})
		}
		return records
	}

	variable.TopSQLVariable.MaxCollect.Store(10000)
	registerSQL(5000)
	require.Equal(t, int64(5000), tsr.sqlMapLength.Load())
	registerPlan(1000)
	require.Equal(t, int64(1000), tsr.planMapLength.Load())

	registerSQL(20000)
	require.Equal(t, int64(10000), tsr.sqlMapLength.Load())
	registerPlan(20000)
	require.Equal(t, int64(10000), tsr.planMapLength.Load())

	variable.TopSQLVariable.MaxCollect.Store(20000)
	registerSQL(50000)
	require.Equal(t, int64(20000), tsr.sqlMapLength.Load())
	registerPlan(50000)
	require.Equal(t, int64(20000), tsr.planMapLength.Load())

	variable.TopSQLVariable.MaxStatementCount.Store(5000)
	collectedData := make(map[string]*dataPoints)
	tsr.doCollect(collectedData, map[uint64]map[stmtstats.SQLPlanDigest]struct{}{}, 1, genRecord(20000))
	require.Equal(t, 5001, len(collectedData))
	require.Equal(t, int64(20000), tsr.sqlMapLength.Load())
	require.Equal(t, int64(20000), tsr.planMapLength.Load())
}

func TestCollectOthers(t *testing.T) {
	collectTarget := make(map[string]*dataPoints)
	addEvictedCPUTime(collectTarget, 1, 10)
	addEvictedCPUTime(collectTarget, 2, 20)
	addEvictedCPUTime(collectTarget, 3, 30)
	others := collectTarget[keyOthers]
	require.Equal(t, uint64(60), others.CPUTimeMsTotal)
	require.Equal(t, []uint64{1, 2, 3}, others.TimestampList)
	require.Equal(t, []uint32{10, 20, 30}, others.CPUTimeMsList)

	others = addEvictedIntoSortedDataPoints(nil, others)
	require.Equal(t, uint64(60), others.CPUTimeMsTotal)

	// test for time jump backward.
	evict := &dataPoints{tsIndex: map[uint64]int{}}
	evict.TimestampList = []uint64{3, 2, 4}
	evict.CPUTimeMsList = []uint32{30, 20, 40}
	evict.CPUTimeMsTotal = 90
	evict.StmtExecCountList = []uint64{0, 0, 0}
	evict.StmtKvExecCountList = []map[string]uint64{nil, nil, nil}
	evict.StmtDurationSumNsList = []uint64{0, 0, 0}
	others = addEvictedIntoSortedDataPoints(others, evict)
	require.Equal(t, uint64(150), others.CPUTimeMsTotal)
	require.Equal(t, []uint64{1, 2, 3, 4}, others.TimestampList)
	require.Equal(t, []uint32{10, 40, 60, 40}, others.CPUTimeMsList)
}

func TestDataPoints(t *testing.T) {
	// test for dataPoints invalid.
	d := &dataPoints{}
	d.TimestampList = []uint64{1}
	d.CPUTimeMsList = []uint32{10, 30}
	require.True(t, d.isInvalid())

	// test for dataPoints sort.
	d = &dataPoints{}
	d.TimestampList = []uint64{1, 2, 5, 6, 3, 4}
	d.CPUTimeMsList = []uint32{10, 20, 50, 60, 30, 40}
	d.StmtExecCountList = []uint64{11, 12, 13, 14, 15, 16}
	d.StmtKvExecCountList = []map[string]uint64{{"": 21}, {"": 22}, {"": 23}, {"": 24}, {"": 25}, {"": 26}}
	d.StmtDurationSumNsList = []uint64{31, 32, 33, 34, 35, 36}
	d.rebuildTsIndex()
	sort.Sort(d)
	require.Equal(t, []uint64{1, 2, 3, 4, 5, 6}, d.TimestampList)
	require.Equal(t, []uint32{10, 20, 30, 40, 50, 60}, d.CPUTimeMsList)
	require.Equal(t, []uint64{11, 12, 15, 16, 13, 14}, d.StmtExecCountList)
	require.Equal(t, []map[string]uint64{{"": 21}, {"": 22}, {"": 25}, {"": 26}, {"": 23}, {"": 24}}, d.StmtKvExecCountList)
	require.Equal(t, []uint64{31, 32, 35, 36, 33, 34}, d.StmtDurationSumNsList)

	// test for dataPoints merge.
	d = &dataPoints{}
	evict := &dataPoints{}
	addEvictedIntoSortedDataPoints(d, evict)
	evict.TimestampList = []uint64{1, 3}
	evict.CPUTimeMsList = []uint32{10, 30}
	evict.CPUTimeMsTotal = 40
	evict.StmtExecCountList = []uint64{0, 0}
	evict.StmtKvExecCountList = []map[string]uint64{{}, {}}
	evict.StmtDurationSumNsList = []uint64{0, 0}
	evict.rebuildTsIndex()
	addEvictedIntoSortedDataPoints(d, evict)
	require.Equal(t, uint64(40), d.CPUTimeMsTotal)
	require.Equal(t, []uint64{1, 3}, d.TimestampList)
	require.Equal(t, []uint32{10, 30}, d.CPUTimeMsList)

	evict.TimestampList = []uint64{1, 2, 3, 4, 5}
	evict.CPUTimeMsList = []uint32{10, 20, 30, 40, 50}
	evict.CPUTimeMsTotal = 150
	evict.StmtExecCountList = []uint64{0, 0, 0, 0, 0}
	evict.StmtKvExecCountList = []map[string]uint64{{}, {}, {}, {}, {}}
	evict.StmtDurationSumNsList = []uint64{0, 0, 0, 0, 0}
	evict.rebuildTsIndex()
	addEvictedIntoSortedDataPoints(d, evict)
	require.Equal(t, uint64(190), d.CPUTimeMsTotal)
	require.Equal(t, []uint64{1, 2, 3, 4, 5}, d.TimestampList)
	require.Equal(t, []uint32{20, 20, 60, 40, 50}, d.CPUTimeMsList)

	// test for time jump backward.
	d = &dataPoints{}
	evict = &dataPoints{}
	evict.TimestampList = []uint64{3, 2}
	evict.CPUTimeMsList = []uint32{30, 20}
	evict.CPUTimeMsTotal = 50
	evict.StmtExecCountList = []uint64{0, 0}
	evict.StmtKvExecCountList = []map[string]uint64{{}, {}}
	evict.StmtDurationSumNsList = []uint64{0, 0}
	evict.rebuildTsIndex()
	addEvictedIntoSortedDataPoints(d, evict)
	require.Equal(t, uint64(50), d.CPUTimeMsTotal)
	require.Equal(t, []uint64{2, 3}, d.TimestampList)
	require.Equal(t, []uint32{20, 30}, d.CPUTimeMsList)

	// test for merge invalid dataPoints
	d = &dataPoints{}
	evict = &dataPoints{}
	evict.TimestampList = []uint64{1}
	evict.CPUTimeMsList = []uint32{10, 30}
	require.True(t, evict.isInvalid())
	addEvictedIntoSortedDataPoints(d, evict)
	require.False(t, d.isInvalid())
	require.Nil(t, d.CPUTimeMsList)
	require.Nil(t, d.TimestampList)
}

func TestCollectInternal(t *testing.T) {
	agentServer, err := mock.StartMockAgentServer()
	require.NoError(t, err)
	defer agentServer.Stop()

	tsr, ds := setupRemoteTopSQLReporter(3000, 1, agentServer.Address())
	defer func() {
		ds.Close()
		tsr.Close()
	}()

	records := []tracecpu.SQLCPUTimeRecord{
		newSQLCPUTimeRecord(tsr, 1, 1),
		newSQLCPUTimeRecord(tsr, 2, 2),
	}
	collectAndWait(tsr, 1, records)

	// Wait agent server collect finish.
	agentServer.WaitCollectCnt(1, time.Second*10)

	// check for equality of server received batch and the original data
	results := agentServer.GetLatestRecords()
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
		sqlMeta, exist := agentServer.GetSQLMetaByDigestBlocking(req.SqlDigest, time.Second)
		require.True(t, exist)
		require.Equal(t, id%2 == 0, sqlMeta.IsInternalSql)
	}
}

func TestMultipleDataSinks(t *testing.T) {
	variable.TopSQLVariable.ReportIntervalSeconds.Store(1)

	tsr := NewRemoteTopSQLReporter(mockPlanBinaryDecoderFunc)
	defer tsr.Close()

	var chs []chan *ReportData
	for i := 0; i < 7; i++ {
		chs = append(chs, make(chan *ReportData, 1))
	}
	var dss []DataSink
	for _, ch := range chs {
		dss = append(dss, newMockDataSink(ch))
	}
	for _, ds := range dss {
		require.NoError(t, tsr.Register(ds))
	}

	records := []tracecpu.SQLCPUTimeRecord{
		newSQLCPUTimeRecord(tsr, 1, 2),
	}
	tsr.Collect(3, records)

	for _, ch := range chs {
		d := <-ch
		require.NotNil(t, d)
		require.Len(t, d.DataRecords, 1)
		require.Equal(t, []byte("sqlDigest1"), d.DataRecords[0].SqlDigest)
		require.Equal(t, []byte("planDigest1"), d.DataRecords[0].PlanDigest)
		require.Equal(t, []uint64{3}, d.DataRecords[0].RecordListTimestampSec)
		require.Equal(t, []uint32{2}, d.DataRecords[0].RecordListCpuTimeMs)

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

	records = []tracecpu.SQLCPUTimeRecord{
		newSQLCPUTimeRecord(tsr, 4, 5),
	}
	tsr.Collect(6, records)

	for i := 1; i < 7; i += 2 {
		d := <-chs[i]
		require.NotNil(t, d)
		require.Len(t, d.DataRecords, 1)
		require.Equal(t, []byte("sqlDigest4"), d.DataRecords[0].SqlDigest)
		require.Equal(t, []byte("planDigest4"), d.DataRecords[0].PlanDigest)
		require.Equal(t, []uint64{6}, d.DataRecords[0].RecordListTimestampSec)
		require.Equal(t, []uint32{5}, d.DataRecords[0].RecordListCpuTimeMs)

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

func BenchmarkTopSQL_CollectAndIncrementFrequency(b *testing.B) {
	tsr, _ := initializeCache(maxSQLNum, 120, ":23333")
	for i := 0; i < b.N; i++ {
		populateCache(tsr, 0, maxSQLNum, uint64(i))
	}
}

func BenchmarkTopSQL_CollectAndEvict(b *testing.B) {
	tsr, _ := initializeCache(maxSQLNum, 120, ":23333")
	begin := 0
	end := maxSQLNum
	for i := 0; i < b.N; i++ {
		begin += maxSQLNum
		end += maxSQLNum
		populateCache(tsr, begin, end, uint64(i))
	}
}

func TestStmtStatsReport(t *testing.T) {
	variable.TopSQLVariable.MaxStatementCount.Store(1)

	tsr := NewRemoteTopSQLReporter(mockPlanBinaryDecoderFunc)
	tsr.Close() // manual control

	r := tsr.getReportData(collectedData{
		normalizedSQLMap:  &sync.Map{},
		normalizedPlanMap: &sync.Map{},
		records: map[string]*dataPoints{
			"S1P1": {
				SQLDigest:             []byte("S1"),
				PlanDigest:            []byte("P1"),
				TimestampList:         []uint64{1, 2, 3, 4},
				CPUTimeMsList:         []uint32{11, 12, 13, 14},
				CPUTimeMsTotal:        11 + 12 + 13 + 14,
				StmtExecCountList:     []uint64{11, 12, 13, 14},
				StmtKvExecCountList:   []map[string]uint64{{"": 11}, {"": 12}, {"": 13}, {"": 14}},
				StmtDurationSumNsList: []uint64{11, 12, 13, 14},
			},
			"S2P2": {
				SQLDigest:             []byte("S2"),
				PlanDigest:            []byte("P2"),
				TimestampList:         []uint64{1, 2, 3, 4},
				CPUTimeMsList:         []uint32{21, 22, 23, 24},
				CPUTimeMsTotal:        21 + 22 + 23 + 24,
				StmtExecCountList:     []uint64{21, 22, 23, 24},
				StmtKvExecCountList:   []map[string]uint64{{"": 21}, {"": 22}, {"": 23}, {"": 24}},
				StmtDurationSumNsList: []uint64{21, 22, 23, 24},
			},
			keyOthers: {
				SQLDigest:             []byte(nil),
				PlanDigest:            []byte(nil),
				TimestampList:         []uint64{1, 2, 3, 4},
				CPUTimeMsList:         []uint32{91, 92, 93, 94},
				CPUTimeMsTotal:        91 + 92 + 93 + 94,
				StmtExecCountList:     []uint64{91, 92, 93, 94},
				StmtKvExecCountList:   []map[string]uint64{{"": 91}, {"": 92}, {"": 93}, {"": 94}},
				StmtDurationSumNsList: []uint64{91, 92, 93, 94},
			},
		},
	})
	assert.True(t, r.hasData())
	assert.Len(t, r.DataRecords, 2)

	s2p2 := r.DataRecords[0]
	assert.Equal(t, []byte("S2"), s2p2.SqlDigest)
	assert.Equal(t, []byte("P2"), s2p2.PlanDigest)
	assert.Equal(t, []uint64{1, 2, 3, 4}, s2p2.RecordListTimestampSec)
	assert.Equal(t, []uint32{21, 22, 23, 24}, s2p2.RecordListCpuTimeMs)
	assert.Equal(t, []uint64{21, 22, 23, 24}, s2p2.RecordListStmtExecCount)
	assert.Equal(t, []uint64{21, 22, 23, 24}, s2p2.RecordListStmtDurationSumNs)
	assert.Equal(t, []*tipb.TopSQLStmtKvExecCount{
		{ExecCount: map[string]uint64{"": 21}},
		{ExecCount: map[string]uint64{"": 22}},
		{ExecCount: map[string]uint64{"": 23}},
		{ExecCount: map[string]uint64{"": 24}},
	}, s2p2.RecordListStmtKvExecCount)

	others := r.DataRecords[1]
	assert.Equal(t, []byte(nil), others.SqlDigest)
	assert.Equal(t, []byte(nil), others.PlanDigest)
	assert.Equal(t, []uint64{1, 2, 3, 4}, others.RecordListTimestampSec)
	assert.Equal(t, []uint32{91 + 11, 92 + 12, 93 + 13, 94 + 14}, others.RecordListCpuTimeMs)
	assert.Equal(t, []uint64{91 + 11, 92 + 12, 93 + 13, 94 + 14}, others.RecordListStmtExecCount)
	assert.Equal(t, []uint64{91 + 11, 92 + 12, 93 + 13, 94 + 14}, others.RecordListStmtDurationSumNs)
	assert.Equal(t, []*tipb.TopSQLStmtKvExecCount{
		{ExecCount: map[string]uint64{"": 91 + 11}},
		{ExecCount: map[string]uint64{"": 92 + 12}},
		{ExecCount: map[string]uint64{"": 93 + 13}},
		{ExecCount: map[string]uint64{"": 94 + 14}},
	}, others.RecordListStmtKvExecCount)
}

func TestStmtStatsCollect(t *testing.T) {
	variable.TopSQLVariable.MaxStatementCount.Store(1000)

	tsr := NewRemoteTopSQLReporter(mockPlanBinaryDecoderFunc)
	tsr.Close() // manual control

	total := map[string]*dataPoints{}
	evicted := map[uint64]map[stmtstats.SQLPlanDigest]struct{}{}
	//   TimestampList: []
	//     CPUTimeList: []
	//   ExecCountList: []
	// KvExecCountList: []

	collectCPUTime(tsr, total, evicted, "S1", "P1", 1, 1)
	//   TimestampList: [1]
	//     CPUTimeList: [1]
	//   ExecCountList: [0]
	// KvExecCountList: [0]

	collectStmtStats(tsr, total, evicted, "S1", "P1", 1, 2, map[string]uint64{"": 3})
	//   TimestampList: [1]
	//     CPUTimeList: [1]
	//   ExecCountList: [2]
	// KvExecCountList: [3]

	collectCPUTime(tsr, total, evicted, "S1", "P1", 2, 1)
	//   TimestampList: [1, 2]
	//     CPUTimeList: [1, 1]
	//   ExecCountList: [2, 0]
	// KvExecCountList: [3, 0]

	collectCPUTime(tsr, total, evicted, "S1", "P1", 3, 1)
	//   TimestampList: [1, 2, 3]
	//     CPUTimeList: [1, 1, 1]
	//   ExecCountList: [2, 0, 0]
	// KvExecCountList: [3, 0, 0]

	collectStmtStats(tsr, total, evicted, "S1", "P1", 3, 2, map[string]uint64{"": 3})
	//   TimestampList: [1, 2, 3]
	//     CPUTimeList: [1, 1, 1]
	//   ExecCountList: [2, 0, 2]
	// KvExecCountList: [3, 0, 3]

	collectStmtStats(tsr, total, evicted, "S1", "P1", 2, 2, map[string]uint64{"": 3})
	//   TimestampList: [1, 2, 3]
	//     CPUTimeList: [1, 1, 1]
	//   ExecCountList: [2, 2, 2]
	// KvExecCountList: [3, 3, 3]

	assert.Empty(t, evicted)
	data, ok := total["S1P1"]
	assert.True(t, ok)
	assert.Equal(t, []byte("S1"), data.SQLDigest)
	assert.Equal(t, []byte("P1"), data.PlanDigest)
	assert.Equal(t, uint64(3), data.CPUTimeMsTotal)
	assert.Equal(t, []uint64{1, 2, 3}, data.TimestampList)
	assert.Equal(t, []uint32{1, 1, 1}, data.CPUTimeMsList)
	assert.Equal(t, []uint64{2, 2, 2}, data.StmtExecCountList)
	assert.Equal(t, []map[string]uint64{{"": 3}, {"": 3}, {"": 3}}, data.StmtKvExecCountList)
}

func TestStmtStatsCollectEvicted(t *testing.T) {
	variable.TopSQLVariable.MaxStatementCount.Store(2)

	tsr := NewRemoteTopSQLReporter(mockPlanBinaryDecoderFunc)
	tsr.Close() // manual control

	total := map[string]*dataPoints{}
	evicted := map[uint64]map[stmtstats.SQLPlanDigest]struct{}{}

	tsr.doCollect(total, evicted, 1, []tracecpu.SQLCPUTimeRecord{
		{SQLDigest: []byte("S1"), PlanDigest: []byte("P1"), CPUTimeMs: 1},
		{SQLDigest: []byte("S2"), PlanDigest: []byte("P2"), CPUTimeMs: 2},
		{SQLDigest: []byte("S3"), PlanDigest: []byte("P3"), CPUTimeMs: 3},
	})
	// S2P2:
	//   TimestampList: [1]
	//     CPUTimeList: [2]
	//   ExecCountList: [0]
	// KvExecCountList: [0]
	//
	// S3P3:
	//   TimestampList: [1]
	//     CPUTimeList: [3]
	//   ExecCountList: [0]
	// KvExecCountList: [0]
	//
	// others:
	//   TimestampList: [1]
	//     CPUTimeList: [1]
	//   ExecCountList: [0]
	// KvExecCountList: [0]
	//
	// evicted: {1: S1P1}

	collectStmtStats(tsr, total, evicted, "S1", "P1", 1, 1, map[string]uint64{"": 1})
	// S2P2:
	//   TimestampList: [1]
	//     CPUTimeList: [2]
	//   ExecCountList: [0]
	// KvExecCountList: [0]
	//
	// S3P3:
	//   TimestampList: [1]
	//     CPUTimeList: [3]
	//   ExecCountList: [0]
	// KvExecCountList: [0]
	//
	// others:
	//   TimestampList: [1]
	//     CPUTimeList: [1]
	//   ExecCountList: [1]
	// KvExecCountList: [1]
	//
	// evicted: {1: S1P1}

	collectStmtStats(tsr, total, evicted, "S2", "P2", 1, 2, map[string]uint64{"": 2})
	// S2P2:
	//   TimestampList: [1]
	//     CPUTimeList: [2]
	//   ExecCountList: [2]
	// KvExecCountList: [2]
	//
	// S3P3:
	//   TimestampList: [1]
	//     CPUTimeList: [3]
	//   ExecCountList: [0]
	// KvExecCountList: [0]
	//
	// others:
	//   TimestampList: [1]
	//     CPUTimeList: [1]
	//   ExecCountList: [1]
	// KvExecCountList: [1]
	//
	// evicted: {1: S1P1}

	collectStmtStats(tsr, total, evicted, "S3", "P3", 1, 3, map[string]uint64{"": 3})
	// S2P2:
	//   TimestampList: [1]
	//     CPUTimeList: [2]
	//   ExecCountList: [2]
	// KvExecCountList: [2]
	//
	// S3P3:
	//   TimestampList: [1]
	//     CPUTimeList: [3]
	//   ExecCountList: [3]
	// KvExecCountList: [3]
	//
	// others:
	//   TimestampList: [1]
	//     CPUTimeList: [1]
	//   ExecCountList: [1]
	// KvExecCountList: [1]
	//
	// evicted: {1: S1P1}

	assert.Len(t, evicted, 1)
	m, ok := evicted[1]
	assert.True(t, ok)
	_, ok = m[stmtstats.SQLPlanDigest{SQLDigest: "S1", PlanDigest: "P1"}]
	assert.True(t, ok)
	_, ok = total["S1P1"]
	assert.False(t, ok)

	s2p2, ok := total["S2P2"]
	assert.True(t, ok)
	assert.Equal(t, []byte("S2"), s2p2.SQLDigest)
	assert.Equal(t, []byte("P2"), s2p2.PlanDigest)
	assert.Equal(t, uint64(2), s2p2.CPUTimeMsTotal)
	assert.Equal(t, []uint64{1}, s2p2.TimestampList)
	assert.Equal(t, []uint32{2}, s2p2.CPUTimeMsList)
	assert.Equal(t, []uint64{2}, s2p2.StmtExecCountList)
	assert.Equal(t, []map[string]uint64{{"": 2}}, s2p2.StmtKvExecCountList)

	s3p3, ok := total["S3P3"]
	assert.True(t, ok)
	assert.Equal(t, []byte("S3"), s3p3.SQLDigest)
	assert.Equal(t, []byte("P3"), s3p3.PlanDigest)
	assert.Equal(t, uint64(3), s3p3.CPUTimeMsTotal)
	assert.Equal(t, []uint64{1}, s3p3.TimestampList)
	assert.Equal(t, []uint32{3}, s3p3.CPUTimeMsList)
	assert.Equal(t, []uint64{3}, s3p3.StmtExecCountList)
	assert.Equal(t, []map[string]uint64{{"": 3}}, s3p3.StmtKvExecCountList)

	others, ok := total[keyOthers]
	assert.True(t, ok)
	assert.Equal(t, []byte(nil), others.SQLDigest)
	assert.Equal(t, []byte(nil), others.PlanDigest)
	assert.Equal(t, uint64(1), others.CPUTimeMsTotal)
	assert.Equal(t, []uint64{1}, others.TimestampList)
	assert.Equal(t, []uint32{1}, others.CPUTimeMsList)
	assert.Equal(t, []uint64{1}, others.StmtExecCountList)
	assert.Equal(t, []map[string]uint64{{"": 1}}, others.StmtKvExecCountList)
}

func collectCPUTime(
	tsr *RemoteTopSQLReporter,
	total map[string]*dataPoints,
	evicted map[uint64]map[stmtstats.SQLPlanDigest]struct{},
	sqlDigest, planDigest string,
	ts uint64,
	cpuTime uint32) {
	tsr.doCollect(total, evicted, ts, []tracecpu.SQLCPUTimeRecord{{
		SQLDigest:  []byte(sqlDigest),
		PlanDigest: []byte(planDigest),
		CPUTimeMs:  cpuTime,
	}})
}

func collectStmtStats(
	tsr *RemoteTopSQLReporter,
	total map[string]*dataPoints,
	evicted map[uint64]map[stmtstats.SQLPlanDigest]struct{},
	sqlDigest, planDigest string,
	ts int64,
	execCount uint64,
	kvExecCount map[string]uint64) {
	tsr.doCollectStmtRecords(total, evicted, []stmtstats.StatementStatsRecord{{
		Timestamp: ts,
		Data: stmtstats.StatementStatsMap{
			stmtstats.SQLPlanDigest{
				SQLDigest:  stmtstats.BinaryDigest(sqlDigest),
				PlanDigest: stmtstats.BinaryDigest(planDigest),
			}: &stmtstats.StatementStatsItem{
				ExecCount: execCount,
				KvStatsItem: stmtstats.KvStatementStatsItem{
					KvExecCount: kvExecCount,
				},
				// TODO(mornyx): add duration
			},
		},
	}})
}
