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

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/topsql/reporter/mock"
	"github.com/pingcap/tidb/util/topsql/tracecpu"
	"github.com/pingcap/tipb/go-tipb"
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

func setupRemoteTopSQLReporter(maxStatementsNum, interval int, addr string) *RemoteTopSQLReporter {
	variable.TopSQLVariable.MaxStatementCount.Store(int64(maxStatementsNum))
	variable.TopSQLVariable.MaxCollect.Store(10000)
	variable.TopSQLVariable.ReportIntervalSeconds.Store(int64(interval))
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TopSQL.ReceiverAddress = addr
	})

	rc := NewSingleTargetDataSink()
	ts := NewRemoteTopSQLReporter(rc, mockPlanBinaryDecoderFunc)
	return ts
}

func initializeCache(maxStatementsNum, interval int, addr string) *RemoteTopSQLReporter {
	ts := setupRemoteTopSQLReporter(maxStatementsNum, interval, addr)
	populateCache(ts, 0, maxStatementsNum, 1)
	return ts
}

func TestCollectAndSendBatch(t *testing.T) {
	agentServer, err := mock.StartMockAgentServer()
	require.NoError(t, err)
	defer agentServer.Stop()

	tsr := setupRemoteTopSQLReporter(maxSQLNum, 1, agentServer.Address())
	defer tsr.Close()
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

	tsr := setupRemoteTopSQLReporter(maxSQLNum, 1, agentServer.Address())
	defer tsr.Close()
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

	tsr := setupRemoteTopSQLReporter(2, 1, agentServer.Address())
	defer tsr.Close()

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
	getTotalCPUTime := func(record *tipb.CPUTimeRecord) int {
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
	tsr := setupRemoteTopSQLReporter(maxSQLNum, 60, "")
	defer tsr.Close()

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
	tsr.doCollect(collectedData, 1, genRecord(20000))
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
	evict := &dataPoints{}
	evict.TimestampList = []uint64{3, 2, 4}
	evict.CPUTimeMsList = []uint32{30, 20, 40}
	evict.CPUTimeMsTotal = 90
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
	sort.Sort(d)
	require.Equal(t, []uint64{1, 2, 3, 4, 5, 6}, d.TimestampList)
	require.Equal(t, []uint32{10, 20, 30, 40, 50, 60}, d.CPUTimeMsList)

	// test for dataPoints merge.
	d = &dataPoints{}
	evict := &dataPoints{}
	addEvictedIntoSortedDataPoints(d, evict)
	evict.TimestampList = []uint64{1, 3}
	evict.CPUTimeMsList = []uint32{10, 30}
	evict.CPUTimeMsTotal = 40
	addEvictedIntoSortedDataPoints(d, evict)
	require.Equal(t, uint64(40), d.CPUTimeMsTotal)
	require.Equal(t, []uint64{1, 3}, d.TimestampList)
	require.Equal(t, []uint32{10, 30}, d.CPUTimeMsList)

	evict.TimestampList = []uint64{1, 2, 3, 4, 5}
	evict.CPUTimeMsList = []uint32{10, 20, 30, 40, 50}
	evict.CPUTimeMsTotal = 150
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

	tsr := setupRemoteTopSQLReporter(3000, 1, agentServer.Address())
	defer tsr.Close()

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

func BenchmarkTopSQL_CollectAndIncrementFrequency(b *testing.B) {
	tsr := initializeCache(maxSQLNum, 120, ":23333")
	for i := 0; i < b.N; i++ {
		populateCache(tsr, 0, maxSQLNum, uint64(i))
	}
}

func BenchmarkTopSQL_CollectAndEvict(b *testing.B) {
	tsr := initializeCache(maxSQLNum, 120, ":23333")
	begin := 0
	end := maxSQLNum
	for i := 0; i < b.N; i++ {
		begin += maxSQLNum
		end += maxSQLNum
		populateCache(tsr, begin, end, uint64(i))
	}
}
