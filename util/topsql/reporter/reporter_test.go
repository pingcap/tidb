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
// See the License for the specific language governing permissions and
// limitations under the License.

package reporter

import (
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/topsql/reporter/mock"
	"github.com/pingcap/tidb/util/topsql/tracecpu"
	"github.com/pingcap/tipb/go-tipb"
)

const (
	maxSQLNum = 5000
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = SerialSuites(&testTopSQLReporter{})

type testTopSQLReporter struct{}

func (s *testTopSQLReporter) SetUpSuite(c *C) {}

func (s *testTopSQLReporter) SetUpTest(c *C) {}

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
	variable.TopSQLVariable.ReportIntervalSeconds.Store(int64(interval))
	variable.TopSQLVariable.AgentAddress.Store(addr)

	rc := NewGRPCReportClient(mockPlanBinaryDecoderFunc)
	ts := NewRemoteTopSQLReporter(rc)
	return ts
}

func initializeCache(maxStatementsNum, interval int, addr string) *RemoteTopSQLReporter {
	ts := setupRemoteTopSQLReporter(maxStatementsNum, interval, addr)
	populateCache(ts, 0, maxStatementsNum, 1)
	return ts
}

func (s *testTopSQLReporter) TestCollectAndSendBatch(c *C) {
	agentServer, err := mock.StartMockAgentServer()
	c.Assert(err, IsNil)
	defer agentServer.Stop()

	tsr := setupRemoteTopSQLReporter(maxSQLNum, 1, agentServer.Address())
	defer tsr.Close()
	populateCache(tsr, 0, maxSQLNum, 1)

	agentServer.WaitCollectCnt(1, time.Second*5)

	c.Assert(agentServer.GetLatestRecords(), HasLen, maxSQLNum)

	// check for equality of server received batch and the original data
	records := agentServer.GetLatestRecords()
	for _, req := range records {
		id := 0
		prefix := "sqlDigest"
		if strings.HasPrefix(string(req.SqlDigest), prefix) {
			n, err := strconv.Atoi(string(req.SqlDigest)[len(prefix):])
			c.Assert(err, IsNil)
			id = n
		}
		c.Assert(req.RecordListCpuTimeMs, HasLen, 1)
		for i := range req.RecordListCpuTimeMs {
			c.Assert(req.RecordListCpuTimeMs[i], Equals, uint32(id))
		}
		c.Assert(req.RecordListTimestampSec, HasLen, 1)
		for i := range req.RecordListTimestampSec {
			c.Assert(req.RecordListTimestampSec[i], Equals, uint64(1))
		}
		sqlMeta, exist := agentServer.GetSQLMetaByDigestBlocking(req.SqlDigest, time.Second)
		c.Assert(exist, IsTrue)
		c.Assert(sqlMeta.NormalizedSql, Equals, "sqlNormalized"+strconv.Itoa(id))
		normalizedPlan, exist := agentServer.GetPlanMetaByDigestBlocking(req.PlanDigest, time.Second)
		c.Assert(exist, IsTrue)
		c.Assert(normalizedPlan, Equals, "planNormalized"+strconv.Itoa(id))
	}
}

func (s *testTopSQLReporter) TestCollectAndEvicted(c *C) {
	agentServer, err := mock.StartMockAgentServer()
	c.Assert(err, IsNil)
	defer agentServer.Stop()

	tsr := setupRemoteTopSQLReporter(maxSQLNum, 1, agentServer.Address())
	defer tsr.Close()
	populateCache(tsr, 0, maxSQLNum*2, 2)

	agentServer.WaitCollectCnt(1, time.Second*10)

	// check for equality of server received batch and the original data
	records := agentServer.GetLatestRecords()
	c.Assert(records, HasLen, maxSQLNum+1)
	for _, req := range records {
		id := 0
		prefix := "sqlDigest"
		if strings.HasPrefix(string(req.SqlDigest), prefix) {
			n, err := strconv.Atoi(string(req.SqlDigest)[len(prefix):])
			c.Assert(err, IsNil)
			id = n
		}
		c.Assert(req.RecordListTimestampSec, HasLen, 1)
		c.Assert(req.RecordListTimestampSec[0], Equals, uint64(2))
		c.Assert(req.RecordListCpuTimeMs, HasLen, 1)
		if id == 0 {
			// test for others
			c.Assert(req.SqlDigest, IsNil)
			c.Assert(req.PlanDigest, IsNil)
			// 12502500 is the sum of all evicted item's cpu time. 1 + 2 + 3 + ... + 5000 = (1 + 5000) * 2500 = 12502500
			c.Assert(int(req.RecordListCpuTimeMs[0]), Equals, 12502500)
			continue
		}
		c.Assert(id > maxSQLNum, IsTrue)
		c.Assert(req.RecordListCpuTimeMs[0], Equals, uint32(id))
		sqlMeta, exist := agentServer.GetSQLMetaByDigestBlocking(req.SqlDigest, time.Second)
		c.Assert(exist, IsTrue)
		c.Assert(sqlMeta.NormalizedSql, Equals, "sqlNormalized"+strconv.Itoa(id))
		normalizedPlan, exist := agentServer.GetPlanMetaByDigestBlocking(req.PlanDigest, time.Second)
		c.Assert(exist, IsTrue)
		c.Assert(normalizedPlan, Equals, "planNormalized"+strconv.Itoa(id))
	}
}

func (s *testTopSQLReporter) newSQLCPUTimeRecord(tsr *RemoteTopSQLReporter, sqlID int, cpuTimeMs uint32) tracecpu.SQLCPUTimeRecord {
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

func (s *testTopSQLReporter) collectAndWait(tsr *RemoteTopSQLReporter, timestamp uint64, records []tracecpu.SQLCPUTimeRecord) {
	tsr.Collect(timestamp, records)
	time.Sleep(time.Millisecond * 100)
}

func (s *testTopSQLReporter) TestCollectAndTopN(c *C) {
	agentServer, err := mock.StartMockAgentServer()
	c.Assert(err, IsNil)
	defer agentServer.Stop()

	tsr := setupRemoteTopSQLReporter(2, 1, agentServer.Address())
	defer tsr.Close()

	records := []tracecpu.SQLCPUTimeRecord{
		s.newSQLCPUTimeRecord(tsr, 1, 1),
		s.newSQLCPUTimeRecord(tsr, 2, 2),
	}
	s.collectAndWait(tsr, 1, records)

	records = []tracecpu.SQLCPUTimeRecord{
		s.newSQLCPUTimeRecord(tsr, 3, 3),
		s.newSQLCPUTimeRecord(tsr, 1, 1),
	}
	s.collectAndWait(tsr, 2, records)

	records = []tracecpu.SQLCPUTimeRecord{
		s.newSQLCPUTimeRecord(tsr, 4, 1),
		s.newSQLCPUTimeRecord(tsr, 1, 1),
	}
	s.collectAndWait(tsr, 3, records)

	records = []tracecpu.SQLCPUTimeRecord{
		s.newSQLCPUTimeRecord(tsr, 5, 1),
		s.newSQLCPUTimeRecord(tsr, 1, 1),
	}
	s.collectAndWait(tsr, 4, records)

	// Test for time jump back.
	records = []tracecpu.SQLCPUTimeRecord{
		s.newSQLCPUTimeRecord(tsr, 6, 1),
		s.newSQLCPUTimeRecord(tsr, 1, 1),
	}
	s.collectAndWait(tsr, 0, records)

	// Wait agent server collect finish.
	agentServer.WaitCollectCnt(1, time.Second*10)

	// check for equality of server received batch and the original data
	results := agentServer.GetLatestRecords()
	c.Assert(results, HasLen, 3)
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
	c.Assert(results[0].SqlDigest, IsNil)
	c.Assert(getTotalCPUTime(results[0]), Equals, 5)
	c.Assert(results[0].RecordListTimestampSec, DeepEquals, []uint64{0, 1, 3, 4})
	c.Assert(results[0].RecordListCpuTimeMs, DeepEquals, []uint32{1, 2, 1, 1})
	c.Assert(results[1].SqlDigest, DeepEquals, []byte("sqlDigest1"))
	c.Assert(getTotalCPUTime(results[1]), Equals, 5)
	c.Assert(results[2].SqlDigest, DeepEquals, []byte("sqlDigest3"))
	c.Assert(getTotalCPUTime(results[2]), Equals, 3)
}

func (s *testTopSQLReporter) TestCollectCapacity(c *C) {
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
	c.Assert(tsr.sqlMapLength.Load(), Equals, int64(5000))
	registerPlan(1000)
	c.Assert(tsr.planMapLength.Load(), Equals, int64(1000))

	registerSQL(20000)
	c.Assert(tsr.sqlMapLength.Load(), Equals, int64(10000))
	registerPlan(20000)
	c.Assert(tsr.planMapLength.Load(), Equals, int64(10000))

	variable.TopSQLVariable.MaxCollect.Store(20000)
	registerSQL(50000)
	c.Assert(tsr.sqlMapLength.Load(), Equals, int64(20000))
	registerPlan(50000)
	c.Assert(tsr.planMapLength.Load(), Equals, int64(20000))

	variable.TopSQLVariable.MaxStatementCount.Store(5000)
	collectedData := make(map[string]*dataPoints)
	tsr.doCollect(collectedData, 1, genRecord(20000))
	c.Assert(len(collectedData), Equals, 5001)
	c.Assert(tsr.sqlMapLength.Load(), Equals, int64(5000))
	c.Assert(tsr.planMapLength.Load(), Equals, int64(5000))
}

func (s *testTopSQLReporter) TestCollectOthers(c *C) {
	collectTarget := make(map[string]*dataPoints)
	addEvictedCPUTime(collectTarget, 1, 10)
	addEvictedCPUTime(collectTarget, 2, 20)
	addEvictedCPUTime(collectTarget, 3, 30)
	others := collectTarget[keyOthers]
	c.Assert(others.CPUTimeMsTotal, Equals, uint64(60))
	c.Assert(others.TimestampList, DeepEquals, []uint64{1, 2, 3})
	c.Assert(others.CPUTimeMsList, DeepEquals, []uint32{10, 20, 30})

	others = addEvictedIntoSortedDataPoints(nil, others)
	c.Assert(others.CPUTimeMsTotal, Equals, uint64(60))

	// test for time jump backward.
	evict := &dataPoints{}
	evict.TimestampList = []uint64{3, 2, 4}
	evict.CPUTimeMsList = []uint32{30, 20, 40}
	evict.CPUTimeMsTotal = 90
	others = addEvictedIntoSortedDataPoints(others, evict)
	c.Assert(others.CPUTimeMsTotal, Equals, uint64(150))
	c.Assert(others.TimestampList, DeepEquals, []uint64{1, 2, 3, 4})
	c.Assert(others.CPUTimeMsList, DeepEquals, []uint32{10, 40, 60, 40})
}

func (s *testTopSQLReporter) TestDataPoints(c *C) {
	// test for dataPoints invalid.
	d := &dataPoints{}
	d.TimestampList = []uint64{1}
	d.CPUTimeMsList = []uint32{10, 30}
	c.Assert(d.isInvalid(), Equals, true)

	// test for dataPoints sort.
	d = &dataPoints{}
	d.TimestampList = []uint64{1, 2, 5, 6, 3, 4}
	d.CPUTimeMsList = []uint32{10, 20, 50, 60, 30, 40}
	sort.Sort(d)
	c.Assert(d.TimestampList, DeepEquals, []uint64{1, 2, 3, 4, 5, 6})
	c.Assert(d.CPUTimeMsList, DeepEquals, []uint32{10, 20, 30, 40, 50, 60})

	// test for dataPoints merge.
	d = &dataPoints{}
	evict := &dataPoints{}
	addEvictedIntoSortedDataPoints(d, evict)
	evict.TimestampList = []uint64{1, 3}
	evict.CPUTimeMsList = []uint32{10, 30}
	evict.CPUTimeMsTotal = 40
	addEvictedIntoSortedDataPoints(d, evict)
	c.Assert(d.CPUTimeMsTotal, Equals, uint64(40))
	c.Assert(d.TimestampList, DeepEquals, []uint64{1, 3})
	c.Assert(d.CPUTimeMsList, DeepEquals, []uint32{10, 30})

	evict.TimestampList = []uint64{1, 2, 3, 4, 5}
	evict.CPUTimeMsList = []uint32{10, 20, 30, 40, 50}
	evict.CPUTimeMsTotal = 150
	addEvictedIntoSortedDataPoints(d, evict)
	c.Assert(d.CPUTimeMsTotal, Equals, uint64(190))
	c.Assert(d.TimestampList, DeepEquals, []uint64{1, 2, 3, 4, 5})
	c.Assert(d.CPUTimeMsList, DeepEquals, []uint32{20, 20, 60, 40, 50})

	// test for time jump backward.
	d = &dataPoints{}
	evict = &dataPoints{}
	evict.TimestampList = []uint64{3, 2}
	evict.CPUTimeMsList = []uint32{30, 20}
	evict.CPUTimeMsTotal = 50
	addEvictedIntoSortedDataPoints(d, evict)
	c.Assert(d.CPUTimeMsTotal, Equals, uint64(50))
	c.Assert(d.TimestampList, DeepEquals, []uint64{2, 3})
	c.Assert(d.CPUTimeMsList, DeepEquals, []uint32{20, 30})

	// test for merge invalid dataPoints
	d = &dataPoints{}
	evict = &dataPoints{}
	evict.TimestampList = []uint64{1}
	evict.CPUTimeMsList = []uint32{10, 30}
	c.Assert(evict.isInvalid(), Equals, true)
	addEvictedIntoSortedDataPoints(d, evict)
	c.Assert(d.isInvalid(), Equals, false)
	c.Assert(d.CPUTimeMsList, IsNil)
	c.Assert(d.TimestampList, IsNil)
}

func (s *testTopSQLReporter) TestCollectInternal(c *C) {
	agentServer, err := mock.StartMockAgentServer()
	c.Assert(err, IsNil)
	defer agentServer.Stop()

	tsr := setupRemoteTopSQLReporter(3000, 1, agentServer.Address())
	defer tsr.Close()

	records := []tracecpu.SQLCPUTimeRecord{
		s.newSQLCPUTimeRecord(tsr, 1, 1),
		s.newSQLCPUTimeRecord(tsr, 2, 2),
	}
	s.collectAndWait(tsr, 1, records)

	// Wait agent server collect finish.
	agentServer.WaitCollectCnt(1, time.Second*10)

	// check for equality of server received batch and the original data
	results := agentServer.GetLatestRecords()
	c.Assert(results, HasLen, 2)
	for _, req := range results {
		id := 0
		prefix := "sqlDigest"
		if strings.HasPrefix(string(req.SqlDigest), prefix) {
			n, err := strconv.Atoi(string(req.SqlDigest)[len(prefix):])
			c.Assert(err, IsNil)
			id = n
		}
		if id == 0 {
			c.Fatalf("the id should not be 0")
		}
		sqlMeta, exist := agentServer.GetSQLMetaByDigestBlocking(req.SqlDigest, time.Second)
		c.Assert(exist, IsTrue)
		c.Assert(sqlMeta.IsInternalSql, Equals, id%2 == 0)
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
