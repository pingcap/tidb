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
	"strconv"
	"strings"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/topsql/reporter/mock"
	"github.com/pingcap/tidb/util/topsql/tracecpu"
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
		tsr.RegisterSQL(key, value)
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
		normalizedSQL, exist := agentServer.GetSQLMetaByDigestBlocking(req.SqlDigest, time.Second)
		c.Assert(exist, IsTrue)
		c.Assert(normalizedSQL, Equals, "sqlNormalized"+strconv.Itoa(id))
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
		c.Assert(id >= maxSQLNum, IsTrue)
		c.Assert(req.RecordListCpuTimeMs, HasLen, 1)
		for i := range req.RecordListCpuTimeMs {
			c.Assert(req.RecordListCpuTimeMs[i], Equals, uint32(id))
		}
		c.Assert(req.RecordListTimestampSec, HasLen, 1)
		for i := range req.RecordListTimestampSec {
			c.Assert(req.RecordListTimestampSec[i], Equals, uint64(2))
		}
		normalizedSQL, exist := agentServer.GetSQLMetaByDigestBlocking(req.SqlDigest, time.Second)
		c.Assert(exist, IsTrue)
		c.Assert(normalizedSQL, Equals, "sqlNormalized"+strconv.Itoa(id))
		normalizedPlan, exist := agentServer.GetPlanMetaByDigestBlocking(req.PlanDigest, time.Second)
		c.Assert(exist, IsTrue)
		c.Assert(normalizedPlan, Equals, "planNormalized"+strconv.Itoa(id))
	}
}

func (s *testTopSQLReporter) TestCollectCapacity(c *C) {
	tsr := setupRemoteTopSQLReporter(maxSQLNum, 60, "")
	defer tsr.Close()

	registerSQL := func(n int) {
		for i := 0; i < n; i++ {
			key := []byte("sqlDigest" + strconv.Itoa(i))
			value := "sqlNormalized" + strconv.Itoa(i)
			tsr.RegisterSQL(key, value)
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
	c.Assert(len(collectedData), Equals, 5000)
	c.Assert(tsr.sqlMapLength.Load(), Equals, int64(5000))
	c.Assert(tsr.planMapLength.Load(), Equals, int64(5000))

	tsr.takeDataAndSendToReportChan(&collectedData)
	c.Assert(len(collectedData), Equals, 0)
	c.Assert(tsr.sqlMapLength.Load(), Equals, int64(0))
	c.Assert(tsr.planMapLength.Load(), Equals, int64(0))
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
