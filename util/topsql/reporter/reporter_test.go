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
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/topsql/tracecpu"
	"github.com/pingcap/tipb/go-tipb"
	"google.golang.org/grpc"
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

func testPlanBinaryDecoderFunc(plan string) (string, error) {
	return plan, nil
}

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
	// sleep a while for the asynchronouse collect
	time.Sleep(100 * time.Millisecond)
}

func initializeCache(maxStatementsNum, interval int, addr string) *RemoteTopSQLReporter {
	variable.TopSQLVariable.MaxStatementCount.Store(int64(maxStatementsNum))
	variable.TopSQLVariable.ReportIntervalSeconds.Store(int64(interval))
	variable.TopSQLVariable.AgentAddress.Store(addr)

	rc := NewReportGRPCClient()
	ts := NewRemoteTopSQLReporter(rc, testPlanBinaryDecoderFunc)
	populateCache(ts, 0, maxStatementsNum, 1)
	return ts
}

func setupRemoteTopSQLReporter(maxStatementsNum, interval int, addr string) *RemoteTopSQLReporter {
	variable.TopSQLVariable.MaxStatementCount.Store(int64(maxStatementsNum))
	variable.TopSQLVariable.ReportIntervalSeconds.Store(int64(interval))
	variable.TopSQLVariable.AgentAddress.Store(addr)

	rc := NewReportGRPCClient()
	ts := NewRemoteTopSQLReporter(rc, testPlanBinaryDecoderFunc)
	return ts
}

type testAgentServer struct {
	addr      string
	sqlMetas  map[string]string
	planMetas map[string]string
	records   []*tipb.CPUTimeRecord
}

func (svr *testAgentServer) ReportCPUTimeRecords(stream tipb.TopSQLAgent_ReportCPUTimeRecordsServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		svr.records = append(svr.records, req)
	}
	return stream.SendAndClose(&tipb.EmptyResponse{})
}

func (svr *testAgentServer) ReportSQLMeta(stream tipb.TopSQLAgent_ReportSQLMetaServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		svr.sqlMetas[string(req.SqlDigest)] = req.NormalizedSql
	}
	return stream.SendAndClose(&tipb.EmptyResponse{})
}

func (svr *testAgentServer) ReportPlanMeta(stream tipb.TopSQLAgent_ReportPlanMetaServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		svr.planMetas[string(req.PlanDigest)] = req.NormalizedPlan
	}
	return stream.SendAndClose(&tipb.EmptyResponse{})
}

func startTestServer(c *C) (*grpc.Server, *testAgentServer) {
	addr := "127.0.0.1:0"
	lis, err := net.Listen("tcp", addr)
	c.Assert(err, IsNil, Commentf("failed to listen to address %s", addr))
	server := grpc.NewServer()
	agentServer := &testAgentServer{
		addr:      fmt.Sprintf("127.0.0.1:%d", lis.Addr().(*net.TCPAddr).Port),
		sqlMetas:  make(map[string]string, maxSQLNum),
		planMetas: make(map[string]string, maxSQLNum),
	}
	tipb.RegisterTopSQLAgentServer(server, agentServer)

	go func() {
		err := server.Serve(lis)
		c.Assert(err, IsNil, Commentf("failed to start server"))
	}()

	return server, agentServer
}

//func (s *testTopSQLReporter) TestCollectAndGet(c *C) {
//	tsr := initializeCache(maxSQLNum, ":23333")
//	for i := 0; i < maxSQLNum; i++ {
//		sqlDigest := []byte("sqlDigest" + strconv.Itoa(i+1))
//		planDigest := []byte("planDigest" + strconv.Itoa(i+1))
//		encodedKey := encodeKey(sqlDigest, planDigest)
//		entry := tsr.topSQLMap[string(encodedKey)]
//		c.Assert(entry.CPUTimeMsList[0], Equals, uint32(i+1))
//		c.Assert(entry.TimestampList[0], Equals, uint64(1))
//	}
//}

//func (s *testTopSQLReporter) TestCollectAndVerifyFrequency(c *C) {
//	tsr := initializeCache(maxSQLNum, ":23333")
//	// traverse the map, and check CPU time and content
//	for i := 0; i < maxSQLNum; i++ {
//		sqlDigest := []byte("sqlDigest" + strconv.Itoa(i+1))
//		planDigest := []byte("planDigest" + strconv.Itoa(i+1))
//		encodedKey := encodeKey(sqlDigest, planDigest)
//		value, exist := tsr.topSQLMap[string(encodedKey)]
//		c.Assert(exist, Equals, true)
//		c.Assert(value.CPUTimeMsTotal, Equals, uint64(i+1))
//		c.Assert(len(value.CPUTimeMsList), Equals, 1)
//		c.Assert(len(value.TimestampList), Equals, 1)
//		c.Assert(value.CPUTimeMsList[0], Equals, uint32(i+1))
//		c.Assert(value.TimestampList[0], Equals, uint64(1))
//	}
//}
//

//func (s *testTopSQLReporter) TestCollectAndEvict(c *C) {
//	tsr := initializeCache(maxSQLNum, ":23333")
//	// Collect maxSQLNum records with timestamp 2 and sql plan digest from maxSQLNum/2 to maxSQLNum/2*3.
//	populateCache(tsr, maxSQLNum/2, maxSQLNum/2*3, 2)
//	// The first maxSQLNum/2 sql plan digest should have been evicted
//	for i := 0; i < maxSQLNum/2; i++ {
//		sqlDigest := []byte("sqlDigest" + strconv.Itoa(i+1))
//		planDigest := []byte("planDigest" + strconv.Itoa(i+1))
//		encodedKey := encodeKey(sqlDigest, planDigest)
//		_, exist := tsr.topSQLMap[encodedKey]
//		c.Assert(exist, Equals, false, Commentf("cache key '%' should be evicted", encodedKey))
//		_, exist = tsr.normalizedSQLMap.Load().(*sync.Map).Load(string(sqlDigest))
//		c.Assert(exist, Equals, false, Commentf("normalized SQL with digest '%s' should be evicted", sqlDigest))
//		_, exist = tsr.normalizedPlanMap.Load().(*sync.Map).Load(string(planDigest))
//		c.Assert(exist, Equals, false, Commentf("normalized plan with digest '%s' should be evicted", planDigest))
//	}
//	// Because CPU time is populated as i+1,
//	// we should expect digest maxSQLNum/2+1 - maxSQLNum to have CPU time maxSQLNum+2, maxSQLNum+4, ..., maxSQLNum*2
//	// and digest maxSQLNum+1 - maxSQLNum/2*3 to have CPU time maxSQLNum+1, maxSQLNum+2, ..., maxSQLNum/2*3.
//	for i := maxSQLNum / 2; i < maxSQLNum/2*3; i++ {
//		sqlDigest := []byte("sqlDigest" + strconv.Itoa(i+1))
//		planDigest := []byte("planDigest" + strconv.Itoa(i+1))
//		encodedKey := encodeKey(sqlDigest, planDigest)
//		value, exist := tsr.topSQLMap[string(encodedKey)]
//		c.Assert(exist, Equals, true, Commentf("cache key '%s' should exist", encodedKey))
//		if i < maxSQLNum {
//			c.Assert(value.CPUTimeMsTotal, Equals, uint64((i+1)*2))
//		} else {
//			c.Assert(value.CPUTimeMsTotal, Equals, uint64(i+1))
//		}
//	}
//}

func (s *testTopSQLReporter) TestCollectAndSendBatch(c *C) {
	server, agentServer := startTestServer(c)
	c.Logf("server is listening on %v", agentServer.addr)
	defer server.Stop()

	tsr := setupRemoteTopSQLReporter(maxSQLNum, 1, agentServer.addr)
	populateCache(tsr, 0, maxSQLNum, 1)

	time.Sleep(2 * time.Second)

	c.Assert(agentServer.records, HasLen, maxSQLNum)

	// check for equality of server received batch and the original data
	for _, req := range agentServer.records {
		id := 0
		prefix := "sqlDigest"
		if strings.HasPrefix(string(req.SqlDigest), prefix) {
			n, err := strconv.Atoi(string(req.SqlDigest)[len(prefix):])
			c.Assert(err, IsNil)
			id = n
		}
		c.Assert(req.CpuTimeMsList, HasLen, 1)
		for i := range req.CpuTimeMsList {
			c.Assert(req.CpuTimeMsList[i], Equals, uint32(id))
		}
		c.Assert(req.TimestampList, HasLen, 1)
		for i := range req.TimestampList {
			c.Assert(req.TimestampList[i], Equals, uint64(1))
		}
		normalizedSQL, exist := agentServer.sqlMetas[string(req.SqlDigest)]
		c.Assert(exist, IsTrue)
		c.Assert(normalizedSQL, Equals, "sqlNormalized"+strconv.Itoa(id))
		normalizedPlan, exist := agentServer.planMetas[string(req.PlanDigest)]
		c.Assert(exist, IsTrue)
		c.Assert(normalizedPlan, Equals, "planNormalized"+strconv.Itoa(id))
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
