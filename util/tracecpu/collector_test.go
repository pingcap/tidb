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

package tracecpu

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tipb/go-tipb"
	"google.golang.org/grpc"
)

const (
	maxSQLNum = 5000
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testTopSQLCollector{})

type testTopSQLCollector struct{}

func (s *testTopSQLCollector) SetUpSuite(c *C) {}

func (s *testTopSQLCollector) SetUpTest(c *C) {}

func testPlanBinaryDecoderFunc(plan string) (string, error) {
	return plan, nil
}

func populateCache(ts *TopSQLCollector, begin, end int, timestamp uint64) {
	// register normalized sql
	for i := begin; i < end; i++ {
		key := "sqlDigest" + strconv.Itoa(i+1)
		value := "sqlNormalized" + strconv.Itoa(i+1)
		ts.RegisterNormalizedSQL(key, value)
	}
	// register normalized plan
	for i := begin; i < end; i++ {
		key := "planDigest" + strconv.Itoa(i+1)
		value := "planNormalized" + strconv.Itoa(i+1)
		ts.RegisterNormalizedPlan(key, value)
	}
	// collect
	var records []TopSQLRecord
	for i := begin; i < end; i++ {
		records = append(records, TopSQLRecord{
			SQLDigest:  []byte("sqlDigest" + strconv.Itoa(i+1)),
			PlanDigest: []byte("planDigest" + strconv.Itoa(i+1)),
			CPUTimeMs:  uint32(i + 1),
		})
	}
	ts.Collect(timestamp, records)
}

func initializeCache(maxSQLNum int, addr string) *TopSQLCollector {
	config := &TopSQLCollectorConfig{
		PlanBinaryDecoder:   testPlanBinaryDecoderFunc,
		MaxSQLNum:           maxSQLNum,
		SendToAgentInterval: time.Minute,
		AgentGRPCAddress:    addr,
		InstanceID:          "tidb-server",
	}
	ts := NewTopSQLCollector(config)
	populateCache(ts, 0, maxSQLNum, 1)
	return ts
}

type testAgentServer struct {
	batch []*tipb.CollectCPUTimeRequest
}

func (svr *testAgentServer) CollectCPUTime(stream tipb.TopSQLAgent_CollectCPUTimeServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		svr.batch = append(svr.batch, req)
	}
	resp := &tipb.CollectCPUTimeResponse{}
	stream.SendAndClose(resp)
	return nil
}

func startTestServer(c *C) (*grpc.Server, *testAgentServer, int) {
	addr := ":0"
	lis, err := net.Listen("tcp", addr)
	c.Assert(err, IsNil, Commentf("failed to listen to address %s", addr))
	server := grpc.NewServer()
	agentServer := &testAgentServer{}
	tipb.RegisterTopSQLAgentServer(server, agentServer)

	go func() {
		err := server.Serve(lis)
		c.Assert(err, IsNil, Commentf("failed to start server"))
	}()

	return server, agentServer, lis.Addr().(*net.TCPAddr).Port
}

func (s *testTopSQLCollector) TestCollectAndGet(c *C) {
	ts := initializeCache(maxSQLNum, ":23333")
	for i := 0; i < maxSQLNum; i++ {
		sqlDigest := []byte("sqlDigest" + strconv.Itoa(i+1))
		planDigest := []byte("planDigest" + strconv.Itoa(i+1))
		encodedKey := encodeCacheKey(sqlDigest, planDigest)
		entry := ts.topSQLMap[string(encodedKey)]
		c.Assert(entry.CPUTimeMsList[0], Equals, uint32(i+1))
		c.Assert(entry.TimestampList[0], Equals, uint64(1))
	}
}

func (s *testTopSQLCollector) TestCollectAndVerifyFrequency(c *C) {
	ts := initializeCache(maxSQLNum, ":23333")
	// traverse the map, and check CPU time and content
	for i := 0; i < maxSQLNum; i++ {
		sqlDigest := []byte("sqlDigest" + strconv.Itoa(i+1))
		planDigest := []byte("planDigest" + strconv.Itoa(i+1))
		encodedKey := encodeCacheKey(sqlDigest, planDigest)
		value, exist := ts.topSQLMap[string(encodedKey)]
		c.Assert(exist, Equals, true)
		c.Assert(value.CPUTimeMsTotal, Equals, uint64(i+1))
		c.Assert(len(value.CPUTimeMsList), Equals, 1)
		c.Assert(len(value.TimestampList), Equals, 1)
		c.Assert(value.CPUTimeMsList[0], Equals, uint32(i+1))
		c.Assert(value.TimestampList[0], Equals, uint64(1))
	}
}

func (s *testTopSQLCollector) TestCollectAndEvict(c *C) {
	ts := initializeCache(maxSQLNum, ":23333")
	// Collect maxSQLNum records with timestamp 2 and sql plan digest from maxSQLNum/2 to maxSQLNum/2*3.
	populateCache(ts, maxSQLNum/2, maxSQLNum/2*3, 2)
	// The first maxSQLNum/2 sql plan digest should have been evicted
	for i := 0; i < maxSQLNum/2; i++ {
		sqlDigest := []byte("sqlDigest" + strconv.Itoa(i+1))
		planDigest := []byte("planDigest" + strconv.Itoa(i+1))
		encodedKey := encodeCacheKey(sqlDigest, planDigest)
		_, exist := ts.topSQLMap[string(encodedKey)]
		c.Assert(exist, Equals, false, Commentf("cache key '%' should be evicted", encodedKey))
		_, exist = ts.normalizedSQLMap[string(sqlDigest)]
		c.Assert(exist, Equals, false, Commentf("normalized SQL with digest '%s' should be evicted", sqlDigest))
		_, exist = ts.normalizedPlanMap[string(planDigest)]
		c.Assert(exist, Equals, false, Commentf("normalized plan with digest '%s' should be evicted", planDigest))
	}
	// Because CPU time is populated as i+1,
	// we should expect digest maxSQLNum/2+1 - maxSQLNum to have CPU time maxSQLNum+2, maxSQLNum+4, ..., maxSQLNum*2
	// and digest maxSQLNum+1 - maxSQLNum/2*3 to have CPU time maxSQLNum+1, maxSQLNum+2, ..., maxSQLNum/2*3.
	for i := maxSQLNum / 2; i < maxSQLNum/2*3; i++ {
		sqlDigest := []byte("sqlDigest" + strconv.Itoa(i+1))
		planDigest := []byte("planDigest" + strconv.Itoa(i+1))
		encodedKey := encodeCacheKey(sqlDigest, planDigest)
		value, exist := ts.topSQLMap[string(encodedKey)]
		c.Assert(exist, Equals, true, Commentf("cache key '%s' should exist", encodedKey))
		if i < maxSQLNum {
			c.Assert(value.CPUTimeMsTotal, Equals, uint64((i+1)*2))
		} else {
			c.Assert(value.CPUTimeMsTotal, Equals, uint64(i+1))
		}
	}
}

func (s *testTopSQLCollector) TestCollectAndSnapshot(c *C) {
	ts := initializeCache(maxSQLNum, ":23333")
	batch := ts.snapshot()
	for _, req := range batch {
		sqlDigest := req.SqlDigest
		planDigest := req.PlanDigest
		encodedKey := encodeCacheKey(sqlDigest, planDigest)
		value, exist := ts.topSQLMap[string(encodedKey)]
		c.Assert(exist, Equals, true, Commentf("key '%s' should exist", string(encodedKey)))
		c.Assert(len(req.CpuTimeMsList), Equals, len(value.CPUTimeMsList))
		for i, ct := range value.CPUTimeMsList {
			c.Assert(req.CpuTimeMsList[i], Equals, ct)
		}
		c.Assert(len(req.TimestampList), Equals, len(value.TimestampList))
		for i, ts := range value.TimestampList {
			c.Assert(req.TimestampList[i], Equals, ts)
		}
	}
}

func (s *testTopSQLCollector) TestCollectAndSendBatch(c *C) {
	server, agentServer, port := startTestServer(c)
	c.Logf("server is listening on :%d", port)
	defer server.Stop()

	ts := initializeCache(maxSQLNum, fmt.Sprintf(":%d", port))
	batch := ts.snapshot()

	conn, stream, cancel, err := newAgentClient(ts.agentGRPCAddress, 30*time.Second)
	c.Assert(err, IsNil, Commentf("failed to create agent client"))
	defer cancel()
	err = ts.sendBatch(stream, batch)
	c.Assert(err, IsNil, Commentf("failed to send batch to server"))
	err = conn.Close()
	c.Assert(err, IsNil, Commentf("failed to close connection"))

	// check for equality of server received batch and the original data
	for _, req := range agentServer.batch {
		encodedKey := encodeCacheKey(req.SqlDigest, req.PlanDigest)
		value, exist := ts.topSQLMap[string(encodedKey)]
		c.Assert(exist, Equals, true, Commentf("key '%s' should exist in topSQLMap", string(encodedKey)))
		for i, ct := range value.CPUTimeMsList {
			c.Assert(req.CpuTimeMsList[i], Equals, ct)
		}
		for i, ts := range value.TimestampList {
			c.Assert(req.TimestampList[i], Equals, ts)
		}
		normalizedSQL, exist := ts.normalizedSQLMap[string(req.SqlDigest)]
		c.Assert(exist, Equals, true, Commentf("key '%s' should exist in normalizedSQLMap", req.SqlDigest))
		c.Assert(req.NormalizedSql, Equals, normalizedSQL)
		normalizedPlan, exist := ts.normalizedPlanMap[string(req.PlanDigest)]
		c.Assert(exist, Equals, true, Commentf("key '%s' should exist in normalizedPlanMap", req.PlanDigest))
		c.Assert(req.NormalizedPlan, Equals, normalizedPlan)
	}
}

func BenchmarkTopSQL_CollectAndIncrementFrequency(b *testing.B) {
	ts := initializeCache(maxSQLNum, ":23333")
	for i := 0; i < b.N; i++ {
		populateCache(ts, 0, maxSQLNum, uint64(i))
	}
}

func BenchmarkTopSQL_CollectAndEvict(b *testing.B) {
	ts := initializeCache(maxSQLNum, ":23333")
	begin := 0
	end := maxSQLNum
	for i := 0; i < b.N; i++ {
		begin += maxSQLNum
		end += maxSQLNum
		populateCache(ts, begin, end, uint64(i))
	}
}
