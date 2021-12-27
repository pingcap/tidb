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

package mock

import (
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type mockAgentServer struct {
	sync.Mutex
	addr       string
	grpcServer *grpc.Server
	sqlMetas   map[string]tipb.SQLMeta
	planMetas  map[string]string
	records    [][]*tipb.TopSQLRecord
	hang       struct {
		beginTime atomic.Value // time.Time
		endTime   atomic.Value // time.Time
	}
}

// StartMockAgentServer starts the mock agent server.
func StartMockAgentServer() (*mockAgentServer, error) {
	addr := "127.0.0.1:0"
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	server := grpc.NewServer()
	agentServer := &mockAgentServer{
		addr:       fmt.Sprintf("127.0.0.1:%d", lis.Addr().(*net.TCPAddr).Port),
		grpcServer: server,
		sqlMetas:   make(map[string]tipb.SQLMeta, 5000),
		planMetas:  make(map[string]string, 5000),
	}
	agentServer.hang.beginTime.Store(time.Now())
	agentServer.hang.endTime.Store(time.Now())
	tipb.RegisterTopSQLAgentServer(server, agentServer)

	go func() {
		err := server.Serve(lis)
		if err != nil {
			logutil.BgLogger().Warn("[top-sql] mock agent server serve failed", zap.Error(err))
		}
	}()

	return agentServer, nil
}

func (svr *mockAgentServer) HangFromNow(duration time.Duration) {
	now := time.Now()
	svr.hang.beginTime.Store(now)
	svr.hang.endTime.Store(now.Add(duration))
}

// mayHang will check the hanging period, and ensure to sleep through it
func (svr *mockAgentServer) mayHang() {
	now := time.Now()
	beginTime := svr.hang.beginTime.Load().(time.Time)
	endTime := svr.hang.endTime.Load().(time.Time)
	if now.Before(endTime) && now.After(beginTime) {
		time.Sleep(endTime.Sub(now))
	}
}

func (svr *mockAgentServer) ReportTopSQLRecords(stream tipb.TopSQLAgent_ReportTopSQLRecordsServer) error {
	records := make([]*tipb.TopSQLRecord, 0, 10)
	for {
		svr.mayHang()
		req, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		records = append(records, req)
	}
	svr.Lock()
	svr.records = append(svr.records, records)
	svr.Unlock()
	return stream.SendAndClose(&tipb.EmptyResponse{})
}

func (svr *mockAgentServer) ReportSQLMeta(stream tipb.TopSQLAgent_ReportSQLMetaServer) error {
	for {
		svr.mayHang()
		req, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		svr.Lock()
		svr.sqlMetas[string(req.SqlDigest)] = *req
		svr.Unlock()
	}
	return stream.SendAndClose(&tipb.EmptyResponse{})
}

func (svr *mockAgentServer) ReportPlanMeta(stream tipb.TopSQLAgent_ReportPlanMetaServer) error {
	for {
		svr.mayHang()
		req, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		svr.Lock()
		svr.planMetas[string(req.PlanDigest)] = req.NormalizedPlan
		svr.Unlock()
	}
	return stream.SendAndClose(&tipb.EmptyResponse{})
}

func (svr *mockAgentServer) WaitCollectCnt(cnt int, timeout time.Duration) {
	start := time.Now()
	svr.Lock()
	old := len(svr.records)
	svr.Unlock()
	for {
		svr.Lock()
		if len(svr.records)-old >= cnt {
			svr.Unlock()
			return
		}
		svr.Unlock()
		if time.Since(start) > timeout {
			return
		}
		time.Sleep(time.Millisecond)
	}
}

func (svr *mockAgentServer) GetSQLMetaByDigestBlocking(digest []byte, timeout time.Duration) (meta tipb.SQLMeta, exist bool) {
	start := time.Now()
	for {
		svr.Lock()
		sqlMeta, exist := svr.sqlMetas[string(digest)]
		svr.Unlock()
		if exist || time.Since(start) > timeout {
			return sqlMeta, exist
		}
		time.Sleep(time.Millisecond)
	}
}

func (svr *mockAgentServer) GetPlanMetaByDigestBlocking(digest []byte, timeout time.Duration) (normalizedPlan string, exist bool) {
	start := time.Now()
	for {
		svr.Lock()
		normalizedPlan, exist = svr.planMetas[string(digest)]
		svr.Unlock()
		if exist || time.Since(start) > timeout {
			return normalizedPlan, exist
		}
		time.Sleep(time.Millisecond)
	}
}

func (svr *mockAgentServer) GetLatestRecords() []*tipb.TopSQLRecord {
	svr.Lock()
	records := svr.records
	svr.records = [][]*tipb.TopSQLRecord{}
	svr.Unlock()

	if len(records) == 0 {
		return nil
	}
	return records[len(records)-1]
}

func (svr *mockAgentServer) GetTotalSQLMetas() []tipb.SQLMeta {
	svr.Lock()
	defer svr.Unlock()
	metas := make([]tipb.SQLMeta, 0, len(svr.sqlMetas))
	for _, meta := range svr.sqlMetas {
		metas = append(metas, meta)
	}
	return metas
}

func (svr *mockAgentServer) Address() string {
	return svr.addr
}

func (svr *mockAgentServer) Stop() {
	if svr.grpcServer != nil {
		svr.grpcServer.Stop()
	}
}
