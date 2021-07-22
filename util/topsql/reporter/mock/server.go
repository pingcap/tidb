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
	records    [][]*tipb.CPUTimeRecord
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

func (svr *mockAgentServer) ReportCPUTimeRecords(stream tipb.TopSQLAgent_ReportCPUTimeRecordsServer) error {
	records := make([]*tipb.CPUTimeRecord, 0, 10)
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

func (svr *mockAgentServer) GetLatestRecords() []*tipb.CPUTimeRecord {
	svr.Lock()
	records := svr.records
	svr.records = [][]*tipb.CPUTimeRecord{}
	svr.Unlock()

	if len(records) == 0 {
		return nil
	}
	return records[len(records)-1]
}

func (svr *mockAgentServer) Address() string {
	return svr.addr
}

func (svr *mockAgentServer) Stop() {
	if svr.grpcServer != nil {
		svr.grpcServer.Stop()
	}
}
