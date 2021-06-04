package mock

import (
	"fmt"
	"io"
	"net"
	"time"

	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type mockAgentServer struct {
	addr       string
	grpcServer *grpc.Server
	sqlMetas   map[string]string
	planMetas  map[string]string
	records    []*tipb.CPUTimeRecord
}

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
		sqlMetas:   make(map[string]string, 5000),
		planMetas:  make(map[string]string, 5000),
	}
	tipb.RegisterTopSQLAgentServer(server, agentServer)

	go func() {
		err := server.Serve(lis)
		if err != nil {
			logutil.BgLogger().Warn("[top-sql] mock agent server serve failed", zap.Error(err))
		}
	}()

	return agentServer, nil
}

func (svr *mockAgentServer) ReportCPUTimeRecords(stream tipb.TopSQLAgent_ReportCPUTimeRecordsServer) error {
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

func (svr *mockAgentServer) ReportSQLMeta(stream tipb.TopSQLAgent_ReportSQLMetaServer) error {
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

func (svr *mockAgentServer) ReportPlanMeta(stream tipb.TopSQLAgent_ReportPlanMetaServer) error {
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

func (svr *mockAgentServer) WaitServerCollect(recordCount int, timeout time.Duration) {
	start := time.Now()
	for {
		if len(svr.records) >= recordCount {
			return
		}
		if time.Since(start) > timeout {
			return
		}
		time.Sleep(time.Millisecond)
	}
}

func (svr *mockAgentServer) GetSQLMetas() map[string]string {
	m := svr.sqlMetas
	svr.sqlMetas = make(map[string]string)
	return m
}

func (svr *mockAgentServer) GetPlanMetas() map[string]string {
	m := svr.planMetas
	svr.planMetas = make(map[string]string)
	return m
}

func (svr *mockAgentServer) GetRecords() []*tipb.CPUTimeRecord {
	records := svr.records
	svr.records = []*tipb.CPUTimeRecord{}
	return records
}

func (svr *mockAgentServer) Address() string {
	return svr.addr
}

func (svr *mockAgentServer) Stop() {
	if svr.grpcServer != nil {
		svr.grpcServer.Stop()
	}
}
