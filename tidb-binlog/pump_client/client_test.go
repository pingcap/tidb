// Copyright 2022 PingCAP, Inc.
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

package client

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/tidb-binlog/node"
	binlog "github.com/pingcap/tipb/go-binlog"
	pb "github.com/pingcap/tipb/go-binlog"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

var (
	testMaxRecvMsgSize = 1024
	testRetryTime      = 5
)

type testCase struct {
	binlogs     []*pb.Binlog
	choosePumps []*PumpStatus
	setAvliable []bool
	setNodeID   []string
}

func TestSelector(t *testing.T) {
	strategys := []string{Hash, Range}
	for _, strategy := range strategys {
		testSelector(t, strategy)
	}
}

func testSelector(t *testing.T, strategy string) {
	pumpsClient := &PumpsClient{
		Pumps:              NewPumpInfos(),
		Selector:           NewSelector(strategy),
		BinlogWriteTimeout: DefaultBinlogWriteTimeout,
	}

	pumps := []*PumpStatus{{}, {}, {}}
	for i, pump := range pumps {
		pump.NodeID = fmt.Sprintf("pump%d", i)
		pump.State = node.Offline
		// set pump client to avoid create grpc client.
		pump.Client = pb.NewPumpClient(nil)
	}

	for _, pump := range pumps {
		pumpsClient.addPump(pump, false)
	}
	pumpsClient.Selector.SetPumps(copyPumps(pumpsClient.Pumps.AvaliablePumps))

	tCase := &testCase{}

	tCase.binlogs = []*pb.Binlog{
		{
			Tp:      pb.BinlogType_Prewrite,
			StartTs: 1,
		}, {
			Tp:       pb.BinlogType_Commit,
			StartTs:  1,
			CommitTs: 2,
		}, {
			Tp:      pb.BinlogType_Prewrite,
			StartTs: 3,
		}, {
			Tp:       pb.BinlogType_Commit,
			StartTs:  3,
			CommitTs: 4,
		}, {
			Tp:      pb.BinlogType_Prewrite,
			StartTs: 5,
		}, {
			Tp:       pb.BinlogType_Commit,
			StartTs:  5,
			CommitTs: 6,
		},
	}

	tCase.setNodeID = []string{"pump0", "", "pump0", "pump1", "", "pump2"}
	tCase.setAvliable = []bool{true, false, false, true, false, true}
	tCase.choosePumps = []*PumpStatus{pumpsClient.Pumps.Pumps["pump0"], pumpsClient.Pumps.Pumps["pump0"], nil,
		nil, pumpsClient.Pumps.Pumps["pump1"], pumpsClient.Pumps.Pumps["pump1"]}

	for i, nodeID := range tCase.setNodeID {
		if nodeID != "" {
			pumpsClient.setPumpAvailable(pumpsClient.Pumps.Pumps[nodeID], tCase.setAvliable[i])
		}
		pump := pumpsClient.Selector.Select(tCase.binlogs[i], 0)
		pumpsClient.Selector.Feedback(tCase.binlogs[i].StartTs, tCase.binlogs[i].Tp, pump)
		require.Equal(t, pump, tCase.choosePumps[i])
	}

	for j := 0; j < 10; j++ {
		prewriteBinlog := &pb.Binlog{
			Tp:      pb.BinlogType_Prewrite,
			StartTs: int64(j),
		}
		commitBinlog := &pb.Binlog{
			Tp:      pb.BinlogType_Commit,
			StartTs: int64(j),
		}

		pump1 := pumpsClient.Selector.Select(prewriteBinlog, 0)
		if j%2 == 0 {
			pump1 = pumpsClient.Selector.Select(prewriteBinlog, 1)
		}
		pumpsClient.Selector.Feedback(prewriteBinlog.StartTs, prewriteBinlog.Tp, pump1)

		pumpsClient.setPumpAvailable(pump1, false)
		pump2 := pumpsClient.Selector.Select(commitBinlog, 0)
		pumpsClient.Selector.Feedback(commitBinlog.StartTs, commitBinlog.Tp, pump2)
		// prewrite binlog and commit binlog with same start ts should choose same pump
		require.Equal(t, pump1.NodeID, pump2.NodeID)
		pumpsClient.setPumpAvailable(pump1, true)

		// after change strategy, prewrite binlog and commit binlog will choose same pump
		pump1 = pumpsClient.Selector.Select(prewriteBinlog, 0)
		pumpsClient.Selector.Feedback(prewriteBinlog.StartTs, prewriteBinlog.Tp, pump1)
		if strategy == Range {
			err := pumpsClient.SetSelectStrategy(Hash)
			require.NoError(t, err)
		} else {
			err := pumpsClient.SetSelectStrategy(Range)
			require.NoError(t, err)
		}
		pump2 = pumpsClient.Selector.Select(commitBinlog, 0)
		require.Equal(t, pump1.NodeID, pump2.NodeID)

		// set back
		err := pumpsClient.SetSelectStrategy(strategy)
		require.NoError(t, err)
	}
}

func TestWriteBinlog(t *testing.T) {
	pumpServerConfig := []struct {
		addr       string
		serverMode string
	}{
		{
			"/tmp/mock-pump.sock",
			"unix",
		}, {
			"127.0.0.1:15049",
			"tcp",
		},
	}

	// make test faster
	RetryInterval = 100 * time.Millisecond
	CommitBinlogTimeout = time.Second

	for _, cfg := range pumpServerConfig {
		pumpServer, err := createMockPumpServer(cfg.addr, cfg.serverMode, true)
		require.NoError(t, err)

		opt := grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
			return net.DialTimeout(cfg.serverMode, addr, timeout)
		})
		clientCon, err := grpc.Dial(cfg.addr, opt, grpc.WithInsecure())
		require.NoError(t, err)
		require.NotNil(t, clientCon)
		pumpClient := mockPumpsClient(pb.NewPumpClient(clientCon), true)

		// test binlog size bigger than grpc's MaxRecvMsgSize
		blog := &pb.Binlog{
			Tp:            pb.BinlogType_Prewrite,
			PrewriteValue: make([]byte, testMaxRecvMsgSize+1),
		}
		err = pumpClient.WriteBinlog(blog)
		require.Error(t, err)

		for i := 0; i < 10; i++ {
			// test binlog size small than grpc's MaxRecvMsgSize
			blog = &pb.Binlog{
				Tp:            pb.BinlogType_Prewrite,
				PrewriteValue: make([]byte, 1),
			}
			err = pumpClient.WriteBinlog(blog)
			require.NoError(t, err)
		}

		// after write some binlog, the pump without grpc client will move to unavailable list in pump client.
		require.Len(t, pumpClient.Pumps.UnAvaliablePumps, 1)

		// test write commit binlog, will not return error although write binlog failed.
		preWriteBinlog := &pb.Binlog{
			Tp:            pb.BinlogType_Prewrite,
			StartTs:       123,
			PrewriteValue: make([]byte, 1),
		}
		commitBinlog := &pb.Binlog{
			Tp:            pb.BinlogType_Commit,
			StartTs:       123,
			CommitTs:      123,
			PrewriteValue: make([]byte, 1),
		}

		err = pumpClient.WriteBinlog(preWriteBinlog)
		require.NoError(t, err)

		// test when pump is down
		pumpServer.Close()

		// write commit binlog failed will not return error
		err = pumpClient.WriteBinlog(commitBinlog)
		require.NoError(t, err)

		err = pumpClient.WriteBinlog(blog)
		require.Error(t, err)
	}
}

type mockPumpServer struct {
	mode   string
	addr   string
	server *grpc.Server

	withError bool
	retryTime int
}

// WriteBinlog implements PumpServer interface.
func (p *mockPumpServer) WriteBinlog(ctx context.Context, req *binlog.WriteBinlogReq) (*binlog.WriteBinlogResp, error) {
	if !p.withError {
		return &binlog.WriteBinlogResp{}, nil
	}

	p.retryTime++
	if p.retryTime < testRetryTime {
		return &binlog.WriteBinlogResp{}, errors.New("fake error")
	}

	// only the last retry will return succuess
	p.retryTime = 0
	return &binlog.WriteBinlogResp{}, nil
}

// PullBinlogs implements PumpServer interface.
func (p *mockPumpServer) PullBinlogs(req *binlog.PullBinlogReq, srv binlog.Pump_PullBinlogsServer) error {
	return nil
}

func (p *mockPumpServer) Close() {
	p.server.Stop()
	if p.mode == "unix" {
		os.Remove(p.addr)
	}
}

func createMockPumpServer(addr string, mode string, withError bool) (*mockPumpServer, error) {
	if mode == "unix" {
		os.Remove(addr)
	}

	l, err := net.Listen(mode, addr)
	if err != nil {
		return nil, err
	}
	serv := grpc.NewServer(grpc.MaxRecvMsgSize(testMaxRecvMsgSize))
	pump := &mockPumpServer{
		mode:      mode,
		addr:      addr,
		server:    serv,
		withError: withError,
	}
	pb.RegisterPumpServer(serv, pump)
	go serv.Serve(l)

	return pump, nil
}

// mockPumpsClient creates a PumpsClient, used for test.
func mockPumpsClient(client pb.PumpClient, withBadPump bool) *PumpsClient {
	// add a available pump
	nodeID1 := "pump-1"
	pump1 := &PumpStatus{
		Status: node.Status{
			NodeID: nodeID1,
			State:  node.Online,
		},
		Client: client,
	}

	pumps := []*PumpStatus{pump1}

	// add a pump without grpc client
	nodeID2 := "pump-2"
	pump2 := &PumpStatus{
		Status: node.Status{
			NodeID: nodeID2,
			State:  node.Online,
		},
	}

	pumpInfos := NewPumpInfos()
	pumpInfos.Pumps[nodeID1] = pump1
	pumpInfos.AvaliablePumps[nodeID1] = pump1

	if withBadPump {
		pumpInfos.Pumps[nodeID2] = pump2
		pumpInfos.AvaliablePumps[nodeID2] = pump2
		pumps = append(pumps, pump2)
	}

	pCli := &PumpsClient{
		ClusterID:          1,
		Pumps:              pumpInfos,
		Selector:           NewSelector(Range),
		BinlogWriteTimeout: time.Second,
	}
	pCli.Selector.SetPumps(pumps)

	return pCli
}
