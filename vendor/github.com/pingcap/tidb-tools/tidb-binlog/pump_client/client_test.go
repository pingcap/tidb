// Copyright 2018 PingCAP, Inc.
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

package client

import (
	"fmt"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/tidb-binlog/node"
	pb "github.com/pingcap/tipb/go-binlog"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

type testCase struct {
	binlogs     []*pb.Binlog
	choosePumps []*PumpStatus
	setAvliable []bool
	setNodeID   []string
}

var _ = Suite(&testClientSuite{})

type testClientSuite struct{}

func (t *testClientSuite) TestPumpsClient(c *C) {
	algorithms := []string{Hash, Range}
	for _, algorithm := range algorithms {
		t.testPumpsClient(c, algorithm)
	}
}

func (*testClientSuite) testPumpsClient(c *C, algorithm string) {
	pumpInfos := &PumpInfos{
		Pumps:            make(map[string]*PumpStatus),
		AvaliablePumps:   make(map[string]*PumpStatus),
		UnAvaliablePumps: make(map[string]*PumpStatus),
	}

	pumpsClient := &PumpsClient{
		Pumps:              pumpInfos,
		Selector:           NewSelector(algorithm),
		RetryTime:          DefaultRetryTime,
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
		pumpsClient.Pumps.Pumps["pump1"], pumpsClient.Pumps.Pumps["pump1"], pumpsClient.Pumps.Pumps["pump1"]}

	for i, nodeID := range tCase.setNodeID {
		if nodeID != "" {
			pumpsClient.setPumpAvaliable(pumpsClient.Pumps.Pumps[nodeID], tCase.setAvliable[i])
		}
		pump := pumpsClient.Selector.Select(tCase.binlogs[i])
		c.Assert(pump, Equals, tCase.choosePumps[i])
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

		pump1 := pumpsClient.Selector.Select(prewriteBinlog)
		if j%2 == 0 {
			pump1 = pumpsClient.Selector.Next(prewriteBinlog, 0)
		}

		pumpsClient.setPumpAvaliable(pump1, false)
		pump2 := pumpsClient.Selector.Select(commitBinlog)
		c.Assert(pump2.IsAvaliable, Equals, false)
		// prewrite binlog and commit binlog with same start ts should choose same pump
		c.Assert(pump1.NodeID, Equals, pump2.NodeID)

		pumpsClient.setPumpAvaliable(pump1, true)
		c.Assert(pump2.IsAvaliable, Equals, true)
	}
}
