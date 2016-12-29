// Copyright 2016 PingCAP, Inc.
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

package binloginfo_test

import (
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-binlog"
	goctx "golang.org/x/net/context"
	"google.golang.org/grpc"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

type mockBinlogPump struct {
	mu struct {
		sync.Mutex
		payloads [][]byte
	}
}

func (p *mockBinlogPump) WriteBinlog(ctx goctx.Context, req *binlog.WriteBinlogReq) (*binlog.WriteBinlogResp, error) {
	p.mu.Lock()
	p.mu.payloads = append(p.mu.payloads, req.Payload)
	p.mu.Unlock()
	return &binlog.WriteBinlogResp{}, nil
}

// PullBinlogs implements PumpServer interface.
func (p *mockBinlogPump) PullBinlogs(ctx goctx.Context, req *binlog.PullBinlogReq) (*binlog.PullBinlogResp, error) {
	return &binlog.PullBinlogResp{}, nil
}

var _ = Suite(&testBinlogSuite{})

type testBinlogSuite struct {
	store    kv.Storage
	unixFile string
	serv     *grpc.Server
	pump     *mockBinlogPump
	tk       *testkit.TestKit
	ddl      ddl.DDL
}

func (s *testBinlogSuite) SetUpSuite(c *C) {
	logLevel := os.Getenv("log_level")
	log.SetLevelByString(logLevel)
	store, err := tikv.NewMockTikvStore()
	c.Assert(err, IsNil)
	s.store = store
	tidb.SetSchemaLease(0)
	s.unixFile = "/tmp/mock-binlog-pump"
	os.Remove(s.unixFile)
	l, err := net.Listen("unix", s.unixFile)
	c.Assert(err, IsNil)
	s.serv = grpc.NewServer()
	s.pump = new(mockBinlogPump)
	binlog.RegisterPumpServer(s.serv, s.pump)
	go s.serv.Serve(l)
	opt := grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
		return net.DialTimeout("unix", addr, timeout)
	})
	clientCon, err := grpc.Dial(s.unixFile, opt, grpc.WithInsecure())
	c.Assert(err, IsNil)
	c.Assert(clientCon, NotNil)
	binloginfo.PumpClient = binlog.NewPumpClient(clientCon)
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test")
	domain := sessionctx.GetDomain(s.tk.Se.(context.Context))
	s.ddl = domain.DDL()
}

func (s *testBinlogSuite) TearDownSuite(c *C) {
	s.ddl.Stop()
	binloginfo.PumpClient = nil
	s.serv.Stop()
	os.Remove(s.unixFile)
	s.store.Close()
}

func (s *testBinlogSuite) TestBinlog(c *C) {
	tk := s.tk
	pump := s.pump
	tk.MustExec("drop table if exists local_binlog")
	ddlQuery := "create table local_binlog (id int primary key, name varchar(10))"
	tk.MustExec(ddlQuery)
	var matched bool // got matched pre DDL and commit DDL
	for i := 0; i < 10; i++ {
		preDDL, commitDDL := getLatestDDLBinlog(c, pump, ddlQuery)
		if preDDL.DdlJobId == commitDDL.DdlJobId {
			c.Assert(commitDDL.StartTs, Equals, preDDL.StartTs)
			c.Assert(commitDDL.CommitTs, Greater, commitDDL.StartTs)
			matched = true
			break
		}
		time.Sleep(time.Millisecond * 10)
	}
	c.Assert(matched, IsTrue)

	tk.MustExec("insert local_binlog values (1, 'abc'), (2, 'cde')")
	prewriteVal := getLatestBinlogPrewriteValue(c, pump)
	c.Assert(prewriteVal.SchemaVersion, Greater, int64(0))
	c.Assert(prewriteVal.Mutations[0].TableId, Greater, int64(0))
	expected := [][]types.Datum{
		{types.NewIntDatum(1), types.NewStringDatum("abc")},
		{types.NewIntDatum(2), types.NewStringDatum("cde")},
	}
	gotRows := mutationRowsToRows(c, prewriteVal.Mutations[0].InsertedRows, 0, 2)
	c.Assert(gotRows, DeepEquals, expected)

	tk.MustExec("update local_binlog set name = 'xyz' where id = 2")
	prewriteVal = getLatestBinlogPrewriteValue(c, pump)
	expected = [][]types.Datum{
		{types.NewIntDatum(2), types.NewStringDatum("xyz")},
	}
	gotRows = mutationRowsToRows(c, prewriteVal.Mutations[0].UpdatedRows, 2, 4)
	c.Assert(gotRows, DeepEquals, expected)

	tk.MustExec("delete from local_binlog where id = 1")
	prewriteVal = getLatestBinlogPrewriteValue(c, pump)
	c.Assert(prewriteVal.Mutations[0].DeletedIds, DeepEquals, []int64{1})

	// Test table primary key is not integer.
	tk.MustExec("create table local_binlog2 (name varchar(64) primary key, age int)")
	tk.MustExec("insert local_binlog2 values ('abc', 16), ('def', 18)")
	tk.MustExec("delete from local_binlog2 where name = 'def'")
	prewriteVal = getLatestBinlogPrewriteValue(c, pump)
	c.Assert(prewriteVal.Mutations[0].Sequence[0], Equals, binlog.MutationType_DeletePK)
	_, deletedPK, _ := codec.DecodeOne(prewriteVal.Mutations[0].DeletedPks[0])
	c.Assert(deletedPK.GetString(), Equals, "def")

	// Test Table don't have primary key.
	tk.MustExec("create table local_binlog3 (c1 int, c2 int)")
	tk.MustExec("insert local_binlog3 values (1, 2), (1, 3), (2, 3)")
	tk.MustExec("update local_binlog3 set c1 = 3 where c1 = 2")
	prewriteVal = getLatestBinlogPrewriteValue(c, pump)
	gotRows = mutationRowsToRows(c, prewriteVal.Mutations[0].UpdatedRows, 5, 7)
	expected = [][]types.Datum{
		{types.NewIntDatum(3), types.NewIntDatum(3)},
	}
	c.Assert(gotRows, DeepEquals, expected)

	tk.MustExec("delete from local_binlog3 where c1 = 3 and c2 = 3")
	prewriteVal = getLatestBinlogPrewriteValue(c, pump)
	c.Assert(prewriteVal.Mutations[0].Sequence[0], Equals, binlog.MutationType_DeleteRow)
	gotRows = mutationRowsToRows(c, prewriteVal.Mutations[0].DeletedRows, 1, 3)
	expected = [][]types.Datum{
		{types.NewIntDatum(3), types.NewIntDatum(3)},
	}
	c.Assert(gotRows, DeepEquals, expected)

	// Test Mutation Sequence.
	tk.MustExec("create table local_binlog4 (c1 int primary key, c2 int)")
	tk.MustExec("insert local_binlog4 values (1, 1), (2, 2), (3, 2)")
	tk.MustExec("begin")
	tk.MustExec("delete from local_binlog4 where c1 = 1")
	tk.MustExec("insert local_binlog4 values (1, 1)")
	tk.MustExec("update local_binlog4 set c2 = 3 where c1 = 3")
	tk.MustExec("commit")
	prewriteVal = getLatestBinlogPrewriteValue(c, pump)
	c.Assert(prewriteVal.Mutations[0].Sequence, DeepEquals, []binlog.MutationType{
		binlog.MutationType_DeleteID,
		binlog.MutationType_Insert,
		binlog.MutationType_Update,
	})

	checkBinlogCount(c, pump)

	pump.mu.Lock()
	originBinlogLen := len(pump.mu.payloads)
	pump.mu.Unlock()
	tk.MustExec("set @@global.autocommit = 0")
	tk.MustExec("set @@global.autocommit = 1")
	pump.mu.Lock()
	newBinlogLen := len(pump.mu.payloads)
	pump.mu.Unlock()
	c.Assert(newBinlogLen, Equals, originBinlogLen)
}

func getLatestBinlogPrewriteValue(c *C, pump *mockBinlogPump) *binlog.PrewriteValue {
	var bin *binlog.Binlog
	pump.mu.Lock()
	for i := len(pump.mu.payloads) - 1; i >= 0; i-- {
		payload := pump.mu.payloads[i]
		bin = new(binlog.Binlog)
		bin.Unmarshal(payload)
		if bin.Tp == binlog.BinlogType_Prewrite {
			break
		}
	}
	pump.mu.Unlock()
	c.Assert(bin, NotNil)
	preVal := new(binlog.PrewriteValue)
	preVal.Unmarshal(bin.PrewriteValue)
	return preVal
}

func getLatestDDLBinlog(c *C, pump *mockBinlogPump, ddlQuery string) (preDDL, commitDDL *binlog.Binlog) {
	pump.mu.Lock()
	for i := len(pump.mu.payloads) - 1; i >= 0; i-- {
		payload := pump.mu.payloads[i]
		bin := new(binlog.Binlog)
		bin.Unmarshal(payload)
		if bin.Tp == binlog.BinlogType_Commit && bin.DdlJobId > 0 {
			commitDDL = bin
		}
		if bin.Tp == binlog.BinlogType_Prewrite && bin.DdlJobId != 0 {
			preDDL = bin
		}
		if preDDL != nil && commitDDL != nil {
			break
		}
	}
	pump.mu.Unlock()
	c.Assert(preDDL.DdlJobId, Greater, int64(0))
	c.Assert(preDDL.StartTs, Greater, int64(0))
	c.Assert(preDDL.CommitTs, Equals, int64(0))
	c.Assert(string(preDDL.DdlQuery), Equals, ddlQuery)
	return
}

func checkBinlogCount(c *C, pump *mockBinlogPump) {
	var bin *binlog.Binlog
	prewriteCount := 0
	ddlCount := 0
	pump.mu.Lock()
	length := len(pump.mu.payloads)
	for i := length - 1; i >= 0; i-- {
		payload := pump.mu.payloads[i]
		bin = new(binlog.Binlog)
		bin.Unmarshal(payload)
		if bin.Tp == binlog.BinlogType_Prewrite {
			if bin.DdlJobId != 0 {
				ddlCount++
			} else {
				prewriteCount++
			}
		}
	}
	pump.mu.Unlock()
	c.Assert(ddlCount, Greater, 0)
	match := false
	for i := 0; i < 10; i++ {
		pump.mu.Lock()
		length = len(pump.mu.payloads)
		pump.mu.Unlock()
		if (prewriteCount+ddlCount)*2 == length {
			match = true
			break
		}
		time.Sleep(time.Millisecond * 10)
	}
	c.Assert(match, IsTrue)
}

func mutationRowsToRows(c *C, mutationRows [][]byte, firstColumn, secondColumn int) [][]types.Datum {
	var rows [][]types.Datum
	for _, mutationRow := range mutationRows {
		datums, err := codec.Decode(mutationRow, 5)
		c.Assert(err, IsNil)
		for i := range datums {
			if i != firstColumn && i != secondColumn {
				// Column ID or handle
				c.Assert(datums[i].GetInt64(), Greater, int64(0))
			}
			if datums[i].Kind() == types.KindBytes {
				datums[i].SetBytesAsString(datums[i].GetBytes())
			}
		}
		row := []types.Datum{datums[firstColumn], datums[secondColumn]}
		rows = append(rows, row)
	}
	return rows
}
