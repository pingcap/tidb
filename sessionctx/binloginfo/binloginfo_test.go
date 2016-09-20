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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-binlog"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func TestT(t *testing.T) {
	TestingT(t)
}

type mockBinlogPump struct {
	mu struct {
		sync.Mutex
		payloads [][]byte
	}
}

func (p *mockBinlogPump) WriteBinlog(ctx context.Context, req *binlog.WriteBinlogReq) (*binlog.WriteBinlogResp, error) {
	p.mu.Lock()
	p.mu.payloads = append(p.mu.payloads, req.Payload)
	p.mu.Unlock()
	return &binlog.WriteBinlogResp{}, nil
}

var _ = Suite(&testBinlogSuite{})

type testBinlogSuite struct {
	store    kv.Storage
	unixFile string
	serv     *grpc.Server
	pump     *mockBinlogPump
}

func (s *testBinlogSuite) SetUpSuite(c *C) {
	log.SetLevelByString("warn")
	store, err := tikv.NewMockTikvStore()
	c.Assert(err, IsNil)
	s.store = store
	tidb.SetSchemaLease(0)
	s.unixFile = "/tmp/mock-binlog-pump"
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
}

func (s *testBinlogSuite) TearDownSuite(c *C) {
	binloginfo.PumpClient = nil
	s.serv.Stop()
	os.Remove(s.unixFile)
	s.store.Close()
}

func (s *testBinlogSuite) TestBinlog(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists local_binlog")
	tk.MustExec("create table local_binlog (id int primary key, name varchar(10))")
	tk.MustExec("insert local_binlog values (1, 'abc'), (2, 'cde')")
	pump := s.pump
	prewriteVal := getLatestBinlogPrewriteValue(c, pump)
	c.Assert(prewriteVal.SchemaVersion, Greater, int64(0))
	c.Assert(prewriteVal.Mutations[0].TableId, Greater, int64(0))
	insertedRow1, _ := codec.EncodeValue(nil, types.NewIntDatum(1), types.NewStringDatum("abc"))
	insertedRow2, _ := codec.EncodeValue(nil, types.NewIntDatum(2), types.NewStringDatum("cde"))
	c.Assert(prewriteVal.Mutations[0].InsertedRows, DeepEquals, [][]byte{insertedRow1, insertedRow2})

	tk.MustExec("update local_binlog set name = 'xyz' where id = 2")
	prewriteVal = getLatestBinlogPrewriteValue(c, pump)
	updatedRow, _ := codec.EncodeValue(nil, types.NewIntDatum(2), types.NewStringDatum("xyz"))
	c.Assert(prewriteVal.Mutations[0].UpdatedRows, DeepEquals, [][]byte{updatedRow})

	tk.MustExec("delete from local_binlog where id = 1")
	prewriteVal = getLatestBinlogPrewriteValue(c, pump)
	c.Assert(prewriteVal.Mutations[0].DeletedIds, DeepEquals, []int64{1})

	// Test table primary key is not integer.
	tk.MustExec("create table local_binlog2 (name varchar(64) primary key)")
	tk.MustExec("insert local_binlog2 values ('abc'), ('def')")
	tk.MustExec("delete from local_binlog2 where name = 'def'")
	prewriteVal = getLatestBinlogPrewriteValue(c, pump)
	_, deletedPK, _ := codec.DecodeOne(prewriteVal.Mutations[0].DeletedPks[0])
	c.Assert(deletedPK.GetString(), Equals, "def")

	// Test Table don't have primary key.
	tk.MustExec("create table local_binlog3 (c1 int, c2 int)")
	tk.MustExec("insert local_binlog3 values (1, 2), (1, 3), (2, 3)")
	tk.MustExec("delete from local_binlog3 where c2 = 3")
	prewriteVal = getLatestBinlogPrewriteValue(c, pump)

	deletedRow1, _ := codec.Decode(prewriteVal.Mutations[0].DeletedRows[0], 2)
	c.Assert(deletedRow1[1], DeepEquals, types.NewIntDatum(3))
	deletedRow2, _ := codec.Decode(prewriteVal.Mutations[0].DeletedRows[1], 2)
	c.Assert(deletedRow2[1], DeepEquals, types.NewIntDatum(3))

	checkPrewriteMatchHalfBinlogCount(c, pump)

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

func checkPrewriteMatchHalfBinlogCount(c *C, pump *mockBinlogPump) {
	var bin *binlog.Binlog
	prewriteCount := 0
	pump.mu.Lock()
	length := len(pump.mu.payloads)
	for i := length - 1; i >= 0; i-- {
		payload := pump.mu.payloads[i]
		bin = new(binlog.Binlog)
		bin.Unmarshal(payload)
		if bin.Tp == binlog.BinlogType_Prewrite {
			prewriteCount++
		}
	}
	pump.mu.Unlock()
	match := false
	for i := 0; i < 10; i++ {
		pump.mu.Lock()
		length = len(pump.mu.payloads)
		pump.mu.Unlock()
		if prewriteCount*2 == length {
			match = true
			break
		}
		time.Sleep(time.Millisecond * 10)
	}
	c.Assert(match, IsTrue)
}
