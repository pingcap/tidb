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

package ticlient

import (
	"fmt"
	"log"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/ticlient/oracle/oracles"
)

type testKvSuite struct {
	pd     *mockPDClient
	cache  *RegionCache
	store  *tikvStore
	client *stubClient
}

var _ = Suite(&testKvSuite{})

func (s *testKvSuite) SetUpTest(c *C) {
	s.pd = newMockPDClient()
	s.cache = NewRegionCache(s.pd)
	s.client = newStubClient(c)

	s.pd.setStore(1, "addr1")
	s.pd.setStore(2, "addr2")
	s.pd.setRegion(3, nil, nil, []uint64{1, 2})

	s.store = newMockStore(s.cache)
	s.store.clients["addr1"] = s.client
	s.store.clients["addr2"] = s.client
}

func (s *testKvSuite) beginTxn(c *C) *tikvTxn {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	return txn.(*tikvTxn)
}

func (s *testKvSuite) TestSimpleGetOk(c *C) {
	expectReq := &pb.Request{}
	expectResp := makeGetResp("value")
	s.client.push(expectReq, expectResp)
	region, err := s.store.getRegion(nil)
	c.Assert(err, IsNil)
	resp, err := s.store.SendKVReq(expectReq, region)
	c.Assert(expectResp, Equals, resp)
}

func (s *testKvSuite) TestGetOk(c *C) {
	txn := s.beginTxn(c)
	key, val := "key", "value"
	region, err := s.cache.GetRegion([]byte(key))
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)
	expectReq := makeGetReq(region, key, txn.StartTS())
	expectResp := makeGetResp(val)
	s.client.push(expectReq, expectResp)
	value, err := txn.Get([]byte(key))
	c.Assert(err, IsNil)
	c.Assert(value, BytesEquals, []byte(val))
}

// stubClient is used for simulating sending Request to TiKV and receiving Response.
type stubClient struct {
	// q is a message queue, it will be pushed by test cases as they expected,
	// and popped when calling SendKVReq/SendCopReq once.
	// It maybe pushed multiple <req, resp> before calling some function.
	// e.g.: Commit(), Get() but key is locked.
	q *queueMsg
	c *C
}

func newStubClient(c *C) *stubClient {
	return &stubClient{
		q: &queueMsg{
			data: make([]*msgPair, 0),
			idx:  0,
		},
		c: c,
	}
}

func (cli *stubClient) push(req *pb.Request, resp *pb.Response) {
	cli.q.Push(&msgPair{req: req, resp: resp})
}

func (cli *stubClient) SendKVReq(req *pb.Request) (*pb.Response, error) {
	msg, err := cli.q.Pop()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !cli.c.Check(msg.req, DeepEquals, req) {
		// HELP: cli.c.Assert can't fatal here.
		log.Fatalf("requests don't mismatch.\nexp[%s]\ngot[%s]", msg.req, req)
	}
	return msg.resp, nil
}

// TODO: implements this.
func (cli *stubClient) SendCopReq(req *coprocessor.Request) (*coprocessor.Response, error) {
	return nil, nil
}

// Close clear message queue.
func (cli *stubClient) Close() error {
	cli.q = nil
	return nil
}

type msgPair struct {
	req  *pb.Request
	resp *pb.Response
}

// queueMsg is a simple queue that contains <Request, Response> pair.
type queueMsg struct {
	data []*msgPair
	idx  int
}

func (q *queueMsg) Push(x *msgPair) {
	q.data = append(q.data, x)
}

func (q *queueMsg) Pop() (*msgPair, error) {
	if q.Empty() {
		return nil, errors.New("queue is empty")
	}
	ret := q.data[q.idx]
	q.idx++
	return ret, nil
}

func (q *queueMsg) Empty() bool {
	return q.idx >= len(q.data)
}

func newMockStore(cache *RegionCache) *tikvStore {
	return &tikvStore{
		uuid:        fmt.Sprintf("kv_%d", time.Now().Unix()),
		oracle:      oracles.NewLocalOracle(),
		clients:     make(map[string]Client),
		regionCache: cache,
	}
}

func makeGetReq(region *Region, key string, ver uint64) *pb.Request {
	return &pb.Request{
		Type: pb.MessageType_CmdGet.Enum(),
		CmdGetReq: &pb.CmdGetRequest{
			Key:     []byte(key),
			Version: proto.Uint64(ver),
		},
		Context: region.GetContext(),
	}
}

func makeGetResp(value string) *pb.Response {
	return &pb.Response{
		Type: pb.MessageType_CmdGet.Enum(),
		CmdGetResp: &pb.CmdGetResponse{
			Value: []byte(value),
		},
	}
}
