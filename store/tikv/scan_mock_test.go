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

package tikv

import (
	"github.com/gogo/protobuf/proto"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/errorpb"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
)

type testScanMockSuite struct {
	pd     *mockPDClient
	cache  *RegionCache
	store  *tikvStore
	client *stubClient
}

var _ = Suite(&testScanMockSuite{})

func (s *testScanMockSuite) SetUpTest(c *C) {
	s.pd = newMockPDClient()
	s.cache = NewRegionCache(s.pd)
	s.client = newStubClient(c)

	s.pd.setStore(1, "addr1")
	s.pd.setStore(2, "addr2")
	// ["", "a"), ["a", "m"), ["m", nil)
	s.pd.setRegion(3, nil, []byte("a"), []uint64{1, 2})
	s.pd.setRegion(4, []byte("a"), []byte("m"), []uint64{1, 2})
	s.pd.setRegion(5, []byte("m"), nil, []uint64{1, 2})

	s.store = newMockStore(s.cache)
	s.store.clients["addr1"] = s.client
	s.store.clients["addr2"] = s.client
}

func (s *testScanMockSuite) beginTxn(c *C) (*tikvTxn, *tikvSnapshot) {
	transaction, err := s.store.Begin()
	c.Assert(err, IsNil)
	txn := transaction.(*tikvTxn)
	kvSnapshot, err := txn.store.GetSnapshot(kv.Version{Ver: txn.StartTS()})
	c.Assert(err, IsNil)
	c.Assert(kvSnapshot, NotNil)
	snapshot := kvSnapshot.(*tikvSnapshot)
	return txn, snapshot
}

func (s *testScanMockSuite) getRegion(c *C, startKey []byte) *requestRegion {
	region, err := s.store.getRegion(startKey)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)
	return region
}

func (s *testScanMockSuite) TestStaleRegionEpoch(c *C) {
	const batchSize = 10
	txn, snapshot := s.beginTxn(c)
	region := s.getRegion(c, []byte("a"))
	expectReq := makeScanReq(region, "a", batchSize, txn.StartTS())
	expectResp := makeScanStaleEpochResp()
	s.client.push(expectReq, expectResp)
	_, err := newScanner(region, []byte("a"), txn.StartTS(), *snapshot, batchSize)
	c.Assert(err, NotNil)
}

func (s *testScanMockSuite) TestScanMultiRegions(c *C) {
	const batchSize = 10
	startKey := []byte("a")
	txn, snapshot := s.beginTxn(c)
	region4 := s.getRegion(c, startKey)
	expectReq := makeScanReq(region4, "a", batchSize, txn.StartTS())
	// Assume kv has key r4["a","l"] + r5["m","q"].
	// Making first request & response, response returns keys whose length is batchSize.
	// That is "a"-"j".
	res := makeRangeKvs('a', 'a'+batchSize-1)
	expectResp := makeScanOkResp(res)
	s.client.push(expectReq, expectResp)
	// Making second request & response, response returns the rest keys of region4.
	// That is "j"-"l" (total 3, "j" will be skipped).
	res2 := makeRangeKvs('a'+batchSize-1, 'l')
	expectReq2 := makeScanReq(region4, res2[0].key, batchSize, txn.StartTS())
	expectResp2 := makeScanOkResp(res2)
	s.client.push(expectReq2, expectResp2)
	// Making third request & response, response returns all keys of region5.
	// That is "m"-"q" (total 5).
	region5 := s.getRegion(c, []byte("m"))
	res3 := makeRangeKvs('m', 'q')
	expectReq3 := makeScanReq(region5, res3[0].key, batchSize, txn.StartTS())
	expectResp3 := makeScanOkResp(res3)
	s.client.push(expectReq3, expectResp3)
	// Making last request & response, response return empty and scanner will Close().
	expectReq4 := makeScanReq(region5, res3[len(res3)-1].key, batchSize, txn.StartTS())
	expectResp4 := makeScanOkResp([]*KvPair{})
	s.client.push(expectReq4, expectResp4)

	scanner, err := newScanner(region4, startKey, txn.StartTS(), *snapshot, batchSize)
	c.Assert(err, IsNil)
	for k := 'a'; k <= 'q'; k++ {
		c.Assert([]byte(string(k)), BytesEquals, []byte(scanner.Key()))
		if k < 'q' {
			c.Assert(scanner.Next(), IsNil)
		}
	}
	c.Assert(scanner.Next(), NotNil)
	c.Assert(scanner.Valid(), IsFalse)
}

func makeScanReq(region *requestRegion, key string, limit uint32, ver uint64) *pb.Request {
	return &pb.Request{
		Type: pb.MessageType_CmdScan.Enum(),
		CmdScanReq: &pb.CmdScanRequest{
			StartKey: []byte(key),
			Limit:    proto.Uint32(limit),
			Version:  proto.Uint64(ver),
		},
		Context: region.GetContext(),
	}
}

func makeScanOkResp(res []*KvPair) *pb.Response {
	var pairs []*pb.KvPair
	for _, kv := range res {
		pairs = append(pairs, &pb.KvPair{
			Key:   []byte(kv.key),
			Value: []byte(kv.value),
		})
	}
	return &pb.Response{
		Type: pb.MessageType_CmdScan.Enum(),
		CmdScanResp: &pb.CmdScanResponse{
			Pairs: pairs,
		},
	}
}

func makeScanStaleEpochResp() *pb.Response {
	return &pb.Response{
		Type: pb.MessageType_CmdScan.Enum(),
		RegionError: &errorpb.Error{
			Message:    proto.String("stale epoch"),
			StaleEpoch: &errorpb.StaleEpoch{},
		},
	}
}

func makeRangeKvs(startKey, endKey rune) []*KvPair {
	var res []*KvPair
	for key := startKey; key <= endKey; key++ {
		// Just set value = key for easy testing.
		res = append(res, &KvPair{key: string(key), value: string(key)})
	}
	return res

}

type KvPair struct {
	key   string
	value string
}
