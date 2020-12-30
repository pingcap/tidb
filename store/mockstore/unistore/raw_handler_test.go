// Copyright 2020 PingCAP, Inc.
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

package unistore

import (
	"fmt"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
)

func TestT(t *testing.T) {
	TestingT(t)
}

type testSuite struct{}

func (ts testSuite) SetUpSuite(c *C) {}

func (ts testSuite) TearDownSuite(c *C) {}

var _ = Suite(testSuite{})

func (ts testSuite) TestRawHandler(c *C) {
	h := newRawHandler()
	keys := make([][]byte, 10)
	vals := make([][]byte, 10)
	for i := 0; i < 10; i++ {
		keys[i] = []byte(fmt.Sprintf("key%d", i))
		vals[i] = []byte(fmt.Sprintf("val%d", i))
	}
	putResp, _ := h.RawPut(nil, &kvrpcpb.RawPutRequest{Key: keys[0], Value: vals[0]})
	c.Assert(putResp, NotNil)
	getResp, _ := h.RawGet(nil, &kvrpcpb.RawGetRequest{Key: keys[0]})
	c.Assert(getResp, NotNil)
	c.Assert(getResp.Value, BytesEquals, vals[0])
	delResp, _ := h.RawDelete(nil, &kvrpcpb.RawDeleteRequest{Key: keys[0]})
	c.Assert(delResp, NotNil)

	batchPutReq := &kvrpcpb.RawBatchPutRequest{Pairs: []*kvrpcpb.KvPair{
		{Key: keys[1], Value: vals[1]},
		{Key: keys[3], Value: vals[3]},
		{Key: keys[5], Value: vals[5]},
	}}
	batchPutResp, _ := h.RawBatchPut(nil, batchPutReq)
	c.Assert(batchPutResp, NotNil)
	batchGetResp, _ := h.RawBatchGet(nil, &kvrpcpb.RawBatchGetRequest{Keys: [][]byte{keys[1], keys[3], keys[5]}})
	c.Assert(batchGetResp, NotNil)
	c.Assert(batchGetResp.Pairs, DeepEquals, batchPutReq.Pairs)
	batchDelResp, _ := h.RawBatchDelete(nil, &kvrpcpb.RawBatchDeleteRequest{Keys: [][]byte{keys[1], keys[3], keys[5]}})
	c.Assert(batchDelResp, NotNil)

	batchPutReq.Pairs = []*kvrpcpb.KvPair{
		{Key: keys[6], Value: vals[6]},
		{Key: keys[7], Value: vals[7]},
		{Key: keys[8], Value: vals[8]},
	}
	batchPutResp, _ = h.RawBatchPut(nil, batchPutReq)
	c.Assert(batchPutResp, NotNil)

	scanReq := &kvrpcpb.RawScanRequest{StartKey: keys[0], EndKey: keys[9], Limit: 2}
	scanResp, _ := h.RawScan(nil, scanReq)
	c.Assert(batchPutResp, NotNil)
	c.Assert(scanResp.Kvs, HasLen, 2)
	c.Assert(batchPutReq.Pairs[:2], DeepEquals, scanResp.Kvs)

	delRangeResp, _ := h.RawDeleteRange(nil, &kvrpcpb.RawDeleteRangeRequest{StartKey: keys[0], EndKey: keys[9]})
	c.Assert(delRangeResp, NotNil)

	scanResp, _ = h.RawScan(nil, scanReq)
	c.Assert(scanResp.Kvs, HasLen, 0)
}
