// Copyright 2019 PingCAP, Inc.
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

package kv

import (
	"testing"

	"github.com/docker/go-units"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/parser/mysql"
)

type kvSuite struct{}

var _ = Suite(&kvSuite{})

func TestKV(t *testing.T) {
	TestingT(t)
}

func (s *kvSuite) TestSession(c *C) {
	session := newSession(&SessionOptions{SQLMode: mysql.ModeNone, Timestamp: 1234567890})
	_, err := session.Txn(true)
	c.Assert(err, IsNil)
}

func (s *kvSuite) TestKVMemBufInterweaveAllocAndRecycle(c *C) {
	type testCase struct {
		AllocSizes                []int
		FinalAvailableByteBufCaps []int
	}
	for _, tc := range []testCase{
		{
			AllocSizes: []int{
				1 * units.MiB,
				2 * units.MiB,
				3 * units.MiB,
				4 * units.MiB,
				5 * units.MiB,
			},
			// [2] => [2,4] => [2,4,8] => [4,2,8] => [4,2,8,16]
			FinalAvailableByteBufCaps: []int{
				4 * units.MiB,
				2 * units.MiB,
				8 * units.MiB,
				16 * units.MiB,
			},
		},
		{
			AllocSizes: []int{
				5 * units.MiB,
				4 * units.MiB,
				3 * units.MiB,
				2 * units.MiB,
				1 * units.MiB,
			},
			// [16] => [16] => [16] => [16] => [16]
			FinalAvailableByteBufCaps: []int{16 * units.MiB},
		},
		{
			AllocSizes: []int{5, 4, 3, 2, 1},
			// [1] => [1] => [1] => [1] => [1]
			FinalAvailableByteBufCaps: []int{1 * units.MiB},
		},
		{
			AllocSizes: []int{
				1 * units.MiB,
				2 * units.MiB,
				3 * units.MiB,
				2 * units.MiB,
				1 * units.MiB,
				5 * units.MiB,
			},
			// [2] => [2,4] => [2,4,8] => [2,8,4] => [8,4,2] => [8,4,2,16]
			FinalAvailableByteBufCaps: []int{
				8 * units.MiB,
				4 * units.MiB,
				2 * units.MiB,
				16 * units.MiB,
			},
		},
	} {
		testKVMemBuf := &kvMemBuf{}
		for _, allocSize := range tc.AllocSizes {
			testKVMemBuf.AllocateBuf(allocSize)
			testKVMemBuf.Recycle(testKVMemBuf.buf)
		}
		c.Assert(len(testKVMemBuf.availableBufs), Equals, len(tc.FinalAvailableByteBufCaps))
		for i, bb := range testKVMemBuf.availableBufs {
			c.Assert(bb.cap, Equals, tc.FinalAvailableByteBufCaps[i])
		}
	}
}

func (s *kvSuite) TestKVMemBufBatchAllocAndRecycle(c *C) {
	testKVMemBuf := &kvMemBuf{}
	bBufs := []*bytesBuf{}
	for i := 0; i < maxAvailableBufSize; i++ {
		testKVMemBuf.AllocateBuf(1 * units.MiB)
		bBufs = append(bBufs, testKVMemBuf.buf)
	}
	for i := 0; i < maxAvailableBufSize; i++ {
		testKVMemBuf.AllocateBuf(2 * units.MiB)
		bBufs = append(bBufs, testKVMemBuf.buf)
	}
	for _, bb := range bBufs {
		testKVMemBuf.Recycle(bb)
	}
	c.Assert(len(testKVMemBuf.availableBufs), Equals, maxAvailableBufSize)
	for _, bb := range testKVMemBuf.availableBufs {
		c.Assert(bb.cap, Equals, 4*units.MiB)
	}
	bBufs = bBufs[:0]
	for i := 0; i < maxAvailableBufSize; i++ {
		testKVMemBuf.AllocateBuf(1 * units.MiB)
		bb := testKVMemBuf.buf
		c.Assert(bb.cap, Equals, 4*units.MiB)
		bBufs = append(bBufs, bb)
		c.Assert(len(testKVMemBuf.availableBufs), Equals, maxAvailableBufSize-i-1)
	}
	for _, bb := range bBufs {
		testKVMemBuf.Recycle(bb)
	}
	c.Assert(len(testKVMemBuf.availableBufs), Equals, maxAvailableBufSize)
}
