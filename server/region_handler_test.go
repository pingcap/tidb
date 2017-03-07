// Copyright 2017 PingCAP, Inc.
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
// +build !race

package server

import (
	"math"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
)

type TidbRegionHandlerTestSuite struct {
	tidbdrv *TiDBDriver
	server  *Server
}

var _ = Suite(new(TidbRegionHandlerTestSuite))

func (ts *TidbRegionHandlerTestSuite) TestRegionIndexRange(c *C) {
	sTableID := int64(3)
	sIndex := int64(11)
	eTableID := int64(9)

	startKey := codec.EncodeBytes(nil, tablecodec.EncodeTableIndexPrefix(sTableID, sIndex))
	endKey := codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(eTableID))
	region := &tikv.KeyLocation{
		tikv.RegionVerID{},
		startKey,
		endKey,
	}
	indexRange := NewRegionIndexRange(region)
	c.Assert(indexRange.firstTableID(), Equals, sTableID)
	c.Assert(indexRange.lastTableID(), Equals, eTableID)
	c.Assert(indexRange.first.indexID, Equals, sIndex)
	c.Assert(indexRange.first.isRecordKey, IsFalse)
	c.Assert(indexRange.last.isRecordKey, IsTrue)
	start, end := indexRange.getIndexRangeForTable(sTableID)
	c.Assert(start, Equals, sIndex)
	c.Assert(end, Equals, int64(math.MaxInt64))
	start, end = indexRange.getIndexRangeForTable(eTableID)
	c.Assert(start, Equals, int64(math.MinInt64))
	c.Assert(end, Equals, int64(math.MaxInt64))
}

func (ts *TidbRegionHandlerTestSuite) TestRegionIndexRangeWithEndNoLimit(c *C) {
	sTableID := int64(15)
	eTableID := int64(math.MaxInt64)
	startKey := codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(sTableID))
	endKey := codec.EncodeBytes(nil, []byte("z_aaaaafdfd"))
	region := &tikv.KeyLocation{
		tikv.RegionVerID{},
		startKey,
		endKey,
	}
	indexRange := NewRegionIndexRange(region)
	c.Assert(indexRange.firstTableID(), Equals, sTableID)
	c.Assert(indexRange.lastTableID(), Equals, eTableID)
	c.Assert(indexRange.first.isRecordKey, IsTrue)
	c.Assert(indexRange.last.isRecordKey, IsTrue)
	start, end := indexRange.getIndexRangeForTable(sTableID)
	c.Assert(start, Equals, int64(math.MaxInt64))
	c.Assert(end, Equals, int64(math.MaxInt64))
	start, end = indexRange.getIndexRangeForTable(eTableID)
	c.Assert(start, Equals, int64(math.MinInt64))
	c.Assert(end, Equals, int64(math.MaxInt64))
}

func (ts *TidbRegionHandlerTestSuite) TestRegionIndexRangeWithStartNoLimit(c *C) {
	sTableID := int64(0)
	sIndexID := int64(math.MinInt64)
	eTableID := int64(9)
	startKey := codec.EncodeBytes(nil, []byte("m_aaaaafdfd"))
	endKey := codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(eTableID))
	region := &tikv.KeyLocation{
		tikv.RegionVerID{},
		startKey,
		endKey,
	}
	indexRange := NewRegionIndexRange(region)
	c.Assert(indexRange.firstTableID(), Equals, sTableID)
	c.Assert(indexRange.lastTableID(), Equals, eTableID)
	c.Assert(indexRange.first.indexID, Equals, sIndexID)
	c.Assert(indexRange.first.isRecordKey, IsFalse)
	c.Assert(indexRange.last.isRecordKey, IsTrue)
	start, end := indexRange.getIndexRangeForTable(sTableID)
	c.Assert(start, Equals, sIndexID)
	c.Assert(end, Equals, int64(math.MaxInt64))
	start, end = indexRange.getIndexRangeForTable(eTableID)
	c.Assert(start, Equals, int64(math.MinInt64))
	c.Assert(end, Equals, int64(math.MaxInt64))
}

func (ts *TidbRegionHandlerTestSuite) TestRegionIndexRangeWithBinarySearch(c *C) {
	sTableID := int64(5)
	eTableID := int64(1000)
	startKey := codec.EncodeBytes(nil, append(tablecodec.EncodeTablePrefix(sTableID-1), []byte("_xxxx")...))
	endKey := codec.EncodeBytes(nil, append(tablecodec.EncodeTablePrefix(eTableID+1), []byte("_aaa")...))
	region := &tikv.KeyLocation{
		tikv.RegionVerID{},
		startKey,
		endKey,
	}
	indexRange := NewRegionIndexRange(region)
	c.Assert(indexRange.firstTableID(), Equals, sTableID)
	c.Assert(indexRange.lastTableID(), Equals, eTableID)
	c.Assert(indexRange.first.isRecordKey, IsFalse)
	c.Assert(indexRange.last.isRecordKey, IsTrue)
	start, end := indexRange.getIndexRangeForTable(sTableID)
	c.Assert(start, Equals, int64(math.MinInt64))
	c.Assert(end, Equals, int64(math.MaxInt64))
	start, end = indexRange.getIndexRangeForTable(eTableID)
	c.Assert(start, Equals, int64(math.MinInt64))
	c.Assert(end, Equals, int64(math.MaxInt64))
}
