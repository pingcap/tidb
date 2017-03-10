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
	"encoding/json"
	"fmt"
	"math"
	"net/http"

	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
)

type TidbRegionHandlerTestSuite struct {
	server *Server
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

func (ts *TidbRegionHandlerTestSuite) TestRegionsAPI(c *C) {
	ts.startServer(c)
	defer ts.stopServer(c)
	resp, err := http.Get("http://127.0.0.1:10090/tables/information_schema/SCHEMATA/regions")
	c.Assert(err, IsNil)

	//c.Assert(resp.StatusCode,Equals,http.StatusOK)
	defer resp.Body.Close()
	decoder := json.NewDecoder(resp.Body)
	type TableRegionsResponse struct {
		Success   bool          `json:"status"`
		Msg       string        `json:"message,omitempty"`
		RequestID string        `json:"request_id"`
		Data      *TableRegions `json:"data,omitempty"`
	}
	var data TableRegionsResponse
	err = decoder.Decode(&data)
	fmt.Printf("%+v", data)
	c.Assert(err, IsNil)

	c.Assert(len(data.Data.RowRegions) > 0, IsTrue)
	regionID := data.Data.RowRegions[0]

	// list region
	c.Assert(regionContainsTable(c, regionID, data.Data.TableID), IsTrue)
}

func regionContainsTable(c *C, regionID uint64, tableID int64) bool {
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:10090/regions/%v", regionID))
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	defer resp.Body.Close()
	decoder := json.NewDecoder(resp.Body)
	type RegionsResponse struct {
		Success   bool        `json:"status"`
		Msg       string      `json:"message,omitempty"`
		RequestID string      `json:"request_id"`
		Data      *RegionItem `json:"data,omitempty"`
	}
	var data RegionsResponse
	err = decoder.Decode(&data)
	c.Assert(err, IsNil)
	c.Assert(data.Success, IsTrue)
	ret := data.Data
	for _, index := range ret.Indices {
		if index.TableID == tableID {
			return true
		}
	}
	return false
}

func (ts *TidbRegionHandlerTestSuite) TestListTableRegionsWithError(c *C) {
	ts.startServer(c)
	defer ts.stopServer(c)
	resp, err := http.Get("http://127.0.0.1:10090/tables/fdsfds/aaa/regions")
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)
	decoder := json.NewDecoder(resp.Body)
	var data RegionsResponse
	err = decoder.Decode(&data)
	c.Assert(err, IsNil)
	c.Assert(data.Success, IsFalse)
	//c.Assert(resp.StatusCode,Equals,http.StatusOK)
	defer resp.Body.Close()
}

func (ts *TidbRegionHandlerTestSuite) TestGetRegionByIDWithError(c *C) {
	ts.startServer(c)
	defer ts.stopServer(c)
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:10090/regions/xxx"))
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)
	defer resp.Body.Close()
	decoder := json.NewDecoder(resp.Body)
	var data RegionsResponse
	err = decoder.Decode(&data)
	c.Assert(err, IsNil)
	c.Assert(data.Success, IsFalse)
}

func (ts *TidbRegionHandlerTestSuite) startServer(c *C) {
	log.SetLevelByString("error")
	store, err := tidb.NewStore("memory:///tmp/tidb")
	c.Assert(err, IsNil)
	_, err = tidb.BootstrapSession(store)
	c.Assert(err, IsNil)
	tidbdrv := NewTiDBDriver(store)
	cfg := &Config{
		Addr:         ":4001",
		LogLevel:     "debug",
		StatusAddr:   ":10090",
		ReportStatus: true,
		Store:        "tikv",
	}
	server, err := NewServer(cfg, tidbdrv)
	c.Assert(err, IsNil)
	ts.server = server
	go ts.server.startStatusHTTP()
	waitUntilServerOnline()
}

func (ts *TidbRegionHandlerTestSuite) stopServer(c *C) {
	if ts.server != nil {
		ts.server.Close()
	}
}
