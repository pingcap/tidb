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

package server

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/mock-tikv"
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
	indexRange, err := NewRegionFrameRange(region)
	c.Assert(err, IsNil)
	c.Assert(indexRange.firstTableID(), Equals, sTableID)
	c.Assert(indexRange.lastTableID(), Equals, eTableID)
	c.Assert(indexRange.first.IndexID, Equals, sIndex)
	c.Assert(indexRange.first.IsRecord, IsFalse)
	c.Assert(indexRange.last.IsRecord, IsTrue)
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
	indexRange, err := NewRegionFrameRange(region)
	c.Assert(err, IsNil)
	c.Assert(indexRange.firstTableID(), Equals, sTableID)
	c.Assert(indexRange.lastTableID(), Equals, eTableID)
	c.Assert(indexRange.first.IsRecord, IsTrue)
	c.Assert(indexRange.last.IsRecord, IsTrue)
	start, end := indexRange.getIndexRangeForTable(sTableID)
	c.Assert(start, Equals, int64(math.MaxInt64))
	c.Assert(end, Equals, int64(math.MaxInt64))
	start, end = indexRange.getIndexRangeForTable(eTableID)
	c.Assert(start, Equals, int64(math.MinInt64))
	c.Assert(end, Equals, int64(math.MaxInt64))
}

func (ts *TidbRegionHandlerTestSuite) TestRegionIndexRangeWithStartNoLimit(c *C) {
	sTableID := int64(math.MinInt64)
	sIndexID := int64(math.MinInt64)
	eTableID := int64(9)
	startKey := codec.EncodeBytes(nil, []byte("m_aaaaafdfd"))
	endKey := codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(eTableID))
	region := &tikv.KeyLocation{
		tikv.RegionVerID{},
		startKey,
		endKey,
	}
	indexRange, err := NewRegionFrameRange(region)
	c.Assert(err, IsNil)
	c.Assert(indexRange.firstTableID(), Equals, sTableID)
	c.Assert(indexRange.lastTableID(), Equals, eTableID)
	c.Assert(indexRange.first.IndexID, Equals, sIndexID)
	c.Assert(indexRange.first.IsRecord, IsFalse)
	c.Assert(indexRange.last.IsRecord, IsTrue)
	start, end := indexRange.getIndexRangeForTable(sTableID)
	c.Assert(start, Equals, sIndexID)
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
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	defer resp.Body.Close()
	decoder := json.NewDecoder(resp.Body)

	var data TableRegions
	err = decoder.Decode(&data)
	c.Assert(err, IsNil)
	c.Assert(len(data.RecordRegions) > 0, IsTrue)

	// list region
	for _, region := range data.RecordRegions {
		c.Assert(regionContainsTable(c, region.ID, data.TableID), IsTrue)
	}
}

func regionContainsTable(c *C, regionID uint64, tableID int64) bool {
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:10090/regions/%d", regionID))
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	defer resp.Body.Close()
	decoder := json.NewDecoder(resp.Body)
	var data RegionDetail
	err = decoder.Decode(&data)
	c.Assert(err, IsNil)
	for _, index := range data.Frames {
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
	defer resp.Body.Close()
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)
}

func (ts *TidbRegionHandlerTestSuite) TestGetRegionByIDWithError(c *C) {
	ts.startServer(c)
	defer ts.stopServer(c)
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:10090/regions/xxx"))
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)
	defer resp.Body.Close()
}

func (ts *TidbRegionHandlerTestSuite) startServer(c *C) {
	cluster := mocktikv.NewCluster()
	store, err := tikv.NewMockTikvStoreWithCluster(cluster)
	c.Assert(err, IsNil)
	pdCli := mocktikv.NewPDClient(cluster)
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
	once.Do(func() {
		go server.startHTTPServer(pdCli)
	})
	waitUntilServerOnline(cfg.StatusAddr)
}

func (ts *TidbRegionHandlerTestSuite) stopServer(c *C) {
	if ts.server != nil {
		ts.server.Close()
	}
}
