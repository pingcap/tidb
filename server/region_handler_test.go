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
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"net/http"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/config"
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
		Region:   tikv.RegionVerID{},
		StartKey: startKey,
		EndKey:   endKey,
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
		Region:   tikv.RegionVerID{},
		StartKey: startKey,
		EndKey:   endKey,
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
		Region:   tikv.RegionVerID{},
		StartKey: startKey,
		EndKey:   endKey,
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
	c.Assert(err, IsNil)
	defer resp.Body.Close()
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
	mocktikv.BootstrapWithSingleStore(cluster)
	store, err := tikv.NewMockTikvStore(tikv.WithCluster(cluster))
	c.Assert(err, IsNil)
	_, err = tidb.BootstrapSession(store)
	c.Assert(err, IsNil)
	tidbdrv := NewTiDBDriver(store)

	cfg := &config.Config{
		Addr:         ":4001",
		LogLevel:     "debug",
		StatusAddr:   ":10090",
		ReportStatus: true,
		Store:        "tikv",
	}

	server, err := NewServer(cfg, tidbdrv)
	c.Assert(err, IsNil)
	ts.server = server
	go server.Run()
	waitUntilServerOnline(cfg.StatusAddr)
}

func (ts *TidbRegionHandlerTestSuite) stopServer(c *C) {
	if ts.server != nil {
		ts.server.Close()
	}
}

func (ts *TidbRegionHandlerTestSuite) prepareData(c *C) {
	db, err := sql.Open("mysql", dsn)
	c.Assert(err, IsNil, Commentf("Error connecting"))
	defer db.Close()
	dbt := &DBTest{c, db}

	dbt.mustExec("create database tidb;")
	dbt.mustExec("use tidb;")
	dbt.mustExec("create table tidb.test (a int auto_increment primary key, b int);")
	dbt.mustExec("insert tidb.test values (1, 1);")
	txn1, err := dbt.db.Begin()
	c.Assert(err, IsNil)
	_, err = txn1.Exec("update tidb.test set b = b + 1 where a = 1;")
	c.Assert(err, IsNil)
	_, err = txn1.Exec("insert tidb.test values (2,2);")
	c.Assert(err, IsNil)
	err = txn1.Commit()
	c.Assert(err, IsNil)
}

func (ts *TidbRegionHandlerTestSuite) TestGetMvcc(c *C) {
	ts.startServer(c)
	ts.prepareData(c)
	defer ts.stopServer(c)
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:10090/mvcc/key/tidb/test/1"))
	c.Assert(err, IsNil)
	decoder := json.NewDecoder(resp.Body)
	var data kvrpcpb.MvccGetByKeyResponse
	err = decoder.Decode(&data)
	c.Assert(err, IsNil)
	c.Assert(data.Info, NotNil)
	c.Assert(len(data.Info.Writes), Greater, 0)
	startTs := data.Info.Writes[0].StartTs

	resp, err = http.Get(fmt.Sprintf("http://127.0.0.1:10090/mvcc/txn/%d", startTs))
	c.Assert(err, IsNil)
	var p1 kvrpcpb.MvccGetByStartTsResponse
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&p1)
	c.Assert(err, IsNil)

	resp, err = http.Get(fmt.Sprintf("http://127.0.0.1:10090/mvcc/txn/%d/tidb/test", startTs))
	c.Assert(err, IsNil)
	var p2 kvrpcpb.MvccGetByStartTsResponse
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&p2)
	c.Assert(err, IsNil)

	for id, expect := range data.Info.Values {
		v1 := p1.Info.Values[id].Value
		v2 := p2.Info.Values[id].Value
		c.Assert(bytes.Equal(v1, expect.Value), IsTrue)
		c.Assert(bytes.Equal(v2, expect.Value), IsTrue)
	}
}

func (ts *TidbRegionHandlerTestSuite) TestGetMvccNotFound(c *C) {
	ts.startServer(c)
	ts.prepareData(c)
	defer ts.stopServer(c)
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:10090/mvcc/key/tidb/test/1234"))
	c.Assert(err, IsNil)
	decoder := json.NewDecoder(resp.Body)
	var data kvrpcpb.MvccGetByKeyResponse
	err = decoder.Decode(&data)
	c.Assert(err, IsNil)
	c.Assert(data.Info, IsNil)

	resp, err = http.Get(fmt.Sprintf("http://127.0.0.1:10090/mvcc/txn/0"))
	c.Assert(err, IsNil)
	var p kvrpcpb.MvccGetByStartTsResponse
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&p)
	c.Assert(err, IsNil)
	c.Assert(p.Info, IsNil)
}
