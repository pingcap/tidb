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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/mocktikv"
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

	startKey := tablecodec.EncodeTableIndexPrefix(sTableID, sIndex)
	endKey := tablecodec.GenTableRecordPrefix(eTableID)

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
	startKey := tablecodec.GenTableRecordPrefix(sTableID)
	endKey := []byte("z_aaaaafdfd")
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
	startKey := []byte("m_aaaaafdfd")
	endKey := tablecodec.GenTableRecordPrefix(eTableID)
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

func (ts *TidbRegionHandlerTestSuite) TestRegionsFromMeta(c *C) {
	ts.startServer(c)
	defer ts.stopServer(c)
	resp, err := http.Get("http://127.0.0.1:10090/regions/meta")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)

	// Verify the resp body.
	decoder := json.NewDecoder(resp.Body)
	metas := make([]RegionMeta, 0)
	err = decoder.Decode(&metas)
	c.Assert(err, IsNil)
	for _, meta := range metas {
		c.Assert(meta.ID != 0, IsTrue)
	}
}

func (ts *TidbRegionHandlerTestSuite) startServer(c *C) {
	mvccStore := mocktikv.NewMvccStore()
	store, err := tikv.NewMockTikvStore(tikv.WithMVCCStore(mvccStore))
	c.Assert(err, IsNil)
	_, err = tidb.BootstrapSession(store)
	c.Assert(err, IsNil)
	tidbdrv := NewTiDBDriver(store)

	cfg := config.NewConfig()
	cfg.Port = 4001
	cfg.Store = "tikv"
	cfg.Status.StatusPort = 10090
	cfg.Status.ReportStatus = true

	server, err := NewServer(cfg, tidbdrv)
	c.Assert(err, IsNil)
	ts.server = server
	go server.Run()
	waitUntilServerOnline(cfg.Status.StatusPort)
}

func (ts *TidbRegionHandlerTestSuite) stopServer(c *C) {
	if ts.server != nil {
		ts.server.Close()
	}
}

func (ts *TidbRegionHandlerTestSuite) prepareData(c *C) {
	db, err := sql.Open("mysql", getDSN())
	c.Assert(err, IsNil, Commentf("Error connecting"))
	defer db.Close()
	dbt := &DBTest{c, db}

	dbt.mustExec("create database tidb;")
	dbt.mustExec("use tidb;")
	dbt.mustExec("create table tidb.test (a int auto_increment primary key, b varchar(20));")
	dbt.mustExec("insert tidb.test values (1, 1);")
	txn1, err := dbt.db.Begin()
	c.Assert(err, IsNil)
	_, err = txn1.Exec("update tidb.test set b = b + 1 where a = 1;")
	c.Assert(err, IsNil)
	_, err = txn1.Exec("insert tidb.test values (2, 2);")
	c.Assert(err, IsNil)
	_, err = txn1.Exec("insert tidb.test (a) values (3);")
	c.Assert(err, IsNil)
	_, err = txn1.Exec("insert tidb.test values (4, '');")
	c.Assert(err, IsNil)
	err = txn1.Commit()
	c.Assert(err, IsNil)
	dbt.mustExec("alter table tidb.test add index idx1 (a, b);")
	dbt.mustExec("alter table tidb.test add unique index idx2 (a, b);")
}

func decodeKeyMvcc(closer io.ReadCloser, c *C, valid bool) {
	decoder := json.NewDecoder(closer)
	var data kvrpcpb.MvccGetByKeyResponse
	err := decoder.Decode(&data)
	c.Assert(err, IsNil)
	if valid {
		c.Assert(data.Info, NotNil)
		c.Assert(len(data.Info.Writes), Greater, 0)
	} else {
		c.Assert(data.Info, IsNil)
	}
}

func (ts *TidbRegionHandlerTestSuite) TestGetTableMvcc(c *C) {
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

	_, key, err := codec.DecodeBytes(p1.Key)
	c.Assert(err, IsNil)
	hexKey := hex.EncodeToString(key)
	resp, err = http.Get("http://127.0.0.1:10090/mvcc/hex/" + hexKey)
	c.Assert(err, IsNil)
	decoder = json.NewDecoder(resp.Body)
	var data2 kvrpcpb.MvccGetByKeyResponse
	err = decoder.Decode(&data2)
	c.Assert(err, IsNil)
	c.Assert(data2, DeepEquals, data)
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

func (ts *TidbRegionHandlerTestSuite) TestGetIndexMvcc(c *C) {
	ts.startServer(c)
	ts.prepareData(c)
	defer ts.stopServer(c)

	// tests for normal index key
	resp, err := http.Get("http://127.0.0.1:10090/mvcc/index/tidb/test/idx1/1?a=1&b=2")
	c.Assert(err, IsNil)
	decodeKeyMvcc(resp.Body, c, true)

	resp, err = http.Get("http://127.0.0.1:10090/mvcc/index/tidb/test/idx2/1?a=1&b=2")
	c.Assert(err, IsNil)
	decodeKeyMvcc(resp.Body, c, true)

	// tests for index key which includes null
	resp, err = http.Get("http://127.0.0.1:10090/mvcc/index/tidb/test/idx1/3?a=3&b")
	c.Assert(err, IsNil)
	decodeKeyMvcc(resp.Body, c, true)

	resp, err = http.Get("http://127.0.0.1:10090/mvcc/index/tidb/test/idx2/3?a=3&b")
	c.Assert(err, IsNil)
	decodeKeyMvcc(resp.Body, c, true)

	// tests for index key which includes empty string
	resp, err = http.Get("http://127.0.0.1:10090/mvcc/index/tidb/test/idx1/4?a=4&b=")
	c.Assert(err, IsNil)
	decodeKeyMvcc(resp.Body, c, true)

	resp, err = http.Get("http://127.0.0.1:10090/mvcc/index/tidb/test/idx2/3?a=4&b=")
	c.Assert(err, IsNil)
	decodeKeyMvcc(resp.Body, c, true)

	// tests for wrong key
	resp, err = http.Get("http://127.0.0.1:10090/mvcc/index/tidb/test/idx1/5?a=5&b=1")
	c.Assert(err, IsNil)
	decodeKeyMvcc(resp.Body, c, false)

	resp, err = http.Get("http://127.0.0.1:10090/mvcc/index/tidb/test/idx2/5?a=5&b=1")
	c.Assert(err, IsNil)
	decodeKeyMvcc(resp.Body, c, false)

	// tests for missing column value
	resp, err = http.Get("http://127.0.0.1:10090/mvcc/index/tidb/test/idx1/1?a=1")
	c.Assert(err, IsNil)
	decoder := json.NewDecoder(resp.Body)
	var data1 kvrpcpb.MvccGetByKeyResponse
	err = decoder.Decode(&data1)
	c.Assert(err, NotNil)

	resp, err = http.Get("http://127.0.0.1:10090/mvcc/index/tidb/test/idx2/1?a=1")
	c.Assert(err, IsNil)
	decoder = json.NewDecoder(resp.Body)
	var data2 kvrpcpb.MvccGetByKeyResponse
	err = decoder.Decode(&data2)
	c.Assert(err, NotNil)
}

func (ts *TidbRegionHandlerTestSuite) TestGetSchema(c *C) {
	ts.startServer(c)
	ts.prepareData(c)
	defer ts.stopServer(c)
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:10090/schema"))
	c.Assert(err, IsNil)
	decoder := json.NewDecoder(resp.Body)
	var dbs []*model.DBInfo
	err = decoder.Decode(&dbs)
	c.Assert(err, IsNil)
	expects := []string{"information_schema", "mysql", "performance_schema", "test", "tidb"}
	names := make([]string, len(dbs))
	for i, v := range dbs {
		names[i] = v.Name.L
	}
	sort.Strings(names)
	c.Assert(names, DeepEquals, expects)

	resp, err = http.Get(fmt.Sprintf("http://127.0.0.1:10090/schema?table_id=5"))
	c.Assert(err, IsNil)
	var t *model.TableInfo
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&t)
	c.Assert(err, IsNil)
	c.Assert(t.Name.L, Equals, "user")

	_, err = http.Get(fmt.Sprintf("http://127.0.0.1:10090/schema?table_id=a"))
	c.Assert(err, IsNil)

	_, err = http.Get(fmt.Sprintf("http://127.0.0.1:10090/schema?table_id=1"))
	c.Assert(err, IsNil)

	_, err = http.Get(fmt.Sprintf("http://127.0.0.1:10090/schema?table_id=-1"))
	c.Assert(err, IsNil)

	resp, err = http.Get(fmt.Sprintf("http://127.0.0.1:10090/schema/tidb"))
	c.Assert(err, IsNil)
	var lt []*model.TableInfo
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&lt)
	c.Assert(err, IsNil)
	c.Assert(lt[0].Name.L, Equals, "test")

	_, err = http.Get(fmt.Sprintf("http://127.0.0.1:10090/schema/abc"))
	c.Assert(err, IsNil)

	resp, err = http.Get(fmt.Sprintf("http://127.0.0.1:10090/schema/tidb/test"))
	c.Assert(err, IsNil)
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&t)
	c.Assert(err, IsNil)
	c.Assert(t.Name.L, Equals, "test")

	_, err = http.Get(fmt.Sprintf("http://127.0.0.1:10090/schema/tidb/abc"))
	c.Assert(err, IsNil)
}
