// Copyright 2018 PingCAP, Inc.
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
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"sort"
	"sync/atomic"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	zaplog "github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/printer"
	log "github.com/sirupsen/logrus"
	"go.uber.org/zap"
)

type HTTPHandlerTestSuite struct {
	server  *Server
	store   kv.Storage
	domain  *domain.Domain
	tidbdrv *TiDBDriver
}

var _ = Suite(new(HTTPHandlerTestSuite))

func (ts *HTTPHandlerTestSuite) TestRegionIndexRange(c *C) {
	sTableID := int64(3)
	sIndex := int64(11)
	eTableID := int64(9)
	recordID := int64(133)
	indexValues := []types.Datum{
		types.NewIntDatum(100),
		types.NewBytesDatum([]byte("foobar")),
		types.NewFloat64Datum(-100.25),
	}
	expectIndexValues := make([]string, 0, len(indexValues))
	for _, v := range indexValues {
		str, err := v.ToString()
		if err != nil {
			str = fmt.Sprintf("%d-%v", v.Kind(), v.GetValue())
		}
		expectIndexValues = append(expectIndexValues, str)
	}
	encodedValue, err := codec.EncodeKey(&stmtctx.StatementContext{TimeZone: time.Local}, nil, indexValues...)
	c.Assert(err, IsNil)

	startKey := tablecodec.EncodeIndexSeekKey(sTableID, sIndex, encodedValue)
	recordPrefix := tablecodec.GenTableRecordPrefix(eTableID)
	endKey := tablecodec.EncodeRecordKey(recordPrefix, recordID)

	region := &tikv.KeyLocation{
		Region:   tikv.RegionVerID{},
		StartKey: startKey,
		EndKey:   endKey,
	}
	r, err := helper.NewRegionFrameRange(region)
	c.Assert(err, IsNil)
	c.Assert(r.First.IndexID, Equals, sIndex)
	c.Assert(r.First.IsRecord, IsFalse)
	c.Assert(r.First.RecordID, Equals, int64(0))
	c.Assert(r.First.IndexValues, DeepEquals, expectIndexValues)
	c.Assert(r.Last.RecordID, Equals, recordID)
	c.Assert(r.Last.IndexValues, IsNil)

	testCases := []struct {
		tableID int64
		indexID int64
		isCover bool
	}{
		{2, 0, false},
		{3, 0, true},
		{9, 0, true},
		{10, 0, false},
		{2, 10, false},
		{3, 10, false},
		{3, 11, true},
		{3, 20, true},
		{9, 10, true},
		{10, 1, false},
	}
	for _, t := range testCases {
		var f *helper.FrameItem
		if t.indexID == 0 {
			f = r.GetRecordFrame(t.tableID, "", "")
		} else {
			f = r.GetIndexFrame(t.tableID, t.indexID, "", "", "")
		}
		if t.isCover {
			c.Assert(f, NotNil)
		} else {
			c.Assert(f, IsNil)
		}
	}
}

func (ts *HTTPHandlerTestSuite) TestRegionIndexRangeWithEndNoLimit(c *C) {
	sTableID := int64(15)
	startKey := tablecodec.GenTableRecordPrefix(sTableID)
	endKey := []byte("z_aaaaafdfd")
	region := &tikv.KeyLocation{
		Region:   tikv.RegionVerID{},
		StartKey: startKey,
		EndKey:   endKey,
	}
	r, err := helper.NewRegionFrameRange(region)
	c.Assert(err, IsNil)
	c.Assert(r.First.IsRecord, IsTrue)
	c.Assert(r.Last.IsRecord, IsTrue)
	c.Assert(r.GetRecordFrame(300, "", ""), NotNil)
	c.Assert(r.GetIndexFrame(200, 100, "", "", ""), NotNil)
}

func (ts *HTTPHandlerTestSuite) TestRegionIndexRangeWithStartNoLimit(c *C) {
	eTableID := int64(9)
	startKey := []byte("m_aaaaafdfd")
	endKey := tablecodec.GenTableRecordPrefix(eTableID)
	region := &tikv.KeyLocation{
		Region:   tikv.RegionVerID{},
		StartKey: startKey,
		EndKey:   endKey,
	}
	r, err := helper.NewRegionFrameRange(region)
	c.Assert(err, IsNil)
	c.Assert(r.First.IsRecord, IsFalse)
	c.Assert(r.Last.IsRecord, IsTrue)
	c.Assert(r.GetRecordFrame(3, "", ""), NotNil)
	c.Assert(r.GetIndexFrame(8, 1, "", "", ""), NotNil)
}

func (ts *HTTPHandlerTestSuite) TestRegionsAPI(c *C) {
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

func (ts *HTTPHandlerTestSuite) TestListTableRegions(c *C) {
	ts.startServer(c)
	defer ts.stopServer(c)
	ts.prepareData(c)
	// Test list table regions with error
	resp, err := http.Get("http://127.0.0.1:10090/tables/fdsfds/aaa/regions")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)

	resp, err = http.Get("http://127.0.0.1:10090/tables/tidb/pt/regions")
	c.Assert(err, IsNil)
	defer resp.Body.Close()

	var data []*TableRegions
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&data)
	c.Assert(err, IsNil)

	region := data[1]
	resp, err = http.Get(fmt.Sprintf("http://127.0.0.1:10090/regions/%d", region.TableID))
	c.Assert(err, IsNil)
}

func (ts *HTTPHandlerTestSuite) TestGetRegionByIDWithError(c *C) {
	ts.startServer(c)
	defer ts.stopServer(c)
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:10090/regions/xxx"))
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)
	defer resp.Body.Close()
}

func (ts *HTTPHandlerTestSuite) TestBinlogRecover(c *C) {
	ts.startServer(c)
	defer ts.stopServer(c)
	binloginfo.EnableSkipBinlogFlag()
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, true)
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:10090/binlog/recover"))
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, false)

	// Invalid operation will use the default operation.
	binloginfo.EnableSkipBinlogFlag()
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, true)
	resp, err = http.Get(fmt.Sprintf("http://127.0.0.1:10090/binlog/recover?op=abc"))
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, false)

	binloginfo.EnableSkipBinlogFlag()
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, true)
	resp, err = http.Get(fmt.Sprintf("http://127.0.0.1:10090/binlog/recover?op=abc&seconds=1"))
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, false)

	binloginfo.EnableSkipBinlogFlag()
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, true)
	binloginfo.AddOneSkippedCommitter()
	resp, err = http.Get(fmt.Sprintf("http://127.0.0.1:10090/binlog/recover?op=abc&seconds=1"))
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, false)
	binloginfo.RemoveOneSkippedCommitter()

	binloginfo.AddOneSkippedCommitter()
	c.Assert(binloginfo.SkippedCommitterCount(), Equals, int32(1))
	resp, err = http.Get(fmt.Sprintf("http://127.0.0.1:10090/binlog/recover?op=reset"))
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(binloginfo.SkippedCommitterCount(), Equals, int32(0))

	binloginfo.EnableSkipBinlogFlag()
	resp, err = http.Get(fmt.Sprintf("http://127.0.0.1:10090/binlog/recover?op=nowait"))
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, false)

	// Only the first should work.
	binloginfo.EnableSkipBinlogFlag()
	resp, err = http.Get(fmt.Sprintf("http://127.0.0.1:10090/binlog/recover?op=nowait&op=reset"))
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, false)

	resp, err = http.Get(fmt.Sprintf("http://127.0.0.1:10090/binlog/recover?op=status"))
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
}

func (ts *HTTPHandlerTestSuite) TestRegionsFromMeta(c *C) {
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

	// test no panic
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/server/errGetRegionByIDEmpty", `return(true)`), IsNil)
	resp1, err := http.Get("http://127.0.0.1:10090/regions/meta")
	c.Assert(err, IsNil)
	defer resp1.Body.Close()
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/server/errGetRegionByIDEmpty"), IsNil)
}

func (ts *HTTPHandlerTestSuite) startServer(c *C) {
	mvccStore := mocktikv.MustNewMVCCStore()
	var err error
	ts.store, err = mockstore.NewMockTikvStore(mockstore.WithMVCCStore(mvccStore))
	c.Assert(err, IsNil)
	ts.domain, err = session.BootstrapSession(ts.store)
	c.Assert(err, IsNil)
	ts.tidbdrv = NewTiDBDriver(ts.store)

	cfg := config.NewConfig()
	cfg.Port = 4001
	cfg.Store = "tikv"
	cfg.Status.StatusPort = 10090
	cfg.Status.ReportStatus = true

	server, err := NewServer(cfg, ts.tidbdrv)
	c.Assert(err, IsNil)
	ts.server = server
	go server.Run()
	waitUntilServerOnline(cfg.Status.StatusPort)
}

func (ts *HTTPHandlerTestSuite) stopServer(c *C) {
	if ts.domain != nil {
		ts.domain.Close()
	}
	if ts.store != nil {
		ts.store.Close()
	}
	if ts.server != nil {
		ts.server.Close()
	}
}

func (ts *HTTPHandlerTestSuite) prepareData(c *C) {
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

	dbt.mustExec(`create table tidb.pt (a int primary key, b varchar(20), key idx(a, b))
partition by range (a)
(partition p0 values less than (256),
 partition p1 values less than (512),
 partition p2 values less than (1024))`)

	txn2, err := dbt.db.Begin()
	c.Assert(err, IsNil)
	txn2.Exec("insert into tidb.pt values (42, '123')")
	txn2.Exec("insert into tidb.pt values (256, 'b')")
	txn2.Exec("insert into tidb.pt values (666, 'def')")
	err = txn2.Commit()
	c.Assert(err, IsNil)
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

func (ts *HTTPHandlerTestSuite) TestGetTableMVCC(c *C) {
	ts.startServer(c)
	ts.prepareData(c)
	defer ts.stopServer(c)

	c.Skip("MVCCLevelDB doesn't implement MVCCDebugger interface.")
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

	_, key, err := codec.DecodeBytes(p1.Key, nil)
	c.Assert(err, IsNil)
	hexKey := hex.EncodeToString(key)
	resp, err = http.Get("http://127.0.0.1:10090/mvcc/hex/" + hexKey)
	c.Assert(err, IsNil)
	decoder = json.NewDecoder(resp.Body)
	var data2 kvrpcpb.MvccGetByKeyResponse
	err = decoder.Decode(&data2)
	c.Assert(err, IsNil)
	c.Assert(data2, DeepEquals, data)

	resp, err = http.Get(fmt.Sprintf("http://127.0.0.1:10090/mvcc/key/tidb/test/1?decode=true"))
	c.Assert(err, IsNil)
	decoder = json.NewDecoder(resp.Body)
	var data3 map[string]interface{}
	err = decoder.Decode(&data3)
	c.Assert(err, IsNil)
	c.Assert(data3["key"], NotNil)
	c.Assert(data3["info"], NotNil)
	c.Assert(data3["data"], NotNil)
	c.Assert(data3["decode_error"], IsNil)

	resp, err = http.Get(fmt.Sprintf("http://127.0.0.1:10090/mvcc/key/tidb/pt(p0)/42?decode=true"))
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	decoder = json.NewDecoder(resp.Body)
	var data4 map[string]interface{}
	err = decoder.Decode(&data4)
	c.Assert(err, IsNil)
	c.Assert(data4["key"], NotNil)
	c.Assert(data4["info"], NotNil)
	c.Assert(data4["data"], NotNil)
	c.Assert(data4["decode_error"], IsNil)
}

func (ts *HTTPHandlerTestSuite) TestGetMVCCNotFound(c *C) {
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

	c.Skip("MVCCLevelDB doesn't implement MVCCDebugger interface.")
	resp, err = http.Get(fmt.Sprintf("http://127.0.0.1:10090/mvcc/txn/0"))
	c.Assert(err, IsNil)
	var p kvrpcpb.MvccGetByStartTsResponse
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&p)
	c.Assert(err, IsNil)
	c.Assert(p.Info, IsNil)
}

func (ts *HTTPHandlerTestSuite) TestDecodeColumnValue(c *C) {
	ts.startServer(c)
	ts.prepareData(c)
	defer ts.stopServer(c)

	// column is a structure used for test
	type column struct {
		id int64
		tp *types.FieldType
	}
	// Backfill columns.
	c1 := &column{id: 1, tp: types.NewFieldType(mysql.TypeLonglong)}
	c2 := &column{id: 2, tp: types.NewFieldType(mysql.TypeVarchar)}
	c3 := &column{id: 3, tp: types.NewFieldType(mysql.TypeNewDecimal)}
	c4 := &column{id: 4, tp: types.NewFieldType(mysql.TypeTimestamp)}
	cols := []*column{c1, c2, c3, c4}
	row := make([]types.Datum, len(cols))
	row[0] = types.NewIntDatum(100)
	row[1] = types.NewBytesDatum([]byte("abc"))
	row[2] = types.NewDecimalDatum(types.NewDecFromInt(1))
	row[3] = types.NewTimeDatum(types.Time{Time: types.FromGoTime(time.Now()), Fsp: 6, Type: mysql.TypeTimestamp})

	// Encode the row.
	colIDs := make([]int64, 0, 3)
	for _, col := range cols {
		colIDs = append(colIDs, col.id)
	}
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	bs, err := tablecodec.EncodeRow(sc, row, colIDs, nil, nil)
	c.Assert(err, IsNil)
	c.Assert(bs, NotNil)
	bin := base64.StdEncoding.EncodeToString(bs)

	unitTest := func(col *column) {
		url := fmt.Sprintf("http://127.0.0.1:10090/tables/%d/%v/%d/%d?rowBin=%s", col.id, col.tp.Tp, col.tp.Flag, col.tp.Flen, bin)
		resp, err := http.Get(url)
		c.Assert(err, IsNil, Commentf("url:%s", url))
		decoder := json.NewDecoder(resp.Body)
		var data interface{}
		err = decoder.Decode(&data)
		c.Assert(err, IsNil, Commentf("url:%v\ndata%v", url, data))
		colVal, err := types.DatumsToString([]types.Datum{row[col.id-1]}, false)
		c.Assert(err, IsNil)
		c.Assert(data, Equals, colVal, Commentf("url:%v", url))
	}

	for _, col := range cols {
		unitTest(col)
	}

	// Test bin has `+`.
	// 2018-03-08 16:01:00.315313
	bin = "CAIIyAEIBAIGYWJjCAYGAQCBCAgJsZ+TgISg1M8Z"
	row[3] = types.NewTimeDatum(types.Time{Time: types.FromGoTime(time.Date(2018, 3, 8, 16, 1, 0, 315313000, time.UTC)), Fsp: 6, Type: mysql.TypeTimestamp})
	unitTest(cols[3])

	// Test bin has `/`.
	// 2018-03-08 02:44:46.409199
	bin = "CAIIyAEIBAIGYWJjCAYGAQCBCAgJ7/yY8LKF1M8Z"
	row[3] = types.NewTimeDatum(types.Time{Time: types.FromGoTime(time.Date(2018, 3, 8, 2, 44, 46, 409199000, time.UTC)), Fsp: 6, Type: mysql.TypeTimestamp})
	unitTest(cols[3])
}

func (ts *HTTPHandlerTestSuite) TestGetIndexMVCC(c *C) {
	ts.startServer(c)
	ts.prepareData(c)
	defer ts.stopServer(c)

	c.Skip("MVCCLevelDB doesn't implement MVCCDebugger interface.")
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

	resp, err = http.Get("http://127.0.0.1:10090/mvcc/index/tidb/pt(p2)/idx/666?a=666&b=def")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	decodeKeyMvcc(resp.Body, c, true)
}

func (ts *HTTPHandlerTestSuite) TestGetSettings(c *C) {
	ts.startServer(c)
	ts.prepareData(c)
	defer ts.stopServer(c)
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:10090/settings"))
	c.Assert(err, IsNil)
	decoder := json.NewDecoder(resp.Body)
	var settings *config.Config
	err = decoder.Decode(&settings)
	c.Assert(err, IsNil)
	c.Assert(settings, DeepEquals, config.GetGlobalConfig())
}

func (ts *HTTPHandlerTestSuite) TestGetSchema(c *C) {
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
	c.Assert(len(lt), Greater, 0)

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

	resp, err = http.Get(fmt.Sprintf("http://127.0.0.1:10090/db-table/5"))
	c.Assert(err, IsNil)
	var dbtbl *dbTableInfo
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&dbtbl)
	c.Assert(err, IsNil)
	c.Assert(dbtbl.TableInfo.Name.L, Equals, "user")
	c.Assert(dbtbl.DBInfo.Name.L, Equals, "mysql")
	se, err := session.CreateSession(ts.store.(kv.Storage))
	c.Assert(err, IsNil)
	c.Assert(dbtbl.SchemaVersion, Equals, domain.GetDomain(se.(sessionctx.Context)).InfoSchema().SchemaMetaVersion())

	db, err := sql.Open("mysql", getDSN())
	c.Assert(err, IsNil, Commentf("Error connecting"))
	defer db.Close()
	dbt := &DBTest{c, db}

	dbt.mustExec("create database if not exists test;")
	dbt.mustExec("use test;")
	dbt.mustExec(` create table t1 (id int KEY)
		partition by range (id) (
		PARTITION p0 VALUES LESS THAN (3),
		PARTITION p1 VALUES LESS THAN (5),
		PARTITION p2 VALUES LESS THAN (7),
		PARTITION p3 VALUES LESS THAN (9))`)

	resp, err = http.Get(fmt.Sprintf("http://127.0.0.1:10090/schema/test/t1"))
	c.Assert(err, IsNil)
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&t)
	c.Assert(err, IsNil)
	c.Assert(t.Name.L, Equals, "t1")

	resp, err = http.Get(fmt.Sprintf(fmt.Sprintf("http://127.0.0.1:10090/db-table/%v", t.GetPartitionInfo().Definitions[0].ID)))
	c.Assert(err, IsNil)
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&dbtbl)
	c.Assert(err, IsNil)
	c.Assert(dbtbl.TableInfo.Name.L, Equals, "t1")
	c.Assert(dbtbl.DBInfo.Name.L, Equals, "test")
	c.Assert(dbtbl.TableInfo, DeepEquals, t)
}

func (ts *HTTPHandlerTestSuite) TestAllHistory(c *C) {
	ts.startServer(c)
	ts.prepareData(c)
	defer ts.stopServer(c)
	_, err := http.Get(fmt.Sprintf("http://127.0.0.1:10090/ddl/history/?limit=3"))
	c.Assert(err, IsNil)
	_, err = http.Get(fmt.Sprintf("http://127.0.0.1:10090/ddl/history/?limit=-1"))
	c.Assert(err, IsNil)

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:10090/ddl/history"))
	c.Assert(err, IsNil)
	decoder := json.NewDecoder(resp.Body)

	var jobs []*model.Job
	s, _ := session.CreateSession(ts.server.newTikvHandlerTool().Store.(kv.Storage))
	defer s.Close()
	store := domain.GetDomain(s.(sessionctx.Context)).Store()
	txn, _ := store.Begin()
	txnMeta := meta.NewMeta(txn)
	txnMeta.GetAllHistoryDDLJobs()
	data, _ := txnMeta.GetAllHistoryDDLJobs()
	err = decoder.Decode(&jobs)

	c.Assert(err, IsNil)
	c.Assert(jobs, DeepEquals, data)
}

func (ts *HTTPHandlerTestSuite) TestPostSettings(c *C) {
	ts.startServer(c)
	ts.prepareData(c)
	defer ts.stopServer(c)
	form := make(url.Values)
	form.Set("log_level", "error")
	form.Set("tidb_general_log", "1")
	resp, err := http.PostForm("http://127.0.0.1:10090/settings", form)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(log.GetLevel(), Equals, log.ErrorLevel)
	c.Assert(zaplog.GetLevel(), Equals, zap.ErrorLevel)
	c.Assert(config.GetGlobalConfig().Log.Level, Equals, "error")
	c.Assert(atomic.LoadUint32(&variable.ProcessGeneralLog), Equals, uint32(1))
	form = make(url.Values)
	form.Set("log_level", "info")
	form.Set("tidb_general_log", "0")
	resp, err = http.PostForm("http://127.0.0.1:10090/settings", form)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(atomic.LoadUint32(&variable.ProcessGeneralLog), Equals, uint32(0))
	c.Assert(log.GetLevel(), Equals, log.InfoLevel)
	c.Assert(zaplog.GetLevel(), Equals, zap.InfoLevel)
	c.Assert(config.GetGlobalConfig().Log.Level, Equals, "info")

	// test ddl_slow_threshold
	form = make(url.Values)
	form.Set("ddl_slow_threshold", "200")
	resp, err = http.PostForm("http://127.0.0.1:10090/settings", form)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(atomic.LoadUint32(&variable.DDLSlowOprThreshold), Equals, uint32(200))

	// test check_mb4_value_in_utf8
	db, err := sql.Open("mysql", getDSN())
	c.Assert(err, IsNil, Commentf("Error connecting"))
	defer db.Close()
	dbt := &DBTest{c, db}

	dbt.mustExec("create database tidb_test;")
	dbt.mustExec("use tidb_test;")
	dbt.mustExec("drop table if exists t2;")
	dbt.mustExec("create table t2(a varchar(100) charset utf8);")
	form.Set("check_mb4_value_in_utf8", "1")
	resp, err = http.PostForm("http://127.0.0.1:10090/settings", form)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(config.GetGlobalConfig().CheckMb4ValueInUTF8, Equals, true)
	txn1, err := dbt.db.Begin()
	c.Assert(err, IsNil)
	_, err = txn1.Exec("insert t2 values (unhex('F0A48BAE'));")
	c.Assert(err, NotNil)
	txn1.Commit()

	// Disable CheckMb4ValueInUTF8.
	form = make(url.Values)
	form.Set("check_mb4_value_in_utf8", "0")
	resp, err = http.PostForm("http://127.0.0.1:10090/settings", form)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(config.GetGlobalConfig().CheckMb4ValueInUTF8, Equals, false)
	dbt.mustExec("insert t2 values (unhex('f09f8c80'));")
}

func (ts *HTTPHandlerTestSuite) TestPprof(c *C) {
	ts.startServer(c)
	defer ts.stopServer(c)
	retryTime := 100
	for retry := 0; retry < retryTime; retry++ {
		resp, err := http.Get("http://127.0.0.1:10090/debug/pprof/heap")
		if err == nil {
			ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			return
		}
		time.Sleep(time.Millisecond * 10)
	}
	zaplog.Fatal("failed to get profile for %d retries in every 10 ms", zap.Int("retryTime", retryTime))
}

func (ts *HTTPHandlerTestSuite) TestServerInfo(c *C) {
	ts.startServer(c)
	defer ts.stopServer(c)
	resp, err := http.Get("http://127.0.0.1:10090/info")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	decoder := json.NewDecoder(resp.Body)

	info := serverInfo{}
	err = decoder.Decode(&info)
	c.Assert(err, IsNil)

	cfg := config.GetGlobalConfig()
	c.Assert(info.IsOwner, IsTrue)
	c.Assert(info.IP, Equals, cfg.AdvertiseAddress)
	c.Assert(info.StatusPort, Equals, cfg.Status.StatusPort)
	c.Assert(info.Lease, Equals, cfg.Lease)
	c.Assert(info.Version, Equals, mysql.ServerVersion)
	c.Assert(info.GitHash, Equals, printer.TiDBGitHash)

	store := ts.server.newTikvHandlerTool().Store.(kv.Storage)
	do, err := session.GetDomain(store.(kv.Storage))
	c.Assert(err, IsNil)
	ddl := do.DDL()
	c.Assert(info.ID, Equals, ddl.GetID())
}

func (ts *HTTPHandlerTestSuite) TestAllServerInfo(c *C) {
	ts.startServer(c)
	defer ts.stopServer(c)
	resp, err := http.Get("http://127.0.0.1:10090/info/all")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	decoder := json.NewDecoder(resp.Body)

	clusterInfo := clusterServerInfo{}
	err = decoder.Decode(&clusterInfo)
	c.Assert(err, IsNil)

	c.Assert(clusterInfo.IsAllServerVersionConsistent, IsTrue)
	c.Assert(clusterInfo.ServersNum, Equals, 1)

	store := ts.server.newTikvHandlerTool().Store.(kv.Storage)
	do, err := session.GetDomain(store.(kv.Storage))
	c.Assert(err, IsNil)
	ddl := do.DDL()
	c.Assert(clusterInfo.OwnerID, Equals, ddl.GetID())
	serverInfo, ok := clusterInfo.AllServersInfo[ddl.GetID()]
	c.Assert(ok, Equals, true)

	cfg := config.GetGlobalConfig()
	c.Assert(serverInfo.IP, Equals, cfg.AdvertiseAddress)
	c.Assert(serverInfo.StatusPort, Equals, cfg.Status.StatusPort)
	c.Assert(serverInfo.Lease, Equals, cfg.Lease)
	c.Assert(serverInfo.Version, Equals, mysql.ServerVersion)
	c.Assert(serverInfo.GitHash, Equals, printer.TiDBGitHash)
	c.Assert(serverInfo.ID, Equals, ddl.GetID())
}

func (ts *HTTPHandlerTestSuite) TestCheckCN(c *C) {
	s := &Server{cfg: &config.Config{Security: config.Security{ClusterVerifyCN: []string{"a ", "b", "c"}}}}
	tlsConfig := &tls.Config{}
	s.setCNChecker(tlsConfig)
	c.Assert(tlsConfig.VerifyPeerCertificate, NotNil)
	err := tlsConfig.VerifyPeerCertificate(nil, [][]*x509.Certificate{{{Subject: pkix.Name{CommonName: "a"}}}})
	c.Assert(err, IsNil)
	err = tlsConfig.VerifyPeerCertificate(nil, [][]*x509.Certificate{{{Subject: pkix.Name{CommonName: "b"}}}})
	c.Assert(err, IsNil)
	err = tlsConfig.VerifyPeerCertificate(nil, [][]*x509.Certificate{{{Subject: pkix.Name{CommonName: "d"}}}})
	c.Assert(err, NotNil)
}
