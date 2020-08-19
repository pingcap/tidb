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
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	zaplog "github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
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
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/pingcap/tidb/util/versioninfo"
	log "github.com/sirupsen/logrus"
	"go.uber.org/zap"
)

type basicHTTPHandlerTestSuite struct {
	*testServerClient
	server  *Server
	store   kv.Storage
	domain  *domain.Domain
	tidbdrv *TiDBDriver
}

type HTTPHandlerTestSuite struct {
	*basicHTTPHandlerTestSuite
}

type HTTPHandlerTestSerialSuite struct {
	*basicHTTPHandlerTestSuite
}

var _ = Suite(&HTTPHandlerTestSuite{&basicHTTPHandlerTestSuite{}})

var _ = SerialSuites(&HTTPHandlerTestSerialSuite{&basicHTTPHandlerTestSuite{}})

func (ts *basicHTTPHandlerTestSuite) SetUpSuite(c *C) {
	ts.testServerClient = newTestServerClient()
}

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
	endKey := tablecodec.EncodeRecordKey(recordPrefix, kv.IntHandle(recordID))

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
			f = r.GetRecordFrame(t.tableID, "", "", false)
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

func (ts *HTTPHandlerTestSuite) TestRegionCommonHandleRange(c *C) {
	sTableID := int64(3)
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

	startKey := tablecodec.EncodeCommonHandleSeekKey(sTableID, encodedValue)

	region := &tikv.KeyLocation{
		Region:   tikv.RegionVerID{},
		StartKey: startKey,
	}
	r, err := helper.NewRegionFrameRange(region)
	c.Assert(err, IsNil)
	c.Assert(r.First.IsRecord, IsTrue)
	c.Assert(r.First.RecordID, Equals, int64(0))
	c.Assert(r.First.IndexValues, DeepEquals, expectIndexValues)
	c.Assert(r.First.IndexName, Equals, "ClusterHandle")
	c.Assert(r.Last.RecordID, Equals, int64(0))
	c.Assert(r.Last.IndexValues, IsNil)
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
	c.Assert(r.GetRecordFrame(300, "", "", false), NotNil)
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
	c.Assert(r.GetRecordFrame(3, "", "", false), NotNil)
	c.Assert(r.GetIndexFrame(8, 1, "", "", ""), NotNil)
}

func (ts *HTTPHandlerTestSuite) TestRegionsAPI(c *C) {
	ts.startServer(c)
	defer ts.stopServer(c)
	resp, err := ts.fetchStatus("/tables/information_schema/SCHEMATA/regions")
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
		c.Assert(ts.regionContainsTable(c, region.ID, data.TableID), IsTrue)
	}
}

func (ts *HTTPHandlerTestSuite) TestRegionsAPIForClusterIndex(c *C) {
	ts.startServer(c)
	defer ts.stopServer(c)
	ts.prepareData(c)
	resp, err := ts.fetchStatus("/tables/tidb/t/regions")
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
		resp, err := ts.fetchStatus(fmt.Sprintf("/regions/%d", region.ID))
		c.Assert(err, IsNil)
		c.Assert(resp.StatusCode, Equals, http.StatusOK)
		decoder := json.NewDecoder(resp.Body)
		var data RegionDetail
		err = decoder.Decode(&data)
		c.Assert(err, IsNil)
		frameCnt := 0
		for _, f := range data.Frames {
			if f.DBName == "tidb" && f.TableName == "t" {
				frameCnt++
			}
		}
		c.Assert(frameCnt, Equals, 2)
		c.Assert(resp.Body.Close(), IsNil)
	}
}

func (ts *HTTPHandlerTestSuite) regionContainsTable(c *C, regionID uint64, tableID int64) bool {
	resp, err := ts.fetchStatus(fmt.Sprintf("/regions/%d", regionID))
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
	resp, err := ts.fetchStatus("/tables/fdsfds/aaa/regions")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)

	resp, err = ts.fetchStatus("/tables/tidb/pt/regions")
	c.Assert(err, IsNil)
	defer resp.Body.Close()

	var data []*TableRegions
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&data)
	c.Assert(err, IsNil)

	region := data[1]
	_, err = ts.fetchStatus(fmt.Sprintf("/regions/%d", region.TableID))
	c.Assert(err, IsNil)
}

func (ts *HTTPHandlerTestSuite) TestGetRegionByIDWithError(c *C) {
	ts.startServer(c)
	defer ts.stopServer(c)
	resp, err := ts.fetchStatus("/regions/xxx")
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)
	defer resp.Body.Close()
}

func (ts *HTTPHandlerTestSuite) TestBinlogRecover(c *C) {
	ts.startServer(c)
	defer ts.stopServer(c)
	binloginfo.EnableSkipBinlogFlag()
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, true)
	resp, err := ts.fetchStatus("/binlog/recover")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, false)

	// Invalid operation will use the default operation.
	binloginfo.EnableSkipBinlogFlag()
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, true)
	resp, err = ts.fetchStatus("/binlog/recover?op=abc")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, false)

	binloginfo.EnableSkipBinlogFlag()
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, true)
	resp, err = ts.fetchStatus("/binlog/recover?op=abc&seconds=1")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, false)

	binloginfo.EnableSkipBinlogFlag()
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, true)
	binloginfo.AddOneSkippedCommitter()
	resp, err = ts.fetchStatus("/binlog/recover?op=abc&seconds=1")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, false)
	binloginfo.RemoveOneSkippedCommitter()

	binloginfo.AddOneSkippedCommitter()
	c.Assert(binloginfo.SkippedCommitterCount(), Equals, int32(1))
	resp, err = ts.fetchStatus("/binlog/recover?op=reset")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(binloginfo.SkippedCommitterCount(), Equals, int32(0))

	binloginfo.EnableSkipBinlogFlag()
	resp, err = ts.fetchStatus("/binlog/recover?op=nowait")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, false)

	// Only the first should work.
	binloginfo.EnableSkipBinlogFlag()
	resp, err = ts.fetchStatus("/binlog/recover?op=nowait&op=reset")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, false)

	resp, err = ts.fetchStatus("/binlog/recover?op=status")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
}

func (ts *HTTPHandlerTestSuite) TestRegionsFromMeta(c *C) {
	ts.startServer(c)
	defer ts.stopServer(c)
	resp, err := ts.fetchStatus("/regions/meta")
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
	resp1, err := ts.fetchStatus("/regions/meta")
	c.Assert(err, IsNil)
	defer resp1.Body.Close()
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/server/errGetRegionByIDEmpty"), IsNil)
}

func (ts *basicHTTPHandlerTestSuite) startServer(c *C) {
	var err error
	ts.store, err = mockstore.NewMockStore()
	c.Assert(err, IsNil)
	ts.domain, err = session.BootstrapSession(ts.store)
	c.Assert(err, IsNil)
	ts.tidbdrv = NewTiDBDriver(ts.store)

	cfg := newTestConfig()
	cfg.Port = ts.port
	cfg.Store = "tikv"
	cfg.Status.StatusPort = ts.statusPort
	cfg.Status.ReportStatus = true

	server, err := NewServer(cfg, ts.tidbdrv)
	c.Assert(err, IsNil)
	ts.server = server
	go server.Run()
	ts.waitUntilServerOnline()
}

func (ts *basicHTTPHandlerTestSuite) stopServer(c *C) {
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

func (ts *basicHTTPHandlerTestSuite) prepareData(c *C) {
	db, err := sql.Open("mysql", ts.getDSN())
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

	dbt.mustExec("set @@tidb_enable_clustered_index = 1")
	dbt.mustExec("drop table if exists t")
	dbt.mustExec("create table t (a double, b varchar(20), c int, primary key(a,b))")
	dbt.mustExec("insert into t values(1.1,'111',1),(2.2,'222',2)")
}

func decodeKeyMvcc(closer io.ReadCloser, c *C, valid bool) {
	decoder := json.NewDecoder(closer)
	var data mvccKV
	err := decoder.Decode(&data)
	c.Assert(err, IsNil)
	if valid {
		c.Assert(data.Value.Info, NotNil)
		c.Assert(len(data.Value.Info.Writes), Greater, 0)
	} else {
		c.Assert(data.Value.Info.Lock, IsNil)
		c.Assert(data.Value.Info.Writes, IsNil)
		c.Assert(data.Value.Info.Values, IsNil)
	}
}

func (ts *HTTPHandlerTestSuite) TestGetTableMVCC(c *C) {
	ts.startServer(c)
	ts.prepareData(c)
	defer ts.stopServer(c)

	resp, err := ts.fetchStatus(fmt.Sprintf("/mvcc/key/tidb/test/1"))
	c.Assert(err, IsNil)
	decoder := json.NewDecoder(resp.Body)
	var data mvccKV
	err = decoder.Decode(&data)
	c.Assert(err, IsNil)
	c.Assert(data.Value, NotNil)
	info := data.Value.Info
	c.Assert(info, NotNil)
	c.Assert(len(info.Writes), Greater, 0)

	// TODO: Unistore will not return Op_Lock.
	// Use this workaround to support two backend, we can remove this hack after deprecated mocktikv.
	var startTs uint64
	for _, w := range info.Writes {
		if w.Type == kvrpcpb.Op_Lock {
			continue
		}
		startTs = w.StartTs
		break
	}

	resp, err = ts.fetchStatus(fmt.Sprintf("/mvcc/txn/%d/tidb/test", startTs))
	c.Assert(err, IsNil)
	var p2 mvccKV
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&p2)
	c.Assert(err, IsNil)

	for i, expect := range info.Values {
		v2 := p2.Value.Info.Values[i].Value
		c.Assert(v2, BytesEquals, expect.Value)
	}

	hexKey := p2.Key
	resp, err = ts.fetchStatus("/mvcc/hex/" + hexKey)
	c.Assert(err, IsNil)
	decoder = json.NewDecoder(resp.Body)
	var data2 mvccKV
	err = decoder.Decode(&data2)
	c.Assert(err, IsNil)
	c.Assert(data2, DeepEquals, data)

	resp, err = ts.fetchStatus(fmt.Sprintf("/mvcc/key/tidb/test/1?decode=true"))
	c.Assert(err, IsNil)
	decoder = json.NewDecoder(resp.Body)
	var data3 map[string]interface{}
	err = decoder.Decode(&data3)
	c.Assert(err, IsNil)
	c.Assert(data3["key"], NotNil)
	c.Assert(data3["info"], NotNil)
	c.Assert(data3["data"], NotNil)
	c.Assert(data3["decode_error"], IsNil)

	resp, err = ts.fetchStatus("/mvcc/key/tidb/pt(p0)/42?decode=true")
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
	resp, err := ts.fetchStatus(fmt.Sprintf("/mvcc/key/tidb/test/1234"))
	c.Assert(err, IsNil)
	decoder := json.NewDecoder(resp.Body)
	var data mvccKV
	err = decoder.Decode(&data)
	c.Assert(err, IsNil)
	c.Assert(data.Value.Info.Lock, IsNil)
	c.Assert(data.Value.Info.Writes, IsNil)
	c.Assert(data.Value.Info.Values, IsNil)
}

func (ts *HTTPHandlerTestSuite) TestTiFlashReplica(c *C) {
	ts.startServer(c)
	ts.prepareData(c)
	defer ts.stopServer(c)

	db, err := sql.Open("mysql", ts.getDSN())
	c.Assert(err, IsNil, Commentf("Error connecting"))
	defer db.Close()
	dbt := &DBTest{c, db}

	defer func(originGC bool) {
		if originGC {
			ddl.EmulatorGCEnable()
		} else {
			ddl.EmulatorGCDisable()
		}
	}(ddl.IsEmulatorGCEnable())

	// Disable emulator GC.
	// Otherwise emulator GC will delete table record as soon as possible after execute drop table DDL.
	ddl.EmulatorGCDisable()
	gcTimeFormat := "20060102-15:04:05 -0700 MST"
	timeBeforeDrop := time.Now().Add(0 - 48*60*60*time.Second).Format(gcTimeFormat)
	safePointSQL := `INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('tikv_gc_safe_point', '%[1]s', ''),('tikv_gc_enable','true','')
			       ON DUPLICATE KEY
			       UPDATE variable_value = '%[1]s'`
	// Set GC safe point and enable GC.
	dbt.mustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))

	resp, err := ts.fetchStatus("/tiflash/replica")
	c.Assert(err, IsNil)
	decoder := json.NewDecoder(resp.Body)
	var data []tableFlashReplicaInfo
	err = decoder.Decode(&data)
	c.Assert(err, IsNil)
	c.Assert(len(data), Equals, 0)

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount", `return(true)`), IsNil)
	defer failpoint.Disable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount")
	dbt.mustExec("use tidb")
	dbt.mustExec("alter table test set tiflash replica 2 location labels 'a','b';")

	resp, err = ts.fetchStatus("/tiflash/replica")
	c.Assert(err, IsNil)
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&data)
	c.Assert(err, IsNil)
	c.Assert(len(data), Equals, 1)
	c.Assert(data[0].ReplicaCount, Equals, uint64(2))
	c.Assert(strings.Join(data[0].LocationLabels, ","), Equals, "a,b")
	c.Assert(data[0].Available, Equals, false)

	resp, err = ts.postStatus("/tiflash/replica", "application/json", bytes.NewBuffer([]byte(`{"id":84,"region_count":3,"flash_region_count":3}`)))
	c.Assert(err, IsNil)
	c.Assert(resp, NotNil)
	body, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	c.Assert(string(body), Equals, "[schema:1146]Table which ID = 84 does not exist.")

	t, err := ts.domain.InfoSchema().TableByName(model.NewCIStr("tidb"), model.NewCIStr("test"))
	c.Assert(err, IsNil)
	req := fmt.Sprintf(`{"id":%d,"region_count":3,"flash_region_count":3}`, t.Meta().ID)
	resp, err = ts.postStatus("/tiflash/replica", "application/json", bytes.NewBuffer([]byte(req)))
	c.Assert(err, IsNil)
	c.Assert(resp, NotNil)
	body, err = ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	c.Assert(string(body), Equals, "")

	resp, err = ts.fetchStatus("/tiflash/replica")
	c.Assert(err, IsNil)
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&data)
	c.Assert(err, IsNil)
	resp.Body.Close()
	c.Assert(len(data), Equals, 1)
	c.Assert(data[0].ReplicaCount, Equals, uint64(2))
	c.Assert(strings.Join(data[0].LocationLabels, ","), Equals, "a,b")
	c.Assert(data[0].Available, Equals, true) // The status should be true now.

	// Should not take effect.
	dbt.mustExec("alter table test set tiflash replica 2 location labels 'a','b';")
	checkFunc := func() {
		resp, err = ts.fetchStatus("/tiflash/replica")
		c.Assert(err, IsNil)
		decoder = json.NewDecoder(resp.Body)
		err = decoder.Decode(&data)
		c.Assert(err, IsNil)
		resp.Body.Close()
		c.Assert(len(data), Equals, 1)
		c.Assert(data[0].ReplicaCount, Equals, uint64(2))
		c.Assert(strings.Join(data[0].LocationLabels, ","), Equals, "a,b")
		c.Assert(data[0].Available, Equals, true) // The status should be true now.
	}

	// Test for get dropped table tiflash replica info.
	dbt.mustExec("drop table test")
	checkFunc()

	// Test unique table id replica info.
	dbt.mustExec("flashback table test")
	checkFunc()
	dbt.mustExec("drop table test")
	checkFunc()
	dbt.mustExec("flashback table test")
	checkFunc()

	// Test for partition table.
	dbt.mustExec("alter table pt set tiflash replica 2 location labels 'a','b';")
	dbt.mustExec("alter table test set tiflash replica 0;")
	resp, err = ts.fetchStatus("/tiflash/replica")
	c.Assert(err, IsNil)
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&data)
	c.Assert(err, IsNil)
	resp.Body.Close()
	c.Assert(len(data), Equals, 3)
	c.Assert(data[0].ReplicaCount, Equals, uint64(2))
	c.Assert(strings.Join(data[0].LocationLabels, ","), Equals, "a,b")
	c.Assert(data[0].Available, Equals, false)

	pid0 := data[0].ID
	pid1 := data[1].ID
	pid2 := data[2].ID

	// Mock for partition 1 replica was available.
	req = fmt.Sprintf(`{"id":%d,"region_count":3,"flash_region_count":3}`, pid1)
	resp, err = ts.postStatus("/tiflash/replica", "application/json", bytes.NewBuffer([]byte(req)))
	c.Assert(err, IsNil)
	resp.Body.Close()
	resp, err = ts.fetchStatus("/tiflash/replica")
	c.Assert(err, IsNil)
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&data)
	c.Assert(err, IsNil)
	resp.Body.Close()
	c.Assert(len(data), Equals, 3)
	c.Assert(data[0].Available, Equals, false)
	c.Assert(data[1].Available, Equals, true)
	c.Assert(data[2].Available, Equals, false)

	// Mock for partition 0,2 replica was available.
	req = fmt.Sprintf(`{"id":%d,"region_count":3,"flash_region_count":3}`, pid0)
	resp, err = ts.postStatus("/tiflash/replica", "application/json", bytes.NewBuffer([]byte(req)))
	c.Assert(err, IsNil)
	resp.Body.Close()
	req = fmt.Sprintf(`{"id":%d,"region_count":3,"flash_region_count":3}`, pid2)
	resp, err = ts.postStatus("/tiflash/replica", "application/json", bytes.NewBuffer([]byte(req)))
	c.Assert(err, IsNil)
	resp.Body.Close()
	checkFunc = func() {
		resp, err = ts.fetchStatus("/tiflash/replica")
		c.Assert(err, IsNil)
		decoder = json.NewDecoder(resp.Body)
		err = decoder.Decode(&data)
		c.Assert(err, IsNil)
		resp.Body.Close()
		c.Assert(len(data), Equals, 3)
		c.Assert(data[0].Available, Equals, true)
		c.Assert(data[1].Available, Equals, true)
		c.Assert(data[2].Available, Equals, true)
	}

	// Test for get truncated table tiflash replica info.
	dbt.mustExec("truncate table pt")
	dbt.mustExec("alter table pt set tiflash replica 0;")
	checkFunc()
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
	row[3] = types.NewTimeDatum(types.NewTime(types.FromGoTime(time.Now()), mysql.TypeTimestamp, 6))

	// Encode the row.
	colIDs := make([]int64, 0, 3)
	for _, col := range cols {
		colIDs = append(colIDs, col.id)
	}
	rd := rowcodec.Encoder{Enable: true}
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	bs, err := tablecodec.EncodeRow(sc, row, colIDs, nil, nil, &rd)
	c.Assert(err, IsNil)
	c.Assert(bs, NotNil)
	bin := base64.StdEncoding.EncodeToString(bs)

	unitTest := func(col *column) {
		path := fmt.Sprintf("/tables/%d/%v/%d/%d?rowBin=%s", col.id, col.tp.Tp, col.tp.Flag, col.tp.Flen, bin)
		resp, err := ts.fetchStatus(path)
		c.Assert(err, IsNil, Commentf("url:%s", ts.statusURL(path)))
		decoder := json.NewDecoder(resp.Body)
		var data interface{}
		err = decoder.Decode(&data)
		c.Assert(err, IsNil, Commentf("url:%v\ndata%v", ts.statusURL(path), data))
		colVal, err := types.DatumsToString([]types.Datum{row[col.id-1]}, false)
		c.Assert(err, IsNil)
		c.Assert(data, Equals, colVal, Commentf("url:%v", ts.statusURL(path)))
	}

	for _, col := range cols {
		unitTest(col)
	}

	// Test bin has `+`.
	// 2018-03-08 16:01:00.315313
	bin = "CAIIyAEIBAIGYWJjCAYGAQCBCAgJsZ+TgISg1M8Z"
	row[3] = types.NewTimeDatum(types.NewTime(types.FromGoTime(time.Date(2018, 3, 8, 16, 1, 0, 315313000, time.UTC)), mysql.TypeTimestamp, 6))
	unitTest(cols[3])

	// Test bin has `/`.
	// 2018-03-08 02:44:46.409199
	bin = "CAIIyAEIBAIGYWJjCAYGAQCBCAgJ7/yY8LKF1M8Z"
	row[3] = types.NewTimeDatum(types.NewTime(types.FromGoTime(time.Date(2018, 3, 8, 2, 44, 46, 409199000, time.UTC)), mysql.TypeTimestamp, 6))
	unitTest(cols[3])
}

func (ts *HTTPHandlerTestSuite) TestGetIndexMVCC(c *C) {
	ts.startServer(c)
	ts.prepareData(c)
	defer ts.stopServer(c)

	// tests for normal index key
	resp, err := ts.fetchStatus("/mvcc/index/tidb/test/idx1/1?a=1&b=2")
	c.Assert(err, IsNil)
	decodeKeyMvcc(resp.Body, c, true)

	resp, err = ts.fetchStatus("/mvcc/index/tidb/test/idx2/1?a=1&b=2")
	c.Assert(err, IsNil)
	decodeKeyMvcc(resp.Body, c, true)

	// tests for index key which includes null
	resp, err = ts.fetchStatus("/mvcc/index/tidb/test/idx1/3?a=3&b")
	c.Assert(err, IsNil)
	decodeKeyMvcc(resp.Body, c, true)

	resp, err = ts.fetchStatus("/mvcc/index/tidb/test/idx2/3?a=3&b")
	c.Assert(err, IsNil)
	decodeKeyMvcc(resp.Body, c, true)

	// tests for index key which includes empty string
	resp, err = ts.fetchStatus("/mvcc/index/tidb/test/idx1/4?a=4&b=")
	c.Assert(err, IsNil)
	decodeKeyMvcc(resp.Body, c, true)

	resp, err = ts.fetchStatus("/mvcc/index/tidb/test/idx2/3?a=4&b=")
	c.Assert(err, IsNil)
	decodeKeyMvcc(resp.Body, c, true)

	// tests for wrong key
	resp, err = ts.fetchStatus("/mvcc/index/tidb/test/idx1/5?a=5&b=1")
	c.Assert(err, IsNil)
	decodeKeyMvcc(resp.Body, c, false)

	resp, err = ts.fetchStatus("/mvcc/index/tidb/test/idx2/5?a=5&b=1")
	c.Assert(err, IsNil)
	decodeKeyMvcc(resp.Body, c, false)

	// tests for missing column value
	resp, err = ts.fetchStatus("/mvcc/index/tidb/test/idx1/1?a=1")
	c.Assert(err, IsNil)
	decoder := json.NewDecoder(resp.Body)
	var data1 mvccKV
	err = decoder.Decode(&data1)
	c.Assert(err, NotNil)

	resp, err = ts.fetchStatus("/mvcc/index/tidb/test/idx2/1?a=1")
	c.Assert(err, IsNil)
	decoder = json.NewDecoder(resp.Body)
	var data2 mvccKV
	err = decoder.Decode(&data2)
	c.Assert(err, NotNil)

	resp, err = ts.fetchStatus("/mvcc/index/tidb/pt(p2)/idx/666?a=666&b=def")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	decodeKeyMvcc(resp.Body, c, true)
}

func (ts *HTTPHandlerTestSuite) TestGetSettings(c *C) {
	ts.startServer(c)
	ts.prepareData(c)
	defer ts.stopServer(c)
	resp, err := ts.fetchStatus("/settings")
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
	resp, err := ts.fetchStatus("/schema")
	c.Assert(err, IsNil)
	decoder := json.NewDecoder(resp.Body)
	var dbs []*model.DBInfo
	err = decoder.Decode(&dbs)
	c.Assert(err, IsNil)
	expects := []string{"information_schema", "metrics_schema", "mysql", "performance_schema", "test", "tidb"}
	names := make([]string, len(dbs))
	for i, v := range dbs {
		names[i] = v.Name.L
	}
	sort.Strings(names)
	c.Assert(names, DeepEquals, expects)

	resp, err = ts.fetchStatus("/schema?table_id=5")
	c.Assert(err, IsNil)
	var t *model.TableInfo
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&t)
	c.Assert(err, IsNil)
	c.Assert(t.Name.L, Equals, "user")

	_, err = ts.fetchStatus("/schema?table_id=a")
	c.Assert(err, IsNil)

	_, err = ts.fetchStatus("/schema?table_id=1")
	c.Assert(err, IsNil)

	_, err = ts.fetchStatus("/schema?table_id=-1")
	c.Assert(err, IsNil)

	resp, err = ts.fetchStatus("/schema/tidb")
	c.Assert(err, IsNil)
	var lt []*model.TableInfo
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&lt)
	c.Assert(err, IsNil)
	c.Assert(len(lt), Greater, 0)

	_, err = ts.fetchStatus("/schema/abc")
	c.Assert(err, IsNil)

	resp, err = ts.fetchStatus("/schema/tidb/test")
	c.Assert(err, IsNil)
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&t)
	c.Assert(err, IsNil)
	c.Assert(t.Name.L, Equals, "test")

	_, err = ts.fetchStatus("/schema/tidb/abc")
	c.Assert(err, IsNil)

	resp, err = ts.fetchStatus("/db-table/5")
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

	db, err := sql.Open("mysql", ts.getDSN())
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

	resp, err = ts.fetchStatus("/schema/test/t1")
	c.Assert(err, IsNil)
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&t)
	c.Assert(err, IsNil)
	c.Assert(t.Name.L, Equals, "t1")

	resp, err = ts.fetchStatus(fmt.Sprintf("/db-table/%v", t.GetPartitionInfo().Definitions[0].ID))
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
	_, err := ts.fetchStatus("/ddl/history/?limit=3")
	c.Assert(err, IsNil)
	_, err = ts.fetchStatus("/ddl/history/?limit=-1")
	c.Assert(err, IsNil)

	resp, err := ts.fetchStatus("/ddl/history")
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
	resp, err := ts.formStatus("/settings", form)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(log.GetLevel(), Equals, log.ErrorLevel)
	c.Assert(zaplog.GetLevel(), Equals, zap.ErrorLevel)
	c.Assert(config.GetGlobalConfig().Log.Level, Equals, "error")
	c.Assert(atomic.LoadUint32(&variable.ProcessGeneralLog), Equals, uint32(1))
	form = make(url.Values)
	form.Set("log_level", "fatal")
	form.Set("tidb_general_log", "0")
	resp, err = ts.formStatus("/settings", form)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(atomic.LoadUint32(&variable.ProcessGeneralLog), Equals, uint32(0))
	c.Assert(log.GetLevel(), Equals, log.FatalLevel)
	c.Assert(zaplog.GetLevel(), Equals, zap.FatalLevel)
	c.Assert(config.GetGlobalConfig().Log.Level, Equals, "fatal")
	form.Set("log_level", os.Getenv("log_level"))

	// test ddl_slow_threshold
	form = make(url.Values)
	form.Set("ddl_slow_threshold", "200")
	resp, err = ts.formStatus("/settings", form)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(atomic.LoadUint32(&variable.DDLSlowOprThreshold), Equals, uint32(200))

	// test check_mb4_value_in_utf8
	db, err := sql.Open("mysql", ts.getDSN())
	c.Assert(err, IsNil, Commentf("Error connecting"))
	defer db.Close()
	dbt := &DBTest{c, db}

	dbt.mustExec("create database tidb_test;")
	dbt.mustExec("use tidb_test;")
	dbt.mustExec("drop table if exists t2;")
	dbt.mustExec("create table t2(a varchar(100) charset utf8);")
	form.Set("check_mb4_value_in_utf8", "1")
	resp, err = ts.formStatus("/settings", form)
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
	resp, err = ts.formStatus("/settings", form)
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
		resp, err := ts.fetchStatus("/debug/pprof/heap")
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
	resp, err := ts.fetchStatus("/info")
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
	c.Assert(info.GitHash, Equals, versioninfo.TiDBGitHash)

	store := ts.server.newTikvHandlerTool().Store.(kv.Storage)
	do, err := session.GetDomain(store.(kv.Storage))
	c.Assert(err, IsNil)
	ddl := do.DDL()
	c.Assert(info.ID, Equals, ddl.GetID())
}

func (ts *HTTPHandlerTestSerialSuite) TestAllServerInfo(c *C) {
	ts.startServer(c)
	defer ts.stopServer(c)
	resp, err := ts.fetchStatus("/info/all")
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
	c.Assert(serverInfo.GitHash, Equals, versioninfo.TiDBGitHash)
	c.Assert(serverInfo.ID, Equals, ddl.GetID())
}

func (ts *HTTPHandlerTestSuite) TestHotRegionInfo(c *C) {
	ts.startServer(c)
	defer ts.stopServer(c)
	resp, err := ts.fetchStatus("/regions/hot")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)
}

func (ts *HTTPHandlerTestSuite) TestDebugZip(c *C) {
	ts.startServer(c)
	defer ts.stopServer(c)
	resp, err := ts.fetchStatus("/debug/zip?seconds=1")
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	b, err := httputil.DumpResponse(resp, true)
	c.Assert(err, IsNil)
	c.Assert(len(b), Greater, 0)
	c.Assert(resp.Body.Close(), IsNil)
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

func (ts *HTTPHandlerTestSuite) TestZipInfoForSQL(c *C) {
	ts.startServer(c)
	defer ts.stopServer(c)

	db, err := sql.Open("mysql", ts.getDSN())
	c.Assert(err, IsNil, Commentf("Error connecting"))
	defer db.Close()
	dbt := &DBTest{c, db}

	dbt.mustExec("use test")
	dbt.mustExec("create table if not exists t (a int)")

	urlValues := url.Values{
		"sql":        {"select * from t"},
		"current_db": {"test"},
	}
	resp, err := ts.formStatus("/debug/sub-optimal-plan", urlValues)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	b, err := httputil.DumpResponse(resp, true)
	c.Assert(err, IsNil)
	c.Assert(len(b), Greater, 0)
	c.Assert(resp.Body.Close(), IsNil)

	resp, err = ts.formStatus("/debug/sub-optimal-plan?pprof_time=5&timeout=0", urlValues)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	b, err = httputil.DumpResponse(resp, true)
	c.Assert(err, IsNil)
	c.Assert(len(b), Greater, 0)
	c.Assert(resp.Body.Close(), IsNil)

	resp, err = ts.formStatus("/debug/sub-optimal-plan?pprof_time=5", urlValues)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	b, err = httputil.DumpResponse(resp, true)
	c.Assert(err, IsNil)
	c.Assert(len(b), Greater, 0)
	c.Assert(resp.Body.Close(), IsNil)

	resp, err = ts.formStatus("/debug/sub-optimal-plan?timeout=1", urlValues)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	b, err = httputil.DumpResponse(resp, true)
	c.Assert(err, IsNil)
	c.Assert(len(b), Greater, 0)
	c.Assert(resp.Body.Close(), IsNil)

	urlValues.Set("current_db", "non_exists_db")
	resp, err = ts.formStatus("/debug/sub-optimal-plan", urlValues)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusInternalServerError)
	b, err = ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	c.Assert(string(b), Equals, "use database non_exists_db failed, err: [schema:1049]Unknown database 'non_exists_db'\n")
	c.Assert(resp.Body.Close(), IsNil)
}

func (ts *HTTPHandlerTestSuite) TestFailpointHandler(c *C) {
	defer ts.stopServer(c)

	// start server without enabling failpoint integration
	ts.startServer(c)
	resp, err := ts.fetchStatus("/fail/")
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusNotFound)
	ts.stopServer(c)

	// enable failpoint integration and start server
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/server/enableTestAPI", "return"), IsNil)
	ts.startServer(c)
	resp, err = ts.fetchStatus("/fail/")
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	b, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(b), "github.com/pingcap/tidb/server/enableTestAPI=return"), IsTrue)
	c.Assert(resp.Body.Close(), IsNil)
}

func (ts *HTTPHandlerTestSuite) TestTestHandler(c *C) {
	defer ts.stopServer(c)

	// start server without enabling failpoint integration
	ts.startServer(c)
	resp, err := ts.fetchStatus("/test")
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusNotFound)
	ts.stopServer(c)

	// enable failpoint integration and start server
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/server/enableTestAPI", "return"), IsNil)
	ts.startServer(c)

	resp, err = ts.fetchStatus("/test/gc/gc")
	c.Assert(err, IsNil)
	resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)

	resp, err = ts.fetchStatus("/test/gc/resolvelock")
	c.Assert(err, IsNil)
	resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)

	resp, err = ts.fetchStatus("/test/gc/resolvelock?safepoint=a")
	c.Assert(err, IsNil)
	resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)

	resp, err = ts.fetchStatus("/test/gc/resolvelock?physical=1")
	c.Assert(err, IsNil)
	resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)

	resp, err = ts.fetchStatus("/test/gc/resolvelock?physical=true")
	c.Assert(err, IsNil)
	resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)

	resp, err = ts.fetchStatus("/test/gc/resolvelock?safepoint=10000&physical=true")
	c.Assert(err, IsNil)
	resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
}
