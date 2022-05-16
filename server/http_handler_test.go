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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
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
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"sort"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/external"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/deadlockhistory"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

type basicHTTPHandlerTestSuite struct {
	*testServerClient
	server  *Server
	store   kv.Storage
	domain  *domain.Domain
	tidbdrv *TiDBDriver
	sh      *StatsHandler
}

func createBasicHTTPHandlerTestSuite() *basicHTTPHandlerTestSuite {
	ts := &basicHTTPHandlerTestSuite{}
	ts.testServerClient = newTestServerClient()
	return ts
}

func TestRegionIndexRange(t *testing.T) {
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
	require.NoError(t, err)

	startKey := tablecodec.EncodeIndexSeekKey(sTableID, sIndex, encodedValue)
	recordPrefix := tablecodec.GenTableRecordPrefix(eTableID)
	endKey := tablecodec.EncodeRecordKey(recordPrefix, kv.IntHandle(recordID))

	region := &tikv.KeyLocation{
		Region:   tikv.RegionVerID{},
		StartKey: startKey,
		EndKey:   endKey,
	}
	r, err := helper.NewRegionFrameRange(region)
	require.NoError(t, err)
	require.Equal(t, sIndex, r.First.IndexID)
	require.False(t, r.First.IsRecord)
	require.Equal(t, int64(0), r.First.RecordID)
	require.Equal(t, expectIndexValues, r.First.IndexValues)
	require.Equal(t, recordID, r.Last.RecordID)
	require.Nil(t, r.Last.IndexValues)

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
	for _, c := range testCases {
		var f *helper.FrameItem
		if c.indexID == 0 {
			f = r.GetRecordFrame(c.tableID, "", "", false)
		} else {
			f = r.GetIndexFrame(c.tableID, c.indexID, "", "", "")
		}
		if c.isCover {
			require.NotNil(t, f)
		} else {
			require.Nil(t, f)
		}
	}
}

func TestRegionCommonHandleRange(t *testing.T) {
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
	require.NoError(t, err)

	startKey := tablecodec.EncodeRowKey(sTableID, encodedValue)

	region := &tikv.KeyLocation{
		Region:   tikv.RegionVerID{},
		StartKey: startKey,
		EndKey:   nil,
	}
	r, err := helper.NewRegionFrameRange(region)
	require.NoError(t, err)
	require.True(t, r.First.IsRecord)
	require.Equal(t, int64(0), r.First.RecordID)
	require.Equal(t, expectIndexValues, r.First.IndexValues)
	require.Equal(t, "PRIMARY", r.First.IndexName)
	require.Equal(t, int64(0), r.Last.RecordID)
	require.Nil(t, r.Last.IndexValues)
}

func TestRegionIndexRangeWithEndNoLimit(t *testing.T) {
	sTableID := int64(15)
	startKey := tablecodec.GenTableRecordPrefix(sTableID)
	endKey := []byte("z_aaaaafdfd")
	region := &tikv.KeyLocation{
		Region:   tikv.RegionVerID{},
		StartKey: startKey,
		EndKey:   endKey,
	}
	r, err := helper.NewRegionFrameRange(region)
	require.NoError(t, err)
	require.True(t, r.First.IsRecord)
	require.True(t, r.Last.IsRecord)
	require.NotNil(t, r.GetRecordFrame(300, "", "", false))
	require.NotNil(t, r.GetIndexFrame(200, 100, "", "", ""))
}

func TestRegionIndexRangeWithStartNoLimit(t *testing.T) {
	eTableID := int64(9)
	startKey := []byte("m_aaaaafdfd")
	endKey := tablecodec.GenTableRecordPrefix(eTableID)
	region := &tikv.KeyLocation{
		Region:   tikv.RegionVerID{},
		StartKey: startKey,
		EndKey:   endKey,
	}
	r, err := helper.NewRegionFrameRange(region)
	require.NoError(t, err)
	require.False(t, r.First.IsRecord)
	require.True(t, r.Last.IsRecord)
	require.NotNil(t, r.GetRecordFrame(3, "", "", false))
	require.NotNil(t, r.GetIndexFrame(8, 1, "", "", ""))
}

func TestRegionsAPI(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()
	ts.startServer(t)
	defer ts.stopServer(t)
	ts.prepareData(t)
	resp, err := ts.fetchStatus("/tables/tidb/t/regions")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	decoder := json.NewDecoder(resp.Body)

	var data TableRegions
	err = decoder.Decode(&data)
	require.NoError(t, err)
	require.True(t, len(data.RecordRegions) > 0)

	// list region
	for _, region := range data.RecordRegions {
		require.True(t, ts.regionContainsTable(t, region.ID, data.TableID))
	}
}

func TestRegionsAPIForClusterIndex(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()
	ts.startServer(t)
	defer ts.stopServer(t)
	ts.prepareData(t)
	resp, err := ts.fetchStatus("/tables/tidb/t/regions")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	decoder := json.NewDecoder(resp.Body)
	var data TableRegions
	err = decoder.Decode(&data)
	require.NoError(t, err)
	require.True(t, len(data.RecordRegions) > 0)
	// list region
	for _, region := range data.RecordRegions {
		resp, err := ts.fetchStatus(fmt.Sprintf("/regions/%d", region.ID))
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		decoder := json.NewDecoder(resp.Body)
		var data RegionDetail
		err = decoder.Decode(&data)
		require.NoError(t, err)
		frameCnt := 0
		for _, f := range data.Frames {
			if f.DBName == "tidb" && f.TableName == "t" {
				frameCnt++
			}
		}
		// frameCnt = clustered primary key + secondary index(idx) = 2.
		require.Equal(t, 2, frameCnt)
		require.NoError(t, resp.Body.Close())
	}
}

func TestRangesAPI(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()
	ts.startServer(t)
	defer ts.stopServer(t)
	ts.prepareData(t)
	resp, err := ts.fetchStatus("/tables/tidb/t/ranges")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	decoder := json.NewDecoder(resp.Body)

	var data TableRanges
	err = decoder.Decode(&data)
	require.NoError(t, err)
	require.Equal(t, "t", data.TableName)
	require.Equal(t, 2, len(data.Indices))
	_, ok := data.Indices["PRIMARY"]
	require.True(t, ok)
	_, ok = data.Indices["idx"]
	require.True(t, ok)
}

func (ts *basicHTTPHandlerTestSuite) regionContainsTable(t *testing.T, regionID uint64, tableID int64) bool {
	resp, err := ts.fetchStatus(fmt.Sprintf("/regions/%d", regionID))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	decoder := json.NewDecoder(resp.Body)
	var data RegionDetail
	err = decoder.Decode(&data)
	require.NoError(t, err)
	for _, index := range data.Frames {
		if index.TableID == tableID {
			return true
		}
	}
	return false
}

func TestListTableRegions(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()
	ts.startServer(t)
	defer ts.stopServer(t)
	ts.prepareData(t)
	// Test list table regions with error
	resp, err := ts.fetchStatus("/tables/fdsfds/aaa/regions")
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp, err = ts.fetchStatus("/tables/tidb/pt/regions")
	require.NoError(t, err)

	var data []*TableRegions
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&data)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	region := data[1]
	resp, err = ts.fetchStatus(fmt.Sprintf("/regions/%d", region.TableID))
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
}

func TestListTableRanges(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()
	ts.startServer(t)
	defer ts.stopServer(t)
	ts.prepareData(t)
	// Test list table regions with error
	resp, err := ts.fetchStatus("/tables/fdsfds/aaa/ranges")
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)

	resp, err = ts.fetchStatus("/tables/tidb/pt/ranges")
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()

	var data []*TableRanges
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&data)
	require.NoError(t, err)
	require.Equal(t, 3, len(data))
	for i, partition := range data {
		require.Equal(t, fmt.Sprintf("p%d", i), partition.TableName)
	}
}

func TestGetRegionByIDWithError(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()
	ts.startServer(t)
	defer ts.stopServer(t)
	resp, err := ts.fetchStatus("/regions/xxx")
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	defer func() { require.NoError(t, resp.Body.Close()) }()
}

func TestBinlogRecover(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()
	ts.startServer(t)
	defer ts.stopServer(t)
	binloginfo.EnableSkipBinlogFlag()
	require.Equal(t, true, binloginfo.IsBinlogSkipped())
	resp, err := ts.fetchStatus("/binlog/recover")
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, false, binloginfo.IsBinlogSkipped())

	// Invalid operation will use the default operation.
	binloginfo.EnableSkipBinlogFlag()
	require.Equal(t, true, binloginfo.IsBinlogSkipped())
	resp, err = ts.fetchStatus("/binlog/recover?op=abc")
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, false, binloginfo.IsBinlogSkipped())

	binloginfo.EnableSkipBinlogFlag()
	require.Equal(t, true, binloginfo.IsBinlogSkipped())
	resp, err = ts.fetchStatus("/binlog/recover?op=abc&seconds=1")
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, false, binloginfo.IsBinlogSkipped())

	binloginfo.EnableSkipBinlogFlag()
	require.Equal(t, true, binloginfo.IsBinlogSkipped())
	binloginfo.AddOneSkippedCommitter()
	resp, err = ts.fetchStatus("/binlog/recover?op=abc&seconds=1")
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, false, binloginfo.IsBinlogSkipped())
	binloginfo.RemoveOneSkippedCommitter()

	binloginfo.AddOneSkippedCommitter()
	require.Equal(t, int32(1), binloginfo.SkippedCommitterCount())
	resp, err = ts.fetchStatus("/binlog/recover?op=reset")
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, int32(0), binloginfo.SkippedCommitterCount())

	binloginfo.EnableSkipBinlogFlag()
	resp, err = ts.fetchStatus("/binlog/recover?op=nowait")
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, false, binloginfo.IsBinlogSkipped())

	// Only the first should work.
	binloginfo.EnableSkipBinlogFlag()
	resp, err = ts.fetchStatus("/binlog/recover?op=nowait&op=reset")
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, false, binloginfo.IsBinlogSkipped())

	resp, err = ts.fetchStatus("/binlog/recover?op=status")
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func (ts *basicHTTPHandlerTestSuite) startServer(t *testing.T) {
	var err error
	ts.store, err = mockstore.NewMockStore()
	require.NoError(t, err)
	ts.domain, err = session.BootstrapSession(ts.store)
	require.NoError(t, err)
	ts.tidbdrv = NewTiDBDriver(ts.store)

	cfg := newTestConfig()
	cfg.Store = "tikv"
	cfg.Port = 0
	cfg.Status.StatusPort = 0
	cfg.Status.ReportStatus = true

	server, err := NewServer(cfg, ts.tidbdrv)
	require.NoError(t, err)
	ts.port = getPortFromTCPAddr(server.listener.Addr())
	ts.statusPort = getPortFromTCPAddr(server.statusListener.Addr())
	ts.server = server
	go func() {
		err := server.Run()
		require.NoError(t, err)
	}()
	ts.waitUntilServerOnline()

	do, err := session.GetDomain(ts.store)
	require.NoError(t, err)
	ts.sh = &StatsHandler{do}
}

func getPortFromTCPAddr(addr net.Addr) uint {
	return uint(addr.(*net.TCPAddr).Port)
}

func (ts *basicHTTPHandlerTestSuite) stopServer(t *testing.T) {
	if ts.server != nil {
		ts.server.Close()
	}
	if ts.domain != nil {
		ts.domain.Close()
	}
	if ts.store != nil {
		require.NoError(t, ts.store.Close())
	}
}

func (ts *basicHTTPHandlerTestSuite) prepareData(t *testing.T) {
	db, err := sql.Open("mysql", ts.getDSN())
	require.NoError(t, err)
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()
	dbt := testkit.NewDBTestKit(t, db)

	dbt.MustExec("create database tidb;")
	dbt.MustExec("use tidb;")
	dbt.MustExec("create table tidb.test (a int auto_increment primary key, b varchar(20));")
	dbt.MustExec("insert tidb.test values (1, 1);")
	txn1, err := dbt.GetDB().Begin()
	require.NoError(t, err)
	_, err = txn1.Exec("update tidb.test set b = b + 1 where a = 1;")
	require.NoError(t, err)
	_, err = txn1.Exec("insert tidb.test values (2, 2);")
	require.NoError(t, err)
	_, err = txn1.Exec("insert tidb.test (a) values (3);")
	require.NoError(t, err)
	_, err = txn1.Exec("insert tidb.test values (4, '');")
	require.NoError(t, err)
	err = txn1.Commit()
	require.NoError(t, err)
	dbt.MustExec("alter table tidb.test add index idx1 (a, b);")
	dbt.MustExec("alter table tidb.test add unique index idx2 (a, b);")

	dbt.MustExec(`create table tidb.pt (a int primary key, b varchar(20), key idx(a, b))
partition by range (a)
(partition p0 values less than (256),
 partition p1 values less than (512),
 partition p2 values less than (1024))`)

	txn2, err := dbt.GetDB().Begin()
	require.NoError(t, err)
	_, err = txn2.Exec("insert into tidb.pt values (42, '123')")
	require.NoError(t, err)
	_, err = txn2.Exec("insert into tidb.pt values (256, 'b')")
	require.NoError(t, err)
	_, err = txn2.Exec("insert into tidb.pt values (666, 'def')")
	require.NoError(t, err)
	err = txn2.Commit()
	require.NoError(t, err)
	dbt.MustExec("drop table if exists t")
	dbt.MustExec("create table t (a double, b varchar(20), c int, primary key(a,b) clustered, key idx(c))")
	dbt.MustExec("insert into t values(1.1,'111',1),(2.2,'222',2)")
}

func decodeKeyMvcc(closer io.ReadCloser, t *testing.T, valid bool) {
	decoder := json.NewDecoder(closer)
	var data helper.MvccKV
	err := decoder.Decode(&data)
	require.NoError(t, err)
	if valid {
		require.NotNil(t, data.Value.Info)
		require.Greater(t, len(data.Value.Info.Writes), 0)
	} else {
		require.Nil(t, data.Value.Info.Lock)
		require.Nil(t, data.Value.Info.Writes)
		require.Nil(t, data.Value.Info.Values)
	}
}

func TestGetTableMVCC(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()
	ts.startServer(t)
	ts.prepareData(t)
	defer ts.stopServer(t)

	resp, err := ts.fetchStatus("/mvcc/key/tidb/test/1")
	require.NoError(t, err)
	decoder := json.NewDecoder(resp.Body)
	var data helper.MvccKV
	err = decoder.Decode(&data)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.NotNil(t, data.Value)
	info := data.Value.Info
	require.NotNil(t, info)
	require.Greater(t, len(info.Writes), 0)

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
	require.NoError(t, err)
	var p2 helper.MvccKV
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&p2)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	for i, expect := range info.Values {
		v2 := p2.Value.Info.Values[i].Value
		require.Equal(t, expect.Value, v2)
	}

	hexKey := p2.Key
	resp, err = ts.fetchStatus("/mvcc/hex/" + hexKey)
	require.NoError(t, err)
	decoder = json.NewDecoder(resp.Body)
	var data2 helper.MvccKV
	err = decoder.Decode(&data2)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, data, data2)

	resp, err = ts.fetchStatus("/mvcc/key/tidb/test/1?decode=true")
	require.NoError(t, err)
	decoder = json.NewDecoder(resp.Body)
	var data3 map[string]interface{}
	err = decoder.Decode(&data3)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.NotNil(t, data3["key"])
	require.NotNil(t, data3["info"])
	require.NotNil(t, data3["data"])
	require.Nil(t, data3["decode_error"])

	resp, err = ts.fetchStatus("/mvcc/key/tidb/pt(p0)/42?decode=true")
	require.NoError(t, err)
	decoder = json.NewDecoder(resp.Body)
	var data4 map[string]interface{}
	err = decoder.Decode(&data4)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.NotNil(t, data4["key"])
	require.NotNil(t, data4["info"])
	require.NotNil(t, data4["data"])
	require.Nil(t, data4["decode_error"])
	require.NoError(t, resp.Body.Close())

	resp, err = ts.fetchStatus("/mvcc/key/tidb/t/42")
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
	resp, err = ts.fetchStatus("/mvcc/key/tidb/t?a=1.1")
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
	resp, err = ts.fetchStatus("/mvcc/key/tidb/t?a=1.1&b=111&decode=1")
	require.NoError(t, err)
	decoder = json.NewDecoder(resp.Body)
	var data5 map[string]interface{}
	err = decoder.Decode(&data5)
	require.NoError(t, err)
	require.NotNil(t, data4["key"])
	require.NotNil(t, data4["info"])
	require.NotNil(t, data4["data"])
	require.Nil(t, data4["decode_error"])
	require.NoError(t, resp.Body.Close())
}

func TestGetMVCCNotFound(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()
	ts.startServer(t)
	ts.prepareData(t)
	defer ts.stopServer(t)
	resp, err := ts.fetchStatus("/mvcc/key/tidb/test/1234")
	require.NoError(t, err)
	decoder := json.NewDecoder(resp.Body)
	var data helper.MvccKV
	err = decoder.Decode(&data)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Nil(t, data.Value.Info.Lock)
	require.Nil(t, data.Value.Info.Writes)
	require.Nil(t, data.Value.Info.Values)
}

func TestDecodeColumnValue(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()
	ts.startServer(t)
	ts.prepareData(t)
	defer ts.stopServer(t)

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
	require.NoError(t, err)
	require.NotNil(t, bs)
	bin := base64.StdEncoding.EncodeToString(bs)

	unitTest := func(col *column) {
		path := fmt.Sprintf("/tables/%d/%v/%d/%d?rowBin=%s", col.id, col.tp.GetType(), col.tp.GetFlag(), col.tp.GetFlen(), bin)
		resp, err := ts.fetchStatus(path)
		require.NoErrorf(t, err, "url: %v", ts.statusURL(path))
		decoder := json.NewDecoder(resp.Body)
		var data interface{}
		err = decoder.Decode(&data)
		require.NoErrorf(t, err, "url: %v\ndata: %v", ts.statusURL(path), data)
		require.NoError(t, resp.Body.Close())
		colVal, err := types.DatumsToString([]types.Datum{row[col.id-1]}, false)
		require.NoError(t, err)
		require.Equalf(t, colVal, data, "url: %v", ts.statusURL(path))
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

func TestGetIndexMVCC(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()
	ts.startServer(t)
	ts.prepareData(t)
	defer ts.stopServer(t)

	// tests for normal index key
	resp, err := ts.fetchStatus("/mvcc/index/tidb/test/idx1/1?a=1&b=2")
	require.NoError(t, err)
	decodeKeyMvcc(resp.Body, t, true)
	require.NoError(t, resp.Body.Close())

	resp, err = ts.fetchStatus("/mvcc/index/tidb/test/idx2/1?a=1&b=2")
	require.NoError(t, err)
	decodeKeyMvcc(resp.Body, t, true)
	require.NoError(t, resp.Body.Close())

	// tests for index key which includes null
	resp, err = ts.fetchStatus("/mvcc/index/tidb/test/idx1/3?a=3&b")
	require.NoError(t, err)
	decodeKeyMvcc(resp.Body, t, true)
	require.NoError(t, resp.Body.Close())

	resp, err = ts.fetchStatus("/mvcc/index/tidb/test/idx2/3?a=3&b")
	require.NoError(t, err)
	decodeKeyMvcc(resp.Body, t, true)
	require.NoError(t, resp.Body.Close())

	// tests for index key which includes empty string
	resp, err = ts.fetchStatus("/mvcc/index/tidb/test/idx1/4?a=4&b=")
	require.NoError(t, err)
	decodeKeyMvcc(resp.Body, t, true)
	require.NoError(t, resp.Body.Close())

	resp, err = ts.fetchStatus("/mvcc/index/tidb/test/idx2/3?a=4&b=")
	require.NoError(t, err)
	decodeKeyMvcc(resp.Body, t, true)
	require.NoError(t, resp.Body.Close())

	resp, err = ts.fetchStatus("/mvcc/index/tidb/t/idx?a=1.1&b=111&c=1")
	require.NoError(t, err)
	decodeKeyMvcc(resp.Body, t, true)
	require.NoError(t, resp.Body.Close())

	// tests for wrong key
	resp, err = ts.fetchStatus("/mvcc/index/tidb/test/idx1/5?a=5&b=1")
	require.NoError(t, err)
	decodeKeyMvcc(resp.Body, t, false)
	require.NoError(t, resp.Body.Close())

	resp, err = ts.fetchStatus("/mvcc/index/tidb/test/idx2/5?a=5&b=1")
	require.NoError(t, err)
	decodeKeyMvcc(resp.Body, t, false)
	require.NoError(t, resp.Body.Close())

	// tests for missing column value
	resp, err = ts.fetchStatus("/mvcc/index/tidb/test/idx1/1?a=1")
	require.NoError(t, err)
	decoder := json.NewDecoder(resp.Body)
	var data1 helper.MvccKV
	err = decoder.Decode(&data1)
	require.Error(t, err)
	require.NoError(t, resp.Body.Close())

	resp, err = ts.fetchStatus("/mvcc/index/tidb/test/idx2/1?a=1")
	require.NoError(t, err)
	decoder = json.NewDecoder(resp.Body)
	var data2 helper.MvccKV
	err = decoder.Decode(&data2)
	require.Error(t, err)
	require.NoError(t, resp.Body.Close())

	resp, err = ts.fetchStatus("/mvcc/index/tidb/pt(p2)/idx/666?a=666&b=def")
	require.NoError(t, err)
	decodeKeyMvcc(resp.Body, t, true)
	require.NoError(t, resp.Body.Close())
}

func TestGetSettings(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()
	ts.startServer(t)
	ts.prepareData(t)
	defer ts.stopServer(t)
	resp, err := ts.fetchStatus("/settings")
	require.NoError(t, err)
	decoder := json.NewDecoder(resp.Body)
	var settings *config.Config
	err = decoder.Decode(&settings)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	var configBytes []byte
	configBytes, err = json.Marshal(config.GetGlobalConfig())
	require.NoError(t, err)
	var settingBytes []byte
	settingBytes, err = json.Marshal(settings)
	require.NoError(t, err)
	require.Equal(t, configBytes, settingBytes)
}

func TestGetSchema(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()
	ts.startServer(t)
	ts.prepareData(t)
	defer ts.stopServer(t)
	resp, err := ts.fetchStatus("/schema")
	require.NoError(t, err)
	decoder := json.NewDecoder(resp.Body)
	var dbs []*model.DBInfo
	err = decoder.Decode(&dbs)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	expects := []string{"information_schema", "metrics_schema", "mysql", "performance_schema", "test", "tidb"}
	names := make([]string, len(dbs))
	for i, v := range dbs {
		names[i] = v.Name.L
	}
	sort.Strings(names)
	require.Equal(t, expects, names)
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	userTbl := external.GetTableByName(t, tk, "mysql", "user")
	resp, err = ts.fetchStatus(fmt.Sprintf("/schema?table_id=%d", userTbl.Meta().ID))
	require.NoError(t, err)
	var ti *model.TableInfo
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&ti)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, "user", ti.Name.L)

	resp, err = ts.fetchStatus("/schema?table_id=a")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	resp, err = ts.fetchStatus("/schema?table_id=1")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	resp, err = ts.fetchStatus("/schema?table_id=-1")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	resp, err = ts.fetchStatus("/schema/tidb")
	require.NoError(t, err)
	var lt []*model.TableInfo
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&lt)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Greater(t, len(lt), 2)

	resp, err = ts.fetchStatus("/schema/abc")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	resp, err = ts.fetchStatus("/schema/tidb/test")
	require.NoError(t, err)
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&ti)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, "test", ti.Name.L)

	resp, err = ts.fetchStatus("/schema/tidb/abc")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	resp, err = ts.fetchStatus(fmt.Sprintf("/db-table/%d", userTbl.Meta().ID))
	require.NoError(t, err)
	var dbtbl *dbTableInfo
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&dbtbl)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, "user", dbtbl.TableInfo.Name.L)
	require.Equal(t, "mysql", dbtbl.DBInfo.Name.L)
	se, err := session.CreateSession(ts.store)
	require.NoError(t, err)
	require.Equal(t, domain.GetDomain(se.(sessionctx.Context)).InfoSchema().SchemaMetaVersion(), dbtbl.SchemaVersion)

	db, err := sql.Open("mysql", ts.getDSN())
	require.NoError(t, err)
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()
	dbt := testkit.NewDBTestKit(t, db)

	dbt.MustExec("create database if not exists test;")
	dbt.MustExec("use test;")
	dbt.MustExec(` create table t1 (id int KEY)
		partition by range (id) (
		PARTITION p0 VALUES LESS THAN (3),
		PARTITION p1 VALUES LESS THAN (5),
		PARTITION p2 VALUES LESS THAN (7),
		PARTITION p3 VALUES LESS THAN (9))`)

	resp, err = ts.fetchStatus("/schema/test/t1")
	require.NoError(t, err)
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&ti)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, "t1", ti.Name.L)

	resp, err = ts.fetchStatus(fmt.Sprintf("/db-table/%v", ti.GetPartitionInfo().Definitions[0].ID))
	require.NoError(t, err)
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&dbtbl)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, "t1", dbtbl.TableInfo.Name.L)
	require.Equal(t, "test", dbtbl.DBInfo.Name.L)
	require.Equal(t, ti, dbtbl.TableInfo)
}

func TestAllHistory(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()
	ts.startServer(t)
	ts.prepareData(t)
	defer ts.stopServer(t)
	resp, err := ts.fetchStatus("/ddl/history/?limit=3")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	resp, err = ts.fetchStatus("/ddl/history/?limit=-1")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	resp, err = ts.fetchStatus("/ddl/history")
	require.NoError(t, err)
	decoder := json.NewDecoder(resp.Body)

	var jobs []*model.Job
	s, _ := session.CreateSession(ts.server.newTikvHandlerTool().Store.(kv.Storage))
	defer s.Close()
	store := domain.GetDomain(s.(sessionctx.Context)).Store()
	txn, _ := store.Begin()
	txnMeta := meta.NewMeta(txn)
	_, err = txnMeta.GetAllHistoryDDLJobs()
	require.NoError(t, err)
	data, _ := txnMeta.GetAllHistoryDDLJobs()
	err = decoder.Decode(&jobs)

	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, data, jobs)
}

func dummyRecord() *deadlockhistory.DeadlockRecord {
	return &deadlockhistory.DeadlockRecord{}
}

func TestPprof(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()
	ts.startServer(t)
	defer ts.stopServer(t)
	retryTime := 100
	for retry := 0; retry < retryTime; retry++ {
		resp, err := ts.fetchStatus("/debug/pprof/heap")
		if err == nil {
			_, err = io.ReadAll(resp.Body)
			require.NoError(t, err)
			err = resp.Body.Close()
			require.NoError(t, err)
			return
		}
		time.Sleep(time.Millisecond * 10)
	}
	log.Fatal("failed to get profile for %d retries in every 10 ms", zap.Int("retryTime", retryTime))
}

func TestHotRegionInfo(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()
	ts.startServer(t)
	defer ts.stopServer(t)
	resp, err := ts.fetchStatus("/regions/hot")
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestDebugZip(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()
	ts.startServer(t)
	defer ts.stopServer(t)
	resp, err := ts.fetchStatus("/debug/zip?seconds=1")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	b, err := httputil.DumpResponse(resp, true)
	require.NoError(t, err)
	require.Greater(t, len(b), 0)
	require.NoError(t, resp.Body.Close())
}

func TestCheckCN(t *testing.T) {
	s := &Server{cfg: &config.Config{Security: config.Security{ClusterVerifyCN: []string{"a ", "b", "c"}}}}
	tlsConfig := &tls.Config{}
	s.setCNChecker(tlsConfig)
	require.NotNil(t, tlsConfig.VerifyPeerCertificate)
	err := tlsConfig.VerifyPeerCertificate(nil, [][]*x509.Certificate{{{Subject: pkix.Name{CommonName: "a"}}}})
	require.NoError(t, err)
	err = tlsConfig.VerifyPeerCertificate(nil, [][]*x509.Certificate{{{Subject: pkix.Name{CommonName: "b"}}}})
	require.NoError(t, err)
	err = tlsConfig.VerifyPeerCertificate(nil, [][]*x509.Certificate{{{Subject: pkix.Name{CommonName: "d"}}}})
	require.Error(t, err)
}

func TestDDLHookHandler(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()

	ts.startServer(t)
	defer ts.stopServer(t)
	resp, err := ts.fetchStatus("/test/ddl/hook")
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp, err = ts.postStatus("/test/ddl/hook", "application/x-www-form-urlencoded", bytes.NewBuffer([]byte(`ddl_hook=ctc_hook`)))
	require.NoError(t, err)
	require.NotNil(t, resp)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, "\"success!\"", string(body))
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp, err = ts.postStatus("/test/ddl/hook", "application/x-www-form-urlencoded", bytes.NewBuffer([]byte(`ddl_hook=default_hook`)))
	require.NoError(t, err)
	require.NotNil(t, resp)
	body, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, "\"success!\"", string(body))
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestWriteDBTablesData(t *testing.T) {
	// No table in a schema.
	info := infoschema.MockInfoSchema([]*model.TableInfo{})
	rc := httptest.NewRecorder()
	tbs := info.SchemaTables(model.NewCIStr("test"))
	require.Equal(t, 0, len(tbs))
	writeDBTablesData(rc, tbs)
	var ti []*model.TableInfo
	decoder := json.NewDecoder(rc.Body)
	err := decoder.Decode(&ti)
	require.NoError(t, err)
	require.Equal(t, 0, len(ti))

	// One table in a schema.
	info = infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable()})
	rc = httptest.NewRecorder()
	tbs = info.SchemaTables(model.NewCIStr("test"))
	require.Equal(t, 1, len(tbs))
	writeDBTablesData(rc, tbs)
	decoder = json.NewDecoder(rc.Body)
	err = decoder.Decode(&ti)
	require.NoError(t, err)
	require.Equal(t, 1, len(ti))
	require.Equal(t, ti[0].ID, tbs[0].Meta().ID)
	require.Equal(t, ti[0].Name.String(), tbs[0].Meta().Name.String())

	// Two tables in a schema.
	info = infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
	rc = httptest.NewRecorder()
	tbs = info.SchemaTables(model.NewCIStr("test"))
	require.Equal(t, 2, len(tbs))
	writeDBTablesData(rc, tbs)
	decoder = json.NewDecoder(rc.Body)
	err = decoder.Decode(&ti)
	require.NoError(t, err)
	require.Equal(t, 2, len(ti))
	require.Equal(t, ti[0].ID, tbs[0].Meta().ID)
	require.Equal(t, ti[1].ID, tbs[1].Meta().ID)
	require.Equal(t, ti[0].Name.String(), tbs[0].Meta().Name.String())
	require.Equal(t, ti[1].Name.String(), tbs[1].Meta().Name.String())
}
