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

package tests

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"sort"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/executor/mppcoordmanager"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core"
	server2 "github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/server/handler"
	"github.com/pingcap/tidb/pkg/server/handler/optimizor"
	"github.com/pingcap/tidb/pkg/server/handler/tikvhandler"
	"github.com/pingcap/tidb/pkg/server/internal/testserverclient"
	"github.com/pingcap/tidb/pkg/server/internal/testutil"
	"github.com/pingcap/tidb/pkg/server/internal/util"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/binloginfo"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"go.etcd.io/etcd/tests/v3/integration"
	"go.uber.org/zap"
)

type basicHTTPHandlerTestSuite struct {
	*testserverclient.TestServerClient
	server  *server2.Server
	store   kv.Storage
	domain  *domain.Domain
	tidbdrv *server2.TiDBDriver
	sh      *optimizor.StatsHandler
}

func createBasicHTTPHandlerTestSuite() *basicHTTPHandlerTestSuite {
	ts := &basicHTTPHandlerTestSuite{}
	ts.TestServerClient = testserverclient.NewTestServerClient()
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
	encodedValue, err := codec.EncodeKey(stmtctx.NewStmtCtxWithTimeZone(time.Local).TimeZone(), nil, indexValues...)
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
	encodedValue, err := codec.EncodeKey(stmtctx.NewStmtCtxWithTimeZone(time.Local).TimeZone(), nil, indexValues...)
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
	resp, err := ts.FetchStatus("/tables/tidb/t/regions")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	decoder := json.NewDecoder(resp.Body)

	var data tikvhandler.TableRegions
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
	resp, err := ts.FetchStatus("/tables/tidb/t/regions")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	decoder := json.NewDecoder(resp.Body)
	var data tikvhandler.TableRegions
	err = decoder.Decode(&data)
	require.NoError(t, err)
	require.True(t, len(data.RecordRegions) > 0)
	// list region
	for _, region := range data.RecordRegions {
		resp, err := ts.FetchStatus(fmt.Sprintf("/regions/%d", region.ID))
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		decoder := json.NewDecoder(resp.Body)
		var data tikvhandler.RegionDetail
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
	resp, err := ts.FetchStatus("/tables/tidb/t/ranges")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	decoder := json.NewDecoder(resp.Body)

	var data tikvhandler.TableRanges
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
	resp, err := ts.FetchStatus(fmt.Sprintf("/regions/%d", regionID))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	decoder := json.NewDecoder(resp.Body)
	var data tikvhandler.RegionDetail
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
	resp, err := ts.FetchStatus("/tables/fdsfds/aaa/regions")
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp, err = ts.FetchStatus("/tables/tidb/pt/regions")
	require.NoError(t, err)

	var data []*tikvhandler.TableRegions
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&data)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	region := data[1]
	resp, err = ts.FetchStatus(fmt.Sprintf("/regions/%d", region.TableID))
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
}

func TestListTableRanges(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()
	ts.startServer(t)
	defer ts.stopServer(t)
	ts.prepareData(t)
	// Test list table regions with error
	resp, err := ts.FetchStatus("/tables/fdsfds/aaa/ranges")
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)

	resp, err = ts.FetchStatus("/tables/tidb/pt/ranges")
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()

	var data []*tikvhandler.TableRanges
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
	resp, err := ts.FetchStatus("/regions/xxx")
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
	resp, err := ts.FetchStatus("/binlog/recover")
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, false, binloginfo.IsBinlogSkipped())

	// Invalid operation will use the default operation.
	binloginfo.EnableSkipBinlogFlag()
	require.Equal(t, true, binloginfo.IsBinlogSkipped())
	resp, err = ts.FetchStatus("/binlog/recover?op=abc")
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, false, binloginfo.IsBinlogSkipped())

	binloginfo.EnableSkipBinlogFlag()
	require.Equal(t, true, binloginfo.IsBinlogSkipped())
	resp, err = ts.FetchStatus("/binlog/recover?op=abc&seconds=1")
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, false, binloginfo.IsBinlogSkipped())

	binloginfo.EnableSkipBinlogFlag()
	require.Equal(t, true, binloginfo.IsBinlogSkipped())
	binloginfo.AddOneSkippedCommitter()
	resp, err = ts.FetchStatus("/binlog/recover?op=abc&seconds=1")
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, false, binloginfo.IsBinlogSkipped())
	binloginfo.RemoveOneSkippedCommitter()

	binloginfo.AddOneSkippedCommitter()
	require.Equal(t, int32(1), binloginfo.SkippedCommitterCount())
	resp, err = ts.FetchStatus("/binlog/recover?op=reset")
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, int32(0), binloginfo.SkippedCommitterCount())

	binloginfo.EnableSkipBinlogFlag()
	resp, err = ts.FetchStatus("/binlog/recover?op=nowait")
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, false, binloginfo.IsBinlogSkipped())

	// Only the first should work.
	binloginfo.EnableSkipBinlogFlag()
	resp, err = ts.FetchStatus("/binlog/recover?op=nowait&op=reset")
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, false, binloginfo.IsBinlogSkipped())

	resp, err = ts.FetchStatus("/binlog/recover?op=status")
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
	ts.tidbdrv = server2.NewTiDBDriver(ts.store)

	cfg := util.NewTestConfig()
	cfg.Store = "tikv"
	cfg.Port = 0
	cfg.Status.StatusPort = 0
	cfg.Status.ReportStatus = true
	server2.RunInGoTestChan = make(chan struct{})
	server, err := server2.NewServer(cfg, ts.tidbdrv)
	require.NoError(t, err)
	ts.server = server
	ts.server.SetDomain(ts.domain)
	go func() {
		err := server.Run(ts.domain)
		require.NoError(t, err)
	}()
	<-server2.RunInGoTestChan
	ts.Port = testutil.GetPortFromTCPAddr(server.ListenAddr())
	ts.StatusPort = testutil.GetPortFromTCPAddr(server.StatusListenerAddr())
	ts.WaitUntilServerOnline()

	do, err := session.GetDomain(ts.store)
	require.NoError(t, err)
	ts.sh = optimizor.NewStatsHandler(do)
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
	db, err := sql.Open("mysql", ts.GetDSN())
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
	dbt.MustExec("alter table tidb.test drop index idx1;")
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
	var data []helper.MvccKV
	err := decoder.Decode(&data)
	require.NoError(t, err)
	if valid {
		require.NotNil(t, data[0].Value.Info)
		require.Greater(t, len(data[0].Value.Info.Writes), 0)
	} else {
		require.Nil(t, data[0].Value.Info.Lock)
		require.Nil(t, data[0].Value.Info.Writes)
		require.Nil(t, data[0].Value.Info.Values)
	}
}

func TestGetTableMVCC(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()
	ts.startServer(t)
	ts.prepareData(t)
	defer ts.stopServer(t)

	resp, err := ts.FetchStatus("/mvcc/key/tidb/test/1")
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

	resp, err = ts.FetchStatus(fmt.Sprintf("/mvcc/txn/%d/tidb/test", startTs))
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
	resp, err = ts.FetchStatus("/mvcc/hex/" + hexKey)
	require.NoError(t, err)
	decoder = json.NewDecoder(resp.Body)
	var data2 helper.MvccKV
	err = decoder.Decode(&data2)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, data, data2)

	resp, err = ts.FetchStatus("/mvcc/key/tidb/test/1?decode=true")
	require.NoError(t, err)
	decoder = json.NewDecoder(resp.Body)
	var data3 map[string]any
	err = decoder.Decode(&data3)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.NotNil(t, data3["key"])
	require.NotNil(t, data3["info"])
	require.NotNil(t, data3["data"])
	require.Nil(t, data3["decode_error"])

	resp, err = ts.FetchStatus("/mvcc/key/tidb/pt(p0)/42?decode=true")
	require.NoError(t, err)
	decoder = json.NewDecoder(resp.Body)
	var data4 map[string]any
	err = decoder.Decode(&data4)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.NotNil(t, data4["key"])
	require.NotNil(t, data4["info"])
	require.NotNil(t, data4["data"])
	require.Nil(t, data4["decode_error"])
	require.NoError(t, resp.Body.Close())

	resp, err = ts.FetchStatus("/mvcc/key/tidb/t/42")
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
	resp, err = ts.FetchStatus("/mvcc/key/tidb/t?a=1.1")
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
	resp, err = ts.FetchStatus("/mvcc/key/tidb/t?a=1.1&b=111&decode=1")
	require.NoError(t, err)
	decoder = json.NewDecoder(resp.Body)
	var data5 map[string]any
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
	resp, err := ts.FetchStatus("/mvcc/key/tidb/test/1234")
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
	sc := stmtctx.NewStmtCtxWithTimeZone(time.UTC)
	bs, err := tablecodec.EncodeRow(sc.TimeZone(), row, colIDs, nil, nil, &rd)
	require.NoError(t, err)
	require.NotNil(t, bs)
	bin := base64.StdEncoding.EncodeToString(bs)

	unitTest := func(col *column) {
		path := fmt.Sprintf("/tables/%d/%v/%d/%d?rowBin=%s", col.id, col.tp.GetType(), col.tp.GetFlag(), col.tp.GetFlen(), bin)
		resp, err := ts.FetchStatus(path)
		require.NoErrorf(t, err, "url: %v", ts.StatusURL(path))
		decoder := json.NewDecoder(resp.Body)
		var data any
		err = decoder.Decode(&data)
		require.NoErrorf(t, err, "url: %v\ndata: %v", ts.StatusURL(path), data)
		require.NoError(t, resp.Body.Close())
		colVal, err := types.DatumsToString([]types.Datum{row[col.id-1]}, false)
		require.NoError(t, err)
		require.Equalf(t, colVal, data, "url: %v", ts.StatusURL(path))
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
	resp, err := ts.FetchStatus("/mvcc/index/tidb/test/idx1/1?a=1&b=2")
	require.NoError(t, err)
	decodeKeyMvcc(resp.Body, t, true)
	require.NoError(t, resp.Body.Close())

	resp, err = ts.FetchStatus("/mvcc/index/tidb/test/idx2/1?a=1&b=2")
	require.NoError(t, err)
	decodeKeyMvcc(resp.Body, t, true)
	require.NoError(t, resp.Body.Close())

	// tests for index key which includes null
	resp, err = ts.FetchStatus("/mvcc/index/tidb/test/idx1/3?a=3&b")
	require.NoError(t, err)
	decodeKeyMvcc(resp.Body, t, true)
	require.NoError(t, resp.Body.Close())

	resp, err = ts.FetchStatus("/mvcc/index/tidb/test/idx2/3?a=3&b")
	require.NoError(t, err)
	decodeKeyMvcc(resp.Body, t, true)
	require.NoError(t, resp.Body.Close())

	// tests for index key which includes empty string
	resp, err = ts.FetchStatus("/mvcc/index/tidb/test/idx1/4?a=4&b=")
	require.NoError(t, err)
	decodeKeyMvcc(resp.Body, t, true)
	require.NoError(t, resp.Body.Close())

	resp, err = ts.FetchStatus("/mvcc/index/tidb/test/idx2/3?a=4&b=")
	require.NoError(t, err)
	decodeKeyMvcc(resp.Body, t, true)
	require.NoError(t, resp.Body.Close())

	resp, err = ts.FetchStatus("/mvcc/index/tidb/t/idx?a=1.1&b=111&c=1")
	require.NoError(t, err)
	decodeKeyMvcc(resp.Body, t, true)
	require.NoError(t, resp.Body.Close())

	// tests for wrong key
	resp, err = ts.FetchStatus("/mvcc/index/tidb/test/idx1/5?a=5&b=1")
	require.NoError(t, err)
	decodeKeyMvcc(resp.Body, t, false)
	require.NoError(t, resp.Body.Close())

	resp, err = ts.FetchStatus("/mvcc/index/tidb/test/idx2/5?a=5&b=1")
	require.NoError(t, err)
	decodeKeyMvcc(resp.Body, t, false)
	require.NoError(t, resp.Body.Close())

	// tests for missing column value
	resp, err = ts.FetchStatus("/mvcc/index/tidb/test/idx1/1?a=1")
	require.NoError(t, err)
	decoder := json.NewDecoder(resp.Body)
	var data1 helper.MvccKV
	err = decoder.Decode(&data1)
	require.Error(t, err)
	require.NoError(t, resp.Body.Close())

	resp, err = ts.FetchStatus("/mvcc/index/tidb/test/idx2/1?a=1")
	require.NoError(t, err)
	decoder = json.NewDecoder(resp.Body)
	var data2 helper.MvccKV
	err = decoder.Decode(&data2)
	require.Error(t, err)
	require.NoError(t, resp.Body.Close())

	resp, err = ts.FetchStatus("/mvcc/index/tidb/pt(p2)/idx/666?a=666&b=def")
	require.NoError(t, err)
	decodeKeyMvcc(resp.Body, t, true)
	require.NoError(t, resp.Body.Close())
}

func TestGetSettings(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()
	ts.startServer(t)
	ts.prepareData(t)
	defer ts.stopServer(t)
	resp, err := ts.FetchStatus("/settings")
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
	resp, err := ts.FetchStatus("/schema")
	require.NoError(t, err)
	decoder := json.NewDecoder(resp.Body)
	var dbs []*model.DBInfo
	err = decoder.Decode(&dbs)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	expects := []string{"information_schema", "metrics_schema", "mysql", "performance_schema", "sys", "test", "tidb"}
	names := make([]string, len(dbs))
	for i, v := range dbs {
		names[i] = v.Name.L
	}
	sort.Strings(names)
	require.Equal(t, expects, names)
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	userTbl := external.GetTableByName(t, tk, "mysql", "user")
	resp, err = ts.FetchStatus(fmt.Sprintf("/schema?table_id=%d", userTbl.Meta().ID))
	require.NoError(t, err)
	var ti *model.TableInfo
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&ti)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, "user", ti.Name.L)

	resp, err = ts.FetchStatus("/schema?table_id=a")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	resp, err = ts.FetchStatus("/schema?table_id=1")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	resp, err = ts.FetchStatus("/schema?table_id=-1")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	resp, err = ts.FetchStatus("/schema/tidb")
	require.NoError(t, err)
	var lt []*model.TableInfo
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&lt)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Greater(t, len(lt), 2)

	resp, err = ts.FetchStatus("/schema/abc")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	resp, err = ts.FetchStatus("/schema/tidb/test")
	require.NoError(t, err)
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&ti)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, "test", ti.Name.L)

	resp, err = ts.FetchStatus("/schema/tidb/abc")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	resp, err = ts.FetchStatus(fmt.Sprintf("/db-table/%d", userTbl.Meta().ID))
	require.NoError(t, err)
	var dbtbl *tikvhandler.DBTableInfo
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&dbtbl)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, "user", dbtbl.TableInfo.Name.L)
	require.Equal(t, "mysql", dbtbl.DBInfo.Name.L)
	se, err := session.CreateSession(ts.store)
	require.NoError(t, err)
	require.Equal(t, domain.GetDomain(se.(sessionctx.Context)).InfoSchema().SchemaMetaVersion(), dbtbl.SchemaVersion)

	db, err := sql.Open("mysql", ts.GetDSN())
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

	resp, err = ts.FetchStatus("/schema/test/t1")
	require.NoError(t, err)
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&ti)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, "t1", ti.Name.L)

	resp, err = ts.FetchStatus(fmt.Sprintf("/db-table/%v", ti.GetPartitionInfo().Definitions[0].ID))
	require.NoError(t, err)
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&dbtbl)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, "t1", dbtbl.TableInfo.Name.L)
	require.Equal(t, "test", dbtbl.DBInfo.Name.L)
	require.Equal(t, ti, dbtbl.TableInfo)

	resp, err = ts.FetchStatus(fmt.Sprintf("/schema?table_id=%v", ti.GetPartitionInfo().Definitions[0].ID))
	require.NoError(t, err)
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&ti)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, "t1", ti.Name.L)
	require.Equal(t, ti, ti)
}

func TestAllHistory(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()
	ts.startServer(t)
	ts.prepareData(t)
	defer ts.stopServer(t)
	resp, err := ts.FetchStatus("/ddl/history/?limit=3")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	resp, err = ts.FetchStatus("/ddl/history/?limit=-1")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	resp, err = ts.FetchStatus("/ddl/history")
	require.NoError(t, err)
	decoder := json.NewDecoder(resp.Body)

	var jobs []*model.Job
	s, _ := session.CreateSession(ts.server.NewTikvHandlerTool().Store.(kv.Storage))
	defer s.Close()
	store := domain.GetDomain(s.(sessionctx.Context)).Store()
	txn, _ := store.Begin()
	txnMeta := meta.NewMeta(txn)
	data, err := ddl.GetAllHistoryDDLJobs(txnMeta)
	require.NoError(t, err)
	err = decoder.Decode(&jobs)

	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, len(data), len(jobs))
	for i := range data {
		// For the jobs that have arguments(job.Args) for GC delete range,
		// the RawArgs should be the same after filtering the spaces.
		data[i].RawArgs = filterSpaces(data[i].RawArgs)
		jobs[i].RawArgs = filterSpaces(jobs[i].RawArgs)
		require.Equal(t, data[i], jobs[i], i)
	}

	// Cover the start_job_id parameter.
	resp, err = ts.FetchStatus("/ddl/history?start_job_id=41")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	resp, err = ts.FetchStatus("/ddl/history?start_job_id=41&limit=3")
	require.NoError(t, err)
	decoder = json.NewDecoder(resp.Body)
	err = decoder.Decode(&jobs)
	require.NoError(t, err)

	// The result is in descending order
	lastID := int64(42)
	for _, job := range jobs {
		require.Less(t, job.ID, lastID)
		lastID = job.ID
	}
	require.NoError(t, resp.Body.Close())
}

func filterSpaces(bs []byte) []byte {
	if len(bs) == 0 {
		return nil
	}
	tmp := bs[:0]
	for _, b := range bs {
		// 0xa is the line feed character.
		// 0xd is the carriage return character.
		// 0x20 is the space character.
		if b != 0xa && b != 0xd && b != 0x20 {
			tmp = append(tmp, b)
		}
	}
	return tmp
}

func TestPprof(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()
	ts.startServer(t)
	defer ts.stopServer(t)
	retryTime := 100
	for retry := 0; retry < retryTime; retry++ {
		resp, err := ts.FetchStatus("/debug/pprof/heap")
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
	resp, err := ts.FetchStatus("/regions/hot")
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestDebugZip(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()
	ts.startServer(t)
	defer ts.stopServer(t)
	resp, err := ts.FetchStatus("/debug/zip?seconds=1")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	b, err := httputil.DumpResponse(resp, true)
	require.NoError(t, err)
	require.Greater(t, len(b), 0)
	require.NoError(t, resp.Body.Close())
}

func TestCheckCN(t *testing.T) {
	cfg := &config.Config{Security: config.Security{ClusterVerifyCN: []string{"a ", "b", "c"}}}
	s := server2.NewTestServer(cfg)
	tlsConfig := &tls.Config{}
	s.SetCNChecker(tlsConfig)
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
	resp, err := ts.FetchStatus("/test/ddl/hook")
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp, err = ts.PostStatus("/test/ddl/hook", "application/x-www-form-urlencoded", bytes.NewBuffer([]byte(`ddl_hook=ctc_hook`)))
	require.NoError(t, err)
	require.NotNil(t, resp)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, "\"success!\"", string(body))
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp, err = ts.PostStatus("/test/ddl/hook", "application/x-www-form-urlencoded", bytes.NewBuffer([]byte(`ddl_hook=default_hook`)))
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
	tbs := info.SchemaTableInfos(model.NewCIStr("test"))
	require.Equal(t, 0, len(tbs))
	tikvhandler.WriteDBTablesData(rc, tbs)
	var ti []*model.TableInfo
	decoder := json.NewDecoder(rc.Body)
	err := decoder.Decode(&ti)
	require.NoError(t, err)
	require.Equal(t, 0, len(ti))

	// One table in a schema.
	info = infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable()})
	rc = httptest.NewRecorder()
	tbs = info.SchemaTableInfos(model.NewCIStr("test"))
	require.Equal(t, 1, len(tbs))
	tikvhandler.WriteDBTablesData(rc, tbs)
	decoder = json.NewDecoder(rc.Body)
	err = decoder.Decode(&ti)
	require.NoError(t, err)
	require.Equal(t, 1, len(ti))
	require.Equal(t, ti[0].ID, tbs[0].ID)
	require.Equal(t, ti[0].Name.String(), tbs[0].Name.String())

	// Two tables in a schema.
	info = infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
	rc = httptest.NewRecorder()
	tbs = info.SchemaTableInfos(model.NewCIStr("test"))
	require.Equal(t, 2, len(tbs))
	tikvhandler.WriteDBTablesData(rc, tbs)
	decoder = json.NewDecoder(rc.Body)
	err = decoder.Decode(&ti)
	require.NoError(t, err)
	require.Equal(t, 2, len(ti))
	require.Equal(t, ti[0].ID, tbs[0].ID)
	require.Equal(t, ti[1].ID, tbs[1].ID)
	require.Equal(t, ti[0].Name.String(), tbs[0].Name.String())
	require.Equal(t, ti[1].Name.String(), tbs[1].Name.String())
}

func TestSetLabels(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()

	ts.startServer(t)
	defer ts.stopServer(t)

	testUpdateLabels := func(labels, expected map[string]string) {
		buffer := bytes.NewBuffer([]byte{})
		require.Nil(t, json.NewEncoder(buffer).Encode(labels))
		resp, err := ts.PostStatus("/labels", "application/json", buffer)
		require.NoError(t, err)
		require.NotNil(t, resp)
		defer func() {
			require.NoError(t, resp.Body.Close())
		}()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		newLabels := config.GetGlobalConfig().Labels
		require.Equal(t, newLabels, expected)
	}

	labels := map[string]string{
		"zone": "us-west-1",
		"test": "123",
	}
	testUpdateLabels(labels, labels)

	updated := map[string]string{
		"zone": "bj-1",
	}
	labels["zone"] = "bj-1"
	testUpdateLabels(updated, labels)

	// reset the global variable
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Labels = map[string]string{}
	})
}

func TestSetLabelsWithEtcd(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ts.startServer(t)
	defer ts.stopServer(t)

	integration.BeforeTestExternal(t)
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)
	client := cluster.RandClient()
	infosync.SetEtcdClient(client)
	ts.domain.InfoSyncer().Restart(ctx)

	testUpdateLabels := func(labels, expected map[string]string) {
		buffer := bytes.NewBuffer([]byte{})
		require.Nil(t, json.NewEncoder(buffer).Encode(labels))
		resp, err := ts.PostStatus("/labels", "application/json", buffer)
		require.NoError(t, err)
		require.NotNil(t, resp)
		defer func() {
			require.NoError(t, resp.Body.Close())
		}()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		newLabels := config.GetGlobalConfig().Labels
		require.Equal(t, newLabels, expected)
		servers, err := infosync.GetAllServerInfo(ctx)
		require.NoError(t, err)
		for _, server := range servers {
			for k, expectV := range expected {
				v, ok := server.Labels[k]
				require.True(t, ok)
				require.Equal(t, expectV, v)
			}
			return
		}
		require.Fail(t, "no server found")
	}

	labels := map[string]string{
		"zone": "us-west-1",
		"test": "123",
	}
	testUpdateLabels(labels, labels)

	updated := map[string]string{
		"zone": "bj-1",
	}
	labels["zone"] = "bj-1"
	testUpdateLabels(updated, labels)

	// reset the global variable
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Labels = map[string]string{}
	})
}

func TestSetLabelsConcurrentWithGetLabel(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()

	ts.startServer(t)
	defer ts.stopServer(t)

	testUpdateLabels := func() {
		labels := map[string]string{}
		labels["zone"] = fmt.Sprintf("z-%v", rand.Intn(100000))
		buffer := bytes.NewBuffer([]byte{})
		require.Nil(t, json.NewEncoder(buffer).Encode(labels))
		resp, err := ts.PostStatus("/labels", "application/json", buffer)
		require.NoError(t, err)
		require.NotNil(t, resp)
		defer func() {
			require.NoError(t, resp.Body.Close())
		}()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		newLabels := config.GetGlobalConfig().Labels
		require.Equal(t, newLabels, labels)
	}
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				config.GetGlobalConfig().GetTiKVConfig()
			}
		}
	}()
	for i := 0; i < 100; i++ {
		testUpdateLabels()
	}
	close(done)

	// reset the global variable
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Labels = map[string]string{}
	})
}

func TestUpgrade(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()
	ts.startServer(t)
	defer ts.stopServer(t)

	resp, err := ts.FetchStatus("/upgrade/start")
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	require.NoError(t, err)
	require.NotNil(t, resp)
	// test upgrade start
	resp, err = ts.PostStatus("/upgrade/start", "application/x-www-form-urlencoded", nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	b, err := httputil.DumpResponse(resp, true)
	require.NoError(t, err)
	require.Greater(t, len(b), 0)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, "\"success!\"", string(body))
	// check the result
	se, err := session.CreateSession(ts.store)
	require.NoError(t, err)
	isUpgrading, err := session.IsUpgradingClusterState(se)
	require.NoError(t, err)
	require.True(t, isUpgrading)

	// Do start upgrade again.
	resp, err = ts.PostStatus("/upgrade/start", "application/x-www-form-urlencoded", nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	b, err = httputil.DumpResponse(resp, true)
	require.NoError(t, err)
	require.Greater(t, len(b), 0)
	body, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, "\"It's a duplicated operation and the cluster is already in upgrading state.\"", string(body))
	// check the result
	se, err = session.CreateSession(ts.store)
	require.NoError(t, err)
	isUpgrading, err = session.IsUpgradingClusterState(se)
	require.NoError(t, err)
	require.True(t, isUpgrading)

	// test upgrade show
	testUpgradeShow(t, ts)
	// check the cluster state
	se, err = session.CreateSession(ts.store)
	require.NoError(t, err)
	isUpgrading, err = session.IsUpgradingClusterState(se)
	require.NoError(t, err)
	require.True(t, isUpgrading)

	// test upgrade finish
	resp, err = ts.PostStatus("/upgrade/finish", "application/x-www-form-urlencoded", nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	b, err = httputil.DumpResponse(resp, true)
	require.NoError(t, err)
	require.Greater(t, len(b), 0)
	body, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, "\"success!\"", string(body))
	// check the result
	se, err = session.CreateSession(ts.store)
	require.NoError(t, err)
	isUpgrading, err = session.IsUpgradingClusterState(se)
	require.NoError(t, err)
	require.False(t, isUpgrading)

	// test upgrade show failed
	resp, err = ts.PostStatus("/upgrade/show", "application/x-www-form-urlencoded", nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	b, err = httputil.DumpResponse(resp, true)
	require.NoError(t, err)
	require.Greater(t, len(b), 0)
	body, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, "\"The cluster state is normal.\"\"success!\"", string(body))

	// Do finish upgrade again.
	resp, err = ts.PostStatus("/upgrade/finish", "application/x-www-form-urlencoded", nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	b, err = httputil.DumpResponse(resp, true)
	require.NoError(t, err)
	require.Greater(t, len(b), 0)
	body, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, "\"It's a duplicated operation and the cluster is already in normal state.\"", string(body))
	// check the result
	se, err = session.CreateSession(ts.store)
	require.NoError(t, err)
	isUpgrading, err = session.IsUpgradingClusterState(se)
	require.NoError(t, err)
	require.False(t, isUpgrading)
}

func testUpgradeShow(t *testing.T, ts *basicHTTPHandlerTestSuite) {
	do, err := session.GetDomain(ts.store)
	require.NoError(t, err)
	ddlID := do.DDL().GetID()
	// check the result for upgrade show
	mockedAllServerInfos := map[string]*infosync.ServerInfo{
		"s0": {
			ID:           ddlID,
			IP:           "127.0.0.1",
			Port:         4000,
			JSONServerID: 0,
			ServerVersionInfo: infosync.ServerVersionInfo{
				Version: "ver",
				GitHash: "hash",
			},
		},
		"s2": {
			ID:           "ID2",
			IP:           "127.0.0.1",
			Port:         4002,
			JSONServerID: 2,
			ServerVersionInfo: infosync.ServerVersionInfo{
				Version: "ver2",
				GitHash: "hash2",
			},
		},
		"s1": {
			ID:           "ID1",
			IP:           "127.0.0.1",
			Port:         4001,
			JSONServerID: 1,
			ServerVersionInfo: infosync.ServerVersionInfo{
				Version: "ver",
				GitHash: "hash",
			},
		},
	}
	makeFailpointRes := func(v any) string {
		bytes, err := json.Marshal(v)
		require.NoError(t, err)
		return fmt.Sprintf("return(`%s`)", string(bytes))
	}
	checkSimpleServerInfo := func(sInfo handler.SimpleServerInfo) {
		key := fmt.Sprintf("s%d", sInfo.JSONServerID)
		val, ok := mockedAllServerInfos[key]
		require.True(t, ok)
		require.Equal(t, val.Version, sInfo.Version)
		require.Equal(t, val.GitHash, sInfo.GitHash)
		require.Equal(t, val.IP, sInfo.IP)
		require.Equal(t, val.Port, sInfo.Port)
	}
	checkUpgradeShow := func(serverNum, upgradedPercent, diffInfos int) {
		resp, err := ts.PostStatus("/upgrade/show", "application/x-www-form-urlencoded", nil)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		b, err := httputil.DumpResponse(resp, true)
		require.NoError(t, err)
		require.Greater(t, len(b), 0)
		decoder := json.NewDecoder(resp.Body)
		clusterInfo := handler.ClusterUpgradeInfo{}
		err = decoder.Decode(&clusterInfo)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, ddlID, clusterInfo.OwnerID)
		require.Equal(t, serverNum, clusterInfo.ServersNum)
		require.Equal(t, upgradedPercent, clusterInfo.UpgradedPercent)
		require.Equal(t, diffInfos, len(clusterInfo.AllServersDiffInfos))
		if diffInfos > 0 {
			require.False(t, clusterInfo.IsAllUpgraded)
			for _, info := range clusterInfo.AllServersDiffInfos {
				checkSimpleServerInfo(info)
			}
		} else {
			require.True(t, clusterInfo.IsAllUpgraded)
		}
	}

	// test upgrade show for 1 server
	checkUpgradeShow(1, 100, 0)
	// test upgrade show for 3 servers
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/domain/infosync/mockGetAllServerInfo", makeFailpointRes(mockedAllServerInfos)))
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/domain/infosync/mockGetAllServerInfo")
	// test upgrade show again with 3 different version servers
	checkUpgradeShow(3, 33, 3)
	// test upgrade show again with 3 servers of the same version
	mockedAllServerInfos["s2"].Version = mockedAllServerInfos["s0"].Version
	mockedAllServerInfos["s2"].GitHash = mockedAllServerInfos["s0"].GitHash
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/domain/infosync/mockGetAllServerInfo", makeFailpointRes(mockedAllServerInfos)))
	checkUpgradeShow(3, 100, 0)
}

func TestIssue52608(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()

	ts.startServer(t)
	defer ts.stopServer(t)
	on, addr := mppcoordmanager.InstanceMPPCoordinatorManager.GetServerAddr()
	require.Equal(t, on, true)
	require.Equal(t, addr[:10], "127.0.0.1:")
}
