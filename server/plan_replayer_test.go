// Copyright 2021 PingCAP, Inc.
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
	"archive/zip"
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func prepareServerAndClientForTest(t *testing.T, store kv.Storage, dom *domain.Domain) (server *Server, client *testServerClient) {
	driver := NewTiDBDriver(store)
	client = newTestServerClient()

	cfg := newTestConfig()
	cfg.Port = client.port
	cfg.Status.StatusPort = client.statusPort
	cfg.Status.ReportStatus = true

	server, err := NewServer(cfg, driver)
	server.SetDomain(dom)
	require.NoError(t, err)
	go func() {
		err := server.Run()
		require.NoError(t, err)
	}()

	client.port = getPortFromTCPAddr(server.listener.Addr())
	client.statusPort = getPortFromTCPAddr(server.statusListener.Addr())
	client.waitUntilServerOnline()
	return
}

func TestDumpPlanReplayerAPI(t *testing.T) {
	store := testkit.CreateMockStore(t)
	dom, err := session.GetDomain(store)
	require.NoError(t, err)
	server, client := prepareServerAndClientForTest(t, store, dom)
	defer server.Close()
	statsHandle := dom.StatsHandle()

	filename := prepareData4PlanReplayer(t, client, statsHandle)

	resp0, err := client.fetchStatus(filepath.Join("/plan_replayer/dump/", filename))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, resp0.Body.Close())
	}()

	body, err := io.ReadAll(resp0.Body)
	require.NoError(t, err)

	path := "/tmp/plan_replayer.zip"
	fp, err := os.Create(path)
	require.NoError(t, err)
	require.NotNil(t, fp)
	defer func() {
		require.NoError(t, fp.Close())
		require.NoError(t, os.Remove(path))
	}()

	_, err = io.Copy(fp, bytes.NewReader(body))
	require.NoError(t, err)
	require.NoError(t, fp.Sync())

	db, err := sql.Open("mysql", client.getDSN(func(config *mysql.Config) {
		config.AllowAllFiles = true
	}))
	require.NoError(t, err, "Error connecting")
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()
	tk := testkit.NewDBTestKit(t, db)

	tk.MustExec("use planReplayer")
	tk.MustExec("drop table planReplayer.t")
	tk.MustExec(`plan replayer load "/tmp/plan_replayer.zip"`)
	rows := tk.MustQuery("show stats_meta")
	require.True(t, rows.Next(), "unexpected data")
	var dbName, tableName string
	var modifyCount, count int64
	var other interface{}
	err = rows.Scan(&dbName, &tableName, &other, &other, &modifyCount, &count)
	require.NoError(t, err)
	require.Equal(t, "planReplayer", dbName)
	require.Equal(t, "t", tableName)
	require.Equal(t, int64(4), modifyCount)
	require.Equal(t, int64(8), count)
}

func prepareData4PlanReplayer(t *testing.T, client *testServerClient, h *handle.Handle) string {
	db, err := sql.Open("mysql", client.getDSN())
	require.NoError(t, err, "Error connecting")
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()
	tk := testkit.NewDBTestKit(t, db)

	tk.MustExec("create database planReplayer")
	tk.MustExec("use planReplayer")
	tk.MustExec("create table t(a int)")
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)
	tk.MustExec("insert into t values(1), (2), (3), (4)")
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	tk.MustExec("analyze table t")
	tk.MustExec("insert into t values(5), (6), (7), (8)")
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	rows := tk.MustQuery("plan replayer dump explain select * from t")
	require.True(t, rows.Next(), "unexpected data")
	var filename string
	err = rows.Scan(&filename)
	require.NoError(t, err)
	rows.Close()
	rows = tk.MustQuery("select @@tidb_last_plan_replayer_token")
	require.True(t, rows.Next(), "unexpected data")
	var filename2 string
	err = rows.Scan(&filename2)
	require.NoError(t, err)
	rows.Close()
	require.Equal(t, filename, filename2)
	return filename
}

func getStatsAndMetaFromPlanReplayerAPI(
	t *testing.T,
	client *testServerClient,
	filename string,
) (
	jsonTbls []*handle.JSONTable,
	metas []map[string]string,
) {
	resp0, err := client.fetchStatus(filepath.Join("/plan_replayer/dump/", filename))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, resp0.Body.Close())
	}()
	body, err := io.ReadAll(resp0.Body)
	require.NoError(t, err)
	b := bytes.NewReader(body)
	z, err := zip.NewReader(b, int64(len(body)))
	require.NoError(t, err)

	for _, zipFile := range z.File {
		if strings.HasPrefix(zipFile.Name, "stats/") {
			jsonTbl := &handle.JSONTable{}
			r, err := zipFile.Open()
			require.NoError(t, err)
			//nolint: all_revive
			defer func() {
				require.NoError(t, r.Close())
			}()
			buf := new(bytes.Buffer)
			_, err = buf.ReadFrom(r)
			require.NoError(t, err)
			err = json.Unmarshal(buf.Bytes(), jsonTbl)
			require.NoError(t, err)

			jsonTbls = append(jsonTbls, jsonTbl)
		} else if zipFile.Name == "sql_meta.toml" {
			meta := make(map[string]string)
			r, err := zipFile.Open()
			require.NoError(t, err)
			//nolint: all_revive
			defer func() {
				require.NoError(t, r.Close())
			}()
			_, err = toml.NewDecoder(r).Decode(&meta)
			require.NoError(t, err)

			metas = append(metas, meta)
		}
	}
	return
}

func TestDumpPlanReplayerAPIWithHistoryStats(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/domain/sendHistoricalStats", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/domain/sendHistoricalStats"))
	}()
	store := testkit.CreateMockStore(t)
	dom, err := session.GetDomain(store)
	require.NoError(t, err)
	server, client := prepareServerAndClientForTest(t, store, dom)
	defer server.Close()
	statsHandle := dom.StatsHandle()
	hsWorker := dom.GetHistoricalStatsWorker()

	// 1. prepare test data

	// time1, ts1: before everything starts
	tk := testkit.NewTestKit(t, store)
	time1 := time.Now()
	ts1 := oracle.GoTimeToTS(time1)

	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, c int, index ia(a))")
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblInfo := tbl.Meta()

	// 1-1. first insert and first analyze, trigger first dump history stats
	tk.MustExec("insert into t value(1,1,1), (2,2,2), (3,3,3)")
	tk.MustExec("analyze table t with 1 samplerate")
	tblID := hsWorker.GetOneHistoricalStatsTable()
	err = hsWorker.DumpHistoricalStats(tblID, statsHandle)
	require.NoError(t, err)

	// time2, stats1: after first analyze
	time2 := time.Now()
	ts2 := oracle.GoTimeToTS(time2)
	stats1, err := statsHandle.DumpStatsToJSON("test", tblInfo, nil, true)
	require.NoError(t, err)

	// 1-2. second insert and second analyze, trigger second dump history stats
	tk.MustExec("insert into t value(4,4,4), (5,5,5), (6,6,6)")
	tk.MustExec("analyze table t with 1 samplerate")
	tblID = hsWorker.GetOneHistoricalStatsTable()
	err = hsWorker.DumpHistoricalStats(tblID, statsHandle)
	require.NoError(t, err)

	// time3, stats2: after second analyze
	time3 := time.Now()
	ts3 := oracle.GoTimeToTS(time3)
	stats2, err := statsHandle.DumpStatsToJSON("test", tblInfo, nil, true)
	require.NoError(t, err)

	// 2. get the plan replayer and assert

	template := "plan replayer dump with stats as of timestamp '%s' explain %s"
	query := "select * from t where a > 1"

	// 2-1. specify time1 to get the plan replayer
	filename1 := tk.MustQuery(
		fmt.Sprintf(template, strconv.FormatUint(ts1, 10), query),
	).Rows()[0][0].(string)
	jsonTbls1, metas1 := getStatsAndMetaFromPlanReplayerAPI(t, client, filename1)

	// the TS is recorded in the plan replayer, and it's the same as the TS we calculated above
	require.Len(t, metas1, 1)
	require.Contains(t, metas1[0], "historicalStatsTS")
	tsInReplayerMeta1, err := strconv.ParseUint(metas1[0]["historicalStatsTS"], 10, 64)
	require.NoError(t, err)
	require.Equal(t, ts1, tsInReplayerMeta1)

	// the result is the same as stats2, and IsHistoricalStats is false.
	require.Len(t, jsonTbls1, 1)
	require.False(t, jsonTbls1[0].IsHistoricalStats)
	require.Equal(t, jsonTbls1[0], stats2)

	// 2-2. specify time2 to get the plan replayer
	filename2 := tk.MustQuery(
		fmt.Sprintf(template, time2.Format("2006-01-02 15:04:05.000000"), query),
	).Rows()[0][0].(string)
	jsonTbls2, metas2 := getStatsAndMetaFromPlanReplayerAPI(t, client, filename2)

	// the TS is recorded in the plan replayer, and it's the same as the TS we calculated above
	require.Len(t, metas2, 1)
	require.Contains(t, metas2[0], "historicalStatsTS")
	tsInReplayerMeta2, err := strconv.ParseUint(metas2[0]["historicalStatsTS"], 10, 64)
	require.NoError(t, err)
	require.Equal(t, ts2, tsInReplayerMeta2)

	// the result is the same as stats1, and IsHistoricalStats is true.
	require.Len(t, jsonTbls2, 1)
	require.True(t, jsonTbls2[0].IsHistoricalStats)
	jsonTbls2[0].IsHistoricalStats = false
	require.Equal(t, jsonTbls2[0], stats1)

	// 2-3. specify time3 to get the plan replayer
	filename3 := tk.MustQuery(
		fmt.Sprintf(template, time3.Format("2006-01-02T15:04:05.000000Z07:00"), query),
	).Rows()[0][0].(string)
	jsonTbls3, metas3 := getStatsAndMetaFromPlanReplayerAPI(t, client, filename3)

	// the TS is recorded in the plan replayer, and it's the same as the TS we calculated above
	require.Len(t, metas3, 1)
	require.Contains(t, metas3[0], "historicalStatsTS")
	tsInReplayerMeta3, err := strconv.ParseUint(metas3[0]["historicalStatsTS"], 10, 64)
	require.NoError(t, err)
	require.Equal(t, ts3, tsInReplayerMeta3)

	// the result is the same as stats2, and IsHistoricalStats is true.
	require.Len(t, jsonTbls3, 1)
	require.True(t, jsonTbls3[0].IsHistoricalStats)
	jsonTbls3[0].IsHistoricalStats = false
	require.Equal(t, jsonTbls3[0], stats2)

	// 3. remove the plan replayer files generated during the test
	gcHandler := dom.GetDumpFileGCChecker()
	gcHandler.GCDumpFiles(0, 0)
}
