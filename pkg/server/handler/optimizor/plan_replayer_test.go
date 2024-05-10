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

package optimizor_test

import (
	"archive/zip"
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/server/internal/testserverclient"
	"github.com/pingcap/tidb/pkg/server/internal/testutil"
	"github.com/pingcap/tidb/pkg/server/internal/util"
	"github.com/pingcap/tidb/pkg/session"
	util2 "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/replayer"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

var expectedFilesInReplayer = []string{
	"config.toml",
	"debug_trace/debug_trace0.json",
	"explain.txt",
	"global_bindings.sql",
	"meta.txt",
	"schema/planreplayer.t.schema.txt",
	"schema/schema_meta.txt",
	"session_bindings.sql",
	"sql/sql0.sql",
	"sql_meta.toml",
	"stats/planreplayer.t.json",
	"statsMem/planreplayer.t.txt",
	"table_tiflash_replica.txt",
	"variables.toml",
}

var expectedFilesInReplayerForCapture = []string{
	"config.toml",
	"debug_trace/debug_trace0.json",
	"explain/sql.txt",
	"global_bindings.sql",
	"meta.txt",
	"schema/planreplayer.t.schema.txt",
	"schema/schema_meta.txt",
	"session_bindings.sql",
	"sql/sql0.sql",
	"sql_meta.toml",
	"stats/planreplayer.t.json",
	"statsMem/planreplayer.t.txt",
	"table_tiflash_replica.txt",
	"variables.toml",
}

func prepareServerAndClientForTest(t *testing.T, store kv.Storage, dom *domain.Domain) (srv *server.Server, client *testserverclient.TestServerClient) {
	driver := server.NewTiDBDriver(store)
	client = testserverclient.NewTestServerClient()

	cfg := util.NewTestConfig()
	cfg.Port = client.Port
	cfg.Status.StatusPort = client.StatusPort
	cfg.Status.ReportStatus = true

	srv, err := server.NewServer(cfg, driver)
	srv.SetDomain(dom)
	require.NoError(t, err)
	go func() {
		err := srv.Run(nil)
		require.NoError(t, err)
	}()
	<-server.RunInGoTestChan
	client.Port = testutil.GetPortFromTCPAddr(srv.ListenAddr())
	client.StatusPort = testutil.GetPortFromTCPAddr(srv.StatusListenerAddr())
	client.WaitUntilServerOnline()
	return
}

func TestDumpPlanReplayerAPI(t *testing.T) {
	store := testkit.CreateMockStore(t)
	dom, err := session.GetDomain(store)
	require.NoError(t, err)
	// 1. setup and prepare plan replayer files by manual command and capture
	server, client := prepareServerAndClientForTest(t, store, dom)
	defer server.Close()

	filename, fileNameFromCapture := prepareData4PlanReplayer(t, client, dom)
	defer os.RemoveAll(replayer.GetPlanReplayerDirName())

	// 2. check the contents of the plan replayer zip files.

	var filesInReplayer []string
	collectFileNameAndAssertFileSize := func(f *zip.File) {
		// collect file name
		filesInReplayer = append(filesInReplayer, f.Name)
		// except for {global,session}_bindings.sql and table_tiflash_replica.txt, the file should not be empty
		if !strings.Contains(f.Name, "table_tiflash_replica.txt") &&
			!strings.Contains(f.Name, "bindings.sql") {
			require.NotZero(t, f.UncompressedSize64, f.Name)
		}
	}

	// 2-1. check the plan replayer file from manual command
	resp0, err := client.FetchStatus(filepath.Join("/plan_replayer/dump/", filename))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, resp0.Body.Close())
	}()
	body, err := io.ReadAll(resp0.Body)
	require.NoError(t, err)
	forEachFileInZipBytes(t, body, collectFileNameAndAssertFileSize)
	slices.Sort(filesInReplayer)
	require.Equal(t, expectedFilesInReplayer, filesInReplayer)

	// 2-2. check the plan replayer file from capture
	resp1, err := client.FetchStatus(filepath.Join("/plan_replayer/dump/", fileNameFromCapture))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, resp1.Body.Close())
	}()
	body, err = io.ReadAll(resp1.Body)
	require.NoError(t, err)
	filesInReplayer = filesInReplayer[:0]
	forEachFileInZipBytes(t, body, collectFileNameAndAssertFileSize)
	slices.Sort(filesInReplayer)
	require.Equal(t, expectedFilesInReplayerForCapture, filesInReplayer)

	// 3. check plan replayer load

	// 3-1. write the plan replayer file from manual command to a file
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

	// 3-2. connect to tidb and use PLAN REPLAYER LOAD to load this file
	db, err := sql.Open("mysql", client.GetDSN(func(config *mysql.Config) {
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

	// 3-3. assert that the count and modify count in the stats is as expected
	rows := tk.MustQuery("show stats_meta")
	require.True(t, rows.Next(), "unexpected data")
	var dbName, tableName string
	var modifyCount, count int64
	var other any
	err = rows.Scan(&dbName, &tableName, &other, &other, &modifyCount, &count, &other)
	require.NoError(t, err)
	require.Equal(t, "planReplayer", dbName)
	require.Equal(t, "t", tableName)
	require.Equal(t, int64(4), modifyCount)
	require.Equal(t, int64(8), count)
}

// prepareData4PlanReplayer trigger tidb to dump 2 plan replayer files,
// one by manual command, the other by capture, and return the filenames.
func prepareData4PlanReplayer(t *testing.T, client *testserverclient.TestServerClient, dom *domain.Domain) (string, string) {
	h := dom.StatsHandle()
	replayerHandle := dom.GetPlanReplayerHandle()
	db, err := sql.Open("mysql", client.GetDSN())
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
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	tk.MustExec("analyze table t")
	tk.MustExec("insert into t values(5), (6), (7), (8)")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	rows := tk.MustQuery("plan replayer dump explain select * from t")
	require.True(t, rows.Next(), "unexpected data")
	var filename string
	require.NoError(t, rows.Scan(&filename))
	require.NoError(t, rows.Close())
	rows = tk.MustQuery("select @@tidb_last_plan_replayer_token")
	require.True(t, rows.Next(), "unexpected data")
	var filename2 string
	require.NoError(t, rows.Scan(&filename2))
	require.NoError(t, rows.Close())
	require.Equal(t, filename, filename2)

	tk.MustExec("plan replayer capture 'e5796985ccafe2f71126ed6c0ac939ffa015a8c0744a24b7aee6d587103fd2f7' '*'")
	tk.MustQuery("select * from t")
	task := replayerHandle.DrainTask()
	require.NotNil(t, task)
	worker := replayerHandle.GetWorker()
	require.True(t, worker.HandleTask(task))
	rows = tk.MustQuery("select token from mysql.plan_replayer_status where length(sql_digest) > 0")
	require.True(t, rows.Next(), "unexpected data")
	var filename3 string
	require.NoError(t, rows.Scan(&filename3))
	require.NoError(t, rows.Close())

	return filename, filename3
}

func TestIssue43192(t *testing.T) {
	store := testkit.CreateMockStore(t)
	dom, err := session.GetDomain(store)
	require.NoError(t, err)
	// 1. setup and prepare plan replayer files by manual command and capture
	server, client := prepareServerAndClientForTest(t, store, dom)
	defer server.Close()

	filename := prepareData4Issue43192(t, client, dom)
	defer os.RemoveAll(replayer.GetPlanReplayerDirName())

	// 2. check the contents of the plan replayer zip files.
	var filesInReplayer []string
	collectFileNameAndAssertFileSize := func(f *zip.File) {
		// collect file name
		filesInReplayer = append(filesInReplayer, f.Name)
	}

	// 2-1. check the plan replayer file from manual command
	resp0, err := client.FetchStatus(filepath.Join("/plan_replayer/dump/", filename))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, resp0.Body.Close())
	}()
	body, err := io.ReadAll(resp0.Body)
	require.NoError(t, err)
	forEachFileInZipBytes(t, body, collectFileNameAndAssertFileSize)
	slices.Sort(filesInReplayer)
	require.Equal(t, expectedFilesInReplayer, filesInReplayer)

	// 3. check plan replayer load
	// 3-1. write the plan replayer file from manual command to a file
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

	// 3-2. connect to tidb and use PLAN REPLAYER LOAD to load this file
	db, err := sql.Open("mysql", client.GetDSN(func(config *mysql.Config) {
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

	// 3-3. check whether binding takes effect
	tk.MustExec(`select a, b from t where a in (1, 2, 3)`)
	rows := tk.MustQuery("select @@last_plan_from_binding")
	require.True(t, rows.Next(), "unexpected data")
	var count int64
	err = rows.Scan(&count)
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
}

func prepareData4Issue43192(t *testing.T, client *testserverclient.TestServerClient, dom *domain.Domain) string {
	h := dom.StatsHandle()
	db, err := sql.Open("mysql", client.GetDSN())
	require.NoError(t, err, "Error connecting")
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()
	tk := testkit.NewDBTestKit(t, db)

	tk.MustExec("create database planReplayer")
	tk.MustExec("use planReplayer")
	tk.MustExec("create table t(a int, b int, INDEX ia (a), INDEX ib (b));")
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)
	tk.MustExec("create global binding for select a, b from t where a in (1, 2, 3) using select a, b from t use index (ib) where a in (1, 2, 3)")
	rows := tk.MustQuery("plan replayer dump explain select a, b from t where a in (1, 2, 3)")
	require.True(t, rows.Next(), "unexpected data")
	var filename string
	require.NoError(t, rows.Scan(&filename))
	require.NoError(t, rows.Close())
	rows = tk.MustQuery("select @@tidb_last_plan_replayer_token")
	require.True(t, rows.Next(), "unexpected data")
	return filename
}

func forEachFileInZipBytes(t *testing.T, b []byte, fn func(file *zip.File)) {
	br := bytes.NewReader(b)
	z, err := zip.NewReader(br, int64(len(b)))
	require.NoError(t, err)
	for _, f := range z.File {
		fn(f)
	}
}

func fetchZipFromPlanReplayerAPI(t *testing.T, client *testserverclient.TestServerClient, filename string) *zip.Reader {
	resp0, err := client.FetchStatus(filepath.Join("/plan_replayer/dump/", filename))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, resp0.Body.Close())
	}()
	body, err := io.ReadAll(resp0.Body)
	require.NoError(t, err)
	b := bytes.NewReader(body)
	z, err := zip.NewReader(b, int64(len(body)))
	require.NoError(t, err)
	return z
}

func getInfoFromPlanReplayerZip(
	t *testing.T,
	z *zip.Reader,
) (
	jsonTbls []*util2.JSONTable,
	metas []map[string]string,
	errMsgs []string,
) {
	for _, zipFile := range z.File {
		if strings.HasPrefix(zipFile.Name, "stats/") {
			jsonTbl := &util2.JSONTable{}
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
		} else if zipFile.Name == "errors.txt" {
			r, err := zipFile.Open()
			require.NoError(t, err)
			//nolint: all_revive
			defer func() {
				require.NoError(t, r.Close())
			}()
			content, err := io.ReadAll(r)
			require.NoError(t, err)
			errMsgs = strings.Split(string(content), "\n")
		}
	}
	return
}

func TestDumpPlanReplayerAPIWithHistoryStats(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/domain/sendHistoricalStats", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/domain/sendHistoricalStats"))
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
	tk.MustExec("set global tidb_enable_historical_stats = 1")
	defer tk.MustExec("set global tidb_enable_historical_stats = 0")
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
	zip1 := fetchZipFromPlanReplayerAPI(t, client, filename1)
	jsonTbls1, metas1, errMsg1 := getInfoFromPlanReplayerZip(t, zip1)

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

	// because we failed to get historical stats, there's an error message.
	require.Equal(t, []string{"Historical stats for test.t are unavailable, fallback to latest stats", ""}, errMsg1)

	// 2-2. specify time2 to get the plan replayer
	filename2 := tk.MustQuery(
		fmt.Sprintf(template, time2.Format("2006-01-02 15:04:05.000000"), query),
	).Rows()[0][0].(string)
	zip2 := fetchZipFromPlanReplayerAPI(t, client, filename2)
	jsonTbls2, metas2, errMsg2 := getInfoFromPlanReplayerZip(t, zip2)

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

	// succeeded to get historical stats, there should be no error message.
	require.Empty(t, errMsg2)

	// 2-3. specify time3 to get the plan replayer
	filename3 := tk.MustQuery(
		fmt.Sprintf(template, time3.Format("2006-01-02T15:04:05.000000Z07:00"), query),
	).Rows()[0][0].(string)
	zip3 := fetchZipFromPlanReplayerAPI(t, client, filename3)
	jsonTbls3, metas3, errMsg3 := getInfoFromPlanReplayerZip(t, zip3)

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

	// succeeded to get historical stats, there should be no error message.
	require.Empty(t, errMsg3)

	// 3. remove the plan replayer files generated during the test
	gcHandler := dom.GetDumpFileGCChecker()
	gcHandler.GCDumpFiles(0, 0)
}
