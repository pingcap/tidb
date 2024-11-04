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
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/replayer"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
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

func TestDumpPlanReplayerAPI(t *testing.T) {
	store := testkit.CreateMockStore(t)

	// 1. setup and prepare plan replayer files by manual command and capture
	driver := NewTiDBDriver(store)
	client := newTestServerClient()
	cfg := newTestConfig()
	cfg.Port = client.port
	cfg.Status.StatusPort = client.statusPort
	cfg.Status.ReportStatus = true
	RunInGoTestChan = make(chan struct{})
	server, err := NewServer(cfg, driver)
	require.NoError(t, err)
	defer server.Close()

	dom, err := session.GetDomain(store)
	require.NoError(t, err)
	server.SetDomain(dom)
	go func() {
		err := server.Run(nil)
		require.NoError(t, err)
	}()
	<-RunInGoTestChan
	client.port = getPortFromTCPAddr(server.listener.Addr())
	client.statusPort = getPortFromTCPAddr(server.statusListener.Addr())
	client.waitUntilServerOnline()
	filename, fileNameFromCapture := prepareData4PlanReplayer(t, client, dom)

	router := mux.NewRouter()
	planReplayerHandler := &PlanReplayerHandler{}
	router.Handle("/plan_replayer/dump/{filename}", planReplayerHandler)

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
	resp0, err := client.fetchStatus(filepath.Join("/plan_replayer/dump/", filename))
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
	resp1, err := client.fetchStatus(filepath.Join("/plan_replayer/dump/", fileNameFromCapture))
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

	// 3-3. assert that the count and modify count in the stats is as expected
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

// prepareData4PlanReplayer trigger tidb to dump 2 plan replayer files,
// one by manual command, the other by capture, and return the filenames.
func prepareData4PlanReplayer(t *testing.T, client *testServerClient, dom *domain.Domain) (string, string) {
	h := dom.StatsHandle()
	replayerHandle := dom.GetPlanReplayerHandle()
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
	tk.MustExec("CREATE TABLE authors (id INT PRIMARY KEY AUTO_INCREMENT,name VARCHAR(100) NOT NULL,email VARCHAR(100) UNIQUE NOT NULL);")
	tk.MustExec("CREATE TABLE books (id INT PRIMARY KEY AUTO_INCREMENT,title VARCHAR(200) NOT NULL,publication_date DATE NOT NULL,author_id INT,FOREIGN KEY (author_id) REFERENCES authors(id) ON DELETE CASCADE);")
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

func forEachFileInZipBytes(t *testing.T, b []byte, fn func(file *zip.File)) {
	br := bytes.NewReader(b)
	z, err := zip.NewReader(br, int64(len(b)))
	require.NoError(t, err)
	for _, f := range z.File {
		fn(f)
	}
}

func prepareData4Issue56458(t *testing.T, client *testServerClient, dom *domain.Domain) string {
	h := dom.StatsHandle()
	db, err := sql.Open("mysql", client.getDSN())
	require.NoError(t, err, "Error connecting")
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()
	tk := testkit.NewDBTestKit(t, db)

	tk.MustExec("create database planReplayer")
	tk.MustExec("create database planReplayer2")
	tk.MustExec("use planReplayer")
	tk.MustExec("create placement policy p " +
		"LEARNERS=1 " +
		"LEARNER_CONSTRAINTS=\"[+region=cn-west-1]\" " +
		"FOLLOWERS=3 " +
		"FOLLOWER_CONSTRAINTS=\"[+disk=ssd]\"")
	tk.MustExec("CREATE TABLE v(id INT PRIMARY KEY AUTO_INCREMENT);")
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)
	tk.MustExec("create table planReplayer2.t(a int, b int, INDEX ia (a), INDEX ib (b), author_id int, FOREIGN KEY (author_id) REFERENCES planReplayer.v(id) ON DELETE CASCADE);")
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)
	tk.MustExec("create table t(a int, b int, INDEX ia (a), INDEX ib (b), author_id int, FOREIGN KEY (author_id) REFERENCES planReplayer2.t(a) ON DELETE CASCADE) placement policy p;")
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

func prepareServerAndClientForTest(t *testing.T, store kv.Storage, dom *domain.Domain) (srv *Server, client *testServerClient) {
	driver := NewTiDBDriver(store)
	client = newTestServerClient()

	cfg := newTestConfig()
	cfg.Port = client.port
	cfg.Status.StatusPort = client.statusPort
	cfg.Status.ReportStatus = true

	srv, err := NewServer(cfg, driver)
	srv.SetDomain(dom)
	require.NoError(t, err)
	go func() {
		err := srv.Run(nil)
		require.NoError(t, err)
	}()
	<-RunInGoTestChan
	client.port = getPortFromTCPAddr(srv.listener.Addr())
	client.statusPort = getPortFromTCPAddr(srv.statusListener.Addr())
	client.waitUntilServerOnline()
	return
}

func TestPlanReplayerWithMultiForeignKey(t *testing.T) {
	store := testkit.CreateMockStore(t)
	dom, err := session.GetDomain(store)
	require.NoError(t, err)
	// 1. setup and prepare plan replayer files by manual command and capture
	server, client := prepareServerAndClientForTest(t, store, dom)
	defer server.Close()

	filename := prepareData4Issue56458(t, client, dom)
	defer os.RemoveAll(replayer.GetPlanReplayerDirName())

	// 2. check the contents of the plan replayer zip files.
	var filesInReplayer []string
	collectFileNameAndAssertFileSize := func(f *zip.File) {
		// collect file name
		filesInReplayer = append(filesInReplayer, f.Name)
	}

	// 2-1. check the plan replayer file from manual command
	resp0, err := client.fetchStatus(filepath.Join("/plan_replayer/dump/", filename))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, resp0.Body.Close())
	}()
	body, err := io.ReadAll(resp0.Body)
	require.NoError(t, err)
	forEachFileInZipBytes(t, body, collectFileNameAndAssertFileSize)
	slices.Sort(filesInReplayer)
	require.Equal(t, []string{
		"config.toml",
		"debug_trace/debug_trace0.json",
		"explain.txt",
		"global_bindings.sql",
		"meta.txt",
		"schema/planreplayer.t.schema.txt",
		"schema/planreplayer.v.schema.txt",
		"schema/planreplayer2.t.schema.txt",
		"schema/schema_meta.txt",
		"session_bindings.sql",
		"sql/sql0.sql",
		"sql_meta.toml",
		"stats/planreplayer.t.json",
		"stats/planreplayer.v.json",
		"stats/planreplayer2.t.json",
		"statsMem/planreplayer.t.txt",
		"statsMem/planreplayer.v.txt",
		"statsMem/planreplayer2.t.txt",
		"table_tiflash_replica.txt",
		"variables.toml",
	}, filesInReplayer)

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
	tk.MustExec("drop table planReplayer2.t")
	tk.MustExec("drop table planReplayer.v")
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
