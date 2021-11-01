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
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/gorilla/mux"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestDumpPlanReplayerAPI(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	driver := NewTiDBDriver(store)
	client := newTestServerClient()
	cfg := newTestConfig()
	cfg.Port = client.port
	cfg.Status.StatusPort = client.statusPort
	cfg.Status.ReportStatus = true

	server, err := NewServer(cfg, driver)
	require.NoError(t, err)
	defer server.Close()

	client.port = getPortFromTCPAddr(server.listener.Addr())
	client.statusPort = getPortFromTCPAddr(server.statusListener.Addr())
	go func() {
		err := server.Run()
		require.NoError(t, err)
	}()
	client.waitUntilServerOnline()

	planReplayerHandler := &PlanReplayerHandler{}
	filename := prepareData4PlanReplayer(t, client)

	router := mux.NewRouter()
	router.Handle("/plan_replayer/dump/{filename}", planReplayerHandler)

	resp0, err := client.fetchStatus(filepath.Join("/plan_replayer/dump/", filename))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, resp0.Body.Close())
	}()

	body, err := ioutil.ReadAll(resp0.Body)
	require.NoError(t, err)

	zipReader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err)

	require.Equal(t, len(zipReader.File), 9)
}

func prepareData4PlanReplayer(t *testing.T, client *testServerClient) string {
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
	tk.MustExec("insert into t values(1), (2), (3), (4)")
	tk.MustExec("analyze table t")
	rows := tk.MustQuery("plan replayer dump explain select * from t")
	require.True(t, rows.Next(), "unexpected data")
	var filename string
	err = rows.Scan(&filename)
	require.NoError(t, err)
	return filename
}
