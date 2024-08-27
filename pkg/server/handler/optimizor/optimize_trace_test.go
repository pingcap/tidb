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
	"database/sql"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/gorilla/mux"
	server2 "github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/server/handler/optimizor"
	"github.com/pingcap/tidb/pkg/server/internal/testserverclient"
	"github.com/pingcap/tidb/pkg/server/internal/testutil"
	"github.com/pingcap/tidb/pkg/server/internal/util"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestDumpOptimizeTraceAPI(t *testing.T) {
	store := testkit.CreateMockStore(t)

	driver := server2.NewTiDBDriver(store)
	client := testserverclient.NewTestServerClient()
	cfg := util.NewTestConfig()
	cfg.Port = client.Port
	cfg.Status.StatusPort = client.StatusPort
	cfg.Status.ReportStatus = true

	server, err := server2.NewServer(cfg, driver)
	require.NoError(t, err)
	defer server.Close()

	dom, err := session.GetDomain(store)
	require.NoError(t, err)
	server.SetDomain(dom)

	go func() {
		err := server.Run(nil)
		require.NoError(t, err)
	}()
	<-server2.RunInGoTestChan
	client.Port = testutil.GetPortFromTCPAddr(server.ListenAddr())
	client.StatusPort = testutil.GetPortFromTCPAddr(server.StatusListenerAddr())
	client.WaitUntilServerOnline()

	statsHandler := optimizor.NewStatsHandler(dom)

	otHandler := &optimizor.OptimizeTraceHandler{}
	filename := prepareData4OptimizeTrace(t, client, statsHandler)

	router := mux.NewRouter()
	router.Handle("/optimize_trace/dump/{filename}", otHandler)

	resp0, err := client.FetchStatus(filepath.Join("/optimize_trace/dump/", filename))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, resp0.Body.Close())
	}()
	require.Equal(t, http.StatusOK, resp0.StatusCode)
}

func prepareData4OptimizeTrace(t *testing.T, client *testserverclient.TestServerClient, statHandle *optimizor.StatsHandler) string {
	db, err := sql.Open("mysql", client.GetDSN())
	require.NoError(t, err, "Error connecting")
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()
	tk := testkit.NewDBTestKit(t, db)

	h := statHandle.Domain().StatsHandle()
	tk.MustExec("create database optimizeTrace")
	tk.MustExec("use optimizeTrace")
	tk.MustExec("create table t(a int)")
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)
	rows := tk.MustQuery("trace plan select * from t")
	require.True(t, rows.Next(), "unexpected data")
	var filename string
	err = rows.Scan(&filename)
	require.NoError(t, err)
	return filename
}
