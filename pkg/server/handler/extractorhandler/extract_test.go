// Copyright 2023 PingCAP, Inc.
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

package extractorhandler_test

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	server2 "github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/server/handler/extractorhandler"
	"github.com/pingcap/tidb/pkg/server/internal/testserverclient"
	"github.com/pingcap/tidb/pkg/server/internal/testutil"
	"github.com/pingcap/tidb/pkg/server/internal/util"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	stmtsummaryv2 "github.com/pingcap/tidb/pkg/util/stmtsummary/v2"
	"github.com/stretchr/testify/require"
)

func TestExtractHandler(t *testing.T) {
	setupStmtSummary()
	defer closeStmtSummary()

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
	startTime := time.Now()
	time.Sleep(time.Second)
	prepareData4ExtractPlanTask(t, client)
	time.Sleep(time.Second)
	endTime := time.Now()
	eh := &extractorhandler.ExtractTaskServeHandler{ExtractHandler: dom.GetExtractHandle()}
	router := mux.NewRouter()
	router.Handle("/extract_task/dump", eh)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/server/extractTaskServeHandler", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/server/extractTaskServeHandler"))
	}()
	resp0, err := client.FetchStatus(fmt.Sprintf("/extract_task/dump?type=plan&begin=%s&end=%s",
		url.QueryEscape(startTime.Format(types.TimeFormat)), url.QueryEscape(endTime.Format(types.TimeFormat))))
	defer os.RemoveAll(domain.GetExtractTaskDirName())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, resp0.Body.Close())
	}()
	require.Equal(t, resp0.StatusCode, http.StatusOK)
	resp0, err = client.FetchStatus("/extract_task/dump?type=plan")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, resp0.Body.Close())
	}()
	require.Equal(t, resp0.StatusCode, http.StatusOK)
}

func prepareData4ExtractPlanTask(t *testing.T, client *testserverclient.TestServerClient) {
	db, err := sql.Open("mysql", client.GetDSN())
	require.NoError(t, err, "Error connecting")
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()
	tk := testkit.NewDBTestKit(t, db)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int)")
	tk.MustExec("select * from t")
}

func setupStmtSummary() {
	stmtsummaryv2.Setup(&stmtsummaryv2.Config{
		Filename: "tidb-statements.log",
	})
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Instance.StmtSummaryEnablePersistent = true
	})
}

func closeStmtSummary() {
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Instance.StmtSummaryEnablePersistent = false
	})
	stmtsummaryv2.GlobalStmtSummary.Close()
	stmtsummaryv2.GlobalStmtSummary = nil
	_ = os.Remove(config.GetGlobalConfig().Instance.StmtSummaryFilename)
}
