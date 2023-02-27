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

package server

import (
	"bytes"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	stmtsummaryv2 "github.com/pingcap/tidb/util/stmtsummary/v2"
	"github.com/stretchr/testify/require"
)

func TestExtractHandler(t *testing.T) {
	setupStmtSummary()
	defer closeStmtSummary()

	store := testkit.CreateMockStore(t)

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
	startTime := time.Now()
	time.Sleep(time.Second)
	prepareData4ExtractPlanTask(t, client)
	time.Sleep(time.Second)
	endTime := time.Now()
	dom, err := session.GetDomain(store)
	require.NoError(t, err)
	eh := &ExtractTaskServeHandler{extractHandler: dom.GetExtractHandle()}
	router := mux.NewRouter()
	router.Handle("/extract_task/dump", eh)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/server/extractTaskServeHandler", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/server/extractTaskServeHandler"))
	}()
	b := fmt.Sprintf(`{"type":"plan","begin":"%s","end":"%s"}`, startTime.Format(types.TimeFormat), endTime.Format(types.TimeFormat))
	resp0, err := client.postStatus("/extract_task/dump", "application/json", bytes.NewBuffer([]byte(b)))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, resp0.Body.Close())
	}()
	//body, err := io.ReadAll(resp0.Body)
	//require.NoError(t, err)
	//logutil.BgLogger().Info("extract response", zap.String("response", string(body)))
	require.Equal(t, resp0.StatusCode, http.StatusOK)
}

func prepareData4ExtractPlanTask(t *testing.T, client *testServerClient) {
	db, err := sql.Open("mysql", client.getDSN())
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
