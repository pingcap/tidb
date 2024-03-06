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
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/gorilla/mux"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/pingcap/tidb/util/arena"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/replayer"
	"github.com/stretchr/testify/require"
)

func TestDumpOptimizeTraceAPI(t *testing.T) {
	store := testkit.CreateMockStore(t)

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
	go func() {
		err := server.Run(nil)
		require.NoError(t, err)
	}()
	<-RunInGoTestChan
	client.port = getPortFromTCPAddr(server.listener.Addr())
	client.statusPort = getPortFromTCPAddr(server.statusListener.Addr())
	client.waitUntilServerOnline()

	dom, err := session.GetDomain(store)
	require.NoError(t, err)
	statsHandler := &StatsHandler{dom}

	otHandler := &OptimizeTraceHandler{}
	filename := prepareData4OptimizeTrace(t, client, statsHandler)

	router := mux.NewRouter()
	router.Handle("/optimize_trace/dump/{filename}", otHandler)

	resp0, err := client.fetchStatus(filepath.Join("/optimize_trace/dump/", filename))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, resp0.Body.Close())
	}()
	require.Equal(t, http.StatusOK, resp0.StatusCode)
}

func prepareData4OptimizeTrace(t *testing.T, client *testServerClient, statHandle *StatsHandler) string {
	db, err := sql.Open("mysql", client.getDSN())
	require.NoError(t, err, "Error connecting")
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()
	tk := testkit.NewDBTestKit(t, db)

	h := statHandle.do.StatsHandle()
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

func TestOptimizerDebugTrace(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/planner/SetBindingTimeToZero", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/planner/core/DebugTraceStableStatsTbl", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/planner/SetBindingTimeToZero"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/planner/core/DebugTraceStableStatsTbl"))
	}()
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tidbdrv := NewTiDBDriver(store)
	cfg := newTestConfig()
	cfg.Port, cfg.Status.StatusPort = 0, 0
	cfg.Status.ReportStatus = false
	server, err := NewServer(cfg, tidbdrv)
	require.NoError(t, err)
	defer server.Close()
	cc := &clientConn{
		server:     server,
		alloc:      arena.NewAllocator(1024),
		chunkAlloc: chunk.NewAllocator(),
		pkt: &packetIO{
			bufWriter: bufio.NewWriter(bytes.NewBuffer(nil)),
		},
	}
	ctx := context.Background()
	cc.setCtx(&TiDBContext{Session: tk.Session(), stmts: make(map[int]*TiDBStatement)})

	tk.MustExec("use test")
	tk.MustExec("create table t (col1 int, index i(col1))")
	h := dom.StatsHandle()
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))

	tk.MustExec("plan replayer capture '0595c79f25d183319d0830ff8ca538c9054cbf407e5e27488b5dc40e4738a7c8' '*'")
	tk.MustExec("plan replayer capture 'c0fcc0abbaaffcaafe21115a3c67ae5d96a188cc197559953d2865ea6852d3cc' '*'")
	tk.MustExec("plan replayer capture '58fcbdd56a722c02225488c89a782cd2d626f8219c8ef8f57cd3bcdb6eb7c1b2' '*'")
	require.NoError(t, cc.handleStmtPrepare(ctx, "select sum(col1) from t where col1 < ? and col1 > 100"))
	tk.MustExec("prepare stmt from 'select * from t where col1 in (?, 2, 3)'")
	tk.MustExec("set @a = 1")
	var (
		in  []string
		out []interface{}
	)
	optSuiteData := testDataMap["optimizer_suite"]
	optSuiteData.LoadTestCases(t, &in, &out)
	for i, cmdString := range in {
		require.NoError(t, cc.dispatch(ctx, []byte(cmdString)))
		traceInfo := cc.ctx.Session.GetSessionVars().StmtCtx.OptimizerDebugTrace
		var buf bytes.Buffer
		encoder := json.NewEncoder(&buf)
		encoder.SetEscapeHTML(false)
		require.NoError(t, encoder.Encode(traceInfo))
		var res interface{}
		require.NoError(t, json.Unmarshal(buf.Bytes(), &res))
		testdata.OnRecord(func() {
			out[i] = res
		})
		require.Equal(t, out[i], res, cmdString)
	}

	prHandle := dom.GetPlanReplayerHandle()
	worker := prHandle.GetWorker()
	for i := 0; i < 3; i++ {
		task := prHandle.DrainTask()
		success := worker.HandleTask(task)
		require.True(t, success)
		require.NoError(t, os.Remove(filepath.Join(replayer.GetPlanReplayerDirName(), task.FileName)))
	}
}
