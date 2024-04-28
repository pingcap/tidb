// Copyright 2015 PingCAP, Inc.
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
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/server/internal"
	"github.com/pingcap/tidb/pkg/server/internal/testutil"
	"github.com/pingcap/tidb/pkg/server/internal/util"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/util/arena"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/replayer"
	"github.com/stretchr/testify/require"
)

// cmd: go test -run=^TestOptimizerDebugTrace$ --tags=intest github.com/pingcap/tidb/pkg/server
// If you want to update the test result, please run the following command:
// cmd: go test -run=^TestOptimizerDebugTrace$ --tags=intest github.com/pingcap/tidb/pkg/server --record
func TestOptimizerDebugTrace(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/planner/SetBindingTimeToZero", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/planner/core/DebugTraceStableStatsTbl", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/planner/SetBindingTimeToZero"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/planner/core/DebugTraceStableStatsTbl"))
	}()
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tidbdrv := NewTiDBDriver(store)
	cfg := util.NewTestConfig()
	cfg.Port, cfg.Status.StatusPort = 0, 0
	cfg.Status.ReportStatus = false
	server, err := NewServer(cfg, tidbdrv)
	require.NoError(t, err)
	defer server.Close()
	cc := &clientConn{
		server:     server,
		alloc:      arena.NewAllocator(1024),
		chunkAlloc: chunk.NewAllocator(),
		pkt:        internal.NewPacketIOForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
	}
	ctx := context.Background()
	cc.SetCtx(&TiDBContext{Session: tk.Session(), stmts: make(map[int]*TiDBStatement)})

	tk.MustExec("use test")
	tk.MustExec("create table t (col1 int, index i(col1))")
	h := dom.StatsHandle()
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))

	tk.MustExec("plan replayer capture '0595c79f25d183319d0830ff8ca538c9054cbf407e5e27488b5dc40e4738a7c8' '*'")
	tk.MustExec("plan replayer capture 'c0fcc0abbaaffcaafe21115a3c67ae5d96a188cc197559953d2865ea6852d3cc' '*'")
	tk.MustExec("plan replayer capture '58fcbdd56a722c02225488c89a782cd2d626f8219c8ef8f57cd3bcdb6eb7c1b2' '*'")
	require.NoError(t, cc.HandleStmtPrepare(ctx, "select sum(col1) from t where col1 < ? and col1 > 100"))
	tk.MustExec("prepare stmt from 'select * from t where col1 in (?, 2, 3)'")
	tk.MustExec("set @a = 1")
	var (
		in  []string
		out []any
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
		var res any
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

func TestIssue46197(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tidbdrv := NewTiDBDriver(store)
	cfg := util.NewTestConfig()
	cfg.Port, cfg.Status.StatusPort = 0, 0
	cfg.Status.ReportStatus = false
	server, err := NewServer(cfg, tidbdrv)
	require.NoError(t, err)
	defer server.Close()

	// Mock the content of the SQL file in PacketIO buffer.
	// First 4 bytes are the header, followed by the actual content.
	// This acts like we are sending "select * from t1;" from the client when tidb requests the "a.txt" file.
	var inBuffer bytes.Buffer
	_, err = inBuffer.Write([]byte{0x11, 0x00, 0x00, 0x01})
	require.NoError(t, err)
	_, err = inBuffer.Write([]byte("select * from t1;"))
	require.NoError(t, err)

	// clientConn setup
	brc := util.NewBufferedReadConn(&testutil.BytesConn{Buffer: inBuffer})
	pkt := internal.NewPacketIO(brc)
	pkt.SetBufWriter(bufio.NewWriter(bytes.NewBuffer(nil)))
	cc := &clientConn{
		server:     server,
		alloc:      arena.NewAllocator(1024),
		chunkAlloc: chunk.NewAllocator(),
		pkt:        pkt,
		capability: mysql.ClientLocalFiles,
	}
	ctx := context.Background()
	cc.SetCtx(&TiDBContext{Session: tk.Session(), stmts: make(map[int]*TiDBStatement)})

	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int, b int)")

	// 3 is mysql.ComQuery, followed by the SQL text.
	require.NoError(t, cc.dispatch(ctx, []byte("\u0003plan replayer dump explain 'a.txt'")))

	// clean up
	path := testdata.ConvertRowsToStrings(tk.MustQuery("select @@tidb_last_plan_replayer_token").Rows())
	require.NoError(t, os.Remove(filepath.Join(replayer.GetPlanReplayerDirName(), path[0])))
}

func TestGetConAttrs(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tidbdrv := NewTiDBDriver(store)
	cfg := util.NewTestConfig()
	cfg.Port, cfg.Status.StatusPort = 0, 0
	cfg.Status.ReportStatus = false
	server, err := NewServer(cfg, tidbdrv)
	require.NoError(t, err)

	cc := &clientConn{
		server:     server,
		alloc:      arena.NewAllocator(1024),
		chunkAlloc: chunk.NewAllocator(),
		pkt:        internal.NewPacketIOForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
		attrs: map[string]string{
			"_client_name": "tidb_test",
		},
		user:     "userA",
		peerHost: "foo.example.com",
	}
	cc.SetCtx(&TiDBContext{Session: tk.Session(), stmts: make(map[int]*TiDBStatement)})
	server.registerConn(cc)

	// Get attributes for all clients
	attrs := server.GetConAttrs(nil)
	require.Equal(t, attrs[1]["_client_name"], "tidb_test")

	// Get attributes for userA@foo.example.com, which should have results for connID 1.
	userA := &auth.UserIdentity{Username: "userA", Hostname: "foo.example.com"}
	attrs = server.GetConAttrs(userA)
	require.Equal(t, attrs[1]["_client_name"], "tidb_test")
	_, hasClientName := attrs[1]
	require.True(t, hasClientName)

	// Get attributes for userB@foo.example.com, which should NOT have results for connID 1.
	userB := &auth.UserIdentity{Username: "userB", Hostname: "foo.example.com"}
	attrs = server.GetConAttrs(userB)
	_, hasClientName = attrs[1]
	require.False(t, hasClientName)
}

func TestSeverHealth(t *testing.T) {
	RunInGoTestChan = make(chan struct{})
	RunInGoTest = true
	store := testkit.CreateMockStore(t)
	tidbdrv := NewTiDBDriver(store)
	cfg := util.NewTestConfig()
	cfg.Port, cfg.Status.StatusPort = 0, 0
	cfg.Status.ReportStatus = false
	server, err := NewServer(cfg, tidbdrv)
	require.NoError(t, err)
	require.False(t, server.health.Load(), "server should not be healthy")
	go func() {
		err = server.Run(nil)
		require.NoError(t, err)
	}()
	defer server.Close()
	for range RunInGoTestChan {
		// wait for server to be healthy
	}
	require.True(t, server.health.Load(), "server should be healthy")
}
