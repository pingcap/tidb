// Copyright 2024 PingCAP, Inc.
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

package cursor

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	tmysql "github.com/pingcap/tidb/pkg/parser/mysql"
	server2 "github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestCursorFetchErrorInFetch(t *testing.T) {
	tmpStoragePath := t.TempDir()
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempStoragePath = tmpStoragePath
	})

	store, dom := testkit.CreateMockStoreAndDomain(t)
	srv := server2.CreateMockServer(t, store)
	srv.SetDomain(dom)
	defer srv.Close()

	appendUint32 := binary.LittleEndian.AppendUint32
	ctx := context.Background()
	c := server2.CreateMockConn(t, srv)

	tk := testkit.NewTestKitWithSession(t, store, c.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int, payload BLOB)")
	payload := make([]byte, 512)
	for i := 0; i < 2048; i++ {
		rand.Read(payload)
		tk.MustExec("insert into t values (?, ?)", i, payload)
	}

	tk.MustExec("set global tidb_enable_tmp_storage_on_oom = ON")
	tk.MustExec("set global tidb_mem_oom_action = 'CANCEL'")
	defer tk.MustExec("set global tidb_mem_oom_action= DEFAULT")
	require.Len(t, tk.Session().GetSessionVars().MemTracker.GetChildrenForTest(), 1)

	// execute a normal statement, it'll spill to disk
	stmt, _, _, err := c.Context().Prepare("select * from t")
	require.NoError(t, err)

	tk.MustExec(fmt.Sprintf("set tidb_mem_quota_query=%d", 1))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/chunk/get-chunk-error", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/util/chunk/get-chunk-error"))
	}()

	require.NoError(t, c.Dispatch(ctx, append(
		appendUint32([]byte{tmysql.ComStmtExecute}, uint32(stmt.ID())),
		tmysql.CursorTypeReadOnly, 0x1, 0x0, 0x0, 0x0,
	)))

	require.ErrorContains(t, c.Dispatch(ctx, appendUint32(appendUint32([]byte{tmysql.ComStmtFetch}, uint32(stmt.ID())), 1024)), "fail to get chunk for test")
	// after getting a failed FETCH, the cursor should have been reseted
	require.False(t, stmt.GetCursorActive())
	require.Len(t, tk.Session().GetSessionVars().MemTracker.GetChildrenForTest(), 0)
	require.Len(t, tk.Session().GetSessionVars().DiskTracker.GetChildrenForTest(), 0)
}

func TestCursorFetchShouldSpill(t *testing.T) {
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempStoragePath = t.TempDir()
	})

	store, dom := testkit.CreateMockStoreAndDomain(t)
	srv := server2.CreateMockServer(t, store)
	srv.SetDomain(dom)
	defer srv.Close()

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/server/testCursorFetchSpill", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/server/testCursorFetchSpill"))
	}()

	appendUint32 := binary.LittleEndian.AppendUint32
	ctx := context.Background()
	c := server2.CreateMockConn(t, srv)

	tk := testkit.NewTestKitWithSession(t, store, c.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id_1 int, id_2 int)")
	tk.MustExec("insert into t values (1, 1), (1, 2)")
	tk.MustExec("set global tidb_enable_tmp_storage_on_oom = ON")
	tk.MustExec("set global tidb_mem_oom_action = 'CANCEL'")
	defer tk.MustExec("set global tidb_mem_oom_action= DEFAULT")
	require.Len(t, tk.Session().GetSessionVars().MemTracker.GetChildrenForTest(), 1)

	// execute a normal statement, it'll spill to disk
	stmt, _, _, err := c.Context().Prepare("select * from t")
	require.NoError(t, err)

	tk.MustExec(fmt.Sprintf("set tidb_mem_quota_query=%d", 1))

	require.NoError(t, c.Dispatch(ctx, append(
		appendUint32([]byte{tmysql.ComStmtExecute}, uint32(stmt.ID())),
		tmysql.CursorTypeReadOnly, 0x1, 0x0, 0x0, 0x0,
	)))
}

func TestCursorFetchExecuteCheck(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	srv := server2.CreateMockServer(t, store)
	srv.SetDomain(dom)
	defer srv.Close()

	appendUint32 := binary.LittleEndian.AppendUint32
	ctx := context.Background()
	c := server2.CreateMockConn(t, srv)

	stmt, _, _, err := c.Context().Prepare("select 1")
	require.NoError(t, err)

	// execute with wrong ID
	require.Error(t, c.Dispatch(ctx, append(
		appendUint32([]byte{tmysql.ComStmtExecute}, uint32(stmt.ID()+1)),
		tmysql.CursorTypeReadOnly, 0x1, 0x0, 0x0, 0x0,
	)))

	// execute with wrong flag
	require.Error(t, c.Dispatch(ctx, append(
		appendUint32([]byte{tmysql.ComStmtExecute}, uint32(stmt.ID())),
		tmysql.CursorTypeReadOnly|tmysql.CursorTypeForUpdate, 0x1, 0x0, 0x0, 0x0,
	)))

	require.Error(t, c.Dispatch(ctx, append(
		appendUint32([]byte{tmysql.ComStmtExecute}, uint32(stmt.ID())),
		tmysql.CursorTypeReadOnly|tmysql.CursorTypeScrollable, 0x1, 0x0, 0x0, 0x0,
	)))
}
