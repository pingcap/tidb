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
	"fmt"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/server/internal"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/arena"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestRcReadCheckTSConflict(t *testing.T) {
	store := testkit.CreateMockStore(t)

	cc := &clientConn{
		alloc:      arena.NewAllocator(1024),
		chunkAlloc: chunk.NewAllocator(),
		pkt:        internal.NewPacketIOForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
	}
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_rc_read_check_ts = ON")
	tk.RefreshSession()
	cc.SetCtx(&TiDBContext{Session: tk.Session(), stmts: make(map[int]*TiDBStatement)})

	tk.MustExec("use test")
	tk.MustExec("create table t(a int not null primary key, b int not null)")
	dml := "insert into t values"
	for i := 0; i < 50; i++ {
		dml += fmt.Sprintf("(%v, 0)", i)
		if i != 49 {
			dml += ","
		}
	}
	tk.MustExec(dml)
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("50"))
	require.Equal(t, "ON", tk.MustQuery("show variables like 'tidb_rc_read_check_ts'").Rows()[0][1])

	ctx := context.Background()
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/server/fetchNextErr", "return(\"secondNextAndRetConflict\")"))
	err := cc.handleQuery(ctx, "select * from t limit 20")
	require.NoError(t, err)

	err = cc.handleQuery(ctx, "select * from t t1 join t t2")
	require.Equal(t, kv.ErrWriteConflict, err)

	tk.MustExec("set session tidb_max_chunk_size = 4096")
	require.Equal(t, "4096", tk.MustQuery("show variables like 'tidb_max_chunk_size'").Rows()[0][1])
	err = cc.handleQuery(ctx, "select * from t t1 join t t2")
	require.NoError(t, err)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/server/fetchNextErr"))

	tk.MustExec("drop table t")
}

func TestRcReadCheckTSConflictExtra(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/sessiontxn/isolation/CallOnStmtRetry", "return"))
	defer func() {
		defer require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/sessiontxn/isolation/CallOnStmtRetry"))
	}()
	store := testkit.CreateMockStore(t)

	ctx := context.Background()
	cc := &clientConn{
		alloc:      arena.NewAllocator(1024),
		chunkAlloc: chunk.NewAllocator(),
		pkt:        internal.NewPacketIOForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
	}
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_rc_read_check_ts = ON")

	se := tk.Session()
	cc.SetCtx(&TiDBContext{Session: se, stmts: make(map[int]*TiDBStatement)})

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(id1 int, id2 int, id3 int, PRIMARY KEY(id1), UNIQUE KEY udx_id2 (id2))")
	tk.MustExec("insert into t1 values (1, 1, 1)")
	tk.MustExec("insert into t1 values (10, 10, 10)")
	require.Equal(t, "ON", tk.MustQuery("show variables like 'tidb_rc_read_check_ts'").Rows()[0][1])

	tk.MustExec("set transaction_isolation = 'READ-COMMITTED'")
	tk2.MustExec("set transaction_isolation = 'READ-COMMITTED'")

	// Execute in text protocol
	se.SetValue(sessiontxn.CallOnStmtRetryCount, 0)
	tk.MustExec("begin pessimistic")
	tk2.MustExec("update t1 set id3 = id3 + 1 where id1 = 1")
	err := cc.handleQuery(ctx, "select * from t1 where id1 = 1")
	require.NoError(t, err)
	tk.MustExec("commit")
	count, ok := se.Value(sessiontxn.CallOnStmtRetryCount).(int)
	require.Equal(t, true, ok)
	require.Equal(t, 1, count)

	// Execute in prepare binary protocol
	se.SetValue(sessiontxn.CallOnStmtRetryCount, 0)
	tk.MustExec("begin pessimistic")
	tk2.MustExec("update t1 set id3 = id3 + 1 where id1 = 1")
	require.NoError(t, cc.HandleStmtPrepare(ctx, "select * from t1 where id1 = 1"))
	require.NoError(t, cc.handleStmtExecute(ctx, []byte{0x1, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0}))
	tk.MustExec("commit")
	count, ok = se.Value(sessiontxn.CallOnStmtRetryCount).(int)
	require.Equal(t, true, ok)
	require.Equal(t, 1, count)

	tk.MustExec("drop table t1")
}
