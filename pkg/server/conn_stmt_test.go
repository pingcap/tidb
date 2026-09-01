// Copyright 2018 PingCAP, Inc.
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
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	servererr "github.com/pingcap/tidb/pkg/server/err"
	"github.com/pingcap/tidb/pkg/server/internal"
	"github.com/pingcap/tidb/pkg/server/internal/column"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/arena"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestCursorExistsFlag(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	srv := CreateMockServer(t, store)
	srv.SetDomain(dom)
	defer srv.Close()

	appendUint32 := binary.LittleEndian.AppendUint32
	ctx := context.Background()
	c := CreateMockConn(t, srv).(*mockConn)
	out := new(bytes.Buffer)
	c.pkt.ResetBufWriter(out)
	c.capability |= mysql.ClientDeprecateEOF | mysql.ClientProtocol41
	tk := testkit.NewTestKitWithSession(t, store, c.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key)")
	tk.MustExec("insert into t values (1), (2), (3), (4), (5), (6), (7), (8)")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("8"))

	getLastStatus := func() uint16 {
		raw := out.Bytes()
		return binary.LittleEndian.Uint16(raw[len(raw)-4 : len(raw)-2])
	}

	stmt, _, _, err := c.Context().Prepare("select * from t")
	require.NoError(t, err)

	require.NoError(t, c.Dispatch(ctx, append(
		appendUint32([]byte{mysql.ComStmtExecute}, uint32(stmt.ID())),
		mysql.CursorTypeReadOnly, 0x1, 0x0, 0x0, 0x0,
	)))
	require.True(t, mysql.HasCursorExistsFlag(getLastStatus()))

	// fetch first 5
	require.NoError(t, c.Dispatch(ctx, appendUint32(appendUint32([]byte{mysql.ComStmtFetch}, uint32(stmt.ID())), 5)))
	require.True(t, mysql.HasCursorExistsFlag(getLastStatus()))

	// COM_QUERY during fetch
	require.NoError(t, c.Dispatch(ctx, append([]byte{mysql.ComQuery}, "select * from t"...)))
	require.False(t, mysql.HasCursorExistsFlag(getLastStatus()))

	// fetch last 3, the `CursorExist` flag should have been unset and the `LastRowSend` flag should have been set
	require.NoError(t, c.Dispatch(ctx, appendUint32(appendUint32([]byte{mysql.ComStmtFetch}, uint32(stmt.ID())), 5)))
	require.False(t, mysql.HasCursorExistsFlag(getLastStatus()))
	require.True(t, getLastStatus()&mysql.ServerStatusLastRowSend > 0)

	// COM_QUERY after fetch
	require.NoError(t, c.Dispatch(ctx, append([]byte{mysql.ComQuery}, "select * from t"...)))
	require.False(t, mysql.HasCursorExistsFlag(getLastStatus()))

	// try another query without response
	stmt, _, _, err = c.Context().Prepare("select * from t where a = 100")
	require.NoError(t, err)

	require.NoError(t, c.Dispatch(ctx, append(
		appendUint32([]byte{mysql.ComStmtExecute}, uint32(stmt.ID())),
		mysql.CursorTypeReadOnly, 0x1, 0x0, 0x0, 0x0,
	)))
	require.True(t, mysql.HasCursorExistsFlag(getLastStatus()))

	// fetch 5 rows, it will return no data with the `CursorExist` unset and `LastRowSend` set.
	require.NoError(t, c.Dispatch(ctx, appendUint32(appendUint32([]byte{mysql.ComStmtFetch}, uint32(stmt.ID())), 5)))
	require.False(t, mysql.HasCursorExistsFlag(getLastStatus()))
	require.True(t, getLastStatus()&mysql.ServerStatusLastRowSend > 0)

	// the following FETCH should fail, as the cursor has been automatically closed
	require.Error(t, c.Dispatch(ctx, appendUint32(appendUint32([]byte{mysql.ComStmtFetch}, uint32(stmt.ID())), 5)))
}

func TestCursorWithParams(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	srv := CreateMockServer(t, store)
	srv.SetDomain(dom)
	defer srv.Close()

	appendUint32 := binary.LittleEndian.AppendUint32
	ctx := context.Background()
	c := CreateMockConn(t, srv).(*mockConn)

	tk := testkit.NewTestKitWithSession(t, store, c.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id_1 int, id_2 int)")
	tk.MustExec("insert into t values (1, 1), (1, 2)")

	stmt1, _, _, err := c.Context().Prepare("select * from t where id_1 = ? and id_2 = ?")
	require.NoError(t, err)
	stmt2, _, _, err := c.Context().Prepare("select * from t where id_1 = ?")
	require.NoError(t, err)

	// `execute stmt1 using 1,2` with cursor
	require.NoError(t, c.Dispatch(ctx, append(
		appendUint32([]byte{mysql.ComStmtExecute}, uint32(stmt1.ID())),
		mysql.CursorTypeReadOnly, 0x1, 0x0, 0x0, 0x0,
		0x0, 0x1, 0x3, 0x0, 0x3, 0x0,
		0x1, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0,
	)))
	rows := c.Context().stmts[stmt1.ID()].GetResultSet().GetRowIterator()
	require.Equal(t, int64(1), rows.Current(context.Background()).GetInt64(0))
	require.Equal(t, int64(2), rows.Current(context.Background()).GetInt64(1))
	rows.Next(context.Background())
	require.Equal(t, rows.End(), rows.Current(context.Background()))

	// `execute stmt2 using 1` with cursor
	require.NoError(t, c.Dispatch(ctx, append(
		appendUint32([]byte{mysql.ComStmtExecute}, uint32(stmt2.ID())),
		mysql.CursorTypeReadOnly, 0x1, 0x0, 0x0, 0x0,
		0x0, 0x1, 0x3, 0x0,
		0x1, 0x0, 0x0, 0x0,
	)))
	rows = c.Context().stmts[stmt2.ID()].GetResultSet().GetRowIterator()
	require.Equal(t, int64(1), rows.Current(context.Background()).GetInt64(0))
	require.Equal(t, int64(1), rows.Current(context.Background()).GetInt64(1))
	require.Equal(t, int64(1), rows.Next(context.Background()).GetInt64(0))
	require.Equal(t, int64(2), rows.Current(context.Background()).GetInt64(1))
	rows.Next(context.Background())
	require.Equal(t, rows.End(), rows.Current(context.Background()))

	// fetch stmt2 with fetch size 256
	require.NoError(t, c.Dispatch(ctx, append(
		appendUint32([]byte{mysql.ComStmtFetch}, uint32(stmt2.ID())),
		0x0, 0x1, 0x0, 0x0,
	)))

	// fetch stmt1 with fetch size 256, as it has more params, if we fetch the result at the first execute command, it
	// will panic because the params have been overwritten and is not long enough.
	require.NoError(t, c.Dispatch(ctx, append(
		appendUint32([]byte{mysql.ComStmtFetch}, uint32(stmt1.ID())),
		0x0, 0x1, 0x0, 0x0,
	)))
}

func TestFix69990BinaryProtocolNullBitmapAndTypeReuse(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	srv := CreateMockServer(t, store)
	srv.SetDomain(dom)
	defer srv.Close()

	ctx := context.Background()
	c := CreateMockConn(t, srv).(*mockConn)
	tk := testkit.NewTestKitWithSession(t, store, c.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_plan_cache_dml_param_type_match=ON")
	tk.MustExec("set tidb_opt_fix_control='49736:ON'")
	tk.MustExec("set tidb_enable_prepared_plan_cache=1")
	tk.MustExec("create table phase_c_protocol(id int primary key, b bit(8))")
	tk.MustExec("insert into phase_c_protocol values(1,null)")

	// Exercise COM_STMT_PREPARE itself rather than calling Session.PrepareStmt.
	prepareSQL := "update phase_c_protocol set b=? where id=?"
	require.NoError(t, c.Dispatch(ctx, append([]byte{mysql.ComStmtPrepare}, prepareSQL...)))
	require.Len(t, c.Context().stmts, 1)
	var stmt *TiDBStatement
	for _, prepared := range c.Context().stmts {
		stmt = prepared
	}
	require.NotNil(t, stmt)

	makeExecute := func(nullValue, newTypes bool, valueType byte, value []byte) []byte {
		req := binary.LittleEndian.AppendUint32([]byte{mysql.ComStmtExecute}, uint32(stmt.ID()))
		req = append(req, 0, 1, 0, 0, 0) // flags and iteration count
		var nullBitmap byte
		if nullValue {
			nullBitmap = 1 // parameter zero is SQL NULL
		}
		req = append(req, nullBitmap)
		if newTypes {
			req = append(req, 1, valueType, 0, mysql.TypeLong, 0)
		} else {
			req = append(req, 0)
		}
		if !nullValue {
			req = append(req, value...)
		}
		return binary.LittleEndian.AppendUint32(req, 1)
	}
	varcharValue := func(value string) []byte {
		return append([]byte{byte(len(value))}, value...)
	}
	execute := func(nullValue, newTypes bool, valueType byte, value []byte) {
		require.NoError(t, c.Dispatch(ctx, makeExecute(nullValue, newTypes, valueType, value)))
	}

	// Publish using a NULL bitmap while retaining the client's declared
	// VARCHAR type. A flag=0 packet must reuse those type bytes and still use
	// the current execution's NULL sidecar to hit the normalized entry.
	execute(true, true, mysql.TypeVarchar, nil)
	require.False(t, c.Context().GetSessionVars().FoundInPlanCache)
	require.Equal(t, []byte{mysql.TypeVarchar, 0, mysql.TypeLong, 0}, stmt.GetParamsType())
	cacheSize := c.Context().Session.GetSessionPlanCache().Size()
	require.Greater(t, cacheSize, 0)
	execute(true, false, 0, nil)
	require.True(t, c.Context().GetSessionVars().FoundInPlanCache)

	// With flag=0 the same retained VARCHAR declaration and a non-NULL value
	// reuses the target-typed entry; executor conversion handles the raw value.
	execute(false, false, 0, varcharValue("1"))
	require.True(t, c.Context().GetSessionVars().FoundInPlanCache)
	require.Equal(t, cacheSize, c.Context().Session.GetSessionPlanCache().Size())
	require.Empty(t, tk.MustQuery("show warnings").Rows())
	execute(true, false, 0, nil)
	require.True(t, c.Context().GetSessionVars().FoundInPlanCache)

	// Rotate the client's declared type with new-params-bound-flag=1, then
	// prove flag=0 reuses the new declaration while NULL remains allowed.
	execute(false, true, mysql.TypeLong, binary.LittleEndian.AppendUint32(nil, 1))
	require.True(t, c.Context().GetSessionVars().FoundInPlanCache)
	require.Equal(t, []byte{mysql.TypeLong, 0, mysql.TypeLong, 0}, stmt.GetParamsType())
	require.Equal(t, cacheSize, c.Context().Session.GetSessionPlanCache().Size())
	require.Empty(t, tk.MustQuery("show warnings").Rows())
	execute(true, false, 0, nil)
	require.True(t, c.Context().GetSessionVars().FoundInPlanCache)
	tk.MustQuery("select b is null from phase_c_protocol where id=1").Check(testkit.Rows("1"))
}

func TestComStmtCloseDeletesAllPlanCacheDMLParamMatchVariants(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	srv := CreateMockServer(t, store)
	srv.SetDomain(dom)
	defer srv.Close()

	ctx := context.Background()
	c := CreateMockConn(t, srv).(*mockConn)
	tk := testkit.NewTestKitWithSession(t, store, c.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_prepared_plan_cache=ON")
	tk.MustExec("create table protocol_close_variants(id int primary key, v bigint)")
	tk.MustExec("insert into protocol_close_variants values(1,0)")

	require.NoError(t, c.Dispatch(ctx, append([]byte{mysql.ComStmtPrepare},
		"update protocol_close_variants set v=? where id=?"...)))
	require.Len(t, c.Context().stmts, 1)
	var stmt *TiDBStatement
	for _, prepared := range c.Context().stmts {
		stmt = prepared
	}
	require.NotNil(t, stmt)
	baseline := c.Context().Session.GetSessionPlanCache().Size()
	execute := func(value uint32) {
		req := binary.LittleEndian.AppendUint32([]byte{mysql.ComStmtExecute}, uint32(stmt.ID()))
		req = append(req, 0, 1, 0, 0, 0) // flags and iteration count
		req = append(req, 0, 1, mysql.TypeLong, 0, mysql.TypeLong, 0)
		req = binary.LittleEndian.AppendUint32(req, value)
		req = binary.LittleEndian.AppendUint32(req, 1)
		require.NoError(t, c.Dispatch(ctx, req))
	}

	tk.MustExec("set tidb_enable_plan_cache_dml_param_type_match=OFF")
	execute(1)
	tk.MustExec("set tidb_enable_plan_cache_dml_param_type_match=ON")
	execute(2)
	require.Equal(t, baseline+2, c.Context().Session.GetSessionPlanCache().Size())
	require.True(t, c.Context().GetSessionVars().TxnCtx.CouldRetry,
		"exercise the protocol close branch that defers prepared metadata cleanup")

	closeReq := binary.LittleEndian.AppendUint32([]byte{mysql.ComStmtClose}, uint32(stmt.ID()))
	require.NoError(t, c.Dispatch(ctx, closeReq))
	require.Equal(t, baseline, c.Context().Session.GetSessionPlanCache().Size())

	// The retry cleanup path may delete the same variants again; it must remain
	// idempotent and must not disturb unrelated cache state.
	tk.MustExec("do 1")
	require.Equal(t, baseline, c.Context().Session.GetSessionPlanCache().Size())
}

func TestCursorDetachMemTracker(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	srv := CreateMockServer(t, store)
	srv.SetDomain(dom)
	defer srv.Close()

	appendUint32 := binary.LittleEndian.AppendUint32
	ctx := context.Background()
	c := CreateMockConn(t, srv).(*mockConn)

	tk := testkit.NewTestKitWithSession(t, store, c.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id_1 int, id_2 int)")
	tk.MustExec("insert into t values (1, 1), (1, 2)")
	tk.MustExec("set global tidb_mem_oom_action = 'CANCEL'")
	defer tk.MustExec("set global tidb_mem_oom_action= DEFAULT")
	// TODO: find whether it's expected to have one child at the beginning
	require.Len(t, tk.Session().GetSessionVars().MemTracker.GetChildrenForTest(), 1)

	// execute a normal statement, it'll success
	stmt, _, _, err := c.Context().Prepare("select count(id_2) from t")
	require.NoError(t, err)

	require.NoError(t, c.Dispatch(ctx, append(
		appendUint32([]byte{mysql.ComStmtExecute}, uint32(stmt.ID())),
		mysql.CursorTypeReadOnly, 0x1, 0x0, 0x0, 0x0,
	)))
	maxConsumed := tk.Session().GetSessionVars().MemTracker.MaxConsumed()

	// testkit also uses `PREPARE` related calls to run statement with arguments.
	// format the SQL to avoid the interference from testkit.
	tk.MustExec(fmt.Sprintf("set tidb_mem_quota_query=%d", maxConsumed/2))
	// there is one memTracker for the resultSet spill-disk
	require.Len(t, tk.Session().GetSessionVars().MemTracker.GetChildrenForTest(), 1)

	// This query should exceed the memory limitation during `openExecutor`
	require.Error(t, c.Dispatch(ctx, append(
		appendUint32([]byte{mysql.ComStmtExecute}, uint32(stmt.ID())),
		mysql.CursorTypeReadOnly, 0x1, 0x0, 0x0, 0x0,
	)))
	require.Len(t, tk.Session().GetSessionVars().MemTracker.GetChildrenForTest(), 0)

	// The next query should succeed
	tk.MustExec(fmt.Sprintf("set tidb_mem_quota_query=%d", maxConsumed+1))
	require.Len(t, tk.Session().GetSessionVars().MemTracker.GetChildrenForTest(), 0)
	// This query should succeed
	require.NoError(t, c.Dispatch(ctx, append(
		appendUint32([]byte{mysql.ComStmtExecute}, uint32(stmt.ID())),
		mysql.CursorTypeReadOnly, 0x1, 0x0, 0x0, 0x0,
	)))
	// there is one memTracker for the resultSet spill-disk
	require.Len(t, tk.Session().GetSessionVars().MemTracker.GetChildrenForTest(), 1)
}

func TestMemoryTrackForPrepareBinaryProtocol(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	srv := CreateMockServer(t, store)
	srv.SetDomain(dom)
	defer srv.Close()

	c := CreateMockConn(t, srv).(*mockConn)

	tk := testkit.NewTestKitWithSession(t, store, c.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id_2 int)")
	for i := 0; i <= 10; i++ {
		stmt, _, _, err := c.Context().Prepare("select count(id_2) from t")
		require.NoError(t, err)
		require.NoError(t, stmt.Close())
	}
	require.Len(t, tk.Session().GetSessionVars().MemTracker.GetChildrenForTest(), 0)
}

func getExpectOutput(t *testing.T, originalConn *mockConn, writeFn func(conn *clientConn)) []byte {
	buf := bytes.NewBuffer([]byte{})
	conn := &clientConn{
		alloc:      arena.NewAllocator(1024),
		capability: originalConn.capability,
		pkt:        internal.NewPacketIOForTest(bufio.NewWriter(buf)),
	}
	conn.pkt.SetSequence(originalConn.pkt.Sequence())
	conn.SetCtx(originalConn.getCtx())
	writeFn(conn)
	require.NoError(t, conn.flush(context.Background()))

	return buf.Bytes()
}

func expectedLonglongFetchResult(t *testing.T, c *mockConn, i int64) []byte {
	return getExpectOutput(t, c, func(conn *clientConn) {
		var err error

		cols := []*column.Info{{
			Name:  "id",
			Table: "t",
			Type:  mysql.TypeLonglong,
		}}

		chk := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}, 1)
		chk.AppendInt64(0, i)

		data := make([]byte, 4)
		data, err = column.DumpBinaryRow(data, cols, chk.GetRow(0), conn.rsEncoder)
		require.NoError(t, err)
		require.NoError(t, conn.writePacket(data))
		require.NoError(t, conn.writeEOF(context.Background(), mysql.ServerStatusCursorExists|mysql.ServerStatusAutocommit))
	})
}

func TestCursorFetchExecuteWithOpenCursor(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	srv := CreateMockServer(t, store)
	srv.SetDomain(dom)
	defer srv.Close()

	appendUint32 := binary.LittleEndian.AppendUint32
	ctx := context.Background()
	c := CreateMockConn(t, srv).(*mockConn)
	c.capability = mysql.ClientProtocol41

	stmt, _, _, err := c.Context().Prepare("select * from (select 1 as id union all select 2 as id union all select 3 as id) t order by id")
	require.NoError(t, err)

	// execute the statement
	require.NoError(t, c.Dispatch(ctx, append(
		appendUint32([]byte{mysql.ComStmtExecute}, uint32(stmt.ID())),
		mysql.CursorTypeReadOnly, 0x1, 0x0, 0x0, 0x0,
	)))

	out := c.GetOutput()
	// fetch one row
	expected := expectedLonglongFetchResult(t, c, 1)
	require.NoError(t, c.Dispatch(ctx, appendUint32(appendUint32([]byte{mysql.ComStmtFetch}, uint32(stmt.ID())), 1)))
	require.NoError(t, c.flush(context.Background()))
	require.Equal(t, expected, out.Bytes())

	out = c.GetOutput()
	// fetch the next row
	expected = expectedLonglongFetchResult(t, c, 2)
	require.NoError(t, c.Dispatch(ctx, appendUint32(appendUint32([]byte{mysql.ComStmtFetch}, uint32(stmt.ID())), 1)))
	require.NoError(t, c.flush(context.Background()))
	require.Equal(t, expected, out.Bytes())

	// re-execute the statement
	require.NoError(t, c.Dispatch(ctx, append(
		appendUint32([]byte{mysql.ComStmtExecute}, uint32(stmt.ID())),
		mysql.CursorTypeReadOnly, 0x1, 0x0, 0x0, 0x0,
	)))
	out = c.GetOutput()
	// the first row should be 1 again
	expected = expectedLonglongFetchResult(t, c, 1)
	require.NoError(t, c.Dispatch(ctx, appendUint32(appendUint32([]byte{mysql.ComStmtFetch}, uint32(stmt.ID())), 1)))
	require.NoError(t, c.flush(context.Background()))
	require.Equal(t, expected, out.Bytes())
}

func TestCursorFetchReset(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	srv := CreateMockServer(t, store)
	srv.SetDomain(dom)
	defer srv.Close()

	appendUint32 := binary.LittleEndian.AppendUint32
	ctx := context.Background()
	c := CreateMockConn(t, srv).(*mockConn)
	c.capability = mysql.ClientProtocol41

	tk := testkit.NewTestKitWithSession(t, store, c.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id_1 BIGINT, id_2 BIGINT)")
	tk.MustExec("insert into t values (1, 1), (1, 2)")

	stmt, _, _, err := c.Context().Prepare("select id_1 from t where id_1 = ?")
	require.NoError(t, err)

	// execute the statement with id_1 = 1
	require.NoError(t, c.Dispatch(ctx, append(
		appendUint32([]byte{mysql.ComStmtExecute}, uint32(stmt.ID())),
		mysql.CursorTypeReadOnly, 0x1, 0x0, 0x0, 0x0,
		0x0, 0x1, 0x3, 0x0,
		0x1, 0x0, 0x0, 0x0,
	)))

	out := c.GetOutput()
	// fetch one row
	expected := expectedLonglongFetchResult(t, c, 1)
	require.NoError(t, c.Dispatch(ctx, appendUint32(appendUint32([]byte{mysql.ComStmtFetch}, uint32(stmt.ID())), 1)))
	require.NoError(t, c.flush(context.Background()))
	require.Equal(t, expected, out.Bytes())
	// reset the statement
	require.NoError(t, c.Dispatch(ctx, appendUint32(
		[]byte{mysql.ComStmtReset}, uint32(stmt.ID()),
	)))
	// the following fetch will fail
	require.Error(t, c.Dispatch(ctx, appendUint32(appendUint32([]byte{mysql.ComStmtFetch}, uint32(stmt.ID())), 1)))
}

func dispatchSendLongData(c *mockConn, stmtID int, paramIndex uint16, parameter []byte) error {
	appendUint16 := binary.LittleEndian.AppendUint16
	appendUint32 := binary.LittleEndian.AppendUint32

	return c.Dispatch(context.Background(),
		append(
			appendUint16(
				appendUint32([]byte{mysql.ComStmtSendLongData}, uint32(stmtID)),
				paramIndex), // the index of parameter
			parameter..., // the parameter
		),
	)
}

func TestStmtSendLongDataMaxAllowedPacket(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	srv := CreateMockServer(t, store)
	srv.SetDomain(dom)
	defer srv.Close()

	c := CreateMockConn(t, srv).(*mockConn)
	c.capability = mysql.ClientProtocol41

	tk := testkit.NewTestKitWithSession(t, store, c.Context().Session)
	tk.MustExec("use test")

	stmt, _, _, err := c.Context().Prepare("select ?, ?")
	require.NoError(t, err)

	c.Context().GetSessionVars().MaxAllowedPacket = 1024
	require.NoError(t, dispatchSendLongData(c, stmt.ID(), 0, bytes.Repeat([]byte{'a'}, 1024)))
	require.NoError(t, dispatchSendLongData(c, stmt.ID(), 1, bytes.Repeat([]byte{'b'}, 1024)))
	require.NoError(t, stmt.CheckLongDataSize())

	err = dispatchSendLongData(c, stmt.ID(), 0, []byte{'c'})
	require.NoError(t, err)
	require.Len(t, stmt.BoundParams()[0], 1024)
	require.Len(t, stmt.BoundParams()[1], 1024)

	err = c.Dispatch(context.Background(), append(
		binary.LittleEndian.AppendUint32([]byte{mysql.ComStmtExecute}, uint32(stmt.ID())),
		0x0, 0x1, 0x0, 0x0, 0x0,
		0x0, 0x0,
	))
	require.ErrorIs(t, err, servererr.ErrNetPacketTooLarge)
	require.Nil(t, stmt.BoundParams()[0])
	require.Nil(t, stmt.BoundParams()[1])
	require.NoError(t, stmt.CheckLongDataSize())
}

func TestCursorFetchSendLongData(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	srv := CreateMockServer(t, store)
	srv.SetDomain(dom)
	defer srv.Close()

	appendUint32 := binary.LittleEndian.AppendUint32
	appendUint64 := binary.LittleEndian.AppendUint64

	ctx := context.Background()
	c := CreateMockConn(t, srv).(*mockConn)
	c.capability = mysql.ClientProtocol41

	tk := testkit.NewTestKitWithSession(t, store, c.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id_1 BIGINT, id_2 BIGINT)")
	tk.MustExec("insert into t values (1, 1), (1, 2)")

	// as the `ComStmtSendLongData` only sends bytes parameters, use `SUBSTR(HEX(?), 1, 2)` to convert a little endian 1 to
	// a string "1"
	stmt, _, _, err := c.Context().Prepare("select id_1 from t where id_1 = cast(SUBSTR(HEX(?), 1, 2) as UNSIGNED)")
	require.NoError(t, err)

	// send a parameter to the server
	require.NoError(t, dispatchSendLongData(c, stmt.ID(), 0, appendUint64([]byte{}, 1)))

	// execute the statement without argument
	require.NoError(t, c.Dispatch(ctx, append(
		appendUint32([]byte{mysql.ComStmtExecute}, uint32(stmt.ID())),
		mysql.CursorTypeReadOnly, 0x1, 0x0, 0x0, 0x0,
		0x0, 0x0,
	)))

	out := c.GetOutput()
	// fetch one row
	expected := expectedLonglongFetchResult(t, c, 1)
	require.NoError(t, c.Dispatch(ctx, appendUint32(appendUint32([]byte{mysql.ComStmtFetch}, uint32(stmt.ID())), 1)))
	require.NoError(t, c.flush(context.Background()))
	require.Equal(t, expected, out.Bytes())
}

func TestCursorFetchSendLongDataReset(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	srv := CreateMockServer(t, store)
	srv.SetDomain(dom)
	defer srv.Close()

	appendUint32 := binary.LittleEndian.AppendUint32
	appendUint64 := binary.LittleEndian.AppendUint64
	ctx := context.Background()
	c := CreateMockConn(t, srv).(*mockConn)
	c.capability = mysql.ClientProtocol41

	tk := testkit.NewTestKitWithSession(t, store, c.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id_1 BIGINT, id_2 BIGINT)")
	tk.MustExec("insert into t values (1, 1), (1, 1), (2, 2), (2, 2)")

	// as the `ComStmtSendLongData` only sends bytes parameters, use `SUBSTR(HEX(?), 1, 2)` to convert a little endian 2 to
	// a string "2"
	stmt, _, _, err := c.Context().Prepare("select id_1 from t where id_1 = cast(SUBSTR(HEX(?), 1, 2) as UNSIGNED)")
	require.NoError(t, err)

	// send a parameter to the server
	require.NoError(t, dispatchSendLongData(c, stmt.ID(), 0, appendUint64([]byte{}, 1)))
	// reset the statement
	require.NoError(t, c.Dispatch(ctx, appendUint32(
		[]byte{mysql.ComStmtReset}, uint32(stmt.ID())),
	))
	// execute directly will fail
	require.Error(t, c.Dispatch(ctx, append(
		appendUint32([]byte{mysql.ComStmtExecute}, uint32(stmt.ID())),
		mysql.CursorTypeReadOnly, 0x1, 0x0, 0x0, 0x0,
		0x0, 0x0,
	)))

	// send a parameter to the server
	require.NoError(t, dispatchSendLongData(c, stmt.ID(), 0, appendUint64([]byte{}, 2)))
	require.NoError(t, c.Dispatch(ctx, append(
		appendUint32([]byte{mysql.ComStmtExecute}, uint32(stmt.ID())),
		mysql.CursorTypeReadOnly, 0x1, 0x0, 0x0, 0x0,
		0x0, 0x0,
	)))

	out := c.GetOutput()
	// fetch one row, it will get "2" because the argument is 2 now.
	expected := expectedLonglongFetchResult(t, c, 2)
	require.NoError(t, c.Dispatch(ctx, appendUint32(appendUint32([]byte{mysql.ComStmtFetch}, uint32(stmt.ID())), 1)))
	require.NoError(t, c.flush(context.Background()))
	require.Equal(t, expected, out.Bytes())
}
