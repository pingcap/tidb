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
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

func TestParseExecArgs(t *testing.T) {
	type args struct {
		args        []expression.Expression
		boundParams [][]byte
		nullBitmap  []byte
		paramTypes  []byte
		paramValues []byte
	}
	tests := []struct {
		args   args
		err    error
		expect interface{}
	}{
		// Tests for int overflow
		{
			args{
				expression.Args2Expressions4Test(1),
				[][]byte{nil},
				[]byte{0x0},
				[]byte{1, 0},
				[]byte{0xff},
			},
			nil,
			int64(-1),
		},
		{
			args{
				expression.Args2Expressions4Test(1),
				[][]byte{nil},
				[]byte{0x0},
				[]byte{2, 0},
				[]byte{0xff, 0xff},
			},
			nil,
			int64(-1),
		},
		{
			args{
				expression.Args2Expressions4Test(1),
				[][]byte{nil},
				[]byte{0x0},
				[]byte{3, 0},
				[]byte{0xff, 0xff, 0xff, 0xff},
			},
			nil,
			int64(-1),
		},
		// Tests for date/datetime/timestamp
		{
			args{
				expression.Args2Expressions4Test(1),
				[][]byte{nil},
				[]byte{0x0},
				[]byte{12, 0},
				[]byte{0x0b, 0xda, 0x07, 0x0a, 0x11, 0x13, 0x1b, 0x1e, 0x01, 0x00, 0x00, 0x00},
			},
			nil,
			"2010-10-17 19:27:30.000001",
		},
		{
			args{
				expression.Args2Expressions4Test(1),
				[][]byte{nil},
				[]byte{0x0},
				[]byte{10, 0},
				[]byte{0x04, 0xda, 0x07, 0x0a, 0x11},
			},
			nil,
			"2010-10-17",
		},
		{
			args{
				expression.Args2Expressions4Test(1),
				[][]byte{nil},
				[]byte{0x0},
				[]byte{7, 0},
				[]byte{0x0b, 0xda, 0x07, 0x0a, 0x11, 0x13, 0x1b, 0x1e, 0x01, 0x00, 0x00, 0x00},
			},
			nil,
			"2010-10-17 19:27:30.000001",
		},
		{
			args{
				expression.Args2Expressions4Test(1),
				[][]byte{nil},
				[]byte{0x0},
				[]byte{7, 0},
				[]byte{0x07, 0xda, 0x07, 0x0a, 0x11, 0x13, 0x1b, 0x1e},
			},
			nil,
			"2010-10-17 19:27:30",
		},
		{
			args{
				expression.Args2Expressions4Test(1),
				[][]byte{nil},
				[]byte{0x0},
				[]byte{7, 0},
				[]byte{0x00},
			},
			nil,
			types.ZeroDatetimeStr,
		},
		// Tests for time
		{
			args{
				expression.Args2Expressions4Test(1),
				[][]byte{nil},
				[]byte{0x0},
				[]byte{11, 0},
				[]byte{0x0c, 0x01, 0x78, 0x00, 0x00, 0x00, 0x13, 0x1b, 0x1e, 0x01, 0x00, 0x00, 0x00},
			},
			nil,
			"-120 19:27:30.000001",
		},
		{
			args{
				expression.Args2Expressions4Test(1),
				[][]byte{nil},
				[]byte{0x0},
				[]byte{11, 0},
				[]byte{0x08, 0x01, 0x78, 0x00, 0x00, 0x00, 0x13, 0x1b, 0x1e},
			},
			nil,
			"-120 19:27:30",
		},
		{
			args{
				expression.Args2Expressions4Test(1),
				[][]byte{nil},
				[]byte{0x0},
				[]byte{11, 0},
				[]byte{0x00},
			},
			nil,
			"0",
		},
		// For error test
		{
			args{
				expression.Args2Expressions4Test(1),
				[][]byte{nil},
				[]byte{0x0},
				[]byte{7, 0},
				[]byte{10},
			},
			mysql.ErrMalformPacket,
			nil,
		},
		{
			args{
				expression.Args2Expressions4Test(1),
				[][]byte{nil},
				[]byte{0x0},
				[]byte{11, 0},
				[]byte{10},
			},
			mysql.ErrMalformPacket,
			nil,
		},
		{
			args{
				expression.Args2Expressions4Test(1),
				[][]byte{nil},
				[]byte{0x0},
				[]byte{11, 0},
				[]byte{8, 2},
			},
			mysql.ErrMalformPacket,
			nil,
		},
	}
	for _, tt := range tests {
		err := parseExecArgs(&stmtctx.StatementContext{}, tt.args.args, tt.args.boundParams, tt.args.nullBitmap, tt.args.paramTypes, tt.args.paramValues, nil)
		require.Truef(t, terror.ErrorEqual(err, tt.err), "err %v", err)
		if err == nil {
			require.Equal(t, tt.expect, tt.args.args[0].(*expression.Constant).Value.GetValue())
		}
	}
}

func TestParseExecArgsAndEncode(t *testing.T) {
	dt := expression.Args2Expressions4Test(1)
	err := parseExecArgs(&stmtctx.StatementContext{},
		dt,
		[][]byte{nil},
		[]byte{0x0},
		[]byte{mysql.TypeVarchar, 0},
		[]byte{4, 178, 226, 202, 212},
		newInputDecoder("gbk"))
	require.NoError(t, err)
	require.Equal(t, "测试", dt[0].(*expression.Constant).Value.GetValue())

	err = parseExecArgs(&stmtctx.StatementContext{},
		dt,
		[][]byte{{178, 226, 202, 212}},
		[]byte{0x0},
		[]byte{mysql.TypeString, 0},
		[]byte{},
		newInputDecoder("gbk"))
	require.NoError(t, err)
	require.Equal(t, "测试", dt[0].(*expression.Constant).Value.GetString())
}

func TestParseExecArgsForDecimal(t *testing.T) {
	// "1" -> 1
	dt := expression.Args2Expressions4Test(1)
	err := parseExecArgs(&stmtctx.StatementContext{},
		dt,
		[][]byte{nil},
		[]byte{0x0},
		[]byte{0xf6, 0},
		[]byte{0x1, '1'},
		nil)
	require.NoError(t, err)
	require.Equal(t, types.NewDecFromStringForTest("1"), dt[0].(*expression.Constant).Value.GetValue())

	// Maximum supported range of decimal, 81 digitsInt
	err = parseExecArgs(&stmtctx.StatementContext{},
		dt,
		[][]byte{nil},
		[]byte{0x0},
		[]byte{0xf6, 0},
		[]byte{81, '1', '2', '3', '4', '5', '6', '7', '8', '9',
			'1', '2', '3', '4', '5', '6', '7', '8', '9', '1', '2', '3', '4', '5', '6', '7', '8', '9',
			'1', '2', '3', '4', '5', '6', '7', '8', '9', '1', '2', '3', '4', '5', '6', '7', '8', '9',
			'1', '2', '3', '4', '5', '6', '7', '8', '9', '1', '2', '3', '4', '5', '6', '7', '8', '9',
			'1', '2', '3', '4', '5', '6', '7', '8', '9', '1', '2', '3', '4', '5', '6', '7', '8', '9'},
		nil)
	require.NoError(t, err)
	require.Equal(t, types.NewDecFromStringForTest("123456789"+
		"123456789123456789"+
		"123456789123456789"+
		"123456789123456789"+
		"123456789123456789"), dt[0].(*expression.Constant).Value.GetValue())

	// Maximum supported range of decimal, 9 digitsInt and 72 digitsFrac
	err = parseExecArgs(&stmtctx.StatementContext{},
		dt,
		[][]byte{nil},
		[]byte{0x0},
		[]byte{0xf6, 0},
		[]byte{82, '1', '2', '3', '4', '5', '6', '7', '8', '9', '.',
			'1', '2', '3', '4', '5', '6', '7', '8', '9', '1', '2', '3', '4', '5', '6', '7', '8', '9',
			'1', '2', '3', '4', '5', '6', '7', '8', '9', '1', '2', '3', '4', '5', '6', '7', '8', '9',
			'1', '2', '3', '4', '5', '6', '7', '8', '9', '1', '2', '3', '4', '5', '6', '7', '8', '9',
			'1', '2', '3', '4', '5', '6', '7', '8', '9', '1', '2', '3', '4', '5', '6', '7', '8', '9'},
		nil)
	require.NoError(t, err)
	require.Equal(t, types.NewDecFromStringForTest("123456789."+
		"123456789123456789"+
		"123456789123456789"+
		"123456789123456789"+
		"123456789123456789"), dt[0].(*expression.Constant).Value.GetValue())

	// Truncate 81-digit decimal string to 72-digit length
	err = parseExecArgs(&stmtctx.StatementContext{},
		dt,
		[][]byte{nil},
		[]byte{0x0},
		[]byte{0xf6, 0},
		[]byte{83, '0', '.', '1', '2', '3', '4', '5', '6', '7', '8', '9',
			'1', '2', '3', '4', '5', '6', '7', '8', '9', '1', '2', '3', '4', '5', '6', '7', '8', '9',
			'1', '2', '3', '4', '5', '6', '7', '8', '9', '1', '2', '3', '4', '5', '6', '7', '8', '9',
			'1', '2', '3', '4', '5', '6', '7', '8', '9', '1', '2', '3', '4', '5', '6', '7', '8', '9',
			'1', '2', '3', '4', '5', '6', '7', '8', '9', '1', '2', '3', '4', '5', '6', '7', '8', '9'},
		nil)
	require.NoError(t, err)
	require.Equal(t, types.NewDecFromStringForTest("0.123456789"+
		"123456789123456789"+
		"123456789123456789"+
		"123456789123456789"+
		"123456789"), dt[0].(*expression.Constant).Value.GetValue())
}

func TestParseStmtFetchCmd(t *testing.T) {
	tests := []struct {
		arg       []byte
		stmtID    uint32
		fetchSize uint32
		err       error
	}{
		{[]byte{3, 0, 0, 0, 50, 0, 0, 0}, 3, 50, nil},
		{[]byte{5, 0, 0, 0, 232, 3, 0, 0}, 5, 1000, nil},
		{[]byte{5, 0, 0, 0, 0, 8, 0, 0}, 5, maxFetchSize, nil},
		{[]byte{5, 0, 0}, 0, 0, mysql.ErrMalformPacket},
		{[]byte{1, 0, 0, 0, 3, 2, 0, 0, 3, 5, 6}, 0, 0, mysql.ErrMalformPacket},
		{[]byte{}, 0, 0, mysql.ErrMalformPacket},
	}

	for _, tc := range tests {
		stmtID, fetchSize, err := parseStmtFetchCmd(tc.arg)
		require.Equal(t, tc.stmtID, stmtID)
		require.Equal(t, tc.fetchSize, fetchSize)
		require.Equal(t, tc.err, err)
	}
}

func TestCursorExistsFlag(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	srv := CreateMockServer(t, store)
	srv.SetDomain(dom)
	defer srv.Close()

	appendUint32 := binary.LittleEndian.AppendUint32
	ctx := context.Background()
	c := CreateMockConn(t, srv).(*mockConn)
	out := new(bytes.Buffer)
	c.pkt.bufWriter.Reset(out)
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
	rows := c.Context().stmts[stmt1.ID()].GetResultSet().GetRowContainerReader()
	require.Equal(t, int64(1), rows.Current().GetInt64(0))
	require.Equal(t, int64(2), rows.Current().GetInt64(1))
	rows.Next()
	require.Equal(t, rows.End(), rows.Current())

	// `execute stmt2 using 1` with cursor
	require.NoError(t, c.Dispatch(ctx, append(
		appendUint32([]byte{mysql.ComStmtExecute}, uint32(stmt2.ID())),
		mysql.CursorTypeReadOnly, 0x1, 0x0, 0x0, 0x0,
		0x0, 0x1, 0x3, 0x0,
		0x1, 0x0, 0x0, 0x0,
	)))
	rows = c.Context().stmts[stmt2.ID()].GetResultSet().GetRowContainerReader()
	require.Equal(t, int64(1), rows.Current().GetInt64(0))
	require.Equal(t, int64(1), rows.Current().GetInt64(1))
	require.Equal(t, int64(1), rows.Next().GetInt64(0))
	require.Equal(t, int64(2), rows.Current().GetInt64(1))
	rows.Next()
	require.Equal(t, rows.End(), rows.Current())

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

func TestCursorFetchShouldSpill(t *testing.T) {
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempStoragePath = t.TempDir()
	})

	store, dom := testkit.CreateMockStoreAndDomain(t)
	srv := CreateMockServer(t, store)
	srv.SetDomain(dom)
	defer srv.Close()

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/server/testCursorFetchSpill", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/server/testCursorFetchSpill"))
	}()

	appendUint32 := binary.LittleEndian.AppendUint32
	ctx := context.Background()
	c := CreateMockConn(t, srv).(*mockConn)

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
		appendUint32([]byte{mysql.ComStmtExecute}, uint32(stmt.ID())),
		mysql.CursorTypeReadOnly, 0x1, 0x0, 0x0, 0x0,
	)))
}

func TestCursorFetchErrorInFetch(t *testing.T) {
	tmpStoragePath := t.TempDir()
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempStoragePath = tmpStoragePath
	})

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

	require.NoError(t, c.Dispatch(ctx, append(
		appendUint32([]byte{mysql.ComStmtExecute}, uint32(stmt.ID())),
		mysql.CursorTypeReadOnly, 0x1, 0x0, 0x0, 0x0,
	)))

	// close these disk files to produce error
	filepath.Walk("/proc/self/fd", func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		target, err := os.Readlink(path)
		if err != nil {
			return nil
		}
		if strings.HasPrefix(target, tmpStoragePath) {
			fd, err := strconv.Atoi(filepath.Base(path))
			require.NoError(t, err)
			require.NoError(t, syscall.Close(fd))
		}
		return nil
	})

	// it'll get "bad file descriptor", as it has been closed in the test.
	require.Error(t, c.Dispatch(ctx, appendUint32(appendUint32([]byte{mysql.ComStmtFetch}, uint32(stmt.ID())), 1024)))
	// after getting a failed FETCH, the cursor should have been reseted
	require.False(t, stmt.GetCursorActive())
	require.Len(t, tk.Session().GetSessionVars().MemTracker.GetChildrenForTest(), 0)
	require.Len(t, tk.Session().GetSessionVars().DiskTracker.GetChildrenForTest(), 0)
}
