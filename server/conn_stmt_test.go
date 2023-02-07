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
	"encoding/binary"
	"testing"

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

func TestCursorReadHoldTS(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	srv := CreateMockServer(t, store)
	srv.SetDomain(dom)
	defer srv.Close()

	appendUint32 := binary.LittleEndian.AppendUint32
	ctx := context.Background()
	c := CreateMockConn(t, srv)
	tk := testkit.NewTestKitWithSession(t, store, c.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key)")
	tk.MustExec("insert into t values (1), (2), (3), (4), (5), (6), (7), (8)")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("8"))

	stmt, _, _, err := c.Context().Prepare("select * from t")
	require.NoError(t, err)
	require.Zero(t, tk.Session().ShowProcess().GetMinStartTS(0))

	// should hold ts after executing stmt with cursor
	require.NoError(t, c.Dispatch(ctx, append(
		appendUint32([]byte{mysql.ComStmtExecute}, uint32(stmt.ID())),
		mysql.CursorTypeReadOnly, 0x1, 0x0, 0x0, 0x0,
	)))
	ts := tk.Session().ShowProcess().GetMinStartTS(0)
	require.Positive(t, ts)
	// should unhold ts when result set exhausted
	require.NoError(t, c.Dispatch(ctx, appendUint32(appendUint32([]byte{mysql.ComStmtFetch}, uint32(stmt.ID())), 5)))
	require.Equal(t, ts, tk.Session().ShowProcess().GetMinStartTS(0))
	require.Equal(t, ts, srv.GetMinStartTS(0))
	require.NoError(t, c.Dispatch(ctx, appendUint32(appendUint32([]byte{mysql.ComStmtFetch}, uint32(stmt.ID())), 5)))
	require.Equal(t, ts, tk.Session().ShowProcess().GetMinStartTS(0))
	require.Equal(t, ts, srv.GetMinStartTS(0))
	require.NoError(t, c.Dispatch(ctx, appendUint32(appendUint32([]byte{mysql.ComStmtFetch}, uint32(stmt.ID())), 5)))
	require.Zero(t, tk.Session().ShowProcess().GetMinStartTS(0))

	// should hold ts after executing stmt with cursor
	require.NoError(t, c.Dispatch(ctx, append(
		appendUint32([]byte{mysql.ComStmtExecute}, uint32(stmt.ID())),
		mysql.CursorTypeReadOnly, 0x1, 0x0, 0x0, 0x0,
	)))
	require.Positive(t, tk.Session().ShowProcess().GetMinStartTS(0))
	// should unhold ts when stmt reset
	require.NoError(t, c.Dispatch(ctx, appendUint32([]byte{mysql.ComStmtReset}, uint32(stmt.ID()))))
	require.Zero(t, tk.Session().ShowProcess().GetMinStartTS(0))

	// should hold ts after executing stmt with cursor
	require.NoError(t, c.Dispatch(ctx, append(
		appendUint32([]byte{mysql.ComStmtExecute}, uint32(stmt.ID())),
		mysql.CursorTypeReadOnly, 0x1, 0x0, 0x0, 0x0,
	)))
	require.Positive(t, tk.Session().ShowProcess().GetMinStartTS(0))
	// should unhold ts when stmt closed
	require.NoError(t, c.Dispatch(ctx, appendUint32([]byte{mysql.ComStmtClose}, uint32(stmt.ID()))))
	require.Zero(t, tk.Session().ShowProcess().GetMinStartTS(0))

	// create another 2 stmts and execute them
	stmt1, _, _, err := c.Context().Prepare("select * from t")
	require.NoError(t, err)
	require.NoError(t, c.Dispatch(ctx, append(
		appendUint32([]byte{mysql.ComStmtExecute}, uint32(stmt1.ID())),
		mysql.CursorTypeReadOnly, 0x1, 0x0, 0x0, 0x0,
	)))
	ts1 := tk.Session().ShowProcess().GetMinStartTS(0)
	require.Positive(t, ts1)
	stmt2, _, _, err := c.Context().Prepare("select * from t")
	require.NoError(t, err)
	require.NoError(t, c.Dispatch(ctx, append(
		appendUint32([]byte{mysql.ComStmtExecute}, uint32(stmt2.ID())),
		mysql.CursorTypeReadOnly, 0x1, 0x0, 0x0, 0x0,
	)))
	ts2 := tk.Session().ShowProcess().GetMinStartTS(ts1)
	require.Positive(t, ts2)

	require.Less(t, ts1, ts2)
	require.Equal(t, ts1, srv.GetMinStartTS(0))
	require.Equal(t, ts2, srv.GetMinStartTS(ts1))
	require.Zero(t, srv.GetMinStartTS(ts2))

	// should unhold all when session closed
	c.Close()
	require.Zero(t, tk.Session().ShowProcess().GetMinStartTS(0))
	require.Zero(t, srv.GetMinStartTS(0))
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

	// fetch last 3
	require.NoError(t, c.Dispatch(ctx, appendUint32(appendUint32([]byte{mysql.ComStmtFetch}, uint32(stmt.ID())), 5)))
	require.True(t, mysql.HasCursorExistsFlag(getLastStatus()))

	// final fetch with no row retured
	// (tidb doesn't unset cursor-exists flag in the previous response like mysql, one more fetch is needed)
	require.NoError(t, c.Dispatch(ctx, appendUint32(appendUint32([]byte{mysql.ComStmtFetch}, uint32(stmt.ID())), 5)))
	require.False(t, mysql.HasCursorExistsFlag(getLastStatus()))
	require.True(t, getLastStatus()&mysql.ServerStatusLastRowSend > 0)

	// COM_QUERY after fetch
	require.NoError(t, c.Dispatch(ctx, append([]byte{mysql.ComQuery}, "select * from t"...)))
	require.False(t, mysql.HasCursorExistsFlag(getLastStatus()))
}
