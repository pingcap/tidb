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
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/param"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/server/internal/column"
	"github.com/pingcap/tidb/pkg/server/internal/util"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/stretchr/testify/require"
)

// decodeAndParse uses the `parseBinaryParams` and `expression.ExecBinaryParam` to parse the params passed through binary protocol
// It helps to test the integration of these two functions
func decodeAndParse(typectx types.Context, args []expression.Expression, boundParams [][]byte,
	nullBitmap, paramTypes, paramValues []byte, enc *util.InputDecoder) (err error) {
	binParams := make([]param.BinaryParam, len(args))
	_, err = parseBinaryParams(binParams, boundParams, nullBitmap, paramTypes, paramValues, enc)
	if err != nil {
		return err
	}

	parsedArgs, err := expression.ExecBinaryParam(typectx, binParams)
	if err != nil {
		return err
	}

	for i := 0; i < len(args); i++ {
		args[i] = parsedArgs[i]
	}
	return
}

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
		warn   error
		expect any
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
			nil,
			types.NewTime(types.FromDate(2010, 10, 17, 19, 27, 30, 1), mysql.TypeDatetime, types.MaxFsp),
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
			nil,
			types.NewTime(types.FromDate(2010, 10, 17, 0, 0, 0, 0), mysql.TypeDate, types.DefaultFsp),
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
			nil,
			types.NewTime(types.FromDate(2010, 10, 17, 19, 27, 30, 1), mysql.TypeDatetime, types.MaxFsp),
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
			nil,
			types.NewTime(types.FromDate(2010, 10, 17, 19, 27, 30, 0), mysql.TypeDatetime, types.DefaultFsp),
		},
		{
			args{
				expression.Args2Expressions4Test(1),
				[][]byte{nil},
				[]byte{0x0},
				[]byte{7, 0},
				[]byte{0x0d, 0xdb, 0x07, 0x02, 0x03, 0x04, 0x05, 0x06, 0x40, 0xe2, 0x01, 0x00, 0xf2, 0x02},
			},
			nil,
			nil,
			types.NewTime(types.FromDate(2011, 02, 02, 15, 31, 06, 123456), mysql.TypeDatetime, types.MaxFsp),
		},
		{
			args{
				expression.Args2Expressions4Test(1),
				[][]byte{nil},
				[]byte{0x0},
				[]byte{7, 0},
				[]byte{0x0d, 0xdb, 0x07, 0x02, 0x03, 0x04, 0x05, 0x06, 0x40, 0xe2, 0x01, 0x00, 0x0e, 0xfd},
			},
			nil,
			nil,
			types.NewTime(types.FromDate(2011, 02, 03, 16, 39, 06, 123456), mysql.TypeDatetime, types.MaxFsp),
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
			nil,
			types.NewTime(types.ZeroCoreTime, mysql.TypeDatetime, types.DefaultFsp),
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
			types.ErrTruncatedWrongVal,
			types.Duration{Duration: types.MinTime, Fsp: types.MaxFsp},
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
			types.ErrTruncatedWrongVal,
			types.Duration{Duration: types.MinTime, Fsp: 0},
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
			nil,
			types.Duration{Duration: time.Duration(0), Fsp: 0},
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
			nil,
		},
	}
	for _, tt := range tests {
		var warn error
		typectx := types.NewContext(types.DefaultStmtFlags.WithTruncateAsWarning(true), time.UTC, contextutil.NewFuncWarnAppenderForTest(func(err error) {
			warn = err
		}))
		err := decodeAndParse(typectx, tt.args.args, tt.args.boundParams, tt.args.nullBitmap, tt.args.paramTypes, tt.args.paramValues, nil)
		require.Truef(t, terror.ErrorEqual(err, tt.err), "err %v", err)
		require.Truef(t, terror.ErrorEqual(warn, tt.warn), "warn %v", warn)
		if err == nil {
			require.Equal(t, tt.expect, tt.args.args[0].(*expression.Constant).Value.GetValue())
		}
	}
}

func TestParseExecArgsAndEncode(t *testing.T) {
	dt := expression.Args2Expressions4Test(1)
	err := decodeAndParse(types.DefaultStmtNoWarningContext,
		dt,
		[][]byte{nil},
		[]byte{0x0},
		[]byte{mysql.TypeVarchar, 0},
		[]byte{4, 178, 226, 202, 212},
		util.NewInputDecoder("gbk"))
	require.NoError(t, err)
	require.Equal(t, "测试", dt[0].(*expression.Constant).Value.GetValue())

	err = decodeAndParse(types.DefaultStmtNoWarningContext,
		dt,
		[][]byte{{178, 226, 202, 212}},
		[]byte{0x0},
		[]byte{mysql.TypeString, 0},
		[]byte{},
		util.NewInputDecoder("gbk"))
	require.NoError(t, err)
	require.Equal(t, "测试", dt[0].(*expression.Constant).Value.GetString())
}

func buildDatetimeParam(year uint16, month uint8, day uint8, hour uint8, min uint8, sec uint8, msec uint32) []byte {
	endian := binary.LittleEndian

	result := []byte{mysql.TypeDatetime, 0x0, 11}
	result = endian.AppendUint16(result, year)
	result = append(result, month)
	result = append(result, day)
	result = append(result, hour)
	result = append(result, min)
	result = append(result, sec)
	result = endian.AppendUint32(result, msec)
	return result
}

func expectedDatetimeExecuteResult(t *testing.T, c *mockConn, time types.Time, warnCount int) []byte {
	return getExpectOutput(t, c, func(conn *clientConn) {
		var err error

		cols := []*column.Info{{
			Name:         "t",
			Table:        "",
			Type:         mysql.TypeDatetime,
			Charset:      uint16(mysql.CharsetNameToID(charset.CharsetBin)),
			Flag:         uint16(mysql.NotNullFlag | mysql.BinaryFlag),
			Decimal:      6,
			ColumnLength: 26,
		}}
		require.NoError(t, conn.writeColumnInfo(cols))

		chk := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeDatetime)}, 1)
		chk.AppendTime(0, time)
		data := make([]byte, 4)
		data, err = column.DumpBinaryRow(data, cols, chk.GetRow(0), conn.rsEncoder)
		require.NoError(t, err)
		require.NoError(t, conn.writePacket(data))

		for i := 0; i < warnCount; i++ {
			conn.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError("any error"))
		}
		require.NoError(t, conn.writeEOF(context.Background(), mysql.ServerStatusAutocommit))
	})
}

func TestDateTimeTypes(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	srv := CreateMockServer(t, store)
	srv.SetDomain(dom)
	defer srv.Close()

	appendUint32 := binary.LittleEndian.AppendUint32
	ctx := context.Background()
	c := CreateMockConn(t, srv).(*mockConn)
	c.capability = mysql.ClientProtocol41 | mysql.ClientDeprecateEOF

	tk := testkit.NewTestKitWithSession(t, store, c.Context().Session)
	tk.MustExec("use test")
	stmt, _, _, err := c.Context().Prepare("select ? as t")
	require.NoError(t, err)

	expectedTimeDatum, err := types.ParseDatetime(types.DefaultStmtNoWarningContext, "2023-11-09 14:23:45.000100")
	require.NoError(t, err)
	expected := expectedDatetimeExecuteResult(t, c, expectedTimeDatum, 1)

	// execute the statement with datetime parameter
	req := append(
		appendUint32([]byte{mysql.ComStmtExecute}, uint32(stmt.ID())),
		0x0, 0x1, 0x0, 0x0, 0x0,
		0x0, 0x1,
	)
	req = append(req, buildDatetimeParam(2023, 11, 9, 14, 23, 45, 100)...)
	out := c.GetOutput()
	require.NoError(t, c.Dispatch(ctx, req))

	require.Equal(t, expected, out.Bytes())
}
