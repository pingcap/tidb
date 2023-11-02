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

package parse

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/server/internal/util"
	"github.com/pingcap/tidb/pkg/types"
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
		warn   error
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
			types.Duration{types.MinTime, types.MaxFsp},
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
			types.Duration{types.MinTime, types.MaxFsp},
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
			types.Duration{time.Duration(0), types.MaxFsp},
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
		typectx := types.NewContext(types.DefaultStmtFlags.WithTruncateAsWarning(true), time.UTC, func(err error) {
			warn = err
		})
		err := ExecArgs(typectx, tt.args.args, tt.args.boundParams, tt.args.nullBitmap, tt.args.paramTypes, tt.args.paramValues, nil)
		require.Truef(t, terror.ErrorEqual(err, tt.err), "err %v", err)
		require.Truef(t, terror.ErrorEqual(warn, tt.warn), "warn %v", warn)
		if err == nil {
			require.Equal(t, tt.expect, tt.args.args[0].(*expression.Constant).Value.GetValue())
		}
	}
}

func TestParseExecArgsAndEncode(t *testing.T) {
	dt := expression.Args2Expressions4Test(1)
	err := ExecArgs(types.DefaultStmtNoWarningContext,
		dt,
		[][]byte{nil},
		[]byte{0x0},
		[]byte{mysql.TypeVarchar, 0},
		[]byte{4, 178, 226, 202, 212},
		util.NewInputDecoder("gbk"))
	require.NoError(t, err)
	require.Equal(t, "测试", dt[0].(*expression.Constant).Value.GetValue())

	err = ExecArgs(types.DefaultStmtNoWarningContext,
		dt,
		[][]byte{{178, 226, 202, 212}},
		[]byte{0x0},
		[]byte{mysql.TypeString, 0},
		[]byte{},
		util.NewInputDecoder("gbk"))
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
		stmtID, fetchSize, err := StmtFetchCmd(tc.arg)
		require.Equal(t, tc.stmtID, stmtID)
		require.Equal(t, tc.fetchSize, fetchSize)
		require.Equal(t, tc.err, err)
	}
}
