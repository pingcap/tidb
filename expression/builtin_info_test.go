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

package expression

import (
	"math"
	"testing"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/testkit/testutil"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/printer"
	"github.com/stretchr/testify/require"
)

func TestDatabase(t *testing.T) {
	fc := funcs[ast.Database]
	ctx := mock.NewContext()
	f, err := fc.getFunction(ctx, nil)
	require.NoError(t, err)
	d, err := evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, types.KindNull, d.Kind())
	ctx.GetSessionVars().CurrentDB = "test"
	d, err = evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, "test", d.GetString())
	require.Equal(t, f.PbCode(), f.Clone().PbCode())

	// Test case for schema().
	fc = funcs[ast.Schema]
	require.NotNil(t, fc)
	f, err = fc.getFunction(ctx, nil)
	require.NoError(t, err)
	d, err = evalBuiltinFunc(f, chunk.MutRowFromDatums(types.MakeDatums()).ToRow())
	require.NoError(t, err)
	require.Equal(t, "test", d.GetString())
	require.Equal(t, f.PbCode(), f.Clone().PbCode())
}

func TestFoundRows(t *testing.T) {
	ctx := mock.NewContext()
	sessionVars := ctx.GetSessionVars()
	sessionVars.LastFoundRows = 2

	fc := funcs[ast.FoundRows]
	f, err := fc.getFunction(ctx, nil)
	require.NoError(t, err)
	d, err := evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, uint64(2), d.GetUint64())
}

func TestUser(t *testing.T) {
	ctx := mock.NewContext()
	sessionVars := ctx.GetSessionVars()
	sessionVars.User = &auth.UserIdentity{Username: "root", Hostname: "localhost"}

	fc := funcs[ast.User]
	f, err := fc.getFunction(ctx, nil)
	require.NoError(t, err)
	d, err := evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, "root@localhost", d.GetString())
	require.Equal(t, f.PbCode(), f.Clone().PbCode())
}

func TestCurrentUser(t *testing.T) {
	ctx := mock.NewContext()
	sessionVars := ctx.GetSessionVars()
	sessionVars.User = &auth.UserIdentity{Username: "root", Hostname: "localhost", AuthUsername: "root", AuthHostname: "localhost"}

	fc := funcs[ast.CurrentUser]
	f, err := fc.getFunction(ctx, nil)
	require.NoError(t, err)
	d, err := evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, "root@localhost", d.GetString())
	require.Equal(t, f.PbCode(), f.Clone().PbCode())
}

func TestCurrentRole(t *testing.T) {
	ctx := mock.NewContext()
	fc := funcs[ast.CurrentRole]
	f, err := fc.getFunction(ctx, nil)
	require.NoError(t, err)

	// empty roles
	var d types.Datum
	d, err = evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, "NONE", d.GetString())
	require.Equal(t, f.PbCode(), f.Clone().PbCode())

	// add roles
	sessionVars := ctx.GetSessionVars()
	sessionVars.ActiveRoles = make([]*auth.RoleIdentity, 0, 10)
	sessionVars.ActiveRoles = append(sessionVars.ActiveRoles, &auth.RoleIdentity{Username: "r_1", Hostname: "%"})
	sessionVars.ActiveRoles = append(sessionVars.ActiveRoles, &auth.RoleIdentity{Username: "r_2", Hostname: "localhost"})

	d, err = evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, "`r_1`@`%`,`r_2`@`localhost`", d.GetString())
	require.Equal(t, f.PbCode(), f.Clone().PbCode())
}

func TestConnectionID(t *testing.T) {
	ctx := mock.NewContext()
	sessionVars := ctx.GetSessionVars()
	sessionVars.ConnectionID = uint64(1)

	fc := funcs[ast.ConnectionID]
	f, err := fc.getFunction(ctx, nil)
	require.NoError(t, err)
	d, err := evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, uint64(1), d.GetUint64())
	require.Equal(t, f.PbCode(), f.Clone().PbCode())
}

func TestVersion(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.Version]
	f, err := fc.getFunction(ctx, nil)
	require.NoError(t, err)
	v, err := evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, mysql.ServerVersion, v.GetString())
	require.Equal(t, f.PbCode(), f.Clone().PbCode())
}

func TestBenchMark(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		LoopCount  int
		Expression interface{}
		Expected   int64
		IsNil      bool
	}{
		{-3, 1, 0, true},
		{0, 1, 0, false},
		{3, 1, 0, false},
		{3, 1.234, 0, false},
		{3, types.NewDecFromFloatForTest(1.234), 0, false},
		{3, "abc", 0, false},
		{3, types.CurrentTime(mysql.TypeDatetime), 0, false},
		{3, types.CurrentTime(mysql.TypeTimestamp), 0, false},
		{3, types.CurrentTime(mysql.TypeDuration), 0, false},
		{3, json.CreateBinary("[1]"), 0, false},
	}

	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.Benchmark, primitiveValsToConstants(ctx, []interface{}{
			c.LoopCount,
			c.Expression,
		})...)
		require.NoError(t, err)

		d, err := f.Eval(chunk.Row{})
		require.NoError(t, err)
		if c.IsNil {
			require.True(t, d.IsNull())
		} else {
			require.Equal(t, c.Expected, d.GetInt64())
		}

		// test clone
		b1 := f.Clone().(*ScalarFunction).Function.(*builtinBenchmarkSig)
		require.Equal(t, int64(c.LoopCount), b1.constLoopCount)
	}
}

func TestCharset(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.Charset]
	f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(nil)))
	require.NotNil(t, f)
	require.NoError(t, err)
	require.Equal(t, 64, f.getRetTp().GetFlen())
}

func TestCoercibility(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.Coercibility]
	f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(nil)))
	require.NotNil(t, f)
	require.NoError(t, err)
}

func TestCollation(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.Collation]
	f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(nil)))
	require.NotNil(t, f)
	require.NoError(t, err)
	require.Equal(t, 64, f.getRetTp().GetFlen())
}

func TestRowCount(t *testing.T) {
	ctx := mock.NewContext()
	sessionVars := ctx.GetSessionVars()
	sessionVars.StmtCtx.PrevAffectedRows = 10

	f, err := funcs[ast.RowCount].getFunction(ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, f)
	sig, ok := f.(*builtinRowCountSig)
	require.True(t, ok)
	require.NotNil(t, sig)
	intResult, isNull, err := sig.evalInt(chunk.Row{})
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, int64(10), intResult)
	require.Equal(t, f.PbCode(), f.Clone().PbCode())
}

// TestTiDBVersion for tidb_server().
func TestTiDBVersion(t *testing.T) {
	ctx := createContext(t)
	f, err := newFunctionForTest(ctx, ast.TiDBVersion, primitiveValsToConstants(ctx, []interface{}{})...)
	require.NoError(t, err)
	v, err := f.Eval(chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, printer.GetTiDBInfo(), v.GetString())
}

func TestLastInsertID(t *testing.T) {
	ctx := createContext(t)
	maxUint64 := uint64(math.MaxUint64)
	cases := []struct {
		insertID uint64
		args     interface{}
		expected uint64
		isNil    bool
		getErr   bool
	}{
		{0, 1, 1, false, false},
		{0, 1.1, 1, false, false},
		{0, maxUint64, maxUint64, false, false},
		{0, -1, math.MaxUint64, false, false},
		{1, nil, 1, false, false},
		{math.MaxUint64, nil, math.MaxUint64, false, false},
	}

	for _, c := range cases {
		var (
			f   Expression
			err error
		)
		if c.insertID > 0 {
			ctx.GetSessionVars().StmtCtx.PrevLastInsertID = c.insertID
		}

		if c.args != nil {
			f, err = newFunctionForTest(ctx, ast.LastInsertId, primitiveValsToConstants(ctx, []interface{}{c.args})...)
		} else {
			f, err = newFunctionForTest(ctx, ast.LastInsertId)
		}
		tp := f.GetType()
		require.NoError(t, err)
		require.Equal(t, mysql.TypeLonglong, tp.GetType())
		require.Equal(t, charset.CharsetBin, tp.GetCharset())
		require.Equal(t, charset.CollationBin, tp.GetCollate())
		require.Equal(t, mysql.BinaryFlag, tp.GetFlag()&mysql.BinaryFlag)
		require.Equal(t, mysql.MaxIntWidth, tp.GetFlen())
		d, err := f.Eval(chunk.Row{})
		if c.getErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			if c.isNil {
				require.Equal(t, types.KindNull, d.Kind())
			} else {
				require.Equal(t, c.expected, d.GetUint64())
			}
		}
	}

	_, err := funcs[ast.LastInsertId].getFunction(ctx, []Expression{NewZero()})
	require.NoError(t, err)
}

func TestFormatBytes(t *testing.T) {
	ctx := createContext(t)
	tbl := []struct {
		Arg interface{}
		Ret interface{}
	}{
		{nil, nil},
		{float64(0), "0 bytes"},
		{float64(2048), "2.00 KiB"},
		{float64(75295729), "71.81 MiB"},
		{float64(5287242702), "4.92 GiB"},
		{float64(5039757204245), "4.58 TiB"},
		{float64(890250274520475525), "790.70 PiB"},
		{float64(18446644073709551615), "16.00 EiB"},
		{float64(287952852482075252752429875), "2.50e+08 EiB"},
		{float64(-18446644073709551615), "-16.00 EiB"},
	}
	Dtbl := tblToDtbl(tbl)

	for _, tt := range Dtbl {
		fc := funcs[ast.FormatBytes]
		f, err := fc.getFunction(ctx, datumsToConstants(tt["Arg"]))
		require.NoError(t, err)
		v, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, tt["Ret"][0], v)
	}
}

func TestFormatNanoTime(t *testing.T) {
	ctx := createContext(t)
	tbl := []struct {
		Arg interface{}
		Ret interface{}
	}{
		{nil, nil},
		{float64(0), "0 ns"},
		{float64(2000), "2.00 us"},
		{float64(898787877), "898.79 ms"},
		{float64(9999999991), "10.00 s"},
		{float64(898787877424), "14.98 min"},
		{float64(5827527520021), "1.62 h"},
		{float64(42566623663736353), "492.67 d"},
		{float64(4827524825702572425242552), "5.59e+10 d"},
		{float64(-9999999991), "-10.00 s"},
	}
	Dtbl := tblToDtbl(tbl)

	for _, tt := range Dtbl {
		fc := funcs[ast.FormatNanoTime]
		f, err := fc.getFunction(ctx, datumsToConstants(tt["Arg"]))
		require.NoError(t, err)
		v, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, tt["Ret"][0], v)
	}
}
