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
	"reflect"
	"sync"
	"testing"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/stretchr/testify/require"
)

func evalBuiltinFuncConcurrent(f builtinFunc, row chunk.Row) (d types.Datum, err error) {
	var wg util.WaitGroupWrapper
	concurrency := 10
	var lock sync.Mutex
	err = nil
	for i := 0; i < concurrency; i++ {
		wg.Run(func() {
			di, erri := evalBuiltinFunc(f, chunk.Row{})
			lock.Lock()
			if err == nil {
				d, err = di, erri
			}
			lock.Unlock()
		})
	}
	wg.Wait()
	return
}

func evalBuiltinFunc(f builtinFunc, row chunk.Row) (d types.Datum, err error) {
	var (
		res    interface{}
		isNull bool
	)
	switch f.getRetTp().EvalType() {
	case types.ETInt:
		var intRes int64
		intRes, isNull, err = f.evalInt(row)
		if mysql.HasUnsignedFlag(f.getRetTp().GetFlag()) {
			res = uint64(intRes)
		} else {
			res = intRes
		}
	case types.ETReal:
		res, isNull, err = f.evalReal(row)
	case types.ETDecimal:
		res, isNull, err = f.evalDecimal(row)
	case types.ETDatetime, types.ETTimestamp:
		res, isNull, err = f.evalTime(row)
	case types.ETDuration:
		res, isNull, err = f.evalDuration(row)
	case types.ETJson:
		res, isNull, err = f.evalJSON(row)
	case types.ETString:
		res, isNull, err = f.evalString(row)
	}

	if isNull || err != nil {
		d.SetNull()
		return d, err
	}
	d.SetValue(res, f.getRetTp())
	return
}

// tblToDtbl is a utility function for test.
func tblToDtbl(i interface{}) []map[string][]types.Datum {
	l := reflect.ValueOf(i).Len()
	tbl := make([]map[string][]types.Datum, l)
	for j := 0; j < l; j++ {
		v := reflect.ValueOf(i).Index(j).Interface()
		val := reflect.ValueOf(v)
		t := reflect.TypeOf(v)
		item := make(map[string][]types.Datum, val.NumField())
		for k := 0; k < val.NumField(); k++ {
			tmp := val.Field(k).Interface()
			item[t.Field(k).Name] = makeDatums(tmp)
		}
		tbl[j] = item
	}
	return tbl
}

func makeDatums(i interface{}) []types.Datum {
	if i != nil {
		t := reflect.TypeOf(i)
		val := reflect.ValueOf(i)
		switch t.Kind() {
		case reflect.Slice:
			l := val.Len()
			res := make([]types.Datum, l)
			for j := 0; j < l; j++ {
				res[j] = types.NewDatum(val.Index(j).Interface())
			}
			return res
		}
	}
	return types.MakeDatums(i)
}

func TestIsNullFunc(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.IsNull]
	f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(1)))
	require.NoError(t, err)
	v, err := evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, int64(0), v.GetInt64())

	f, err = fc.getFunction(ctx, datumsToConstants(types.MakeDatums(nil)))
	require.NoError(t, err)
	v, err = evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, int64(1), v.GetInt64())
}

func TestLock(t *testing.T) {
	ctx := createContext(t)
	lock := funcs[ast.GetLock]
	f, err := lock.getFunction(ctx, datumsToConstants(types.MakeDatums("mylock", 1)))
	require.NoError(t, err)
	v, err := evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, int64(1), v.GetInt64())

	releaseLock := funcs[ast.ReleaseLock]
	f, err = releaseLock.getFunction(ctx, datumsToConstants(types.MakeDatums("mylock")))
	require.NoError(t, err)
	v, err = evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, int64(1), v.GetInt64())
}

func TestDisplayName(t *testing.T) {
	require.Equal(t, "=", GetDisplayName(ast.EQ))
	require.Equal(t, "<=>", GetDisplayName(ast.NullEQ))
	require.Equal(t, "IS TRUE", GetDisplayName(ast.IsTruthWithoutNull))
	require.Equal(t, "abs", GetDisplayName("abs"))
	require.Equal(t, "other_unknown_func", GetDisplayName("other_unknown_func"))
}

// newFunctionForTest creates a new ScalarFunction using funcName and arguments,
// it is different from expression.NewFunction which needs an additional retType argument.
func newFunctionForTest(ctx sessionctx.Context, funcName string, args ...Expression) (Expression, error) {
	fc, ok := funcs[funcName]
	if !ok {
		return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", funcName)
	}
	funcArgs := make([]Expression, len(args))
	copy(funcArgs, args)
	f, err := fc.getFunction(ctx, funcArgs)
	if err != nil {
		return nil, err
	}
	return &ScalarFunction{
		FuncName: model.NewCIStr(funcName),
		RetType:  f.getRetTp(),
		Function: f,
	}, nil
}

var (
	// MySQL int8.
	int8Con = &Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).SetCharset(charset.CharsetBin).SetCollate(charset.CollationBin).BuildP()}
	// MySQL varchar.
	varcharCon = &Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeVarchar).SetCharset(charset.CharsetUTF8).SetCollate(charset.CollationUTF8).BuildP()}
)

func getInt8Con() Expression {
	return int8Con.Clone()
}

func getVarcharCon() Expression {
	return varcharCon.Clone()
}
