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
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func evalBuiltinFuncConcurrent(f builtinFunc, ctx EvalContext, row chunk.Row) (d types.Datum, err error) {
	var wg util.WaitGroupWrapper
	concurrency := 10
	var lock sync.Mutex
	err = nil
	for i := 0; i < concurrency; i++ {
		wg.Run(func() {
			di, erri := evalBuiltinFunc(f, ctx, chunk.Row{})
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

func evalBuiltinFunc(f builtinFunc, ctx EvalContext, row chunk.Row) (d types.Datum, err error) {
	ctx = wrapEvalAssert(ctx, f)
	var (
		res    any
		isNull bool
	)
	switch f.getRetTp().EvalType() {
	case types.ETInt:
		var intRes int64
		intRes, isNull, err = f.evalInt(ctx, row)
		if mysql.HasUnsignedFlag(f.getRetTp().GetFlag()) {
			res = uint64(intRes)
		} else {
			res = intRes
		}
	case types.ETReal:
		res, isNull, err = f.evalReal(ctx, row)
	case types.ETDecimal:
		res, isNull, err = f.evalDecimal(ctx, row)
	case types.ETDatetime, types.ETTimestamp:
		res, isNull, err = f.evalTime(ctx, row)
	case types.ETDuration:
		res, isNull, err = f.evalDuration(ctx, row)
	case types.ETJson:
		res, isNull, err = f.evalJSON(ctx, row)
	case types.ETString:
		res, isNull, err = f.evalString(ctx, row)
	}

	d.SetValue(res, f.getRetTp())
	if isNull {
		d.SetNull()
		return d, err
	}
	return
}

// tblToDtbl is a utility function for test.
func tblToDtbl(i any) []map[string][]types.Datum {
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

func makeDatums(i any) []types.Datum {
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
	v, err := evalBuiltinFunc(f, ctx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, int64(0), v.GetInt64())

	f, err = fc.getFunction(ctx, datumsToConstants(types.MakeDatums(nil)))
	require.NoError(t, err)
	v, err = evalBuiltinFunc(f, ctx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, int64(1), v.GetInt64())
}

func TestLock(t *testing.T) {
	ctx := createContext(t)
	lock := funcs[ast.GetLock]
	f, err := lock.getFunction(ctx, datumsToConstants(types.MakeDatums("mylock", 1)))
	require.NoError(t, err)
	v, err := evalBuiltinFunc(f, ctx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, int64(1), v.GetInt64())

	releaseLock := funcs[ast.ReleaseLock]
	f, err = releaseLock.getFunction(ctx, datumsToConstants(types.MakeDatums("mylock")))
	require.NoError(t, err)
	v, err = evalBuiltinFunc(f, ctx, chunk.Row{})
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

func TestBuiltinFuncCacheConcurrency(t *testing.T) {
	cache := builtinFuncCache[int]{}
	ctx := createContext(t)

	var invoked atomic.Int64
	construct := func() (int, error) {
		invoked.Add(1)
		time.Sleep(time.Millisecond)
		return 100 + int(invoked.Load()), nil
	}

	var wg sync.WaitGroup
	concurrency := 8
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			v, err := cache.getOrInitCache(ctx, construct)
			// all goroutines should get the same value
			require.NoError(t, err)
			require.Equal(t, 101, v)
		}()
	}

	wg.Wait()
	// construct will only be called once even in concurrency
	require.Equal(t, int64(1), invoked.Load())
}

func TestBuiltinFuncCache(t *testing.T) {
	cache := builtinFuncCache[int]{}
	ctx := createContext(t)

	// ok should be false when no cache present
	v, ok := cache.getCache(ctx.GetSessionVars().StmtCtx.CtxID())
	require.Equal(t, 0, v)
	require.False(t, ok)

	// getCache should not init cache
	v, ok = cache.getCache(ctx.GetSessionVars().StmtCtx.CtxID())
	require.Equal(t, 0, v)
	require.False(t, ok)

	var invoked atomic.Int64
	returnError := false
	construct := func() (int, error) {
		invoked.Add(1)
		if returnError {
			return 128, errors.New("mockError")
		}
		return 100 + int(invoked.Load()), nil
	}

	// the first getOrInitCache should init cache
	v, err := cache.getOrInitCache(ctx, construct)
	require.NoError(t, err)
	require.Equal(t, 101, v)
	require.Equal(t, int64(1), invoked.Load())

	// get should return the cache
	v, ok = cache.getCache(ctx.GetSessionVars().StmtCtx.CtxID())
	require.Equal(t, 101, v)
	require.True(t, ok)

	// the second should use the cached one
	v, err = cache.getOrInitCache(ctx, construct)
	require.NoError(t, err)
	require.Equal(t, 101, v)
	require.Equal(t, int64(1), invoked.Load())

	// if ctxID changed, should re-init cache
	ctx = createContext(t)
	v, err = cache.getOrInitCache(ctx, construct)
	require.NoError(t, err)
	require.Equal(t, 102, v)
	require.Equal(t, int64(2), invoked.Load())
	v, ok = cache.getCache(ctx.GetSessionVars().StmtCtx.CtxID())
	require.Equal(t, 102, v)
	require.True(t, ok)

	// error should be returned
	ctx = createContext(t)
	returnError = true
	v, err = cache.getOrInitCache(ctx, construct)
	require.Equal(t, 0, v)
	require.EqualError(t, err, "mockError")

	// error should not be cached
	returnError = false
	v, err = cache.getOrInitCache(ctx, construct)
	require.NoError(t, err)
	require.Equal(t, 104, v)
}

// newFunctionForTest creates a new ScalarFunction using funcName and arguments,
// it is different from expression.NewFunction which needs an additional retType argument.
func newFunctionForTest(ctx BuildContext, funcName string, args ...Expression) (Expression, error) {
	fc, ok := funcs[funcName]
	if !ok {
		return nil, ErrFunctionNotExists.GenWithStackByArgs("FUNCTION", funcName)
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
