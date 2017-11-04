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
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"reflect"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/testleak"
)

func evalBuiltinFunc(f builtinFunc, row types.Row) (d types.Datum, err error) {
	var (
		res    interface{}
		isNull bool
	)
	switch f.getRetTp().EvalType() {
	case types.ETInt:
		var intRes int64
		intRes, isNull, err = f.evalInt(row)
		if mysql.HasUnsignedFlag(f.getRetTp().Flag) {
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
		d.SetValue(nil)
		return d, errors.Trace(err)
	}
	d.SetValue(res)
	return
}

// tblToDtbl is a util function for test.
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

func (s *testEvaluatorSuite) TestIsNullFunc(c *C) {
	defer testleak.AfterTest(c)()

	fc := funcs[ast.IsNull]
	f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(1)))
	c.Assert(err, IsNil)
	v, err := evalBuiltinFunc(f, nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetInt64(), Equals, int64(0))

	f, err = fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(nil)))
	c.Assert(err, IsNil)
	v, err = evalBuiltinFunc(f, nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetInt64(), Equals, int64(1))
}

func (s *testEvaluatorSuite) TestLock(c *C) {
	defer testleak.AfterTest(c)()

	lock := funcs[ast.GetLock]
	f, err := lock.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(nil, 1)))
	c.Assert(err, IsNil)
	v, err := evalBuiltinFunc(f, nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetInt64(), Equals, int64(1))

	releaseLock := funcs[ast.ReleaseLock]
	f, err = releaseLock.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(1)))
	c.Assert(err, IsNil)
	v, err = evalBuiltinFunc(f, nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetInt64(), Equals, int64(1))
}

// newFunctionForTest creates a new ScalarFunction using funcName and arguments,
// it is different from expression.NewFunction which needs an additional retType argument.
func newFunctionForTest(ctx context.Context, funcName string, args ...Expression) (Expression, error) {
	fc, ok := funcs[funcName]
	if !ok {
		return nil, errFunctionNotExists.GenByArgs("FUNCTION", funcName)
	}
	funcArgs := make([]Expression, len(args))
	copy(funcArgs, args)
	f, err := fc.getFunction(ctx, funcArgs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &ScalarFunction{
		FuncName: model.NewCIStr(funcName),
		RetType:  f.getRetTp(),
		Function: f,
	}, nil
}

var (
	// MySQL int8.
	int8Con = &Constant{RetType: &types.FieldType{Tp: mysql.TypeLonglong, Charset: charset.CharsetBin, Collate: charset.CollationBin}}
	// MySQL varchar.
	varcharCon = &Constant{RetType: &types.FieldType{Tp: mysql.TypeVarchar, Charset: charset.CharsetUTF8, Collate: charset.CollationUTF8}}
)
