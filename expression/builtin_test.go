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
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

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

func (s *testEvaluatorSuite) TestGreatestLeastFuncs(c *C) {
	defer testleak.AfterTest(c)()

	var datums []types.Datum

	datums = types.MakeDatums(2, 0)
	greatest := funcs[ast.Greatest]
	f, err := greatest.getFunction(datumsToConstants(datums), s.ctx)
	c.Assert(err, IsNil)
	v, err := f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetInt64(), Equals, int64(2))
	least := funcs[ast.Least]
	f, err = least.getFunction(datumsToConstants(datums), s.ctx)
	c.Assert(err, IsNil)
	v, err = f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetInt64(), Equals, int64(0))

	datums = types.MakeDatums(34.0, 3.0, 5.0, 767.0)
	f, err = greatest.getFunction(datumsToConstants(datums), s.ctx)
	c.Assert(err, IsNil)
	v, err = f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetFloat64(), Equals, float64(767.0))
	f, err = least.getFunction(datumsToConstants(datums), s.ctx)
	c.Assert(err, IsNil)
	v, err = f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetFloat64(), Equals, float64(3.0))

	datums = types.MakeDatums("B", "A", "C")
	f, err = greatest.getFunction(datumsToConstants(datums), s.ctx)
	c.Assert(err, IsNil)
	v, err = f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, "C")
	f, err = least.getFunction(datumsToConstants(datums), s.ctx)
	c.Assert(err, IsNil)
	v, err = f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, "A")

	// GREATEST() and LEAST() return NULL if any argument is NULL.
	datums = types.MakeDatums(nil, 1, 2)
	f, err = greatest.getFunction(datumsToConstants(datums), s.ctx)
	c.Assert(err, IsNil)
	v, err = f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.IsNull(), IsTrue)
	f, err = least.getFunction(datumsToConstants(datums), s.ctx)
	c.Assert(err, IsNil)
	v, err = f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.IsNull(), IsTrue)

	datums = types.MakeDatums(1, nil, 2)
	f, err = greatest.getFunction(datumsToConstants(datums), s.ctx)
	c.Assert(err, IsNil)
	v, err = f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.IsNull(), IsTrue)
	f, err = least.getFunction(datumsToConstants(datums), s.ctx)
	c.Assert(err, IsNil)
	v, err = f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.IsNull(), IsTrue)
}

func (s *testEvaluatorSuite) TestIsNullFunc(c *C) {
	defer testleak.AfterTest(c)()

	fc := funcs[ast.IsNull]
	f, err := fc.getFunction(datumsToConstants(types.MakeDatums(1)), s.ctx)
	c.Assert(err, IsNil)
	v, err := f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetInt64(), Equals, int64(0))

	f, err = fc.getFunction(datumsToConstants(types.MakeDatums(nil)), s.ctx)
	c.Assert(err, IsNil)
	v, err = f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetInt64(), Equals, int64(1))
}

func (s *testEvaluatorSuite) TestLock(c *C) {
	defer testleak.AfterTest(c)()

	lock := funcs[ast.GetLock]
	f, err := lock.getFunction(datumsToConstants(types.MakeDatums(nil, 1)), s.ctx)
	c.Assert(err, IsNil)
	v, err := f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetInt64(), Equals, int64(1))

	releaseLock := funcs[ast.ReleaseLock]
	f, err = releaseLock.getFunction(datumsToConstants(types.MakeDatums(1)), s.ctx)
	c.Assert(err, IsNil)
	v, err = f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetInt64(), Equals, int64(1))
}

// newFunctionForTest creates a new ScalarFunction using funcName and arguments,
// it is different from expression.NewFunction which needs an additional retType argument.
func newFunctionForTest(ctx context.Context, funcName string, args ...Expression) (Expression, error) {
	fc, ok := funcs[funcName]
	if !ok {
		return nil, errFunctionNotExists.GenByArgs(funcName)
	}
	funcArgs := make([]Expression, len(args))
	copy(funcArgs, args)
	f, err := fc.getFunction(funcArgs, ctx)
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
	// MySQL decimal.
	decimalCon = &Constant{RetType: &types.FieldType{Tp: mysql.TypeNewDecimal, Charset: charset.CharsetBin, Collate: charset.CollationBin}}
	// MySQL float.
	floatCon = &Constant{RetType: &types.FieldType{Tp: mysql.TypeFloat, Charset: charset.CharsetBin, Collate: charset.CollationBin}}
	// MySQL double.
	doubleCon = &Constant{RetType: &types.FieldType{Tp: mysql.TypeDouble, Charset: charset.CharsetBin, Collate: charset.CollationBin}}
	// MySQL char.
	charCon = &Constant{RetType: &types.FieldType{Tp: mysql.TypeString, Charset: charset.CharsetUTF8, Collate: charset.CollationUTF8}}
	// MySQL binary.
	binaryCon = &Constant{RetType: &types.FieldType{Tp: mysql.TypeString, Charset: charset.CharsetBin, Collate: charset.CollationBin, Flag: mysql.BinaryFlag}}
	// MySQL varchar.
	varcharCon = &Constant{RetType: &types.FieldType{Tp: mysql.TypeVarchar, Charset: charset.CharsetUTF8, Collate: charset.CollationUTF8}}
	// MySQL varbinary.
	varbinaryCon = &Constant{RetType: &types.FieldType{Tp: mysql.TypeVarchar, Charset: charset.CharsetBin, Collate: charset.CollationBin, Flag: mysql.BinaryFlag}}
	// MySQL text.
	textCon = &Constant{RetType: &types.FieldType{Tp: mysql.TypeBlob, Charset: charset.CharsetUTF8, Collate: charset.CollationUTF8}}
	// MySQL blob.
	blobCon = &Constant{RetType: &types.FieldType{Tp: mysql.TypeBlob, Charset: charset.CharsetBin, Collate: charset.CollationBin, Flag: mysql.BinaryFlag}}
)
