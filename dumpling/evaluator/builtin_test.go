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

package evaluator

import (
	"fmt"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/types"
	"reflect"
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

// DatumEquals checker.
type datumEqualsChecker struct {
	*CheckerInfo
}

// The DatumEquals checker verifies that the obtained value is equal to
// the expected value.
// For example:
//     c.Assert(value, DatumEquals, NewDatum(42))
var DatumEquals Checker = &datumEqualsChecker{
	&CheckerInfo{Name: "DatumEquals", Params: []string{"obtained", "expected"}},
}

func (checker *datumEqualsChecker) Check(params []interface{}, names []string) (result bool, error string) {
	defer func() {
		if v := recover(); v != nil {
			result = false
			error = fmt.Sprint(v)
		}
	}()
	paramFirst, ok := params[0].(types.Datum)
	if !ok {
		panic("the first param should be datum")
	}
	paramSecond, ok := params[1].(types.Datum)
	if !ok {
		panic("the second param should be datum")
	}

	res, err := paramFirst.CompareDatum(paramSecond)
	if err != nil {
		panic(err)
	}
	return res == 0, ""
}

func (s *testEvaluatorSuite) TestCoalesce(c *C) {
	args := types.MakeDatums(1, nil)
	v, err := builtinCoalesce(args, nil)
	c.Assert(err, IsNil)
	c.Assert(v, DatumEquals, types.NewDatum(1))

	args = types.MakeDatums(nil, nil)
	v, err = builtinCoalesce(args, nil)
	c.Assert(err, IsNil)
	c.Assert(v, DatumEquals, types.NewDatum(nil))
}
