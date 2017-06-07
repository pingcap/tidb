// Copyright 2017 PingCAP, Inc.
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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tidb/util/types/json"
)

func (s *testEvaluatorSuite) TestJSONType(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.JSONType]
	tbl := []struct {
		Input    interface{}
		Expected interface{}
	}{
		{nil, nil},
		{`3`, `INTEGER`},
		{`3.0`, `DOUBLE`},
		{`null`, `NULL`},
		{`true`, `BOOLEAN`},
		{`[]`, `ARRAY`},
		{`{}`, `OBJECT`},
	}
	dtbl := tblToDtbl(tbl)
	for _, t := range dtbl {
		f, err := fc.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		d, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(d, testutil.DatumEquals, t["Expected"][0])
	}
}

func (s *testEvaluatorSuite) TestJSONUnquote(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.JSONUnquote]
	tbl := []struct {
		Input    interface{}
		Expected interface{}
	}{
		{nil, nil},
		{`"a"`, `a`},
		{`3`, `3`},
		{`{"a": "b"}`, `{"a":"b"}`},
		{`"hello,\"quoted string\",world"`, `hello,"quoted string",world`},
		{`"hello,\"宽字符\",world"`, `hello,"宽字符",world`},
	}
	dtbl := tblToDtbl(tbl)
	for _, t := range dtbl {
		f, err := fc.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		d, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(d, testutil.DatumEquals, t["Expected"][0])
	}
}

func (s *testEvaluatorSuite) TestJSONExtract(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.JSONExtract]
	jstr := `{"a": [{"aa": [{"aaa": 1}]}], "aaa": 2}`
	tbl := []struct {
		Input    []interface{}
		Expected interface{}
		Success  bool
	}{
		{[]interface{}{nil, nil}, nil, true},
		{[]interface{}{jstr, `$.a[0].aa[0].aaa`, `$.aaa`}, `[1, 2]`, true},
		{[]interface{}{jstr, `$.a[0].aa[0].aaa`, `$InvalidPath`}, nil, false},
	}
	for _, t := range tbl {
		args := types.MakeDatums(t.Input...)
		f, err := fc.getFunction(datumsToConstants(args), s.ctx)
		c.Assert(err, IsNil)
		d, err := f.eval(nil)
		if t.Success {
			c.Assert(err, IsNil)
			switch x := t.Expected.(type) {
			case string:
				j1, err := json.ParseFromString(x)
				c.Assert(err, IsNil)
				j2 := d.GetMysqlJSON()
				cmp, err := json.CompareJSON(j1, j2)
				c.Assert(err, IsNil)
				c.Assert(cmp, Equals, 0)
			}
		} else {
			c.Assert(err, NotNil)
		}
	}
}

// TestJSONSetInsertReplace tests grammar of json_{set,insert,replace}.
func (s *testEvaluatorSuite) TestJSONSetInsertReplace(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		fc       functionClass
		Input    []interface{}
		Expected interface{}
		Success  bool
	}{
		{funcs[ast.JSONSet], []interface{}{nil, nil, nil}, nil, true},
		{funcs[ast.JSONSet], []interface{}{`{}`, `$.a`, 3}, `{"a": 3}`, true},
		{funcs[ast.JSONInsert], []interface{}{`{}`, `$.a`, 3}, `{"a": 3}`, true},
		{funcs[ast.JSONReplace], []interface{}{`{}`, `$.a`, 3}, `{}`, true},
		{funcs[ast.JSONSet], []interface{}{`{}`, `$.a`, 3, `$.b`, "3"}, `{"a": 3, "b": "3"}`, true},
		{funcs[ast.JSONSet], []interface{}{`{}`, `$.a`, 3, `$.b`}, nil, false},
		{funcs[ast.JSONSet], []interface{}{`{}`, `$InvalidPath`, 3}, nil, false},
	}
	for _, t := range tbl {
		args := types.MakeDatums(t.Input...)
		f, err := t.fc.getFunction(datumsToConstants(args), s.ctx)
		c.Assert(err, IsNil)
		d, err := f.eval(nil)
		if t.Success {
			c.Assert(err, IsNil)
			switch x := t.Expected.(type) {
			case string:
				j1, err := json.ParseFromString(x)
				c.Assert(err, IsNil)
				j2 := d.GetMysqlJSON()
				cmp, err := json.CompareJSON(j1, j2)
				c.Assert(err, IsNil)
				c.Assert(cmp, Equals, 0)
			}
		} else {
			c.Assert(err, NotNil)
		}
	}
}

func (s *testEvaluatorSuite) TestJSONMerge(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.JSONMerge]
	tbl := []struct {
		Input    []interface{}
		Expected interface{}
	}{
		{[]interface{}{nil, nil}, nil},
		{[]interface{}{`{}`, `[]`}, `[{}]`},
		{[]interface{}{`{}`, `[]`, `3`, `"4"`}, `[{}, 3, "4"]`},
	}
	for _, t := range tbl {
		args := types.MakeDatums(t.Input...)
		f, err := fc.getFunction(datumsToConstants(args), s.ctx)
		c.Assert(err, IsNil)
		d, err := f.eval(nil)
		c.Assert(err, IsNil)

		switch x := t.Expected.(type) {
		case string:
			j1, err := json.ParseFromString(x)
			c.Assert(err, IsNil)
			j2 := d.GetMysqlJSON()
			cmp, err := json.CompareJSON(j1, j2)
			c.Assert(err, IsNil)
			c.Assert(cmp, Equals, 0)
		}
	}
}
