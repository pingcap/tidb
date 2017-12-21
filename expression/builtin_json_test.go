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
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
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
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, nil)
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
		{``, ``},
		{`""`, ``},
		{`''`, `''`},
		{`"a"`, `a`},
		{`3`, `3`},
		{`{"a": "b"}`, `{"a": "b"}`},
		{`{"a":     "b"}`, `{"a":     "b"}`},
		{`"hello,\"quoted string\",world"`, `hello,"quoted string",world`},
		{`"hello,\"宽字符\",world"`, `hello,"宽字符",world`},
		{`Invalid Json string\tis OK`, `Invalid Json string	is OK`},
	}
	dtbl := tblToDtbl(tbl)
	for _, t := range dtbl {
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, nil)
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
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(args))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, nil)
		if t.Success {
			c.Assert(err, IsNil)
			switch x := t.Expected.(type) {
			case string:
				var j1 json.BinaryJSON
				j1, err = json.ParseBinaryFromString(x)
				c.Assert(err, IsNil)
				j2 := d.GetMysqlJSON()
				var cmp int
				cmp = json.CompareBinary(j1, j2)
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
		fc           functionClass
		Input        []interface{}
		Expected     interface{}
		BuildSuccess bool
		Success      bool
	}{
		{funcs[ast.JSONSet], []interface{}{nil, nil, nil}, nil, true, true},
		{funcs[ast.JSONSet], []interface{}{`{}`, `$.a`, 3}, `{"a": 3}`, true, true},
		{funcs[ast.JSONInsert], []interface{}{`{}`, `$.a`, 3}, `{"a": 3}`, true, true},
		{funcs[ast.JSONReplace], []interface{}{`{}`, `$.a`, 3}, `{}`, true, true},
		{funcs[ast.JSONSet], []interface{}{`{}`, `$.a`, 3, `$.b`, "3"}, `{"a": 3, "b": "3"}`, true, true},
		{funcs[ast.JSONSet], []interface{}{`{}`, `$.a`, nil, `$.b`, "nil"}, `{"a": null, "b": "nil"}`, true, true},
		{funcs[ast.JSONSet], []interface{}{`{}`, `$.a`, 3, `$.b`}, nil, false, false},
		{funcs[ast.JSONSet], []interface{}{`{}`, `$InvalidPath`, 3}, nil, true, false},
	}
	var err error
	var f builtinFunc
	var d types.Datum
	for _, t := range tbl {
		args := types.MakeDatums(t.Input...)
		f, err = t.fc.getFunction(s.ctx, s.datumsToConstants(args))
		if t.BuildSuccess {
			c.Assert(err, IsNil)
			d, err = evalBuiltinFunc(f, nil)
			if t.Success {
				c.Assert(err, IsNil)
				switch x := t.Expected.(type) {
				case string:
					var j1 json.BinaryJSON
					j1, err = json.ParseBinaryFromString(x)
					c.Assert(err, IsNil)
					j2 := d.GetMysqlJSON()
					var cmp int
					cmp = json.CompareBinary(j1, j2)
					c.Assert(cmp, Equals, 0)
				}
				continue
			}
		}
		c.Assert(err, NotNil)
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
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(args))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, nil)
		c.Assert(err, IsNil)

		switch x := t.Expected.(type) {
		case string:
			j1, err := json.ParseBinaryFromString(x)
			c.Assert(err, IsNil)
			j2 := d.GetMysqlJSON()
			cmp := json.CompareBinary(j1, j2)
			c.Assert(cmp, Equals, 0, Commentf("got %v expect %v", j1.String(), j2.String()))
		}
	}
}

func (s *testEvaluatorSuite) TestJSONArray(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.JSONArray]
	tbl := []struct {
		Input    []interface{}
		Expected string
	}{
		{[]interface{}{1}, `[1]`},
		{[]interface{}{nil, "a", 3, `{"a": "b"}`}, `[null, "a", 3, "{\"a\": \"b\"}"]`},
	}
	for _, t := range tbl {
		args := types.MakeDatums(t.Input...)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(args))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, nil)
		c.Assert(err, IsNil)

		j1, err := json.ParseBinaryFromString(t.Expected)
		c.Assert(err, IsNil)
		j2 := d.GetMysqlJSON()
		cmp := json.CompareBinary(j1, j2)
		c.Assert(cmp, Equals, 0)
	}
}

func (s *testEvaluatorSuite) TestJSONObject(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.JSONObject]
	tbl := []struct {
		Input        []interface{}
		Expected     interface{}
		BuildSuccess bool
		Success      bool
	}{
		{[]interface{}{1, 2, 3}, nil, false, false},
		{[]interface{}{1, 2, "hello", nil}, `{"1": 2, "hello": null}`, true, true},
		{[]interface{}{nil, 2}, nil, true, false},

		// TiDB can only tell booleans from parser.
		{[]interface{}{1, true}, `{"1": 1}`, true, true},
	}
	var err error
	var f builtinFunc
	var d types.Datum
	for _, t := range tbl {
		args := types.MakeDatums(t.Input...)
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(args))
		if t.BuildSuccess {
			c.Assert(err, IsNil)
			d, err = evalBuiltinFunc(f, nil)
			if t.Success {
				c.Assert(err, IsNil)
				switch x := t.Expected.(type) {
				case string:
					var j1 json.BinaryJSON
					j1, err = json.ParseBinaryFromString(x)
					c.Assert(err, IsNil)
					j2 := d.GetMysqlJSON()
					var cmp int
					cmp = json.CompareBinary(j1, j2)
					c.Assert(cmp, Equals, 0)
				}
				continue
			}
		}
		c.Assert(err, NotNil)
	}
}

func (s *testEvaluatorSuite) TestJSONORemove(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.JSONRemove]
	tbl := []struct {
		Input    []interface{}
		Expected interface{}
		Success  bool
	}{
		{[]interface{}{`{"a": [1, 2, {"aa": "xx"}]}`, "$"}, nil, false},
		{[]interface{}{`{"a": [1, 2, {"aa": "xx"}]}`, "$.*"}, nil, false},
		{[]interface{}{`{"a": [1, 2, {"aa": "xx"}]}`, "$[*]"}, nil, false},
		{[]interface{}{`{"a": [1, 2, {"aa": "xx"}]}`, "$**.a"}, nil, false},

		{[]interface{}{nil, "$.a"}, nil, true},
		{[]interface{}{`{"a": [1, 2, {"aa": "xx"}]}`, "$.a[2].aa"}, `{"a": [1, 2, {}]}`, true},
		{[]interface{}{`{"a": [1, 2, {"aa": "xx"}]}`, "$.a[1]"}, `{"a": [1, {"aa": "xx"}]}`, true},

		// Tests multi path expressions.
		{[]interface{}{`{"a": [1, 2, {"aa": "xx"}]}`, "$.a[2].aa", "$.a[1]"}, `{"a": [1, {}]}`, true},
		{[]interface{}{`{"a": [1, 2, {"aa": "xx"}]}`, "$.a[1]", "$.a[1].aa"}, `{"a": [1, {}]}`, true},

		// Tests path expressions not exists.
		{[]interface{}{`{"a": [1, 2, {"aa": "xx"}]}`, "$.a[3]"}, `{"a": [1, 2, {"aa": "xx"}]}`, true},
		{[]interface{}{`{"a": [1, 2, {"aa": "xx"}]}`, "$.b"}, `{"a": [1, 2, {"aa": "xx"}]}`, true},
		{[]interface{}{`{"a": [1, 2, {"aa": "xx"}]}`, "$.a[3]", "$.b"}, `{"a": [1, 2, {"aa": "xx"}]}`, true},
	}
	for _, t := range tbl {
		args := types.MakeDatums(t.Input...)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(args))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, nil)

		if t.Success {
			c.Assert(err, IsNil)
			switch x := t.Expected.(type) {
			case string:
				var j1 json.BinaryJSON
				j1, err = json.ParseBinaryFromString(x)
				c.Assert(err, IsNil)
				j2 := d.GetMysqlJSON()
				var cmp int
				cmp = json.CompareBinary(j1, j2)
				c.Assert(cmp, Equals, 0, Commentf("got %v expect %v", j2.Value, j1.Value))
			}
		} else {
			c.Assert(err, NotNil)
		}
	}
}
