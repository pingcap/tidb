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
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
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
		d, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(d, testutil.DatumEquals, t["Expected"][0])
	}
}

func (s *testEvaluatorSuite) TestJSONQuote(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.JSONQuote]
	tbl := []struct {
		Input    interface{}
		Expected interface{}
	}{
		{nil, nil},
		{``, `""`},
		{`""`, `"\"\""`},
		{`a`, `"a"`},
		{`3`, `"3"`},
		{`{"a": "b"}`, `"{\"a\": \"b\"}"`},
		{`{"a":     "b"}`, `"{\"a\":     \"b\"}"`},
		{`hello,"quoted string",world`, `"hello,\"quoted string\",world"`},
		{`hello,"宽字符",world`, `"hello,\"宽字符\",world"`},
		{`Invalid Json string	is OK`, `"Invalid Json string\tis OK"`},
		{`1\u2232\u22322`, `"1\\u2232\\u22322"`},
	}
	dtbl := tblToDtbl(tbl)
	for _, t := range dtbl {
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
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
		{`Invalid Json string\tis OK`, `Invalid Json string\tis OK`},
		{`"1\\u2232\\u22322"`, `1\u2232\u22322`},
		{`"[{\"x\":\"{\\\"y\\\":12}\"}]"`, `[{"x":"{\"y\":12}"}]`},
		{`[{\"x\":\"{\\\"y\\\":12}\"}]`, `[{\"x\":\"{\\\"y\\\":12}\"}]`},
	}
	dtbl := tblToDtbl(tbl)
	for _, t := range dtbl {
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
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
		d, err := evalBuiltinFunc(f, chunk.Row{})
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
			d, err = evalBuiltinFunc(f, chunk.Row{})
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
		d, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)

		switch x := t.Expected.(type) {
		case string:
			j1, err := json.ParseBinaryFromString(x)
			c.Assert(err, IsNil)
			j2 := d.GetMysqlJSON()
			cmp := json.CompareBinary(j1, j2)
			c.Assert(cmp, Equals, 0, Commentf("got %v expect %v", j1.String(), j2.String()))
		case nil:
			c.Assert(d.IsNull(), IsTrue)
		}
	}
}

func (s *testEvaluatorSuite) TestJSONMergePreserve(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.JSONMergePreserve]
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
		d, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)

		switch x := t.Expected.(type) {
		case string:
			j1, err := json.ParseBinaryFromString(x)
			c.Assert(err, IsNil)
			j2 := d.GetMysqlJSON()
			cmp := json.CompareBinary(j1, j2)
			c.Assert(cmp, Equals, 0, Commentf("got %v expect %v", j1.String(), j2.String()))
		case nil:
			c.Assert(d.IsNull(), IsTrue)
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
		d, err := evalBuiltinFunc(f, chunk.Row{})
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
			d, err = evalBuiltinFunc(f, chunk.Row{})
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

func (s *testEvaluatorSuite) TestJSONRemove(c *C) {
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
		d, err := evalBuiltinFunc(f, chunk.Row{})

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

func (s *testEvaluatorSuite) TestJSONContains(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.JSONContains]
	tbl := []struct {
		input    []interface{}
		expected interface{}
		err      error
	}{
		// Tests nil arguments
		{[]interface{}{nil, `1`, "$.c"}, nil, nil},
		{[]interface{}{`{"a": [1, 2, {"aa": "xx"}]}`, nil, "$.a[3]"}, nil, nil},
		{[]interface{}{`{"a": [1, 2, {"aa": "xx"}]}`, `1`, nil}, nil, nil},
		// Tests with path expression
		{[]interface{}{`[1,2,[1,[5,[3]]]]`, `[1,3]`, "$[2]"}, 1, nil},
		{[]interface{}{`[1,2,[1,[5,{"a":[2,3]}]]]`, `[1,{"a":[3]}]`, "$[2]"}, 1, nil},
		{[]interface{}{`[{"a":1}]`, `{"a":1}`, "$"}, 1, nil},
		{[]interface{}{`[{"a":1,"b":2}]`, `{"a":1,"b":2}`, "$"}, 1, nil},
		{[]interface{}{`[{"a":{"a":1},"b":2}]`, `{"a":1}`, "$.a"}, 0, nil},
		// Tests without path expression
		{[]interface{}{`{}`, `{}`}, 1, nil},
		{[]interface{}{`{"a":1}`, `{}`}, 1, nil},
		{[]interface{}{`{"a":1}`, `1`}, 0, nil},
		{[]interface{}{`{"a":[1]}`, `[1]`}, 0, nil},
		{[]interface{}{`{"b":2, "c":3}`, `{"c":3}`}, 1, nil},
		{[]interface{}{`1`, `1`}, 1, nil},
		{[]interface{}{`[1]`, `1`}, 1, nil},
		{[]interface{}{`[1,2]`, `[1]`}, 1, nil},
		{[]interface{}{`[1,2]`, `[1,3]`}, 0, nil},
		{[]interface{}{`[1,2]`, `["1"]`}, 0, nil},
		{[]interface{}{`[1,2,[1,3]]`, `[1,3]`}, 1, nil},
		{[]interface{}{`[1,2,[1,3]]`, `[1,      3]`}, 1, nil},
		{[]interface{}{`[1,2,[1,[5,[3]]]]`, `[1,3]`}, 1, nil},
		{[]interface{}{`[1,2,[1,[5,{"a":[2,3]}]]]`, `[1,{"a":[3]}]`}, 1, nil},
		{[]interface{}{`[{"a":1}]`, `{"a":1}`}, 1, nil},
		{[]interface{}{`[{"a":1,"b":2}]`, `{"a":1}`}, 1, nil},
		{[]interface{}{`[{"a":{"a":1},"b":2}]`, `{"a":1}`}, 0, nil},
		// Tests path expression contains any asterisk
		{[]interface{}{`{"a": [1, 2, {"aa": "xx"}]}`, `1`, "$.*"}, nil, json.ErrInvalidJSONPathWildcard},
		{[]interface{}{`{"a": [1, 2, {"aa": "xx"}]}`, `1`, "$[*]"}, nil, json.ErrInvalidJSONPathWildcard},
		{[]interface{}{`{"a": [1, 2, {"aa": "xx"}]}`, `1`, "$**.a"}, nil, json.ErrInvalidJSONPathWildcard},
		// Tests path expression does not identify a section of the target document
		{[]interface{}{`{"a": [1, 2, {"aa": "xx"}]}`, `1`, "$.c"}, nil, nil},
		{[]interface{}{`{"a": [1, 2, {"aa": "xx"}]}`, `1`, "$.a[3]"}, nil, nil},
		{[]interface{}{`{"a": [1, 2, {"aa": "xx"}]}`, `1`, "$.a[2].b"}, nil, nil},
		// For issue 9957: test 'argument 1 and 2 as valid json object'
		{[]interface{}{`[1,2,[1,3]]`, `a:1`}, 1, json.ErrInvalidJSONText},
		{[]interface{}{`a:1`, `1`}, 1, json.ErrInvalidJSONText},
	}
	for _, t := range tbl {
		args := types.MakeDatums(t.input...)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(args))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		if t.err == nil {
			c.Assert(err, IsNil)
			if t.expected == nil {
				c.Assert(d.IsNull(), IsTrue)
			} else {
				c.Assert(d.GetInt64(), Equals, int64(t.expected.(int)))
			}
		} else {
			c.Assert(t.err.(*terror.Error).Equal(err), IsTrue)
		}
	}
	// For issue 9957: test 'argument 1 and 2 as valid json object'
	cases := []struct {
		arg1 interface{}
		arg2 interface{}
	}{
		{1, ""},
		{0.05, ""},
		{"", 1},
		{"", 0.05},
	}
	for _, cs := range cases {
		_, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(cs.arg1, cs.arg2)))
		c.Assert(json.ErrInvalidJSONData.Equal(err), IsTrue)
	}
}

func (s *testEvaluatorSuite) TestJSONContainsPath(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.JSONContainsPath]
	jsonString := `{"a": 1, "b": 2, "c": {"d": 4}}`
	invalidJSON := `{"a": 1`
	tbl := []struct {
		input    []interface{}
		expected interface{}
		success  bool
	}{
		// Tests nil arguments
		{[]interface{}{nil, json.ContainsPathOne, "$.c"}, nil, true},
		{[]interface{}{nil, json.ContainsPathAll, "$.c"}, nil, true},
		{[]interface{}{jsonString, nil, "$.a[3]"}, nil, true},
		{[]interface{}{jsonString, json.ContainsPathOne, nil}, nil, true},
		{[]interface{}{jsonString, json.ContainsPathAll, nil}, nil, true},
		// Tests with one path expression
		{[]interface{}{jsonString, json.ContainsPathOne, "$.c.d"}, 1, true},
		{[]interface{}{jsonString, json.ContainsPathOne, "$.a.d"}, 0, true},
		{[]interface{}{jsonString, json.ContainsPathAll, "$.c.d"}, 1, true},
		{[]interface{}{jsonString, json.ContainsPathAll, "$.a.d"}, 0, true},
		// Tests with multiple path expression
		{[]interface{}{jsonString, json.ContainsPathOne, "$.a", "$.e"}, 1, true},
		{[]interface{}{jsonString, json.ContainsPathOne, "$.a", "$.c"}, 1, true},
		{[]interface{}{jsonString, json.ContainsPathAll, "$.a", "$.e"}, 0, true},
		{[]interface{}{jsonString, json.ContainsPathAll, "$.a", "$.c"}, 1, true},
		// Tests path expression contains any asterisk
		{[]interface{}{jsonString, json.ContainsPathOne, "$.*"}, 1, true},
		{[]interface{}{jsonString, json.ContainsPathOne, "$[*]"}, 0, true},
		{[]interface{}{jsonString, json.ContainsPathAll, "$.*"}, 1, true},
		{[]interface{}{jsonString, json.ContainsPathAll, "$[*]"}, 0, true},
		// Tests invalid json document
		{[]interface{}{invalidJSON, json.ContainsPathOne, "$.a"}, nil, false},
		{[]interface{}{invalidJSON, json.ContainsPathAll, "$.a"}, nil, false},
		// Tests compatible contains path
		{[]interface{}{jsonString, "ONE", "$.c.d"}, 1, true},
		{[]interface{}{jsonString, "ALL", "$.c.d"}, 1, true},
		{[]interface{}{jsonString, "One", "$.a", "$.e"}, 1, true},
		{[]interface{}{jsonString, "aLl", "$.a", "$.e"}, 0, true},
		{[]interface{}{jsonString, "test", "$.a"}, nil, false},
	}
	for _, t := range tbl {
		args := types.MakeDatums(t.input...)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(args))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		if t.success {
			c.Assert(err, IsNil)
			if t.expected == nil {
				c.Assert(d.IsNull(), IsTrue)
			} else {
				c.Assert(d.GetInt64(), Equals, int64(t.expected.(int)))
			}
		} else {
			c.Assert(err, NotNil)
		}
	}
}

func (s *testEvaluatorSuite) TestJSONLength(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.JSONLength]
	tbl := []struct {
		input    []interface{}
		expected interface{}
		success  bool
	}{
		// Tests scalar arguments
		{[]interface{}{`null`}, 1, true},
		{[]interface{}{`true`}, 1, true},
		{[]interface{}{`false`}, 1, true},
		{[]interface{}{`1`}, 1, true},
		{[]interface{}{`-1`}, 1, true},
		{[]interface{}{`1.1`}, 1, true},
		{[]interface{}{`"1"`}, 1, true},
		{[]interface{}{`"1"`, "$.a"}, 1, true},
		{[]interface{}{`null`, "$.a"}, 1, true},
		// Tests nil arguments
		{[]interface{}{nil}, nil, true},
		{[]interface{}{nil, "a"}, nil, true},
		{[]interface{}{`{"a": 1}`, nil}, nil, true},
		{[]interface{}{nil, nil}, nil, true},
		// Tests with path expression
		{[]interface{}{`[1,2,[1,[5,[3]]]]`, "$[2]"}, 2, true},
		{[]interface{}{`[{"a":1}]`, "$"}, 1, true},
		{[]interface{}{`[{"a":1,"b":2}]`, "$[0].a"}, 1, true},
		{[]interface{}{`{"a":{"a":1},"b":2}`, "$"}, 2, true},
		{[]interface{}{`{"a":{"a":1},"b":2}`, "$.a"}, 1, true},
		{[]interface{}{`{"a":{"a":1},"b":2}`, "$.a.a"}, 1, true},
		{[]interface{}{`{"a": [1, 2, {"aa": "xx"}]}`, "$.a[2].aa"}, 1, true},
		// Tests without path expression
		{[]interface{}{`{}`}, 0, true},
		{[]interface{}{`{"a":1}`}, 1, true},
		{[]interface{}{`{"a":[1]}`}, 1, true},
		{[]interface{}{`{"b":2, "c":3}`}, 2, true},
		{[]interface{}{`[1]`}, 1, true},
		{[]interface{}{`[1,2]`}, 2, true},
		{[]interface{}{`[1,2,[1,3]]`}, 3, true},
		{[]interface{}{`[1,2,[1,[5,[3]]]]`}, 3, true},
		{[]interface{}{`[1,2,[1,[5,{"a":[2,3]}]]]`}, 3, true},
		{[]interface{}{`[{"a":1}]`}, 1, true},
		{[]interface{}{`[{"a":1,"b":2}]`}, 1, true},
		{[]interface{}{`[{"a":{"a":1},"b":2}]`}, 1, true},
		// Tests path expression contains any asterisk
		{[]interface{}{`{"a": [1, 2, {"aa": "xx"}]}`, "$.*"}, nil, false},
		{[]interface{}{`{"a": [1, 2, {"aa": "xx"}]}`, "$[*]"}, nil, false},
		{[]interface{}{`{"a": [1, 2, {"aa": "xx"}]}`, "$**.a"}, nil, false},
		// Tests path expression does not identify a section of the target document
		{[]interface{}{`{"a": [1, 2, {"aa": "xx"}]}`, "$.c"}, nil, true},
		{[]interface{}{`{"a": [1, 2, {"aa": "xx"}]}`, "$.a[3]"}, nil, true},
		{[]interface{}{`{"a": [1, 2, {"aa": "xx"}]}`, "$.a[2].b"}, nil, true},
	}
	for _, t := range tbl {
		args := types.MakeDatums(t.input...)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(args))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		if t.success {
			c.Assert(err, IsNil)

			if t.expected == nil {
				c.Assert(d.IsNull(), IsTrue)
			} else {
				c.Assert(d.GetInt64(), Equals, int64(t.expected.(int)))
			}
		} else {
			c.Assert(err, NotNil)
		}
	}
}

func (s *testEvaluatorSuite) TestJSONKeys(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.JSONKeys]
	tbl := []struct {
		input    []interface{}
		expected interface{}
		success  bool
	}{
		// Tests nil arguments
		{[]interface{}{nil}, nil, true},
		{[]interface{}{nil, "$.c"}, nil, true},
		{[]interface{}{`{"a": 1}`, nil}, nil, true},
		{[]interface{}{nil, nil}, nil, true},

		// Tests with other type
		{[]interface{}{`1`}, nil, false},
		{[]interface{}{`"str"`}, nil, false},
		{[]interface{}{`true`}, nil, false},
		{[]interface{}{`null`}, nil, false},
		{[]interface{}{`[1, 2]`}, nil, false},
		{[]interface{}{`["1", "2"]`}, nil, false},

		// Tests without path expression
		{[]interface{}{`{}`}, `[]`, true},
		{[]interface{}{`{"a": 1}`}, `["a"]`, true},
		{[]interface{}{`{"a": 1, "b": 2}`}, `["a", "b"]`, true},
		{[]interface{}{`{"a": {"c": 3}, "b": 2}`}, `["a", "b"]`, true},

		// Tests with path expression
		{[]interface{}{`{"a": 1}`, "$.a"}, nil, true},
		{[]interface{}{`{"a": {"c": 3}, "b": 2}`, "$.a"}, `["c"]`, true},
		{[]interface{}{`{"a": {"c": 3}, "b": 2}`, "$.a.c"}, nil, true},
		{[]interface{}{`{"a": {"c": 3}, "b": 2}`, nil}, nil, true},

		// Tests path expression contains any asterisk
		{[]interface{}{`{}`, "$.*"}, nil, false},
		{[]interface{}{`{"a": 1}`, "$.*"}, nil, false},
		{[]interface{}{`{"a": {"c": 3}, "b": 2}`, "$.*"}, nil, false},
		{[]interface{}{`{"a": {"c": 3}, "b": 2}`, "$.a.*"}, nil, false},

		// Tests path expression does not identify a section of the target document
		{[]interface{}{`{"a": 1}`, "$.b"}, nil, true},
		{[]interface{}{`{"a": {"c": 3}, "b": 2}`, "$.c"}, nil, true},
		{[]interface{}{`{"a": {"c": 3}, "b": 2}`, "$.a.d"}, nil, true},
	}
	for _, t := range tbl {
		args := types.MakeDatums(t.input...)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(args))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		if t.success {
			c.Assert(err, IsNil)
			switch x := t.expected.(type) {
			case string:
				var j1 json.BinaryJSON
				j1, err = json.ParseBinaryFromString(x)
				c.Assert(err, IsNil)
				j2 := d.GetMysqlJSON()
				var cmp int
				cmp = json.CompareBinary(j1, j2)
				c.Assert(cmp, Equals, 0)
			case nil:
				c.Assert(d.IsNull(), IsTrue)
			}
		} else {
			c.Assert(err, NotNil)
		}
	}
}

func (s *testEvaluatorSuite) TestJSONDepth(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.JSONDepth]
	tbl := []struct {
		input    []interface{}
		expected interface{}
		success  bool
	}{
		// Tests scalar arguments
		{[]interface{}{`null`}, 1, true},
		{[]interface{}{`true`}, 1, true},
		{[]interface{}{`false`}, 1, true},
		{[]interface{}{`1`}, 1, true},
		{[]interface{}{`-1`}, 1, true},
		{[]interface{}{`1.1`}, 1, true},
		{[]interface{}{`"1"`}, 1, true},
		// Tests nil arguments
		{[]interface{}{nil}, nil, true},
		// Tests depth
		{[]interface{}{`{}`}, 1, true},
		{[]interface{}{`[]`}, 1, true},
		{[]interface{}{`[10, 20]`}, 2, true},
		{[]interface{}{`[[], {}]`}, 2, true},
		{[]interface{}{`{"Name": "Homer"}`}, 2, true},
		{[]interface{}{`[10, {"a": 20}]`}, 3, true},
		{[]interface{}{`{"Person": {"Name": "Homer", "Age": 39, "Hobbies": ["Eating", "Sleeping"]} }`}, 4, true},
		{[]interface{}{`{"a":1}`}, 2, true},
		{[]interface{}{`{"a":[1]}`}, 3, true},
		{[]interface{}{`{"b":2, "c":3}`}, 2, true},
		{[]interface{}{`[1]`}, 2, true},
		{[]interface{}{`[1,2]`}, 2, true},
		{[]interface{}{`[1,2,[1,3]]`}, 3, true},
		{[]interface{}{`[1,2,[1,[5,[3]]]]`}, 5, true},
		{[]interface{}{`[1,2,[1,[5,{"a":[2,3]}]]]`}, 6, true},
		{[]interface{}{`[{"a":1}]`}, 3, true},
		{[]interface{}{`[{"a":1,"b":2}]`}, 3, true},
		{[]interface{}{`[{"a":{"a":1},"b":2}]`}, 4, true},
		// Tests non-json
		{[]interface{}{`a`}, nil, false},
	}
	for _, t := range tbl {
		args := types.MakeDatums(t.input...)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(args))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		if t.success {
			c.Assert(err, IsNil)

			if t.expected == nil {
				c.Assert(d.IsNull(), IsTrue)
			} else {
				c.Assert(d.GetInt64(), Equals, int64(t.expected.(int)))
			}
		} else {
			c.Assert(err, NotNil)
		}
	}
}

func (s *testEvaluatorSuite) TestJSONArrayAppend(c *C) {
	defer testleak.AfterTest(c)()
	sampleJSON, err := json.ParseBinaryFromString(`{"b": 2}`)
	c.Assert(err, IsNil)
	fc := funcs[ast.JSONArrayAppend]
	tbl := []struct {
		input    []interface{}
		expected interface{}
		err      *terror.Error
	}{
		{[]interface{}{`{"a": 1, "b": [2, 3], "c": 4}`, `$.d`, `z`}, `{"a": 1, "b": [2, 3], "c": 4}`, nil},
		{[]interface{}{`{"a": 1, "b": [2, 3], "c": 4}`, `$`, `w`}, `[{"a": 1, "b": [2, 3], "c": 4}, "w"]`, nil},
		{[]interface{}{`{"a": 1, "b": [2, 3], "c": 4}`, `$`, nil}, `[{"a": 1, "b": [2, 3], "c": 4}, null]`, nil},
		{[]interface{}{`{"a": 1}`, `$`, `{"b": 2}`}, `[{"a": 1}, "{\"b\": 2}"]`, nil},
		{[]interface{}{`{"a": 1}`, `$`, sampleJSON}, `[{"a": 1}, {"b": 2}]`, nil},
		{[]interface{}{`{"a": 1}`, `$.a`, sampleJSON}, `{"a": [1, {"b": 2}]}`, nil},

		{[]interface{}{`{"a": 1}`, `$.a`, sampleJSON, `$.a[1]`, sampleJSON}, `{"a": [1, [{"b": 2}, {"b": 2}]]}`, nil},
		{[]interface{}{nil, `$`, nil}, nil, nil},
		{[]interface{}{nil, `$`, `a`}, nil, nil},
		{[]interface{}{`null`, `$`, nil}, `[null, null]`, nil},
		{[]interface{}{`[]`, `$`, nil}, `[null]`, nil},
		{[]interface{}{`{}`, `$`, nil}, `[{}, null]`, nil},
		// Bad arguments.
		{[]interface{}{`asdf`, `$`, nil}, nil, json.ErrInvalidJSONText},
		{[]interface{}{``, `$`, nil}, nil, json.ErrInvalidJSONText},
		{[]interface{}{`{"a": 1, "b": [2, 3], "c": 4}`, `$.d`}, nil, ErrIncorrectParameterCount},
		{[]interface{}{`{"a": 1, "b": [2, 3], "c": 4}`, `$.c`, `y`, `$.b`}, nil, ErrIncorrectParameterCount},
		{[]interface{}{`{"a": 1, "b": [2, 3], "c": 4}`, nil, nil}, nil, nil},
		{[]interface{}{`{"a": 1, "b": [2, 3], "c": 4}`, `asdf`, nil}, nil, json.ErrInvalidJSONPath},
		{[]interface{}{`{"a": 1, "b": [2, 3], "c": 4}`, 42, nil}, nil, json.ErrInvalidJSONPath},
		{[]interface{}{`{"a": 1, "b": [2, 3], "c": 4}`, `$.*`, nil}, nil, json.ErrInvalidJSONPathWildcard},
		// Following tests come from MySQL doc.
		{[]interface{}{`["a", ["b", "c"], "d"]`, `$[1]`, 1}, `["a", ["b", "c", 1], "d"]`, nil},
		{[]interface{}{`["a", ["b", "c"], "d"]`, `$[0]`, 2}, `[["a", 2], ["b", "c"], "d"]`, nil},
		{[]interface{}{`["a", ["b", "c"], "d"]`, `$[1][0]`, 3}, `["a", [["b", 3], "c"], "d"]`, nil},
		{[]interface{}{`{"a": 1, "b": [2, 3], "c": 4}`, `$.b`, `x`}, `{"a": 1, "b": [2, 3, "x"], "c": 4}`, nil},
		{[]interface{}{`{"a": 1, "b": [2, 3], "c": 4}`, `$.c`, `y`}, `{"a": 1, "b": [2, 3], "c": [4, "y"]}`, nil},
		// Following tests come from MySQL test.
		{[]interface{}{`[1,2,3, {"a":[4,5,6]}]`, `$`, 7}, `[1, 2, 3, {"a": [4, 5, 6]}, 7]`, nil},
		{[]interface{}{`[1,2,3, {"a":[4,5,6]}]`, `$`, 7, `$[3].a`, 3.14}, `[1, 2, 3, {"a": [4, 5, 6, 3.14]}, 7]`, nil},
		{[]interface{}{`[1,2,3, {"a":[4,5,6]}]`, `$`, 7, `$[3].b`, 8}, `[1, 2, 3, {"a": [4, 5, 6]}, 7]`, nil},
	}

	for i, t := range tbl {
		args := types.MakeDatums(t.input...)
		s.ctx.GetSessionVars().StmtCtx.SetWarnings(nil)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(args))
		// No error should return in getFunction if t.err is nil.
		if err != nil {
			c.Assert(t.err, NotNil)
			c.Assert(t.err.Equal(err), Equals, true)
			continue
		}

		c.Assert(f, NotNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		comment := Commentf("case:%v \n input:%v \n output: %s \n expected: %v \n warnings: %v \n expected error %v", i, t.input, d.GetMysqlJSON(), t.expected, s.ctx.GetSessionVars().StmtCtx.GetWarnings(), t.err)

		if t.err != nil {
			c.Assert(t.err.Equal(err), Equals, true, comment)
			continue
		}

		c.Assert(err, IsNil, comment)
		c.Assert(int(s.ctx.GetSessionVars().StmtCtx.WarningCount()), Equals, 0, comment)

		if t.expected == nil {
			c.Assert(d.IsNull(), IsTrue, comment)
			continue
		}

		j1, err := json.ParseBinaryFromString(t.expected.(string))

		c.Assert(err, IsNil, comment)
		c.Assert(json.CompareBinary(j1, d.GetMysqlJSON()), Equals, 0, comment)
	}
}

func (s *testEvaluatorSuite) TestJSONSearch(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.JSONSearch]
	jsonString := `["abc", [{"k": "10"}, "def"], {"x":"abc"}, {"y":"bcd"}]`
	jsonString2 := `["abc", [{"k": "10"}, "def"], {"x":"ab%d"}, {"y":"abcd"}]`
	tbl := []struct {
		input    []interface{}
		expected interface{}
		success  bool
	}{
		// simple case
		{[]interface{}{jsonString, `one`, `abc`}, `"$[0]"`, true},
		{[]interface{}{jsonString, `all`, `abc`}, `["$[0]", "$[2].x"]`, true},
		{[]interface{}{jsonString, `all`, `ghi`}, nil, true},
		{[]interface{}{jsonString, `all`, `10`}, `"$[1][0].k"`, true},
		{[]interface{}{jsonString, `all`, `10`, nil, `$`}, `"$[1][0].k"`, true},
		{[]interface{}{jsonString, `all`, `10`, nil, `$[*]`}, `"$[1][0].k"`, true},
		{[]interface{}{jsonString, `all`, `10`, nil, `$**.k`}, `"$[1][0].k"`, true},
		{[]interface{}{jsonString, `all`, `10`, nil, `$[*][0].k`}, `"$[1][0].k"`, true},
		{[]interface{}{jsonString, `all`, `10`, nil, `$[1]`}, `"$[1][0].k"`, true},
		{[]interface{}{jsonString, `all`, `10`, nil, `$[1][0]`}, `"$[1][0].k"`, true},
		{[]interface{}{jsonString, `all`, `abc`, nil, `$[2]`}, `"$[2].x"`, true},
		{[]interface{}{jsonString, `all`, `abc`, nil, `$[2]`, `$[0]`}, `["$[2].x", "$[0]"]`, true},
		{[]interface{}{jsonString, `all`, `abc`, nil, `$[2]`, `$[2]`}, `"$[2].x"`, true},

		// search pattern
		{[]interface{}{jsonString, `all`, `%a%`}, `["$[0]", "$[2].x"]`, true},
		{[]interface{}{jsonString, `all`, `%b%`}, `["$[0]", "$[2].x", "$[3].y"]`, true},
		{[]interface{}{jsonString, `all`, `%b%`, nil, `$[0]`}, `"$[0]"`, true},
		{[]interface{}{jsonString, `all`, `%b%`, nil, `$[2]`}, `"$[2].x"`, true},
		{[]interface{}{jsonString, `all`, `%b%`, nil, `$[1]`}, nil, true},
		{[]interface{}{jsonString, `all`, `%b%`, ``, `$[1]`}, nil, true},
		{[]interface{}{jsonString, `all`, `%b%`, nil, `$[3]`}, `"$[3].y"`, true},
		{[]interface{}{jsonString2, `all`, `ab_d`}, `["$[2].x", "$[3].y"]`, true},

		// escape char
		{[]interface{}{jsonString2, `all`, `ab%d`}, `["$[2].x", "$[3].y"]`, true},
		{[]interface{}{jsonString2, `all`, `ab\%d`}, `"$[2].x"`, true},
		{[]interface{}{jsonString2, `all`, `ab|%d`, `|`}, `"$[2].x"`, true},

		// error handle
		{[]interface{}{nil, `all`, `abc`}, nil, true},                     // NULL json
		{[]interface{}{`a`, `all`, `abc`}, nil, false},                    // non json
		{[]interface{}{jsonString, `wrong`, `abc`}, nil, false},           // wrong one_or_all
		{[]interface{}{jsonString, `all`, nil}, nil, true},                // NULL search_str
		{[]interface{}{jsonString, `all`, `abc`, `??`}, nil, false},       // wrong escape_char
		{[]interface{}{jsonString, `all`, `abc`, nil, nil}, nil, true},    // NULL path
		{[]interface{}{jsonString, `all`, `abc`, nil, `$xx`}, nil, false}, // wrong path
	}
	for _, t := range tbl {
		args := types.MakeDatums(t.input...)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(args))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		if t.success {
			c.Assert(err, IsNil)
			switch x := t.expected.(type) {
			case string:
				var j1, j2 json.BinaryJSON
				j1, err = json.ParseBinaryFromString(x)
				c.Assert(err, IsNil)
				j2 = d.GetMysqlJSON()
				cmp := json.CompareBinary(j1, j2)
				//fmt.Println(j1, j2)
				c.Assert(cmp, Equals, 0)
			case nil:
				c.Assert(d.IsNull(), IsTrue)
			}
		} else {
			c.Assert(err, NotNil)
		}
	}
}

func (s *testEvaluatorSuite) TestJSONArrayInsert(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.JSONArrayInsert]
	tbl := []struct {
		input    []interface{}
		expected interface{}
		success  bool
		err      *terror.Error
	}{
		// Success
		{[]interface{}{`{"a": 1, "b": [2, 3], "c": 4}`, `$.b[1]`, `z`}, `{"a": 1, "b": [2, "z", 3], "c": 4}`, true, nil},
		{[]interface{}{`{"a": 1, "b": [2, 3], "c": 4}`, `$.a[1]`, `z`}, `{"a": 1, "b": [2, 3], "c": 4}`, true, nil},
		{[]interface{}{`{"a": 1, "b": [2, 3], "c": 4}`, `$.d[1]`, `z`}, `{"a": 1, "b": [2, 3], "c": 4}`, true, nil},
		{[]interface{}{`[{"a": 1, "b": [2, 3], "c": 4}]`, `$[1]`, `w`}, `[{"a": 1, "b": [2, 3], "c": 4}, "w"]`, true, nil},
		{[]interface{}{`[{"a": 1, "b": [2, 3], "c": 4}]`, `$[0]`, nil}, `[null, {"a": 1, "b": [2, 3], "c": 4}]`, true, nil},
		{[]interface{}{`[1, 2, 3]`, `$[100]`, `{"b": 2}`}, `[1, 2, 3, "{\"b\": 2}"]`, true, nil},
		// About null
		{[]interface{}{nil, `$`, nil}, nil, true, nil},
		{[]interface{}{nil, `$`, `a`}, nil, true, nil},
		{[]interface{}{`[]`, `$[0]`, nil}, `[null]`, true, nil},
		{[]interface{}{`{}`, `$[0]`, nil}, `{}`, true, nil},
		// Bad arguments
		{[]interface{}{`asdf`, `$`, nil}, nil, false, json.ErrInvalidJSONText},
		{[]interface{}{``, `$`, nil}, nil, false, json.ErrInvalidJSONText},
		{[]interface{}{`{"a": 1, "b": [2, 3], "c": 4}`, `$.d`}, nil, false, ErrIncorrectParameterCount},
		{[]interface{}{`{"a": 1, "b": [2, 3], "c": 4}`, `$.c`, `y`, `$.b`}, nil, false, ErrIncorrectParameterCount},
		{[]interface{}{`{"a": 1, "b": [2, 3], "c": 4}`, nil, nil}, nil, true, nil},
		{[]interface{}{`{"a": 1, "b": [2, 3], "c": 4}`, `asdf`, nil}, nil, false, json.ErrInvalidJSONPath},
		{[]interface{}{`{"a": 1, "b": [2, 3], "c": 4}`, 42, nil}, nil, false, json.ErrInvalidJSONPath},
		{[]interface{}{`{"a": 1, "b": [2, 3], "c": 4}`, `$.*`, nil}, nil, false, json.ErrInvalidJSONPathWildcard},
		{[]interface{}{`{"a": 1, "b": [2, 3], "c": 4}`, `$.b[0]`, nil, `$.a`, nil}, nil, false, json.ErrInvalidJSONPathArrayCell},
		{[]interface{}{`{"a": 1, "b": [2, 3], "c": 4}`, `$.a`, nil}, nil, false, json.ErrInvalidJSONPathArrayCell},
		// Following tests come from MySQL doc.
		{[]interface{}{`["a", {"b": [1, 2]}, [3, 4]]`, `$[1]`, `x`}, `["a", "x", {"b": [1, 2]}, [3, 4]]`, true, nil},
		{[]interface{}{`["a", {"b": [1, 2]}, [3, 4]]`, `$[100]`, `x`}, `["a", {"b": [1, 2]}, [3, 4], "x"]`, true, nil},
		{[]interface{}{`["a", {"b": [1, 2]}, [3, 4]]`, `$[1].b[0]`, `x`}, `["a", {"b": ["x", 1, 2]}, [3, 4]]`, true, nil},
		{[]interface{}{`["a", {"b": [1, 2]}, [3, 4]]`, `$[2][1]`, `y`}, `["a", {"b": [1, 2]}, [3, "y", 4]]`, true, nil},
		{[]interface{}{`["a", {"b": [1, 2]}, [3, 4]]`, `$[0]`, `x`, `$[2][1]`, `y`}, `["x", "a", {"b": [1, 2]}, [3, 4]]`, true, nil},
		// More test cases
		{[]interface{}{`["a", {"b": [1, 2]}, [3, 4]]`, `$[0]`, `x`, `$[0]`, `y`}, `["y", "x", "a", {"b": [1, 2]}, [3, 4]]`, true, nil},
	}
	for _, t := range tbl {
		args := types.MakeDatums(t.input...)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(args))
		// Parameter count error
		if err != nil {
			c.Assert(t.err, NotNil)
			c.Assert(t.err.Equal(err), Equals, true)
			continue
		}

		d, err := evalBuiltinFunc(f, chunk.Row{})

		if t.success {
			c.Assert(err, IsNil)
			switch x := t.expected.(type) {
			case string:
				var j1, j2 json.BinaryJSON
				j1, err = json.ParseBinaryFromString(x)
				c.Assert(err, IsNil)
				j2 = d.GetMysqlJSON()
				var cmp int
				cmp = json.CompareBinary(j1, j2)
				c.Assert(cmp, Equals, 0)
			case nil:
				c.Assert(d.IsNull(), IsTrue)
			}
		} else {
			c.Assert(t.err.Equal(err), Equals, true)
		}
	}
}

func (s *testEvaluatorSuite) TestJSONValid(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.JSONValid]
	tbl := []struct {
		Input    interface{}
		Expected interface{}
	}{
		{`{"a":1}`, 1},
		{`hello`, 0},
		{`"hello"`, 1},
		{`null`, 1},
		{`{}`, 1},
		{`[]`, 1},
		{`2`, 1},
		{`2.5`, 1},
		{`2019-8-19`, 0},
		{`"2019-8-19"`, 1},
		{2, 0},
		{2.5, 0},
		{nil, nil},
	}
	dtbl := tblToDtbl(tbl)
	for _, t := range dtbl {
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(d, testutil.DatumEquals, t["Expected"][0])
	}
}
