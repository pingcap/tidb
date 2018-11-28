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
		{`"1\\u2232\\u22322"`, `1\u2232\u22322`},
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
		success  bool
	}{
		// Tests nil arguments
		{[]interface{}{nil, `1`, "$.c"}, nil, true},
		{[]interface{}{`{"a": [1, 2, {"aa": "xx"}]}`, nil, "$.a[3]"}, nil, true},
		{[]interface{}{`{"a": [1, 2, {"aa": "xx"}]}`, `1`, nil}, nil, true},
		// Tests with path expression
		{[]interface{}{`[1,2,[1,[5,[3]]]]`, `[1,3]`, "$[2]"}, 1, true},
		{[]interface{}{`[1,2,[1,[5,{"a":[2,3]}]]]`, `[1,{"a":[3]}]`, "$[2]"}, 1, true},
		{[]interface{}{`[{"a":1}]`, `{"a":1}`, "$"}, 1, true},
		{[]interface{}{`[{"a":1,"b":2}]`, `{"a":1,"b":2}`, "$"}, 1, true},
		{[]interface{}{`[{"a":{"a":1},"b":2}]`, `{"a":1}`, "$.a"}, 0, true},
		// Tests without path expression
		{[]interface{}{`{}`, `{}`}, 1, true},
		{[]interface{}{`{"a":1}`, `{}`}, 1, true},
		{[]interface{}{`{"a":1}`, `1`}, 0, true},
		{[]interface{}{`{"a":[1]}`, `[1]`}, 0, true},
		{[]interface{}{`{"b":2, "c":3}`, `{"c":3}`}, 1, true},
		{[]interface{}{`1`, `1`}, 1, true},
		{[]interface{}{`[1]`, `1`}, 1, true},
		{[]interface{}{`[1,2]`, `[1]`}, 1, true},
		{[]interface{}{`[1,2]`, `[1,3]`}, 0, true},
		{[]interface{}{`[1,2]`, `["1"]`}, 0, true},
		{[]interface{}{`[1,2,[1,3]]`, `[1,3]`}, 1, true},
		{[]interface{}{`[1,2,[1,[5,[3]]]]`, `[1,3]`}, 1, true},
		{[]interface{}{`[1,2,[1,[5,{"a":[2,3]}]]]`, `[1,{"a":[3]}]`}, 1, true},
		{[]interface{}{`[{"a":1}]`, `{"a":1}`}, 1, true},
		{[]interface{}{`[{"a":1,"b":2}]`, `{"a":1}`}, 1, true},
		{[]interface{}{`[{"a":{"a":1},"b":2}]`, `{"a":1}`}, 0, true},
		// Tests path expression contains any asterisk
		{[]interface{}{`{"a": [1, 2, {"aa": "xx"}]}`, `1`, "$.*"}, nil, false},
		{[]interface{}{`{"a": [1, 2, {"aa": "xx"}]}`, `1`, "$[*]"}, nil, false},
		{[]interface{}{`{"a": [1, 2, {"aa": "xx"}]}`, `1`, "$**.a"}, nil, false},
		// Tests path expression does not identify a section of the target document
		{[]interface{}{`{"a": [1, 2, {"aa": "xx"}]}`, `1`, "$.c"}, nil, true},
		{[]interface{}{`{"a": [1, 2, {"aa": "xx"}]}`, `1`, "$.a[3]"}, nil, true},
		{[]interface{}{`{"a": [1, 2, {"aa": "xx"}]}`, `1`, "$.a[2].b"}, nil, true},
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
		{[]interface{}{`{"a": 1}`, "$.a"}, nil, false},
		{[]interface{}{`{"a": {"c": 3}, "b": 2}`, "$.a"}, `["c"]`, true},
		{[]interface{}{`{"a": {"c": 3}, "b": 2}`, "$.a.c"}, nil, false},

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
