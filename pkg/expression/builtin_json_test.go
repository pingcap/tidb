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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"fmt"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/testkit/testutil"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestJSONType(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.JSONType]
	tbl := []struct {
		Input    any
		Expected any
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
	for _, tt := range dtbl {
		f, err := fc.getFunction(ctx, datumsToConstants(tt["Input"]))
		require.NoError(t, err)
		d, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, tt["Expected"][0], d)
	}
}

func TestJSONQuote(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.JSONQuote]
	tbl := []struct {
		Input    any
		Expected any
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
	for _, tt := range dtbl {
		f, err := fc.getFunction(ctx, datumsToConstants(tt["Input"]))
		require.NoError(t, err)
		d, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, tt["Expected"][0], d)
	}
}

func TestJSONUnquote(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.JSONUnquote]
	tbl := []struct {
		Input  string
		Result string
		Error  error
	}{
		{``, ``, nil},
		{`""`, ``, nil},
		{`''`, `''`, nil},
		{`3`, `3`, nil},
		{`{"a": "b"}`, `{"a": "b"}`, nil},
		{`{"a":     "b"}`, `{"a":     "b"}`, nil},
		{`"hello,\"quoted string\",world"`, `hello,"quoted string",world`, nil},
		{`"hello,\"宽字符\",world"`, `hello,"宽字符",world`, nil},
		{`Invalid Json string\tis OK`, `Invalid Json string\tis OK`, nil},
		{`"1\\u2232\\u22322"`, `1\u2232\u22322`, nil},
		{`"[{\"x\":\"{\\\"y\\\":12}\"}]"`, `[{"x":"{\"y\":12}"}]`, nil},
		{`[{\"x\":\"{\\\"y\\\":12}\"}]`, `[{\"x\":\"{\\\"y\\\":12}\"}]`, nil},
		{`"a"`, `a`, nil},
		{`""a""`, `""a""`, types.ErrInvalidJSONText.GenWithStackByArgs("The document root must not be followed by other values.")},
		{`"""a"""`, `"""a"""`, types.ErrInvalidJSONText.GenWithStackByArgs("The document root must not be followed by other values.")},
	}
	for _, tt := range tbl {
		var d types.Datum
		d.SetString(tt.Input, mysql.DefaultCollationName)
		f, err := fc.getFunction(ctx, datumsToConstants([]types.Datum{d}))
		require.NoError(t, err)
		d, err = evalBuiltinFunc(f, ctx, chunk.Row{})
		if tt.Error == nil {
			require.Equal(t, tt.Result, d.GetString())
			require.NoError(t, err)
		} else {
			require.Error(t, err)
			require.Contains(t, err.Error(), "The document root must not be followed by other values")
		}
	}
}

func TestJSONExtract(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.JSONExtract]
	jstr := `{"a": [{"aa": [{"aaa": 1}]}], "aaa": 2}`
	tbl := []struct {
		Input    []any
		Expected any
		Success  bool
	}{
		{[]any{nil, nil}, nil, true},
		{[]any{jstr, `$.a[0].aa[0].aaa`, `$.aaa`}, `[1, 2]`, true},
		{[]any{jstr, `$.a[0].aa[0].aaa`, `$InvalidPath`}, nil, false},
	}
	for _, tt := range tbl {
		args := types.MakeDatums(tt.Input...)
		f, err := fc.getFunction(ctx, datumsToConstants(args))
		require.NoError(t, err)
		d, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		if tt.Success {
			require.NoError(t, err)
			switch x := tt.Expected.(type) {
			case string:
				var j1 types.BinaryJSON
				j1, err = types.ParseBinaryJSONFromString(x)
				require.NoError(t, err)
				j2 := d.GetMysqlJSON()
				var cmp int
				cmp = types.CompareBinaryJSON(j1, j2)
				require.NoError(t, err)
				require.Equal(t, 0, cmp)
			}
		} else {
			require.Error(t, err)
		}
	}
}

// TestJSONSetInsertReplace tests grammar of json_{set,insert,replace}.
func TestJSONSetInsertReplace(t *testing.T) {
	ctx := createContext(t)
	tbl := []struct {
		fc           functionClass
		Input        []any
		Expected     any
		BuildSuccess bool
		Success      bool
	}{
		{funcs[ast.JSONSet], []any{nil, nil, nil}, nil, true, true},
		{funcs[ast.JSONSet], []any{`{}`, `$.a`, 3}, `{"a": 3}`, true, true},
		{funcs[ast.JSONInsert], []any{`{}`, `$.a`, 3}, `{"a": 3}`, true, true},
		{funcs[ast.JSONReplace], []any{`{}`, `$.a`, 3}, `{}`, true, true},
		{funcs[ast.JSONSet], []any{`{}`, `$.a`, 3, `$.b`, "3"}, `{"a": 3, "b": "3"}`, true, true},
		{funcs[ast.JSONSet], []any{`{}`, `$.a`, nil, `$.b`, "nil"}, `{"a": null, "b": "nil"}`, true, true},
		{funcs[ast.JSONSet], []any{`{}`, `$.a`, 3, `$.b`}, nil, false, false},
		{funcs[ast.JSONSet], []any{`{}`, `$InvalidPath`, 3}, nil, true, false},
	}
	var err error
	var f builtinFunc
	var d types.Datum
	for _, tt := range tbl {
		args := types.MakeDatums(tt.Input...)
		f, err = tt.fc.getFunction(ctx, datumsToConstants(args))
		if tt.BuildSuccess {
			require.NoError(t, err)
			d, err = evalBuiltinFunc(f, ctx, chunk.Row{})
			if tt.Success {
				require.NoError(t, err)
				switch x := tt.Expected.(type) {
				case string:
					var j1 types.BinaryJSON
					j1, err = types.ParseBinaryJSONFromString(x)
					require.NoError(t, err)
					j2 := d.GetMysqlJSON()
					var cmp int
					cmp = types.CompareBinaryJSON(j1, j2)
					require.Equal(t, 0, cmp)
				}
				continue
			}
		}
		require.Error(t, err)
	}
}

func TestJSONMerge(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.JSONMerge]
	tbl := []struct {
		Input    []any
		Expected any
	}{
		{[]any{nil, nil}, nil},
		{[]any{`{}`, `[]`}, `[{}]`},
		{[]any{`{}`, `[]`, `3`, `"4"`}, `[{}, 3, "4"]`},
	}
	for _, tt := range tbl {
		args := types.MakeDatums(tt.Input...)
		f, err := fc.getFunction(ctx, datumsToConstants(args))
		require.NoError(t, err)
		d, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)

		switch x := tt.Expected.(type) {
		case string:
			j1, err := types.ParseBinaryJSONFromString(x)
			require.NoError(t, err)
			j2 := d.GetMysqlJSON()
			cmp := types.CompareBinaryJSON(j1, j2)
			require.Zerof(t, cmp, "got %v expect %v", j1.String(), j2.String())
		case nil:
			require.True(t, d.IsNull())
		}
	}
}

func TestJSONMergePreserve(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.JSONMergePreserve]
	tbl := []struct {
		Input    []any
		Expected any
	}{
		{[]any{nil, nil}, nil},
		{[]any{`{}`, `[]`}, `[{}]`},
		{[]any{`{}`, `[]`, `3`, `"4"`}, `[{}, 3, "4"]`},
	}
	for _, tt := range tbl {
		args := types.MakeDatums(tt.Input...)
		f, err := fc.getFunction(ctx, datumsToConstants(args))
		require.NoError(t, err)
		d, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)

		switch x := tt.Expected.(type) {
		case string:
			j1, err := types.ParseBinaryJSONFromString(x)
			require.NoError(t, err)
			j2 := d.GetMysqlJSON()
			cmp := types.CompareBinaryJSON(j1, j2)
			require.Zerof(t, cmp, "got %v expect %v", j1.String(), j2.String())
		case nil:
			require.True(t, d.IsNull())
		}
	}
}

func TestJSONArray(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.JSONArray]
	tbl := []struct {
		Input    []any
		Expected string
	}{
		{[]any{1}, `[1]`},
		{[]any{nil, "a", 3, `{"a": "b"}`}, `[null, "a", 3, "{\"a\": \"b\"}"]`},
	}
	for _, tt := range tbl {
		args := types.MakeDatums(tt.Input...)
		f, err := fc.getFunction(ctx, datumsToConstants(args))
		require.NoError(t, err)
		d, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)

		j1, err := types.ParseBinaryJSONFromString(tt.Expected)
		require.NoError(t, err)
		j2 := d.GetMysqlJSON()
		cmp := types.CompareBinaryJSON(j1, j2)
		require.Equal(t, 0, cmp)
	}
}

func TestJSONObject(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.JSONObject]
	tbl := []struct {
		Input        []any
		Expected     any
		BuildSuccess bool
		Success      bool
	}{
		{[]any{1, 2, 3}, nil, false, false},
		{[]any{1, 2, "hello", nil}, `{"1": 2, "hello": null}`, true, true},
		{[]any{nil, 2}, nil, true, false},

		// TiDB can only tell booleans from parser.
		{[]any{1, true}, `{"1": 1}`, true, true},
	}
	var err error
	var f builtinFunc
	var d types.Datum
	for _, tt := range tbl {
		args := types.MakeDatums(tt.Input...)
		f, err = fc.getFunction(ctx, datumsToConstants(args))
		if tt.BuildSuccess {
			require.NoError(t, err)
			d, err = evalBuiltinFunc(f, ctx, chunk.Row{})
			if tt.Success {
				require.NoError(t, err)
				switch x := tt.Expected.(type) {
				case string:
					var j1 types.BinaryJSON
					j1, err = types.ParseBinaryJSONFromString(x)
					require.NoError(t, err)
					j2 := d.GetMysqlJSON()
					var cmp int
					cmp = types.CompareBinaryJSON(j1, j2)
					require.Equal(t, 0, cmp)
				}
				continue
			}
		}
		require.Error(t, err)
	}
}

func TestJSONRemove(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.JSONRemove]
	tbl := []struct {
		Input    []any
		Expected any
		Success  bool
	}{
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, "$"}, nil, false},
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, "$.*"}, nil, false},
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, "$[*]"}, nil, false},
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, "$**.a"}, nil, false},

		{[]any{nil, "$.a"}, nil, true},
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, "$.a[2].aa"}, `{"a": [1, 2, {}]}`, true},
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, "$.a[1]"}, `{"a": [1, {"aa": "xx"}]}`, true},

		// Tests multi path expressions.
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, "$.a[2].aa", "$.a[1]"}, `{"a": [1, {}]}`, true},
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, "$.a[1]", "$.a[1].aa"}, `{"a": [1, {}]}`, true},

		// Tests path expressions not exists.
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, "$.a[3]"}, `{"a": [1, 2, {"aa": "xx"}]}`, true},
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, "$.b"}, `{"a": [1, 2, {"aa": "xx"}]}`, true},
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, "$.a[3]", "$.b"}, `{"a": [1, 2, {"aa": "xx"}]}`, true},
	}
	for _, tt := range tbl {
		args := types.MakeDatums(tt.Input...)
		f, err := fc.getFunction(ctx, datumsToConstants(args))
		require.NoError(t, err)
		d, err := evalBuiltinFunc(f, ctx, chunk.Row{})

		if tt.Success {
			require.NoError(t, err)
			switch x := tt.Expected.(type) {
			case string:
				var j1 types.BinaryJSON
				j1, err = types.ParseBinaryJSONFromString(x)
				require.NoError(t, err)
				j2 := d.GetMysqlJSON()
				var cmp int
				cmp = types.CompareBinaryJSON(j1, j2)
				require.Zerof(t, cmp, "got %v expect %v", j2.Value, j1.Value)
			}
		} else {
			require.Error(t, err)
		}
	}
}

func TestJSONMemberOf(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.JSONMemberOf]
	tbl := []struct {
		input    []any
		expected any
		err      error
	}{
		{[]any{`1`, `a:1`}, 1, types.ErrInvalidJSONText},

		{[]any{1, `[1, 2]`}, 1, nil},
		{[]any{1, `[1]`}, 1, nil},
		{[]any{1, `[0]`}, 0, nil},
		{[]any{1, `[1]`}, 1, nil},
		{[]any{1, `[[1]]`}, 0, nil},
		{[]any{"1", `[1]`}, 0, nil},
		{[]any{"1", `["1"]`}, 1, nil},
		{[]any{`{"a":1}`, `{"a":1}`}, 0, nil},
		{[]any{`{"a":1}`, `[{"a":1}]`}, 0, nil},
		{[]any{`{"a":1}`, `[{"a":1}, 1]`}, 0, nil},
		{[]any{`{"a":1}`, `["{\"a\":1}"]`}, 1, nil},
		{[]any{`{"a":1}`, `["{\"a\":1}", 1]`}, 1, nil},
	}
	for _, tt := range tbl {
		args := types.MakeDatums(tt.input...)
		f, err := fc.getFunction(ctx, datumsToConstants(args))
		require.NoError(t, err, tt.input)
		d, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		if tt.err == nil {
			require.NoError(t, err, tt.input)
			if tt.expected == nil {
				require.True(t, d.IsNull(), tt.input)
			} else {
				require.Equal(t, int64(tt.expected.(int)), d.GetInt64(), tt.input)
			}
		} else {
			require.True(t, tt.err.(*terror.Error).Equal(err), tt.input)
		}
	}
}

func TestJSONContains(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.JSONContains]
	tbl := []struct {
		input    []any
		expected any
		err      error
	}{
		// Tests nil arguments
		{[]any{nil, `1`, "$.c"}, nil, nil},
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, nil, "$.a[3]"}, nil, nil},
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, `1`, nil}, nil, nil},
		// Tests with path expression
		{[]any{`[1,2,[1,[5,[3]]]]`, `[1,3]`, "$[2]"}, 1, nil},
		{[]any{`[1,2,[1,[5,{"a":[2,3]}]]]`, `[1,{"a":[3]}]`, "$[2]"}, 1, nil},
		{[]any{`[{"a":1}]`, `{"a":1}`, "$"}, 1, nil},
		{[]any{`[{"a":1,"b":2}]`, `{"a":1,"b":2}`, "$"}, 1, nil},
		{[]any{`[{"a":{"a":1},"b":2}]`, `{"a":1}`, "$.a"}, 0, nil},
		// Tests without path expression
		{[]any{`{}`, `{}`}, 1, nil},
		{[]any{`{"a":1}`, `{}`}, 1, nil},
		{[]any{`{"a":1}`, `1`}, 0, nil},
		{[]any{`{"a":[1]}`, `[1]`}, 0, nil},
		{[]any{`{"b":2, "c":3}`, `{"c":3}`}, 1, nil},
		{[]any{`1`, `1`}, 1, nil},
		{[]any{`[1]`, `1`}, 1, nil},
		{[]any{`[1,2]`, `[1]`}, 1, nil},
		{[]any{`[1,2]`, `[1,3]`}, 0, nil},
		{[]any{`[1,2]`, `["1"]`}, 0, nil},
		{[]any{`[1,2,[1,3]]`, `[1,3]`}, 1, nil},
		{[]any{`[1,2,[1,3]]`, `[1,      3]`}, 1, nil},
		{[]any{`[1,2,[1,[5,[3]]]]`, `[1,3]`}, 1, nil},
		{[]any{`[1,2,[1,[5,{"a":[2,3]}]]]`, `[1,{"a":[3]}]`}, 1, nil},
		{[]any{`[{"a":1}]`, `{"a":1}`}, 1, nil},
		{[]any{`[{"a":1,"b":2}]`, `{"a":1}`}, 1, nil},
		{[]any{`[{"a":{"a":1},"b":2}]`, `{"a":1}`}, 0, nil},
		// Tests path expression contains any asterisk
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, `1`, "$.*"}, nil, types.ErrInvalidJSONPathMultipleSelection},
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, `1`, "$[*]"}, nil, types.ErrInvalidJSONPathMultipleSelection},
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, `1`, "$**.a"}, nil, types.ErrInvalidJSONPathMultipleSelection},
		// Tests path expression does not identify a section of the target document
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, `1`, "$.c"}, nil, nil},
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, `1`, "$.a[3]"}, nil, nil},
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, `1`, "$.a[2].b"}, nil, nil},
		// For issue 9957: test 'argument 1 and 2 as valid json object'
		{[]any{`[1,2,[1,3]]`, `a:1`}, 1, types.ErrInvalidJSONText},
		{[]any{`a:1`, `1`}, 1, types.ErrInvalidJSONText},
	}
	for _, tt := range tbl {
		args := types.MakeDatums(tt.input...)
		f, err := fc.getFunction(ctx, datumsToConstants(args))
		require.NoError(t, err)
		d, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		if tt.err == nil {
			require.NoError(t, err)
			if tt.expected == nil {
				require.True(t, d.IsNull())
			} else {
				require.Equal(t, int64(tt.expected.(int)), d.GetInt64())
			}
		} else {
			require.True(t, tt.err.(*terror.Error).Equal(err))
		}
	}
	// For issue 9957: test 'argument 1 and 2 as valid json object'
	cases := []struct {
		arg1 any
		arg2 any
	}{
		{1, ""},
		{0.05, ""},
		{"", 1},
		{"", 0.05},
	}
	for _, cs := range cases {
		_, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(cs.arg1, cs.arg2)))
		require.True(t, types.ErrInvalidJSONData.Equal(err))
	}
}

func TestJSONOverlaps(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.JSONOverlaps]
	tbl := []struct {
		input    []any
		expected any
		err      error
	}{
		{[]any{`[1,2,[1,3]]`, `a:1`}, 1, types.ErrInvalidJSONText},
		{[]any{`a:1`, `1`}, 1, types.ErrInvalidJSONText},
		{[]any{nil, `1`}, nil, nil},
		{[]any{`1`, nil}, nil, nil},

		{[]any{`[1, 2]`, `[2,3]`}, 1, nil},
		{[]any{`[1, 2]`, `[2]`}, 1, nil},
		{[]any{`[1, 2]`, `2`}, 1, nil},
		{[]any{`[{"a":1}]`, `{"a":1}`}, 1, nil},
		{[]any{`[{"a":1}]`, `{"a":1,"b":2}`}, 0, nil},
		{[]any{`[{"a":1}]`, `{"a":2}`}, 0, nil},
		{[]any{`{"a":[1,2]}`, `{"a":[1]}`}, 0, nil},
		{[]any{`{"a":[1,2]}`, `{"a":[2,1]}`}, 0, nil},
		{[]any{`[1,1,1]`, `1`}, 1, nil},
		{[]any{`1`, `1`}, 1, nil},
		{[]any{`0`, `1`}, 0, nil},
		{[]any{`[[1,2], 3]`, `[1,[2,3]]`}, 0, nil},
		{[]any{`[[1,2], 3]`, `[1,3]`}, 1, nil},
		{[]any{`{"a":1,"b":10,"d":10}`, `{"a":5,"e":10,"f":1,"d":20}`}, 0, nil},
		{[]any{`[4,5,"6",7]`, `6`}, 0, nil},
		{[]any{`[4,5,6,7]`, `"6"`}, 0, nil},

		{[]any{`[2,3]`, `[1, 2]`}, 1, nil},
		{[]any{`[2]`, `[1, 2]`}, 1, nil},
		{[]any{`2`, `[1, 2]`}, 1, nil},
		{[]any{`{"a":1}`, `[{"a":1}]`}, 1, nil},
		{[]any{`{"a":1,"b":2}`, `[{"a":1}]`}, 0, nil},
		{[]any{`{"a":2}`, `[{"a":1}]`}, 0, nil},
		{[]any{`{"a":[1]}`, `{"a":[1,2]}`}, 0, nil},
		{[]any{`{"a":[2,1]}`, `{"a":[1,2]}`}, 0, nil},
		{[]any{`1`, `[1,1,1]`}, 1, nil},
		{[]any{`1`, `1`}, 1, nil},
		{[]any{`1`, `0`}, 0, nil},
		{[]any{`[1,[2,3]]`, `[[1,2], 3]`}, 0, nil},
		{[]any{`[1,3]`, `[[1,2], 3]`}, 1, nil},
		{[]any{`{"a":5,"e":10,"f":1,"d":20}`, `{"a":1,"b":10,"d":10}`}, 0, nil},
		{[]any{`6`, `[4,5,"6",7]`}, 0, nil},
		{[]any{`"6"`, `[4,5,6,7]`}, 0, nil},
	}
	for _, tt := range tbl {
		args := types.MakeDatums(tt.input...)
		f, err := fc.getFunction(ctx, datumsToConstants(args))
		require.NoError(t, err, tt.input)
		d, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		if tt.err == nil {
			require.NoError(t, err, tt.input)
			if tt.expected == nil {
				require.True(t, d.IsNull(), tt.input)
			} else {
				require.Equal(t, int64(tt.expected.(int)), d.GetInt64(), tt.input)
			}
		} else {
			require.True(t, tt.err.(*terror.Error).Equal(err), tt.input)
		}
	}
}

func TestJSONContainsPath(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.JSONContainsPath]
	jsonString := `{"a": 1, "b": 2, "c": {"d": 4}}`
	invalidJSON := `{"a": 1`
	tbl := []struct {
		input    []any
		expected any
		success  bool
	}{
		// Tests nil arguments
		{[]any{nil, types.JSONContainsPathOne, "$.c"}, nil, true},
		{[]any{nil, types.JSONContainsPathAll, "$.c"}, nil, true},
		{[]any{jsonString, nil, "$.a[3]"}, nil, true},
		{[]any{jsonString, types.JSONContainsPathOne, nil}, nil, true},
		{[]any{jsonString, types.JSONContainsPathAll, nil}, nil, true},
		// Tests with one path expression
		{[]any{jsonString, types.JSONContainsPathOne, "$.c.d"}, 1, true},
		{[]any{jsonString, types.JSONContainsPathOne, "$.a.d"}, 0, true},
		{[]any{jsonString, types.JSONContainsPathAll, "$.c.d"}, 1, true},
		{[]any{jsonString, types.JSONContainsPathAll, "$.a.d"}, 0, true},
		// Tests with multiple path expression
		{[]any{jsonString, types.JSONContainsPathOne, "$.a", "$.e"}, 1, true},
		{[]any{jsonString, types.JSONContainsPathOne, "$.a", "$.c"}, 1, true},
		{[]any{jsonString, types.JSONContainsPathAll, "$.a", "$.e"}, 0, true},
		{[]any{jsonString, types.JSONContainsPathAll, "$.a", "$.c"}, 1, true},
		// Tests path expression contains any asterisk
		{[]any{jsonString, types.JSONContainsPathOne, "$.*"}, 1, true},
		{[]any{jsonString, types.JSONContainsPathOne, "$[*]"}, 0, true},
		{[]any{jsonString, types.JSONContainsPathAll, "$.*"}, 1, true},
		{[]any{jsonString, types.JSONContainsPathAll, "$[*]"}, 0, true},
		// Tests invalid json document
		{[]any{invalidJSON, types.JSONContainsPathOne, "$.a"}, nil, false},
		{[]any{invalidJSON, types.JSONContainsPathAll, "$.a"}, nil, false},
		// Tests compatible contains path
		{[]any{jsonString, "ONE", "$.c.d"}, 1, true},
		{[]any{jsonString, "ALL", "$.c.d"}, 1, true},
		{[]any{jsonString, "One", "$.a", "$.e"}, 1, true},
		{[]any{jsonString, "aLl", "$.a", "$.e"}, 0, true},
		{[]any{jsonString, "test", "$.a"}, nil, false},
	}
	for _, tt := range tbl {
		args := types.MakeDatums(tt.input...)
		f, err := fc.getFunction(ctx, datumsToConstants(args))
		require.NoError(t, err)
		d, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		if tt.success {
			require.NoError(t, err)
			if tt.expected == nil {
				require.True(t, d.IsNull())
			} else {
				require.Equal(t, int64(tt.expected.(int)), d.GetInt64())
			}
		} else {
			require.Error(t, err)
		}
	}
}

func TestJSONLength(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.JSONLength]
	tbl := []struct {
		input    []any
		expected any
		success  bool
	}{
		// Tests scalar arguments
		{[]any{`null`}, 1, true},
		{[]any{`true`}, 1, true},
		{[]any{`false`}, 1, true},
		{[]any{`1`}, 1, true},
		{[]any{`-1`}, 1, true},
		{[]any{`1.1`}, 1, true},
		{[]any{`"1"`}, 1, true},
		{[]any{`"1"`, "$.a"}, nil, true},
		{[]any{`null`, "$.a"}, nil, true},
		// Tests nil arguments
		{[]any{nil}, nil, true},
		{[]any{nil, "a"}, nil, true},
		{[]any{`{"a": 1}`, nil}, nil, true},
		{[]any{nil, nil}, nil, true},
		// Tests with path expression
		{[]any{`[1,2,[1,[5,[3]]]]`, "$[2]"}, 2, true},
		{[]any{`[{"a":1}]`, "$"}, 1, true},
		{[]any{`[{"a":1,"b":2}]`, "$[0].a"}, 1, true},
		{[]any{`{"a":{"a":1},"b":2}`, "$"}, 2, true},
		{[]any{`{"a":{"a":1},"b":2}`, "$.a"}, 1, true},
		{[]any{`{"a":{"a":1},"b":2}`, "$.a.a"}, 1, true},
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, "$.a[2].aa"}, 1, true},
		// Tests without path expression
		{[]any{`{}`}, 0, true},
		{[]any{`{"a":1}`}, 1, true},
		{[]any{`{"a":[1]}`}, 1, true},
		{[]any{`{"b":2, "c":3}`}, 2, true},
		{[]any{`[1]`}, 1, true},
		{[]any{`[1,2]`}, 2, true},
		{[]any{`[1,2,[1,3]]`}, 3, true},
		{[]any{`[1,2,[1,[5,[3]]]]`}, 3, true},
		{[]any{`[1,2,[1,[5,{"a":[2,3]}]]]`}, 3, true},
		{[]any{`[{"a":1}]`}, 1, true},
		{[]any{`[{"a":1,"b":2}]`}, 1, true},
		{[]any{`[{"a":{"a":1},"b":2}]`}, 1, true},
		// Tests path expression contains any asterisk
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, "$.*"}, nil, false},
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, "$[*]"}, nil, false},
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, "$**.a"}, nil, false},
		// Tests path expression does not identify a section of the target document
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, "$.c"}, nil, true},
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, "$.a[3]"}, nil, true},
		{[]any{`{"a": [1, 2, {"aa": "xx"}]}`, "$.a[2].b"}, nil, true},
	}
	for _, tt := range tbl {
		args := types.MakeDatums(tt.input...)
		f, err := fc.getFunction(ctx, datumsToConstants(args))
		require.NoError(t, err)
		d, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		if tt.success {
			require.NoError(t, err)

			if tt.expected == nil {
				require.True(t, d.IsNull())
			} else {
				require.Equal(t, int64(tt.expected.(int)), d.GetInt64())
			}
		} else {
			require.Error(t, err)
		}
	}
}

func TestJSONKeys(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.JSONKeys]
	tbl := []struct {
		input    []any
		expected any
		success  bool
	}{

		// Tests nil arguments
		{[]any{nil}, nil, true},
		{[]any{nil, "$.c"}, nil, true},
		{[]any{`{"a": 1}`, nil}, nil, true},
		{[]any{nil, nil}, nil, true},

		// Tests with other type
		{[]any{`1`}, nil, true},
		{[]any{`"str"`}, nil, true},
		{[]any{`true`}, nil, true},
		{[]any{`null`}, nil, true},
		{[]any{`[1, 2]`}, nil, true},
		{[]any{`["1", "2"]`}, nil, true},

		// Tests without path expression
		{[]any{`{}`}, `[]`, true},
		{[]any{`{"a": 1}`}, `["a"]`, true},
		{[]any{`{"a": 1, "b": 2}`}, `["a", "b"]`, true},
		{[]any{`{"a": {"c": 3}, "b": 2}`}, `["a", "b"]`, true},

		// Tests with path expression
		{[]any{`{"a": 1}`, "$.a"}, nil, true},
		{[]any{`{"a": {"c": 3}, "b": 2}`, "$.a"}, `["c"]`, true},
		{[]any{`{"a": {"c": 3}, "b": 2}`, "$.a.c"}, nil, true},
		{[]any{`{"a": {"c": 3}, "b": 2}`, nil}, nil, true},

		// Tests path expression contains any asterisk
		{[]any{`{}`, "$.*"}, nil, false},
		{[]any{`{"a": 1}`, "$.*"}, nil, false},
		{[]any{`{"a": {"c": 3}, "b": 2}`, "$.*"}, nil, false},
		{[]any{`{"a": {"c": 3}, "b": 2}`, "$.a.*"}, nil, false},

		// Tests path expression does not identify a section of the target document
		{[]any{`{"a": 1}`, "$.b"}, nil, true},
		{[]any{`{"a": {"c": 3}, "b": 2}`, "$.c"}, nil, true},
		{[]any{`{"a": {"c": 3}, "b": 2}`, "$.a.d"}, nil, true},
	}
	for _, tt := range tbl {
		args := types.MakeDatums(tt.input...)
		f, err := fc.getFunction(ctx, datumsToConstants(args))
		require.NoError(t, err)
		d, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		if tt.success {
			require.NoError(t, err)
			switch x := tt.expected.(type) {
			case string:
				var j1 types.BinaryJSON
				j1, err = types.ParseBinaryJSONFromString(x)
				require.NoError(t, err)
				j2 := d.GetMysqlJSON()
				var cmp int
				cmp = types.CompareBinaryJSON(j1, j2)
				require.Equal(t, 0, cmp)
			case nil:
				require.True(t, d.IsNull())
			}
		} else {
			require.Error(t, err)
		}
	}
}

func TestJSONDepth(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.JSONDepth]
	tbl := []struct {
		input    []any
		expected any
		success  bool
	}{
		// Tests scalar arguments
		{[]any{`null`}, 1, true},
		{[]any{`true`}, 1, true},
		{[]any{`false`}, 1, true},
		{[]any{`1`}, 1, true},
		{[]any{`-1`}, 1, true},
		{[]any{`1.1`}, 1, true},
		{[]any{`"1"`}, 1, true},
		// Tests nil arguments
		{[]any{nil}, nil, true},
		// Tests depth
		{[]any{`{}`}, 1, true},
		{[]any{`[]`}, 1, true},
		{[]any{`[10, 20]`}, 2, true},
		{[]any{`[[], {}]`}, 2, true},
		{[]any{`{"Name": "Homer"}`}, 2, true},
		{[]any{`[10, {"a": 20}]`}, 3, true},
		{[]any{`{"Person": {"Name": "Homer", "Age": 39, "Hobbies": ["Eating", "Sleeping"]} }`}, 4, true},
		{[]any{`{"a":1}`}, 2, true},
		{[]any{`{"a":[1]}`}, 3, true},
		{[]any{`{"b":2, "c":3}`}, 2, true},
		{[]any{`[1]`}, 2, true},
		{[]any{`[1,2]`}, 2, true},
		{[]any{`[1,2,[1,3]]`}, 3, true},
		{[]any{`[1,2,[1,[5,[3]]]]`}, 5, true},
		{[]any{`[1,2,[1,[5,{"a":[2,3]}]]]`}, 6, true},
		{[]any{`[{"a":1}]`}, 3, true},
		{[]any{`[{"a":1,"b":2}]`}, 3, true},
		{[]any{`[{"a":{"a":1},"b":2}]`}, 4, true},
		// Tests non-json
		{[]any{`a`}, nil, false},
	}
	for _, tt := range tbl {
		args := types.MakeDatums(tt.input...)
		f, err := fc.getFunction(ctx, datumsToConstants(args))
		require.NoError(t, err)
		d, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		if tt.success {
			require.NoError(t, err)

			if tt.expected == nil {
				require.True(t, d.IsNull())
			} else {
				require.Equal(t, int64(tt.expected.(int)), d.GetInt64())
			}
		} else {
			require.Error(t, err)
		}
	}
}

func TestJSONArrayAppend(t *testing.T) {
	ctx := createContext(t)
	sampleJSON, err := types.ParseBinaryJSONFromString(`{"b": 2}`)
	require.NoError(t, err)
	fc := funcs[ast.JSONArrayAppend]
	tbl := []struct {
		input    []any
		expected any
		err      *terror.Error
	}{
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, `$.d`, `z`}, `{"a": 1, "b": [2, 3], "c": 4}`, nil},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, `$`, `w`}, `[{"a": 1, "b": [2, 3], "c": 4}, "w"]`, nil},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, `$`, nil}, `[{"a": 1, "b": [2, 3], "c": 4}, null]`, nil},
		{[]any{`{"a": 1}`, `$`, `{"b": 2}`}, `[{"a": 1}, "{\"b\": 2}"]`, nil},
		{[]any{`{"a": 1}`, `$`, sampleJSON}, `[{"a": 1}, {"b": 2}]`, nil},
		{[]any{`{"a": 1}`, `$.a`, sampleJSON}, `{"a": [1, {"b": 2}]}`, nil},

		{[]any{`{"a": 1}`, `$.a`, sampleJSON, `$.a[1]`, sampleJSON}, `{"a": [1, [{"b": 2}, {"b": 2}]]}`, nil},
		{[]any{nil, `$`, nil}, nil, nil},
		{[]any{nil, `$`, `a`}, nil, nil},
		{[]any{`null`, `$`, nil}, `[null, null]`, nil},
		{[]any{`[]`, `$`, nil}, `[null]`, nil},
		{[]any{`{}`, `$`, nil}, `[{}, null]`, nil},
		// Bad arguments.
		{[]any{`asdf`, `$`, nil}, nil, types.ErrInvalidJSONText},
		{[]any{``, `$`, nil}, nil, types.ErrInvalidJSONText},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, `$.d`}, nil, ErrIncorrectParameterCount},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, `$.c`, `y`, `$.b`}, nil, ErrIncorrectParameterCount},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, nil, nil}, nil, nil},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, `asdf`, nil}, nil, types.ErrInvalidJSONPath},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, 42, nil}, nil, types.ErrInvalidJSONPath},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, `$.*`, nil}, nil, types.ErrInvalidJSONPathMultipleSelection},
		// Following tests come from MySQL doc.
		{[]any{`["a", ["b", "c"], "d"]`, `$[1]`, 1}, `["a", ["b", "c", 1], "d"]`, nil},
		{[]any{`["a", ["b", "c"], "d"]`, `$[0]`, 2}, `[["a", 2], ["b", "c"], "d"]`, nil},
		{[]any{`["a", ["b", "c"], "d"]`, `$[1][0]`, 3}, `["a", [["b", 3], "c"], "d"]`, nil},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, `$.b`, `x`}, `{"a": 1, "b": [2, 3, "x"], "c": 4}`, nil},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, `$.c`, `y`}, `{"a": 1, "b": [2, 3], "c": [4, "y"]}`, nil},
		// Following tests come from MySQL test.
		{[]any{`[1,2,3, {"a":[4,5,6]}]`, `$`, 7}, `[1, 2, 3, {"a": [4, 5, 6]}, 7]`, nil},
		{[]any{`[1,2,3, {"a":[4,5,6]}]`, `$`, 7, `$[3].a`, 3.14}, `[1, 2, 3, {"a": [4, 5, 6, 3.14]}, 7]`, nil},
		{[]any{`[1,2,3, {"a":[4,5,6]}]`, `$`, 7, `$[3].b`, 8}, `[1, 2, 3, {"a": [4, 5, 6]}, 7]`, nil},
	}

	for i, tt := range tbl {
		args := types.MakeDatums(tt.input...)
		ctx.GetSessionVars().StmtCtx.SetWarnings(nil)
		f, err := fc.getFunction(ctx, datumsToConstants(args))
		// No error should return in getFunction if tt.err is nil.
		if err != nil {
			require.Error(t, tt.err)
			require.True(t, tt.err.Equal(err))
			continue
		}

		require.NotNil(t, f)
		d, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		comment := fmt.Sprintf("case:%v \n input:%v \n output: %s \n expected: %v \n warnings: %v \n expected error %v", i, tt.input, d.GetMysqlJSON(), tt.expected, ctx.GetSessionVars().StmtCtx.GetWarnings(), tt.err)

		if tt.err != nil {
			require.True(t, tt.err.Equal(err), comment)
			continue
		}

		require.NoError(t, err, comment)
		require.Equal(t, 0, int(ctx.GetSessionVars().StmtCtx.WarningCount()), comment)

		if tt.expected == nil {
			require.True(t, d.IsNull(), comment)
			continue
		}

		j1, err := types.ParseBinaryJSONFromString(tt.expected.(string))

		require.NoError(t, err, comment)
		require.Equal(t, 0, types.CompareBinaryJSON(j1, d.GetMysqlJSON()), comment)
	}
}

func TestJSONSearch(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.JSONSearch]
	jsonString := `["abc", [{"k": "10"}, "def"], {"x":"abc"}, {"y":"bcd"}]`
	jsonString2 := `["abc", [{"k": "10"}, "def"], {"x":"ab%d"}, {"y":"abcd"}]`
	tbl := []struct {
		input    []any
		expected any
		success  bool
	}{
		// simple case
		{[]any{jsonString, `one`, `abc`}, `"$[0]"`, true},
		{[]any{jsonString, `all`, `abc`}, `["$[0]", "$[2].x"]`, true},
		{[]any{jsonString, `all`, `ghi`}, nil, true},
		{[]any{jsonString, `ALL`, `ghi`}, nil, true},
		{[]any{jsonString, `all`, `10`}, `"$[1][0].k"`, true},
		{[]any{jsonString, `all`, `10`, nil, `$`}, `"$[1][0].k"`, true},
		{[]any{jsonString, `all`, `10`, nil, `$[*]`}, `"$[1][0].k"`, true},
		{[]any{jsonString, `all`, `10`, nil, `$**.k`}, `"$[1][0].k"`, true},
		{[]any{jsonString, `all`, `10`, nil, `$[*][0].k`}, `"$[1][0].k"`, true},
		{[]any{jsonString, `all`, `10`, nil, `$[1]`}, `"$[1][0].k"`, true},
		{[]any{jsonString, `all`, `10`, nil, `$[1][0]`}, `"$[1][0].k"`, true},
		{[]any{jsonString, `all`, `abc`, nil, `$[2]`}, `"$[2].x"`, true},
		{[]any{jsonString, `all`, `abc`, nil, `$[2]`, `$[0]`}, `["$[2].x", "$[0]"]`, true},
		{[]any{jsonString, `all`, `abc`, nil, `$[2]`, `$[2]`}, `"$[2].x"`, true},

		// search pattern
		{[]any{jsonString, `all`, `%a%`}, `["$[0]", "$[2].x"]`, true},
		{[]any{jsonString, `all`, `%b%`}, `["$[0]", "$[2].x", "$[3].y"]`, true},
		{[]any{jsonString, `all`, `%b%`, nil, `$[0]`}, `"$[0]"`, true},
		{[]any{jsonString, `all`, `%b%`, nil, `$[2]`}, `"$[2].x"`, true},
		{[]any{jsonString, `all`, `%b%`, nil, `$[1]`}, nil, true},
		{[]any{jsonString, `all`, `%b%`, ``, `$[1]`}, nil, true},
		{[]any{jsonString, `all`, `%b%`, nil, `$[3]`}, `"$[3].y"`, true},
		{[]any{jsonString2, `all`, `ab_d`}, `["$[2].x", "$[3].y"]`, true},

		// escape char
		{[]any{jsonString2, `all`, `ab%d`}, `["$[2].x", "$[3].y"]`, true},
		{[]any{jsonString2, `all`, `ab\%d`}, `"$[2].x"`, true},
		{[]any{jsonString2, `all`, `ab|%d`, `|`}, `"$[2].x"`, true},

		// error handle
		{[]any{nil, `all`, `abc`}, nil, true},                     // NULL json
		{[]any{`a`, `all`, `abc`}, nil, false},                    // non json
		{[]any{jsonString, `wrong`, `abc`}, nil, false},           // wrong one_or_all
		{[]any{jsonString, `all`, nil}, nil, true},                // NULL search_str
		{[]any{jsonString, `all`, `abc`, `??`}, nil, false},       // wrong escape_char
		{[]any{jsonString, `all`, `abc`, nil, nil}, nil, true},    // NULL path
		{[]any{jsonString, `all`, `abc`, nil, `$xx`}, nil, false}, // wrong path
		{[]any{jsonString, nil, `abc`}, nil, true},
	}
	for _, tt := range tbl {
		args := types.MakeDatums(tt.input...)
		f, err := fc.getFunction(ctx, datumsToConstants(args))
		require.NoError(t, err)
		d, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		if tt.success {
			require.NoError(t, err)
			switch x := tt.expected.(type) {
			case string:
				var j1, j2 types.BinaryJSON
				j1, err = types.ParseBinaryJSONFromString(x)
				require.NoError(t, err)
				j2 = d.GetMysqlJSON()
				cmp := types.CompareBinaryJSON(j1, j2)
				require.Equal(t, 0, cmp)
			case nil:
				require.True(t, d.IsNull())
			}
		} else {
			require.Error(t, err)
		}
	}
}

func TestJSONArrayInsert(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.JSONArrayInsert]
	tbl := []struct {
		input    []any
		expected any
		success  bool
		err      *terror.Error
	}{
		// Success
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, `$.b[1]`, `z`}, `{"a": 1, "b": [2, "z", 3], "c": 4}`, true, nil},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, `$.a[1]`, `z`}, `{"a": 1, "b": [2, 3], "c": 4}`, true, nil},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, `$.d[1]`, `z`}, `{"a": 1, "b": [2, 3], "c": 4}`, true, nil},
		{[]any{`[{"a": 1, "b": [2, 3], "c": 4}]`, `$[1]`, `w`}, `[{"a": 1, "b": [2, 3], "c": 4}, "w"]`, true, nil},
		{[]any{`[{"a": 1, "b": [2, 3], "c": 4}]`, `$[0]`, nil}, `[null, {"a": 1, "b": [2, 3], "c": 4}]`, true, nil},
		{[]any{`[1, 2, 3]`, `$[100]`, `{"b": 2}`}, `[1, 2, 3, "{\"b\": 2}"]`, true, nil},
		// About null
		{[]any{nil, `$`, nil}, nil, true, nil},
		{[]any{nil, `$`, `a`}, nil, true, nil},
		{[]any{`[]`, `$[0]`, nil}, `[null]`, true, nil},
		{[]any{`{}`, `$[0]`, nil}, `{}`, true, nil},
		// Bad arguments
		{[]any{`asdf`, `$`, nil}, nil, false, types.ErrInvalidJSONText},
		{[]any{``, `$`, nil}, nil, false, types.ErrInvalidJSONText},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, `$.d`}, nil, false, ErrIncorrectParameterCount},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, `$.c`, `y`, `$.b`}, nil, false, ErrIncorrectParameterCount},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, nil, nil}, nil, true, nil},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, `asdf`, nil}, nil, false, types.ErrInvalidJSONPath},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, 42, nil}, nil, false, types.ErrInvalidJSONPath},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, `$.*`, nil}, nil, false, types.ErrInvalidJSONPathMultipleSelection},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, `$.b[0]`, nil, `$.a`, nil}, nil, false, types.ErrInvalidJSONPathArrayCell},
		{[]any{`{"a": 1, "b": [2, 3], "c": 4}`, `$.a`, nil}, nil, false, types.ErrInvalidJSONPathArrayCell},
		// Following tests come from MySQL doc.
		{[]any{`["a", {"b": [1, 2]}, [3, 4]]`, `$[1]`, `x`}, `["a", "x", {"b": [1, 2]}, [3, 4]]`, true, nil},
		{[]any{`["a", {"b": [1, 2]}, [3, 4]]`, `$[100]`, `x`}, `["a", {"b": [1, 2]}, [3, 4], "x"]`, true, nil},
		{[]any{`["a", {"b": [1, 2]}, [3, 4]]`, `$[1].b[0]`, `x`}, `["a", {"b": ["x", 1, 2]}, [3, 4]]`, true, nil},
		{[]any{`["a", {"b": [1, 2]}, [3, 4]]`, `$[2][1]`, `y`}, `["a", {"b": [1, 2]}, [3, "y", 4]]`, true, nil},
		{[]any{`["a", {"b": [1, 2]}, [3, 4]]`, `$[0]`, `x`, `$[2][1]`, `y`}, `["x", "a", {"b": [1, 2]}, [3, 4]]`, true, nil},
		// More test cases
		{[]any{`["a", {"b": [1, 2]}, [3, 4]]`, `$[0]`, `x`, `$[0]`, `y`}, `["y", "x", "a", {"b": [1, 2]}, [3, 4]]`, true, nil},
	}
	for _, tt := range tbl {
		args := types.MakeDatums(tt.input...)
		f, err := fc.getFunction(ctx, datumsToConstants(args))
		// Parameter count error
		if err != nil {
			require.Error(t, tt.err)
			require.True(t, tt.err.Equal(err))
			continue
		}

		d, err := evalBuiltinFunc(f, ctx, chunk.Row{})

		if tt.success {
			require.NoError(t, err)
			switch x := tt.expected.(type) {
			case string:
				var j1, j2 types.BinaryJSON
				j1, err = types.ParseBinaryJSONFromString(x)
				require.NoError(t, err)
				j2 = d.GetMysqlJSON()
				var cmp int
				cmp = types.CompareBinaryJSON(j1, j2)
				require.Equal(t, 0, cmp)
			case nil:
				require.True(t, d.IsNull())
			}
		} else {
			require.True(t, tt.err.Equal(err))
		}
	}
}

func TestJSONValid(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.JSONValid]
	tbl := []struct {
		Input    any
		Expected any
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
	for _, tt := range dtbl {
		f, err := fc.getFunction(ctx, datumsToConstants(tt["Input"]))
		require.NoError(t, err)
		d, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, tt["Expected"][0], d)
	}
}

func TestJSONStorageFree(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.JSONStorageFree]
	tbl := []struct {
		input    []any
		expected any
		success  bool
	}{
		// Tests scalar arguments
		{[]any{`null`}, 0, true},
		{[]any{`true`}, 0, true},
		{[]any{`1`}, 0, true},
		{[]any{`"1"`}, 0, true},
		// Tests nil arguments
		{[]any{nil}, nil, true},
		// Tests valid json documents
		{[]any{`{}`}, 0, true},
		{[]any{`{"a":1}`}, 0, true},
		{[]any{`[{"a":{"a":1},"b":2}]`}, 0, true},
		{[]any{`{"a": 1000, "b": "wxyz", "c": "[1, 3, 5, 7]"}`}, 0, true},
		// Tests invalid json documents
		{[]any{`[{"a":1]`}, 0, false},
		{[]any{`[{a":1]`}, 0, false},
	}
	for _, tt := range tbl {
		args := types.MakeDatums(tt.input...)
		f, err := fc.getFunction(ctx, datumsToConstants(args))
		require.NoError(t, err)
		d, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		if tt.success {
			require.NoError(t, err)

			if tt.expected == nil {
				require.True(t, d.IsNull())
			} else {
				require.Equal(t, int64(tt.expected.(int)), d.GetInt64())
			}
		} else {
			require.Error(t, err)
		}
	}
}

func TestJSONStorageSize(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.JSONStorageSize]
	tbl := []struct {
		input    []any
		expected any
		success  bool
	}{
		// Tests scalar arguments
		{[]any{`null`}, 2, true},
		{[]any{`true`}, 2, true},
		{[]any{`1`}, 9, true},
		{[]any{`"1"`}, 3, true},
		// Tests nil arguments
		{[]any{nil}, nil, true},
		// Tests valid json documents
		{[]any{`{}`}, 9, true},
		{[]any{`{"a":1}`}, 29, true},
		{[]any{`[{"a":{"a":1},"b":2}]`}, 82, true},
		{[]any{`{"a": 1000, "b": "wxyz", "c": "[1, 3, 5, 7]"}`}, 71, true},
		// Tests invalid json documents
		{[]any{`[{"a":1]`}, 0, false},
		{[]any{`[{a":1]`}, 0, false},
	}
	for _, tt := range tbl {
		args := types.MakeDatums(tt.input...)
		f, err := fc.getFunction(ctx, datumsToConstants(args))
		require.NoError(t, err)
		d, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		if tt.success {
			require.NoError(t, err)

			if tt.expected == nil {
				require.True(t, d.IsNull())
			} else {
				require.Equal(t, int64(tt.expected.(int)), d.GetInt64())
			}
		} else {
			require.Error(t, err)
		}
	}
}

func TestJSONPretty(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.JSONPretty]
	tbl := []struct {
		input    []any
		expected any
		success  bool
	}{
		// Tests scalar arguments
		{[]any{nil}, nil, true},
		{[]any{`true`}, "true", true},
		{[]any{`false`}, "false", true},
		{[]any{`2223`}, "2223", true},
		// Tests simple json
		{[]any{`{"a":1}`}, `{
  "a": 1
}`, true},
		{[]any{`[1]`}, `[
  1
]`, true},
		// Test complex json
		{[]any{`{"a":1,"b":[{"d":1},{"e":2},{"f":3}],"c":"eee"}`}, `{
  "a": 1,
  "b": [
    {
      "d": 1
    },
    {
      "e": 2
    },
    {
      "f": 3
    }
  ],
  "c": "eee"
}`, true},
		{[]any{`{"a":1,"b":"qwe","c":[1,2,3,"123",null],"d":{"d1":1,"d2":2}}`}, `{
  "a": 1,
  "b": "qwe",
  "c": [
    1,
    2,
    3,
    "123",
    null
  ],
  "d": {
    "d1": 1,
    "d2": 2
  }
}`, true},
		// Tests invalid json data
		{[]any{`{1}`}, nil, false},
		{[]any{`[1,3,4,5]]`}, nil, false},
	}
	for _, tt := range tbl {
		args := types.MakeDatums(tt.input...)
		f, err := fc.getFunction(ctx, datumsToConstants(args))
		require.NoError(t, err)
		d, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		if tt.success {
			require.NoError(t, err)

			if tt.expected == nil {
				require.True(t, d.IsNull())
			} else {
				require.Equal(t, tt.expected.(string), d.GetString())
			}
		} else {
			require.Error(t, err)
		}
	}
}

func TestJSONMergePatch(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.JSONMergePatch]
	tbl := []struct {
		input    []any
		expected any
		success  bool
	}{
		// RFC 7396 document: https://datatracker.ietf.org/doc/html/rfc7396
		// RFC 7396 Example Test Cases
		{[]any{`{"a":"b"}`, `{"a":"c"}`}, `{"a": "c"}`, true},
		{[]any{`{"a":"b"}`, `{"b":"c"}`}, `{"a": "b","b": "c"}`, true},
		{[]any{`{"a":"b"}`, `{"a":null}`}, `{}`, true},
		{[]any{`{"a":"b", "b":"c"}`, `{"a":null}`}, `{"b": "c"}`, true},
		{[]any{`{"a":["b"]}`, `{"a":"c"}`}, `{"a": "c"}`, true},
		{[]any{`{"a":"c"}`, `{"a":["b"]}`}, `{"a": ["b"]}`, true},
		{[]any{`{"a":{"b":"c"}}`, `{"a":{"b":"d","c":null}}`}, `{"a": {"b": "d"}}`, true},
		{[]any{`{"a":[{"b":"c"}]}`, `{"a": [1]}`}, `{"a": [1]}`, true},
		{[]any{`["a","b"]`, `["c","d"]`}, `["c", "d"]`, true},
		{[]any{`{"a":"b"}`, `["c"]`}, `["c"]`, true},
		{[]any{`{"a":"foo"}`, `null`}, `null`, true},
		{[]any{`{"a":"foo"}`, `"bar"`}, `"bar"`, true},
		{[]any{`{"e":null}`, `{"a":1}`}, `{"e": null,"a": 1}`, true},
		{[]any{`[1,2]`, `{"a":"b","c":null}`}, `{"a":"b"}`, true},
		{[]any{`{}`, `{"a":{"bb":{"ccc":null}}}`}, `{"a":{"bb": {}}}`, true},
		// RFC 7396 Example Document
		{[]any{`{"title":"Goodbye!","author":{"givenName":"John","familyName":"Doe"},"tags":["example","sample"],"content":"This will be unchanged"}`, `{"title":"Hello!","phoneNumber":"+01-123-456-7890","author":{"familyName":null},"tags":["example"]}`}, `{"title":"Hello!","author":{"givenName":"John"},"tags":["example"],"content":"This will be unchanged","phoneNumber":"+01-123-456-7890"}`, true},

		// From mysql Example Test Cases
		{[]any{nil, `null`, `[1,2,3]`, `{"a":1}`}, `{"a": 1}`, true},
		{[]any{`null`, nil, `[1,2,3]`, `{"a":1}`}, `{"a": 1}`, true},
		{[]any{`null`, `[1,2,3]`, nil, `{"a":1}`}, nil, true},
		{[]any{`null`, `[1,2,3]`, `{"a":1}`, nil}, nil, true},

		{[]any{nil, `null`, `{"a":1}`, `[1,2,3]`}, `[1,2,3]`, true},
		{[]any{`null`, nil, `{"a":1}`, `[1,2,3]`}, `[1,2,3]`, true},
		{[]any{`null`, `{"a":1}`, nil, `[1,2,3]`}, `[1,2,3]`, true},
		{[]any{`null`, `{"a":1}`, `[1,2,3]`, nil}, nil, true},

		{[]any{nil, `null`, `{"a":1}`, `true`}, `true`, true},
		{[]any{`null`, nil, `{"a":1}`, `true`}, `true`, true},
		{[]any{`null`, `{"a":1}`, nil, `true`}, `true`, true},
		{[]any{`null`, `{"a":1}`, `true`, nil}, nil, true},

		// non-object last item
		{[]any{"true", "false", "[]", "{}", "null"}, "null", true},
		{[]any{"false", "[]", "{}", "null", "true"}, "true", true},
		{[]any{"true", "[]", "{}", "null", "false"}, "false", true},
		{[]any{"true", "false", "{}", "null", "[]"}, "[]", true},
		{[]any{"true", "false", "{}", "null", "1"}, "1", true},
		{[]any{"true", "false", "{}", "null", "1.8"}, "1.8", true},
		{[]any{"true", "false", "{}", "null", `"112"`}, `"112"`, true},

		{[]any{`{"a":"foo"}`, nil}, nil, true},
		{[]any{nil, `{"a":"foo"}`}, nil, true},
		{[]any{`{"a":"foo"}`, `false`}, `false`, true},
		{[]any{`{"a":"foo"}`, `123`}, `123`, true},
		{[]any{`{"a":"foo"}`, `123.1`}, `123.1`, true},
		{[]any{`{"a":"foo"}`, `[1,2,3]`}, `[1,2,3]`, true},
		{[]any{`null`, `{"a":1}`}, `{"a":1}`, true},
		{[]any{`{"a":1}`, `null`}, `null`, true},
		{[]any{`{"a":"foo"}`, `{"a":null}`, `{"b":"123"}`, `{"c":1}`}, `{"b":"123","c":1}`, true},
		{[]any{`{"a":"foo"}`, `{"a":null}`, `{"c":1}`}, `{"c":1}`, true},
		{[]any{`{"a":"foo"}`, `{"a":null}`, `true`}, `true`, true},
		{[]any{`{"a":"foo"}`, `{"d":1}`, `{"a":{"bb":{"ccc":null}}}`}, `{"a":{"bb":{}},"d":1}`, true},

		// Invalid json text
		{[]any{`{"a":1}`, `[1]}`}, nil, false},
		{[]any{`{{"a":1}`, `[1]`, `null`}, nil, false},
		{[]any{`{"a":1}`, `jjj`, `null`}, nil, false},
	}
	for _, tt := range tbl {
		args := types.MakeDatums(tt.input...)
		f, err := fc.getFunction(ctx, datumsToConstants(args))
		require.NoError(t, err)
		d, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		if tt.success {
			require.NoError(t, err)

			if tt.expected == nil {
				require.True(t, d.IsNull())
			} else {
				j, e := types.ParseBinaryJSONFromString(tt.expected.(string))
				require.NoError(t, e)
				require.Equal(t, j.String(), d.GetMysqlJSON().String())
			}
		} else {
			require.Error(t, err)
		}
	}
}

func TestJSONSchemaValid(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.JSONSchemaValid]
	tbl := []struct {
		Input    any
		Expected any
	}{
		// nulls
		{[]any{nil, `{}`}, nil},
		{[]any{`{}`, nil}, nil},
		{[]any{nil, nil}, nil},

		// empty
		{[]any{`{}`, `{}`}, 1},

		// required
		{[]any{`{"required": ["a","b"]}`, `{"a": 5}`}, 0},
		{[]any{`{"required": ["a","b"]}`, `{"a": 5, "b": 6}`}, 1},

		// type
		{[]any{`{"type": ["string"]}`, `{}`}, 0},
		{[]any{`{"type": ["string"]}`, `"foobar"`}, 1},
		{[]any{`{"type": ["object"]}`, `{}`}, 1},
		{[]any{`{"type": ["object"]}`, `"foobar"`}, 0},

		// properties, type
		{[]any{`{"properties": {"a": {"type": "number"}}}`, `{}`}, 1},
		{[]any{`{"properties": {"a": {"type": "number"}}}`, `{"a": "foobar"}`}, 0},
		{[]any{`{"properties": {"a": {"type": "number"}}}`, `{"a": 5}`}, 1},

		// properties, minimum
		{[]any{`{"properties": {"a": {"type": "number", "minimum": 6}}}`, `{"a": 5}`}, 0},

		// properties, pattern
		{[]any{`{"properties": {"a": {"type": "string", "pattern": "^a"}}}`, `{"a": "abc"}`}, 1},
		{[]any{`{"properties": {"a": {"type": "string", "pattern": "^a"}}}`, `{"a": "cba"}`}, 0},
	}
	dtbl := tblToDtbl(tbl)
	for _, tt := range dtbl {
		f, err := fc.getFunction(ctx, datumsToConstants(tt["Input"]))
		require.NoError(t, err)
		d, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		if tt["Expected"][0].IsNull() {
			require.True(t, d.IsNull())
		} else {
			testutil.DatumEqual(
				t, tt["Expected"][0], d,
				fmt.Sprintf("JSON_SCHEMA_VALID(%s,%s) = %d (expected: %d)",
					tt["Input"][0].GetString(),
					tt["Input"][1].GetString(),
					d.GetInt64(),
					tt["Expected"][0].GetInt64(),
				),
			)
		}
	}
}

// TestJSONSchemaValidCache is to test if the cached schema is used
func TestJSONSchemaValidCache(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.JSONSchemaValid]
	tbl := []struct {
		Input    any
		Expected any
	}{
		{[]any{`{}`, `{}`}, 1},
	}
	dtbl := tblToDtbl(tbl)

	for _, tt := range dtbl {
		// Get the function and eval once, ensuring it is cached
		f, err := fc.getFunction(ctx, datumsToConstants(tt["Input"]))
		require.NoError(t, err)
		_, err = evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)

		// Disable the cache function
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/expression/jsonSchemaValidDisableCacheRefresh", `return(true)`))

		// This eval should use the cache and not call the function.
		_, err = evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)

		// Now get a new cache by getting the function again.
		f, err = fc.getFunction(ctx, datumsToConstants(tt["Input"]))
		require.NoError(t, err)

		// Empty cache, we call the function. This should return an error.
		_, err = evalBuiltinFunc(f, ctx, chunk.Row{})
		require.Error(t, err)
	}

	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/expression/jsonSchemaValidDisableCacheRefresh"))
}
