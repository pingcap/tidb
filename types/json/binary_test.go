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

package json

import (
	"math"
	"testing"

	. "github.com/pingcap/check"
)

var _ = Suite(&testJSONSuite{})

type testJSONSuite struct{}

func TestT(t *testing.T) {
	TestingT(t)
}

func (s *testJSONSuite) TestBinaryJSONMarshalUnmarshal(c *C) {
	c.Parallel()
	strs := []string{
		`{"a": [1, "2", {"aa": "bb"}, 4, null], "b": true, "c": null}`,
		`{"aaaaaaaaaaa": [1, "2", {"aa": "bb"}, 4.1], "bbbbbbbbbb": true, "ccccccccc": "d"}`,
		`[{"a": 1, "b": true}, 3, 3.5, "hello, world", null, true]`,
		`{"a": "&<>"}`,
	}
	for _, str := range strs {
		parsedBJ := mustParseBinaryFromString(c, str)
		c.Assert(parsedBJ.String(), Equals, str)
	}
}

func (s *testJSONSuite) TestBinaryJSONExtract(c *C) {
	c.Parallel()
	bj1 := mustParseBinaryFromString(c, `{"\"hello\"": "world", "a": [1, "2", {"aa": "bb"}, 4.0, {"aa": "cc"}], "b": true, "c": ["d"]}`)
	bj2 := mustParseBinaryFromString(c, `[{"a": 1, "b": true}, 3, 3.5, "hello, world", null, true]`)

	var tests = []struct {
		bj              BinaryJSON
		pathExprStrings []string
		expected        BinaryJSON
		found           bool
		err             error
	}{
		// test extract with only one path expression.
		{bj1, []string{"$.a"}, mustParseBinaryFromString(c, `[1, "2", {"aa": "bb"}, 4.0, {"aa": "cc"}]`), true, nil},
		{bj2, []string{"$.a"}, mustParseBinaryFromString(c, "null"), false, nil},
		{bj1, []string{"$[0]"}, bj1, true, nil}, // in Extract, autowraped bj1 as an array.
		{bj2, []string{"$[0]"}, mustParseBinaryFromString(c, `{"a": 1, "b": true}`), true, nil},
		{bj1, []string{"$.a[2].aa"}, mustParseBinaryFromString(c, `"bb"`), true, nil},
		{bj1, []string{"$.a[*].aa"}, mustParseBinaryFromString(c, `["bb", "cc"]`), true, nil},
		{bj1, []string{"$.*[0]"}, mustParseBinaryFromString(c, `["world", 1, true, "d"]`), true, nil},
		{bj1, []string{`$.a[*]."aa"`}, mustParseBinaryFromString(c, `["bb", "cc"]`), true, nil},
		{bj1, []string{`$."\"hello\""`}, mustParseBinaryFromString(c, `"world"`), true, nil},
		{bj1, []string{`$**[1]`}, mustParseBinaryFromString(c, `"2"`), true, nil},

		// test extract with multi path expressions.
		{bj1, []string{"$.a", "$[5]"}, mustParseBinaryFromString(c, `[[1, "2", {"aa": "bb"}, 4.0, {"aa": "cc"}]]`), true, nil},
		{bj2, []string{"$.a", "$[0]"}, mustParseBinaryFromString(c, `[{"a": 1, "b": true}]`), true, nil},
	}

	for _, tt := range tests {
		var pathExprList = make([]PathExpression, 0)
		for _, peStr := range tt.pathExprStrings {
			pe, err := ParseJSONPathExpr(peStr)
			c.Assert(err, IsNil)
			pathExprList = append(pathExprList, pe)
		}

		result, found := tt.bj.Extract(pathExprList)
		c.Assert(found, Equals, tt.found)
		if found {
			c.Assert(result.String(), Equals, tt.expected.String())
		}
	}
}

func (s *testJSONSuite) TestBinaryJSONType(c *C) {
	c.Parallel()
	var tests = []struct {
		In  string
		Out string
	}{
		{`{"a": "b"}`, "OBJECT"},
		{`["a", "b"]`, "ARRAY"},
		{`3`, "INTEGER"},
		{`3.0`, "DOUBLE"},
		{`null`, "NULL"},
		{`true`, "BOOLEAN"},
	}
	for _, tt := range tests {
		bj := mustParseBinaryFromString(c, tt.In)
		c.Assert(bj.Type(), Equals, tt.Out)
	}
	// we can't parse '9223372036854775808' to JSON::Uint64 now,
	// because go builtin JSON parser treats that as DOUBLE.
	c.Assert(CreateBinary(uint64(1<<63)).Type(), Equals, "UNSIGNED INTEGER")
}

func (s *testJSONSuite) TestBinaryJSONUnquote(c *C) {
	var tests = []struct {
		j        string
		unquoted string
	}{
		{j: `3`, unquoted: "3"},
		{j: `"3"`, unquoted: "3"},
		{j: `"[{\"x\":\"{\\\"y\\\":12}\"}]"`, unquoted: `[{"x":"{\"y\":12}"}]`},
		{j: `"hello, \"escaped quotes\" world"`, unquoted: "hello, \"escaped quotes\" world"},
		{j: "\"\\u4f60\"", unquoted: "你"},
		{j: `true`, unquoted: "true"},
		{j: `null`, unquoted: "null"},
		{j: `{"a": [1, 2]}`, unquoted: `{"a": [1, 2]}`},
		{j: `"\""`, unquoted: `"`},
		{j: `"'"`, unquoted: `'`},
		{j: `"''"`, unquoted: `''`},
		{j: `""`, unquoted: ``},
	}
	for _, tt := range tests {
		bj := mustParseBinaryFromString(c, tt.j)
		unquoted, err := bj.Unquote()
		c.Assert(err, IsNil)
		c.Assert(unquoted, Equals, tt.unquoted)
	}
}

func (s *testJSONSuite) TestQuoteString(c *C) {
	var tests = []struct {
		j      string
		quoted string
	}{
		{j: "3", quoted: `3`},
		{j: "hello, \"escaped quotes\" world", quoted: `"hello, \"escaped quotes\" world"`},
		{j: "你", quoted: `你`},
		{j: "true", quoted: `true`},
		{j: "null", quoted: `null`},
		{j: `"`, quoted: `"\""`},
		{j: `'`, quoted: `'`},
		{j: `''`, quoted: `''`},
		{j: ``, quoted: ``},
		{j: "\\ \" \b \f \n \r \t", quoted: `"\\ \" \b \f \n \r \t"`},
	}
	for _, tt := range tests {
		c.Assert(quoteString(tt.j), Equals, tt.quoted)
	}
}

func (s *testJSONSuite) TestBinaryJSONModify(c *C) {
	c.Parallel()
	var tests = []struct {
		base     string
		setField string
		setValue string
		expected string
		success  bool
		mt       ModifyType
	}{
		{`null`, "$", `{}`, `{}`, true, ModifySet},
		{`{}`, "$.a", `3`, `{"a": 3}`, true, ModifySet},
		{`{"a": 3}`, "$.a", `[]`, `{"a": []}`, true, ModifyReplace},
		{`{"a": 3}`, "$.b", `"3"`, `{"a": 3, "b": "3"}`, true, ModifySet},
		{`{"a": []}`, "$.a[0]", `3`, `{"a": [3]}`, true, ModifySet},
		{`{"a": [3]}`, "$.a[1]", `4`, `{"a": [3, 4]}`, true, ModifyInsert},
		{`{"a": [3]}`, "$[0]", `4`, `4`, true, ModifySet},
		{`{"a": [3]}`, "$[1]", `4`, `[{"a": [3]}, 4]`, true, ModifySet},
		{`{"b": true}`, "$.b", `false`, `{"b": false}`, true, ModifySet},

		// nothing changed because the path is empty and we want to insert.
		{`{}`, "$", `1`, `{}`, true, ModifyInsert},
		// nothing changed because the path without last leg doesn't exist.
		{`{"a": [3, 4]}`, "$.b[1]", `3`, `{"a": [3, 4]}`, true, ModifySet},
		// nothing changed because the path without last leg doesn't exist.
		{`{"a": [3, 4]}`, "$.a[2].b", `3`, `{"a": [3, 4]}`, true, ModifySet},
		// nothing changed because we want to insert but the full path exists.
		{`{"a": [3, 4]}`, "$.a[0]", `30`, `{"a": [3, 4]}`, true, ModifyInsert},
		// nothing changed because we want to replace but the full path doesn't exist.
		{`{"a": [3, 4]}`, "$.a[2]", `30`, `{"a": [3, 4]}`, true, ModifyReplace},

		// bad path expression.
		{"null", "$.*", "{}", "null", false, ModifySet},
		{"null", "$[*]", "{}", "null", false, ModifySet},
		{"null", "$**.a", "{}", "null", false, ModifySet},
		{"null", "$**[3]", "{}", "null", false, ModifySet},
	}
	for _, tt := range tests {
		pathExpr, err := ParseJSONPathExpr(tt.setField)
		c.Assert(err, IsNil)

		base := mustParseBinaryFromString(c, tt.base)
		value := mustParseBinaryFromString(c, tt.setValue)
		expected := mustParseBinaryFromString(c, tt.expected)
		obtain, err := base.Modify([]PathExpression{pathExpr}, []BinaryJSON{value}, tt.mt)
		if tt.success {
			c.Assert(err, IsNil)
			c.Assert(obtain.String(), Equals, expected.String())
		} else {
			c.Assert(err, NotNil)
		}
	}
}

func (s *testJSONSuite) TestBinaryJSONRemove(c *C) {
	c.Parallel()
	var tests = []struct {
		base     string
		path     string
		expected string
		success  bool
	}{
		{`null`, "$", `{}`, false},
		{`{"a":[3]}`, "$.a[*]", `{"a":[3]}`, false},
		{`{}`, "$.a", `{}`, true},
		{`{"a":3}`, "$.a", `{}`, true},
		{`{"a":1,"b":2,"c":3}`, "$.b", `{"a":1,"c":3}`, true},
		{`{"a":1,"b":2,"c":3}`, "$.d", `{"a":1,"b":2,"c":3}`, true},
		{`{"a":3}`, "$[0]", `{"a":3}`, true},
		{`{"a":[3,4,5]}`, "$.a[0]", `{"a":[4,5]}`, true},
		{`{"a":[3,4,5]}`, "$.a[1]", `{"a":[3,5]}`, true},
		{`{"a":[3,4,5]}`, "$.a[4]", `{"a":[3,4,5]}`, true},
		{`{"a": [1, 2, {"aa": "xx"}]}`, "$.a[2].aa", `{"a": [1, 2, {}]}`, true},
	}
	for _, tt := range tests {
		pathExpr, err := ParseJSONPathExpr(tt.path)
		c.Assert(err, IsNil)

		base := mustParseBinaryFromString(c, tt.base)
		expected := mustParseBinaryFromString(c, tt.expected)
		obtain, err := base.Remove([]PathExpression{pathExpr})
		if tt.success {
			c.Assert(err, IsNil)
			c.Assert(obtain.String(), Equals, expected.String())
		} else {
			c.Assert(err, NotNil)
		}
	}
}

func (s *testJSONSuite) TestCompareBinary(c *C) {
	c.Parallel()
	jNull := mustParseBinaryFromString(c, `null`)
	jBoolTrue := mustParseBinaryFromString(c, `true`)
	jBoolFalse := mustParseBinaryFromString(c, `false`)
	jIntegerLarge := CreateBinary(uint64(1 << 63))
	jIntegerSmall := mustParseBinaryFromString(c, `3`)
	jStringLarge := mustParseBinaryFromString(c, `"hello, world"`)
	jStringSmall := mustParseBinaryFromString(c, `"hello"`)
	jArrayLarge := mustParseBinaryFromString(c, `["a", "c"]`)
	jArraySmall := mustParseBinaryFromString(c, `["a", "b"]`)
	jObject := mustParseBinaryFromString(c, `{"a": "b"}`)

	var tests = []struct {
		left   BinaryJSON
		right  BinaryJSON
		result int
	}{
		{jNull, jIntegerSmall, -1},
		{jIntegerSmall, jIntegerLarge, -1},
		{jIntegerLarge, jStringSmall, -1},
		{jStringSmall, jStringLarge, -1},
		{jStringLarge, jObject, -1},
		{jObject, jArraySmall, -1},
		{jArraySmall, jArrayLarge, -1},
		{jArrayLarge, jBoolFalse, -1},
		{jBoolFalse, jBoolTrue, -1},
		{CreateBinary(int64(922337203685477580)), CreateBinary(int64(922337203685477580)), 0},
		{CreateBinary(int64(922337203685477580)), CreateBinary(int64(922337203685477581)), -1},
		{CreateBinary(int64(922337203685477581)), CreateBinary(int64(922337203685477580)), 1},

		{CreateBinary(int64(-1)), CreateBinary(uint64(18446744073709551615)), -1},
		{CreateBinary(int64(922337203685477580)), CreateBinary(uint64(922337203685477581)), -1},
		{CreateBinary(int64(2)), CreateBinary(uint64(1)), 1},
		{CreateBinary(int64(math.MaxInt64)), CreateBinary(uint64(math.MaxInt64)), 0},

		{CreateBinary(uint64(18446744073709551615)), CreateBinary(int64(-1)), 1},
		{CreateBinary(uint64(922337203685477581)), CreateBinary(int64(922337203685477580)), 1},
		{CreateBinary(uint64(1)), CreateBinary(int64(2)), -1},
		{CreateBinary(uint64(math.MaxInt64)), CreateBinary(int64(math.MaxInt64)), 0},

		{CreateBinary(float64(9.0)), CreateBinary(int64(9)), 0},
		{CreateBinary(float64(8.9)), CreateBinary(int64(9)), -1},
		{CreateBinary(float64(9.1)), CreateBinary(int64(9)), 1},

		{CreateBinary(float64(9.0)), CreateBinary(uint64(9)), 0},
		{CreateBinary(float64(8.9)), CreateBinary(uint64(9)), -1},
		{CreateBinary(float64(9.1)), CreateBinary(uint64(9)), 1},

		{CreateBinary(int64(9)), CreateBinary(float64(9.0)), 0},
		{CreateBinary(int64(9)), CreateBinary(float64(8.9)), 1},
		{CreateBinary(int64(9)), CreateBinary(float64(9.1)), -1},

		{CreateBinary(uint64(9)), CreateBinary(float64(9.0)), 0},
		{CreateBinary(uint64(9)), CreateBinary(float64(8.9)), 1},
		{CreateBinary(uint64(9)), CreateBinary(float64(9.1)), -1},
	}
	for _, tt := range tests {
		cmp := CompareBinary(tt.left, tt.right)
		c.Assert(cmp == tt.result, IsTrue, Commentf("left: %v, right: %v, expect: %v, got: %v", tt.left, tt.right, tt.result, cmp))
	}
}

func (s *testJSONSuite) TestBinaryJSONMerge(c *C) {
	c.Parallel()
	var tests = []struct {
		suffixes []string
		expected string
	}{
		{[]string{`{"a": 1}`, `{"b": 2}`}, `{"a": 1, "b": 2}`},
		{[]string{`{"a": 1}`, `{"a": 2}`}, `{"a": [1, 2]}`},
		{[]string{`[1]`, `[2]`}, `[1, 2]`},
		{[]string{`{"a": 1}`, `[1]`}, `[{"a": 1}, 1]`},
		{[]string{`[1]`, `{"a": 1}`}, `[1, {"a": 1}]`},
		{[]string{`{"a": 1}`, `4`}, `[{"a": 1}, 4]`},
		{[]string{`[1]`, `4`}, `[1, 4]`},
		{[]string{`4`, `{"a": 1}`}, `[4, {"a": 1}]`},
		{[]string{`4`, `1`}, `[4, 1]`},
		{[]string{`{}`, `[]`}, `[{}]`},
	}

	for _, tt := range tests {
		suffixes := make([]BinaryJSON, 0, len(tt.suffixes)+1)
		for _, s := range tt.suffixes {
			suffixes = append(suffixes, mustParseBinaryFromString(c, s))
		}
		result := MergeBinary(suffixes)
		cmp := CompareBinary(result, mustParseBinaryFromString(c, tt.expected))
		c.Assert(cmp, Equals, 0)
	}
}

func mustParseBinaryFromString(c *C, s string) BinaryJSON {
	bj, err := ParseBinaryFromString(s)
	c.Assert(err, IsNil)
	return bj
}

const benchStr = `{"a":[1,"2",{"aa":"bb"},4,null],"b":true,"c":null}`

func BenchmarkBinaryMarshal(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(int64(len(benchStr)))
	bj, _ := ParseBinaryFromString(benchStr)
	for i := 0; i < b.N; i++ {
		bj.MarshalJSON()
	}
}

func (s *testJSONSuite) TestBinaryJSONContains(c *C) {
	c.Parallel()
	var tests = []struct {
		input    string
		target   string
		expected bool
	}{
		{`{}`, `{}`, true},
		{`{"a":1}`, `{}`, true},
		{`{"a":1}`, `1`, false},
		{`{"a":[1]}`, `[1]`, false},
		{`{"b":2, "c":3}`, `{"c":3}`, true},
		{`1`, `1`, true},
		{`[1]`, `1`, true},
		{`[1,2]`, `[1]`, true},
		{`[1,2]`, `[1,3]`, false},
		{`[1,2]`, `["1"]`, false},
		{`[1,2,[1,3]]`, `[1,3]`, true},
		{`[1,2,[1,[5,[3]]]]`, `[1,3]`, true},
		{`[1,2,[1,[5,{"a":[2,3]}]]]`, `[1,{"a":[3]}]`, true},
		{`[{"a":1}]`, `{"a":1}`, true},
		{`[{"a":1,"b":2}]`, `{"a":1}`, true},
		{`[{"a":{"a":1},"b":2}]`, `{"a":1}`, false},
	}

	for _, tt := range tests {
		obj := mustParseBinaryFromString(c, tt.input)
		target := mustParseBinaryFromString(c, tt.target)
		c.Assert(ContainsBinary(obj, target), Equals, tt.expected)
	}
}

func (s *testJSONSuite) TestBinaryJSONCopy(c *C) {
	c.Parallel()
	strs := []string{
		`{"a": [1, "2", {"aa": "bb"}, 4, null], "b": true, "c": null}`,
		`{"aaaaaaaaaaa": [1, "2", {"aa": "bb"}, 4.1], "bbbbbbbbbb": true, "ccccccccc": "d"}`,
		`[{"a": 1, "b": true}, 3, 3.5, "hello, world", null, true]`,
	}
	for _, str := range strs {
		parsedBJ := mustParseBinaryFromString(c, str)
		c.Assert(parsedBJ.Copy().String(), Equals, parsedBJ.String())
	}
}
func (s *testJSONSuite) TestGetKeys(c *C) {
	c.Parallel()
	parsedBJ := mustParseBinaryFromString(c, "[]")
	c.Assert(parsedBJ.GetKeys().String(), Equals, "[]")
	parsedBJ = mustParseBinaryFromString(c, "{}")
	c.Assert(parsedBJ.GetKeys().String(), Equals, "[]")
}

func (s *testJSONSuite) TestBinaryJSONDepth(c *C) {
	c.Parallel()
	var tests = []struct {
		input    string
		expected int
	}{
		{`{}`, 1},
		{`[]`, 1},
		{`true`, 1},
		{`[10, 20]`, 2},
		{`[[], {}]`, 2},
		{`[10, {"a": 20}]`, 3},
		{`{"Person": {"Name": "Homer", "Age": 39, "Hobbies": ["Eating", "Sleeping"]} }`, 4},
	}

	for _, tt := range tests {
		obj := mustParseBinaryFromString(c, tt.input)
		c.Assert(obj.GetElemDepth(), Equals, tt.expected)
	}
}

func (s *testJSONSuite) TestParseBinaryFromString(c *C) {
	c.Parallel()
	obj, err := ParseBinaryFromString("")
	c.Assert(obj.String(), Equals, "")
	c.Assert(err, ErrorMatches, "*The document is empty*")
	obj, err = ParseBinaryFromString(`"a""`)
	c.Assert(obj.String(), Equals, "")
	c.Assert(err, ErrorMatches, "*The document root must not be followed by other values.*")
}

func (s *testJSONSuite) TestCreateBinary(c *C) {
	c.Parallel()
	bj := CreateBinary(int64(1 << 62))
	c.Assert(bj.TypeCode, Equals, TypeCodeInt64)
	c.Assert(bj.Value, NotNil)
	bj = CreateBinary(123456789.1234567)
	c.Assert(bj.TypeCode, Equals, TypeCodeFloat64)
	bj = CreateBinary(0.00000001)
	c.Assert(bj.TypeCode, Equals, TypeCodeFloat64)
	bj = CreateBinary(1e-20)
	c.Assert(bj.TypeCode, Equals, TypeCodeFloat64)
	c.Assert(bj.Value, NotNil)
	bj2 := CreateBinary(bj)
	c.Assert(bj2.TypeCode, Equals, bj.TypeCode)
	c.Assert(bj2.Value, NotNil)
	func() {
		defer func() {
			r := recover()
			c.Assert(r, ErrorMatches, "unknown type:.*")
		}()
		bj = CreateBinary(int8(123))
		c.Assert(bj.TypeCode, Equals, bj.TypeCode)
	}()

}

func (s *testJSONSuite) TestFunctions(c *C) {
	c.Parallel()
	testByte := []byte{'\\', 'b', 'f', 'n', 'r', 't', 'u', 'z', '0'}
	testOutput, err := unquoteString(string(testByte))
	c.Assert(testOutput, Equals, "\bfnrtuz0")
	c.Assert(err, IsNil)
	n, err := PeekBytesAsJSON(testByte)
	c.Assert(n, Equals, 0)
	c.Assert(err, ErrorMatches, "Invalid JSON bytes")
	n, err = PeekBytesAsJSON([]byte(""))
	c.Assert(n, Equals, 0)
	c.Assert(err, ErrorMatches, "Cant peek from empty bytes")
}

func (s *testJSONSuite) TestBinaryJSONExtractCallback(c *C) {
	bj1 := mustParseBinaryFromString(c, `{"\"hello\"": "world", "a": [1, "2", {"aa": "bb"}, 4.0, {"aa": "cc"}], "b": true, "c": ["d"]}`)
	bj2 := mustParseBinaryFromString(c, `[{"a": 1, "b": true}, 3, 3.5, "hello, world", null, true]`)

	type ExpectedPair struct {
		path string
		bj   BinaryJSON
	}
	var tests = []struct {
		bj       BinaryJSON
		pathExpr string
		expected []ExpectedPair
	}{
		{bj1, "$.a", []ExpectedPair{
			{"$.a", mustParseBinaryFromString(c, `[1, "2", {"aa": "bb"}, 4.0, {"aa": "cc"}]`)},
		}},
		{bj2, "$.a", []ExpectedPair{}},
		{bj1, "$[0]", []ExpectedPair{}}, // in extractToCallback/Walk/Search, DON'T autowraped bj as an array.
		{bj2, "$[0]", []ExpectedPair{
			{"$[0]", mustParseBinaryFromString(c, `{"a": 1, "b": true}`)},
		}},
		{bj1, "$.a[2].aa", []ExpectedPair{
			{"$.a[2].aa", mustParseBinaryFromString(c, `"bb"`)},
		}},
		{bj1, "$.a[*].aa", []ExpectedPair{
			{"$.a[2].aa", mustParseBinaryFromString(c, `"bb"`)},
			{"$.a[4].aa", mustParseBinaryFromString(c, `"cc"`)},
		}},
		{bj1, "$.*[0]", []ExpectedPair{
			// {"$.\"hello\"[0]", mustParseBinaryFromString(c, `"world"`)},  // NO autowraped as an array.
			{"$.a[0]", mustParseBinaryFromString(c, `1`)},
			// {"$.b[0]", mustParseBinaryFromString(c, `true`)},  // NO autowraped as an array.
			{"$.c[0]", mustParseBinaryFromString(c, `"d"`)},
		}},
		{bj1, `$.a[*]."aa"`, []ExpectedPair{
			{"$.a[2].aa", mustParseBinaryFromString(c, `"bb"`)},
			{"$.a[4].aa", mustParseBinaryFromString(c, `"cc"`)},
		}},
		{bj1, `$."\"hello\""`, []ExpectedPair{
			{`$."\"hello\""`, mustParseBinaryFromString(c, `"world"`)},
		}},
		{bj1, `$**[1]`, []ExpectedPair{
			{`$.a[1]`, mustParseBinaryFromString(c, `"2"`)},
		}},
	}

	for _, tt := range tests {
		pe, err := ParseJSONPathExpr(tt.pathExpr)
		c.Assert(err, IsNil)

		count := 0
		cb := func(fullpath PathExpression, bj BinaryJSON) (stop bool, err error) {
			c.Assert(count, Less, len(tt.expected))
			if count < len(tt.expected) {
				c.Assert(fullpath.String(), Equals, tt.expected[count].path)
				c.Assert(bj.String(), Equals, tt.expected[count].bj.String())
			}
			count++
			return false, nil
		}
		fullpath := PathExpression{legs: make([]pathLeg, 0), flags: pathExpressionFlag(0)}
		_, err = tt.bj.extractToCallback(pe, cb, fullpath)
		c.Assert(err, IsNil)
		c.Assert(count, Equals, len(tt.expected))
	}
}

func (s *testJSONSuite) TestBinaryJSONWalk(c *C) {
	bj1 := mustParseBinaryFromString(c, `["abc", [{"k": "10"}, "def"], {"x":"abc"}, {"y":"bcd"}]`)
	bj2 := mustParseBinaryFromString(c, `{}`)

	type ExpectedPair struct {
		path string
		bj   BinaryJSON
	}
	var tests = []struct {
		bj       BinaryJSON
		paths    []string
		expected []ExpectedPair
	}{
		{bj1, []string{}, []ExpectedPair{
			{`$`, mustParseBinaryFromString(c, `["abc", [{"k": "10"}, "def"], {"x":"abc"}, {"y":"bcd"}]`)},
			{`$[0]`, mustParseBinaryFromString(c, `"abc"`)},
			{`$[1]`, mustParseBinaryFromString(c, `[{"k": "10"}, "def"]`)},
			{`$[1][0]`, mustParseBinaryFromString(c, `{"k": "10"}`)},
			{`$[1][0].k`, mustParseBinaryFromString(c, `"10"`)},
			{`$[1][1]`, mustParseBinaryFromString(c, `"def"`)},
			{`$[2]`, mustParseBinaryFromString(c, `{"x":"abc"}`)},
			{`$[2].x`, mustParseBinaryFromString(c, `"abc"`)},
			{`$[3]`, mustParseBinaryFromString(c, `{"y":"bcd"}`)},
			{`$[3].y`, mustParseBinaryFromString(c, `"bcd"`)},
		}},
		{bj1, []string{`$[1]`}, []ExpectedPair{
			{`$[1]`, mustParseBinaryFromString(c, `[{"k": "10"}, "def"]`)},
			{`$[1][0]`, mustParseBinaryFromString(c, `{"k": "10"}`)},
			{`$[1][0].k`, mustParseBinaryFromString(c, `"10"`)},
			{`$[1][1]`, mustParseBinaryFromString(c, `"def"`)},
		}},
		{bj1, []string{`$[1]`, `$[1]`}, []ExpectedPair{ // test for unique
			{`$[1]`, mustParseBinaryFromString(c, `[{"k": "10"}, "def"]`)},
			{`$[1][0]`, mustParseBinaryFromString(c, `{"k": "10"}`)},
			{`$[1][0].k`, mustParseBinaryFromString(c, `"10"`)},
			{`$[1][1]`, mustParseBinaryFromString(c, `"def"`)},
		}},
		{bj1, []string{`$.m`}, []ExpectedPair{}},
		{bj2, []string{}, []ExpectedPair{
			{`$`, mustParseBinaryFromString(c, `{}`)},
		}},
	}

	for _, tt := range tests {
		count := 0
		cb := func(fullpath PathExpression, bj BinaryJSON) (stop bool, err error) {
			c.Assert(count, Less, len(tt.expected))
			if count < len(tt.expected) {
				c.Assert(fullpath.String(), Equals, tt.expected[count].path)
				c.Assert(bj.String(), Equals, tt.expected[count].bj.String())
			}
			count++
			return false, nil
		}

		var err error
		if len(tt.paths) > 0 {
			peList := make([]PathExpression, 0, len(tt.paths))
			for _, path := range tt.paths {
				pe, errPath := ParseJSONPathExpr(path)
				c.Assert(errPath, IsNil)
				peList = append(peList, pe)
			}
			err = tt.bj.Walk(cb, peList...)
		} else {
			err = tt.bj.Walk(cb)
		}
		c.Assert(err, IsNil)
		c.Assert(count, Equals, len(tt.expected))
	}
}
