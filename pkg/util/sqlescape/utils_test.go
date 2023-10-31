// Copyright 2021 PingCAP, Inc.
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

package sqlescape

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReserveBuffer(t *testing.T) {
	res0 := reserveBuffer(nil, 0)
	require.Len(t, res0, 0)

	res1 := reserveBuffer(res0, 3)
	require.Len(t, res1, 3)
	res1[1] = 3

	res2 := reserveBuffer(res1, 9)
	require.Len(t, res2, 12)
	require.Equal(t, 15, cap(res2))
	require.Equal(t, res1, res2[:3])
}

func TestEscapeBackslash(t *testing.T) {
	type TestCase struct {
		name   string
		input  []byte
		output []byte
	}
	tests := []TestCase{
		{
			name:   "normal",
			input:  []byte("hello"),
			output: []byte("hello"),
		},
		{
			name:   "0",
			input:  []byte("he\x00lo"),
			output: []byte("he\\0lo"),
		},
		{
			name:   "break line",
			input:  []byte("he\nlo"),
			output: []byte("he\\nlo"),
		},
		{
			name:   "carry",
			input:  []byte("he\rlo"),
			output: []byte("he\\rlo"),
		},
		{
			name:   "substitute",
			input:  []byte("he\x1alo"),
			output: []byte("he\\Zlo"),
		},
		{
			name:   "single quote",
			input:  []byte("he'lo"),
			output: []byte("he\\'lo"),
		},
		{
			name:   "double quote",
			input:  []byte("he\"lo"),
			output: []byte("he\\\"lo"),
		},
		{
			name:   "back slash",
			input:  []byte("he\\lo"),
			output: []byte("he\\\\lo"),
		},
		{
			name:   "double escape",
			input:  []byte("he\x00lo\""),
			output: []byte("he\\0lo\\\""),
		},
		{
			name:   "chinese",
			input:  []byte("中文?"),
			output: []byte("中文?"),
		},
	}
	for _, v := range tests {
		// copy iterator variable into a new variable, see issue #27779
		v := v
		t.Run(v.name, func(t *testing.T) {
			require.Equal(t, v.output, escapeBytesBackslash(nil, v.input))
			require.Equal(t, v.output, escapeStringBackslash(nil, string(v.input)))
		})
	}
}

type myInt int
type myStr string

func TestEscapeSQL(t *testing.T) {
	type TestCase struct {
		name   string
		input  string
		output string
		err    string
		params []interface{}
	}
	time2, err := time.Parse(time.DateTime, "2018-01-23 04:03:05")
	require.NoError(t, err)
	tests := []TestCase{
		{
			name:   "normal 1",
			input:  "select * from 1",
			params: []interface{}{},
			output: "select * from 1",
			err:    "",
		},
		{
			name:   "normal 2",
			input:  "WHERE source != 'builtin'",
			params: []interface{}{},
			output: "WHERE source != 'builtin'",
			err:    "",
		},
		{
			name:   "discard extra arguments",
			input:  "select * from 1",
			params: []interface{}{4, 5, "rt"},
			output: "select * from 1",
			err:    "",
		},
		{
			name:   "%? missing arguments",
			input:  "select %? from %?",
			params: []interface{}{4},
			err:    "^missing arguments",
		},
		{
			name:   "nil",
			input:  "select %?",
			params: []interface{}{nil},
			output: "select NULL",
			err:    "",
		},
		{
			name:   "int",
			input:  "select %?",
			params: []interface{}{int(3)},
			output: "select 3",
			err:    "",
		},
		{
			name:   "int8",
			input:  "select %?",
			params: []interface{}{int8(4)},
			output: "select 4",
			err:    "",
		},
		{
			name:   "int16",
			input:  "select %?",
			params: []interface{}{int16(5)},
			output: "select 5",
			err:    "",
		},
		{
			name:   "int32",
			input:  "select %?",
			params: []interface{}{int32(6)},
			output: "select 6",
			err:    "",
		},
		{
			name:   "int64",
			input:  "select %?",
			params: []interface{}{int64(7)},
			output: "select 7",
			err:    "",
		},
		{
			name:   "uint",
			input:  "select %?",
			params: []interface{}{uint(8)},
			output: "select 8",
			err:    "",
		},
		{
			name:   "uint8",
			input:  "select %?",
			params: []interface{}{uint8(9)},
			output: "select 9",
			err:    "",
		},
		{
			name:   "uint16",
			input:  "select %?",
			params: []interface{}{uint16(10)},
			output: "select 10",
			err:    "",
		},
		{
			name:   "uint32",
			input:  "select %?",
			params: []interface{}{uint32(11)},
			output: "select 11",
			err:    "",
		},
		{
			name:   "uint64",
			input:  "select %?",
			params: []interface{}{uint64(12)},
			output: "select 12",
			err:    "",
		},
		{
			name:   "float32",
			input:  "select %?",
			params: []interface{}{float32(0.13)},
			output: "select 0.13",
			err:    "",
		},
		{
			name:   "float64",
			input:  "select %?",
			params: []interface{}{float64(0.14)},
			output: "select 0.14",
			err:    "",
		},
		{
			name:   "bool on",
			input:  "select %?",
			params: []interface{}{true},
			output: "select 1",
			err:    "",
		},
		{
			name:   "bool off",
			input:  "select %?",
			params: []interface{}{false},
			output: "select 0",
			err:    "",
		},
		{
			name:   "time 0",
			input:  "select %?",
			params: []interface{}{time.Time{}},
			output: "select '0000-00-00'",
			err:    "",
		},
		{
			name:   "time 1",
			input:  "select %?",
			params: []interface{}{time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC)},
			output: "select '2019-01-01 00:00:00'",
			err:    "",
		},
		{
			name:   "time 2",
			input:  "select %?",
			params: []interface{}{time2},
			output: "select '2018-01-23 04:03:05'",
			err:    "",
		},
		{
			name:   "time 3",
			input:  "select %?",
			params: []interface{}{time.Unix(0, 888888888).UTC()},
			output: "select '1970-01-01 00:00:00.888888'",
			err:    "",
		},
		{
			name:   "empty byte slice1",
			input:  "select %?",
			params: []interface{}{[]byte(nil)},
			output: "select NULL",
			err:    "",
		},
		{
			name:   "empty byte slice2",
			input:  "select %?",
			params: []interface{}{[]byte{}},
			output: "select _binary''",
			err:    "",
		},
		{
			name:   "byte slice",
			input:  "select %?",
			params: []interface{}{[]byte{2, 3}},
			output: "select _binary'\x02\x03'",
			err:    "",
		},
		{
			name:   "string",
			input:  "select %?",
			params: []interface{}{"33"},
			output: "select '33'",
		},
		{
			name:   "string slice",
			input:  "select %?",
			params: []interface{}{[]string{"33", "44"}},
			output: "select '33','44'",
		},
		{
			name:   "raw json",
			input:  "select %?",
			params: []interface{}{json.RawMessage(`{"h": "hello"}`)},
			output: "select '{\\\"h\\\": \\\"hello\\\"}'",
		},
		{
			name:   "unsupported args",
			input:  "select %?",
			params: []interface{}{make(chan byte)},
			err:    "unsupported 1-th argument.*",
		},
		{
			name:   "mixed arguments",
			input:  "select %?, %?, %?",
			params: []interface{}{"33", 44, time.Time{}},
			output: "select '33', 44, '0000-00-00'",
		},
		{
			name:   "simple injection",
			input:  "select %?",
			params: []interface{}{"0; drop database"},
			output: "select '0; drop database'",
		},
		{
			name:   "identifier, wrong arg",
			input:  "use %n",
			params: []interface{}{3},
			err:    "^expect a string identifier",
		},
		{
			name:   "identifier",
			input:  "use %n",
			params: []interface{}{"table`"},
			output: "use `table```",
			err:    "",
		},
		{
			name:   "%n missing arguments",
			input:  "use %n",
			params: []interface{}{},
			err:    "missing arguments.*",
		},
		{
			name:   "% escape",
			input:  "select * from t where val = '%%?'",
			params: []interface{}{},
			output: "select * from t where val = '%?'",
			err:    "",
		},
		{
			name:   "unknown specifier",
			input:  "%v",
			params: []interface{}{},
			output: "%v",
			err:    "",
		},
		{
			name:   "truncated specifier ",
			input:  "rv %",
			params: []interface{}{},
			output: "rv %",
			err:    "",
		},
		{
			name:   "float32 slice",
			input:  "select %?",
			params: []interface{}{[]float32{33.1, 0.44}},
			output: "select 33.1,0.44",
		},
		{
			name:   "float64 slice",
			input:  "select %?",
			params: []interface{}{[]float64{55.2, 0.66}},
			output: "select 55.2,0.66",
		},
		{
			name:   "myInt",
			input:  "select %?",
			params: []interface{}{myInt(3)},
			output: "select 3",
		},
		{
			name:   "myStr",
			input:  "select %?",
			params: []interface{}{myStr("3")},
			output: "select '3'",
		},
	}
	for _, v := range tests {
		// copy iterator variable into a new variable, see issue #27779
		v := v
		t.Run(v.name, func(t *testing.T) {
			r3 := new(strings.Builder)
			r1, e1 := escapeSQL(v.input, v.params...)
			r2, e2 := EscapeSQL(v.input, v.params...)
			e3 := FormatSQL(r3, v.input, v.params...)
			if v.err == "" {
				require.NoError(t, e1)
				require.Equal(t, v.output, string(r1))
				require.NoError(t, e2)
				require.Equal(t, v.output, r2)
				require.NoError(t, e3)
				require.Equal(t, v.output, r3.String())
			} else {
				require.Error(t, e1)
				require.Regexp(t, v.err, e1.Error())
				require.Error(t, e2)
				require.Regexp(t, v.err, e2.Error())
				require.Error(t, e3)
				require.Regexp(t, v.err, e3.Error())
			}
		})
	}
}

func TestMustUtils(t *testing.T) {
	require.PanicsWithError(t, "missing arguments, need 1-th arg, but only got 0 args", func() {
		MustEscapeSQL("%?")
	})

	require.PanicsWithError(t, "missing arguments, need 1-th arg, but only got 0 args", func() {
		sql := new(strings.Builder)
		MustFormatSQL(sql, "%?")
	})

	sql := new(strings.Builder)
	MustFormatSQL(sql, "t")
	MustEscapeSQL("tt")
}

func TestEscapeString(t *testing.T) {
	type testCase struct {
		input  string
		output string
	}
	tests := []testCase{
		{
			input:  "testData",
			output: "testData",
		},
		{
			input:  `it's all good`,
			output: `it\'s all good`,
		},
		{
			input:  `+ -><()~*:""&|`,
			output: `+ -><()~*:\"\"&|`,
		},
	}
	for _, v := range tests {
		require.Equal(t, v.output, EscapeString(v.input))
	}
}

func BenchmarkEscapeString(b *testing.B) {
	for i := 0; i < b.N; i++ {
		escapeSQL("select %?", "3")
	}
}

func BenchmarkUnderlyingString(b *testing.B) {
	for i := 0; i < b.N; i++ {
		escapeSQL("select %?", myStr("3"))
	}
}

func BenchmarkEscapeInt(b *testing.B) {
	for i := 0; i < b.N; i++ {
		escapeSQL("select %?", 3)
	}
}

func BenchmarkUnderlyingInt(b *testing.B) {
	for i := 0; i < b.N; i++ {
		escapeSQL("select %?", myInt(3))
	}
}
