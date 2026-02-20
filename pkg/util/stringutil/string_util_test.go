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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package stringutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnquote(t *testing.T) {
	table := []struct {
		str    string
		expect string
		ok     bool
	}{
		{``, ``, false},
		{`'`, ``, false},
		{`'abc"`, ``, false},
		{`abcdea`, ``, false},
		{`'abc'def'`, ``, false},
		{`"abc\"`, ``, false},

		{`"abcdef"`, `abcdef`, true},
		{`"abc'def"`, `abc'def`, true},
		{`"\a汉字测试"`, `a汉字测试`, true},
		{`"☺"`, `☺`, true},
		{`"\xFF"`, `xFF`, true},
		{`"\U00010111"`, `U00010111`, true},
		{`"\U0001011111"`, `U0001011111`, true},
		{`"\a\b\f\n\r\t\v\\\""`, "a\bf\n\r\tv\\\"", true},
		{`"\Z\%\_"`, "\032" + `\%\_`, true},
		{`"abc\0"`, "abc\000", true},
		{`"abc\"abc"`, `abc"abc`, true},

		{`'abcdef'`, `abcdef`, true},
		{`'"'`, "\"", true},
		{`'\a\b\f\n\r\t\v\\\''`, "a\bf\n\r\tv\\'", true},
		{`' '`, ` `, true},
		{"'\\a汉字'", "a汉字", true},
		{"'\\a\x90'", "a\x90", true},
		{"\"\\a\x18èàø»\x05\"", "a\x18èàø»\x05", true},
	}

	for _, v := range table {
		x, err := Unquote(v.str)
		require.Equal(t, v.expect, x)
		if v.ok {
			require.Nilf(t, err, "source %v", v)
		} else {
			require.NotNilf(t, err, "source %v", v)
		}
	}
}

func TestPatternMatch(t *testing.T) {
	tbl := []struct {
		pattern string
		input   string
		escape  byte
		match   bool
	}{
		{``, `a`, '\\', false},
		{`a`, `a`, '\\', true},
		{`a`, `b`, '\\', false},
		{`aA`, `aA`, '\\', true},
		{`_`, `a`, '\\', true},
		{`_`, `ab`, '\\', false},
		{`__`, `b`, '\\', false},
		{`%`, `abcd`, '\\', true},
		{`%`, ``, '\\', true},
		{`%b`, `AAA`, '\\', false},
		{`%a%`, `BBB`, '\\', false},
		{`a%`, `BBB`, '\\', false},
		{`\%a`, `%a`, '\\', true},
		{`\%a`, `aa`, '\\', false},
		{`\_a`, `_a`, '\\', true},
		{`\_a`, `aa`, '\\', false},
		{`\\_a`, `\xa`, '\\', true},
		{`\a\b`, `\a\b`, '\\', false},
		{`\a\b`, `ab`, '\\', true},
		{`%%_`, `abc`, '\\', true},
		{`%_%_aA`, "aaaA", '\\', true},
		{`+_a`, `_a`, '+', true},
		{`+%a`, `%a`, '+', true},
		{`\%a`, `%a`, '+', false},
		{`++a`, `+a`, '+', true},
		{`+a`, `a`, '+', true},
		{`++_a`, `+xa`, '+', true},
		{`___Հ`, `䇇Հ`, '\\', false},
	}
	for _, v := range tbl {
		patChars, patTypes := CompilePattern(v.pattern, v.escape)
		match := DoMatch(v.input, patChars, patTypes)
		require.Equalf(t, v.match, match, "source %v", v)
	}
}

func TestCompileLike2Regexp(t *testing.T) {
	tbl := []struct {
		pattern string
		regexp  string
	}{
		{``, `^$`},
		{`a`, `^a$`},
		{`aA`, `^aA$`},
		{`_`, `^.$`},
		{`__`, `^..$`},
		{`%`, `^.*$`},
		{`%b`, `^.*b$`},
		{`%a%`, `^.*a.*$`},
		{`a%`, `^a.*$`},
		{`\%a`, `^%a$`},
		{`\_a`, `^_a$`},
		{`\\_a`, `^\.a$`},
		{`\a\b`, `^ab$`},
		{`%%_`, `^..*$`},
		{`%_%_aA`, "^...*aA$"},
	}
	for _, v := range tbl {
		result := CompileLike2Regexp(v.pattern)
		require.Equalf(t, v.regexp, result, "source %v", v)
	}
}

func TestIsExactMatch(t *testing.T) {
	tbl := []struct {
		pattern    string
		escape     byte
		exactMatch bool
	}{
		{``, '\\', true},
		{`_`, '\\', false},
		{`%`, '\\', false},
		{`a`, '\\', true},
		{`a_`, '\\', false},
		{`a%`, '\\', false},
		{`a\_`, '\\', true},
		{`a\%`, '\\', true},
		{`a\\`, '\\', true},
		{`a\\_`, '\\', false},
		{`a+%`, '+', true},
		{`a\%`, '+', false},
		{`a++`, '+', true},
		{`a++_`, '+', false},
	}
	for _, v := range tbl {
		_, patTypes := CompilePattern(v.pattern, v.escape)
		require.Equalf(t, v.exactMatch, IsExactMatch(patTypes), "source %v", v)
	}
}

func TestBuildStringFromLabels(t *testing.T) {
	tbl := []struct {
		name     string
		labels   map[string]string
		expected string
	}{
		{
			name:     "nil map",
			labels:   nil,
			expected: "",
		},
		{
			name: "one label",
			labels: map[string]string{
				"aaa": "bbb",
			},
			expected: "aaa=bbb",
		},
		{
			name: "two labels",
			labels: map[string]string{
				"aaa": "bbb",
				"ccc": "ddd",
			},
			expected: "aaa=bbb,ccc=ddd",
		},
	}
	for _, v := range tbl {
		require.Equalf(t, v.expected, BuildStringFromLabels(v.labels), "source %v", v)
	}
}

func TestEscapeGlobQuestionMark(t *testing.T) {
	cases := [][2]string{
		{"123", "123"},
		{"12*3", "12*3"},
		{"12?", `12\?`},
		{`[1-2]`, `[1-2]`},
	}
	for _, pair := range cases {
		require.Equal(t, pair[1], EscapeGlobQuestionMark(pair[0]))
	}
}

func TestMemoizeStr(t *testing.T) {
	cnt := 0
	slowStringFn := func() string {
		cnt++
		return "slow"
	}
	stringer := MemoizeStr(slowStringFn)
	require.Equal(t, "slow", stringer.String())
	require.Equal(t, "slow", stringer.String())
	require.Equal(t, 1, cnt)
}

func BenchmarkDoMatch(b *testing.B) {
	escape := byte('\\')
	tbl := []struct {
		pattern string
		target  string
	}{
		{`a%_%_%_%_b`, `aababab`},
		{`%_%_a%_%_b`, `bbbaaabb`},
		{`a%_%_a%_%_b`, `aaaabbbbbbaaaaaaaaabbbbb`},
	}

	for _, v := range tbl {
		b.Run(v.pattern, func(b *testing.B) {
			patChars, patTypes := CompilePattern(v.pattern, escape)
			b.ResetTimer()
			for range b.N {
				match := DoMatch(v.target, patChars, patTypes)
				if !match {
					b.Fatal("Match expected.")
				}
			}
		})
	}
}

func BenchmarkDoMatchNegative(b *testing.B) {
	escape := byte('\\')
	tbl := []struct {
		pattern string
		target  string
	}{
		{`a%a%a%a%a%a%a%a%b`, `aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa`},
	}

	for _, v := range tbl {
		b.Run(v.pattern, func(b *testing.B) {
			patChars, patTypes := CompilePattern(v.pattern, escape)
			b.ResetTimer()
			for range b.N {
				match := DoMatch(v.target, patChars, patTypes)
				if match {
					b.Fatal("Unmatch expected.")
				}
			}
		})
	}
}

func BenchmarkBuildStringFromLabels(b *testing.B) {
	cases := []struct {
		labels map[string]string
		name   string
	}{
		{
			name: "normal case",
			labels: map[string]string{
				"aaa": "bbb",
				"foo": "bar",
			},
		},
	}

	for _, testcase := range cases {
		b.Run(testcase.name, func(b *testing.B) {
			b.ResetTimer()
			BuildStringFromLabels(testcase.labels)
		})
	}
}

func TestGetCharsByteCount(t *testing.T) {
	tests := []struct {
		name      string
		str       string
		charCount int
		expected  int
	}{
		// Empty string cases
		{
			name:      "empty string, charCount 0",
			str:       "",
			charCount: 0,
			expected:  0,
		},
		{
			name:      "empty string, charCount > 0",
			str:       "",
			charCount: 5,
			expected:  0,
		},
		// ASCII-only strings (1 byte per character)
		{
			name:      "ASCII string, charCount 0",
			str:       "hello",
			charCount: 0,
			expected:  0,
		},
		{
			name:      "ASCII string, charCount 1",
			str:       "hello",
			charCount: 1,
			expected:  1,
		},
		{
			name:      "ASCII string, charCount equals length",
			str:       "hello",
			charCount: 5,
			expected:  5,
		},
		{
			name:      "ASCII string, charCount exceeds length",
			str:       "hello",
			charCount: 10,
			expected:  5,
		},
		{
			name:      "ASCII string, partial count",
			str:       "abcdef",
			charCount: 3,
			expected:  3,
		},
		// UTF-8 strings with 2-byte characters (e.g., Latin-1 Supplement)
		{
			name:      "UTF-8 2-byte chars, charCount 1",
			str:       "café",
			charCount: 1,
			expected:  1, // 'c' is 1 byte
		},
		{
			name:      "UTF-8 2-byte chars, charCount 4",
			str:       "café",
			charCount: 4,
			expected:  5, // 'c'=1, 'a'=1, 'f'=1, 'é'=2
		},
		{
			name:      "UTF-8 2-byte chars, charCount 3",
			str:       "café",
			charCount: 3,
			expected:  3, // 'c'=1, 'a'=1, 'f'=1
		},
		// UTF-8 strings with 3-byte characters (e.g., Chinese, Japanese, Korean)
		{
			name:      "UTF-8 3-byte chars, single char",
			str:       "你",
			charCount: 1,
			expected:  3,
		},
		{
			name:      "UTF-8 3-byte chars, multiple chars",
			str:       "你好",
			charCount: 1,
			expected:  3,
		},
		{
			name:      "UTF-8 3-byte chars, full string",
			str:       "你好",
			charCount: 2,
			expected:  6,
		},
		{
			name:      "UTF-8 3-byte chars, partial",
			str:       "你好世界",
			charCount: 2,
			expected:  6,
		},
		{
			name:      "UTF-8 3-byte chars, charCount exceeds length",
			str:       "你好世界",
			charCount: 10,
			expected:  12,
		},
		// UTF-8 strings with 4-byte characters (e.g., emojis, some rare CJK)
		{
			name:      "UTF-8 4-byte chars, single emoji",
			str:       "😀",
			charCount: 1,
			expected:  4,
		},
		{
			name:      "UTF-8 4-byte chars, multiple emojis",
			str:       "😀😃😄",
			charCount: 2,
			expected:  8,
		},
		{
			name:      "UTF-8 4-byte chars, full string",
			str:       "😀😃",
			charCount: 2,
			expected:  8,
		},
		{
			name:      "UTF-8 4-byte chars, charCount exceeds length",
			str:       "😀😃",
			charCount: 10,
			expected:  8,
		},
		// Mixed ASCII and UTF-8
		{
			name:      "Mixed ASCII and UTF-8",
			str:       "Hello世界",
			charCount: 1,
			expected:  1, // 'H' is 1 byte
		},
		{
			name:      "Mixed ASCII and UTF-8, full ASCII part",
			str:       "Hello世界",
			charCount: 5,
			expected:  5, // "Hello" = 5 bytes
		},
		{
			name:      "Mixed ASCII and UTF-8, include UTF-8",
			str:       "Hello世界",
			charCount: 6,
			expected:  8, // "Hello"=5 + "世"=3
		},
		{
			name:      "Mixed ASCII and UTF-8, full string",
			str:       "Hello世界",
			charCount: 7,
			expected:  11, // "Hello"=5 + "世界"=6
		},
		{
			name:      "Mixed ASCII, 2-byte, 3-byte, 4-byte",
			str:       "Aé你😀",
			charCount: 1,
			expected:  1, // 'A' is 1 byte
		},
		{
			name:      "Mixed ASCII, 2-byte, 3-byte, 4-byte, count 2",
			str:       "Aé你😀",
			charCount: 2,
			expected:  3, // 'A'=1 + 'é'=2
		},
		{
			name:      "Mixed ASCII, 2-byte, 3-byte, 4-byte, count 3",
			str:       "Aé你😀",
			charCount: 3,
			expected:  6, // 'A'=1 + 'é'=2 + '你'=3
		},
		{
			name:      "Mixed ASCII, 2-byte, 3-byte, 4-byte, count 4",
			str:       "Aé你😀",
			charCount: 4,
			expected:  10, // 'A'=1 + 'é'=2 + '你'=3 + '😀'=4
		},
		{
			name:      "Mixed ASCII, 2-byte, 3-byte, 4-byte, exceeds length",
			str:       "Aé你😀",
			charCount: 10,
			expected:  10, // 'A'=1 + 'é'=2 + '你'=3 + '😀'=4
		},
		// Edge cases
		{
			name:      "Single ASCII character",
			str:       "a",
			charCount: 1,
			expected:  1,
		},
		{
			name:      "Single UTF-8 3-byte character",
			str:       "中",
			charCount: 1,
			expected:  3,
		},
		{
			name:      "String with spaces",
			str:       "hello world",
			charCount: 5,
			expected:  5,
		},
		{
			name:      "String with spaces, include space",
			str:       "hello world",
			charCount: 6,
			expected:  6, // "hello " = 6 bytes
		},
		{
			name:      "Complex mixed string",
			str:       "Test123测试😊",
			charCount: 4,
			expected:  4, // "Test" = 4 bytes
		},
		{
			name:      "Complex mixed string, include UTF-8",
			str:       "Test123测试😊",
			charCount: 7,
			expected:  7, // "Test123" = 7 bytes
		},
		{
			name:      "Complex mixed string, include Chinese",
			str:       "Test123测试😊",
			charCount: 8,
			expected:  10, // "Test123"=7 + "测"=3
		},
		{
			name:      "Complex mixed string, include emoji",
			str:       "Test123测试😊",
			charCount: 9,
			expected:  13, // "Test123"=7 + "测试"=6
		},
		{
			name:      "Complex mixed string, full",
			str:       "Test123测试😊",
			charCount: 10,
			expected:  17, // "Test123"=7 + "测试"=6 + "😊"=4
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetCharsByteCount(tt.str, tt.charCount)
			require.Equalf(t, tt.expected, result,
				"GetCharsByteCount(%q, %d) = %d, want %d",
				tt.str, tt.charCount, result, tt.expected)
		})
	}
}
