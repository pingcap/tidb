// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package types

import (
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

func TestDecodeEscapedUnicode(t *testing.T) {
	testCases := []struct {
		input            string
		expectedResult   string
		size             int
		inSurrogateRange bool
		expectedValid    bool
	}{
		{"597d", "好\x00", 3, false, true},
		{"fffd", "�\x00", 3, false, true},
		{"D83DDE0A", "😊", 4, false, true},
		{"D83D", "", 0, true, false},
		{"D83D11", "", 0, false, false},
		{"ZZZZ", "", 0, false, false},
		{"D83DDE0A597d", "", 0, false, false},
	}

	for _, tc := range testCases {
		result, size, inSurrogateRange, err := decodeOneEscapedUnicode([]byte(tc.input))
		require.Equal(t, tc.inSurrogateRange, inSurrogateRange)
		if tc.expectedValid {
			require.NoError(t, err)
			require.Equal(t, tc.expectedResult, string(result[:]))
			require.Equal(t, tc.size, size)
		} else {
			require.Error(t, err)
		}
	}
}

func TestUnquoteJSONString(t *testing.T) {
	var testCases = []struct {
		input          string
		expectedResult string
		expectedValid  bool
	}{
		{"\\b", "\b", true},
		{"\\f", "\f", true},
		{"\\n", "\n", true},
		{"\\r", "\r", true},
		{"\\t", "\t", true},
		{"\\\\", "\\", true},
		{"\\u597d", "好", true},
		{"0\\u597d0", "0好0", true},
		{"\\a", "a", true},
		{"[", "[", true},
		{"\\ud83e\\udd21", "🤡", true},
		{"\\ufffd", "�", true},
		// invalid input
		{"\\", "", false},
		{"\\u59", "", false},
	}

	for _, tc := range testCases {
		result, err := unquoteJSONString(tc.input)
		if tc.expectedValid {
			require.NoError(t, err)
			require.Equal(t, tc.expectedResult, result)
		} else {
			require.Error(t, err)
		}
	}
}

func BenchmarkDecodeEscapedUnicode(b *testing.B) {
	for i := 0; i < b.N; i++ {
		in := "597d"
		_, _, _, _ = decodeOneEscapedUnicode([]byte(in))
	}
}

func BenchmarkMergePatchBinary(b *testing.B) {
	valueA, _ := ParseBinaryJSONFromString(`{"title":"Goodbye!","author":{"givenName":"John","familyName":"Doe"},"tags":["example","sample"],"content":"This will be unchanged"}`)
	valueB, _ := ParseBinaryJSONFromString(`{"title":"Hello!","phoneNumber":"+01-123-456-7890","author":{"familyName":null},"tags":["example"]}`)
	for i := 0; i < b.N; i++ {
		_, _ = MergePatchBinaryJSON([]*BinaryJSON{&valueA, &valueB})
	}
}

func BenchmarkMergeBinary(b *testing.B) {
	valueA, _ := ParseBinaryJSONFromString(`{"title":"Goodbye!","author":{"givenName":"John","familyName":"Doe"},"tags":["example","sample"],"content":"This will be unchanged"}`)
	valueB, _ := ParseBinaryJSONFromString(`{"title":"Hello!","phoneNumber":"+01-123-456-7890","author":{"familyName":null},"tags":["example"]}`)
	for i := 0; i < b.N; i++ {
		_ = MergeBinaryJSON([]BinaryJSON{valueA, valueB})
	}
}

func TestBinaryCompare(t *testing.T) {
	tests := []struct {
		left   BinaryJSON
		right  BinaryJSON
		result int
	}{
		{
			CreateBinaryJSON("a"),
			CreateBinaryJSON("b"),
			-1,
		},
		{
			CreateBinaryJSON(Opaque{
				TypeCode: 0,
				Buf:      []byte{0, 1, 2, 3},
			}),
			CreateBinaryJSON(Opaque{
				TypeCode: 0,
				Buf:      []byte{0, 1, 2},
			}),
			1,
		},
		{
			CreateBinaryJSON(Opaque{
				TypeCode: 0,
				Buf:      []byte{0, 1, 2, 3},
			}),
			CreateBinaryJSON(Opaque{
				TypeCode: 0,
				Buf:      []byte{0, 2, 1},
			}),
			-1,
		},
		{
			CreateBinaryJSON("test"),
			CreateBinaryJSON(Opaque{
				TypeCode: 0,
				Buf:      []byte{0, 2, 1},
			}),
			-1,
		},
	}

	compareMsg := map[int]string{
		1:  "greater than",
		0:  "equal with",
		-1: "smaller than",
	}

	for _, test := range tests {
		require.Equal(t, test.result, CompareBinaryJSON(test.left, test.right), "%s should be %s %s", test.left.String(), compareMsg[test.result], test.right.String())
	}
}

func TestFlattenPathValues(t *testing.T) {
	testCases := []struct {
		input    string
		expected []string
	}{
		{`{"cells": {"ccf_1": {"state": "error", "errorCode": [1, 7]}, "ccf_2": {"state": "results"}}}`,
			[]string{`$.cells.ccf_1.errorCode=1`, `$.cells.ccf_1.errorCode=7`, `$.cells.ccf_1.state="error"`, `$.cells.ccf_2.state="results"`}},
		// The string "7" and the number 7 are different entries; 7.0 shares an entry with 7.
		{`{"a": "7", "b": 7, "c": 7.0, "d": 7.5}`, []string{`$.a="7"`, `$.b=7`, `$.c=7`, `$.d=7.5`}},
		{`{"t": true, "f": false, "n": null}`, []string{`$.f=false`, `$.n=null`, `$.t=true`}},
		// Keys that are not identifiers are quoted, as in JSON path syntax.
		{`{"a.b": 1, "x y": 2, "*": 3, "q\"": 4}`, []string{`$."*"=3`, `$."a.b"=1`, `$."q\""=4`, `$."x y"=2`}},
		// Array positions are dropped, nested arrays are flattened, and duplicates are removed.
		{`{"a": [[1, 2], [2, {"b": 3}]]}`, []string{`$.a=1`, `$.a=2`, `$.a.b=3`}},
		// Empty objects and arrays produce no entries.
		{`{"a": {}, "b": [], "c": 1}`, []string{`$.c=1`}},
		{`{}`, nil},
		{`[]`, nil},
		// A scalar document produces a single root entry.
		{`"x"`, []string{`$="x"`}},
		{`[1, "1"]`, []string{`$=1`, `$="1"`}},
	}
	for _, tc := range testCases {
		bj, err := ParseBinaryJSONFromString(tc.input)
		require.NoError(t, err)
		entries, err := bj.FlattenPathValues(0)
		require.NoError(t, err)
		require.Equal(t, tc.expected, entries, tc.input)
	}
}

// TestFlattenPathValuesContainment checks the property the path-value index relies on:
// if JSON_CONTAINS(doc, candidate), every entry of candidate is also an entry of doc.
func TestFlattenPathValuesContainment(t *testing.T) {
	doc := `{"cells": {"ccf_1": {"state": "error", "errorCode": [1, 7], "tags": [{"k": "a"}, {"k": "b"}]}, "ccf_2": {"state": "results", "v": 7.0}}}`
	candidates := []string{
		`{"cells": {"ccf_1": {"state": "error"}}}`,
		`{"cells": {"ccf_1": {"errorCode": [7]}}}`,
		`{"cells": {"ccf_1": {"errorCode": 7}}}`,
		`{"cells": {"ccf_1": {"tags": [{"k": "b"}]}}}`,
		`{"cells": {"ccf_1": {"tags": {"k": "a"}}}}`,
		`{"cells": {"ccf_2": {"v": 7}}}`,
		`{"cells": {"ccf_1": {"state": "error"}, "ccf_2": {"state": "results"}}}`,
		`{"cells": {"ccf_1": {"state": "results"}}}`,
		`{"cells": {"ccf_3": {"state": "error"}}}`,
		`{"cells": {"ccf_1": {"errorCode": ["7"]}}}`,
	}
	docJSON, err := ParseBinaryJSONFromString(doc)
	require.NoError(t, err)
	docEntries, err := docJSON.FlattenPathValues(0)
	require.NoError(t, err)
	docSet := make(map[string]struct{}, len(docEntries))
	for _, e := range docEntries {
		docSet[e] = struct{}{}
	}
	containedCount := 0
	for _, c := range candidates {
		candJSON, err := ParseBinaryJSONFromString(c)
		require.NoError(t, err)
		if !ContainsBinaryJSON(docJSON, candJSON) {
			continue
		}
		containedCount++
		candEntries, err := candJSON.FlattenPathValues(0)
		require.NoError(t, err)
		for _, e := range candEntries {
			require.Contains(t, docSet, e, "candidate %s", c)
		}
	}
	require.Equal(t, 7, containedCount)
}

func TestFlattenPathValuesMaxLen(t *testing.T) {
	const maxLen = FlattenMinMaxLen
	flatten := func(doc string, maxLen int) []string {
		bj, err := ParseBinaryJSONFromString(doc)
		require.NoError(t, err)
		entries, err := bj.FlattenPathValues(maxLen)
		require.NoError(t, err)
		return entries
	}
	isShortened := func(entry string) bool {
		if utf8.RuneCountInString(entry) != maxLen {
			return false
		}
		hash := entry[len(entry)-flattenHashLen:]
		for _, c := range hash {
			if !strings.ContainsRune("0123456789abcdef", c) {
				return false
			}
		}
		return entry[len(entry)-flattenHashLen-1] == '#'
	}

	// Short entries are unchanged.
	require.Equal(t, []string{`$.a="x"`}, flatten(`{"a": "x"}`, maxLen))

	// `$.a=` plus a string of n characters in quotes is 6+n characters.
	exact := fmt.Sprintf(`{"a": "%s"}`, strings.Repeat("x", maxLen-6))
	require.Equal(t, maxLen, utf8.RuneCountInString(flatten(exact, maxLen)[0]))
	require.False(t, isShortened(flatten(exact, maxLen)[0]))
	require.Equal(t, flatten(exact, 0), flatten(exact, maxLen))

	over := fmt.Sprintf(`{"a": "%s"}`, strings.Repeat("x", maxLen-5))
	entry := flatten(over, maxLen)[0]
	require.True(t, isShortened(entry), entry)
	require.True(t, strings.HasPrefix(entry, `$.a="xxx`))

	// Characters are counted, not bytes, and the prefix is cut on a character boundary.
	wide := fmt.Sprintf(`{"a": "%s"}`, strings.Repeat("中", 100))
	entry = flatten(wide, maxLen)[0]
	require.True(t, utf8.ValidString(entry))
	require.True(t, isShortened(entry), entry)

	// Shortening is deterministic, and values that differ only after the prefix still differ.
	longA := fmt.Sprintf(`{"a": "%sA"}`, strings.Repeat("x", 200))
	longB := fmt.Sprintf(`{"a": "%sB"}`, strings.Repeat("x", 200))
	require.Equal(t, flatten(longA, maxLen), flatten(longA, maxLen))
	require.NotEqual(t, flatten(longA, maxLen), flatten(longB, maxLen))

	// A candidate flattened with the same limit matches the shortened entries of the document.
	doc := fmt.Sprintf(`{"cells": {"c1": {"note": "%s", "state": "error"}}}`, strings.Repeat("n", 300))
	cand := fmt.Sprintf(`{"cells": {"c1": {"note": "%s"}}}`, strings.Repeat("n", 300))
	docSet := make(map[string]struct{})
	for _, e := range flatten(doc, maxLen) {
		require.LessOrEqual(t, utf8.RuneCountInString(e), maxLen)
		docSet[e] = struct{}{}
	}
	for _, e := range flatten(cand, maxLen) {
		require.Contains(t, docSet, e)
	}
}
