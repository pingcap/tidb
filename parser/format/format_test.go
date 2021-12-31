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

package format

import (
	"bytes"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func checkFormat(t *testing.T, f Formatter, buf *bytes.Buffer, str, expect string) {
	_, err := f.Format(str, 3)
	require.NoError(t, err)
	b, err := ioutil.ReadAll(buf)
	require.NoError(t, err)
	require.Equal(t, expect, string(b))
}

func TestFormat(t *testing.T) {
	str := "abc%d%%e%i\nx\ny\n%uz\n"
	buf := &bytes.Buffer{}
	f := IndentFormatter(buf, "\t")
	expect := `abc3%e
	x
	y
z
`
	checkFormat(t, f, buf, str, expect)

	str = "abc%d%%e%i\nx\ny\n%uz\n%i\n"
	buf = &bytes.Buffer{}
	f = FlatFormatter(buf)
	expect = "abc3%e x y z\n "
	checkFormat(t, f, buf, str, expect)
}

func TestRestoreCtx(t *testing.T) {
	testCases := []struct {
		flag   RestoreFlags
		expect string
	}{
		{0, "key`.'\"Word\\ str`.'\"ing\\ na`.'\"Me\\"},
		{RestoreStringSingleQuotes, "key`.'\"Word\\ 'str`.''\"ing\\' na`.'\"Me\\"},
		{RestoreStringDoubleQuotes, "key`.'\"Word\\ \"str`.'\"\"ing\\\" na`.'\"Me\\"},
		{RestoreStringEscapeBackslash, "key`.'\"Word\\ str`.'\"ing\\\\ na`.'\"Me\\"},
		{RestoreKeyWordUppercase, "KEY`.'\"WORD\\ str`.'\"ing\\ na`.'\"Me\\"},
		{RestoreKeyWordLowercase, "key`.'\"word\\ str`.'\"ing\\ na`.'\"Me\\"},
		{RestoreNameUppercase, "key`.'\"Word\\ str`.'\"ing\\ NA`.'\"ME\\"},
		{RestoreNameLowercase, "key`.'\"Word\\ str`.'\"ing\\ na`.'\"me\\"},
		{RestoreNameDoubleQuotes, "key`.'\"Word\\ str`.'\"ing\\ \"na`.'\"\"Me\\\""},
		{RestoreNameBackQuotes, "key`.'\"Word\\ str`.'\"ing\\ `na``.'\"Me\\`"},
		{DefaultRestoreFlags, "KEY`.'\"WORD\\ 'str`.''\"ing\\' `na``.'\"Me\\`"},
		{RestoreStringSingleQuotes | RestoreStringDoubleQuotes, "key`.'\"Word\\ 'str`.''\"ing\\' na`.'\"Me\\"},
		{RestoreKeyWordUppercase | RestoreKeyWordLowercase, "KEY`.'\"WORD\\ str`.'\"ing\\ na`.'\"Me\\"},
		{RestoreNameUppercase | RestoreNameLowercase, "key`.'\"Word\\ str`.'\"ing\\ NA`.'\"ME\\"},
		{RestoreNameDoubleQuotes | RestoreNameBackQuotes, "key`.'\"Word\\ str`.'\"ing\\ \"na`.'\"\"Me\\\""},
	}
	var sb strings.Builder
	for _, testCase := range testCases {
		sb.Reset()
		ctx := NewRestoreCtx(testCase.flag, &sb)
		ctx.WriteKeyWord("key`.'\"Word\\")
		ctx.WritePlain(" ")
		ctx.WriteString("str`.'\"ing\\")
		ctx.WritePlain(" ")
		ctx.WriteName("na`.'\"Me\\")
		require.Equalf(t, testCase.expect, sb.String(), "case: %#v", testCase)
	}
}

func TestRestoreSpecialComment(t *testing.T) {
	var sb strings.Builder
	sb.Reset()
	ctx := NewRestoreCtx(RestoreTiDBSpecialComment, &sb)
	ctx.WriteWithSpecialComments("fea_id", func() {
		ctx.WritePlain("content")
	})
	require.Equal(t, "/*T![fea_id] content */", sb.String())

	sb.Reset()
	ctx.WriteWithSpecialComments("", func() {
		ctx.WritePlain("shard_row_id_bits")
	})
	require.Equal(t, "/*T! shard_row_id_bits */", sb.String())
}
