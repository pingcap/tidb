// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEscape(t *testing.T) {
	var bf bytes.Buffer
	str := []byte(`MWQeWw""'\rNmtGxzGp`)
	expectStrBackslash := `MWQeWw\"\"\'\\rNmtGxzGp`
	expectStrWithoutBackslash := `MWQeWw""''\rNmtGxzGp`
	expectStrBackslashDoubleQuote := `MWQeWw\"\"'\\rNmtGxzGp`
	expectStrWithoutBackslashDoubleQuote := `MWQeWw""""'\rNmtGxzGp`
	escapeSQL(str, &bf, true)
	require.Equal(t, expectStrBackslash, bf.String())

	bf.Reset()
	escapeSQL(str, &bf, false)
	require.Equal(t, expectStrWithoutBackslash, bf.String())

	bf.Reset()
	opt := &csvOption{
		delimiter: []byte(`"`),
		separator: []byte(`,`),
	}
	escapeCSV(str, &bf, true, opt)
	require.Equal(t, expectStrBackslashDoubleQuote, bf.String())

	bf.Reset()
	escapeCSV(str, &bf, false, opt)
	require.Equal(t, expectStrWithoutBackslashDoubleQuote, bf.String())

	bf.Reset()
	str = []byte(`a|*|b"cd`)
	expectedStrWithDelimiter := `a|*|b""cd`
	expectedStrBackslashWithoutDelimiter := `a\|*\|b"cd`
	expectedStrWithoutDelimiter := `a|*|b"cd`
	escapeCSV(str, &bf, false, opt)
	require.Equal(t, expectedStrWithDelimiter, bf.String())

	bf.Reset()
	opt.delimiter = []byte("")
	opt.separator = []byte(`|*|`)
	escapeCSV(str, &bf, true, opt)
	require.Equal(t, expectedStrBackslashWithoutDelimiter, bf.String())

	bf.Reset()
	escapeCSV(str, &bf, false, opt)
	require.Equal(t, expectedStrWithoutDelimiter, bf.String())
}
