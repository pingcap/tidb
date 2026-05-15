// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"bytes"
	"database/sql"
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

	t.Run("GetRawBytes reuses row buffer", func(t *testing.T) {
		row := MakeRowReceiver([]string{"INT", "VARCHAR", "BLOB"})

		row.receivers[0].(*SQLTypeNumber).RawBytes = sql.RawBytes("1")
		row.receivers[1].(*SQLTypeString).RawBytes = sql.RawBytes("alpha")
		row.receivers[2].(*SQLTypeBytes).RawBytes = sql.RawBytes{0x01, 0x02}

		first := row.GetRawBytes()
		require.Len(t, first, 3)
		require.Equal(t, []sql.RawBytes{
			sql.RawBytes("1"),
			sql.RawBytes("alpha"),
			sql.RawBytes{0x01, 0x02},
		}, first)

		row.receivers[0].(*SQLTypeNumber).RawBytes = sql.RawBytes("2")
		row.receivers[1].(*SQLTypeString).RawBytes = sql.RawBytes("beta")
		row.receivers[2].(*SQLTypeBytes).RawBytes = sql.RawBytes{0x03}
		second := row.GetRawBytes()

		// GetRawBytes returns a reusable row buffer for parquet writer hot path.
		require.Same(t, &first[0], &second[0])
		require.Equal(t, []sql.RawBytes{
			sql.RawBytes("2"),
			sql.RawBytes("beta"),
			sql.RawBytes{0x03},
		}, second)
	})

	t.Run("GetRawBytes hot path stays allocation-free", func(t *testing.T) {
		row := MakeRowReceiver([]string{"INT", "VARCHAR", "BLOB"})
		row.receivers[0].(*SQLTypeNumber).RawBytes = sql.RawBytes("1")
		row.receivers[1].(*SQLTypeString).RawBytes = sql.RawBytes("alpha")
		row.receivers[2].(*SQLTypeBytes).RawBytes = sql.RawBytes{0x01, 0x02}

		allocs := testing.AllocsPerRun(1000, func() {
			_ = row.GetRawBytes()
		})
		require.Zero(t, allocs)
	})
}
