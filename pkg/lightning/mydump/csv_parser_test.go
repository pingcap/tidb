// Copyright 2023 PingCAP, Inc.
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

package mydump_test

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/lightning/worker"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var ioWorkersForCSV = worker.NewPool(context.Background(), 5, "test_csv")

func assertPosEqual(t *testing.T, parser mydump.Parser, expectPos, expectRowID int64) {
	pos, rowID := parser.Pos()
	require.Equal(t, expectPos, pos)
	require.Equal(t, expectRowID, rowID)
}

var nullDatum types.Datum

type testCase struct {
	input    string
	expected [][]types.Datum
}

func runTestCasesCSV(t *testing.T, cfg *config.MydumperRuntime, blockBufSize int64, cases []testCase) {
	for _, tc := range cases {
		charsetConvertor, err := mydump.NewCharsetConvertor(cfg.DataCharacterSet, cfg.DataInvalidCharReplace)
		assert.NoError(t, err)
		parser, err := mydump.NewCSVParser(context.Background(), &cfg.CSV, mydump.NewStringReader(tc.input), blockBufSize, ioWorkersForCSV, false, charsetConvertor)
		assert.NoError(t, err)
		for i, row := range tc.expected {
			comment := fmt.Sprintf("input = %q, row = %d", tc.input, i+1)
			e := parser.ReadRow()
			assert.NoErrorf(t, e, "input = %q, row = %d, error = %s", tc.input, i+1, errors.ErrorStack(e))
			assert.Equal(t, int64(i)+1, parser.LastRow().RowID, comment)
			assert.Equal(t, row, parser.LastRow().Row, comment)
		}
		assert.ErrorIsf(t, errors.Cause(parser.ReadRow()), io.EOF, "input = %q", tc.input)
	}
}

func runFailingTestCasesCSV(t *testing.T, cfg *config.MydumperRuntime, blockBufSize int64, cases []string) {
	for _, tc := range cases {
		charsetConvertor, err := mydump.NewCharsetConvertor(cfg.DataCharacterSet, cfg.DataInvalidCharReplace)
		assert.NoError(t, err)
		parser, err := mydump.NewCSVParser(context.Background(), &cfg.CSV, mydump.NewStringReader(tc), blockBufSize, ioWorkersForCSV, false, charsetConvertor)
		require.NoError(t, err)
		e := parser.ReadRow()
		assert.Regexpf(t, "syntax error.*", e.Error(), "input = %q / %s", tc, errors.ErrorStack(e))
	}
}

func tpchDatums() [][]types.Datum {
	datums := make([][]types.Datum, 0, 3)
	datums = append(datums, []types.Datum{
		types.NewStringDatum("1"),
		types.NewStringDatum("goldenrod lavender spring chocolate lace"),
		types.NewStringDatum("Manufacturer#1"),
		types.NewStringDatum("Brand#13"),
		types.NewStringDatum("PROMO BURNISHED COPPER"),
		types.NewStringDatum("7"),
		types.NewStringDatum("JUMBO PKG"),
		types.NewStringDatum("901.00"),
		types.NewStringDatum("ly. slyly ironi"),
	})
	datums = append(datums, []types.Datum{
		types.NewStringDatum("2"),
		types.NewStringDatum("blush thistle blue yellow saddle"),
		types.NewStringDatum("Manufacturer#1"),
		types.NewStringDatum("Brand#13"),
		types.NewStringDatum("LARGE BRUSHED BRASS"),
		types.NewStringDatum("1"),
		types.NewStringDatum("LG CASE"),
		types.NewStringDatum("902.00"),
		types.NewStringDatum("lar accounts amo"),
	})
	datums = append(datums, []types.Datum{
		types.NewStringDatum("3"),
		types.NewStringDatum("spring green yellow purple cornsilk"),
		types.NewStringDatum("Manufacturer#4"),
		types.NewStringDatum("Brand#42"),
		types.NewStringDatum("STANDARD POLISHED BRASS"),
		types.NewStringDatum("21"),
		types.NewStringDatum("WRAP CASE"),
		types.NewStringDatum("903.00"),
		types.NewStringDatum("egular deposits hag"),
	})

	return datums
}

func datumsToString(datums [][]types.Datum, delimitor string, quote string, lastSep bool) string {
	var b strings.Builder
	doubleQuote := quote + quote
	for _, ds := range datums {
		for i, d := range ds {
			text := d.GetString()
			if len(quote) > 0 {
				b.WriteString(quote)
				b.WriteString(strings.ReplaceAll(text, quote, doubleQuote))
				b.WriteString(quote)
			} else {
				b.WriteString(text)
			}
			if lastSep || i < len(ds)-1 {
				b.WriteString(delimitor)
			}
		}
		b.WriteString("\r\n")
	}
	return b.String()
}

func TestTPCH(t *testing.T) {
	datums := tpchDatums()
	input := datumsToString(datums, "|", "", true)
	reader := mydump.NewStringReader(input)

	cfg := config.CSVConfig{
		Separator:   "|",
		Delimiter:   "",
		TrimLastSep: true,
	}

	parser, err := mydump.NewCSVParser(context.Background(), &cfg, reader, int64(config.ReadBlockSize), ioWorkersForCSV, false, nil)
	require.NoError(t, err)
	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID:  1,
		Row:    datums[0],
		Length: 116,
	}, parser.LastRow())
	assertPosEqual(t, parser, 126, 1)

	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID:  2,
		Row:    datums[1],
		Length: 104,
	}, parser.LastRow())
	assertPosEqual(t, parser, 241, 2)

	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID:  3,
		Row:    datums[2],
		Length: 117,
	}, parser.LastRow())
	assertPosEqual(t, parser, 369, 3)

	require.ErrorIs(t, errors.Cause(parser.ReadRow()), io.EOF)
}

func TestTPCHMultiBytes(t *testing.T) {
	datums := tpchDatums()
	sepsAndQuotes := [][2]string{
		{",", ""},
		{"\000", ""},
		{"，", ""},
		{"🤔", ""},
		{"，", "。"},
		{"||", ""},
		{"|+|", ""},
		{"##", ""},
		{"，", "'"},
		{"，", `"`},
		{"🤔", `''`},
		{"🤔", `"'`},
		{"🤔", `"'`},
		{"🤔", "🌚"}, // this two emoji have same prefix bytes
		{"##", "#-"},
		{"\\s", "\\q"},
		{",", "1"},
		{",", "ac"},
	}
	for _, SepAndQuote := range sepsAndQuotes {
		inputStr := datumsToString(datums, SepAndQuote[0], SepAndQuote[1], false)

		// extract all index in the middle of '\r\n' from the inputStr.
		// they indicate where the parser stops after reading one row.
		// should be equals to the number of datums.
		var allExpectedParserPos []int
		for {
			last := 0
			if len(allExpectedParserPos) > 0 {
				last = allExpectedParserPos[len(allExpectedParserPos)-1]
			}
			pos := strings.IndexByte(inputStr[last:], '\r')
			if pos < 0 {
				break
			}
			allExpectedParserPos = append(allExpectedParserPos, last+pos+1)
		}
		require.Len(t, allExpectedParserPos, len(datums))

		cfg := config.CSVConfig{
			Separator:   SepAndQuote[0],
			Delimiter:   SepAndQuote[1],
			TrimLastSep: false,
		}

		reader := mydump.NewStringReader(inputStr)
		parser, err := mydump.NewCSVParser(context.Background(), &cfg, reader, int64(config.ReadBlockSize), ioWorkersForCSV, false, nil)
		require.NoError(t, err)

		for i, expectedParserPos := range allExpectedParserPos {
			require.Nil(t, parser.ReadRow())
			require.Equal(t, int64(i+1), parser.LastRow().RowID)
			require.Equal(t, datums[i], parser.LastRow().Row)
			assertPosEqual(t, parser, int64(expectedParserPos), int64(i+1))
		}

		require.ErrorIs(t, errors.Cause(parser.ReadRow()), io.EOF)
	}
}

func TestRFC4180(t *testing.T) {
	cfg := config.CSVConfig{
		Separator: ",",
		Delimiter: `"`,
	}

	// example 1, trailing new lines

	parser, err := mydump.NewCSVParser(context.Background(), &cfg, mydump.NewStringReader("aaa,bbb,ccc\nzzz,yyy,xxx\n"), int64(config.ReadBlockSize), ioWorkersForCSV, false, nil)
	require.NoError(t, err)

	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Row: []types.Datum{
			types.NewStringDatum("aaa"),
			types.NewStringDatum("bbb"),
			types.NewStringDatum("ccc"),
		},
		Length: 9,
	}, parser.LastRow())
	assertPosEqual(t, parser, 12, 1)

	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 2,
		Row: []types.Datum{
			types.NewStringDatum("zzz"),
			types.NewStringDatum("yyy"),
			types.NewStringDatum("xxx"),
		},
		Length: 9,
	}, parser.LastRow())
	assertPosEqual(t, parser, 24, 2)

	require.ErrorIs(t, errors.Cause(parser.ReadRow()), io.EOF)

	// example 2, no trailing new lines

	parser, err = mydump.NewCSVParser(context.Background(), &cfg, mydump.NewStringReader("aaa,bbb,ccc\nzzz,yyy,xxx"), int64(config.ReadBlockSize), ioWorkersForCSV, false, nil)
	require.NoError(t, err)

	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Row: []types.Datum{
			types.NewStringDatum("aaa"),
			types.NewStringDatum("bbb"),
			types.NewStringDatum("ccc"),
		},
		Length: 9,
	}, parser.LastRow())
	assertPosEqual(t, parser, 12, 1)

	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 2,
		Row: []types.Datum{
			types.NewStringDatum("zzz"),
			types.NewStringDatum("yyy"),
			types.NewStringDatum("xxx"),
		},
		Length: 9,
	}, parser.LastRow())
	assertPosEqual(t, parser, 23, 2)

	require.ErrorIs(t, errors.Cause(parser.ReadRow()), io.EOF)

	// example 5, quoted fields

	parser, err = mydump.NewCSVParser(context.Background(), &cfg, mydump.NewStringReader(`"aaa","bbb","ccc"`+"\nzzz,yyy,xxx"), int64(config.ReadBlockSize), ioWorkersForCSV, false, nil)
	require.NoError(t, err)

	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Row: []types.Datum{
			types.NewStringDatum("aaa"),
			types.NewStringDatum("bbb"),
			types.NewStringDatum("ccc"),
		},
		Length: 9,
	}, parser.LastRow())
	assertPosEqual(t, parser, 18, 1)

	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 2,
		Row: []types.Datum{
			types.NewStringDatum("zzz"),
			types.NewStringDatum("yyy"),
			types.NewStringDatum("xxx"),
		},
		Length: 9,
	}, parser.LastRow())
	assertPosEqual(t, parser, 29, 2)

	require.ErrorIs(t, errors.Cause(parser.ReadRow()), io.EOF)

	// example 6, line breaks within fields

	parser, err = mydump.NewCSVParser(context.Background(), &cfg, mydump.NewStringReader(`"aaa","b
bb","ccc"
zzz,yyy,xxx`), int64(config.ReadBlockSize), ioWorkersForCSV, false, nil)
	require.NoError(t, err)

	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Row: []types.Datum{
			types.NewStringDatum("aaa"),
			types.NewStringDatum("b\nbb"),
			types.NewStringDatum("ccc"),
		},
		Length: 10,
	}, parser.LastRow())
	assertPosEqual(t, parser, 19, 1)

	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 2,
		Row: []types.Datum{
			types.NewStringDatum("zzz"),
			types.NewStringDatum("yyy"),
			types.NewStringDatum("xxx"),
		},
		Length: 9,
	}, parser.LastRow())
	assertPosEqual(t, parser, 30, 2)

	require.ErrorIs(t, errors.Cause(parser.ReadRow()), io.EOF)

	// example 7, quote escaping

	parser, err = mydump.NewCSVParser(context.Background(), &cfg, mydump.NewStringReader(`"aaa","b""bb","ccc"`), int64(config.ReadBlockSize), ioWorkersForCSV, false, nil)
	require.NoError(t, err)

	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Row: []types.Datum{
			types.NewStringDatum("aaa"),
			types.NewStringDatum("b\"bb"),
			types.NewStringDatum("ccc"),
		},
		Length: 10,
	}, parser.LastRow())
	assertPosEqual(t, parser, 19, 1)

	require.ErrorIs(t, errors.Cause(parser.ReadRow()), io.EOF)
}

func TestMySQL(t *testing.T) {
	cfg := config.CSVConfig{
		Separator:  ",",
		Delimiter:  `"`,
		Terminator: "\n",
		EscapedBy:  `\`,
		NotNull:    false,
		Null:       []string{`\N`},
	}

	parser, err := mydump.NewCSVParser(context.Background(), &cfg, mydump.NewStringReader(`"\"","\\","\?"
"\
",\N,\\N`), int64(config.ReadBlockSize), ioWorkersForCSV, false, nil)
	require.NoError(t, err)

	require.NoError(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Row: []types.Datum{
			types.NewStringDatum(`"`),
			types.NewStringDatum(`\`),
			types.NewStringDatum("?"),
		},
		Length: 6,
	}, parser.LastRow())
	assertPosEqual(t, parser, 15, 1)

	require.NoError(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 2,
		Row: []types.Datum{
			types.NewStringDatum("\n"),
			nullDatum,
			types.NewStringDatum(`\N`),
		},
		Length: 7,
	}, parser.LastRow())
	assertPosEqual(t, parser, 26, 2)

	require.ErrorIs(t, errors.Cause(parser.ReadRow()), io.EOF)

	parser, err = mydump.NewCSVParser(
		context.Background(), &cfg,
		mydump.NewStringReader(`"\0\b\n\r\t\Z\\\  \c\'\""`),
		int64(config.ReadBlockSize), ioWorkersForCSV, false, nil)
	require.NoError(t, err)

	require.NoError(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Row: []types.Datum{
			types.NewStringDatum(string([]byte{0, '\b', '\n', '\r', '\t', 26, '\\', ' ', ' ', 'c', '\'', '"'})),
		},
		Length: 23,
	}, parser.LastRow())

	cfg.UnescapedQuote = true
	parser, err = mydump.NewCSVParser(
		context.Background(), &cfg,
		mydump.NewStringReader(`3,"a string containing a " quote",102.20
`),
		int64(config.ReadBlockSize), ioWorkersForCSV, false, nil)
	require.NoError(t, err)

	require.NoError(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Row: []types.Datum{
			types.NewStringDatum("3"),
			types.NewStringDatum(`a string containing a " quote`),
			types.NewStringDatum("102.20"),
		},
		Length: 36,
	}, parser.LastRow())

	parser, err = mydump.NewCSVParser(
		context.Background(), &cfg,
		mydump.NewStringReader(`3,"a string containing a " quote","102.20"`),
		int64(config.ReadBlockSize), ioWorkersForCSV, false, nil)
	require.NoError(t, err)

	require.NoError(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Row: []types.Datum{
			types.NewStringDatum("3"),
			types.NewStringDatum(`a string containing a " quote`),
			types.NewStringDatum("102.20"),
		},
		Length: 36,
	}, parser.LastRow())

	parser, err = mydump.NewCSVParser(
		context.Background(), &cfg,
		mydump.NewStringReader(`"a"b",c"d"e`),
		int64(config.ReadBlockSize), ioWorkersForCSV, false, nil)
	require.NoError(t, err)

	require.NoError(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Row: []types.Datum{
			types.NewStringDatum(`a"b`),
			types.NewStringDatum(`c"d"e`),
		},
		Length: 8,
	}, parser.LastRow())
}

func TestCustomEscapeChar(t *testing.T) {
	cfg := config.CSVConfig{
		Separator: ",",
		Delimiter: `"`,
		EscapedBy: `!`,
		NotNull:   false,
		Null:      []string{`!N`},
	}

	parser, err := mydump.NewCSVParser(context.Background(), &cfg, mydump.NewStringReader(`"!"","!!","!\"
"!
",!N,!!N`), int64(config.ReadBlockSize), ioWorkersForCSV, false, nil)
	require.NoError(t, err)

	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Row: []types.Datum{
			types.NewStringDatum(`"`),
			types.NewStringDatum(`!`),
			types.NewStringDatum(`\`),
		},
		Length: 6,
	}, parser.LastRow())
	assertPosEqual(t, parser, 15, 1)

	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 2,
		Row: []types.Datum{
			types.NewStringDatum("\n"),
			nullDatum,
			types.NewStringDatum(`!N`),
		},
		Length: 7,
	}, parser.LastRow())
	assertPosEqual(t, parser, 26, 2)

	require.ErrorIs(t, errors.Cause(parser.ReadRow()), io.EOF)

	cfg = config.CSVConfig{
		Separator: ",",
		Delimiter: `"`,
		EscapedBy: ``,
		NotNull:   false,
		Null:      []string{`NULL`},
	}

	parser, err = mydump.NewCSVParser(
		context.Background(), &cfg,
		mydump.NewStringReader(`"{""itemRangeType"":0,""itemContainType"":0,""shopRangeType"":1,""shopJson"":""[{\""id\"":\""A1234\"",\""shopName\"":\""AAAAAA\""}]""}"`),
		int64(config.ReadBlockSize), ioWorkersForCSV, false, nil)
	require.NoError(t, err)

	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Row: []types.Datum{
			types.NewStringDatum(`{"itemRangeType":0,"itemContainType":0,"shopRangeType":1,"shopJson":"[{\"id\":\"A1234\",\"shopName\":\"AAAAAA\"}]"}`),
		},
		Length: 115,
	}, parser.LastRow())
}

func TestSyntaxErrorCSV(t *testing.T) {
	cfg := config.MydumperRuntime{
		CSV: config.CSVConfig{
			Separator: ",",
			Delimiter: `"`,
			EscapedBy: `\`,
		},
	}

	inputs := []string{
		`"???`,
		`\`,
		`"\`,
		`0"`,
		`0\`,
		"\"\v",
		`"""`,
		"\"\r",
		"\"\x01",
	}

	runFailingTestCasesCSV(t, &cfg, int64(config.ReadBlockSize), inputs)

	cfg.CSV.EscapedBy = ""
	runFailingTestCasesCSV(t, &cfg, int64(config.ReadBlockSize), []string{`"\`})
}

func TestTSV(t *testing.T) {
	cfg := config.CSVConfig{
		Separator:         "\t",
		Delimiter:         "",
		BackslashEscape:   false,
		NotNull:           false,
		Null:              []string{""},
		Header:            true,
		HeaderSchemaMatch: true,
	}

	parser, err := mydump.NewCSVParser(context.Background(), &cfg, mydump.NewStringReader(`a	b	c	d	e	f
0				foo	0000-00-00
0				foo	0000-00-00
0	abc	def	ghi	bar	1999-12-31`), int64(config.ReadBlockSize), ioWorkersForCSV, true, nil)
	require.NoError(t, err)

	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Row: []types.Datum{
			types.NewStringDatum("0"),
			nullDatum,
			nullDatum,
			nullDatum,
			types.NewStringDatum("foo"),
			types.NewStringDatum("0000-00-00"),
		},
		Length: 14,
	}, parser.LastRow())
	assertPosEqual(t, parser, 32, 1)
	require.Equal(t, []string{"a", "b", "c", "d", "e", "f"}, parser.Columns())

	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 2,
		Row: []types.Datum{
			types.NewStringDatum("0"),
			nullDatum,
			nullDatum,
			nullDatum,
			types.NewStringDatum("foo"),
			types.NewStringDatum("0000-00-00"),
		},
		Length: 14,
	}, parser.LastRow())
	assertPosEqual(t, parser, 52, 2)

	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 3,
		Row: []types.Datum{
			types.NewStringDatum("0"),
			types.NewStringDatum("abc"),
			types.NewStringDatum("def"),
			types.NewStringDatum("ghi"),
			types.NewStringDatum("bar"),
			types.NewStringDatum("1999-12-31"),
		},
		Length: 23,
	}, parser.LastRow())
	assertPosEqual(t, parser, 80, 3)

	require.ErrorIs(t, errors.Cause(parser.ReadRow()), io.EOF)
}

func TestCsvWithWhiteSpaceLine(t *testing.T) {
	cfg := config.CSVConfig{
		Separator: ",",
		Delimiter: `"`,
		Null:      []string{""},
	}
	data := " \r\n\r\n0,,abc\r\n \r\n123,1999-12-31,test\r\n"
	parser, err := mydump.NewCSVParser(context.Background(), &cfg, mydump.NewStringReader(data), int64(config.ReadBlockSize), ioWorkersForCSV, false, nil)
	require.NoError(t, err)
	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Row: []types.Datum{
			types.NewStringDatum("0"),
			nullDatum,
			types.NewStringDatum("abc"),
		},
		Length: 4,
	}, parser.LastRow())

	assertPosEqual(t, parser, 12, 1)
	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 2,
		Row: []types.Datum{
			types.NewStringDatum("123"),
			types.NewStringDatum("1999-12-31"),
			types.NewStringDatum("test"),
		},
		Length: 17,
	}, parser.LastRow())
	require.Nil(t, parser.Close())

	cfg.Header = true
	cfg.HeaderSchemaMatch = true
	data = " \r\na,b,c\r\n0,,abc\r\n"
	parser, err = mydump.NewCSVParser(context.Background(), &cfg, mydump.NewStringReader(data), int64(config.ReadBlockSize), ioWorkersForCSV, true, nil)
	require.NoError(t, err)
	require.Nil(t, parser.ReadRow())
	require.Equal(t, []string{"a", "b", "c"}, parser.Columns())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Row: []types.Datum{
			types.NewStringDatum("0"),
			nullDatum,
			types.NewStringDatum("abc"),
		},
		Length: 4,
	}, parser.LastRow())

	assertPosEqual(t, parser, 17, 1)
	require.Nil(t, parser.Close())
}

func TestEmpty(t *testing.T) {
	cfg := config.CSVConfig{
		Separator: ",",
		Delimiter: `"`,
	}

	parser, err := mydump.NewCSVParser(context.Background(), &cfg, mydump.NewStringReader(""), int64(config.ReadBlockSize), ioWorkersForCSV, false, nil)
	require.NoError(t, err)
	require.ErrorIs(t, errors.Cause(parser.ReadRow()), io.EOF)

	// Try again with headers.

	cfg.Header = true
	cfg.HeaderSchemaMatch = true

	parser, err = mydump.NewCSVParser(context.Background(), &cfg, mydump.NewStringReader(""), int64(config.ReadBlockSize), ioWorkersForCSV, true, nil)
	require.NoError(t, err)
	require.ErrorIs(t, errors.Cause(parser.ReadRow()), io.EOF)

	parser, err = mydump.NewCSVParser(context.Background(), &cfg, mydump.NewStringReader("h\n"), int64(config.ReadBlockSize), ioWorkersForCSV, true, nil)
	require.NoError(t, err)
	require.ErrorIs(t, errors.Cause(parser.ReadRow()), io.EOF)
}

func TestCRLF(t *testing.T) {
	cfg := config.CSVConfig{
		Separator: ",",
		Delimiter: `"`,
	}
	parser, err := mydump.NewCSVParser(context.Background(), &cfg, mydump.NewStringReader("a\rb\r\nc\n\n\n\nd"), int64(config.ReadBlockSize), ioWorkersForCSV, false, nil)
	require.NoError(t, err)

	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID:  1,
		Row:    []types.Datum{types.NewStringDatum("a")},
		Length: 1,
	}, parser.LastRow())

	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID:  2,
		Row:    []types.Datum{types.NewStringDatum("b")},
		Length: 1,
	}, parser.LastRow())

	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID:  3,
		Row:    []types.Datum{types.NewStringDatum("c")},
		Length: 1,
	}, parser.LastRow())

	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID:  4,
		Row:    []types.Datum{types.NewStringDatum("d")},
		Length: 1,
	}, parser.LastRow())

	require.ErrorIs(t, errors.Cause(parser.ReadRow()), io.EOF)
}

func TestQuotedSeparator(t *testing.T) {
	cfg := config.CSVConfig{
		Separator: ",",
		Delimiter: `"`,
	}

	parser, err := mydump.NewCSVParser(context.Background(), &cfg, mydump.NewStringReader(`",",','`), int64(config.ReadBlockSize), ioWorkersForCSV, false, nil)
	require.NoError(t, err)
	require.Nil(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Row: []types.Datum{
			types.NewStringDatum(","),
			types.NewStringDatum("'"),
			types.NewStringDatum("'"),
		},
		Length: 3,
	}, parser.LastRow())

	require.ErrorIs(t, errors.Cause(parser.ReadRow()), io.EOF)
}

func TestConsecutiveFields(t *testing.T) {
	// Note: the behavior of reading `"xxx"yyy` here is undefined in RFC 4180.
	// Python's CSV module returns `xxxyyy`.
	// Rust's CSV package returns `xxxyyy`.
	// Go's CSV package returns a parse error.
	// NPM's CSV package returns a parse error.
	// MySQL's LOAD DATA statement returns `"xxx"yyy` as-is.

	cfg := config.MydumperRuntime{
		CSV: config.CSVConfig{
			Separator: ",",
			Delimiter: `"`,
		},
	}

	testCases := []string{
		`"x"?`,
		"\"\"\x01",
		"\"\"\v",
		`abc""`,
	}

	runFailingTestCasesCSV(t, &cfg, int64(config.ReadBlockSize), testCases)

	cfg.CSV.Delimiter = "|+|"
	runFailingTestCasesCSV(t, &cfg, int64(config.ReadBlockSize), []string{
		"abc|1|+||+|\r\n",
	})
}

func TestTooLargeRow(t *testing.T) {
	cfg := config.MydumperRuntime{
		CSV: config.CSVConfig{
			Separator: ",",
			Delimiter: `"`,
		},
	}
	var testCase bytes.Buffer
	testCase.WriteString("a,b,c,d")
	// WARN: will take up 10KB memory here.
	mydump.LargestEntryLimit = 10 * 1024
	for i := 0; i < mydump.LargestEntryLimit; i++ {
		testCase.WriteByte('d')
	}
	charsetConvertor, err := mydump.NewCharsetConvertor(cfg.DataCharacterSet, cfg.DataInvalidCharReplace)
	require.NoError(t, err)
	parser, err := mydump.NewCSVParser(context.Background(), &cfg.CSV, mydump.NewStringReader(testCase.String()), int64(config.ReadBlockSize), ioWorkersForCSV, false, charsetConvertor)
	require.NoError(t, err)
	e := parser.ReadRow()
	require.Error(t, e)
	require.Contains(t, e.Error(), "size of row cannot exceed the max value of txn-entry-size-limit")
}

func TestSpecialChars(t *testing.T) {
	cfg := config.MydumperRuntime{
		CSV: config.CSVConfig{Separator: ",", Delimiter: `"`},
	}
	testCases := []testCase{
		{
			input:    "\x00",
			expected: [][]types.Datum{{types.NewStringDatum("\x00")}},
		},
		{
			input:    `0\`,
			expected: [][]types.Datum{{types.NewStringDatum(`0\`)}},
		},
		{
			input:    `\`,
			expected: [][]types.Datum{{types.NewStringDatum(`\`)}},
		},
		{
			input:    "0\v",
			expected: [][]types.Datum{{types.NewStringDatum("0\v")}},
		},
		{
			input:    "0\x00",
			expected: [][]types.Datum{{types.NewStringDatum("0\x00")}},
		},
		{
			input:    "\n\r",
			expected: [][]types.Datum{},
		},
		{
			input:    `"""",0`,
			expected: [][]types.Datum{{types.NewStringDatum(`"`), types.NewStringDatum(`0`)}},
		},
	}

	runTestCasesCSV(t, &cfg, int64(config.ReadBlockSize), testCases)
}

func TestContinuationCSV(t *testing.T) {
	cfg := config.MydumperRuntime{
		CSV: config.CSVConfig{
			Separator:   ",",
			Delimiter:   `"`,
			EscapedBy:   `\`,
			TrimLastSep: true,
		},
	}

	testCases := []testCase{
		{
			input: `"abcdef",\njklm,nop` + "\r\n" + `"""""","\n",a,`,
			expected: [][]types.Datum{
				{
					types.NewStringDatum("abcdef"),
					types.NewStringDatum("\njklm"),
					types.NewStringDatum("nop"),
				},
				{
					types.NewStringDatum(`""`),
					types.NewStringDatum("\n"),
					types.NewStringDatum("a"),
				},
			},
		},
		{
			input:    `"VzMXdTXsLbiIqTYQlwPSudocNPKVsAqXgnuvupXEzlxkaFpBtHNDyoVEydoEgdnhsygaNHLpMTdEkpkrkNdzVjCbSoXvUqwoVaca"`,
			expected: [][]types.Datum{{types.NewStringDatum("VzMXdTXsLbiIqTYQlwPSudocNPKVsAqXgnuvupXEzlxkaFpBtHNDyoVEydoEgdnhsygaNHLpMTdEkpkrkNdzVjCbSoXvUqwoVaca")}},
		},
	}

	runTestCasesCSV(t, &cfg, 1, testCases)
}

func TestBackslashAsSep(t *testing.T) {
	cfg := config.MydumperRuntime{
		CSV: config.CSVConfig{
			Separator: `\`,
			Delimiter: `"`,
			Null:      []string{""},
		},
	}

	testCases := []testCase{
		{
			input:    `0\`,
			expected: [][]types.Datum{{types.NewStringDatum("0"), nullDatum}},
		},
		{
			input:    `\`,
			expected: [][]types.Datum{{nullDatum, nullDatum}},
		},
	}

	runTestCasesCSV(t, &cfg, 1, testCases)

	failingInputs := []string{
		`"\`,
	}
	runFailingTestCasesCSV(t, &cfg, 1, failingInputs)
}

func TestBackslashAsDelim(t *testing.T) {
	cfg := config.MydumperRuntime{
		CSV: config.CSVConfig{
			Separator: ",",
			Delimiter: `\`,
			Null:      []string{""},
		},
	}

	testCases := []testCase{
		{
			input:    `\\`,
			expected: [][]types.Datum{{nullDatum}},
		},
	}
	runTestCasesCSV(t, &cfg, 1, testCases)

	failingInputs := []string{
		`"\`,
	}
	runFailingTestCasesCSV(t, &cfg, 1, failingInputs)

	cfg = config.MydumperRuntime{
		CSV: config.CSVConfig{
			Separator:        ",",
			Delimiter:        `\`,
			Null:             []string{""},
			QuotedNullIsText: true,
		},
	}

	testCases = []testCase{
		{
			input:    `\\`,
			expected: [][]types.Datum{{types.NewStringDatum("")}},
		},
	}
	runTestCasesCSV(t, &cfg, 1, testCases)
}

// errorReader implements the Reader interface which always returns an error.
type errorReader struct{}

func (*errorReader) Read(p []byte) (int, error) {
	return 0, errors.New("fake read error")
}

func (*errorReader) Seek(offset int64, whence int) (int64, error) {
	return 0, errors.New("fake seek error")
}

func (*errorReader) Close() error {
	return errors.New("fake close error")
}

func TestReadError(t *testing.T) {
	cfg := config.CSVConfig{
		Separator: ",",
		Delimiter: `"`,
	}

	parser, err := mydump.NewCSVParser(context.Background(), &cfg, &errorReader{}, int64(config.ReadBlockSize), ioWorkersForCSV, false, nil)
	require.NoError(t, err)
	require.Regexp(t, "fake read error", parser.ReadRow().Error())
}

// TestSyntaxErrorLog checks that a syntax error won't dump huge strings into the log.
func TestSyntaxErrorLog(t *testing.T) {
	cfg := config.MydumperRuntime{
		CSV: config.CSVConfig{
			Separator: "\t",
			Delimiter: "'",
		},
	}

	tc := mydump.NewStringReader("x'" + strings.Repeat("y", 50000))
	parser, err := mydump.NewCSVParser(context.Background(), &cfg.CSV, tc, 50000, ioWorkersForCSV, false, nil)
	require.NoError(t, err)
	logger, buffer := log.MakeTestLogger()
	parser.SetLogger(logger)
	require.Regexp(t, "syntax error.*", parser.ReadRow().Error())
	require.Nil(t, logger.Sync())

	require.Equal(t,
		`{"$lvl":"ERROR","$msg":"syntax error","pos":2,"content":"`+strings.Repeat("y", 256)+`"}`,
		buffer.Stripped(),
	)
}

// TestTrimLastSep checks that set `TrimLastSep` to true trim only the last empty filed.
func TestTrimLastSep(t *testing.T) {
	cfg := config.MydumperRuntime{
		CSV: config.CSVConfig{
			Separator:   ",",
			Delimiter:   `"`,
			TrimLastSep: true,
		},
	}
	parser, err := mydump.NewCSVParser(
		context.Background(),
		&cfg.CSV,
		mydump.NewStringReader("123,456,789,\r\na,b,,\r\n,,,\r\n\"a\",\"\",\"\",\r\n"),
		int64(config.ReadBlockSize),
		ioWorkersForCSV,
		false,
		nil,
	)
	require.NoError(t, err)
	for i := 0; i < 4; i++ {
		require.Nil(t, parser.ReadRow())
		require.Len(t, parser.LastRow().Row, 3)
	}
}

// TestTerminator checks for customized terminators.
func TestTerminator(t *testing.T) {
	cfg := config.MydumperRuntime{
		CSV: config.CSVConfig{
			Separator:  "|+|",
			Terminator: "|+|\n",
		},
	}

	testCases := []testCase{
		{
			input: "5|+|abc\ndef\nghi|+|6|+|\n7|+|xy|+z|+|8|+|\n",
			expected: [][]types.Datum{
				{types.NewStringDatum("5"), types.NewStringDatum("abc\ndef\nghi"), types.NewStringDatum("6")},
				{types.NewStringDatum("7"), types.NewStringDatum("xy|+z"), types.NewStringDatum("8")},
			},
		},
	}

	runTestCasesCSV(t, &cfg, 1, testCases)

	cfg.CSV.Delimiter = "|+>"

	testCases = []testCase{
		{
			input: "xyz|+|+>|+|\n|+>|+|\n|+>|+|\r|+|\n",
			expected: [][]types.Datum{
				{types.NewStringDatum("xyz"), types.NewStringDatum("+>")},
				{types.NewStringDatum("|+|\n"), types.NewStringDatum("\r")},
			},
		},
	}
	runTestCasesCSV(t, &cfg, 1, testCases)
}

func TestReadUntilTerminator(t *testing.T) {
	cfg := config.MydumperRuntime{
		CSV: config.CSVConfig{
			Separator:  "#",
			Terminator: "#\n",
		},
	}
	parser, err := mydump.NewCSVParser(
		context.Background(),
		&cfg.CSV,
		mydump.NewStringReader("xxx1#2#3#4#\n56#78"),
		int64(config.ReadBlockSize),
		ioWorkersForCSV,
		false,
		nil,
	)
	require.NoError(t, err)
	content, idx, err := parser.ReadUntilTerminator()
	require.NoError(t, err)
	require.Equal(t, "xxx1#2#3#4#\n", string(content))
	require.Equal(t, int64(12), idx)
	content, idx, err = parser.ReadUntilTerminator()
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, "56#78", string(content))
	require.Equal(t, int64(17), idx)
}

func TestNULL(t *testing.T) {
	// https://dev.mysql.com/doc/refman/8.0/en/load-data.html
	// - For the default FIELDS and LINES values, NULL is written as a field value of \N for output, and a field value of \N is read as NULL for input (assuming that the ESCAPED BY character is \).
	// - If FIELDS ENCLOSED BY is not empty, a field containing the literal word NULL as its value is read as a NULL value. This differs from the word NULL enclosed within FIELDS ENCLOSED BY characters, which is read as the string 'NULL'.
	// - If FIELDS ESCAPED BY is empty, NULL is written as the word NULL.

	cfg := config.MydumperRuntime{
		CSV: config.CSVConfig{
			Separator:        ",",
			Delimiter:        `"`,
			Terminator:       "\n",
			Null:             []string{`\N`, `NULL`},
			EscapedBy:        `\`,
			QuotedNullIsText: true,
		},
	}
	testCases := []testCase{
		{
			input: `NULL,"NULL"
\N,"\N"
\\N,"\\N"`,
			expected: [][]types.Datum{
				{nullDatum, types.NewStringDatum("NULL")},
				{nullDatum, nullDatum},
				{types.NewStringDatum(`\N`), types.NewStringDatum(`\N`)},
			},
		},
	}
	runTestCasesCSV(t, &cfg, 1, testCases)

	cfg = config.MydumperRuntime{
		CSV: config.CSVConfig{
			Separator:  ",",
			Delimiter:  ``,
			Terminator: "\n",
			Null:       []string{`\N`},
			EscapedBy:  `\`,
		},
	}
	testCases = []testCase{
		{
			input: `NULL,"NULL"
\N,"\N"
\\N,"\\N"`,
			expected: [][]types.Datum{
				{types.NewStringDatum("NULL"), types.NewStringDatum(`"NULL"`)},
				{nullDatum, types.NewStringDatum(`"N"`)},
				{types.NewStringDatum(`\N`), types.NewStringDatum(`"\N"`)},
			},
		},
	}
	runTestCasesCSV(t, &cfg, 1, testCases)

	cfg = config.MydumperRuntime{
		CSV: config.CSVConfig{
			Separator:  ",",
			Delimiter:  ``,
			Terminator: "\n",
			Null:       []string{`\N`},
			EscapedBy:  `\`,
		},
	}
	testCases = []testCase{
		{
			input: `NULL,"NULL"
\N,"\N"
\\N,"\\N"`,
			expected: [][]types.Datum{
				{types.NewStringDatum("NULL"), types.NewStringDatum(`"NULL"`)},
				{nullDatum, types.NewStringDatum(`"N"`)},
				{types.NewStringDatum(`\N`), types.NewStringDatum(`"\N"`)},
			},
		},
	}
	runTestCasesCSV(t, &cfg, 1, testCases)

	cfg = config.MydumperRuntime{
		CSV: config.CSVConfig{
			Separator:        ",",
			Delimiter:        `"`,
			Terminator:       "\n",
			Null:             []string{`NULL`},
			EscapedBy:        ``,
			QuotedNullIsText: true,
		},
	}
	testCases = []testCase{
		{
			input: `NULL,"NULL"
\N,"\N"
\\N,"\\N"`,
			expected: [][]types.Datum{
				{nullDatum, types.NewStringDatum(`NULL`)},
				{types.NewStringDatum(`\N`), types.NewStringDatum(`\N`)},
				{types.NewStringDatum(`\\N`), types.NewStringDatum(`\\N`)},
			},
		},
	}
	runTestCasesCSV(t, &cfg, 1, testCases)
}

func TestStartingBy(t *testing.T) {
	cfg := config.MydumperRuntime{
		CSV: config.CSVConfig{
			Separator:  ",",
			Delimiter:  `"`,
			Terminator: "\n",
			StartingBy: "xxx",
		},
	}
	testCases := []testCase{
		{
			input: `xxx"abc",1
something xxx"def",2
"ghi",3`,
			expected: [][]types.Datum{
				{types.NewStringDatum("abc"), types.NewStringDatum("1")},
				{types.NewStringDatum("def"), types.NewStringDatum("2")},
			},
		},
	}
	runTestCasesCSV(t, &cfg, 1, testCases)

	testCases = []testCase{
		{
			input: `xxxabc,1
something xxxdef,2
ghi,3
"bad syntax"aaa`,
			expected: [][]types.Datum{
				{types.NewStringDatum("abc"), types.NewStringDatum("1")},
				{types.NewStringDatum("def"), types.NewStringDatum("2")},
			},
		},
	}
	runTestCasesCSV(t, &cfg, 1, testCases)

	// test that special characters appears before StartingBy, and StartingBy only takes effect after once

	testCases = []testCase{
		{
			input: `xxx"abc",1
something xxxdef,2
"ghi",3
"yyy"xxx"yyy",4
"yyy",5,xxxyyy,5
qwe,zzzxxxyyy,6
"yyyxxx"yyyxxx",7
yyy",5,xxxxxx,8
`,
			expected: [][]types.Datum{
				{types.NewStringDatum("abc"), types.NewStringDatum("1")},
				{types.NewStringDatum("def"), types.NewStringDatum("2")},
				{types.NewStringDatum("yyy"), types.NewStringDatum("4")},
				{types.NewStringDatum("yyy"), types.NewStringDatum("5")},
				{types.NewStringDatum("yyy"), types.NewStringDatum("6")},
				{types.NewStringDatum("yyyxxx"), types.NewStringDatum("7")},
				{types.NewStringDatum("xxx"), types.NewStringDatum("8")},
			},
		},
	}
	runTestCasesCSV(t, &cfg, 1, testCases)

	// test StartingBy contains special characters

	cfg = config.MydumperRuntime{
		CSV: config.CSVConfig{
			Separator:  ",",
			Delimiter:  `"`,
			Terminator: "\n",
			StartingBy: "x,xx",
		},
	}
	testCases = []testCase{
		{
			input: `x,xx"abc",1
something x,xxdef,2
"ghi",3
"yyy"xxx"yyy",4
"yyy",5,xxxyyy,5
qwe,zzzxxxyyy,6
"yyyxxx"yyyxxx",7
yyy",5,xx,xxxx,8`,
			expected: [][]types.Datum{
				{types.NewStringDatum("abc"), types.NewStringDatum("1")},
				{types.NewStringDatum("def"), types.NewStringDatum("2")},
				{types.NewStringDatum("xx"), types.NewStringDatum("8")},
			},
		},
	}
	runTestCasesCSV(t, &cfg, 1, testCases)

	cfg = config.MydumperRuntime{
		CSV: config.CSVConfig{
			Separator:  ",",
			Delimiter:  `"`,
			Terminator: "\n",
			StartingBy: `x"xx`,
		},
	}
	testCases = []testCase{
		{
			input: `x"xx"abc",1
something x"xxdef,2
"ghi",3
"yyy"xxx"yyy",4
"yyy",5,xxxyyy,5
qwe,zzzxxxyyy,6
"yyyxxx"yyyxxx",7
yyy",5,xx"xxxx,8
`,
			expected: [][]types.Datum{
				{types.NewStringDatum("abc"), types.NewStringDatum("1")},
				{types.NewStringDatum("def"), types.NewStringDatum("2")},
				{types.NewStringDatum("xx"), types.NewStringDatum("8")},
			},
		},
	}
	runTestCasesCSV(t, &cfg, 1, testCases)

	cfg = config.MydumperRuntime{
		CSV: config.CSVConfig{
			Separator:  ",",
			Delimiter:  `"`,
			Terminator: "\n",
			StartingBy: "x\nxx",
		},
	}
	_, err := mydump.NewCSVParser(context.Background(), &cfg.CSV, nil, 1, ioWorkersForCSV, false, nil)
	require.ErrorContains(t, err, "STARTING BY 'x\nxx' cannot contain LINES TERMINATED BY '\n'")
}

func TestCharsetConversion(t *testing.T) {
	cfg := config.MydumperRuntime{
		CSV: config.CSVConfig{
			Separator:  "，",
			Terminator: "。\n",
		},
		DataCharacterSet:       "gb18030",
		DataInvalidCharReplace: string(utf8.RuneError),
	}
	charsetConvertor, err := mydump.NewCharsetConvertor(cfg.DataCharacterSet, cfg.DataInvalidCharReplace)
	require.NoError(t, err)
	originalInputPart1 := `不要温驯地走进那个良夜，老年应当在日暮时燃烧咆哮，怒斥，怒斥光明的消逝。
`
	originalInputPart2 := `虽然智慧的人临终时懂得黑暗有理，因为他们的话没有迸发出闪电，他们也并不温驯地走进那个良夜。
`
	// Insert an invalid char to test DataInvalidCharReplace.
	rawInput, err := charsetConvertor.Encode(originalInputPart1 + string([]byte{0x99}) + originalInputPart2)
	require.NoError(t, err)

	testCases := []testCase{
		{
			input: rawInput,
			expected: [][]types.Datum{
				{types.NewStringDatum("不要温驯地走进那个良夜"),
					types.NewStringDatum("老年应当在日暮时燃烧咆哮"),
					types.NewStringDatum("怒斥"),
					types.NewStringDatum("怒斥光明的消逝")},
				{types.NewStringDatum(cfg.DataInvalidCharReplace + "虽然智慧的人临终时懂得黑暗有理"),
					types.NewStringDatum("因为他们的话没有迸发出闪电"),
					types.NewStringDatum("他们也并不温驯地走进那个良夜")},
			},
		},
	}

	runTestCasesCSV(t, &cfg, 1, testCases)
}

// Run `go test github.com/pingcap/pkg/lightning/mydump -check.b -check.bmem -test.v` to get benchmark result.
// Please ensure your temporary storage has (c.N / 2) KiB of free space.

type benchCSVParserSuite struct {
	csvPath   string
	ioWorkers *worker.Pool
}

func newBenchCSVParserSuite(b *testing.B) *benchCSVParserSuite {
	var s benchCSVParserSuite
	s.ioWorkers = worker.NewPool(context.Background(), 5, "bench_csv")
	dir := b.TempDir()
	s.csvPath = filepath.Join(dir, "input.csv")
	file, err := os.Create(s.csvPath)
	require.NoError(b, err)
	defer func() {
		require.NoError(b, file.Close())
	}()
	for i := 0; i < b.N; i++ {
		_, err = file.WriteString("18,1,1,0.3650,GC,BARBARBAR,rw9AOV1AjoI1,50000.00,-10.00,10.00,1,1,djj3Q2XaIPoYVy1FuF,gc80Q2o82Au3C9xv,PYOolSxG3w,DI,265111111,7586538936787184,2020-02-26 20:06:00.193,OE,YCkSPBVqoJ2V5F8zWs87V5XzbaIY70aWCD4dgcB6bjUzCr5wOJCJ2TYH49J7yWyysbudJIxlTAEWSJahY7hswLtTsqyjEkrlsN8iDMAa9Poj29miJ08tnn2G8mL64IlyywvnRGbLbyGvWDdrOSF42RyUFTWVyqlDWc6Gr5wyMPYgvweKemzFDVD3kro5JsmBmJY08EK54nQoyfo2sScyb34zcM9GFo9ZQTwloINfPYQKXQm32m0XvU7jiNmYpFTFJQjdqA825SEvQqMMefG2WG4jVu9UPdhdUjRsFRd0Gw7YPKByOlcuY0eKxT7sAzMKXx2000RR6dqHNXe47oVYd\n")
		require.NoError(b, err)
	}
	return &s
}

func BenchmarkReadRowUsingMydumpCSVParser(b *testing.B) {
	s := newBenchCSVParserSuite(b)

	file, err := os.Open(s.csvPath)
	require.NoError(b, err)
	defer func() {
		require.NoError(b, file.Close())
	}()

	cfg := config.CSVConfig{Separator: ","}
	parser, err := mydump.NewCSVParser(context.Background(), &cfg, file, 65536, ioWorkersForCSV, false, nil)
	require.NoError(b, err)
	parser.SetLogger(log.Logger{Logger: zap.NewNop()})

	rowsCount := 0
	for {
		err := parser.ReadRow()
		if err == nil {
			parser.RecycleRow(parser.LastRow())
			rowsCount++
			continue
		}
		if errors.Cause(err) == io.EOF {
			break
		}
		b.Fatal(err)
	}
	require.Equal(b, b.N, rowsCount)
}

func BenchmarkReadRowUsingEncodingCSV(b *testing.B) {
	s := newBenchCSVParserSuite(b)

	file, err := os.Open(s.csvPath)
	require.NoError(b, err)
	defer func() {
		require.Nil(b, file.Close())
	}()

	csvParser := csv.NewReader(file)

	rowsCount := 0
	var datums []types.Datum
	for {
		records, err := csvParser.Read()
		if err == nil {
			// for fair comparison, we need to include the cost of conversion to Datum.
			for _, record := range records {
				datums = append(datums, types.NewStringDatum(record))
			}
			datums = datums[:0]
			rowsCount++
			continue
		}
		if errors.Cause(err) == io.EOF {
			break
		}
		b.Fatal(err)
	}
	require.Equal(b, b.N, rowsCount)
}

func TestHeaderSchemaMatch(t *testing.T) {
	cfg := config.MydumperRuntime{
		CSV: config.CSVConfig{
			Separator: ",",
			Delimiter: `"`,
		},
	}

	inputData := `id,val1,val2,val3
1,111,aaa,1.0
2,222,bbb,2.0
3,333,ccc,3.0
4,444,ddd,4.0`

	parsedDataPart := [][]types.Datum{
		{types.NewStringDatum("1"), types.NewStringDatum("111"), types.NewStringDatum("aaa"), types.NewStringDatum("1.0")},
		{types.NewStringDatum("2"), types.NewStringDatum("222"), types.NewStringDatum("bbb"), types.NewStringDatum("2.0")},
		{types.NewStringDatum("3"), types.NewStringDatum("333"), types.NewStringDatum("ccc"), types.NewStringDatum("3.0")},
		{types.NewStringDatum("4"), types.NewStringDatum("444"), types.NewStringDatum("ddd"), types.NewStringDatum("4.0")},
	}

	type testCase struct {
		Header            bool
		HeaderSchemaMatch bool
		ExpectedData      [][]types.Datum
		ExpectedColumns   []string
	}

	for _, tc := range []testCase{
		{
			Header:            true,
			HeaderSchemaMatch: true,
			ExpectedData:      parsedDataPart,
			ExpectedColumns:   []string{"id", "val1", "val2", "val3"},
		},
		{
			Header:            true,
			HeaderSchemaMatch: false,
			ExpectedData:      parsedDataPart,
			ExpectedColumns:   nil,
		},
		{
			Header:            false,
			HeaderSchemaMatch: true,
			ExpectedData: append([][]types.Datum{
				{types.NewStringDatum("id"), types.NewStringDatum("val1"), types.NewStringDatum("val2"), types.NewStringDatum("val3")},
			}, parsedDataPart...),
			ExpectedColumns: nil,
		},
	} {
		comment := fmt.Sprintf("header = %v, header-schema-match = %v", tc.Header, tc.HeaderSchemaMatch)
		cfg.CSV.Header = tc.Header
		cfg.CSV.HeaderSchemaMatch = tc.HeaderSchemaMatch
		charsetConvertor, err := mydump.NewCharsetConvertor(cfg.DataCharacterSet, cfg.DataInvalidCharReplace)
		assert.NoError(t, err)
		parser, err := mydump.NewCSVParser(context.Background(), &cfg.CSV, mydump.NewStringReader(inputData), int64(config.ReadBlockSize), ioWorkersForCSV, tc.Header, charsetConvertor)
		assert.NoError(t, err)
		for i, row := range tc.ExpectedData {
			comment := fmt.Sprintf("row = %d, header = %v, header-schema-match = %v", i+1, tc.Header, tc.HeaderSchemaMatch)
			e := parser.ReadRow()
			assert.NoErrorf(t, e, "row = %d, error = %s", i+1, errors.ErrorStack(e))
			assert.Equal(t, int64(i)+1, parser.LastRow().RowID, comment)
			assert.Equal(t, row, parser.LastRow().Row, comment)
		}
		assert.ErrorIsf(t, errors.Cause(parser.ReadRow()), io.EOF, comment)
		assert.Equal(t, tc.ExpectedColumns, parser.Columns(), comment)
	}
}
