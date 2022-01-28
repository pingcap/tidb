package mydump_test

import (
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
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/lightning/worker"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var ioWorkers = worker.NewPool(context.Background(), 5, "test_csv")

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
		parser, err := mydump.NewCSVParser(&cfg.CSV, mydump.NewStringReader(tc.input), blockBufSize, ioWorkers, false, charsetConvertor)
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
		parser, err := mydump.NewCSVParser(&cfg.CSV, mydump.NewStringReader(tc), blockBufSize, ioWorkers, false, charsetConvertor)
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

	parser, err := mydump.NewCSVParser(&cfg, reader, int64(config.ReadBlockSize), ioWorkers, false, nil)
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
		parser, err := mydump.NewCSVParser(&cfg, reader, int64(config.ReadBlockSize), ioWorkers, false, nil)
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

	parser, err := mydump.NewCSVParser(&cfg, mydump.NewStringReader("aaa,bbb,ccc\nzzz,yyy,xxx\n"), int64(config.ReadBlockSize), ioWorkers, false, nil)
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

	parser, err = mydump.NewCSVParser(&cfg, mydump.NewStringReader("aaa,bbb,ccc\nzzz,yyy,xxx"), int64(config.ReadBlockSize), ioWorkers, false, nil)
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

	parser, err = mydump.NewCSVParser(&cfg, mydump.NewStringReader(`"aaa","bbb","ccc"`+"\nzzz,yyy,xxx"), int64(config.ReadBlockSize), ioWorkers, false, nil)
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

	parser, err = mydump.NewCSVParser(&cfg, mydump.NewStringReader(`"aaa","b
bb","ccc"
zzz,yyy,xxx`), int64(config.ReadBlockSize), ioWorkers, false, nil)
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

	parser, err = mydump.NewCSVParser(&cfg, mydump.NewStringReader(`"aaa","b""bb","ccc"`), int64(config.ReadBlockSize), ioWorkers, false, nil)
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
		Separator:       ",",
		Delimiter:       `"`,
		BackslashEscape: true,
		NotNull:         false,
		Null:            `\N`,
	}

	parser, err := mydump.NewCSVParser(&cfg, mydump.NewStringReader(`"\"","\\","\?"
"\
",\N,\\N`), int64(config.ReadBlockSize), ioWorkers, false, nil)
	require.NoError(t, err)

	require.Nil(t, parser.ReadRow())
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

	require.Nil(t, parser.ReadRow())
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
}

func TestSyntaxErrorCSV(t *testing.T) {
	cfg := config.MydumperRuntime{
		CSV: config.CSVConfig{
			Separator:       ",",
			Delimiter:       `"`,
			BackslashEscape: true,
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

	cfg.CSV.BackslashEscape = false
	runFailingTestCasesCSV(t, &cfg, int64(config.ReadBlockSize), []string{`"\`})
}

func TestTSV(t *testing.T) {
	cfg := config.CSVConfig{
		Separator:       "\t",
		Delimiter:       "",
		BackslashEscape: false,
		NotNull:         false,
		Null:            "",
		Header:          true,
	}

	parser, err := mydump.NewCSVParser(&cfg, mydump.NewStringReader(`a	b	c	d	e	f
0				foo	0000-00-00
0				foo	0000-00-00
0	abc	def	ghi	bar	1999-12-31`), int64(config.ReadBlockSize), ioWorkers, true, nil)
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
	}
	data := " \r\n\r\n0,,abc\r\n \r\n123,1999-12-31,test\r\n"
	parser, err := mydump.NewCSVParser(&cfg, mydump.NewStringReader(data), int64(config.ReadBlockSize), ioWorkers, false, nil)
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
	data = " \r\na,b,c\r\n0,,abc\r\n"
	parser, err = mydump.NewCSVParser(&cfg, mydump.NewStringReader(data), int64(config.ReadBlockSize), ioWorkers, true, nil)
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

	parser, err := mydump.NewCSVParser(&cfg, mydump.NewStringReader(""), int64(config.ReadBlockSize), ioWorkers, false, nil)
	require.NoError(t, err)
	require.ErrorIs(t, errors.Cause(parser.ReadRow()), io.EOF)

	// Try again with headers.

	cfg.Header = true

	parser, err = mydump.NewCSVParser(&cfg, mydump.NewStringReader(""), int64(config.ReadBlockSize), ioWorkers, true, nil)
	require.NoError(t, err)
	require.ErrorIs(t, errors.Cause(parser.ReadRow()), io.EOF)

	parser, err = mydump.NewCSVParser(&cfg, mydump.NewStringReader("h\n"), int64(config.ReadBlockSize), ioWorkers, true, nil)
	require.NoError(t, err)
	require.ErrorIs(t, errors.Cause(parser.ReadRow()), io.EOF)
}

func TestCRLF(t *testing.T) {
	cfg := config.CSVConfig{
		Separator: ",",
		Delimiter: `"`,
	}
	parser, err := mydump.NewCSVParser(&cfg, mydump.NewStringReader("a\rb\r\nc\n\n\n\nd"), int64(config.ReadBlockSize), ioWorkers, false, nil)
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

	parser, err := mydump.NewCSVParser(&cfg, mydump.NewStringReader(`",",','`), int64(config.ReadBlockSize), ioWorkers, false, nil)
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
			Separator:       ",",
			Delimiter:       `"`,
			BackslashEscape: true,
			TrimLastSep:     true,
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

	parser, err := mydump.NewCSVParser(&cfg, &errorReader{}, int64(config.ReadBlockSize), ioWorkers, false, nil)
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
	parser, err := mydump.NewCSVParser(&cfg.CSV, tc, 50000, ioWorkers, false, nil)
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
		&cfg.CSV,
		mydump.NewStringReader("123,456,789,\r\na,b,,\r\n,,,\r\n\"a\",\"\",\"\",\r\n"),
		int64(config.ReadBlockSize),
		ioWorkers,
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

// Run `go test github.com/pingcap/br/pkg/lightning/mydump -check.b -check.bmem -test.v` to get benchmark result.
// Please ensure your temporary storage has (c.N / 2) KiB of free space.

type benchCSVParserSuite struct {
	csvPath   string
	ioWorkers *worker.Pool
}

func newBenchCSVParserSuite(b *testing.B) (*benchCSVParserSuite, func()) {
	var s benchCSVParserSuite
	s.ioWorkers = worker.NewPool(context.Background(), 5, "bench_csv")
	dir, err := os.MkdirTemp("", "bench_csv")
	require.NoError(b, err)
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
	return &s, func() {
		require.NoError(b, os.RemoveAll(dir))
	}
}

func BenchmarkReadRowUsingMydumpCSVParser(b *testing.B) {
	s, clean := newBenchCSVParserSuite(b)
	defer clean()

	file, err := os.Open(s.csvPath)
	require.NoError(b, err)
	defer func() {
		require.NoError(b, file.Close())
	}()

	cfg := config.CSVConfig{Separator: ","}
	parser, err := mydump.NewCSVParser(&cfg, file, 65536, ioWorkers, false, nil)
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
	s, clean := newBenchCSVParserSuite(b)
	defer clean()

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
