// Copyright 2019 PingCAP, Inc.
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
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func runTestCases(t *testing.T, mode mysql.SQLMode, blockBufSize int64, cases []testCase) {
	for _, tc := range cases {
		parser := mydump.NewChunkParser(context.Background(), mode, mydump.NewStringReader(tc.input), blockBufSize, ioWorkersForCSV)
		for i, row := range tc.expected {
			e := parser.ReadRow()
			comment := fmt.Sprintf("input = %q, row = %d, err = %s", tc.input, i+1, errors.ErrorStack(e))
			assert.NoError(t, e, comment)
			assert.Equal(t, int64(i)+1, parser.LastRow().RowID, comment)
			assert.Equal(t, row, parser.LastRow().Row, comment)
		}
		assert.ErrorIsf(t, errors.Cause(parser.ReadRow()), io.EOF, "input = %q", tc.input)
	}
}

func runFailingTestCases(t *testing.T, mode mysql.SQLMode, blockBufSize int64, cases []string) {
	for _, tc := range cases {
		parser := mydump.NewChunkParser(context.Background(), mode, mydump.NewStringReader(tc), blockBufSize, ioWorkersForCSV)
		assert.Regexpf(t, "syntax error.*", parser.ReadRow().Error(), "input = %q", tc)
	}
}

func TestReadRow(t *testing.T) {
	reader := mydump.NewStringReader(
		"/* whatever pragmas */;" +
			"INSERT INTO `namespaced`.`table` (columns, more, columns) VALUES (1,-2, 3),\n(4,5., 6);" +
			"INSERT `namespaced`.`table` (x,y,z) VALUES (7,8,9);" +
			"insert another_table values (10,11e1,12, '(13)', '(', 14, ')');",
	)

	parser := mydump.NewChunkParser(context.Background(), mysql.ModeNone, reader, int64(config.ReadBlockSize), ioWorkersForCSV)

	require.NoError(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 1,
		Row: []types.Datum{
			types.NewUintDatum(1),
			types.NewIntDatum(-2),
			types.NewUintDatum(3),
		},
		Length: 62,
	}, parser.LastRow())
	require.Equal(t, []string{"columns", "more", "columns"}, parser.Columns())
	offset, rowID := parser.Pos()
	require.Equal(t, int64(97), offset)
	require.Equal(t, int64(1), rowID)

	require.NoError(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 2,
		Row: []types.Datum{
			types.NewUintDatum(4),
			types.NewStringDatum("5."),
			types.NewUintDatum(6),
		},
		Length: 6,
	}, parser.LastRow())
	require.Equal(t, []string{"columns", "more", "columns"}, parser.Columns())
	offset, rowID = parser.Pos()
	require.Equal(t, int64(108), offset)
	require.Equal(t, int64(2), rowID)

	require.NoError(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 3,
		Row: []types.Datum{
			types.NewUintDatum(7),
			types.NewUintDatum(8),
			types.NewUintDatum(9),
		},
		Length: 42,
	}, parser.LastRow())
	require.Equal(t, []string{"x", "y", "z"}, parser.Columns())
	offset, rowID = parser.Pos()
	require.Equal(t, int64(159), offset)
	require.Equal(t, int64(3), rowID)

	require.NoError(t, parser.ReadRow())
	require.Equal(t, mydump.Row{
		RowID: 4,
		Row: []types.Datum{
			types.NewUintDatum(10),
			types.NewStringDatum("11e1"),
			types.NewUintDatum(12),
			types.NewStringDatum("(13)"),
			types.NewStringDatum("("),
			types.NewUintDatum(14),
			types.NewStringDatum(")"),
		},
		Length: 49,
	}, parser.LastRow())
	require.Nil(t, parser.Columns())
	offset, rowID = parser.Pos()
	require.Equal(t, int64(222), offset)
	require.Equal(t, int64(4), rowID)

	require.ErrorIs(t, errors.Cause(parser.ReadRow()), io.EOF)
}

func TestReadChunks(t *testing.T) {
	reader := mydump.NewStringReader(`
		INSERT foo VALUES (1,2,3,4),(5,6,7,8),(9,10,11,12);
		INSERT foo VALUES (13,14,15,16),(17,18,19,20),(21,22,23,24),(25,26,27,28);
		INSERT foo VALUES (29,30,31,32),(33,34,35,36);
	`)

	parser := mydump.NewChunkParser(context.Background(), mysql.ModeNone, reader, int64(config.ReadBlockSize), ioWorkersForCSV)

	chunks, err := mydump.ReadChunks(parser, 32)
	require.NoError(t, err)
	require.Equal(t, []mydump.Chunk{
		{
			Offset:       0,
			EndOffset:    40,
			PrevRowIDMax: 0,
			RowIDMax:     2,
		},
		{
			Offset:       40,
			EndOffset:    88,
			PrevRowIDMax: 2,
			RowIDMax:     4,
		},
		{
			Offset:       88,
			EndOffset:    130,
			PrevRowIDMax: 4,
			RowIDMax:     7,
		},
		{
			Offset:       130,
			EndOffset:    165,
			PrevRowIDMax: 7,
			RowIDMax:     8,
		},
		{
			Offset:       165,
			EndOffset:    179,
			PrevRowIDMax: 8,
			RowIDMax:     9,
		},
	}, chunks)
}

func TestNestedRow(t *testing.T) {
	reader := mydump.NewStringReader(`
		INSERT INTO exam_detail VALUES
		("123",CONVERT("{}" USING UTF8MB4)),
		("456",CONVERT("{\"a\":4}" USING UTF8MB4)),
		("789",CONVERT("[]" USING UTF8MB4));
	`)

	parser := mydump.NewChunkParser(context.Background(), mysql.ModeNone, reader, int64(config.ReadBlockSize), ioWorkersForCSV)
	chunks, err := mydump.ReadChunks(parser, 96)

	require.NoError(t, err)
	require.Equal(t, []mydump.Chunk{
		{
			Offset:       0,
			EndOffset:    117,
			PrevRowIDMax: 0,
			RowIDMax:     2,
		},
		{
			Offset:       117,
			EndOffset:    156,
			PrevRowIDMax: 2,
			RowIDMax:     3,
		},
	}, chunks)
}

func TestVariousSyntax(t *testing.T) {
	testCases := []testCase{
		{
			input:    "INSERT INTO foobar VALUES (1, 2);",
			expected: [][]types.Datum{{types.NewUintDatum(1), types.NewUintDatum(2)}},
		},
		{
			input:    "INSERT INTO `foobar` VALUES (3, 4);",
			expected: [][]types.Datum{{types.NewUintDatum(3), types.NewUintDatum(4)}},
		},
		{
			input:    `INSERT INTO "foobar" VALUES (5, 6);`,
			expected: [][]types.Datum{{types.NewUintDatum(5), types.NewUintDatum(6)}},
		},
		{
			input: `(7, -8, Null, '9'), (b'10', 0b11, 0x12, x'13'), ("14", True, False, 0)`,
			expected: [][]types.Datum{
				{
					types.NewUintDatum(7),
					types.NewIntDatum(-8),
					nullDatum,
					types.NewStringDatum("9"),
				},
				{
					types.NewBinaryLiteralDatum(types.BinaryLiteral([]byte{2})),
					types.NewBinaryLiteralDatum(types.BinaryLiteral([]byte{3})),
					types.NewBinaryLiteralDatum(types.BinaryLiteral([]byte{0x12})),
					types.NewBinaryLiteralDatum(types.BinaryLiteral([]byte{0x13})),
				},
				{
					types.NewStringDatum("14"),
					types.NewIntDatum(1),
					types.NewIntDatum(0),
					types.NewUintDatum(0),
				},
			},
		},
		{
			input: `
				(.15, 1.6, 17.),
				(.18e1, 1.9e1, 20.e1), (.21e-1, 2.2e-1, 23.e-1), (.24e+1, 2.5e+1, 26.e+1),
				(-.27, -2.8, -29.),
				(-.30e1, -3.1e1, -32.e1), (-.33e-1, -3.4e-1, -35.e-1), (-.36e+1, -3.7e+1, -38.e+1),
				(1e39, 1e-40, 1e+41),
				(.42E1, 4.3E1, 44.E1), (.45E-1, 4.6E-1, 47.E-1), (.48E+1, 4.9E+1, 50.E+1),
				(-.51E1, -5.2E1, -53.E1), (-.54E-1, -5.5E-1, -56.E-1), (-.57E+1, -5.8E+1, -59.E+1),
				(1E60, 1E-61, 1E+62),
				(6.33333333333333333333333333333333333333333333, -6.44444444444444444444444444444444444444444444, -0.0),
				(65555555555555555555555555555555555555555555.5, -66666666666666666666666666666666666666666666.6, 0.0)
			`,
			expected: [][]types.Datum{
				{types.NewStringDatum(".15"), types.NewStringDatum("1.6"), types.NewStringDatum("17.")},
				{types.NewStringDatum(".18e1"), types.NewStringDatum("1.9e1"), types.NewStringDatum("20.e1")},
				{types.NewStringDatum(".21e-1"), types.NewStringDatum("2.2e-1"), types.NewStringDatum("23.e-1")},
				{types.NewStringDatum(".24e+1"), types.NewStringDatum("2.5e+1"), types.NewStringDatum("26.e+1")},
				{types.NewStringDatum("-.27"), types.NewStringDatum("-2.8"), types.NewStringDatum("-29.")},
				{types.NewStringDatum("-.30e1"), types.NewStringDatum("-3.1e1"), types.NewStringDatum("-32.e1")},
				{types.NewStringDatum("-.33e-1"), types.NewStringDatum("-3.4e-1"), types.NewStringDatum("-35.e-1")},
				{types.NewStringDatum("-.36e+1"), types.NewStringDatum("-3.7e+1"), types.NewStringDatum("-38.e+1")},
				{types.NewStringDatum("1e39"), types.NewStringDatum("1e-40"), types.NewStringDatum("1e+41")},
				{types.NewStringDatum(".42E1"), types.NewStringDatum("4.3E1"), types.NewStringDatum("44.E1")},
				{types.NewStringDatum(".45E-1"), types.NewStringDatum("4.6E-1"), types.NewStringDatum("47.E-1")},
				{types.NewStringDatum(".48E+1"), types.NewStringDatum("4.9E+1"), types.NewStringDatum("50.E+1")},
				{types.NewStringDatum("-.51E1"), types.NewStringDatum("-5.2E1"), types.NewStringDatum("-53.E1")},
				{types.NewStringDatum("-.54E-1"), types.NewStringDatum("-5.5E-1"), types.NewStringDatum("-56.E-1")},
				{types.NewStringDatum("-.57E+1"), types.NewStringDatum("-5.8E+1"), types.NewStringDatum("-59.E+1")},
				{types.NewStringDatum("1E60"), types.NewStringDatum("1E-61"), types.NewStringDatum("1E+62")},
				{
					types.NewStringDatum("6.33333333333333333333333333333333333333333333"),
					types.NewStringDatum("-6.44444444444444444444444444444444444444444444"),
					types.NewStringDatum("-0.0"),
				},
				{
					types.NewStringDatum("65555555555555555555555555555555555555555555.5"),
					types.NewStringDatum("-66666666666666666666666666666666666666666666.6"),
					types.NewStringDatum("0.0"),
				},
			},
		},
		{
			input: `
				(0x123456ABCDEFabcdef, 0xABCDEF123456, 0xabcdef123456, 0x123),
				(x'123456ABCDEFabcdef', x'ABCDEF123456', x'abcdef123456', x''),
				(X'123456ABCDEFabcdef', X'ABCDEF123456', X'abcdef123456', X''),
				(
					0b101010101010101010101010101010101010101010101010101010101010101010,
					b'010101010101010101010101010101010101010101010101010101010101010101',
					B'110011001100110011001100110011001100110011001100110011001100',
					b'',
					B''
				)
			`,
			expected: [][]types.Datum{
				{
					types.NewBinaryLiteralDatum(types.BinaryLiteral([]byte{0x12, 0x34, 0x56, 0xab, 0xcd, 0xef, 0xab, 0xcd, 0xef})),
					types.NewBinaryLiteralDatum(types.BinaryLiteral([]byte{0xab, 0xcd, 0xef, 0x12, 0x34, 0x56})),
					types.NewBinaryLiteralDatum(types.BinaryLiteral([]byte{0xab, 0xcd, 0xef, 0x12, 0x34, 0x56})),
					types.NewBinaryLiteralDatum(types.BinaryLiteral([]byte{0x01, 0x23})),
				},
				{
					types.NewBinaryLiteralDatum(types.BinaryLiteral([]byte{0x12, 0x34, 0x56, 0xab, 0xcd, 0xef, 0xab, 0xcd, 0xef})),
					types.NewBinaryLiteralDatum(types.BinaryLiteral([]byte{0xab, 0xcd, 0xef, 0x12, 0x34, 0x56})),
					types.NewBinaryLiteralDatum(types.BinaryLiteral([]byte{0xab, 0xcd, 0xef, 0x12, 0x34, 0x56})),
					types.NewBinaryLiteralDatum(types.BinaryLiteral([]byte{})),
				},
				{
					types.NewBinaryLiteralDatum(types.BinaryLiteral([]byte{0x12, 0x34, 0x56, 0xab, 0xcd, 0xef, 0xab, 0xcd, 0xef})),
					types.NewBinaryLiteralDatum(types.BinaryLiteral([]byte{0xab, 0xcd, 0xef, 0x12, 0x34, 0x56})),
					types.NewBinaryLiteralDatum(types.BinaryLiteral([]byte{0xab, 0xcd, 0xef, 0x12, 0x34, 0x56})),
					types.NewBinaryLiteralDatum(types.BinaryLiteral([]byte{})),
				},
				{
					types.NewBinaryLiteralDatum(types.BinaryLiteral([]byte{0x02, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa})),
					types.NewBinaryLiteralDatum(types.BinaryLiteral([]byte{0x01, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55})),
					types.NewBinaryLiteralDatum(types.BinaryLiteral([]byte{0x0c, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc})),
					types.NewBinaryLiteralDatum(types.BinaryLiteral([]byte{})),
					types.NewBinaryLiteralDatum(types.BinaryLiteral([]byte{})),
				},
			},
		},
		{
			input:    "/* comment */; -- comment",
			expected: [][]types.Datum{},
		},
		{
			input: `
				-- comment /* ...
				insert into xxx -- comment
				values -- comment
				(true, false), -- comment
				(null, 00000); -- comment ... */
			`,
			expected: [][]types.Datum{
				{types.NewIntDatum(1), types.NewIntDatum(0)},
				{nullDatum, types.NewUintDatum(0)},
			},
		},
		{
			input:    `('\0\b\n\r\t\Z\'\a')`,
			expected: [][]types.Datum{{types.NewStringDatum("\x00\b\n\r\t\x1a'a")}},
		},
		{
			input:    `(CONVERT("[1,2,3]" USING UTF8MB4))`,
			expected: [][]types.Datum{{types.NewStringDatum("[1,2,3]")}},
		},
	}

	runTestCases(t, mysql.ModeNone, int64(config.ReadBlockSize), testCases)
}

func TestContinuation(t *testing.T) {
	testCases := []testCase{
		{
			input: `
				('FUZNtcGYegeXwnMRKtYnXtFhgnAMTzQHEBUTBehAFBQdPsnjHhRwRZhZLtEBsIDUFduzftskgxkYkPmEgvoirfIZRsARXjsdKwOc')
			`,
			expected: [][]types.Datum{
				{types.NewStringDatum("FUZNtcGYegeXwnMRKtYnXtFhgnAMTzQHEBUTBehAFBQdPsnjHhRwRZhZLtEBsIDUFduzftskgxkYkPmEgvoirfIZRsARXjsdKwOc")},
			},
		},
		{
			input: "INSERT INTO `report_case_high_risk` VALUES (2,'4','6',8,10);",
			expected: [][]types.Datum{
				{types.NewUintDatum(2), types.NewStringDatum("4"), types.NewStringDatum("6"), types.NewUintDatum(8), types.NewUintDatum(10)},
			},
		},
	}

	runTestCases(t, mysql.ModeNone, 1, testCases)
}

func TestPseudoKeywords(t *testing.T) {
	reader := mydump.NewStringReader(`
		INSERT INTO t (
			c, C,
			co, CO,
			con, CON,
			conv, CONV,
			conve, CONVE,
			conver, CONVER,
			convert, CONVERT,
			u, U,
			us, US,
			usi, USI,
			usin, USIN,
			ut, UT,
			utf, UTF,
			utf8, UTF8,
			utf8m, UTF8M,
			utf8mb, UTF8MB,
			utf8mb4, UTF8MB4,
			t, T,
			tr, TR,
			tru, TRU,
			f, F,
			fa, FA,
			fal, FAL,
			fals, FALS,
			n, N,
			nu, NU,
			nul, NUL,
			v, V,
			va, VA,
			val, VAL,
			valu, VALU,
			value, VALUE,
			i, I,
			ins, INS,
			inse, INSE,
			inser, INSER,
		) VALUES ();
	`)

	parser := mydump.NewChunkParser(context.Background(), mysql.ModeNone, reader, int64(config.ReadBlockSize), ioWorkersForCSV)
	require.NoError(t, parser.ReadRow())
	require.Equal(t, []string{
		"c", "c",
		"co", "co",
		"con", "con",
		"conv", "conv",
		"conve", "conve",
		"conver", "conver",
		"convert", "convert",
		"u", "u",
		"us", "us",
		"usi", "usi",
		"usin", "usin",
		"ut", "ut",
		"utf", "utf",
		"utf8", "utf8",
		"utf8m", "utf8m",
		"utf8mb", "utf8mb",
		"utf8mb4", "utf8mb4",
		"t", "t",
		"tr", "tr",
		"tru", "tru",
		"f", "f",
		"fa", "fa",
		"fal", "fal",
		"fals", "fals",
		"n", "n",
		"nu", "nu",
		"nul", "nul",
		"v", "v",
		"va", "va",
		"val", "val",
		"valu", "valu",
		"value", "value",
		"i", "i",
		"ins", "ins",
		"inse", "inse",
		"inser", "inser",
	}, parser.Columns())
}

func TestSyntaxError(t *testing.T) {
	inputs := []string{
		"('xxx)",
		`("xxx)`,
		"(`xxx)",
		"(/* xxx)",
		`('\')`,
		`("\")`,
		`('\)`,
		`("\)`,
		"(",
		"(1",
		"(1,",
		"(values)",
		"insert into e (f",
		"insert into e (3) values (4)",
		"insert into e ('3') values (4)",
		"insert into e (0x3) values (4)",
		"insert into e (x'3') values (4)",
		"insert into e (b'3') values (4)",
		"3",
		"(`values`)",
		"/* ...",
	}

	runFailingTestCases(t, mysql.ModeNone, int64(config.ReadBlockSize), inputs)
}

// Various syntax error cases collected via fuzzing.
// These cover most of the tokenizer branches.

func TestMoreSyntaxError(t *testing.T) {
	inputs := []string{
		" usin0",
		"- ",
		"-,",
		"-;",
		"-",
		"-(",
		"-/",
		"-\"",
		"-`",
		", '0\\0",
		",/*000",
		"; con0",
		";CONV0",
		";using UTF0",
		"''",
		"'",
		"'\\",
		"'\\\\",
		"'0''00",
		"'0'",
		"'0\\",
		"(''''0",
		"(''0'0",
		"(fals0",
		"(x'000",
		"*",
		"/",
		"/**",
		"/***",
		"/**0",
		"/*00*0",
		"/0",
		"\"",
		"\"\"",
		"\"\"\"0\\0",
		"\"\\",
		"\"\\\\",
		"\"0\"",
		"\"0\"\x00\"0",
		"\"0\\",
		"\"0000\"\"\"\"\"0",
		"\"00000",
		"\x00;",
		"\xd9/",
		"\xde0 b'0",
		"\xed00000",
		"``",
		"`````0",
		"0 ",
		"0-\"",
		"0,",
		"0;",
		"0",
		"0/",
		"0\"",
		"0\x00 CONVERT0",
		"0\x00\"\"C0",
		"0\x03\fFa0",
		"0`",
		"00 ",
		"00/",
		"00\"",
		"00`",
		"000\xf1/0",
		"00a t0",
		"00b b0",
		"00d f0",
		"00e u0",
		"00l 00",
		"00l v0",
		"00n -0",
		"00n Using UTF8M0",
		"00t using 0",
		"00t x0",
		"0a VA0",
		"0b ",
		"0b;",
		"0b'",
		"0b",
		"0b/",
		"0b\"",
		"0b`",
		"0b0000",
		"0by",
		"0O,CO0",
		"0r tr0",
		"0s us0",
		"0T``CONVER0",
		"0u\vnu0",
		"0x ",
		"0x;",
		"0x",
		"0x/",
		"0x\"",
		"0x\n",
		"0x\x00",
		"0x`",
		"0x0000",
		"6;",
		"a`00`0",
		"b ",
		"b,",
		"B;",
		"b'",
		"b",
		"b/",
		"b\"",
		"B\f",
		"b`",
		"b0",
		"C ",
		"C;",
		"C",
		"C/",
		"C\"",
		"C\n",
		"C`",
		"C0",
		"CO ",
		"CO;",
		"CO",
		"CO/",
		"CO\"",
		"CO\v",
		"CO`",
		"CON ",
		"CON;",
		"CON",
		"CON/",
		"CON\"",
		"CON\v",
		"CON`",
		"CON0",
		"CONV ",
		"CONV;",
		"CONv",
		"CONV/",
		"CONV\"",
		"CONV\v",
		"CONV`",
		"CONV0",
		"CONVE ",
		"CONVE;",
		"CONVE",
		"CONVE/",
		"CONVE\"",
		"CONVE\v",
		"CONVE`",
		"CONVE0",
		"CONVER ",
		"CONVER;",
		"CONVER",
		"CONVER/",
		"CONVER\"",
		"CONVER\n",
		"CONVER`",
		"CONVER0",
		"CONVERT ",
		"CONVERT;",
		"CONVERT",
		"CONVERT/",
		"CONVERT\"",
		"CONVERT\n",
		"CONVERT`",
		"CONVERT0",
		"e tru0",
		"f ",
		"f;",
		"F/",
		"f\"",
		"F\f",
		"f`",
		"fa ",
		"Fa;",
		"fa",
		"FA/",
		"FA\"",
		"Fa\f",
		"FA`",
		"fa0",
		"fal ",
		"fal;",
		"Fal",
		"fal/",
		"FAL\"",
		"FAL\n",
		"FAl`",
		"fal0",
		"fals ",
		"fals;",
		"fals",
		"fals/",
		"fals\"",
		"fals\n",
		"fals`",
		"FALS0",
		"FALSE",
		"g Using UT0",
		"N ",
		"N NUL0",
		"n;",
		"N",
		"N/",
		"n\"",
		"n\v",
		"n`",
		"N0",
		"n00\vn0",
		"nu ",
		"nu;",
		"nu",
		"nu/",
		"nU\"",
		"nu\v",
		"nu`",
		"nu0",
		"NUL ",
		"nuL;",
		"nul",
		"NuL/",
		"NUL\"",
		"NUL\f",
		"nul`",
		"nul0",
		"NULL",
		"O",
		"R FAL0",
		"t n(50",
		"t usi0",
		"t using UTF80",
		"t using UTF8M",
		"t;",
		"t",
		"t/",
		"t\"",
		"t\f",
		"t`",
		"t0 USING U0",
		"t0",
		"tr ",
		"Tr;",
		"tr",
		"tr/",
		"tr\"",
		"tr\f",
		"tr`",
		"tr0",
		"tru ",
		"trU;",
		"TRU",
		"TrU/",
		"tru\"",
		"tru\f",
		"tru`",
		"TRU0",
		"TRUE",
		"u ",
		"u;",
		"u",
		"u/",
		"U\"",
		"u\t",
		"u`",
		"us ",
		"us;",
		"us",
		"us/",
		"US\"",
		"us\t",
		"us`",
		"us0",
		"usi ",
		"usi;",
		"usi",
		"usi/",
		"usi\"",
		"usi\t",
		"usi`",
		"usi0",
		"usin ",
		"usiN;",
		"USIN",
		"usin/",
		"usin\"",
		"usin\t",
		"usin`",
		"USIN0",
		"using ",
		"using 0",
		"using u",
		"USING U",
		"USING U0",
		"USING Ut",
		"using UT0",
		"using utF",
		"using UTf",
		"using utF0",
		"using utf8",
		"using UTF80",
		"using UTF8m",
		"Using UTF8M0",
		"Using UTF8MB",
		"Using utf8mb",
		"using,",
		"using;",
		"using",
		"USING",
		"using/",
		"using\"",
		"using\v",
		"using`",
		"using0",
		"v ",
		"v;",
		"v",
		"V/",
		"V\"",
		"v\v",
		"v`",
		"v0",
		"va ",
		"va;",
		"va",
		"va/",
		"Va\"",
		"va\v",
		"va`",
		"va0",
		"val ",
		"val;",
		"val",
		"val/",
		"Val\"",
		"VAL\v",
		"val`",
		"val0",
		"valu ",
		"valu;",
		"valu",
		"valu/",
		"valu\"",
		"VALU\t",
		"VALU`",
		"valu0",
		"value ",
		"value;",
		"value",
		"Value/",
		"Value\"",
		"VALUE\v",
		"value`",
		"value0",
		"x ",
		"x val0",
		"x;",
		"x'",
		"x'x",
		"x",
		"x/",
		"x\"",
		"X\r",
		"x`",
		"x00`0`Valu0",
	}

	runFailingTestCases(t, mysql.ModeNone, 1, inputs)
	runFailingTestCases(t, mysql.ModeNoBackslashEscapes, 1, inputs)
}

func TestMoreEmptyFiles(t *testing.T) {
	testCases := []testCase{
		{input: ""},
		{input: "--\t"},
		{input: "--\""},
		{input: "-- 000"},
		{input: "--;"},
		{input: "--,"},
		{input: "--0"},
		{input: "--`"},
		{input: "--"},
		{input: "--0000"},
		{input: "--\n"},
		{input: "--/"},
		{input: "--\"\r"},
		{input: "--\r"},
	}

	runTestCases(t, mysql.ModeNone, 1, testCases)
	runTestCases(t, mysql.ModeNoBackslashEscapes, 1, testCases)
}
