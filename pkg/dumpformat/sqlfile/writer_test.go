// Copyright 2026 PingCAP, Inc.
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

package sqlfile

import (
	"bytes"
	"database/sql"
	"errors"
	"testing"

	"github.com/pingcap/tidb/pkg/dumpformat"
	"github.com/stretchr/testify/require"
)

func raw(s string) sql.RawBytes { return sql.RawBytes(s) }

func TestSQLWriterFraming(t *testing.T) {
	var bf bytes.Buffer
	prefix := []byte("INSERT INTO `t` VALUES\n")
	kinds := []dumpformat.FieldKind{dumpformat.KindNumber, dumpformat.KindString, dumpformat.KindBytes}
	sw := NewWriter(&bf, prefix, kinds, &Config{})

	require.NoError(t, sw.Write([]sql.RawBytes{raw("1"), raw("ab"), raw("ab")}))
	require.NoError(t, sw.Write([]sql.RawBytes{raw("2"), nil, raw("")}))
	require.NoError(t, sw.Close())

	expected := "INSERT INTO `t` VALUES\n" +
		"(1,'ab',x'6162'),\n" +
		"(2,NULL,x'');\n"
	require.Equal(t, expected, bf.String())
	require.Equal(t, uint64(len(expected)), sw.EstimateFileSize())
}

func TestSQLWriterEscaping(t *testing.T) {
	kinds := []dumpformat.FieldKind{dumpformat.KindString}
	prefix := []byte("INSERT INTO `t` VALUES\n")

	var backslash bytes.Buffer
	sw := NewWriter(&backslash, prefix, kinds, &Config{EscapeBackslash: true})
	require.NoError(t, sw.Write([]sql.RawBytes{raw("a'b\nc\rd\\e\x00f\"g\x1ah")}))
	require.NoError(t, sw.Close())
	require.Equal(t, "INSERT INTO `t` VALUES\n('a\\'b\\nc\\rd\\\\e\\0f\\\"g\\Zh');\n", backslash.String())

	var double bytes.Buffer
	sw = NewWriter(&double, prefix, kinds, &Config{EscapeBackslash: false})
	require.NoError(t, sw.Write([]sql.RawBytes{raw("a'b")}))
	require.NoError(t, sw.Close())
	require.Equal(t, "INSERT INTO `t` VALUES\n('a''b');\n", double.String())
}

func TestSQLWriterStatementSplit(t *testing.T) {
	var bf bytes.Buffer
	prefix := []byte("P\n")
	kinds := []dumpformat.FieldKind{dumpformat.KindNumber}
	// Tuple "(1)" is 3 bytes + 2 separator = 5; prefix is 2. After the first row
	// statementSize = 2+3+2 = 7 >= 6, so the second row starts a new statement.
	sw := NewWriter(&bf, prefix, kinds, &Config{StatementSize: 6})
	require.NoError(t, sw.Write([]sql.RawBytes{raw("1")}))
	require.NoError(t, sw.Write([]sql.RawBytes{raw("2")}))
	require.NoError(t, sw.Close())

	require.Equal(t, "P\n(1);\nP\n(2);\n", bf.String())
}

func TestSQLWriterEmptyTuple(t *testing.T) {
	var bf bytes.Buffer
	prefix := []byte("INSERT INTO `t` VALUES\n")
	sw := NewWriter(&bf, prefix, nil, &Config{})
	require.NoError(t, sw.Write(nil))
	require.NoError(t, sw.Write(nil))
	require.NoError(t, sw.Close())
	require.Equal(t, "INSERT INTO `t` VALUES\n(),\n();\n", bf.String())
}

// encodeTuple is the one-shot reference encoding of a row, which Write must
// reproduce however it splits the row into writes.
func encodeTuple(row []sql.RawBytes, kinds []dumpformat.FieldKind, escapeBackslash bool) string {
	buf := []byte{'('}
	for i, val := range row {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = AppendValue(buf, val, val == nil, kinds[i], escapeBackslash)
	}
	return string(append(buf, ')'))
}

// maxWriteRecorder records the largest single Write it receives.
type maxWriteRecorder struct {
	bytes.Buffer
	maxWrite int
}

func (r *maxWriteRecorder) Write(p []byte) (int, error) {
	r.maxWrite = max(r.maxWrite, len(p))
	return r.Buffer.Write(p)
}

func TestSQLWriterLargeValues(t *testing.T) {
	const limit = dumpformat.MaxBufferedValueSize
	// Characters that need escaping sit on both sides of every piece boundary.
	str := bytes.Repeat([]byte("a"), 3*limit+5)
	for _, pos := range []int{0, limit - 1, limit, 2*limit - 1, 2 * limit, len(str) - 1} {
		str[pos] = '\''
	}
	str[limit+1] = '\\'
	str[limit+2] = '\n'
	str[limit+3] = 0
	str[2*limit+1] = '\x1a'
	str[2*limit+2] = '"'
	bin := make([]byte, 2*limit+3)
	for i := range bin {
		bin[i] = byte(i * 7)
	}
	medium := sql.RawBytes(bytes.Repeat([]byte("m'"), limit/4))
	kinds := []dumpformat.FieldKind{
		dumpformat.KindNumber, dumpformat.KindString, dumpformat.KindBytes, dumpformat.KindString,
	}
	rows := [][]sql.RawBytes{
		{raw("1"), str, bin, nil},
		// Each value fits the buffer but the row does not.
		{raw("2"), medium, medium, medium},
		{raw("3"), raw("small"), raw("\x01"), raw("")},
	}
	prefix := []byte("INSERT INTO `t` VALUES\n")

	for _, escapeBackslash := range []bool{false, true} {
		tuples := make([]string, len(rows))
		for i, row := range rows {
			tuples[i] = encodeTuple(row, kinds, escapeBackslash)
		}

		var single maxWriteRecorder
		sw := NewWriter(&single, prefix, kinds, &Config{EscapeBackslash: escapeBackslash})
		for _, row := range rows {
			require.NoError(t, sw.Write(row))
		}
		require.NoError(t, sw.Close())
		expected := string(prefix) + tuples[0] + ",\n" + tuples[1] + ",\n" + tuples[2] + ";\n"
		require.Equal(t, expected, single.String())
		require.Equal(t, uint64(len(expected)), sw.EstimateFileSize())
		require.LessOrEqual(t, single.maxWrite, 3*limit)
		require.LessOrEqual(t, cap(sw.buf), 4*limit)

		// With a 1-byte statement limit every row gets its own statement.
		var split bytes.Buffer
		sw = NewWriter(&split, prefix, kinds, &Config{StatementSize: 1, EscapeBackslash: escapeBackslash})
		for _, row := range rows {
			require.NoError(t, sw.Write(row))
		}
		require.NoError(t, sw.Close())
		expected = ""
		for _, tuple := range tuples {
			expected += string(prefix) + tuple + ";\n"
		}
		require.Equal(t, expected, split.String())
		require.Equal(t, uint64(len(expected)), sw.EstimateFileSize())
	}
}

func TestSQLWriterLargeValueBoundsBuffer(t *testing.T) {
	const limit = dumpformat.MaxBufferedValueSize
	// Every quote doubles, so this value encodes to 20*limit bytes.
	quotes := sql.RawBytes(bytes.Repeat([]byte("'"), 10*limit))
	bin := sql.RawBytes(bytes.Repeat([]byte{0xab}, 10*limit))
	kinds := []dumpformat.FieldKind{dumpformat.KindString, dumpformat.KindBytes}

	var rec maxWriteRecorder
	sw := NewWriter(&rec, []byte("P\n"), kinds, &Config{})
	require.NoError(t, sw.Write([]sql.RawBytes{quotes, bin}))
	require.NoError(t, sw.Close())

	require.Equal(t, 2+1+(2+20*limit)+1+(3+20*limit)+1+2, rec.Len())
	require.Equal(t, uint64(rec.Len()), sw.EstimateFileSize())
	require.LessOrEqual(t, rec.maxWrite, 3*limit)
	require.LessOrEqual(t, cap(sw.buf), 4*limit)
}

// failAfterWriter fails every write once it has accepted okWrites of them.
type failAfterWriter struct {
	bytes.Buffer
	okWrites int
}

func (f *failAfterWriter) Write(p []byte) (int, error) {
	if f.okWrites == 0 {
		return 0, errSink
	}
	f.okWrites--
	return f.Buffer.Write(p)
}

var errSink = errors.New("sink failed")

func TestSQLWriterKeepsFirstWriteError(t *testing.T) {
	const limit = dumpformat.MaxBufferedValueSize
	kinds := []dumpformat.FieldKind{dumpformat.KindString}
	// The row is flushed in pieces, so the sink fails with the value half-written.
	f := &failAfterWriter{okWrites: 1}
	sw := NewWriter(f, []byte("INSERT INTO `t` VALUES\n"), kinds, &Config{})

	require.ErrorIs(t, sw.Write([]sql.RawBytes{bytes.Repeat([]byte("a"), 3*limit)}), errSink)
	written := f.Len()
	require.NotZero(t, written)

	// Once a write fails, nothing more reaches the sink: no further rows, and no
	// ";\n" that would turn the truncated value into a statement that parses.
	require.ErrorIs(t, sw.Write([]sql.RawBytes{raw("x")}), errSink)
	require.ErrorIs(t, sw.Close(), errSink)
	require.Equal(t, written, f.Len())
}
