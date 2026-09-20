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

package csvfile

import (
	"bytes"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"testing"

	"github.com/pingcap/tidb/pkg/dumpformat"
	"github.com/stretchr/testify/require"
)

func baseConfig() *Config {
	return &Config{
		FieldsTerminatedBy: ",",
		FieldsEnclosedBy:   `"`,
		LinesTerminatedBy:  "\n",
		NullValue:          []byte(`\N`),
	}
}

func TestCSVWriterBackslashEscape(t *testing.T) {
	cfg := baseConfig()
	cfg.FieldsEscapedBy = "\\"
	var bf bytes.Buffer
	cw := NewWriter(&bf, []dumpformat.FieldKind{dumpformat.KindString}, cfg)
	// NUL, CR, LF, backslash and the delimiter byte are all backslash-escaped.
	require.NoError(t, cw.Write([]sql.RawBytes{sql.RawBytes("a\x00b\rc\nd\\e\"f")}))
	require.Equal(t, "\"a\\0b\\rc\\nd\\\\e\\\"f\"\n", bf.String())
}

func TestCSVWriterQuoteDoubling(t *testing.T) {
	cfg := baseConfig() // EscapeBackslash false -> delimiter is doubled
	var bf bytes.Buffer
	cw := NewWriter(&bf, []dumpformat.FieldKind{dumpformat.KindString}, cfg)
	require.NoError(t, cw.Write([]sql.RawBytes{sql.RawBytes(`a"b"c`)}))
	require.Equal(t, "\"a\"\"b\"\"c\"\n", bf.String())
}

func TestCSVWriterNullAndKinds(t *testing.T) {
	cfg := baseConfig()
	var bf bytes.Buffer
	cw := NewWriter(&bf, []dumpformat.FieldKind{dumpformat.KindNumber, dumpformat.KindString, dumpformat.KindBytes}, cfg)
	require.NoError(t, cw.Write([]sql.RawBytes{sql.RawBytes("1"), nil, sql.RawBytes("ab")}))
	require.Equal(t, "1,\\N,\"ab\"\n", bf.String())
}

func TestCSVWriterBytesHex(t *testing.T) {
	cfg := baseConfig()
	cfg.BinaryFormat = BinaryFormatHEX
	var bf bytes.Buffer
	cw := NewWriter(&bf, []dumpformat.FieldKind{dumpformat.KindBytes}, cfg)
	require.NoError(t, cw.Write([]sql.RawBytes{sql.RawBytes("ab")}))
	require.Equal(t, "\"6162\"\n", bf.String())
}

func TestCSVWriterEmptyRow(t *testing.T) {
	cfg := baseConfig()
	var bf bytes.Buffer
	cw := NewWriter(&bf, nil, cfg)
	require.NoError(t, cw.Write(nil))
	require.NoError(t, cw.Write(nil))
	require.Equal(t, "\n\n", bf.String())
}

func TestCSVWriterUnquotedBackslash(t *testing.T) {
	cfg := &Config{
		FieldsTerminatedBy: ",",
		FieldsEscapedBy:    "\\",
		LinesTerminatedBy:  "\n",
	}
	var bf bytes.Buffer
	cw := NewWriter(&bf, []dumpformat.FieldKind{dumpformat.KindString}, cfg)
	// No enclosure: backslash mode escapes the separator byte along with CR/LF.
	require.NoError(t, cw.Write([]sql.RawBytes{sql.RawBytes("a,b\nc")}))
	require.Equal(t, "a\\,b\\nc\n", bf.String())
}

func TestCSVWriterUnquotedRaw(t *testing.T) {
	cfg := &Config{
		FieldsTerminatedBy: ",",
		LinesTerminatedBy:  "\n",
	}
	var bf bytes.Buffer
	cw := NewWriter(&bf, []dumpformat.FieldKind{dumpformat.KindString}, cfg)
	// No enclosure and no escape: the value passes through unchanged.
	require.NoError(t, cw.Write([]sql.RawBytes{sql.RawBytes("a,b")}))
	require.Equal(t, "a,b\n", bf.String())
}

func TestCSVWriterBytesBase64(t *testing.T) {
	cfg := baseConfig()
	cfg.BinaryFormat = BinaryFormatBase64
	var bf bytes.Buffer
	cw := NewWriter(&bf, []dumpformat.FieldKind{dumpformat.KindBytes}, cfg)
	require.NoError(t, cw.Write([]sql.RawBytes{sql.RawBytes("ab")}))
	require.Equal(t, "\"YWI=\"\n", bf.String())
}

func TestCSVWriterHeader(t *testing.T) {
	cfg := baseConfig()
	var bf bytes.Buffer
	cw := NewWriter(&bf, []dumpformat.FieldKind{dumpformat.KindString, dumpformat.KindString}, cfg)
	// Names are enclosed and separated like a data row, even for number columns.
	require.NoError(t, cw.WriteHeader([][]byte{[]byte("id"), []byte("name")}))
	require.Equal(t, "\"id\",\"name\"\n", bf.String())
}

func TestCSVWriterEstimateFileSize(t *testing.T) {
	cfg := baseConfig()
	var bf bytes.Buffer
	cw := NewWriter(&bf, []dumpformat.FieldKind{dumpformat.KindNumber}, cfg)
	require.NoError(t, cw.WriteHeader([][]byte{[]byte("n")}))
	require.NoError(t, cw.Write([]sql.RawBytes{sql.RawBytes("1")}))
	require.NoError(t, cw.Write([]sql.RawBytes{sql.RawBytes("22")}))
	require.Equal(t, uint64(bf.Len()), cw.EstimateFileSize())
}

func TestCSVWriterRowWidthMismatch(t *testing.T) {
	cfg := baseConfig()
	var bf bytes.Buffer
	cw := NewWriter(&bf, []dumpformat.FieldKind{dumpformat.KindString, dumpformat.KindString}, cfg)
	err := cw.Write([]sql.RawBytes{sql.RawBytes("only-one")})
	require.ErrorContains(t, err, "row has 1 fields, want 2")
}

// refField encodes one field at once with the standard library, as the
// reference that Write must reproduce however it splits the value.
func refField(val []byte, kind dumpformat.FieldKind, cfg *Config) string {
	if val == nil {
		return string(cfg.NullValue)
	}
	if kind == dumpformat.KindNumber {
		return string(val)
	}
	var body []byte
	switch {
	case kind == dumpformat.KindBytes && cfg.BinaryFormat == BinaryFormatHEX:
		body = []byte(hex.EncodeToString(val))
	case kind == dumpformat.KindBytes && cfg.BinaryFormat == BinaryFormatBase64:
		body = []byte(base64.StdEncoding.EncodeToString(val))
	case len(cfg.FieldsEscapedBy) > 0:
		body = appendEscapedBackslash(nil, val, cfg)
	case len(cfg.FieldsEnclosedBy) > 0:
		d := []byte(cfg.FieldsEnclosedBy)
		body = bytes.ReplaceAll(val, d, append(d, d...))
	default:
		body = val
	}
	return cfg.FieldsEnclosedBy + string(body) + cfg.FieldsEnclosedBy
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

func TestCSVWriterLargeValues(t *testing.T) {
	const limit = dumpformat.MaxBufferedValueSize
	withEscape := baseConfig()
	withEscape.FieldsEscapedBy = `\`
	hexCfg := baseConfig()
	hexCfg.BinaryFormat = BinaryFormatHEX
	multiByte := baseConfig()
	multiByte.FieldsEnclosedBy = `"'`
	multiByte.BinaryFormat = BinaryFormatBase64
	overlapping := baseConfig()
	overlapping.FieldsEnclosedBy = "##"
	unquoted := baseConfig()
	unquoted.FieldsEnclosedBy = ""
	configs := []*Config{withEscape, hexCfg, multiByte, overlapping, unquoted}

	// Put the characters that get escaped or doubled on both sides of every
	// piece boundary, including a run of '#' so that "##" matches overlap it.
	str := bytes.Repeat([]byte("a"), 3*limit+5)
	for _, boundary := range []int{limit, 2 * limit} {
		copy(str[boundary-3:], `"'#`)
		copy(str[boundary-1:], `"'##`)
		copy(str[boundary+3:], "###\\n\r\x00")
	}
	copy(str[len(str)-2:], `"'`)
	bin := make([]byte, 2*limit+3)
	for i := range bin {
		bin[i] = byte(i * 7)
	}
	medium := sql.RawBytes(bytes.Repeat([]byte(`m"'#`), limit/8))
	kinds := []dumpformat.FieldKind{
		dumpformat.KindNumber, dumpformat.KindString, dumpformat.KindBytes, dumpformat.KindString,
	}
	rows := [][]sql.RawBytes{
		{sql.RawBytes("1"), str, bin, nil},
		// Each value fits the buffer but the row does not.
		{sql.RawBytes("2"), medium, medium, medium},
		{sql.RawBytes("3"), sql.RawBytes(`s"'#`), sql.RawBytes("\x01"), sql.RawBytes("")},
	}

	for _, cfg := range configs {
		var expected string
		for _, row := range rows {
			for i, val := range row {
				if i > 0 {
					expected += cfg.FieldsTerminatedBy
				}
				expected += refField(val, kinds[i], cfg)
			}
			expected += cfg.LinesTerminatedBy
		}

		var rec maxWriteRecorder
		cw := NewWriter(&rec, kinds, cfg)
		for _, row := range rows {
			require.NoError(t, cw.Write(row))
		}
		require.NoError(t, cw.Close())
		require.Equal(t, expected, rec.String(), "enclosed by %q escaped by %q", cfg.FieldsEnclosedBy, cfg.FieldsEscapedBy)
		require.Equal(t, uint64(len(expected)), cw.EstimateFileSize())
		require.LessOrEqual(t, rec.maxWrite, 3*limit)
		require.LessOrEqual(t, cap(cw.buf), 4*limit)
	}
}

func TestCSVWriterLargeValueBoundsBuffer(t *testing.T) {
	const limit = dumpformat.MaxBufferedValueSize
	// Every enclosure doubles, so this value encodes to 20*limit bytes.
	quotes := sql.RawBytes(bytes.Repeat([]byte(`"`), 10*limit))
	kinds := []dumpformat.FieldKind{dumpformat.KindString}

	var rec maxWriteRecorder
	cw := NewWriter(&rec, kinds, baseConfig())
	require.NoError(t, cw.Write([]sql.RawBytes{quotes}))
	require.NoError(t, cw.Close())

	require.Equal(t, 1+20*limit+1+1, rec.Len())
	require.Equal(t, uint64(rec.Len()), cw.EstimateFileSize())
	require.LessOrEqual(t, rec.maxWrite, 3*limit)
	require.LessOrEqual(t, cap(cw.buf), 4*limit)
}
