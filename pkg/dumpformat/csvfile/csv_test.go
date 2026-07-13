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
	"testing"

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
	cw := NewWriter(&bf, []FieldKind{KindString}, cfg)
	// NUL, CR, LF, backslash and the delimiter byte are all backslash-escaped.
	require.NoError(t, cw.Write([]sql.RawBytes{sql.RawBytes("a\x00b\rc\nd\\e\"f")}))
	require.Equal(t, "\"a\\0b\\rc\\nd\\\\e\\\"f\"\n", bf.String())
}

func TestCSVWriterQuoteDoubling(t *testing.T) {
	cfg := baseConfig() // EscapeBackslash false -> delimiter is doubled
	var bf bytes.Buffer
	cw := NewWriter(&bf, []FieldKind{KindString}, cfg)
	require.NoError(t, cw.Write([]sql.RawBytes{sql.RawBytes(`a"b"c`)}))
	require.Equal(t, "\"a\"\"b\"\"c\"\n", bf.String())
}

func TestCSVWriterNullAndKinds(t *testing.T) {
	cfg := baseConfig()
	var bf bytes.Buffer
	cw := NewWriter(&bf, []FieldKind{KindNumber, KindString, KindBytes}, cfg)
	require.NoError(t, cw.Write([]sql.RawBytes{sql.RawBytes("1"), nil, sql.RawBytes("ab")}))
	require.Equal(t, "1,\\N,\"ab\"\n", bf.String())
}

func TestCSVWriterBytesHex(t *testing.T) {
	cfg := baseConfig()
	cfg.BinaryFormat = BinaryFormatHEX
	var bf bytes.Buffer
	cw := NewWriter(&bf, []FieldKind{KindBytes}, cfg)
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
	cw := NewWriter(&bf, []FieldKind{KindString}, cfg)
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
	cw := NewWriter(&bf, []FieldKind{KindString}, cfg)
	// No enclosure and no escape: the value passes through unchanged.
	require.NoError(t, cw.Write([]sql.RawBytes{sql.RawBytes("a,b")}))
	require.Equal(t, "a,b\n", bf.String())
}

func TestCSVWriterBytesBase64(t *testing.T) {
	cfg := baseConfig()
	cfg.BinaryFormat = BinaryFormatBase64
	var bf bytes.Buffer
	cw := NewWriter(&bf, []FieldKind{KindBytes}, cfg)
	require.NoError(t, cw.Write([]sql.RawBytes{sql.RawBytes("ab")}))
	require.Equal(t, "\"YWI=\"\n", bf.String())
}

func TestCSVWriterHeader(t *testing.T) {
	cfg := baseConfig()
	var bf bytes.Buffer
	cw := NewWriter(&bf, []FieldKind{KindString, KindString}, cfg)
	// Names are enclosed and separated like a data row, even for number columns.
	require.NoError(t, cw.WriteHeader([][]byte{[]byte("id"), []byte("name")}))
	require.Equal(t, "\"id\",\"name\"\n", bf.String())
}

func TestCSVWriterEstimateFileSize(t *testing.T) {
	cfg := baseConfig()
	var bf bytes.Buffer
	cw := NewWriter(&bf, []FieldKind{KindNumber}, cfg)
	require.NoError(t, cw.WriteHeader([][]byte{[]byte("n")}))
	require.NoError(t, cw.Write([]sql.RawBytes{sql.RawBytes("1")}))
	require.NoError(t, cw.Write([]sql.RawBytes{sql.RawBytes("22")}))
	require.Equal(t, uint64(bf.Len()), cw.EstimateFileSize())
}

func TestCSVWriterRowWidthMismatch(t *testing.T) {
	cfg := baseConfig()
	var bf bytes.Buffer
	cw := NewWriter(&bf, []FieldKind{KindString, KindString}, cfg)
	err := cw.Write([]sql.RawBytes{sql.RawBytes("only-one")})
	require.ErrorContains(t, err, "row has 1 fields, want 2")
}
