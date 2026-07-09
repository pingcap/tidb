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
		Separator:      []byte(","),
		Delimiter:      []byte(`"`),
		NullValue:      []byte(`\N`),
		LineTerminator: []byte("\n"),
	}
}

func TestCSVWriterBackslashEscape(t *testing.T) {
	cfg := baseConfig()
	cfg.EscapeBackslash = true
	var bf bytes.Buffer
	cw := NewCSVWriter(&bf, []FieldKind{KindString}, cfg)
	// NUL, CR, LF, backslash and the delimiter byte are all backslash-escaped.
	require.NoError(t, cw.Write([]sql.RawBytes{sql.RawBytes("a\x00b\rc\nd\\e\"f")}))
	require.Equal(t, "\"a\\0b\\rc\\nd\\\\e\\\"f\"\n", bf.String())
}

func TestCSVWriterQuoteDoubling(t *testing.T) {
	cfg := baseConfig() // EscapeBackslash false -> delimiter is doubled
	var bf bytes.Buffer
	cw := NewCSVWriter(&bf, []FieldKind{KindString}, cfg)
	require.NoError(t, cw.Write([]sql.RawBytes{sql.RawBytes(`a"b"c`)}))
	require.Equal(t, "\"a\"\"b\"\"c\"\n", bf.String())
}

func TestCSVWriterNullAndKinds(t *testing.T) {
	cfg := baseConfig()
	var bf bytes.Buffer
	cw := NewCSVWriter(&bf, []FieldKind{KindNumber, KindString, KindBytes}, cfg)
	require.NoError(t, cw.Write([]sql.RawBytes{sql.RawBytes("1"), nil, sql.RawBytes("ab")}))
	require.Equal(t, "1,\\N,\"ab\"\n", bf.String())
}

func TestCSVWriterBytesHex(t *testing.T) {
	cfg := baseConfig()
	cfg.BinaryFormat = BinaryFormatHEX
	var bf bytes.Buffer
	cw := NewCSVWriter(&bf, []FieldKind{KindBytes}, cfg)
	require.NoError(t, cw.Write([]sql.RawBytes{sql.RawBytes("ab")}))
	require.Equal(t, "\"6162\"\n", bf.String())
}

func TestCSVWriterEmptyRow(t *testing.T) {
	cfg := baseConfig()
	var bf bytes.Buffer
	cw := NewCSVWriter(&bf, nil, cfg)
	require.NoError(t, cw.Write(nil))
	require.NoError(t, cw.Write(nil))
	require.Equal(t, "\n\n", bf.String())
}
