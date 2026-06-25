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
	"testing"

	"github.com/stretchr/testify/require"
)

func raw(s string) sql.RawBytes { return sql.RawBytes(s) }

func TestSQLWriterFraming(t *testing.T) {
	var bf bytes.Buffer
	prefix := []byte("INSERT INTO `t` VALUES\n")
	kinds := []FieldKind{KindNumber, KindString, KindBytes}
	sw := NewSQLWriter(&bf, prefix, kinds, &Config{})

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
	kinds := []FieldKind{KindString}
	prefix := []byte("INSERT INTO `t` VALUES\n")

	var backslash bytes.Buffer
	sw := NewSQLWriter(&backslash, prefix, kinds, &Config{EscapeBackslash: true})
	require.NoError(t, sw.Write([]sql.RawBytes{raw("a'b\nc\\d")}))
	require.NoError(t, sw.Close())
	require.Equal(t, "INSERT INTO `t` VALUES\n('a\\'b\\nc\\\\d');\n", backslash.String())

	var double bytes.Buffer
	sw = NewSQLWriter(&double, prefix, kinds, &Config{EscapeBackslash: false})
	require.NoError(t, sw.Write([]sql.RawBytes{raw("a'b")}))
	require.NoError(t, sw.Close())
	require.Equal(t, "INSERT INTO `t` VALUES\n('a''b');\n", double.String())
}

func TestSQLWriterStatementSplit(t *testing.T) {
	var bf bytes.Buffer
	prefix := []byte("P\n")
	kinds := []FieldKind{KindNumber}
	// Tuple "(1)" is 3 bytes + 2 separator = 5; prefix is 2. After the first row
	// statementSize = 2+3+2 = 7 >= 6, so the second row starts a new statement.
	sw := NewSQLWriter(&bf, prefix, kinds, &Config{StatementSize: 6})
	require.NoError(t, sw.Write([]sql.RawBytes{raw("1")}))
	require.NoError(t, sw.Write([]sql.RawBytes{raw("2")}))
	require.NoError(t, sw.Close())

	require.Equal(t, "P\n(1);\nP\n(2);\n", bf.String())
}

func TestSQLWriterEmptyTuple(t *testing.T) {
	var bf bytes.Buffer
	prefix := []byte("INSERT INTO `t` VALUES\n")
	sw := NewSQLWriter(&bf, prefix, nil, &Config{})
	require.NoError(t, sw.Write(nil))
	require.NoError(t, sw.Write(nil))
	require.NoError(t, sw.Close())
	require.Equal(t, "INSERT INTO `t` VALUES\n(),\n();\n", bf.String())
}
