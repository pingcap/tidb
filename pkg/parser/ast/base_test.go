// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// Package ast is the abstract syntax tree parsed from a SQL statement by parser.
// It can be analysed and transformed by optimizer.
package ast

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/stretchr/testify/require"
)

func TestNodeSetText(t *testing.T) {
	n := &node{}
	tests := []struct {
		text           string
		enc            charset.Encoding
		expectUTF8Text string
		expectText     string
	}{
		// GBK encoding
		{"你好", nil, "你好", "你好"},
		{"\xd2\xbb", charset.EncodingGBKImpl, "一", "\xd2\xbb"},
		{"\xc1\xd0", charset.EncodingGBKImpl, "列", "\xc1\xd0"},
		{"\xd2\xe4\xa6\xb8\xc1\xf3\xe5\xd7", charset.EncodingUTF8Impl, `\xd2䦸\xc1\xf3\xe5\xd7`, "\xd2\xe4\xa6\xb8\xc1\xf3\xe5\xd7"},
	}
	for _, tt := range tests {
		n.SetText(tt.enc, tt.text)
		require.Equal(t, tt.expectUTF8Text, n.Text())
		require.Equal(t, tt.expectText, n.OriginalText())
	}
}

func TestBinaryStringLiteralConversion(t *testing.T) {
	n := &node{}

	// UTF-8 printable strings — should all pass through unchanged
	printableTests := []struct {
		name string
		text string
		want string
	}{
		{"single-quoted", "SELECT 'hello world'", "SELECT 'hello world'"},
		{"double-quoted", "SELECT \"hello world\"", "SELECT \"hello world\""},
		{"_binary prefix", "SELECT _binary 'hello world'", "SELECT _binary 'hello world'"},
		{"_utf8mb4 prefix", "SELECT _utf8mb4 'hello world'", "SELECT _utf8mb4 'hello world'"},
		{"_latin1 prefix", "SELECT _latin1 'hello world'", "SELECT _latin1 'hello world'"},
		{"hex literal 0x", "SELECT 0x68656c6c6f", "SELECT 0x68656c6c6f"},
		{"hex literal X''", "SELECT X'68656c6c6f'", "SELECT X'68656c6c6f'"},
		{"escaped '' inside", "SELECT 'it''s here'", "SELECT 'it''s here'"},
		{"escaped \\' inside", "SELECT 'it\\'s here'", "SELECT 'it\\'s here'"},
		{"escaped \"\" inside", "SELECT \"say \"\"hi\"\"\"", "SELECT \"say \"\"hi\"\"\""},
		{"backtick inside string", "SELECT 'has `backtick` inside'", "SELECT 'has `backtick` inside'"},
		{"_binary word inside string", "SELECT 'the word _binary appears'", "SELECT 'the word _binary appears'"},
		{"backslash content", "SELECT 'path\\\\to\\\\file'", "SELECT 'path\\\\to\\\\file'"},
	}
	for _, tt := range printableTests {
		n.SetText(charset.EncodingUTF8Impl, tt.text)
		require.Equal(t, tt.want, n.Text(), tt.name)
	}

	// Binary (non-printable) strings — should convert to 0x hex literals
	binaryTests := []struct {
		name string
		text string
		want string
	}{
		{"single-quoted", "SELECT '\xd2\xe4\xa6\xb8'", "SELECT 0xd2e4a6b8"},
		{"double-quoted", "SELECT \"\xd2\xe4\xa6\xb8\"", "SELECT 0xd2e4a6b8"},
		{"_binary prefix", "SELECT _binary '\xd2\xe4\xa6\xb8'", "SELECT 0xd2e4a6b8"},
		{"_utf8mb4 prefix", "SELECT _utf8mb4 '\xd2\xe4\xa6\xb8'", "SELECT 0xd2e4a6b8"},
		{"_latin1 prefix", "SELECT _latin1 '\xd2\xe4\xa6\xb8'", "SELECT 0xd2e4a6b8"},
		{"hex literal 0x unchanged", "SELECT 0xd2e4a6b8", "SELECT 0xd2e4a6b8"},
		{"hex literal X'' unchanged", "SELECT X'd2e4a6b8'", "SELECT X'd2e4a6b8'"},
		{"escaped '' inside", "SELECT '\xd2''\xe4'", "SELECT 0xd227e4"},
		{"escaped \\' inside", "SELECT '\xd2\\'\xe4'", "SELECT 0xd227e4"},
		{"escaped \"\" inside", "SELECT \"\xd2\"\"\xe4\"", "SELECT 0xd222e4"},
		{"backtick inside binary", "SELECT '\xd2`\xe4'", "SELECT 0xd260e4"},
		{"_binary word inside binary", "SELECT '\xd2 _binary \xe4'", "SELECT 0xd2205f62696e61727920e4"},
		{"mixed binary and text args", "SELECT '\xd2\xe4', 'hello', _binary '\xa1\xb2'", "SELECT 0xd2e4, 'hello', 0xa1b2"},
		{"column name ending in _word not stripped", "SELECT * FROM t WHERE some_column = '\xd2\xe4'", "SELECT * FROM t WHERE some_column = 0xd2e4"},
		{"identifier_word before binary string", "SELECT some_value '\xd2\xe4'", "SELECT some_value 0xd2e4"},
	}
	for _, tt := range binaryTests {
		n.SetText(charset.EncodingUTF8Impl, tt.text)
		require.Equal(t, tt.want, n.Text(), tt.name)
	}
}

func buildBinaryClause() string {
	return "c1 = _binary '\xd2\xe4\xa6\xb8\xc1\xf3\xe5\xd7\xa9\xb2\xc4\xd6\xe8\xf1\xa3\xb5'"
}

func buildPrintableClause() string {
	return "c1 = 'hello world'"
}

func buildNoQuotesClause() string {
	return "c1 = 12345"
}

func buildQuery(clause string, n int) string {
	var b strings.Builder
	b.WriteString("SELECT * FROM t1 WHERE ")
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteString(" OR ")
		}
		b.WriteString(clause)
	}
	return b.String()
}

func BenchmarkConvertBinaryStringLiterals(b *testing.B) {
	enc := charset.EncodingUTF8Impl

	noQuotesShort := buildQuery(buildNoQuotesClause(), 1)
	noQuotesLong := buildQuery(buildNoQuotesClause(), 200)
	printableShort := buildQuery(buildPrintableClause(), 1)
	printableLong := buildQuery(buildPrintableClause(), 200)
	binaryShort := buildQuery(buildBinaryClause(), 1)
	binaryLong := buildQuery(buildBinaryClause(), 200)

	b.Run("NoQuotes/Short", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			convertBinaryStringLiterals(noQuotesShort, enc)
		}
	})
	b.Run("NoQuotes/Long", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			convertBinaryStringLiterals(noQuotesLong, enc)
		}
	})
	b.Run("Printable/Short", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			convertBinaryStringLiterals(printableShort, enc)
		}
	})
	b.Run("Printable/Long", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			convertBinaryStringLiterals(printableLong, enc)
		}
	})
	b.Run("Binary/Short", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			convertBinaryStringLiterals(binaryShort, enc)
		}
	})
	b.Run("Binary/Long", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			convertBinaryStringLiterals(binaryLong, enc)
		}
	})
}

