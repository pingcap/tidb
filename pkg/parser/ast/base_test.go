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
		{"你好", nil, "你好", "你好"},
		{"\xd2\xbb", charset.EncodingGBKImpl, "一", "\xd2\xbb"},
		{"\xc1\xd0", charset.EncodingGBKImpl, "列", "\xc1\xd0"},
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
		{"_utf8 prefix", "SELECT _utf8'hello world'", "SELECT _utf8'hello world'"},
		{"_utf8mb4 prefix", "SELECT _utf8mb4'hello world'", "SELECT _utf8mb4'hello world'"},
		{"N prefix", "SELECT N'hello world'", "SELECT N'hello world'"},
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
		{"_binary prefix preserved", "SELECT _binary '\xd2\xe4\xa6\xb8'", "SELECT _binary 0xd2e4a6b8"},
		{"_binary prefix without space", "SELECT _binary'\x01'", "SELECT _binary 0x01"},
		{"_utf8 prefix without space", "SELECT _utf8'\x01'", "SELECT _utf8 0x01"},
		{"_utf8mb4 prefix without space", "SELECT _utf8mb4'\x01'", "SELECT _utf8mb4 0x01"},
		{"escaped '' inside", "SELECT '\xd2''\xe4'", "SELECT 0xd227e4"},
		{"escaped \\' inside", "SELECT '\xd2\\'\xe4'", "SELECT 0xd227e4"},
		{"escaped \"\" inside", "SELECT \"\xd2\"\"\xe4\"", "SELECT 0xd222e4"},
		{"backtick inside binary", "SELECT '\xd2`\xe4'", "SELECT 0xd260e4"},
		{"mixed binary and text args", "SELECT '\xd2\xe4', 'hello', _binary '\xa1\xb2'", "SELECT 0xd2e4, 'hello', _binary 0xa1b2"},

		// Truncated/invalid UTF-8 sequences
		{"truncated 4-byte utf8", "SELECT '\xf0\x9f\x98'", "SELECT 0xf09f98"},
		{"invalid continuation byte", "SELECT '\x80\x81'", "SELECT 0x8081"},

		// Control characters
		{"NUL byte", "SELECT '\x00'", "SELECT 0x00"},
		{"mixed control and text", "SELECT 'hello\x00world'", "SELECT 0x68656c6c6f00776f726c64"},
		{"multiple control chars", "SELECT '\x01\x02\x03\x04\x05'", "SELECT 0x0102030405"},
	}
	for _, tt := range binaryTests {
		n.SetText(charset.EncodingUTF8Impl, tt.text)
		require.Equal(t, tt.want, n.Text(), tt.name)
	}
}

func TestBinaryStringLiteralSkipsComments(t *testing.T) {
	n := &node{}

	tests := []struct {
		name string
		text string
		want string
	}{
		// -- line comments with quotes must not corrupt the SQL
		{
			"-- with apostrophe",
			"-- don't do this\nSELECT 'hello' FROM t",
			"-- don't do this\nSELECT 'hello' FROM t",
		},
		{
			"-- with commented-out SQL (even quotes)",
			"-- SELECT * FROM t WHERE name='John'\nSELECT 1",
			"-- SELECT * FROM t WHERE name='John'\nSELECT 1",
		},
		{
			"-- double-quote in comment",
			"-- see table \"users\"\nSELECT \"bar\" FROM t",
			"-- see table \"users\"\nSELECT \"bar\" FROM t",
		},
		{
			"-- at end of input",
			"SELECT 1 -- don't",
			"SELECT 1 -- don't",
		},
		{
			"-- quote at end of comment line",
			"-- ending with '\nSELECT 'hello'",
			"-- ending with '\nSELECT 'hello'",
		},
		{
			"-- without space is NOT a comment",
			"SELECT 1 --1",
			"SELECT 1 --1",
		},
		// # line comments
		{
			"# with apostrophe",
			"# user's config\nSELECT 'value' FROM t",
			"# user's config\nSELECT 'value' FROM t",
		},
		// /* */ block comments
		{
			"block comment with apostrophe",
			"/* it's a test */ SELECT 'value' FROM t",
			"/* it's a test */ SELECT 'value' FROM t",
		},
		{
			"multi-line block comment with quote",
			"/*\n * don't modify\n */ SELECT 'value' FROM t",
			"/*\n * don't modify\n */ SELECT 'value' FROM t",
		},
		// -- with form-feed and vertical-tab (unicode.IsSpace matches these)
		{
			"-- with form-feed after dashes",
			"--\f don't\nSELECT 'hello' FROM t",
			"--\f don't\nSELECT 'hello' FROM t",
		},
		{
			"-- with vertical-tab after dashes",
			"--\v don't\nSELECT 'hello' FROM t",
			"--\v don't\nSELECT 'hello' FROM t",
		},
		// Executable comments must NOT be skipped (quotes inside are SQL)
		{
			"/*! executable - binary inside",
			"/*!80000 SELECT '\xd2\xe4' */",
			"/*!80000 SELECT 0xd2e4 */",
		},
		{
			"/*+ hint - binary inside",
			"/*+ SET_VAR(charset='\xd2\xe4') */ SELECT 1",
			"/*+ SET_VAR(charset=0xd2e4) */ SELECT 1",
		},
		// /*T! and /*M! are skipped as comments (conservative: can't check feature gates from ast)
		{
			"/*T! skipped as comment",
			"/*T![unsupported] don't */ SELECT 'hello' FROM t",
			"/*T![unsupported] don't */ SELECT 'hello' FROM t",
		},
		{
			"/*M! skipped as comment",
			"/*M! don't */ SELECT 'hello' FROM t",
			"/*M! don't */ SELECT 'hello' FROM t",
		},
		// Real-world CDC case
		{
			"CREATE VIEW with comment quote",
			"-- (don't use parenthesis)\n\nCREATE OR REPLACE VIEW v AS SELECT 'Attribute' AS t FROM t1 UNION ALL SELECT 'Reference' AS t FROM t2",
			"-- (don't use parenthesis)\n\nCREATE OR REPLACE VIEW v AS SELECT 'Attribute' AS t FROM t1 UNION ALL SELECT 'Reference' AS t FROM t2",
		},
		// Binary string after comment still gets hex-encoded
		{
			"comment + binary string",
			"-- don't\nSELECT '\xd2\xe4' FROM t",
			"-- don't\nSELECT 0xd2e4 FROM t",
		},
	}
	for _, tt := range tests {
		n.SetText(charset.EncodingUTF8Impl, tt.text)
		require.Equal(t, tt.want, n.Text(), tt.name)
	}
}

func TestBinaryStringLiteralNoBackslashEscapes(t *testing.T) {
	n := &node{}

	n.SetText(charset.EncodingUTF8Impl, "SELECT '\\n'")
	n.SetNoBackslashEscapes(true)
	require.Equal(t, "SELECT '\\n'", n.Text(), "NO_BACKSLASH_ESCAPES literal \\n")

	n.SetText(charset.EncodingUTF8Impl, "SELECT '\\' , 'after'")
	n.SetNoBackslashEscapes(true)
	require.Equal(t, "SELECT '\\' , 'after'", n.Text(), "NO_BACKSLASH_ESCAPES quote boundary")

	n.SetText(charset.EncodingUTF8Impl, "SELECT '\xd2\xe4'")
	n.SetNoBackslashEscapes(true)
	require.Equal(t, "SELECT 0xd2e4", n.Text(), "NO_BACKSLASH_ESCAPES binary")
}

func TestBinaryStringLiteralGBK(t *testing.T) {
	n := &node{}

	// GBK Chinese text: \xb1\xed is 表 in GBK, \x31 is '1'.
	// This should be decoded as valid GBK and left as a printable string,
	// not converted to a hex literal.
	n.SetText(charset.EncodingGBKImpl, "select '\xb1\xed\x31'")
	require.Equal(t, "select '表1'", n.Text(), "GBK printable")

	// GBK with actual invalid bytes should still convert to hex
	n.SetText(charset.EncodingGBKImpl, "select '\x80\xff'")
	require.Equal(t, "select 0x80ff", n.Text(), "GBK binary")

	// 筡 = \xb9\x5c in GBK; trail byte 0x5c must not be mistaken for backslash
	n.SetText(charset.EncodingGBKImpl, "select '\xb9\x5c'")
	require.Equal(t, "select '筡'", n.Text(), "GBK 0x5c trail byte")

	// Multiple GBK chars with 0x5c trail bytes: 筡 = \xb9\x5c, 臷 = \xc5\x5c
	n.SetText(charset.EncodingGBKImpl, "select '\xb9\x5c\xc5\x5c'")
	require.Equal(t, "select '筡臷'", n.Text(), "GBK multiple 0x5c trail bytes")

	// 0x5c trail byte right before closing quote must not escape the quote
	n.SetText(charset.EncodingGBKImpl, "select '\xb9\x5c', 'after'")
	require.Equal(t, "select '筡', 'after'", n.Text(), "GBK 0x5c before quote")
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

func buildMixedQuery(n int) string {
	var b strings.Builder
	b.WriteString("SELECT * FROM t1 WHERE ")
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteString(" OR ")
		}
		if i%2 == 0 {
			b.WriteString(buildBinaryClause())
		} else {
			b.WriteString(buildPrintableClause())
		}
	}
	return b.String()
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
	mixedShort := buildMixedQuery(2)
	mixedLong := buildMixedQuery(200)

	b.Run("NoQuotes/Short", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			convertBinaryStringLiterals(noQuotesShort, enc, false)
		}
	})
	b.Run("NoQuotes/Long", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			convertBinaryStringLiterals(noQuotesLong, enc, false)
		}
	})
	b.Run("Printable/Short", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			convertBinaryStringLiterals(printableShort, enc, false)
		}
	})
	b.Run("Printable/Long", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			convertBinaryStringLiterals(printableLong, enc, false)
		}
	})
	b.Run("Binary/Short", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			convertBinaryStringLiterals(binaryShort, enc, false)
		}
	})
	b.Run("Binary/Long", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			convertBinaryStringLiterals(binaryLong, enc, false)
		}
	})
	b.Run("Mixed/Short", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			convertBinaryStringLiterals(mixedShort, enc, false)
		}
	})
	b.Run("Mixed/Long", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			convertBinaryStringLiterals(mixedLong, enc, false)
		}
	})
}
