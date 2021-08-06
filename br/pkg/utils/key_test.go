// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"encoding/hex"

	. "github.com/pingcap/check"
)

type testKeySuite struct{}

var _ = Suite(&testKeySuite{})

func (r *testKeySuite) TestParseKey(c *C) {
	// test rawKey
	testRawKey := []struct {
		rawKey string
		ans    []byte
	}{
		{"1234", []byte("1234")},
		{"abcd", []byte("abcd")},
		{"1a2b", []byte("1a2b")},
		{"AA", []byte("AA")},
		{"\a", []byte("\a")},
		{"\\'", []byte("\\'")},
	}

	for _, tt := range testRawKey {
		parsedKey, err := ParseKey("raw", tt.rawKey)
		c.Assert(err, IsNil)
		c.Assert(parsedKey, BytesEquals, tt.ans)
	}

	// test EscapedKey
	testEscapedKey := []struct {
		EscapedKey string
		ans        []byte
	}{
		{"\\a\\x1", []byte("\a\x01")},
		{"\\b\\f", []byte("\b\f")},
		{"\\n\\r", []byte("\n\r")},
		{"\\t\\v", []byte("\t\v")},
		{"\\'", []byte("'")},
	}

	for _, tt := range testEscapedKey {
		parsedKey, err := ParseKey("escaped", tt.EscapedKey)
		c.Assert(err, IsNil)
		c.Assert(parsedKey, BytesEquals, tt.ans)
	}

	// test hexKey
	testHexKey := []struct {
		hexKey string
		ans    []byte
	}{
		{"1234", []byte("1234")},
		{"abcd", []byte("abcd")},
		{"1a2b", []byte("1a2b")},
		{"AA", []byte("AA")},
		{"\a", []byte("\a")},
		{"\\'", []byte("\\'")},
		{"\x01", []byte("\x01")},
		{"\xAA", []byte("\xAA")},
	}

	for _, tt := range testHexKey {
		key := hex.EncodeToString([]byte(tt.hexKey))
		parsedKey, err := ParseKey("hex", key)
		c.Assert(err, IsNil)
		c.Assert(parsedKey, BytesEquals, tt.ans)
	}

	// test other
	testNotSupportKey := []struct {
		any string
		ans []byte
	}{
		{"1234", []byte("1234")},
		{"abcd", []byte("abcd")},
		{"1a2b", []byte("1a2b")},
		{"AA", []byte("AA")},
		{"\a", []byte("\a")},
		{"\\'", []byte("\\'")},
		{"\x01", []byte("\x01")},
		{"\xAA", []byte("\xAA")},
	}

	for _, tt := range testNotSupportKey {
		_, err := ParseKey("notSupport", tt.any)
		c.Assert(err, ErrorMatches, "unknown format.*")
	}
}

func (r *testKeySuite) TestCompareEndKey(c *C) {
	// test endKey
	testCase := []struct {
		key1 []byte
		key2 []byte
		ans  int
	}{
		{[]byte("1"), []byte("2"), -1},
		{[]byte("1"), []byte("1"), 0},
		{[]byte("2"), []byte("1"), 1},
		{[]byte("1"), []byte(""), -1},
		{[]byte(""), []byte(""), 0},
		{[]byte(""), []byte("1"), 1},
	}

	for _, tt := range testCase {
		res := CompareEndKey(tt.key1, tt.key2)
		c.Assert(res, Equals, tt.ans)
	}
}
