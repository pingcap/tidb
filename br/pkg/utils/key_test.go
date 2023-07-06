// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/kv"
	"github.com/stretchr/testify/require"
)

func TestParseKey(t *testing.T) {
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
		require.NoError(t, err)
		require.Equal(t, tt.ans, parsedKey)
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
		require.NoError(t, err)
		require.Equal(t, tt.ans, parsedKey)
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
		require.NoError(t, err)
		require.Equal(t, tt.ans, parsedKey)
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
		require.Error(t, err)
		require.Regexp(t, "^unknown format", err.Error())
	}
}

func TestCompareEndKey(t *testing.T) {
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
		require.Equal(t, tt.ans, res)
	}
}

func TestClampKeyRanges(t *testing.T) {
	r := func(a, b string) kv.KeyRange {
		return kv.KeyRange{
			StartKey: []byte(a),
			EndKey:   []byte(b),
		}
	}
	type Case struct {
		ranges  []kv.KeyRange
		clampIn []kv.KeyRange
		result  []kv.KeyRange
	}

	cases := []Case{
		{
			ranges:  []kv.KeyRange{r("0001", "0002"), r("0003", "0004"), r("0005", "0008")},
			clampIn: []kv.KeyRange{r("0001", "0004"), r("0006", "0008")},
			result:  []kv.KeyRange{r("0001", "0002"), r("0003", "0004"), r("0006", "0008")},
		},
		{
			ranges:  []kv.KeyRange{r("0001", "0002"), r("00021", "0003"), r("0005", "0009")},
			clampIn: []kv.KeyRange{r("0001", "0004"), r("0005", "0008")},
			result:  []kv.KeyRange{r("0001", "0002"), r("00021", "0003"), r("0005", "0008")},
		},
		{
			ranges:  []kv.KeyRange{r("0001", "0050"), r("0051", "0095"), r("0098", "0152")},
			clampIn: []kv.KeyRange{r("0001", "0100"), r("0150", "0200")},
			result:  []kv.KeyRange{r("0001", "0050"), r("0051", "0095"), r("0098", "0100"), r("0150", "0152")},
		},
		{
			ranges:  []kv.KeyRange{r("0001", "0050"), r("0051", "0095"), r("0098", "0152")},
			clampIn: []kv.KeyRange{r("0001", "0100"), r("0150", "")},
			result:  []kv.KeyRange{r("0001", "0050"), r("0051", "0095"), r("0098", "0100"), r("0150", "0152")},
		},
		{
			ranges:  []kv.KeyRange{r("0001", "0050"), r("0051", "0095"), r("0098", "")},
			clampIn: []kv.KeyRange{r("0001", "0100"), r("0150", "0200")},
			result:  []kv.KeyRange{r("0001", "0050"), r("0051", "0095"), r("0098", "0100"), r("0150", "0200")},
		},
		{
			ranges:  []kv.KeyRange{r("", "0050")},
			clampIn: []kv.KeyRange{r("", "")},
			result:  []kv.KeyRange{r("", "0050")},
		},
	}
	run := func(t *testing.T, c Case) {
		require.ElementsMatch(
			t,
			IntersectAll(CloneSlice(c.ranges), CloneSlice(c.clampIn)),
			c.result)
		require.ElementsMatch(
			t,
			IntersectAll(c.clampIn, c.ranges),
			c.result)
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("#%d", i), func(t *testing.T) {
			run(t, c)
		})
	}
}
