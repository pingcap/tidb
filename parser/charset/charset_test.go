// Copyright 2015 PingCAP, Inc.
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

package charset

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func testValidCharset(t *testing.T, charset string, collation string, expect bool) {
	b := ValidCharsetAndCollation(charset, collation)
	require.Equal(t, expect, b)
}

func TestValidCharset(t *testing.T) {
	tests := []struct {
		cs   string
		co   string
		succ bool
	}{
		{"utf8", "utf8_general_ci", true},
		{"", "utf8_general_ci", true},
		{"utf8mb4", "utf8mb4_bin", true},
		{"latin1", "latin1_bin", true},
		{"utf8", "utf8_invalid_ci", false},
		{"utf16", "utf16_bin", false},
		{"gb2312", "gb2312_chinese_ci", false},
		{"UTF8", "UTF8_BIN", true},
		{"UTF8", "utf8_bin", true},
		{"UTF8MB4", "utf8mb4_bin", true},
		{"UTF8MB4", "UTF8MB4_bin", true},
		{"UTF8MB4", "UTF8MB4_general_ci", true},
		{"Utf8", "uTf8_bIN", true},
	}
	for _, tt := range tests {
		testValidCharset(t, tt.cs, tt.co, tt.succ)
	}
}

func testGetDefaultCollation(t *testing.T, charset string, expectCollation string, succ bool) {
	b, err := GetDefaultCollation(charset)
	if !succ {
		require.Error(t, err)
		return
	}
	require.Equal(t, expectCollation, b)
}

func TestGetDefaultCollation(t *testing.T) {
	tests := []struct {
		cs   string
		co   string
		succ bool
	}{
		{"utf8", "utf8_bin", true},
		{"UTF8", "utf8_bin", true},
		{"utf8mb4", "utf8mb4_bin", true},
		{"ascii", "ascii_bin", true},
		{"binary", "binary", true},
		{"latin1", "latin1_bin", true},
		{"invalid_cs", "", false},
		{"", "utf8_bin", false},
	}
	for _, tt := range tests {
		testGetDefaultCollation(t, tt.cs, tt.co, tt.succ)
	}

	// Test the consistency of collations table and charset desc table
	charsetNum := 0
	for _, collate := range collations {
		if collate.IsDefault {
			if desc, ok := CharacterSetInfos[collate.CharsetName]; ok {
				require.Equal(t, desc.DefaultCollation, collate.Name)
				charsetNum += 1
			}
		}
	}
	require.Equal(t, len(CharacterSetInfos), charsetNum)
}

func TestGetCharsetDesc(t *testing.T) {
	tests := []struct {
		cs     string
		result string
		succ   bool
	}{
		{"utf8", "utf8", true},
		{"UTF8", "utf8", true},
		{"utf8mb4", "utf8mb4", true},
		{"ascii", "ascii", true},
		{"binary", "binary", true},
		{"latin1", "latin1", true},
		{"invalid_cs", "", false},
		{"", "utf8_bin", false},
	}
	for _, tt := range tests {
		desc, err := GetCharsetInfo(tt.cs)
		if !tt.succ {
			require.Error(t, err)
		} else {
			require.Equal(t, tt.result, desc.Name)
		}
	}
}

func TestGetCollationByName(t *testing.T) {
	for _, collation := range collations {
		coll, err := GetCollationByName(collation.Name)
		require.NoError(t, err)
		require.Equal(t, collation, coll)
	}

	_, err := GetCollationByName("non_exist")
	require.EqualError(t, err, "[ddl:1273]Unknown collation: 'non_exist'")
}

func TestValidCustomCharset(t *testing.T) {
	AddCharset(&Charset{"custom", "custom_collation", make(map[string]*Collation), "Custom", 4})
	defer RemoveCharset("custom")
	AddCollation(&Collation{99999, "custom", "custom_collation", true})

	tests := []struct {
		cs   string
		co   string
		succ bool
	}{
		{"custom", "custom_collation", true},
		{"utf8", "utf8_invalid_ci", false},
	}
	for _, tt := range tests {
		testValidCharset(t, tt.cs, tt.co, tt.succ)
	}
}

func BenchmarkGetCharsetDesc(b *testing.B) {
	b.ResetTimer()
	charsets := []string{CharsetUTF8, CharsetUTF8MB4, CharsetASCII, CharsetLatin1, CharsetBin}
	index := rand.Intn(len(charsets))
	cs := charsets[index]

	for i := 0; i < b.N; i++ {
		GetCharsetInfo(cs)
	}
}
