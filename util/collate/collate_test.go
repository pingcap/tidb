// Copyright 2020 PingCAP, Inc.
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

package collate

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

type compareTable struct {
	Left   string
	Right  string
	Expect []int
}

type keyTable struct {
	Str    string
	Expect [][]byte
}

func testCompareTable(t *testing.T, collations []string, tests []compareTable) {
	for i, c := range collations {
		collator := GetCollator(c)
		for _, table := range tests {
			comment := fmt.Sprintf("Compare Left: %v Right: %v, Using %v", table.Left, table.Right, c)
			require.Equal(t, table.Expect[i], collator.Compare(table.Left, table.Right), comment)
		}
	}
}

func testKeyTable(t *testing.T, collations []string, tests []keyTable) {
	for i, c := range collations {
		collator := GetCollator(c)
		for _, test := range tests {
			comment := fmt.Sprintf("key %v, using %v", test.Str, c)
			require.Equal(t, test.Expect[i], collator.Key(test.Str), comment)
		}
	}
}

func TestUTF8CollatorCompare(t *testing.T) {
	SetNewCollationEnabledForTest(true)
	defer SetNewCollationEnabledForTest(false)
	collations := []string{"binary", "utf8mb4_bin", "utf8mb4_general_ci", "utf8mb4_unicode_ci", "gbk_bin", "gbk_chinese_ci"}
	tests := []compareTable{
		{"a", "b", []int{-1, -1, -1, -1, -1, -1}},
		{"a", "A", []int{1, 1, 0, 0, 1, 0}},
		{"À", "A", []int{1, 1, 0, 0, -1, -1}},
		{"abc", "abc", []int{0, 0, 0, 0, 0, 0}},
		{"abc", "ab", []int{1, 1, 1, 1, 1, 1}},
		{"😜", "😃", []int{1, 1, 0, 0, 0, 0}},
		{"a", "a ", []int{-1, 0, 0, 0, 0, 0}},
		{"a ", "a  ", []int{-1, 0, 0, 0, 0, 0}},
		{"a\t", "a", []int{1, 1, 1, 1, 1, 1}},
		{"ß", "s", []int{1, 1, 0, 1, -1, -1}},
		{"ß", "ss", []int{1, 1, -1, 0, -1, -1}},
		{"啊", "吧", []int{1, 1, 1, 1, -1, -1}},
		{"中文", "汉字", []int{-1, -1, -1, -1, 1, 1}},
	}
	testCompareTable(t, collations, tests)
}

func TestUTF8CollatorKey(t *testing.T) {
	SetNewCollationEnabledForTest(true)
	defer SetNewCollationEnabledForTest(false)
	collations := []string{"binary", "utf8mb4_bin", "utf8mb4_general_ci", "utf8mb4_unicode_ci", "gbk_bin", "gbk_chinese_ci"}
	tests := []keyTable{
		{"a", [][]byte{{0x61}, {0x61}, {0x0, 0x41}, {0x0E, 0x33}, {0x61}, {0x41}}},
		{"A", [][]byte{{0x41}, {0x41}, {0x0, 0x41}, {0x0E, 0x33}, {0x41}, {0x41}}},
		{"Foo © bar 𝌆 baz ☃ qux", [][]byte{
			{0x46, 0x6f, 0x6f, 0x20, 0xc2, 0xa9, 0x20, 0x62, 0x61, 0x72, 0x20, 0xf0, 0x9d, 0x8c, 0x86, 0x20, 0x62, 0x61, 0x7a, 0x20, 0xe2, 0x98, 0x83, 0x20, 0x71, 0x75, 0x78},
			{0x46, 0x6f, 0x6f, 0x20, 0xc2, 0xa9, 0x20, 0x62, 0x61, 0x72, 0x20, 0xf0, 0x9d, 0x8c, 0x86, 0x20, 0x62, 0x61, 0x7a, 0x20, 0xe2, 0x98, 0x83, 0x20, 0x71, 0x75, 0x78},
			{0x0, 0x46, 0x0, 0x4f, 0x0, 0x4f, 0x0, 0x20, 0x0, 0xa9, 0x0, 0x20, 0x0, 0x42, 0x0, 0x41, 0x0, 0x52, 0x0, 0x20, 0xff, 0xfd, 0x0, 0x20, 0x0, 0x42, 0x0, 0x41, 0x0, 0x5a, 0x0, 0x20, 0x26, 0x3, 0x0, 0x20, 0x0, 0x51, 0x0, 0x55, 0x0, 0x58},
			{0x0E, 0xB9, 0x0F, 0x82, 0x0F, 0x82, 0x02, 0x09, 0x02, 0xC5, 0x02, 0x09, 0x0E, 0x4A, 0x0E, 0x33, 0x0F, 0xC0, 0x02, 0x09, 0xFF, 0xFD, 0x02, 0x09, 0x0E, 0x4A, 0x0E, 0x33, 0x10, 0x6A, 0x02, 0x09, 0x06, 0xFF, 0x02, 0x09, 0x0F, 0xB4, 0x10, 0x1F, 0x10, 0x5A},
			{0x46, 0x6f, 0x6f, 0x20, 0x3f, 0x20, 0x62, 0x61, 0x72, 0x20, 0x3f, 0x20, 0x62, 0x61, 0x7a, 0x20, 0x3f, 0x20, 0x71, 0x75, 0x78},
			{0x46, 0x4f, 0x4f, 0x20, 0x3f, 0x20, 0x42, 0x41, 0x52, 0x20, 0x3f, 0x20, 0x42, 0x41, 0x5a, 0x20, 0x3f, 0x20, 0x51, 0x55, 0x58},
		}},
		{"a ", [][]byte{{0x61, 0x20}, {0x61}, {0x0, 0x41}, {0x0E, 0x33}, {0x61}, {0x41}}},
		{"ﷻ", [][]byte{
			{0xEF, 0xB7, 0xBB},
			{0xEF, 0xB7, 0xBB},
			{0xFD, 0xFB},
			{0x13, 0x5E, 0x13, 0xAB, 0x02, 0x09, 0x13, 0x5E, 0x13, 0xAB, 0x13, 0x50, 0x13, 0xAB, 0x13, 0xB7},
			{0x3f},
			{0x3F},
		}},
		{"中文", [][]byte{
			{0xE4, 0xB8, 0xAD, 0xE6, 0x96, 0x87},
			{0xE4, 0xB8, 0xAD, 0xE6, 0x96, 0x87},
			{0x4E, 0x2D, 0x65, 0x87},
			{0xFB, 0x40, 0xCE, 0x2D, 0xFB, 0x40, 0xE5, 0x87},
			{0xD6, 0xD0, 0xCE, 0xC4},
			{0xD3, 0x21, 0xC1, 0xAD},
		}},
	}
	testKeyTable(t, collations, tests)
}

func TestSetNewCollateEnabled(t *testing.T) {
	defer SetNewCollationEnabledForTest(false)

	SetNewCollationEnabledForTest(true)
	require.True(t, NewCollationEnabled())
}

func TestRewriteAndRestoreCollationID(t *testing.T) {
	SetNewCollationEnabledForTest(true)
	require.Equal(t, int32(-5), RewriteNewCollationIDIfNeeded(5))
	require.Equal(t, int32(-5), RewriteNewCollationIDIfNeeded(-5))
	require.Equal(t, int32(5), RestoreCollationIDIfNeeded(-5))
	require.Equal(t, int32(5), RestoreCollationIDIfNeeded(5))

	SetNewCollationEnabledForTest(false)
	require.Equal(t, int32(5), RewriteNewCollationIDIfNeeded(5))
	require.Equal(t, int32(-5), RewriteNewCollationIDIfNeeded(-5))
	require.Equal(t, int32(5), RestoreCollationIDIfNeeded(5))
	require.Equal(t, int32(-5), RestoreCollationIDIfNeeded(-5))
}

func TestGetCollator(t *testing.T) {
	SetNewCollationEnabledForTest(true)
	defer SetNewCollationEnabledForTest(false)
	require.IsType(t, &binCollator{}, GetCollator("binary"))
	require.IsType(t, &binPaddingCollator{}, GetCollator("utf8mb4_bin"))
	require.IsType(t, &binPaddingCollator{}, GetCollator("utf8_bin"))
	require.IsType(t, &generalCICollator{}, GetCollator("utf8mb4_general_ci"))
	require.IsType(t, &generalCICollator{}, GetCollator("utf8_general_ci"))
	require.IsType(t, &unicodeCICollator{}, GetCollator("utf8mb4_unicode_ci"))
	require.IsType(t, &unicodeCICollator{}, GetCollator("utf8_unicode_ci"))
	require.IsType(t, &zhPinyinTiDBASCSCollator{}, GetCollator("utf8mb4_zh_pinyin_tidb_as_cs"))
	require.IsType(t, &binPaddingCollator{}, GetCollator("default_test"))
	require.IsType(t, &binCollator{}, GetCollatorByID(63))
	require.IsType(t, &binPaddingCollator{}, GetCollatorByID(46))
	require.IsType(t, &binPaddingCollator{}, GetCollatorByID(83))
	require.IsType(t, &generalCICollator{}, GetCollatorByID(45))
	require.IsType(t, &generalCICollator{}, GetCollatorByID(33))
	require.IsType(t, &unicodeCICollator{}, GetCollatorByID(224))
	require.IsType(t, &unicodeCICollator{}, GetCollatorByID(192))
	require.IsType(t, &zhPinyinTiDBASCSCollator{}, GetCollatorByID(2048))
	require.IsType(t, &binPaddingCollator{}, GetCollatorByID(9999))

	SetNewCollationEnabledForTest(false)
	require.IsType(t, &binCollator{}, GetCollator("binary"))
	require.IsType(t, &binCollator{}, GetCollator("utf8mb4_bin"))
	require.IsType(t, &binCollator{}, GetCollator("utf8_bin"))
	require.IsType(t, &binCollator{}, GetCollator("utf8mb4_general_ci"))
	require.IsType(t, &binCollator{}, GetCollator("utf8_general_ci"))
	require.IsType(t, &binCollator{}, GetCollator("utf8mb4_unicode_ci"))
	require.IsType(t, &binCollator{}, GetCollator("utf8_unicode_ci"))
	require.IsType(t, &binCollator{}, GetCollator("utf8mb4_zh_pinyin_tidb_as_cs"))
	require.IsType(t, &binCollator{}, GetCollator("default_test"))
	require.IsType(t, &binCollator{}, GetCollatorByID(63))
	require.IsType(t, &binCollator{}, GetCollatorByID(46))
	require.IsType(t, &binCollator{}, GetCollatorByID(83))
	require.IsType(t, &binCollator{}, GetCollatorByID(45))
	require.IsType(t, &binCollator{}, GetCollatorByID(33))
	require.IsType(t, &binCollator{}, GetCollatorByID(224))
	require.IsType(t, &binCollator{}, GetCollatorByID(192))
	require.IsType(t, &binCollator{}, GetCollatorByID(2048))
	require.IsType(t, &binCollator{}, GetCollatorByID(9999))

	SetNewCollationEnabledForTest(true)
	defer SetNewCollationEnabledForTest(false)
	require.IsType(t, &gbkBinCollator{}, GetCollator("gbk_bin"))
	require.IsType(t, &gbkBinCollator{}, GetCollatorByID(87))
}
