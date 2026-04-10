// Copyright 2021 PingCAP, Inc.
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

package test

import (
	"fmt"
	"testing"
	"unicode/utf8"

	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/transform"
)

func RunEncoding(t *testing.T) {
	enc := charset.FindEncoding(charset.CharsetGBK)
	require.Equal(t, charset.CharsetGBK, enc.Name())

	txt := []byte("一二三四")
	e, _ := charset.Lookup("gbk")
	gbkEncodedTxt, _, err := transform.Bytes(e.NewEncoder(), txt)
	require.NoError(t, err)
	result, err := enc.Transform(nil, gbkEncodedTxt, charset.OpDecode)
	require.NoError(t, err)
	require.Equal(t, txt, result)

	gbkEncodedTxt2, err := enc.Transform(nil, txt, charset.OpEncode)
	require.NoError(t, err)
	require.Equal(t, gbkEncodedTxt2, gbkEncodedTxt)
	result, err = enc.Transform(nil, gbkEncodedTxt2, charset.OpDecode)
	require.NoError(t, err)
	require.Equal(t, txt, result)

	GBKCases := []struct {
		utf8Str string
		result  string
		isValid bool
	}{
		{"一二三", "涓?簩涓?", false}, // MySQL reports '涓?簩涓'.
		{"一二三123", "涓?簩涓?23", false},
		{"测试", "娴嬭瘯", true},
		{"案1案2", "妗?妗?", false},
		{"焊䏷菡釬", "鐒婁彿鑿￠嚞", true},
		{"鞍杏以伊位依", "闉嶆潖浠ヤ紛浣嶄緷", true},
		{"移維緯胃萎衣謂違", "绉荤董绶?儍钀庤。璎傞仌", false},
		{"仆仂仗仞仭仟价伉佚估", "浠嗕粋浠椾粸浠?粺浠蜂級浣氫及", false},
		{"佝佗佇佶侈侏侘佻佩佰侑佯", "浣濅綏浣囦蕉渚堜緩渚樹交浣╀桨渚戜蒋", true},
		{"\x80", "?", false},
		{"\x80a", "?", false},
		{"\x80aa", "?a", false},
		{"aa\x80ab", "aa?b", false},
		{"a你好\x80a测试", "a浣犲ソ?娴嬭瘯", false},
		{"aa\x80", "aa?", false},
	}
	for _, tc := range GBKCases {
		cmt := fmt.Sprintf("%v", tc)
		result, err := enc.Transform(nil, []byte(tc.utf8Str), charset.OpDecodeReplace)
		if tc.isValid {
			require.NoError(t, err, cmt)
		} else {
			require.Error(t, err, cmt)
		}
		require.Equal(t, tc.result, string(result), cmt)
	}

	utf8Cases := []struct {
		utf8Str string
		result  string
		isValid bool
	}{
		{"一二三", "һ\xb6\xfe\xc8\xfd", true},
		{"🀁", "?", false},
		{"valid_string_🀁", "valid_string_?", false},
		{"€", "?", false},
		{"€a", "?a", false},
		{"a€aa", "a?aa", false},
		{"aaa€", "aaa?", false},
	}
	for _, tc := range utf8Cases {
		cmt := fmt.Sprintf("%v", tc)
		result, err := enc.Transform(nil, []byte(tc.utf8Str), charset.OpEncodeReplace)
		if tc.isValid {
			require.NoError(t, err, cmt)
		} else {
			require.Error(t, err, cmt)
		}
		require.Equal(t, tc.result, string(result), cmt)
	}
}

func RunEncodingValidate(t *testing.T) {
	oxfffefd := string([]byte{0xff, 0xfe, 0xfd})
	testCases := []struct {
		chs      string
		str      string
		expected string
		nSrc     int
		ok       bool
	}{
		{charset.CharsetASCII, "", "", 0, true},
		{charset.CharsetASCII, "qwerty", "qwerty", 6, true},
		{charset.CharsetASCII, "qwÊrty", "qw?rty", 2, false},
		{charset.CharsetASCII, "中文", "??", 0, false},
		{charset.CharsetASCII, "中文?qwert", "???qwert", 0, false},
		{charset.CharsetUTF8MB4, "", "", 0, true},
		{charset.CharsetUTF8MB4, "qwerty", "qwerty", 6, true},
		{charset.CharsetUTF8MB4, "qwÊrty", "qwÊrty", 7, true},
		{charset.CharsetUTF8MB4, "qwÊ合法字符串", "qwÊ合法字符串", 19, true},
		{charset.CharsetUTF8MB4, "😂", "😂", 4, true},
		{charset.CharsetUTF8MB4, oxfffefd, "???", 0, false},
		{charset.CharsetUTF8MB4, "中文" + oxfffefd, "中文???", 6, false},
		{charset.CharsetUTF8MB4, string(utf8.RuneError), "�", 3, true},
		{charset.CharsetUTF8, "", "", 0, true},
		{charset.CharsetUTF8, "qwerty", "qwerty", 6, true},
		{charset.CharsetUTF8, "qwÊrty", "qwÊrty", 7, true},
		{charset.CharsetUTF8, "qwÊ合法字符串", "qwÊ合法字符串", 19, true},
		{charset.CharsetUTF8, "😂", "?", 0, false},
		{charset.CharsetUTF8, "valid_str😂", "valid_str?", 9, false},
		{charset.CharsetUTF8, oxfffefd, "???", 0, false},
		{charset.CharsetUTF8, "中文" + oxfffefd, "中文???", 6, false},
		{charset.CharsetUTF8, string(utf8.RuneError), "�", 3, true},
		{charset.CharsetGBK, "", "", 0, true},
		{charset.CharsetGBK, "asdf", "asdf", 4, true},
		{charset.CharsetGBK, "中文", "中文", 6, true},
		{charset.CharsetGBK, "À", "?", 0, false},
		{charset.CharsetGBK, "中文À中文", "中文?中文", 6, false},
		{charset.CharsetGBK, "asdfÀ", "asdf?", 4, false},
		{charset.CharsetGB18030, "", "", 0, true},
		{charset.CharsetGB18030, "asdf", "asdf", 4, true},
		{charset.CharsetGB18030, "中文", "中文", 6, true},
		{charset.CharsetGB18030, "À", "À", 4, true},
		{charset.CharsetGB18030, "中文À中文", "中文À中文", 6, true},
		{charset.CharsetGB18030, "asdfÀ", "asdfÀ", 4, true},
		{charset.CharsetGB18030, "😂", "😂", 4, true},
	}
	for _, tc := range testCases {
		msg := fmt.Sprintf("%v", tc)
		enc := charset.FindEncoding(tc.chs)
		if tc.chs == charset.CharsetUTF8 {
			enc = charset.EncodingUTF8MB3StrictImpl
		}
		strBytes := []byte(tc.str)
		require.Equal(t, tc.ok, enc.IsValid(strBytes), msg)
		replace, _ := enc.Transform(nil, strBytes, charset.OpReplaceNoErr)
		require.Equal(t, tc.expected, string(replace), msg)
	}
}

func RunEncodingGB18030(t *testing.T) {
	enc := charset.FindEncoding(charset.CharsetGB18030)
	require.Equal(t, charset.CharsetGB18030, enc.Name())

	txt := []byte("一二三四")
	e, _ := charset.Lookup("gb18030")
	gb18030EncodedTxt, _, err := transform.Bytes(e.NewEncoder(), txt)
	require.NoError(t, err)
	result, err := enc.Transform(nil, gb18030EncodedTxt, charset.OpDecode)
	require.NoError(t, err)
	require.Equal(t, txt, result)

	gb18030EncodedTxt2, err := enc.Transform(nil, txt, charset.OpEncode)
	require.NoError(t, err)
	require.Equal(t, gb18030EncodedTxt2, gb18030EncodedTxt)
	result, err = enc.Transform(nil, gb18030EncodedTxt2, charset.OpDecode)
	require.NoError(t, err)
	require.Equal(t, txt, result)

	GB18030Cases := []struct {
		utf8Str string
		result  string
		isValid bool
	}{
		{"一二三", "涓?浜屼笁", false}, // MySQL reports '涓?簩涓'.
		{"一二三123", "涓?浜屼笁123", false},
		{"测试", "娴嬭瘯", true},
		{"案1案2", "妗?1妗?2", false},
		{"焊䏷菡釬", "鐒婁彿鑿￠嚞", true},
		{"鞍杏以伊位依", "闉嶆潖浠ヤ紛浣嶄緷", true},
		{"移維緯胃萎衣謂違", "绉荤董绶\ue21d儍钀庤。璎傞仌", true},
		{"仆仂仗仞仭仟价伉佚估", "浠嗕粋浠椾粸浠\ue15d粺浠蜂級浣氫及", true},
		{"佝佗佇佶侈侏侘佻佩佰侑佯", "浣濅綏浣囦蕉渚堜緩渚樹交浣╀桨渚戜蒋", true},
		{"\x80", "?", false},
		{"\x80a", "?a", false},
		{"\x80aa", "?aa", false},
		{"aa\x80ab", "aa?ab", false},
		{"a你好\x80a测试", "a浣犲ソ?a娴嬭瘯", false},
		{"aa\x80", "aa?", false},
		{"\xb0\xb2", "安", true},
		// rune error is encoded to gb18030, we must decode as-is and report no error
		{
			"\xb0\xb2\x84\x31\xa4\x37\x30\x84\x31\xa4\x37\x32",
			"安\ufffd0\ufffd2",
			true,
		},
		// check encoded rune error and invalid data together
		{"\x80\x84\x31\xa4\x37\x80\x84\x31\xa4\x37", "?\ufffd?\ufffd", false},
		{"\x84\x31\xa4\x37\x81", "\ufffd?", false},
	}
	for _, tc := range GB18030Cases {
		cmt := fmt.Sprintf("utf8Str: %s, result: %s, isValid: %t", tc.utf8Str, tc.result, tc.isValid)
		result, err := enc.Transform(nil, []byte(tc.utf8Str), charset.OpDecodeReplace)
		if tc.isValid {
			require.NoError(t, err, cmt)
		} else {
			require.Error(t, err, cmt)
		}
		require.Equal(t, tc.result, string(result), cmt)
	}

	utf8Cases := []struct {
		utf8Str string
		result  string
		isValid bool
	}{
		{"一二三", "һ\xb6\xfe\xc8\xfd", true},
		{"🀁", "\x948\xe11", true},
		{"€", "\xa2\xe3", true},
		{"€a", "\xa2\xe3a", true},
		{"a€aa", "a\xa2\xe3aa", true},
		{"aaa€", "aaa\xa2\xe3", true},
		{"ḿ", "\xa8\xbc", true},
	}
	for _, tc := range utf8Cases {
		cmt := fmt.Sprintf("%v", tc)
		result, err := enc.Transform(nil, []byte(tc.utf8Str), charset.OpEncodeReplace)
		if tc.isValid {
			require.NoError(t, err, cmt)
		} else {
			require.Error(t, err, cmt)
		}
		require.Equal(t, tc.result, string(result), cmt)
	}
}
