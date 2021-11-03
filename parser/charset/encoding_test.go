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

package charset_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/parser/charset"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/transform"
)

func TestEncoding(t *testing.T) {
	t.Parallel()
	enc := charset.NewEncoding("gbk")
	require.Equal(t, "gbk", enc.Name())
	enc.UpdateEncoding("utf-8")
	require.Equal(t, "utf-8", enc.Name())
	enc.UpdateEncoding("gbk")
	require.Equal(t, "gbk", enc.Name())

	txt := []byte("一二三四")
	e, _ := charset.Lookup("gbk")
	gbkEncodedTxt, _, err := transform.Bytes(e.NewEncoder(), txt)
	require.NoError(t, err)
	result, err := enc.Decode(nil, gbkEncodedTxt)
	require.NoError(t, err)
	require.Equal(t, txt, result)

	gbkEncodedTxt2, err := enc.Encode(nil, txt)
	require.NoError(t, err)
	require.Equal(t, gbkEncodedTxt2, gbkEncodedTxt)
	result, err = enc.Decode(nil, gbkEncodedTxt2)
	require.NoError(t, err)
	require.Equal(t, txt, result)

	GBKCases := []struct {
		utf8Str string
		result  string
		isValid bool
	}{
		{"一二三", "涓?簩涓?", false}, // MySQL reports '涓?簩涓'.
		{"一二三123", "涓?簩涓?23", false},
		{"案1案2", "妗?妗?", false},
		{"焊䏷菡釬", "鐒婁彿鑿￠嚞", true},
		{"鞍杏以伊位依", "闉嶆潖浠ヤ紛浣嶄緷", true},
		{"移維緯胃萎衣謂違", "绉荤董绶?儍钀庤。璎傞仌", false},
		{"仆仂仗仞仭仟价伉佚估", "浠嗕粋浠椾粸浠?粺浠蜂級浣氫及", false},
		{"佝佗佇佶侈侏侘佻佩佰侑佯", "浣濅綏浣囦蕉渚堜緩渚樹交浣╀桨渚戜蒋", true},
	}
	for _, tc := range GBKCases {
		cmt := fmt.Sprintf("%v", tc)
		result, err = enc.Decode(nil, []byte(tc.utf8Str))
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
	}
	for _, tc := range utf8Cases {
		cmt := fmt.Sprintf("%v", tc)
		result, err = enc.Encode(nil, []byte(tc.utf8Str))
		if tc.isValid {
			require.NoError(t, err, cmt)
		} else {
			require.Error(t, err, cmt)
		}
		require.Equal(t, tc.result, string(result), cmt)
	}
}
