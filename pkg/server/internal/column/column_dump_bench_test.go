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
// See the License for the specific language governing permissions and
// limitations under the License.

package column

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/server/internal/util"
	"github.com/stretchr/testify/require"
)

var benchmarkColumnInfoDumpSink []byte
var benchmarkResultEncoderSink *ResultEncoder

func BenchmarkNewResultEncoder(b *testing.B) {
	for _, testCase := range []struct {
		name string
		chs  string
	}{
		{name: "utf8mb4", chs: charset.CharsetUTF8MB4},
		{name: "binary", chs: charset.CharsetBin},
		{name: "gbk", chs: "gbk"},
		{name: "null", chs: ""},
	} {
		b.Run(testCase.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				benchmarkResultEncoderSink = NewResultEncoder(testCase.chs)
			}
		})
	}
}

func BenchmarkUpdateDataEncodingWideRows(b *testing.B) {
	encoder := NewResultEncoder(charset.CharsetUTF8MB4)
	b.ReportAllocs()
	for range b.N {
		for range 510 {
			encoder.UpdateDataEncoding(mysql.DefaultCollationID)
			encoder.UpdateDataEncoding(mysql.DefaultCollationID)
		}
	}
	benchmarkResultEncoderSink = encoder
}

func BenchmarkColumnInfoDump(b *testing.B) {
	pointSelectInfo := Info{
		Schema:       "sbtest",
		Table:        "sbtest1",
		OrgTable:     "sbtest1",
		Name:         "c",
		OrgName:      "c",
		ColumnLength: 120,
		Charset:      uint16(mysql.DefaultCollationID),
		Type:         mysql.TypeString,
	}
	gbkInfo := Info{
		Schema:       "数据库",
		Table:        "表",
		OrgTable:     "原表",
		Name:         "列",
		OrgName:      "原列",
		ColumnLength: 120,
		Charset:      uint16(mysql.DefaultCollationID),
		Type:         mysql.TypeString,
	}
	longNameInfo := pointSelectInfo
	longNameInfo.Name = strings.Repeat("n", 300)
	longNameInfo.OrgName = strings.Repeat("o", 300)
	emptyInfo := Info{
		Charset: uint16(mysql.DefaultCollationID),
		Type:    mysql.TypeString,
	}

	cases := []struct {
		name    string
		info    *Info
		encoder *ResultEncoder
	}{
		{name: "point-select/utf8mb4", info: &pointSelectInfo, encoder: NewResultEncoder(charset.CharsetUTF8MB4)},
		{name: "point-select/binary", info: &pointSelectInfo, encoder: NewResultEncoder(charset.CharsetBin)},
		{name: "point-select/gbk", info: &gbkInfo, encoder: NewResultEncoder("gbk")},
		{name: "long-names/utf8mb4", info: &longNameInfo, encoder: NewResultEncoder(charset.CharsetUTF8MB4)},
		{name: "empty-metadata/utf8mb4", info: &emptyInfo, encoder: NewResultEncoder(charset.CharsetUTF8MB4)},
	}

	for _, testCase := range cases {
		b.Run(testCase.name, func(b *testing.B) {
			buffer := make([]byte, 4, 1024)
			var result []byte
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result = testCase.info.Dump(buffer[:4], testCase.encoder)
			}
			benchmarkColumnInfoDumpSink = result
		})
	}
}

func TestDumpColumnGBKMetadata(t *testing.T) {
	info := Info{
		Schema:       "一",
		Table:        "一",
		OrgTable:     "一",
		Name:         "一",
		OrgName:      "一",
		ColumnLength: 120,
		Charset:      uint16(mysql.DefaultCollationID),
		Type:         mysql.TypeString,
	}
	buffer := info.Dump(nil, NewResultEncoder("gbk"))
	expectedFields := [][]byte{
		[]byte("def"),
		{0xd2, 0xbb},
		{0xd2, 0xbb},
		{0xd2, 0xbb},
		{0xd2, 0xbb},
		{0xd2, 0xbb},
	}

	for _, expected := range expectedFields {
		field, isNull, consumed, err := util.ParseLengthEncodedBytes(buffer)
		require.NoError(t, err)
		require.False(t, isNull)
		require.Equal(t, expected, field)
		buffer = buffer[consumed:]
	}
}
