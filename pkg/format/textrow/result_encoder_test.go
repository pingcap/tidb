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

package textrow_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/format/textrow"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/require"
)

var benchmarkResultEncoderSink *textrow.ResultEncoder

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
				benchmarkResultEncoderSink = textrow.NewResultEncoder(testCase.chs)
			}
		})
	}
}

func BenchmarkUpdateDataEncodingWideRows(b *testing.B) {
	gbkCollation, err := charset.GetCollationByName(charset.CollationGBKBin)
	require.NoError(b, err)
	cases := []struct {
		name string
		ids  []uint16
	}{
		{name: "repeated", ids: []uint16{mysql.DefaultCollationID, mysql.DefaultCollationID}},
		{name: "changing", ids: []uint16{mysql.DefaultCollationID, uint16(gbkCollation.ID)}},
	}
	for _, testCase := range cases {
		b.Run(testCase.name, func(b *testing.B) {
			encoder := textrow.NewResultEncoder(charset.CharsetUTF8MB4)
			b.ReportAllocs()
			for range b.N {
				for range 510 {
					for _, id := range testCase.ids {
						encoder.UpdateDataEncoding(id)
					}
				}
			}
			benchmarkResultEncoderSink = encoder
		})
	}
}

func TestUpdateDataEncodingTransitions(t *testing.T) {
	gbkCollation, err := charset.GetCollationByName(charset.CollationGBKBin)
	require.NoError(t, err)

	encoder := textrow.NewResultEncoder(charset.CharsetBin)
	src := []byte("一")
	gbk := []byte{0xd2, 0xbb}

	encoder.UpdateDataEncoding(uint16(gbkCollation.ID))
	require.Equal(t, gbk, encoder.EncodeData(src))
	encoder.UpdateDataEncoding(uint16(gbkCollation.ID))
	require.Equal(t, gbk, encoder.EncodeData(src))

	encoder.UpdateDataEncoding(mysql.DefaultCollationID)
	require.Equal(t, src, encoder.EncodeData(src))
	encoder.UpdateDataEncoding(^uint16(0))
	require.Equal(t, src, encoder.EncodeData(src))
	encoder.UpdateDataEncoding(uint16(gbkCollation.ID))
	require.Equal(t, gbk, encoder.EncodeData(src))
}

func TestResultEncoder(t *testing.T) {
	// Encode bytes to utf-8.
	d := textrow.NewResultEncoder("utf-8")
	src := []byte("test_string")
	result := d.EncodeMeta(src)
	require.Equal(t, src, result)

	// Encode bytes to GBK.
	d = textrow.NewResultEncoder("gbk")
	result = d.EncodeMeta([]byte("一"))
	require.Equal(t, []byte{0xd2, 0xbb}, result)

	// Encode bytes to binary.
	d = textrow.NewResultEncoder("binary")
	result = d.EncodeMeta([]byte("一"))
	require.Equal(t, "一", string(result))
}

func TestIsStringColumnType(t *testing.T) {
	stringTypes := []byte{
		mysql.TypeString,
		mysql.TypeVarString,
		mysql.TypeVarchar,
		mysql.TypeBit,
		mysql.TypeTinyBlob,
		mysql.TypeMediumBlob,
		mysql.TypeLongBlob,
		mysql.TypeBlob,
		mysql.TypeEnum,
		mysql.TypeSet,
		mysql.TypeJSON,
		mysql.TypeTiDBVectorFloat32,
	}
	for _, tp := range stringTypes {
		require.True(t, textrow.IsStringColumnType(tp), "type %d", tp)
	}

	require.False(t, textrow.IsStringColumnType(mysql.TypeLonglong))
}
