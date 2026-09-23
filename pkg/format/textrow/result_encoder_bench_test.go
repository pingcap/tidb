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

package textrow

import (
	"bytes"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/charset"
)

var benchmarkEncodedLengthSink int

func BenchmarkEncodeWithWideRows(b *testing.B) {
	cases := []struct {
		name   string
		enc    charset.Encoding
		first  []byte
		second []byte
	}{
		{name: "binary-noop", enc: charset.EncodingBinImpl, first: make([]byte, 119), second: make([]byte, 59)},
		{name: "gbk-transform", enc: charset.EncodingGBKImpl, first: bytes.Repeat([]byte("一"), 39), second: bytes.Repeat([]byte("二"), 19)},
	}
	for _, testCase := range cases {
		b.Run(testCase.name, func(b *testing.B) {
			encoder := NewResultEncoder(charset.CharsetUTF8MB4)
			total := 0
			b.ReportAllocs()
			for range b.N {
				for range 510 {
					total += len(encoder.encodeWith(testCase.first, testCase.enc))
					total += len(encoder.encodeWith(testCase.second, testCase.enc))
				}
			}
			benchmarkEncodedLengthSink = total
		})
	}
}
