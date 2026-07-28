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
	"testing"

	"github.com/pingcap/tidb/pkg/parser/charset"
)

var benchmarkEncodedLengthSink int

func BenchmarkEncodeWithWideRows(b *testing.B) {
	encoder := NewResultEncoder(charset.CharsetUTF8MB4)
	c := make([]byte, 119)
	pad := make([]byte, 59)
	total := 0
	b.ReportAllocs()
	for range b.N {
		for range 510 {
			total += len(encoder.encodeWith(c, charset.EncodingBinImpl))
			total += len(encoder.encodeWith(pad, charset.EncodingBinImpl))
		}
	}
	benchmarkEncodedLengthSink = total
}
