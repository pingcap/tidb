// Copyright 2019 PingCAP, Inc.
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
package json

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeEscapedUnicode(t *testing.T) {
	in := "597d"
	r, size, err := decodeEscapedUnicode([]byte(in))
	require.NoError(t, err)
	require.Equal(t, "好\x00", string(r[:]))
	require.Equal(t, 3, size)
}

func BenchmarkDecodeEscapedUnicode(b *testing.B) {
	for i := 0; i < b.N; i++ {
		in := "597d"
		_, _, _ = decodeEscapedUnicode([]byte(in))
	}
}

func BenchmarkMergePatchBinary(b *testing.B) {
	valueA, _ := ParseBinaryFromString(`{"title":"Goodbye!","author":{"givenName":"John","familyName":"Doe"},"tags":["example","sample"],"content":"This will be unchanged"}`)
	valueB, _ := ParseBinaryFromString(`{"title":"Hello!","phoneNumber":"+01-123-456-7890","author":{"familyName":null},"tags":["example"]}`)
	for i := 0; i < b.N; i++ {
		_, _ = MergePatchBinary([]*BinaryJSON{&valueA, &valueB})
	}
}

func BenchmarkMergeBinary(b *testing.B) {
	valueA, _ := ParseBinaryFromString(`{"title":"Goodbye!","author":{"givenName":"John","familyName":"Doe"},"tags":["example","sample"],"content":"This will be unchanged"}`)
	valueB, _ := ParseBinaryFromString(`{"title":"Hello!","phoneNumber":"+01-123-456-7890","author":{"familyName":null},"tags":["example"]}`)
	for i := 0; i < b.N; i++ {
		_ = MergeBinary([]BinaryJSON{valueA, valueB})
	}
}
