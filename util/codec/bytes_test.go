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

package codec

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFastSlowFastReverse(t *testing.T) {
	t.Parallel()

	if !supportsUnaligned {
		return
	}
	b := []byte{1, 2, 3, 4, 5, 6, 7, 8, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247}
	r1 := b
	fastReverseBytes(b)
	r2 := b
	reverseBytes(r2)
	require.Equal(t, r1, r2)
}

func TestBytesCodec(t *testing.T) {
	t.Parallel()

	inputs := []struct {
		enc  []byte
		dec  []byte
		desc bool
	}{
		{[]byte{}, []byte{0, 0, 0, 0, 0, 0, 0, 0, 247}, false},
		{[]byte{}, []byte{255, 255, 255, 255, 255, 255, 255, 255, 8}, true},
		{[]byte{0}, []byte{0, 0, 0, 0, 0, 0, 0, 0, 248}, false},
		{[]byte{0}, []byte{255, 255, 255, 255, 255, 255, 255, 255, 7}, true},
		{[]byte{1, 2, 3}, []byte{1, 2, 3, 0, 0, 0, 0, 0, 250}, false},
		{[]byte{1, 2, 3}, []byte{254, 253, 252, 255, 255, 255, 255, 255, 5}, true},
		{[]byte{1, 2, 3, 0}, []byte{1, 2, 3, 0, 0, 0, 0, 0, 251}, false},
		{[]byte{1, 2, 3, 0}, []byte{254, 253, 252, 255, 255, 255, 255, 255, 4}, true},
		{[]byte{1, 2, 3, 4, 5, 6, 7}, []byte{1, 2, 3, 4, 5, 6, 7, 0, 254}, false},
		{[]byte{1, 2, 3, 4, 5, 6, 7}, []byte{254, 253, 252, 251, 250, 249, 248, 255, 1}, true},
		{[]byte{0, 0, 0, 0, 0, 0, 0, 0}, []byte{0, 0, 0, 0, 0, 0, 0, 0, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247}, false},
		{[]byte{0, 0, 0, 0, 0, 0, 0, 0}, []byte{255, 255, 255, 255, 255, 255, 255, 255, 0, 255, 255, 255, 255, 255, 255, 255, 255, 8}, true},
		{[]byte{1, 2, 3, 4, 5, 6, 7, 8}, []byte{1, 2, 3, 4, 5, 6, 7, 8, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247}, false},
		{[]byte{1, 2, 3, 4, 5, 6, 7, 8}, []byte{254, 253, 252, 251, 250, 249, 248, 247, 0, 255, 255, 255, 255, 255, 255, 255, 255, 8}, true},
		{[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9}, []byte{1, 2, 3, 4, 5, 6, 7, 8, 255, 9, 0, 0, 0, 0, 0, 0, 0, 248}, false},
		{[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9}, []byte{254, 253, 252, 251, 250, 249, 248, 247, 0, 246, 255, 255, 255, 255, 255, 255, 255, 7}, true},
	}

	for _, input := range inputs {
		require.Equal(t, len(input.dec), EncodedBytesLength(len(input.enc)))

		if input.desc {
			b := EncodeBytesDesc(nil, input.enc)
			require.Equal(t, input.dec, b)

			_, d, err := DecodeBytesDesc(b, nil)
			require.NoError(t, err)
			require.Equal(t, input.enc, d)
		} else {
			b := EncodeBytes(nil, input.enc)
			require.Equal(t, input.dec, b)

			_, d, err := DecodeBytes(b, nil)
			require.NoError(t, err)
			require.Equal(t, input.enc, d)
		}
	}

	// Test error decode.
	errInputs := [][]byte{
		{1, 2, 3, 4},
		{0, 0, 0, 0, 0, 0, 0, 247},
		{0, 0, 0, 0, 0, 0, 0, 0, 246},
		{0, 0, 0, 0, 0, 0, 0, 1, 247},
		{1, 2, 3, 4, 5, 6, 7, 8, 0},
		{1, 2, 3, 4, 5, 6, 7, 8, 255, 1},
		{1, 2, 3, 4, 5, 6, 7, 8, 255, 1, 2, 3, 4, 5, 6, 7, 8},
		{1, 2, 3, 4, 5, 6, 7, 8, 255, 1, 2, 3, 4, 5, 6, 7, 8, 255},
		{1, 2, 3, 4, 5, 6, 7, 8, 255, 1, 2, 3, 4, 5, 6, 7, 8, 0},
	}

	for _, input := range errInputs {
		_, _, err := DecodeBytes(input, nil)
		require.Error(t, err)
	}
}
