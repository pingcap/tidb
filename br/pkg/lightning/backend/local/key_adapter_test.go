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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package local

import (
	"bytes"
	"crypto/rand"
	"math"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func randBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

func TestNoopKeyAdapter(t *testing.T) {
	t.Parallel()

	keyAdapter := noopKeyAdapter{}
	key := randBytes(32)
	require.Equal(t, len(key), keyAdapter.EncodedLen(key))
	encodedKey := keyAdapter.Encode(nil, key, 0)
	require.EqualValues(t, key, encodedKey)

	decodedKey, err := keyAdapter.Decode(nil, encodedKey)
	require.NoError(t, err)
	require.EqualValues(t, key, decodedKey)
}

func TestDupDetectKeyAdapter(t *testing.T) {
	t.Parallel()

	inputs := []struct {
		key   []byte
		rowID int64
	}{
		{
			[]byte{0x0},
			0,
		},
		{
			randBytes(32),
			1,
		},
		{
			randBytes(32),
			math.MaxInt32,
		},
		{
			randBytes(32),
			math.MinInt32,
		},
	}

	keyAdapter := dupDetectKeyAdapter{}
	for _, input := range inputs {
		result := keyAdapter.Encode(nil, input.key, input.rowID)

		// Decode the result.
		key, err := keyAdapter.Decode(nil, result)
		require.NoError(t, err)
		require.EqualValues(t, input.key, key)
	}
}

func TestDupDetectKeyOrder(t *testing.T) {
	t.Parallel()

	keys := [][]byte{
		{0x0, 0x1, 0x2},
		{0x0, 0x1, 0x3},
		{0x0, 0x1, 0x3, 0x4},
		{0x0, 0x1, 0x3, 0x4, 0x0},
		{0x0, 0x1, 0x3, 0x4, 0x0, 0x0, 0x0},
	}
	keyAdapter := dupDetectKeyAdapter{}
	var encodedKeys [][]byte
	for _, key := range keys {
		encodedKeys = append(encodedKeys, keyAdapter.Encode(nil, key, 1))
	}
	sorted := sort.SliceIsSorted(encodedKeys, func(i, j int) bool {
		return bytes.Compare(encodedKeys[i], encodedKeys[j]) < 0
	})
	require.True(t, sorted)
}

func TestDupDetectEncodeDupKey(t *testing.T) {
	t.Parallel()

	keyAdapter := dupDetectKeyAdapter{}
	key := randBytes(32)
	result1 := keyAdapter.Encode(nil, key, 10)
	result2 := keyAdapter.Encode(nil, key, 20)
	require.NotEqual(t, result1, result2)
}

func TestEncodeKeyToPreAllocatedBuf(t *testing.T) {
	t.Parallel()

	keyAdapters := []KeyAdapter{noopKeyAdapter{}, dupDetectKeyAdapter{}}
	for _, keyAdapter := range keyAdapters {
		key := randBytes(32)
		buf := make([]byte, 256)
		buf2 := keyAdapter.Encode(buf[:0], key, 1)
		// Verify the encoded result first.
		key2, err := keyAdapter.Decode(nil, buf2)
		require.NoError(t, err)
		require.EqualValues(t, key, key2)
		// There should be no new slice allocated.
		// If we change a byte in `buf`, `buf2` can read the new byte.
		require.EqualValues(t, buf2, buf[:len(buf2)])
		buf[0]++
		require.Equal(t, buf2[0], buf[0])
	}
}

func TestDecodeKeyToPreAllocatedBuf(t *testing.T) {
	t.Parallel()

	keyAdapters := []KeyAdapter{noopKeyAdapter{}, dupDetectKeyAdapter{}}
	for _, keyAdapter := range keyAdapters {
		data := []byte{
			0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0xff, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf7,
			0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf,
		}
		buf := make([]byte, len(data))
		key, err := keyAdapter.Decode(buf[:0], data)
		require.NoError(t, err)
		// There should be no new slice allocated.
		// If we change a byte in `buf`, `buf2` can read the new byte.
		require.EqualValues(t, key[:len(buf)], buf)
		buf[0]++
		require.Equal(t, key[0], buf[0])
	}
}
