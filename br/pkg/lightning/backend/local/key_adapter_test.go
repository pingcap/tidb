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
	"unsafe"

	"github.com/stretchr/testify/require"
)

func randBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

func TestNoopKeyAdapter(t *testing.T) {
	keyAdapter := noopKeyAdapter{}
	key := randBytes(32)
	require.Len(t, key, keyAdapter.EncodedLen(key))
	encodedKey := keyAdapter.Encode(nil, key, 0)
	require.Equal(t, key, encodedKey)

	decodedKey, err := keyAdapter.Decode(nil, encodedKey)
	require.NoError(t, err)
	require.Equal(t, key, decodedKey)
}

func TestDupDetectKeyAdapter(t *testing.T) {
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
		require.Equal(t, keyAdapter.EncodedLen(input.key), len(result))

		// Decode the result.
		key, err := keyAdapter.Decode(nil, result)
		require.NoError(t, err)
		require.Equal(t, input.key, key)
	}
}

func TestDupDetectKeyOrder(t *testing.T) {
	keys := [][]byte{
		{0x0, 0x1, 0x2},
		{0x0, 0x1, 0x3},
		{0x0, 0x1, 0x3, 0x4},
		{0x0, 0x1, 0x3, 0x4, 0x0},
		{0x0, 0x1, 0x3, 0x4, 0x0, 0x0, 0x0},
	}
	keyAdapter := dupDetectKeyAdapter{}
	encodedKeys := make([][]byte, 0, len(keys))
	for _, key := range keys {
		encodedKeys = append(encodedKeys, keyAdapter.Encode(nil, key, 1))
	}
	sorted := sort.SliceIsSorted(encodedKeys, func(i, j int) bool {
		return bytes.Compare(encodedKeys[i], encodedKeys[j]) < 0
	})
	require.True(t, sorted)
}

func TestDupDetectEncodeDupKey(t *testing.T) {
	keyAdapter := dupDetectKeyAdapter{}
	key := randBytes(32)
	result1 := keyAdapter.Encode(nil, key, 10)
	result2 := keyAdapter.Encode(nil, key, 20)
	require.NotEqual(t, result1, result2)
}

func startWithSameMemory(x []byte, y []byte) bool {
	return cap(x) > 0 && cap(y) > 0 && uintptr(unsafe.Pointer(&x[:cap(x)][0])) == uintptr(unsafe.Pointer(&y[:cap(y)][0]))
}

func TestEncodeKeyToPreAllocatedBuf(t *testing.T) {
	keyAdapters := []KeyAdapter{noopKeyAdapter{}, dupDetectKeyAdapter{}}
	for _, keyAdapter := range keyAdapters {
		key := randBytes(32)
		buf := make([]byte, 256)
		buf2 := keyAdapter.Encode(buf[:4], key, 1)
		require.True(t, startWithSameMemory(buf, buf2))
		// Verify the encoded result first.
		key2, err := keyAdapter.Decode(nil, buf2[4:])
		require.NoError(t, err)
		require.Equal(t, key, key2)
	}
}

func TestDecodeKeyToPreAllocatedBuf(t *testing.T) {
	data := []byte{
		0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0xff, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf7,
		0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf,
	}
	keyAdapters := []KeyAdapter{noopKeyAdapter{}, dupDetectKeyAdapter{}}
	for _, keyAdapter := range keyAdapters {
		key, err := keyAdapter.Decode(nil, data)
		require.NoError(t, err)
		buf := make([]byte, 4+len(data))
		buf2, err := keyAdapter.Decode(buf[:4], data)
		require.NoError(t, err)
		require.True(t, startWithSameMemory(buf, buf2))
		require.Equal(t, key, buf2[4:])
	}
}

func TestDecodeKeyDstIsInsufficient(t *testing.T) {
	data := []byte{
		0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0xff, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf7,
		0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf,
	}
	keyAdapters := []KeyAdapter{noopKeyAdapter{}, dupDetectKeyAdapter{}}
	for _, keyAdapter := range keyAdapters {
		key, err := keyAdapter.Decode(nil, data)
		require.NoError(t, err)
		buf := make([]byte, 4, 6)
		copy(buf, []byte{'a', 'b', 'c', 'd'})
		buf2, err := keyAdapter.Decode(buf[:4], data)
		require.NoError(t, err)
		require.False(t, startWithSameMemory(buf, buf2))
		require.Equal(t, buf[:4], buf2[:4])
		require.Equal(t, key, buf2[4:])
	}
}
