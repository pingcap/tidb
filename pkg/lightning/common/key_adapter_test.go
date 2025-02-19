// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"math"
	"sort"
	"testing"
	"time"
	"unsafe"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
)

func randBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

func TestNoopKeyAdapter(t *testing.T) {
	keyAdapter := NoopKeyAdapter{}
	key := randBytes(32)
	rowID := randBytes(8)
	require.Len(t, key, keyAdapter.EncodedLen(key, rowID))
	encodedKey := keyAdapter.Encode(nil, key, rowID)
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

	keyAdapter := DupDetectKeyAdapter{}
	for _, input := range inputs {
		encodedRowID := EncodeIntRowID(input.rowID)
		result := keyAdapter.Encode(nil, input.key, encodedRowID)
		require.Equal(t, keyAdapter.EncodedLen(input.key, encodedRowID), len(result))

		// Decode the result.
		key, err := keyAdapter.Decode(nil, result)
		require.NoError(t, err)
		require.Equal(t, input.key, key)
	}
}

func TestDecode(t *testing.T) {
	hexText := "7480000000000000FF6E5F698000000000FF00000F0166666666FF66656339FF2D6237FF61362D3466FF3562FF2D626664312DFF38FF39383831373237FFFF3066393869487636FFFF61775877455570FF52FF7450746D3678FF4D63FF47386B4E79FF385665FF67505A31FF39474650FF33377AFF33656B5862FF7278FF6E62346D4D66FF50FF50566E446D4155FFFF384F766A64363651FFFF5547574B68704BFF62FF314462577264FF3541FF764F577569FF4C3249FF76507953FF79474141FF765645FF6965706B4CFF6173FF70456143636DFF44FF594A6F4B563254FFFF4E767433687A6D43FFFF704C786B544137FF56FF4A577A4E4957FF3669FF4E67694A56FF655A50FF48477267FF69725A64FF376642FF446B46756DFF7A63FF42726956666CFF73FF56537945416A48FFFF7A35314B57787343FFFF3945526C6B6551FF5AFF437364763570FF784BFF384E417472FF4D4246FF73494569FF59756157FF664661FF4230747945FF6943FF793948634178FF62FF754C42724D736EFFFF5865795A756E6735FFFF38704249663877FF42FF73763977324DFF6F73FF666B4D7250FF575A61FF76384448FF4B683174FF716452FF34466E7061FF656BFF654632635A57FF54FF547867575A7570FFFF65725572657A4766FFFF3778456B564854FF64FF546E67344D37FF5046FF3565574D77FF4F5A4BFF5472744CFF6C503067FF610000FF0000000000F80000FD80000000002C1CBD000800"
	keyAdapter := DupDetectKeyAdapter{}
	bytes, err := hex.DecodeString(hexText)
	require.NoError(t, err)
	dst, err := keyAdapter.Decode(nil, bytes)
	require.NoError(t, err, "%x", dst)
}

func TestDupDetectKeyOrder(t *testing.T) {
	keys := [][]byte{
		{0x0, 0x1, 0x2},
		{0x0, 0x1, 0x3},
		{0x0, 0x1, 0x3, 0x4},
		{0x0, 0x1, 0x3, 0x4, 0x0},
		{0x0, 0x1, 0x3, 0x4, 0x0, 0x0, 0x0},
	}
	keyAdapter := DupDetectKeyAdapter{}
	encodedKeys := make([][]byte, 0, len(keys))
	for _, key := range keys {
		encodedKeys = append(encodedKeys, keyAdapter.Encode(nil, key, EncodeIntRowID(1)))
	}
	sorted := sort.SliceIsSorted(encodedKeys, func(i, j int) bool {
		return bytes.Compare(encodedKeys[i], encodedKeys[j]) < 0
	})
	require.True(t, sorted)
}

func TestDupDetectEncodeDupKey(t *testing.T) {
	keyAdapter := DupDetectKeyAdapter{}
	key := randBytes(32)
	result1 := keyAdapter.Encode(nil, key, EncodeIntRowID(10))
	result2 := keyAdapter.Encode(nil, key, EncodeIntRowID(20))
	require.NotEqual(t, result1, result2)
}

func startWithSameMemory(x []byte, y []byte) bool {
	return cap(x) > 0 && cap(y) > 0 && uintptr(unsafe.Pointer(&x[:cap(x)][0])) == uintptr(unsafe.Pointer(&y[:cap(y)][0]))
}

func TestEncodeKeyToPreAllocatedBuf(t *testing.T) {
	keyAdapters := []KeyAdapter{NoopKeyAdapter{}, DupDetectKeyAdapter{}}
	for _, keyAdapter := range keyAdapters {
		key := randBytes(32)
		buf := make([]byte, 256)
		buf2 := keyAdapter.Encode(buf[:4], key, EncodeIntRowID(1))
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
		0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf, 0x0, 0x8,
	}
	keyAdapters := []KeyAdapter{NoopKeyAdapter{}, DupDetectKeyAdapter{}}
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
		0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf, 0x0, 0x8,
	}
	keyAdapters := []KeyAdapter{NoopKeyAdapter{}, DupDetectKeyAdapter{}}
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

func TestMinRowID(t *testing.T) {
	keyApapter := DupDetectKeyAdapter{}
	key := []byte("key")
	val := []byte("val")
	shouldBeMin := keyApapter.Encode(key, val, MinRowID)

	rowIDs := make([][]byte, 0, 20)

	// DDL

	rowIDs = append(rowIDs, kv.IntHandle(math.MinInt64).Encoded())
	rowIDs = append(rowIDs, kv.IntHandle(-1).Encoded())
	rowIDs = append(rowIDs, kv.IntHandle(0).Encoded())
	rowIDs = append(rowIDs, kv.IntHandle(math.MaxInt64).Encoded())
	handleData := []types.Datum{
		types.NewIntDatum(math.MinInt64),
		types.NewIntDatum(-1),
		types.NewIntDatum(0),
		types.NewIntDatum(math.MaxInt64),
		types.NewBytesDatum(make([]byte, 1)),
		types.NewBytesDatum(make([]byte, 7)),
		types.NewBytesDatum(make([]byte, 8)),
		types.NewBytesDatum(make([]byte, 9)),
		types.NewBytesDatum(make([]byte, 100)),
	}
	for _, d := range handleData {
		encodedKey, err := codec.EncodeKey(time.Local, nil, d)
		require.NoError(t, err)
		ch, err := kv.NewCommonHandle(encodedKey)
		require.NoError(t, err)
		rowIDs = append(rowIDs, ch.Encoded())
	}

	// lightning, IMPORT INTO, ...

	numRowIDs := []int64{math.MinInt64, -1, 0, math.MaxInt64}
	for _, id := range numRowIDs {
		rowIDs = append(rowIDs, codec.EncodeComparableVarint(nil, id))
	}

	for _, id := range rowIDs {
		bs := keyApapter.Encode(key, val, id)
		require.True(t, bytes.Compare(bs, shouldBeMin) >= 0)
	}
}
