// Copyright 2022 PingCAP, Inc.
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

package kv

import (
	"testing"

	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"
)

func TestKVMemBufInterweaveAllocAndRecycle(t *testing.T) {
	type testCase struct {
		AllocSizes                []int
		FinalAvailableByteBufCaps []int
	}
	for _, tc := range []testCase{
		{
			AllocSizes: []int{
				1 * units.MiB,
				2 * units.MiB,
				3 * units.MiB,
				4 * units.MiB,
				5 * units.MiB,
			},
			// [2] => [2,4] => [2,4,8] => [4,2,8] => [4,2,8,16]
			FinalAvailableByteBufCaps: []int{
				4 * units.MiB,
				2 * units.MiB,
				8 * units.MiB,
				16 * units.MiB,
			},
		},
		{
			AllocSizes: []int{
				5 * units.MiB,
				4 * units.MiB,
				3 * units.MiB,
				2 * units.MiB,
				1 * units.MiB,
			},
			// [16] => [16] => [16] => [16] => [16]
			FinalAvailableByteBufCaps: []int{16 * units.MiB},
		},
		{
			AllocSizes: []int{5, 4, 3, 2, 1},
			// [1] => [1] => [1] => [1] => [1]
			FinalAvailableByteBufCaps: []int{1 * units.MiB},
		},
		{
			AllocSizes: []int{
				1 * units.MiB,
				2 * units.MiB,
				3 * units.MiB,
				2 * units.MiB,
				1 * units.MiB,
				5 * units.MiB,
			},
			// [2] => [2,4] => [2,4,8] => [2,8,4] => [8,4,2] => [8,4,2,16]
			FinalAvailableByteBufCaps: []int{
				8 * units.MiB,
				4 * units.MiB,
				2 * units.MiB,
				16 * units.MiB,
			},
		},
	} {
		testKVMemBuf := &MemBuf{}
		for _, allocSize := range tc.AllocSizes {
			testKVMemBuf.AllocateBuf(allocSize)
			testKVMemBuf.Recycle(testKVMemBuf.buf)
		}
		require.Equal(t, len(tc.FinalAvailableByteBufCaps), len(testKVMemBuf.availableBufs))
		for i, bb := range testKVMemBuf.availableBufs {
			require.Equal(t, tc.FinalAvailableByteBufCaps[i], bb.cap)
		}
	}
}

func TestKVMemBufBatchAllocAndRecycle(t *testing.T) {
	type testCase struct {
		AllocSizes                []int
		FinalAvailableByteBufCaps []int
	}
	testKVMemBuf := &MemBuf{}
	bBufs := []*BytesBuf{}
	for i := 0; i < maxAvailableBufSize; i++ {
		testKVMemBuf.AllocateBuf(1 * units.MiB)
		bBufs = append(bBufs, testKVMemBuf.buf)
	}
	for i := 0; i < maxAvailableBufSize; i++ {
		testKVMemBuf.AllocateBuf(2 * units.MiB)
		bBufs = append(bBufs, testKVMemBuf.buf)
	}
	for _, bb := range bBufs {
		testKVMemBuf.Recycle(bb)
	}
	require.Equal(t, maxAvailableBufSize, len(testKVMemBuf.availableBufs))
	for _, bb := range testKVMemBuf.availableBufs {
		require.Equal(t, 4*units.MiB, bb.cap)
	}
	bBufs = bBufs[:0]
	for i := 0; i < maxAvailableBufSize; i++ {
		testKVMemBuf.AllocateBuf(1 * units.MiB)
		bb := testKVMemBuf.buf
		require.Equal(t, 4*units.MiB, bb.cap)
		bBufs = append(bBufs, bb)
		require.Equal(t, maxAvailableBufSize-i-1, len(testKVMemBuf.availableBufs))
	}
	for _, bb := range bBufs {
		testKVMemBuf.Recycle(bb)
	}
	require.Equal(t, maxAvailableBufSize, len(testKVMemBuf.availableBufs))
}
