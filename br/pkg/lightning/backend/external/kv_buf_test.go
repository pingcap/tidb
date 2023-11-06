// Copyright 2023 PingCAP, Inc.
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

package external

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewPreAllocKVBuf(t *testing.T) {
	cases := []struct {
		memLimit     uint64
		expectBlocks int
	}{
		{1, 1},
		{15, 1},
		{16, 1},
		{17, 2},
		{31, 2},
		{32, 2},
	}
	for _, c := range cases {
		buf := newPreAllocKVBuf(c.memLimit, 16)
		require.Equal(t, c.expectBlocks, len(buf.blocks))
		require.Equal(t, 16, buf.blockSize)
		require.Equal(t, buf.blocks[0], buf.curBlock)
		require.Equal(t, 0, buf.curBlockIdx)
		require.Equal(t, 0, buf.curIdx)
	}

	buf := newPreAllocKVBuf(16, 8)
	// alloc larger than block size.
	_, _, _, allocated := buf.Alloc(9)
	require.False(t, allocated)
	blockIdx, res, offset, allocated := buf.Alloc(8)
	require.Equal(t, int32(0), blockIdx)
	require.Equal(t, int32(0), offset)
	require.True(t, allocated)
	copy(res, "12345678")
	blockIdx, res, offset, allocated = buf.Alloc(4)
	require.Equal(t, int32(1), blockIdx)
	require.Equal(t, int32(0), offset)
	require.True(t, allocated)
	copy(res, "aaaa")
	blockIdx, res, offset, allocated = buf.Alloc(4)
	require.Equal(t, int32(1), blockIdx)
	require.Equal(t, int32(4), offset)
	require.True(t, allocated)
	copy(res, "bbbb")
	_, _, _, allocated = buf.Alloc(4)
	require.False(t, allocated)

	require.Equal(t, "12345678", string(buf.blocks[0]))
	require.Equal(t, "aaaabbbb", string(buf.blocks[1]))

	buf.reset()
	require.Equal(t, buf.blocks[0], buf.curBlock)
	require.Equal(t, 0, buf.curBlockIdx)
	require.Equal(t, 0, buf.curIdx)

	buf.destroy()
	require.Nil(t, buf.blocks)
	require.Nil(t, buf.curBlock)
}
