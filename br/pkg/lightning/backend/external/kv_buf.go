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

import "github.com/docker/go-units"

// DefaultBlockSize is the default block size for preAllocKVBuf.
const DefaultBlockSize = 16 * units.MiB

// preAllocKVBuf pre allocates a large buffer of limit memLimit to reduce memory
// allocation, all space in this buffer will be reused when reset.
type preAllocKVBuf struct {
	blocks      [][]byte
	blockSize   int
	curBlock    []byte
	curBlockIdx int
	curIdx      int
}

func newPreAllocKVBuf(memLimit uint64, blockSize int) *preAllocKVBuf {
	blockCount := (memLimit + uint64(blockSize) - 1) / uint64(blockSize)
	b := &preAllocKVBuf{
		blocks:    make([][]byte, 0, blockCount),
		blockSize: blockSize,
	}
	for i := 0; i < int(blockCount); i++ {
		b.blocks = append(b.blocks, make([]byte, blockSize))
	}
	b.reset()
	return b
}

func (b *preAllocKVBuf) Alloc(s int) (blockIdx int32, res []byte, offset int32, allocated bool) {
	if s > b.blockSize {
		return
	}
	if b.blockSize-b.curIdx < s {
		if b.curBlockIdx+1 >= len(b.blocks) {
			return
		}
		b.curBlockIdx++
		b.curBlock = b.blocks[b.curBlockIdx]
		b.curIdx = 0
	}
	blockIdx = int32(b.curBlockIdx)
	res = b.curBlock[b.curIdx : b.curIdx+s]
	offset = int32(b.curIdx)
	allocated = true

	b.curIdx += s
	return
}

func (b *preAllocKVBuf) reset() {
	b.curBlockIdx = 0
	b.curBlock = b.blocks[0]
	b.curIdx = 0
}

func (b *preAllocKVBuf) destroy() {
	b.blocks = nil
	b.curBlock = nil
}
