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

const blockSize = 16 * units.MiB

type kvBuf struct {
	bufs      [][]byte
	curBuf    []byte
	curBufIdx int
	curIdx    int
	cap       int
}

func newKVBuf(memLimit uint64) *kvBuf {
	blockCount := (memLimit + blockSize - 1) / blockSize
	b := &kvBuf{
		bufs: make([][]byte, 0, blockCount),
	}
	for i := 0; i < int(blockCount); i++ {
		b.bufs = append(b.bufs, make([]byte, blockSize))
	}
	b.reset()
	return b
}

func (b *kvBuf) Alloc(s int) (bufIdx int32, res []byte, offset int, allocated bool) {
	if b.cap-b.curIdx < s {
		if b.curBufIdx+1 >= len(b.bufs) {
			return
		}
		b.curBufIdx++
		b.curBuf = b.bufs[b.curBufIdx]
		b.curIdx = 0
		b.cap = blockSize
	}
	bufIdx = int32(b.curBufIdx)
	res = b.curBuf[b.curIdx : b.curIdx+s]
	offset = b.curIdx
	allocated = true

	b.curIdx += s
	return
}

func (b *kvBuf) reset() {
	b.curBufIdx = 0
	b.curBuf = b.bufs[0]
	b.curIdx = 0
	b.cap = blockSize
}

func (b *kvBuf) destroy() {
	b.bufs = nil
	b.curBuf = nil
}
