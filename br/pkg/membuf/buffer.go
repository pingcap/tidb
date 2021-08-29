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

package membuf

const bigValueSize = 1 << 16 // 64K

var allocBufLen = 1 << 20 // 1M

// Allocator is the abstract interface for allocating and freeing memory.
type Allocator interface {
	Alloc(n int) []byte
	Free([]byte)
}

type stdAllocator struct{}

func (stdAllocator) Alloc(n int) []byte {
	return make([]byte, n)
}

func (stdAllocator) Free(_ []byte) {}

// Pool is like `sync.Pool`, which manages memory for all bytes buffers.
//
// NOTE: we don't used a `sync.Pool` because when will sync.Pool release is depending on the
// garbage collector which always release the memory so late. Use a fixed size chan to reuse
// can decrease the memory usage to 1/3 compare with sync.Pool.
type Pool struct {
	allocator Allocator
	recycleCh chan []byte
}

// NewPool creates a new pool.
func NewPool(size int, allocator Allocator) *Pool {
	return &Pool{
		allocator: allocator,
		recycleCh: make(chan []byte, size),
	}
}

func (p *Pool) acquire() []byte {
	select {
	case b := <-p.recycleCh:
		return b
	default:
		return p.allocator.Alloc(allocBufLen)
	}
}

func (p *Pool) release(b []byte) {
	select {
	case p.recycleCh <- b:
	default:
		p.allocator.Free(b)
	}
}

// NewBuffer creates a new buffer in current pool.
func (p *Pool) NewBuffer() *Buffer {
	return &Buffer{pool: p, bufs: make([][]byte, 0, 128), curBufIdx: -1}
}

var globalPool = NewPool(1024, stdAllocator{})

// NewBuffer creates a new buffer in global pool.
func NewBuffer() *Buffer { return globalPool.NewBuffer() }

// Buffer represents the reuse buffer.
type Buffer struct {
	pool      *Pool
	bufs      [][]byte
	curBuf    []byte
	curIdx    int
	curBufIdx int
	curBufLen int
}

// addBuf adds buffer to Buffer.
func (b *Buffer) addBuf() {
	if b.curBufIdx < len(b.bufs)-1 {
		b.curBufIdx++
		b.curBuf = b.bufs[b.curBufIdx]
	} else {
		buf := b.pool.acquire()
		b.bufs = append(b.bufs, buf)
		b.curBuf = buf
		b.curBufIdx = len(b.bufs) - 1
	}

	b.curBufLen = len(b.curBuf)
	b.curIdx = 0
}

// Reset resets the buffer.
func (b *Buffer) Reset() {
	if len(b.bufs) > 0 {
		b.curBuf = b.bufs[0]
		b.curBufLen = len(b.bufs[0])
		b.curBufIdx = 0
		b.curIdx = 0
	}
}

// Destroy frees all buffers.
func (b *Buffer) Destroy() {
	for _, buf := range b.bufs {
		b.pool.release(buf)
	}
	b.bufs = nil
}

// TotalSize represents the total memory size of this Buffer.
func (b *Buffer) TotalSize() int64 {
	return int64(len(b.bufs) * allocBufLen)
}

// AllocBytes allocates bytes with the given length.
func (b *Buffer) AllocBytes(n int) []byte {
	if n > bigValueSize {
		return make([]byte, n)
	}
	if b.curIdx+n > b.curBufLen {
		b.addBuf()
	}
	idx := b.curIdx
	b.curIdx += n
	return b.curBuf[idx:b.curIdx:b.curIdx]
}

// AddBytes adds the bytes into this Buffer.
func (b *Buffer) AddBytes(bytes []byte) []byte {
	buf := b.AllocBytes(len(bytes))
	copy(buf, bytes)
	return buf
}
