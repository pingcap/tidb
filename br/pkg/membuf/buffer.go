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

const (
	defaultPoolSize            = 1024
	defaultBlockSize           = 1 << 20 // 1M
	defaultLargeAllocThreshold = 1 << 16 // 64K
)

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
	allocator           Allocator
	blockSize           int
	blockCache          chan []byte
	largeAllocThreshold int
}

// Option configures a pool.
type Option func(p *Pool)

// WithBlockNum configures how many blocks cached by this pool.
func WithBlockNum(num int) Option {
	return func(p *Pool) {
		p.blockCache = make(chan []byte, num)
	}
}

// WithBlockSize configures the size of each block.
func WithBlockSize(bytes int) Option {
	return func(p *Pool) {
		p.blockSize = bytes
	}
}

// WithAllocator specifies the allocator used by pool to allocate and free memory.
func WithAllocator(allocator Allocator) Option {
	return func(p *Pool) {
		p.allocator = allocator
	}
}

// WithLargeAllocThreshold configures the threshold for large allocation of a Buffer.
// If allocate size is larger than this threshold, bytes will be allocated directly
// by the make built-in function and won't be tracked by the pool.
func WithLargeAllocThreshold(threshold int) Option {
	return func(p *Pool) {
		p.largeAllocThreshold = threshold
	}
}

// NewPool creates a new pool.
func NewPool(opts ...Option) *Pool {
	p := &Pool{
		allocator:           stdAllocator{},
		blockSize:           defaultBlockSize,
		blockCache:          make(chan []byte, defaultPoolSize),
		largeAllocThreshold: defaultLargeAllocThreshold,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

func (p *Pool) acquire() []byte {
	select {
	case b := <-p.blockCache:
		return b
	default:
		return p.allocator.Alloc(p.blockSize)
	}
}

func (p *Pool) release(b []byte) {
	select {
	case p.blockCache <- b:
	default:
		p.allocator.Free(b)
	}
}

// PreAllocPoolSize pre-allocates given memory for the pool.
func (p *Pool) PreAllocPoolSize(bytes int) *Pool {
	for i := 0; i < bytes/p.blockSize; i++ {
		p.blockCache <- p.allocator.Alloc(p.blockSize)
	}
	return p
}

// Destroy frees all buffers.
func (p *Pool) Destroy() {
	close(p.blockCache)
	for b := range p.blockCache {
		p.allocator.Free(b)
	}
}

// TotalSize is the total memory size of this Pool.
func (p *Pool) TotalSize() int64 {
	return int64(len(p.blockCache) * p.blockSize)
}

// Buffer represents the reuse buffer.
type Buffer struct {
	pool          *Pool
	blocks        [][]byte
	blockCntLimit int
	curBlock      []byte
	curBlockIdx   int
	curIdx        int
}

// BufferOption configures a buffer.
type BufferOption func(*Buffer)

// WithMemoryLimit approximately limits the maximum memory size of this Buffer.
// Due to it use blocks to allocate memory, the actual memory size is
// blockSize*ceil(limit/blockSize).
// In order to keep compatibility, it will only restrict AllocBytesWithSliceLocation.
func WithMemoryLimit(limit uint64) BufferOption {
	return func(b *Buffer) {
		blockCntLimit := int(limit+uint64(b.pool.blockSize)-1) / b.pool.blockSize
		b.blockCntLimit = blockCntLimit
		b.blocks = make([][]byte, 0, blockCntLimit)
	}
}

// NewBuffer creates a new buffer in current pool.
func (p *Pool) NewBuffer(opts ...BufferOption) *Buffer {
	b := &Buffer{
		pool:          p,
		blocks:        make([][]byte, 0, 128),
		curBlockIdx:   -1,
		blockCntLimit: -1,
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// Reset resets the buffer.
func (b *Buffer) Reset() {
	if len(b.blocks) > 0 {
		b.curBlock = b.blocks[0]
		b.curBlockIdx = 0
		b.curIdx = 0
	}
}

// Destroy frees all buffers.
func (b *Buffer) Destroy() {
	for _, buf := range b.blocks {
		b.pool.release(buf)
	}
	b.blocks = nil
}

// TotalSize represents the total memory size of this Buffer.
func (b *Buffer) TotalSize() int64 {
	return int64(len(b.blocks) * b.pool.blockSize)
}

// AllocBytes allocates bytes with the given length.
func (b *Buffer) AllocBytes(n int) []byte {
	if n > b.pool.largeAllocThreshold {
		return make([]byte, n)
	}
	if b.curIdx+n > len(b.curBlock) {
		b.addBuf()
	}
	idx := b.curIdx
	b.curIdx += n
	return b.curBlock[idx:b.curIdx:b.curIdx]
}

// addBuf adds buffer to Buffer.
func (b *Buffer) addBuf() {
	if b.curBlockIdx < len(b.blocks)-1 {
		b.curBlockIdx++
		b.curBlock = b.blocks[b.curBlockIdx]
	} else {
		buf := b.pool.acquire()
		b.blocks = append(b.blocks, buf)
		b.curBlock = buf
		b.curBlockIdx = len(b.blocks) - 1
	}

	b.curIdx = 0
}

// SliceLocation is like a reflect.SliceHeader, but it's associated with a
// Buffer. The advantage is that it's smaller than a slice, and it doesn't
// contain a pointer thus more GC-friendly.
type SliceLocation struct {
	bufIdx int32
	offset int32
	length int32
}

// AllocBytesWithSliceLocation is like AllocBytes, but it also returns a
// SliceLocation. The expected usage is after writing data into returned slice we
// do not store the slice itself, but only the SliceLocation. Later we can use
// the SliceLocation to get the slice again. When we have a large number of
// slices in memory this can improve performance.
// nil returned slice means allocation failed.
func (b *Buffer) AllocBytesWithSliceLocation(n int) ([]byte, SliceLocation) {
	if n > b.pool.blockSize {
		return nil, SliceLocation{}
	}

	if b.curIdx+n > len(b.curBlock) {
		if b.blockCntLimit >= 0 && len(b.blocks) >= b.blockCntLimit {
			return nil, SliceLocation{}
		}
		b.addBuf()
	}
	blockIdx := int32(b.curBlockIdx)
	offset := int32(b.curIdx)
	loc := SliceLocation{bufIdx: blockIdx, offset: offset, length: int32(n)}

	idx := b.curIdx
	b.curIdx += n
	return b.curBlock[idx:b.curIdx:b.curIdx], loc
}

func (b *Buffer) GetSlice(loc SliceLocation) []byte {
	return b.blocks[loc.bufIdx][loc.offset : loc.offset+loc.length]
}

// AddBytes adds the bytes into this Buffer.
func (b *Buffer) AddBytes(bytes []byte) []byte {
	buf := b.AllocBytes(len(bytes))
	copy(buf, bytes)
	return buf
}
