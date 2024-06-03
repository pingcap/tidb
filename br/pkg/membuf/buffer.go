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

import "unsafe"

const (
	defaultPoolSize  = 1024
	defaultBlockSize = 1 << 20 // 1M
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

// Pool is like `sync.Pool`, which manages memory for all bytes buffers. You can
// use Pool.NewBuffer to create a new buffer, and use Buffer.Destroy to release
// its memory to the pool. Pool can provide fixed size []byte blocks to Buffer.
//
// NOTE: we don't used a `sync.Pool` because when will sync.Pool release is depending on the
// garbage collector which always release the memory so late. Use a fixed size chan to reuse
// can decrease the memory usage to 1/3 compare with sync.Pool.
type Pool struct {
	allocator  Allocator
	blockSize  int
	blockCache chan []byte
	limiter    *Limiter
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

// WithPoolMemoryLimiter controls the maximum memory returned to buffer. Note
// that when call AllocBytes with size larger than blockSize, the memory is not
// controlled by this limiter.
func WithPoolMemoryLimiter(limiter *Limiter) Option {
	return func(p *Pool) {
		p.limiter = limiter
	}
}

// NewPool creates a new pool.
func NewPool(opts ...Option) *Pool {
	p := &Pool{
		allocator:  stdAllocator{},
		blockSize:  defaultBlockSize,
		blockCache: make(chan []byte, defaultPoolSize),
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

func (p *Pool) acquire() []byte {
	if p.limiter != nil {
		p.limiter.Acquire(p.blockSize)
	}
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
	if p.limiter != nil {
		p.limiter.Release(p.blockSize)
	}
}

// Destroy frees all buffers.
func (p *Pool) Destroy() {
	close(p.blockCache)
	for b := range p.blockCache {
		p.allocator.Free(b)
	}
}

// TotalSize is the total memory size of this Pool, not considering its Buffer.
func (p *Pool) TotalSize() int64 {
	return int64(len(p.blockCache) * p.blockSize)
}

// Buffer represents a buffer that can allocate []byte from its memory.
type Buffer struct {
	pool          *Pool
	blocks        [][]byte
	blockCntLimit int
	curBlock      []byte
	curBlockIdx   int
	curIdx        int

	smallObjOverhead      int
	smallObjOverheadCache int
}

// BufferOption configures a buffer.
type BufferOption func(*Buffer)

// WithBufferMemoryLimit approximately limits the maximum memory size of this
// Buffer. Due to it use blocks to allocate memory, the actual memory size is
// blockSize*ceil(limit/blockSize).
func WithBufferMemoryLimit(limit uint64) BufferOption {
	return func(b *Buffer) {
		blockCntLimit := int(limit+uint64(b.pool.blockSize)-1) / b.pool.blockSize
		b.blockCntLimit = blockCntLimit
		b.blocks = make([][]byte, 0, blockCntLimit)
	}
}

// NewBuffer creates a new buffer in current pool. The buffer can gradually
// acquire memory from the pool and release all memory once it's not used.
func (p *Pool) NewBuffer(opts ...BufferOption) *Buffer {
	b := &Buffer{
		pool:          p,
		curBlockIdx:   -1,
		blockCntLimit: -1,
	}
	for _, opt := range opts {
		opt(b)
	}
	if b.blocks == nil {
		b.blocks = make([][]byte, 0, 128)
	}
	return b
}

// smallObjOverheadBatch is the batch size to acquire memory from limiter. 256KB
// can store 256KB/24B = 10922 []byte objects, or 256KB/12B = 21845 SliceLocation
// objects.
const smallObjOverheadBatch = 256 * 1024

// recordSmallObjOverhead records the memory cost of []byte or SliceLocation into
// pool's limiter. The caller will ensure the pool's limiter is not nil.
func (b *Buffer) recordSmallObjOverhead(n int) {
	if n > b.smallObjOverheadCache {
		b.pool.limiter.Acquire(smallObjOverheadBatch)
		b.smallObjOverheadCache += smallObjOverheadBatch
		b.smallObjOverhead += smallObjOverheadBatch
	}
	b.smallObjOverheadCache -= n
}

// releaseSmallObjOverhead releases the memory cost of []byte or SliceLocation
// that are acquired from this Buffer before to the pool's limiter. The caller
// will ensure the pool's limiter is not nil.
func (b *Buffer) releaseSmallObjOverhead() {
	b.pool.limiter.Release(b.smallObjOverhead)
	b.smallObjOverhead = 0
	b.smallObjOverheadCache = 0
}

// Reset resets the buffer, the memory is still retained in this buffer. Caller
// must release the reference to the returned []byte or SliceLocation before
// calling Reset.
func (b *Buffer) Reset() {
	if b.pool.limiter != nil {
		b.releaseSmallObjOverhead()
	}
	if len(b.blocks) > 0 {
		b.curBlock = b.blocks[0]
		b.curBlockIdx = 0
		b.curIdx = 0
	}
}

// Destroy releases all buffers to the pool. Caller must release the reference to
// the returned []byte or SliceLocation before calling Destroy.
func (b *Buffer) Destroy() {
	if b.pool.limiter != nil {
		b.releaseSmallObjOverhead()
	}
	for _, buf := range b.blocks {
		b.pool.release(buf)
	}
	b.blocks = nil
	b.curBlock = nil
	b.curBlockIdx = -1
	b.curIdx = 0
}

// TotalSize represents the total memory size of this Buffer.
func (b *Buffer) TotalSize() int64 {
	return int64(len(b.blocks) * b.pool.blockSize)
}

var sizeOfSlice = int(unsafe.Sizeof([]byte{}))

// AllocBytes allocates bytes with the given length.
func (b *Buffer) AllocBytes(n int) []byte {
	if n > b.pool.blockSize {
		return make([]byte, n)
	}

	bs, _ := b.allocBytesWithSliceLocation(n)
	if bs != nil && b.pool.limiter != nil {
		b.recordSmallObjOverhead(sizeOfSlice)
	}
	return bs
}

// SliceLocation is like a reflect.SliceHeader, but it's associated with a
// Buffer. The advantage is that it's smaller than a slice, and it doesn't
// contain a pointer thus more GC-friendly.
type SliceLocation struct {
	bufIdx int32
	offset int32
	length int32
}

var sizeOfSliceLocation = int(unsafe.Sizeof(SliceLocation{}))

func (b *Buffer) allocBytesWithSliceLocation(n int) ([]byte, SliceLocation) {
	if n > b.pool.blockSize {
		return nil, SliceLocation{}
	}

	if b.curIdx+n > len(b.curBlock) {
		if b.blockCntLimit >= 0 && b.curBlockIdx+1 >= b.blockCntLimit {
			return nil, SliceLocation{}
		}
		b.addBlock()
	}
	blockIdx := int32(b.curBlockIdx)
	offset := int32(b.curIdx)
	loc := SliceLocation{bufIdx: blockIdx, offset: offset, length: int32(n)}

	idx := b.curIdx
	b.curIdx += n
	return b.curBlock[idx:b.curIdx:b.curIdx], loc
}

// AllocBytesWithSliceLocation is like AllocBytes, but it must allocate the
// buffer in the pool rather from go's runtime. The expected usage is after
// writing data into returned slice **we do not store the slice**, but only the
// SliceLocation. Later we can use the SliceLocation to get the slice again. When
// we have a large number of slices in memory this can reduce memory occupation.
// nil returned slice means allocation failed.
func (b *Buffer) AllocBytesWithSliceLocation(n int) ([]byte, SliceLocation) {
	bs, loc := b.allocBytesWithSliceLocation(n)
	if bs != nil && b.pool.limiter != nil {
		b.recordSmallObjOverhead(sizeOfSliceLocation)
	}
	return bs, loc
}

func (b *Buffer) addBlock() {
	if b.curBlockIdx < len(b.blocks)-1 {
		b.curBlockIdx++
		b.curBlock = b.blocks[b.curBlockIdx]
	} else {
		block := b.pool.acquire()
		b.blocks = append(b.blocks, block)
		b.curBlock = block
		b.curBlockIdx = len(b.blocks) - 1
	}

	b.curIdx = 0
}

func (b *Buffer) GetSlice(loc SliceLocation) []byte {
	return b.blocks[loc.bufIdx][loc.offset : loc.offset+loc.length]
}

// AddBytes adds the bytes into this Buffer's managed memory and return it.
func (b *Buffer) AddBytes(bytes []byte) []byte {
	buf := b.AllocBytes(len(bytes))
	copy(buf, bytes)
	return buf
}
