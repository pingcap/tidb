package chunk

import (
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/cznic/mathutil"
	"github.com/pingcap/tidb/types"
)

var (
	// GlobalAllocator is the root allocator of all allocators.
	GlobalAllocator = NewMultiBufAllocator(uint(runtime.NumCPU()), 15, 64)
)

// Allocator is used to manage byte slices.
type Allocator interface {
	Alloc(l, c int) []byte
	Free(buf []byte)
	SetParent(a Allocator)
	Close()
}

type multiBufChan struct {
	allocIndex    uint32
	freeIndex     uint32
	numAllocators uint32
	maxCap        int
	allocators    []Allocator
}

// NewMultiBufAllocator creates a multiBufChan.
func NewMultiBufAllocator(numAllocators, bitN, bufSize uint) Allocator {
	m := &multiBufChan{0, 0, uint32(numAllocators), 1 << bitN, make([]Allocator, 0, numAllocators)}
	for i := 0; i < int(numAllocators); i++ {
		m.allocators = append(m.allocators, NewBufAllocator(bitN, bufSize))
	}
	return m
}

// Alloc alloc memory.
func (b *multiBufChan) Alloc(l, c int) []byte {
	return b.allocators[atomic.AddUint32(&b.allocIndex, 1)%b.numAllocators].Alloc(l, c)
}

// Free releases memory.
func (b *multiBufChan) Free(buf []byte) {
	b.allocators[atomic.AddUint32(&b.freeIndex, 1)%b.numAllocators].Free(buf)
}

// SetParent sets a parent for this allocator.
func (b *multiBufChan) SetParent(a Allocator) {
	for _, x := range b.allocators {
		x.SetParent(a)
	}
}

// Close closes this allocator.
func (b *multiBufChan) Close() {
	for _, x := range b.allocators {
		x.Close()
	}
}

var (
	capIndexBit           = 15
	allocIndex, freeIndex = getCapIndex(uint(capIndexBit))
	pad                   = make([]byte, (1<<uint(capIndexBit))+1)
)

func getCapIndex(bitN uint) ([]int, []int) {
	allocIndex := make([]int, int(1<<bitN)+1)
	freeIndex := make([]int, int(1<<bitN)+1)
	for i := uint(1); i <= bitN; i++ {
		left := 1 << (i - 1)
		right := 1 << i
		for j := left + 1; j <= right; j++ {
			allocIndex[j] = int(i)
		}
		for j := left; j < right; j++ {
			freeIndex[j] = int(i - 1)
		}
	}
	allocIndex[1] = 0
	freeIndex[1<<bitN] = int(bitN)
	return allocIndex, freeIndex
}

type bufChan struct {
	maxCap     int
	bufList    []chan []byte
	allocIndex []int
	freeIndex  []int
	pad        []byte
	parent     Allocator
}

// NewBufAllocator creates a bufChan.
func NewBufAllocator(bitN, bufSize uint) Allocator {
	b := &bufChan{
		maxCap:  1 << bitN,
		bufList: make([]chan []byte, bitN+1),
	}

	if int(bitN) <= capIndexBit {
		b.allocIndex = allocIndex
		b.freeIndex = freeIndex
		b.pad = pad
	} else {
		b.allocIndex, b.freeIndex = getCapIndex(bitN)
		b.pad = make([]byte, (1<<bitN)+1)
	}

	for i := uint(1); i <= bitN; i++ {
		b.bufList[i] = make(chan []byte, bufSize)
	}
	return b
}

// Alloc alloc memory.
func (b *bufChan) Alloc(l, c int) []byte {
	if c > b.maxCap {
		return make([]byte, l, c)
	}
	idx := b.allocIndex[c]
	select {
	case buf := <-b.bufList[idx]:
		if len(buf) > l {
			buf = buf[:l]
		} else if len(buf) < l {
			// TODO: remove this copy or make it faster
			buf = append(buf, b.pad[:l-len(buf)]...)
		}
		return buf
	default:
		if b.parent != nil {
			return b.parent.Alloc(l, c)
		}
		return make([]byte, l, c)
	}
}

// Free releases memory.
func (b *bufChan) Free(buf []byte) {
	if cap(buf) > b.maxCap {
		return
	}
	idx := b.freeIndex[cap(buf)]
	select {
	case b.bufList[idx] <- buf:
	default:
		if b.parent != nil {
			b.parent.Free(buf)
		}
	}
}

// SetParent sets a parent for this allocator.
func (b *bufChan) SetParent(pb Allocator) {
	b.parent = pb
}

// Close closes this allocator.
func (b *bufChan) Close() {
	for _, ch := range b.bufList {
		for len(ch) > 0 {
			buf := <-ch
			b.parent.Free(buf)
		}
	}
}

var (
	chunkPool = sync.Pool{
		New: func() interface{} {
			return new(Chunk)
		},
	}
	columnPool = sync.Pool{
		New: func() interface{} {
			return new(column)
		},
	}
)

// NewChunkWithAllocator creates a chunk with a specific allocator.
func NewChunkWithAllocator(a Allocator, fields []*types.FieldType, cap, maxChunkSize int) *Chunk {
	if a == nil {
		return New(fields, cap, maxChunkSize)
	}

	cap = mathutil.Min(cap, maxChunkSize)
	chk := chunkPool.Get().(*Chunk)
	chk.columns = make([]*column, 0, len(fields))
	for _, f := range fields {
		elemLen := getFixedLen(f)
		col := columnPool.Get().(*column)
		if elemLen == varElemLen {
			estimatedElemLen := 8
			// TODO: make a buffer pool for offsets
			//col.offsets = *(*[]int64)(unsafe.Pointer(&a.Alloc(8, (cap+1)*8)))
			col.offsets = make([]int64, 1, cap+1)
			col.data = a.Alloc(0, cap*estimatedElemLen)
			col.nullBitmap = a.Alloc(0, cap>>3)
			col.elemBuf = nil
		} else {
			col.elemBuf = a.Alloc(elemLen, elemLen)
			col.data = a.Alloc(0, cap*elemLen)
			col.nullBitmap = a.Alloc(0, cap>>3)
			col.offsets = nil
		}
		col.length = 0
		col.nullCount = 0
		chk.columns = append(chk.columns, col)
	}
	chk.capacity = cap
	chk.numVirtualRows = 0
	chk.requiredRows = maxChunkSize
	chk.a = a
	return chk
}

// ReleaseChunk releases this chunk.
func ReleaseChunk(chk *Chunk) {
	if chk.a == nil {
		return
	}
	for _, c := range chk.columns {
		if c.cantReuse {
			continue
		}
		if c.offsets != nil { // varElemLen
			c.offsets = nil
		} else {
			chk.a.Free(c.elemBuf)
			c.elemBuf = nil
		}
		chk.a.Free(c.data)
		c.data = nil
		chk.a.Free(c.nullBitmap)
		c.nullBitmap = nil
		columnPool.Put(c)
	}
	chk.columns = nil
	chunkPool.Put(chk)
}
