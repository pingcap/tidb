// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package hparser

import "unsafe"

const (
	// defaultBlockSize is the default arena block size (8 KB).
	// Grows by doubling for larger queries.
	defaultBlockSize = 8 * 1024

	// maxAlignment is the maximum alignment we guarantee for arena-allocated objects.
	maxAlignment = 8
)

// Arena is a bump-pointer memory allocator for AST nodes.
//
// All allocations from a single parse operation share one Arena. When the parser
// is reset, the arena's bump pointer is reset to zero—no per-node deallocation
// occurs, which eliminates GC pressure from AST construction entirely.
//
// Safety: Arena uses unsafe.Pointer arithmetic. All returned pointers are valid
// for the lifetime of the Arena (until Reset is called). The caller must not
// retain pointers after Reset.
type Arena struct {
	blocks []*block
	cur    int // index of current block in blocks
}

type block struct {
	data []byte
	used int
}

// NewArena creates a new Arena with a single pre-allocated block.
func NewArena() *Arena {
	a := &Arena{
		blocks: make([]*block, 0, 4),
	}
	a.blocks = append(a.blocks, newBlock(defaultBlockSize))
	return a
}

func newBlock(size int) *block {
	return &block{
		data: make([]byte, size),
		used: 0,
	}
}

// Reset resets the arena for reuse. All previously allocated memory becomes
// invalid. The backing memory is retained to avoid re-allocation.
func (a *Arena) Reset() {
	for _, b := range a.blocks {
		b.used = 0
	}
	a.cur = 0
}

// align rounds up n to the nearest multiple of maxAlignment.
func align(n int) int {
	return (n + maxAlignment - 1) &^ (maxAlignment - 1)
}

// alloc allocates size bytes from the arena, aligned to maxAlignment.
// If the current block is exhausted, a new block is appended.
func (a *Arena) alloc(size int) unsafe.Pointer {
	size = align(size)
	b := a.blocks[a.cur]
	if b.used+size > len(b.data) {
		// Current block exhausted. Try the next existing block or allocate a new one.
		a.cur++
		if a.cur < len(a.blocks) {
			b = a.blocks[a.cur]
			// The existing block might be too small for an oversized allocation.
			if size > len(b.data) {
				b = newBlock(max(size, defaultBlockSize))
				// Insert before the current position to keep larger blocks at end.
				a.blocks = append(a.blocks, nil)
				copy(a.blocks[a.cur+1:], a.blocks[a.cur:])
				a.blocks[a.cur] = b
			}
		} else {
			b = newBlock(max(size, defaultBlockSize))
			a.blocks = append(a.blocks, b)
		}
	}
	ptr := unsafe.Pointer(&b.data[b.used])
	b.used += size
	return ptr
}

// Alloc allocates a zero-initialized value of type T.
//
// NOTE: Currently uses heap allocation (new) instead of arena bump-pointer.
// The original arena approach used unsafe.Pointer into []byte blocks, which
// is invisible to Go's GC. When the parser goes out of scope but AST nodes
// survive (e.g. in the plan cache), the GC collects the arena's []byte blocks
// while AST nodes still reference them, causing SIGBUS. Using new() ensures
// AST nodes are properly GC-tracked. The arena parameter is retained for API
// compatibility and can be re-enabled once a GC-safe arena implementation
// (e.g., Go's native arena package) is available.
func Alloc[T any](_ *Arena) *T {
	return new(T)
}

// AllocSlice allocates a slice of n elements of type T.
// See Alloc for the rationale behind heap allocation.
func AllocSlice[T any](_ *Arena, n int) []T {
	if n == 0 {
		return nil
	}
	return make([]T, n)
}
