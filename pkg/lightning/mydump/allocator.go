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

package mydump

import (
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/pingcap/tidb/pkg/lightning/membuf"
)

var (
	// arenaSize is the size of each arena
	arenaSize = 64 << 20

	// maxParquetMemoryPercent defines the maximum percentage of memory used for parquet parser
	// Because less memory for writer can make more small files, which may affect performance
	// merge and ingest steps, so we set a limit here.
	maxParquetMemoryPercent = 30
)

// Pool manages a pool of reusable byte buffers to reduce memory allocation overhead.
// It uses a buffered channel to store and reuse buffers efficiently.
type Pool struct {
	blockSize  int
	blockCache chan []byte
	limit      int
	// As we may not be able to open all files concurrently due to memory usage,
	// we use a memory limiter to limit the memory usage of parquet parser.
	limiter *membuf.Limiter
}

// GetPool gets a pool with the given capacity.
func GetPool(capacity int) *Pool {
	limit := capacity * maxParquetMemoryPercent / 100
	return &Pool{
		blockSize:  arenaSize,
		blockCache: make(chan []byte, (limit+arenaSize-1)/arenaSize),
		limit:      limit,
		limiter:    membuf.NewLimiter(limit),
	}
}

// Acquire acquires memory from the pool.
func (p *Pool) Acquire(quota int) {
	p.limiter.Acquire(quota)
}

// Release releases memory to the pool.
func (p *Pool) Release(quota int) {
	p.limiter.Release(quota)
}

// Get retrieves a buffer from the pool or allocates a new one if the pool is empty.
func (p *Pool) Get() []byte {
	select {
	case buf := <-p.blockCache:
		return buf
	default:
		return make([]byte, p.blockSize)
	}
}

func (p *Pool) Put(buf []byte) {
	if buf == nil {
		return
	}

	select {
	case p.blockCache <- buf:
	default:
		// Pool is full, discard the buffer
	}
}

// addressOf returns the address of a buffer, return 0 if the buffer is nil or empty.
// This is used to create unique identifiers for tracking buffer allocations.
func addressOf(buf []byte) uintptr {
	if buf == nil || cap(buf) == 0 {
		return 0
	}
	buf = buf[:1]
	return uintptr(unsafe.Pointer(&buf[0]))
}

type AllocatorWithClose interface {
	memory.Allocator
	Close()
}
