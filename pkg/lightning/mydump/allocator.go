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

	"github.com/pingcap/tidb/pkg/util/cpu"
	tidbmemory "github.com/pingcap/tidb/pkg/util/memory"
)

var (
	// arenaSize is the size of each arena
	arenaSize = 64 << 20

	// parserMemoryPercent defines the percentage of memory used for parser
	parserMemoryPercent = 0.3

	// parquetWriterPercent defines the percentage of memory used for parquet writer
	parquetWriterPercent = 0.4

	// otherWriterPercent defines the percentage of memory used for csv writer
	otherWriterPercent = 0.5
)

// GetMemoryForWriter gets the memory for writer according to the file type.
func GetMemoryForWriter(tp string, memPerCon int) int {
	switch tp {
	case "parquet":
		return int(float64(memPerCon) * parquetWriterPercent)
	default:
		return int(float64(memPerCon) * otherWriterPercent)
	}
}

// AdjustEncodeThreadCnt adjusts the concurrency in encode&sort step for parquet IMPORT INTO.
func AdjustEncodeThreadCnt(memoryPerFile, threadCnt int) int {
	totalCPU := cpu.GetCPUCount()
	totalMem, err := tidbmemory.MemTotal()
	if err != nil {
		return threadCnt
	}

	if totalCPU <= 0 || totalMem <= 0 {
		return threadCnt
	}

	// Use half of memory per conn for parquet parser
	memForImport := int(float64(int(totalMem)/totalCPU)*parserMemoryPercent) * threadCnt
	optimalThreads := memForImport / memoryPerFile
	return max(1, min(optimalThreads, threadCnt))
}

// Pool manages a pool of reusable byte buffers to reduce memory allocation overhead.
// It uses a buffered channel to store and reuse buffers efficiently.
type Pool struct {
	blockSize  int
	blockCache chan []byte
}

// GetPool gets a pool with the given capacity.
func GetPool(capacity int) *Pool {
	mem := int(float64(capacity) * parserMemoryPercent)
	return &Pool{
		blockSize:  arenaSize,
		blockCache: make(chan []byte, (mem+arenaSize-1)/arenaSize),
	}
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

type arena interface {
	allocate(int) []byte
	free([]byte)
	reset()
}
