// Copyright 2025 PingCAP, Inc.
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

// fmsketch-bench is a standalone benchmark for FMSketch allocation, merge,
// serialization, and GC behavior. Unlike testing.B benchmarks it gives full
// control over GC timing and measures heap/pause metrics that testing.B
// suppresses.
//
// To keep memory bounded, all three phases are run per-column: for each
// column we process all partitions through Phase 1→2→3 before moving to
// the next column. Peak live memory is O(partitions) sketches for one column.
//
// Usage:
//
//	go run ./cmd/fmsketch-bench/                         # PR branch (default)
//	go run -tags fmsketch_pool ./cmd/fmsketch-bench/     # master branch
//	go run ./cmd/fmsketch-bench/ -destroy                # exercise destroy path
package main

import (
	"flag"
	"fmt"
	"math/rand/v2"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tipb/go-tipb"
)

// Command-line flags.
var (
	flagPartitions = flag.Int("partitions", 64, "number of partitions")
	flagColumns    = flag.Int("columns", 20, "columns/indexes per table")
	flagRegions    = flag.Int("regions", 50, "regions per partition")
	flagEntries    = flag.Int("entries", 500, "hash entries per region FMSketch")
	flagMaxSize    = flag.Int("maxsize", 10000, "MaxSketchSize for partition/global sketches")
	flagOverlap    = flag.Float64("overlap", 0.3, "fraction of hash values shared between regions of same (partition,col)")
	flagGOGC       = flag.Int("gogc", 100, "GOGC percentage (-1 to disable GC)")
	flagRepeat     = flag.Int("repeat", 1, "number of repetitions")
	flagDestroy    = flag.Bool("destroy", false, "call destroySketch instead of plain nil assignment")
)

// memSnapshot holds the fields we compare before/after a phase.
type memSnapshot struct {
	totalAlloc uint64
	mallocs    uint64
	numGC      uint32
	pauseTotal uint64
	pauseNs    [256]uint64
	heapInuse  uint64
}

func takeSnapshot() memSnapshot {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return memSnapshot{
		totalAlloc: m.TotalAlloc,
		mallocs:    m.Mallocs,
		numGC:      m.NumGC,
		pauseTotal: m.PauseTotalNs,
		pauseNs:    m.PauseNs,
		heapInuse:  m.HeapInuse,
	}
}

// phaseResult captures accumulated measurements for one phase across all columns.
type phaseResult struct {
	wallTime   time.Duration
	allocBytes uint64
	allocCount uint64
	peakHeap   uint64
	gcRuns     uint32
	gcPauseNs  uint64
	maxPauseNs uint64
}

func (r *phaseResult) accumulate(before, after memSnapshot) {
	r.allocBytes += after.totalAlloc - before.totalAlloc
	r.allocCount += after.mallocs - before.mallocs
	r.gcRuns += after.numGC - before.numGC
	r.gcPauseNs += after.pauseTotal - before.pauseTotal
	if mp := maxGCPause(before, after); mp > r.maxPauseNs {
		r.maxPauseNs = mp
	}
	// Track peak heap from snapshot boundaries.
	if before.heapInuse > r.peakHeap {
		r.peakHeap = before.heapInuse
	}
	if after.heapInuse > r.peakHeap {
		r.peakHeap = after.heapInuse
	}
}

func (r phaseResult) print() {
	fmt.Printf("  Wall time:       %s\n", r.wallTime.Round(time.Millisecond))
	fmt.Printf("  Allocations:     %d allocs, %.1f MB\n", r.allocCount, float64(r.allocBytes)/(1<<20))
	fmt.Printf("  Peak heap:       %.1f MB\n", float64(r.peakHeap)/(1<<20))
	fmt.Printf("  GC runs:         %d\n", r.gcRuns)
	fmt.Printf("  Total GC pause:  %.3fms\n", float64(r.gcPauseNs)/1e6)
	fmt.Printf("  Max GC pause:    %.3fms\n", float64(r.maxPauseNs)/1e6)
}

// maxGCPause returns the maximum single GC pause between two snapshots.
func maxGCPause(before, after memSnapshot) uint64 {
	gcsBefore := before.numGC
	gcsAfter := after.numGC
	if gcsAfter <= gcsBefore {
		return 0
	}
	var maxP uint64
	// PauseNs is a circular buffer of size 256. GC cycle n (1-indexed) stores
	// its pause at PauseNs[(n+255)%256]. See runtime.MemStats documentation.
	start := gcsBefore + 1
	for i := start; i <= gcsAfter; i++ {
		p := after.pauseNs[(i+255)%256]
		if p > maxP {
			maxP = p
		}
	}
	return maxP
}

// generateColumnRegionProtos generates region-level proto sketches for one
// (partition, column) pair. Returns [region]*tipb.FMSketch.
func generateColumnRegionProtos(partition, column, regions, entries int, overlap float64, seed uint64) []*tipb.FMSketch {
	// Deterministic per-(partition, column) RNG.
	state := seed ^ (uint64(partition)*0x9E3779B97F4A7C15 + uint64(column)*0x517CC1B727220A95 + 1)
	rng := rand.New(rand.NewPCG(state, state>>1|1))

	overlapCount := int(float64(entries) * overlap)
	if overlapCount < 0 {
		overlapCount = 0
	}
	uniqueCount := entries - overlapCount

	// Shared hash values for this (partition, column).
	shared := make([]uint64, overlapCount)
	for i := range shared {
		shared[i] = rng.Uint64()
	}

	protos := make([]*tipb.FMSketch, regions)
	for r := range protos {
		hashset := make([]uint64, 0, entries)
		hashset = append(hashset, shared...)
		for i := 0; i < uniqueCount; i++ {
			hashset = append(hashset, rng.Uint64())
		}
		protos[r] = &tipb.FMSketch{
			Mask:    0, // Raw region sketches have mask=0.
			Hashset: hashset,
		}
	}
	return protos
}

func main() {
	flag.Parse()

	partitions := *flagPartitions
	columns := *flagColumns
	regions := *flagRegions
	entries := *flagEntries
	maxSize := *flagMaxSize
	overlap := *flagOverlap
	gogc := *flagGOGC
	repeat := *flagRepeat
	destroy := *flagDestroy

	if gogc == -1 {
		debug.SetGCPercent(-1)
	} else {
		debug.SetGCPercent(gogc)
	}

	fmt.Println("=== FMSketch Benchmark ===")
	fmt.Printf("Config: partitions=%d columns=%d regions=%d entries=%d maxsize=%d overlap=%.2f GOGC=%d destroy=%v\n",
		partitions, columns, regions, entries, maxSize, overlap, gogc, destroy)
	fmt.Printf("Go: %s %s/%s\n", runtime.Version(), runtime.GOOS, runtime.GOARCH)
	fmt.Println()

	for iter := 0; iter < repeat; iter++ {
		if repeat > 1 {
			fmt.Printf("--- Iteration %d/%d ---\n", iter+1, repeat)
		}

		iterSeed := uint64(42 + iter*997)
		var results [3]phaseResult

		// Process column by column to bound memory at O(partitions) sketches.
		for c := 0; c < columns; c++ {
			if columns >= 10 && c%(columns/10) == 0 {
				fmt.Printf("  column %d/%d...\n", c+1, columns)
			}

			// Phase 1: Region -> Partition merge for this column.
			runtime.GC()
			snap0 := takeSnapshot()
			t1 := time.Now()

			sketches := make([]*statistics.FMSketch, partitions)
			for p := 0; p < partitions; p++ {
				protos := generateColumnRegionProtos(p, c, regions, entries, overlap, iterSeed)
				ps := statistics.NewFMSketch(maxSize)
				for r := 0; r < regions; r++ {
					rs := statistics.FMSketchFromProto(protos[r])
					ps.MergeFMSketch(rs)
					if destroy {
						destroySketch(&rs)
					} else {
						rs = nil
					}
					protos[r] = nil
				}
				sketches[p] = ps
			}

			results[0].wallTime += time.Since(t1)
			snap1 := takeSnapshot()
			results[0].accumulate(snap0, snap1)

			// Phase 2: Save/Load round-trip for this column.
			t2 := time.Now()

			for p := 0; p < partitions; p++ {
				encoded, err := statistics.EncodeFMSketch(sketches[p])
				if err != nil {
					panic(fmt.Sprintf("encode error: %v", err))
				}
				if destroy {
					destroySketch(&sketches[p])
				} else {
					sketches[p] = nil
				}
				decoded, err := statistics.DecodeFMSketch(encoded)
				if err != nil {
					panic(fmt.Sprintf("decode error: %v", err))
				}
				sketches[p] = decoded
			}

			results[1].wallTime += time.Since(t2)
			snap2 := takeSnapshot()
			results[1].accumulate(snap1, snap2)

			// Phase 3: Partition -> Global merge for this column.
			t3 := time.Now()

			global := sketches[0]
			sketches[0] = nil
			for p := 1; p < partitions; p++ {
				if global == nil {
					global = sketches[p]
				} else {
					global.MergeFMSketch(sketches[p])
					if destroy {
						destroySketch(&sketches[p])
					} else {
						sketches[p] = nil
					}
				}
			}
			if global != nil {
				_ = global.NDV()
			}
			if destroy {
				destroySketch(&global)
			} else {
				global = nil
			}

			results[2].wallTime += time.Since(t3)
			snap3 := takeSnapshot()
			results[2].accumulate(snap2, snap3)
		}

		fmt.Printf("\nPhase 1: Region -> Partition merge (%d partitions x %d cols x %d regions)\n", partitions, columns, regions)
		results[0].print()
		fmt.Println()

		fmt.Printf("Phase 2: Save/Load round-trip (%d sketches)\n", partitions*columns)
		results[1].print()
		fmt.Println()

		fmt.Printf("Phase 3: Partition -> Global merge (%d cols x %d partitions)\n", columns, partitions)
		results[2].print()
		fmt.Println()

		// Overall summary.
		var totalWall time.Duration
		var totalGCPause uint64
		var peakHeap uint64
		var peakPhase int
		var maxPause uint64
		var maxPausePhase int
		for i, r := range results {
			totalWall += r.wallTime
			totalGCPause += r.gcPauseNs
			if r.peakHeap > peakHeap {
				peakHeap = r.peakHeap
				peakPhase = i + 1
			}
			if r.maxPauseNs > maxPause {
				maxPause = r.maxPauseNs
				maxPausePhase = i + 1
			}
		}
		fmt.Println("Overall:")
		fmt.Printf("  Total wall time: %s\n", totalWall.Round(time.Millisecond))
		fmt.Printf("  Peak heap:       %.1f MB (phase %d)\n", float64(peakHeap)/(1<<20), peakPhase)
		fmt.Printf("  Total GC pause:  %.3fms\n", float64(totalGCPause)/1e6)
		fmt.Printf("  Max GC pause:    %.3fms (phase %d)\n", float64(maxPause)/1e6, maxPausePhase)
		fmt.Println()
	}
}
