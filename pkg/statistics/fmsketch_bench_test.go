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

package statistics

import (
	"fmt"
	"math/rand"
	"runtime"
	"runtime/debug"
	"testing"
)

// Pre-generate deterministic hash values to isolate map performance from hashing overhead.
func generateHashValues(n int) []uint64 {
	rng := rand.New(rand.NewSource(42))
	vals := make([]uint64, n)
	for i := range vals {
		vals[i] = rng.Uint64()
	}
	return vals
}

// buildBenchSketch builds a FMSketch by inserting pre-hashed values.
func buildBenchSketch(maxSize int, vals []uint64) *FMSketch {
	s := NewFMSketch(maxSize)
	for _, v := range vals {
		s.insertHashValue(v)
	}
	return s
}

func BenchmarkFMSketchInsert(b *testing.B) {
	const maxSize = 10000
	vals := generateHashValues(100000)

	b.Run("1k", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			buildBenchSketch(maxSize, vals[:1000])
		}
	})

	b.Run("10k", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			buildBenchSketch(maxSize, vals[:10000])
		}
	})

	b.Run("100k", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			buildBenchSketch(maxSize, vals[:100000])
		}
	})
}

func BenchmarkFMSketchMerge(b *testing.B) {
	const maxSize = 10000
	vals := generateHashValues(200000)

	b.Run("small", func(b *testing.B) {
		base := buildBenchSketch(maxSize, vals[:1000])
		other := buildBenchSketch(maxSize, vals[100000:101000])
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			s := base.Copy()
			s.MergeFMSketch(other)
		}
	})

	b.Run("full", func(b *testing.B) {
		base := buildBenchSketch(maxSize, vals[:100000])
		other := buildBenchSketch(maxSize, vals[100000:200000])
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			s := base.Copy()
			s.MergeFMSketch(other)
		}
	})
}

func BenchmarkFMSketchCopy(b *testing.B) {
	const maxSize = 10000
	vals := generateHashValues(100000)

	b.Run("small", func(b *testing.B) {
		s := buildBenchSketch(maxSize, vals[:1000])
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			_ = s.Copy()
		}
	})

	b.Run("full", func(b *testing.B) {
		s := buildBenchSketch(maxSize, vals[:100000])
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			_ = s.Copy()
		}
	})
}

func BenchmarkFMSketchNDV(b *testing.B) {
	const maxSize = 10000
	s := buildBenchSketch(maxSize, generateHashValues(100000))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = s.NDV()
	}
}

func BenchmarkFMSketchProto(b *testing.B) {
	const maxSize = 10000
	s := buildBenchSketch(maxSize, generateHashValues(100000))

	b.Run("ToProto", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_ = FMSketchToProto(s)
		}
	})

	b.Run("EncodeDecode", func(b *testing.B) {
		encoded, _ := EncodeFMSketch(s)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			_, _ = DecodeFMSketch(encoded)
		}
	})
}

// BenchmarkFMSketchLifecycle measures the full create-fill-use-destroy cycle.
// Before the PR: NewFMSketch gets from sync.Pool, destroy calls reset()+Put back.
// After the PR: NewFMSketch allocates, destroy sets nil and relies on GC.
// The destroySketch helper in fmsketch_bench_destroy_test.go must match the code under test.
func BenchmarkFMSketchLifecycle(b *testing.B) {
	const maxSize = 10000
	vals := generateHashValues(100000)

	for _, n := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprintf("fill_%dk", n/1000), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				s := buildBenchSketch(maxSize, vals[:n])
				_ = s.NDV()
				destroySketch(&s)
			}
		})
	}
}

// BenchmarkFMSketchGCImpact measures the GC cost of creating and discarding batches of sketches.
// This represents the analyze/global-stats path where many partition sketches are built,
// merged, and released.
// The destroySketch helper in fmsketch_bench_destroy_test.go must match the code under test.
func BenchmarkFMSketchGCImpact(b *testing.B) {
	const maxSize = 10000
	vals := generateHashValues(100000)

	for _, batchSize := range []int{10, 100} {
		b.Run(fmt.Sprintf("batch_%d/with_gc", batchSize), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				sketches := make([]*FMSketch, batchSize)
				for j := range sketches {
					sketches[j] = buildBenchSketch(maxSize, vals)
				}
				for j := range sketches {
					destroySketch(&sketches[j])
				}
				// Force GC to include reclamation cost in the benchmark.
				runtime.GC()
			}
		})
		b.Run(fmt.Sprintf("batch_%d/no_gc", batchSize), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				sketches := make([]*FMSketch, batchSize)
				for j := range sketches {
					sketches[j] = buildBenchSketch(maxSize, vals)
				}
				for j := range sketches {
					destroySketch(&sketches[j])
				}
			}
		})
	}
}

// BenchmarkFMSketchDestroy isolates the cost of destroying a batch of full sketches.
// Before the PR: DestroyAndPutToPool calls reset() which calls swiss.Map.Clear(),
// iterating every entry one-by-one — this is what dominates CPU in the flamegraph.
// After the PR: just nil the pointer.
// Measures build+destroy of a fixed batch. Compare with BenchmarkFMSketchInsert
// (build only) to isolate the destroy cost.
// The destroySketch helper in fmsketch_bench_destroy_test.go must match the code under test.
func BenchmarkFMSketchDestroy(b *testing.B) {
	const maxSize = 10000
	const batchSize = 100
	vals := generateHashValues(100000)

	// "build_only" measures just building the batch (baseline).
	b.Run("build_only", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			sketches := make([]*FMSketch, batchSize)
			for j := range sketches {
				sketches[j] = buildBenchSketch(maxSize, vals)
			}
			runtime.KeepAlive(sketches)
		}
	})

	// "build_and_destroy" measures building + destroying.
	// Subtract build_only to get the destroy cost.
	b.Run("build_and_destroy", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			sketches := make([]*FMSketch, batchSize)
			for j := range sketches {
				sketches[j] = buildBenchSketch(maxSize, vals)
			}
			for j := range sketches {
				destroySketch(&sketches[j])
			}
		}
	})
}

// BenchmarkFMSketchMemoryUsage reports actual vs estimated memory for a full sketch.
func BenchmarkFMSketchMemoryUsage(b *testing.B) {
	const maxSize = 10000
	vals := generateHashValues(100000)

	b.Run("reported_vs_actual", func(b *testing.B) {
		b.ReportAllocs()
		prev := debug.SetGCPercent(-1)
		runtime.GC()
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)
		s := buildBenchSketch(maxSize, vals)
		runtime.KeepAlive(s)
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		debug.SetGCPercent(prev)
		b.Logf("FMSketch NDV=%d: MemoryUsage()=%d B, actual heap=%d B, allocs=%d",
			s.NDV(), s.MemoryUsage(),
			after.TotalAlloc-before.TotalAlloc,
			after.Mallocs-before.Mallocs)
	})
}
