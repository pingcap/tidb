// Copyright 2016 PingCAP, Inc.
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

package sync

import (
	"runtime"
	_ "unsafe"
)

const (
	// cacheLineSize is used in paddings to prevent false sharing;
	// 64B are used instead of 128B as a compromise between
	// memory footprint and performance; 128B usage may give ~30%
	// improvement on NUMA machines.
	cacheLineSize = 64
)

// nextPowOf2 computes the next highest power of 2 of 32-bit v.
// Source: https://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
func nextPowOf2(v uint32) uint32 {
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v++
	return v
}

func parallelism() uint32 {
	maxProcs := uint32(runtime.GOMAXPROCS(0))
	numCores := uint32(runtime.NumCPU())
	if maxProcs < numCores {
		return maxProcs
	}
	return numCores
}

// murmurhash3 64-bit finalizer
func mixhash64ptr(v uintptr) uint64 {
	x := uint64(v)
	x = ((x >> 33) ^ x) * 0xff51afd7ed558ccd
	x = ((x >> 33) ^ x) * 0xc4ceb9fe1a85ec53
	x = (x >> 33) ^ x
	return x
}

//go:noescape
//go:linkname fastrand runtime.fastrand
func fastrand() uint32
