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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package testutil

import (
	"hash/fnv"
	"math/rand"
	"testing"
	"time"
)

const (
	defaultTaskName                = "drr_test_task"
	defaultTaskStartPhysical int64 = 1_700_000_000_000
	regionIDTag                    = 'r'
)

type deterministicRNG struct {
	rng *rand.Rand
}

func newDeterministicRNG(seed int64, component string) *deterministicRNG {
	return &deterministicRNG{
		rng: rand.New(rand.NewSource(deriveDeterministicSeed(seed, component))),
	}
}

func deriveDeterministicSeed(seed int64, component string) int64 {
	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte(component))
	derived := uint64(seed) ^ hasher.Sum64()
	derived &^= uint64(1) << 63
	if derived == 0 {
		derived = 1
	}
	return int64(derived)
}

func (r *deterministicRNG) IntN(n int) int {
	return r.rng.Intn(n)
}

func (r *deterministicRNG) Int63n(n int64) int64 {
	return r.rng.Int63n(n)
}

func (r *deterministicRNG) Uint64InRange(lower, upper uint64) uint64 {
	if lower >= upper {
		return lower
	}
	return lower + uint64(r.Int63n(int64(upper-lower+1)))
}

// TestContext groups testing-only shared state for deterministic helpers.
type TestContext struct {
	T    testing.TB
	seed int64
}

func NewTestContext(t testing.TB) *TestContext {
	return NewTestContextWithSeed(t, time.Now().Unix())
}

func NewTestContextWithSeed(t testing.TB, seed int64) *TestContext {
	t.Helper()
	t.Log("SEED: ", seed)
	return &TestContext{T: t, seed: seed}
}

func (tc *TestContext) Seed() int64 {
	return tc.seed
}

func (tc *TestContext) RNG(component string) *deterministicRNG {
	return newDeterministicRNG(tc.Seed(), component)
}

// RegionBoundary describes a static region layout for a test.
type RegionBoundary struct {
	StartKey []byte
	EndKey   []byte
	StoreID  uint64
}

// RegionState is a read-only snapshot of one simulated region.
type RegionState struct {
	ID         uint64
	Epoch      uint64
	StoreID    uint64
	StartKey   []byte
	EndKey     []byte
	Checkpoint uint64
}

// FlushRecord tracks one simulated store flush and generated files.
type FlushRecord struct {
	Sequence     uint64
	StoreID      uint64
	RegionIDs    []uint64
	CheckpointTS uint64
	FlushTS      uint64
	MinTS        uint64
	MaxTS        uint64
	MetadataPath string
	LogPaths     []string
}

func (r FlushRecord) clone() FlushRecord {
	out := r
	out.RegionIDs = append([]uint64(nil), r.RegionIDs...)
	out.LogPaths = append([]string(nil), r.LogPaths...)
	return out
}
