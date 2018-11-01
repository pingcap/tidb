package rand

import (
	"math/rand"
	"sync/atomic"
)

// Pool represents a pool of random number generators.
// To generate a random id, round robin through the source pool with atomic increment.
// With more and more goroutines, Pool improves the performance of Random vs naive global random
// mutex exponentially.
// Try tests with 20000 goroutines and 500 calls to observe the difference
type Pool struct {
	sources []NumberGenerator
	counter uint64 // used for round robin
	size    uint64
}

// see bit twiddling hacks: https://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
func nextNearestPow2uint64(v uint64) uint64 {
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v |= v >> 32
	v++
	return v
}

// NewPool takes in a size and creates a pool of random id generators with size equal to next closest power of 2.
// eg: NewPool(10) returns a pool with 2^4 = 16 random sources.
func NewPool(seed int64, size uint64) *Pool {
	groupsize := nextNearestPow2uint64(size)
	pool := &Pool{
		size:    groupsize,
		sources: make([]NumberGenerator, groupsize),
	}
	// seed the pool
	pool.seed(seed)
	return pool
}

// seed initializes the pool using a randomized sequence with given seed.
func (r *Pool) seed(seed int64) {
	// init a random sequence to seed all sources
	seedRan := rand.NewSource(seed)
	for i := uint64(0); i < r.size; i++ {
		r.sources[i] = NewLockedRand(seedRan.Int63())
	}
}

// Pick returns a NumberGenerator from a pool of NumberGenerators
func (r *Pool) Pick() NumberGenerator {
	// use round robin with fast modulus of pow2 numbers
	selection := atomic.AddUint64(&r.counter, 1) & (r.size - 1)
	return r.sources[selection]
}
