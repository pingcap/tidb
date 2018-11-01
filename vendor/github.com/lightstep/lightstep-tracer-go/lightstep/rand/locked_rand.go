package rand

import (
	"math/rand"
	"sync"
)

// LockedRand implements NumberGenerator and embeds Rand that is safe for concurrent use.
type LockedRand struct {
	lk sync.Mutex
	r  *rand.Rand
}

// NewLockedRand creates a new LockedRand that implements all Rand functions that is safe
// for concurrent use.
func NewLockedRand(seed int64) *LockedRand {
	return &LockedRand{
		r: rand.New(rand.NewSource(seed)),
	}
}

// Seed uses the provided seed value to initialize the generator to a deterministic state.
// Seed should not be called concurrently with any other Rand method.
func (lr *LockedRand) Seed(seed int64) {
	lr.lk.Lock()
	lr.r.Seed(seed)
	lr.lk.Unlock()
}

// TwoInt63 generates 2 random int64 without locking twice.
func (lr *LockedRand) TwoInt63() (n1, n2 int64) {
	lr.lk.Lock()
	n1 = lr.r.Int63()
	n2 = lr.r.Int63()
	lr.lk.Unlock()
	return
}

// Int63 returns a non-negative pseudo-random 63-bit integer as an int64.
func (lr *LockedRand) Int63() (n int64) {
	lr.lk.Lock()
	n = lr.r.Int63()
	lr.lk.Unlock()
	return
}

// Uint32 returns a pseudo-random 32-bit value as a uint32.
func (lr *LockedRand) Uint32() (n uint32) {
	lr.lk.Lock()
	n = lr.r.Uint32()
	lr.lk.Unlock()
	return
}

// Uint64 returns a pseudo-random 64-bit value as a uint64.
func (lr *LockedRand) Uint64() (n uint64) {
	lr.lk.Lock()
	n = lr.r.Uint64()
	lr.lk.Unlock()
	return
}

// Int31 returns a non-negative pseudo-random 31-bit integer as an int32.
func (lr *LockedRand) Int31() (n int32) {
	lr.lk.Lock()
	n = lr.r.Int31()
	lr.lk.Unlock()
	return
}

// Int returns a non-negative pseudo-random int.
func (lr *LockedRand) Int() (n int) {
	lr.lk.Lock()
	n = lr.r.Int()
	lr.lk.Unlock()
	return
}

// Int63n returns, as an int64, a non-negative pseudo-random number in [0,n).
// It panics if n <= 0.
func (lr *LockedRand) Int63n(n int64) (r int64) {
	lr.lk.Lock()
	r = lr.r.Int63n(n)
	lr.lk.Unlock()
	return
}

// Int31n returns, as an int32, a non-negative pseudo-random number in [0,n).
// It panics if n <= 0.
func (lr *LockedRand) Int31n(n int32) (r int32) {
	lr.lk.Lock()
	r = lr.r.Int31n(n)
	lr.lk.Unlock()
	return
}

// Intn returns, as an int, a non-negative pseudo-random number in [0,n).
// It panics if n <= 0.
func (lr *LockedRand) Intn(n int) (r int) {
	lr.lk.Lock()
	r = lr.r.Intn(n)
	lr.lk.Unlock()
	return
}

// Float64 returns, as a float64, a pseudo-random number in [0.0,1.0).
func (lr *LockedRand) Float64() (n float64) {
	lr.lk.Lock()
	n = lr.r.Float64()
	lr.lk.Unlock()
	return
}

// Float32 returns, as a float32, a pseudo-random number in [0.0,1.0).
func (lr *LockedRand) Float32() (n float32) {
	lr.lk.Lock()
	n = lr.r.Float32()
	lr.lk.Unlock()
	return
}

// Perm returns, as a slice of n ints, a pseudo-random permutation of the integers [0,n).
func (lr *LockedRand) Perm(n int) (r []int) {
	lr.lk.Lock()
	r = lr.r.Perm(n)
	lr.lk.Unlock()
	return
}

// Read generates len(p) random bytes and writes them into p. It
// always returns len(p) and a nil error.
// Read should not be called concurrently with any other Rand method.
func (lr *LockedRand) Read(p []byte) (n int, err error) {
	lr.lk.Lock()
	n, err = lr.r.Read(p)
	lr.lk.Unlock()
	return
}
