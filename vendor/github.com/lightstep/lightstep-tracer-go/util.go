package lightstep

import (
	"math/rand"
	"sync"
	"time"
)

var (
	seededGUIDGen     *rand.Rand
	seededGUIDGenOnce sync.Once
	seededGUIDLock    sync.Mutex
)

func genSeededGUID() uint64 {
	// Golang does not seed the rng for us. Make sure it happens.
	seededGUIDGenOnce.Do(func() {
		seededGUIDGen = rand.New(rand.NewSource(time.Now().UnixNano()))
	})

	// The golang random generators are *not* intrinsically thread-safe.
	seededGUIDLock.Lock()
	defer seededGUIDLock.Unlock()
	return uint64(seededGUIDGen.Int63())
}

func genSeededGUID2() (uint64, uint64) {
	// Golang does not seed the rng for us. Make sure it happens.
	seededGUIDGenOnce.Do(func() {
		seededGUIDGen = rand.New(rand.NewSource(time.Now().UnixNano()))
	})

	seededGUIDLock.Lock()
	defer seededGUIDLock.Unlock()
	return uint64(seededGUIDGen.Int63()), uint64(seededGUIDGen.Int63())
}
