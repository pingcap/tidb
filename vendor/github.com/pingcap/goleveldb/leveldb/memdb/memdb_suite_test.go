package memdb

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/goleveldb/leveldb/comparer"
	"github.com/pingcap/goleveldb/leveldb/testutil"
)

func TestMemDB(t *testing.T) {
	testutil.RunSuite(t, "MemDB Suite")
}

func TestRace(t *testing.T) {
	var wg sync.WaitGroup
	db := New(comparer.DefaultComparer, 0)

	for i := 0; i < 5000; i++ {
		wg.Add(1)
		go func(db *DB, wg *sync.WaitGroup) {
			defer wg.Done()

			for i := 0; i < 2000; i++ {
				if db.rnd.src.Int63()%5 == 0 {
					db.rnd.src.Seed(db.rnd.src.Int63())
				}
			}

		}(db, &wg)
	}
	wg.Wait()
}

func TestBitRand(t *testing.T) {
	src := rand.NewSource(int64(time.Now().Nanosecond()))
	rnd := &bitRand{
		src: src,
	}
	var slot [4]int

	for i := 0; i < 100000; i++ {
		slot[rnd.bitN(2)]++
	}

	sum := 0
	for i := 0; i < 4; i++ {
		x := slot[i] - 25000
		sum += x * x

		if sum >= 200000 {
			t.Fatalf("not so random %d! %d %d %d %d", sum, slot[0], slot[1], slot[2], slot[3])
		}
	}
}
