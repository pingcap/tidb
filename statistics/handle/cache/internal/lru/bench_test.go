package lru

import (
	"sync"
	"testing"

	"github.com/pingcap/tidb/statistics/handle/cache/internal/testutil"
)

const defaultSize int64 = 1000

func BenchmarkLruPut(b *testing.B) {
	var (
		wg sync.WaitGroup
		c  = NewStatsLruCache(defaultSize)
	)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			t1 := testutil.NewMockStatisticsTable(1, 1, true, false, false)
			c.Put(int64(i), t1)
		}(i)
	}

	wg.Wait()

	b.StopTimer()
}
