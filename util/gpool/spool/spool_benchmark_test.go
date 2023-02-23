package spool

import (
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/resourcemanager/util"
	"github.com/tiancaiamao/gp"
	"golang.org/x/sync/errgroup"
)

const (
	RunTimes           = 1e6
	PoolCap            = 5e4
	BenchParam         = 35
	DefaultExpiredTime = 10 * time.Second
)

func demoFunc() {
	f(BenchParam)
}

func demoPoolFunc(args interface{}) {
	n := args.(int)
	f(n)
}

func f(n int) {
	if n == 0 {
		return
	}
	var useStack [100]byte
	_ = useStack[3]
	f(n - 1)
}

func BenchmarkGoroutines(b *testing.B) {
	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		wg.Add(RunTimes)
		for j := 0; j < RunTimes; j++ {
			go func() {
				demoFunc()
				wg.Done()
			}()
		}
		wg.Wait()
	}
}

func BenchmarkChannel(b *testing.B) {
	var wg sync.WaitGroup
	sema := make(chan struct{}, PoolCap)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(RunTimes)
		for j := 0; j < RunTimes; j++ {
			sema <- struct{}{}
			go func() {
				demoFunc()
				<-sema
				wg.Done()
			}()
		}
		wg.Wait()
	}
}

func BenchmarkErrGroup(b *testing.B) {
	var wg sync.WaitGroup
	var pool errgroup.Group
	pool.SetLimit(PoolCap)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(RunTimes)
		for j := 0; j < RunTimes; j++ {
			pool.Go(func() error {
				demoFunc()
				wg.Done()
				return nil
			})
		}
		wg.Wait()
	}
}

func BenchmarkSpoolPool(b *testing.B) {
	var wg sync.WaitGroup
	p, _ := NewPool("BenchmarkSpoolPool", PoolCap, util.UNKNOWN, WithExpiryDuration(DefaultExpiredTime))
	defer p.ReleaseAndWait()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(RunTimes)
		for j := 0; j < RunTimes; j++ {
			_ = p.Run(func() {
				demoFunc()
				wg.Done()
			})
		}
		wg.Wait()
	}
}

func BenchmarkGPPool(b *testing.B) {
	var wg sync.WaitGroup
	p := gp.New(PoolCap, 10*time.Second)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(RunTimes)
		for j := 0; j < RunTimes; j++ {
			p.Go(func() {
				demoFunc()
				wg.Done()
			})
		}
		wg.Wait()
	}
}

func BenchmarkGoroutinesThroughput(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for j := 0; j < RunTimes; j++ {
			go demoFunc()
		}
	}
}

func BenchmarkSemaphoreThroughput(b *testing.B) {
	sema := make(chan struct{}, PoolCap)
	for i := 0; i < b.N; i++ {
		for j := 0; j < RunTimes; j++ {
			sema <- struct{}{}
			go func() {
				demoFunc()
				<-sema
			}()
		}
	}
}

func BenchmarkSpoolPoolThroughput(b *testing.B) {
	p, _ := NewPool("BenchmarkSpoolPoolThroughput", PoolCap, util.UNKNOWN, WithExpiryDuration(DefaultExpiredTime))
	defer p.ReleaseAndWait()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < RunTimes; j++ {
			_ = p.Run(demoFunc)
		}
	}
}

func BenchmarkGPPoolThroughput(b *testing.B) {
	p := gp.New(PoolCap, 10*time.Second)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < RunTimes; j++ {
			p.Go(demoFunc)
		}
	}
}
