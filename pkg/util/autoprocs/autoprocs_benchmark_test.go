package autoprocs

import (
	"runtime"
	"sync"
	"testing"
	"time"
)

func BenchmarkAutoProcsWithMutexContention(b *testing.B) {
	// Save original GOMAXPROCS
	originalProcs := runtime.GOMAXPROCS(0)
	defer runtime.GOMAXPROCS(originalProcs)

	// Start with 4 GOMAXPROCS
	Start(4)
	defer Stop()

	// Create mutex contention
	var mu sync.Mutex
	var wg sync.WaitGroup

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(50)
		for j := 0; j < 50; j++ {
			go func() {
				defer wg.Done()
				for k := 0; k < 50; k++ {
					mu.Lock()
					time.Sleep(100 * time.Nanosecond)
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
	}
}

func BenchmarkAutoProcsWithGCPressure(b *testing.B) {
	// Save original GOMAXPROCS
	originalProcs := runtime.GOMAXPROCS(0)
	defer runtime.GOMAXPROCS(originalProcs)

	// Start with 4 GOMAXPROCS
	Start(4)
	defer Stop()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create GC pressure with smaller allocations
		data := make([][]byte, 50)
		for j := range data {
			data[j] = make([]byte, 512*1024)
		}
		runtime.GC()
	}
}

func BenchmarkAutoProcsWithMixedLoad(b *testing.B) {
	// Save original GOMAXPROCS
	originalProcs := runtime.GOMAXPROCS(0)
	defer runtime.GOMAXPROCS(originalProcs)

	// Start with 4 GOMAXPROCS
	Start(4)
	defer Stop()

	var mu sync.Mutex
	var wg sync.WaitGroup
	done := make(chan struct{})

	// Start background GC pressure with controlled frequency
	gcDone := make(chan struct{})
	go func() {
		defer close(gcDone)
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				data := make([]byte, 512*1024)
				_ = data
				runtime.GC()
			}
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(25)
		for j := 0; j < 25; j++ {
			go func() {
				defer wg.Done()
				for k := 0; k < 50; k++ {
					mu.Lock()
					time.Sleep(100 * time.Nanosecond)
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
	}
	b.StopTimer()
	close(done)
	<-gcDone
}

func BenchmarkBaselineMutexContention(b *testing.B) {
	// Same as BenchmarkAutoProcsWithMutexContention but without autoprocs
	var mu sync.Mutex
	var wg sync.WaitGroup

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(50)
		for j := 0; j < 50; j++ {
			go func() {
				defer wg.Done()
				for k := 0; k < 50; k++ {
					mu.Lock()
					time.Sleep(100 * time.Nanosecond)
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
	}
}
