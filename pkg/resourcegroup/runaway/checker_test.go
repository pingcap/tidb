package runaway

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tikv/client-go/v2/util"
)

func TestConcurrentResetAndCheckThresholds(t *testing.T) {
	checker := &Checker{}

	// Simulate concurrent calls to ResetTotalProcessedKeys and CheckThresholds
	var wg sync.WaitGroup
	numGoroutines := 5
	processKeys := int64(10)

	// Goroutines for CheckThresholds
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = checker.CheckThresholds(&util.RUDetails{}, processKeys, nil)
			}
		}()
	}

	// Goroutines for ResetTotalProcessedKeys
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				checker.ResetTotalProcessedKeys()
				time.Sleep(time.Millisecond) // simulate some delay
			}
		}()
	}

	// Wait for all goroutines to finish
	wg.Wait()

	// Final check to ensure no race conditions occurred
	finalValue := atomic.LoadInt64(&checker.totalProcessedKeys)
	assert.GreaterOrEqual(t, finalValue, int64(0), "unexpected negative totalProcessedKeys value")
}
