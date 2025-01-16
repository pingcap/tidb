// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
