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

package vardef

import (
	"sync"
	"testing"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func TestIsMDLEnabledInNextGen(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("only run in next-gen")
	}
	bak := enableMDL.Load()
	t.Cleanup(func() {
		SetEnableMDL(bak)
	})

	// IsMDLEnabled must return true regardless of the value of enableMDL.
	SetEnableMDL(false)
	require.True(t, IsMDLEnabled())
	SetEnableMDL(true)
	require.True(t, IsMDLEnabled())
}

func runConcurrentTest(b *testing.B, limiter interface {
	Allow() bool
}, goroutines int) {
	var wg sync.WaitGroup
	startCh := make(chan struct{})

	cnt := b.N / goroutines
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			<-startCh
			for i := 0; i < cnt; i++ {
				limiter.Allow()
			}
		}()
	}

	b.ResetTimer()
	close(startCh)
	wg.Wait()
	b.StopTimer()
}

const limit = 10000

func BenchmarkRateLimiterSimple(b *testing.B) {
	b.ReportAllocs()
	rl := rate.NewLimiter(rate.Limit(limit), limit)
	runConcurrentTest(b, rl, 1)
}

func BenchmarkRateLimiterCurrency100(b *testing.B) {
	b.ReportAllocs()
	rl := rate.NewLimiter(rate.Limit(limit), limit)
	runConcurrentTest(b, rl, 100)
}

func BenchmarkRateLimiterCurrency1000(b *testing.B) {
	b.ReportAllocs()
	rl := rate.NewLimiter(rate.Limit(limit), limit)
	runConcurrentTest(b, rl, 1000)
}
