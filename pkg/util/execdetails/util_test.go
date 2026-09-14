// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package execdetails

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPercentileConcurrentAddAndGetPercentile(t *testing.T) {
	var p Percentile[Duration]

	// Preload past the slice->TDigest switchover so readers immediately
	// exercise tdigest.Quantile concurrently with writers still calling
	// tdigest.Add — this is the path that panicked in #70655.
	const preloadSamples = 1500
	for i := 0; i < preloadSamples; i++ {
		p.Add(Duration(time.Duration(i) * time.Microsecond))
	}
	require.NotNil(t, p.dt, "expected TDigest path to already be active")

	const totalSamples = 3000
	const numWriters = 4
	const numReaders = 4
	samplesPerWriter := totalSamples / numWriters

	var wg sync.WaitGroup

	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for i := 0; i < samplesPerWriter; i++ {
				p.Add(Duration(time.Duration(preloadSamples+writerID*samplesPerWriter+i) * time.Microsecond))
			}
		}(w)
	}

	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				val := p.GetPercentile(0.95)
				require.False(t, math.IsNaN(val), "GetPercentile returned NaN")
				require.False(t, math.IsInf(val, 0), "GetPercentile returned Inf")
			}
		}()
	}

	wg.Wait()

	val := p.GetPercentile(0.95)
	require.False(t, math.IsNaN(val), "final GetPercentile returned NaN")
	require.False(t, math.IsInf(val, 0), "final GetPercentile returned Inf")
}
