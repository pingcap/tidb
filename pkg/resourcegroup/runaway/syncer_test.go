// Copyright 2026 PingCAP, Inc.
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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWatchSyncBackoffInterval(t *testing.T) {
	re := require.New(t)

	// Simulate the backoff logic from RunawayWatchSyncLoop.
	computeNextInterval := func(interval time.Duration, found bool, err error) time.Duration {
		if found || err != nil {
			return watchSyncMinInterval
		}
		return min(interval*2, watchSyncMaxInterval)
	}

	// Start at min interval.
	interval := watchSyncMinInterval
	re.Equal(time.Second, interval)

	// Consecutive empty results should double the interval up to max.
	interval = computeNextInterval(interval, false, nil)
	re.Equal(2*time.Second, interval)

	interval = computeNextInterval(interval, false, nil)
	re.Equal(4*time.Second, interval)

	interval = computeNextInterval(interval, false, nil)
	re.Equal(8*time.Second, interval)

	interval = computeNextInterval(interval, false, nil)
	re.Equal(16*time.Second, interval)

	// Should cap at max.
	interval = computeNextInterval(interval, false, nil)
	re.Equal(watchSyncMaxInterval, interval)

	interval = computeNextInterval(interval, false, nil)
	re.Equal(watchSyncMaxInterval, interval)

	// Finding records resets to min.
	interval = computeNextInterval(interval, true, nil)
	re.Equal(watchSyncMinInterval, interval)

	// Back off again.
	interval = computeNextInterval(interval, false, nil)
	re.Equal(2*time.Second, interval)

	interval = computeNextInterval(interval, false, nil)
	re.Equal(4*time.Second, interval)

	// Error also resets to min.
	interval = computeNextInterval(interval, false, errMock)
	re.Equal(watchSyncMinInterval, interval)

	// found=true with error still resets.
	interval = computeNextInterval(8*time.Second, true, errMock)
	re.Equal(watchSyncMinInterval, interval)
}

var errMock = fmt.Errorf("mock error")
