// Copyright 2022 PingCAP, Inc.
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

package scheduler

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/resourcemanager/util"
	"github.com/stretchr/testify/require"
)

func TestGradient2Scheduler(t *testing.T) {
	scheduler := NewGradient2Scheduler()
	pool := util.NewMockGPool(10)
	rms := NewFakeResourceManage()
	rms.RegisterPool(pool)
	rms.Register(scheduler)
	testcases := []struct {
		Name string
		// InFlight is the count of all task submitted to the pool.
		InFlight int64
		Capacity int
		// Tracks a measurement of the long term, less volatile, RTT meant to represent the baseline latency.  When the system
		// is under load this number is expect to trend higher.
		LongRTT float64
		// Tracks a measurement of the short time, and more volatile, RTT meant to represent the current system latency
		ShortRTT uint64
		// QueueSize is the size of task queue in the pool.
		QueueSize int64
		Running   int
		Delta     time.Duration
		Expected  Command
	}{

		{
			Name:     "init",
			Expected: Hold,
		},
		{
			Name:     "p.InFlight() < int64(p.Cap())/2 is hold",
			Capacity: 100,
			LongRTT:  20,
			ShortRTT: 30,
			Delta:    -10 * time.Second,
			Expected: Hold,
		},
		{
			Name:     "p.InFlight() > int64(p.Cap())/2 is hold",
			InFlight: 2,
			Capacity: 100,
			LongRTT:  20,
			ShortRTT: 30,
			Running:  2,
			Delta:    -10 * time.Second,
			Expected: Hold,
		},
		{
			Name:      "basic Downclock and LongRTT / ShortRTT < 2",
			InFlight:  132,
			Capacity:  100,
			LongRTT:   30,
			ShortRTT:  70,
			QueueSize: 30,
			Running:   102,
			Delta:     -10 * time.Second,
			Expected:  Downclock,
		},
		{
			Name:      "near max concurrency，LongRTT / ShortRTT > 2",
			InFlight:  132,
			Capacity:  100,
			LongRTT:   70,
			ShortRTT:  30,
			QueueSize: 30,
			Running:   102,
			Delta:     -10 * time.Second,
			Expected:  Hold,
		},
		{
			Name:      "basic overlock, ShortRTT > LongRTT and LongRTT / ShortRTT < 2",
			InFlight:  60,
			Capacity:  100,
			LongRTT:   30,
			ShortRTT:  80,
			QueueSize: 50,
			Running:   10,
			Delta:     -10 * time.Second,
			Expected:  Overclock,
		},
		{
			Name:      "basic overlock, ShortRTT < LongRTT and LongRTT / ShortRTT > 2",
			InFlight:  60,
			Capacity:  100,
			LongRTT:   70,
			ShortRTT:  30,
			QueueSize: 40,
			Running:   20,
			Delta:     -10 * time.Second,
			Expected:  Overclock,
		},
	}
	for _, tc := range testcases {
		pool.OnSample(0, tc.InFlight, 0, 0, tc.Capacity, tc.LongRTT, tc.ShortRTT, tc.QueueSize, tc.Running)
		pool.ImportLastTunerTs(time.Now().Add(tc.Delta))
		require.Equal(t, tc.Expected, rms.Next(), tc.Name)
	}
}
