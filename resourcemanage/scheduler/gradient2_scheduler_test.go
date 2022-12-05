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

	"github.com/pingcap/tidb/resourcemanage/util"
	"github.com/stretchr/testify/require"
)

func TestGradient2Scheduler(t *testing.T) {
	scheduler := NewGradient2Scheduler()
	pool := util.NewFakeGPool()
	rms := NewFakeResourceManage()
	rms.RegisterPool(pool)
	rms.Register(scheduler)
	// Test the initial state.
	pool.OnSample(0, 0, 0, 0, 0, 0, 0, 0, 0)
	pool.ImportLastTunerTs(
		time.Now())
	require.Equal(t, Hold, rms.Next())
	// p.InFlight() < int64(p.Cap())/2 is hold
	pool.OnSample(0, 0, 0, 0, 100, 20, 30, 0, 0)
	pool.ImportLastTunerTs(
		time.Now().Add(-10 * time.Second))
	require.Equal(t, Hold, rms.Next())
	// Overclock
	pool.OnSample(0, 60, 0, 0, 100, 20, 30, 0, 0)
	pool.ImportLastTunerTs(
		time.Now().Add(-10 * time.Second))
	require.Equal(t, Overclock, rms.Next())
	//
	pool.OnSample(0, 60, 0, 0, 100, 70, 30, 0, 0)
	pool.ImportLastTunerTs(
		time.Now().Add(-10 * time.Second))
	require.Equal(t, Overclock, rms.Next())

	pool.OnSample(0, 60, 0, 0, 100, 70, 30, 40, 20)
	pool.ImportLastTunerTs(
		time.Now().Add(-10 * time.Second))
	require.Equal(t, Overclock, rms.Next())

	pool.OnSample(0, 132, 0, 0, 100, 30, 70, 30, 102)
	pool.ImportLastTunerTs(
		time.Now().Add(-10 * time.Second))
	require.Equal(t, Downclock, rms.Next())
	pool.OnSample(0, 60, 0, 0, 100, 30, 70, 50, 2)
	pool.ImportLastTunerTs(
		time.Now().Add(-10 * time.Second))
	require.Equal(t, Overclock, rms.Next())
}
