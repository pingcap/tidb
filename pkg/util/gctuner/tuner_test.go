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

package gctuner

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

var testHeap []byte

func TestTuner(t *testing.T) {
	EnableGOGCTuner.Store(true)
	memLimit := uint64(1000 * 1024 * 1024) //1000 MB
	threshold := memLimit / 2
	tn := newTuner(threshold)
	currentGCPercent := tn.getGCPercent()
	require.Equal(t, tn.getThreshold(), threshold)
	require.Equal(t, defaultGCPercent, currentGCPercent)

	// wait for tuner set gcPercent to maxGCPercent
	t.Logf("old gc percent before gc: %d", tn.getGCPercent())
	for tn.getGCPercent() != maxGCPercent.Load() {
		runtime.GC()
		t.Logf("new gc percent after gc: %d", tn.getGCPercent())
	}

	// 1/4 threshold
	testHeap = make([]byte, threshold/4)
	// wait for tuner set gcPercent to ~= 300
	t.Logf("old gc percent before gc: %d", tn.getGCPercent())
	for tn.getGCPercent() == maxGCPercent.Load() {
		runtime.GC()
		t.Logf("new gc percent after gc: %d", tn.getGCPercent())
	}
	currentGCPercent = tn.getGCPercent()
	require.GreaterOrEqual(t, currentGCPercent, uint32(250))
	require.LessOrEqual(t, currentGCPercent, uint32(300))

	// 1/2 threshold
	testHeap = make([]byte, threshold/2)
	// wait for tuner set gcPercent to ~= 100
	t.Logf("old gc percent before gc: %d", tn.getGCPercent())
	for tn.getGCPercent() == currentGCPercent {
		runtime.GC()
		t.Logf("new gc percent after gc: %d", tn.getGCPercent())
	}
	currentGCPercent = tn.getGCPercent()
	require.GreaterOrEqual(t, currentGCPercent, uint32(50))
	require.LessOrEqual(t, currentGCPercent, uint32(100))

	// 3/4 threshold
	testHeap = make([]byte, threshold/4*3)
	// wait for tuner set gcPercent to minGCPercent
	t.Logf("old gc percent before gc: %d", tn.getGCPercent())
	for tn.getGCPercent() != minGCPercent.Load() {
		runtime.GC()
		t.Logf("new gc percent after gc: %d", tn.getGCPercent())
	}
	require.Equal(t, minGCPercent.Load(), tn.getGCPercent())

	// out of threshold
	testHeap = make([]byte, threshold+1024)
	t.Logf("old gc percent before gc: %d", tn.getGCPercent())
	runtime.GC()
	for i := 0; i < 8; i++ {
		runtime.GC()
		require.Equal(t, minGCPercent.Load(), tn.getGCPercent())
	}

	// no heap
	testHeap = nil
	// wait for tuner set gcPercent to maxGCPercent
	t.Logf("old gc percent before gc: %d", tn.getGCPercent())
	for tn.getGCPercent() != maxGCPercent.Load() {
		runtime.GC()
		t.Logf("new gc percent after gc: %d", tn.getGCPercent())
	}
}

func TestCalcGCPercent(t *testing.T) {
	const gb = 1024 * 1024 * 1024
	// use default value when invalid params
	require.Equal(t, defaultGCPercent, calcGCPercent(0, 0))
	require.Equal(t, defaultGCPercent, calcGCPercent(0, 1))
	require.Equal(t, defaultGCPercent, calcGCPercent(1, 0))

	require.Equal(t, maxGCPercent.Load(), calcGCPercent(1, 3*gb))
	require.Equal(t, maxGCPercent.Load(), calcGCPercent(gb/10, 4*gb))
	require.Equal(t, maxGCPercent.Load(), calcGCPercent(gb/2, 4*gb))
	require.Equal(t, uint32(300), calcGCPercent(1*gb, 4*gb))
	require.Equal(t, uint32(166), calcGCPercent(1.5*gb, 4*gb))
	require.Equal(t, uint32(100), calcGCPercent(2*gb, 4*gb))
	require.Equal(t, uint32(100), calcGCPercent(3*gb, 4*gb))
	require.Equal(t, minGCPercent.Load(), calcGCPercent(4*gb, 4*gb))
	require.Equal(t, minGCPercent.Load(), calcGCPercent(5*gb, 4*gb))
}
