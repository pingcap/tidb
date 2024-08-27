// Copyright 2023 PingCAP, Inc.
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

package proto

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSubtaskIsDone(t *testing.T) {
	cases := []struct {
		state SubtaskState
		done  bool
	}{
		{SubtaskStatePending, false},
		{SubtaskStateRunning, false},
		{SubtaskStateSucceed, true},
		{SubtaskStateFailed, true},
		{SubtaskStatePaused, false},
		{SubtaskStateCanceled, true},
	}
	for _, c := range cases {
		require.Equal(t, c.done, (&Subtask{SubtaskBase: SubtaskBase{State: c.state}}).IsDone())
	}
}

func TestAllocatable(t *testing.T) {
	allocatable := NewAllocatable(123456)
	require.Equal(t, int64(123456), allocatable.Capacity())
	require.Equal(t, int64(0), allocatable.Used())

	require.False(t, allocatable.Alloc(123457))
	require.Equal(t, int64(0), allocatable.Used())
	require.True(t, allocatable.Alloc(123))
	require.Equal(t, int64(123), allocatable.Used())
	allocatable.Free(123)
	require.Equal(t, int64(0), allocatable.Used())

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			seed := time.Now().UnixNano()
			t.Logf("routine: %d, seed: %d", i, seed)
			rand.New(rand.NewSource(seed))
			for i := 0; i < 10000; i++ {
				n := rand.Int63n(1000)
				if allocatable.Alloc(n) {
					allocatable.Free(n)
				}
			}
		}()
	}
	wg.Wait()
	require.Equal(t, int64(0), allocatable.Used())
}
