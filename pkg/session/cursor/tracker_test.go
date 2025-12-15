// Copyright 2024 PingCAP, Inc.
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

package cursor

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewCursor(t *testing.T) {
	tracker := &cursorTracker{cursors: &sync.Map{}}
	cursor := tracker.NewCursor(State{})
	require.NotNil(t, cursor)
	require.Equal(t, 1, cursor.ID())

	cursor2 := tracker.NewCursor(State{})
	require.NotNil(t, cursor2)
	require.Equal(t, 2, cursor2.ID())
}

func TestGetCursor(t *testing.T) {
	tracker := &cursorTracker{cursors: &sync.Map{}}
	newCursor := tracker.NewCursor(State{})
	retrievedCursor := tracker.GetCursor(newCursor.ID())

	require.True(t, reflect.DeepEqual(newCursor, retrievedCursor))
}

func TestRangeCursor(t *testing.T) {
	tracker := &cursorTracker{cursors: &sync.Map{}}
	tracker.NewCursor(State{})

	called := false
	tracker.RangeCursor(func(cursor Handle) bool {
		called = true
		require.Equal(t, 1, cursor.ID())
		return false
	})

	require.True(t, called)
}

func TestCursorHandleClose(t *testing.T) {
	tracker := &cursorTracker{cursors: &sync.Map{}}
	cursor := tracker.NewCursor(State{})
	id := cursor.ID()
	cursor.Close()

	c := tracker.GetCursor(id)
	require.Nil(t, c)
}

func TestCursorTrackerConcurrentCreateDelete(t *testing.T) {
	tracker := &cursorTracker{cursors: &sync.Map{}}
	stop := make(chan struct{})
	wg := sync.WaitGroup{}

	threadsForEachOperation := 100

	// Concurrently create cursors
	for range threadsForEachOperation {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					tracker.NewCursor(State{})
				}
			}
		}()
	}

	// Concurrently delete cursors
	for range threadsForEachOperation {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					tracker.RangeCursor(func(cursor Handle) bool {
						cursor.Close()
						return true // continue ranging
					})
				}
			}
		}()
	}

	// Run the concurrent operations for at least 2 seconds
	time.Sleep(2 * time.Second)
	close(stop)
	wg.Wait()
}
