// Copyright 2024 PingCAP, Inc.
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
package queue

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQueue(t *testing.T) {
	t.Run("basic operations", func(t *testing.T) {
		q := NewQueue[int](2)

		// Test initial state
		require.True(t, q.IsEmpty(), "new queue should be empty")
		require.Equal(t, 0, q.Len(), "new queue should have length 0")
		require.Equal(t, 2, q.Cap(), "new queue should have capacity 2")

		// Test Push
		q.Push(1)
		q.Push(2)
		require.Equal(t, 2, q.Len(), "queue length should be 2 after pushing 2 elements")
		require.False(t, q.IsEmpty(), "queue should not be empty after pushing elements")

		// Test automatic capacity increase
		q.Push(3)
		require.Equal(t, 4, q.Cap(), "queue capacity should double when full")

		// Test Pop
		require.Equal(t, 1, q.Pop(), "first pop should return 1")
		require.Equal(t, 2, q.Pop(), "second pop should return 2")
		require.Equal(t, 3, q.Pop(), "third pop should return 3")

		// Test empty queue
		require.True(t, q.IsEmpty(), "queue should be empty after popping all elements")
	})

	t.Run("clear operation", func(t *testing.T) {
		q := NewQueue[string](4)
		q.Push("a")
		q.Push("b")
		q.Push("c")

		q.Clear()
		require.True(t, q.IsEmpty(), "queue should be empty after clear")
		require.Equal(t, 0, q.Len(), "queue length should be 0 after clear")
	})

	t.Run("panic on empty pop", func(t *testing.T) {
		defer func() {
			r := recover()
			require.NotNil(t, r, "pop on empty queue should panic")
		}()

		q := NewQueue[int](1)
		q.Pop() // Should panic
	})

	t.Run("circular buffer behavior", func(t *testing.T) {
		q := NewQueue[int](3)
		q.Push(1)
		q.Push(2)
		q.Pop() // Remove 1
		q.Push(3)
		q.Push(4) // This should wrap around

		// check queue.head and queue.tail
		require.Equal(t, 1, q.head, "queue.head should be 1")
		require.Equal(t, 1, q.tail, "queue.tail should be 1")

		require.Equal(t, 2, q.Pop(), "expected 2")
		require.Equal(t, 3, q.Pop(), "expected 3")
		require.Equal(t, 4, q.Pop(), "expected 4")
	})
}
