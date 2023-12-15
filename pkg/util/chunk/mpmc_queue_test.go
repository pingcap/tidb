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

package chunk

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func createChunkWithMemoryUsageSlice(chks []*Chunk) []ChunkWithMemoryUsage {
	retVal := make([]ChunkWithMemoryUsage, len(chks))
	for i := range retVal {
		retVal[i].Chk = chks[i]
		retVal[i].MemoryUsage = chks[i].MemoryUsage() + int64(i)
	}
	return retVal
}

func checkChunkWithMemoryUsage(t *testing.T, expectChk *ChunkWithMemoryUsage, chk *ChunkWithMemoryUsage) {
	require.Equal(t, expectChk.MemoryUsage, chk.MemoryUsage)
	checkChunk(t, expectChk.Chk, chk.Chk)
}

func TestMPMCQueue(t *testing.T) {
	numChk := 10
	numRow := 10
	queueLimit := numChk * 2
	chks, _ := initChunks(numChk, numRow)
	chksWithMem := createChunkWithMemoryUsageSlice(chks)

	{
		// Push, pop, push, pop and we could get all chunks.
		queue := NewMPMCQueue(queueLimit)
		i := 0
		for ; i < numChk/2; i++ {
			queue.Push(&chksWithMem[i])
		}

		j := 0
		for ; j < i; j++ {
			chk, res := queue.Pop()
			require.Equal(t, Result(OK), res)
			checkChunkWithMemoryUsage(t, &chksWithMem[j], chk)
		}

		for ; i < numChk; i++ {
			queue.Push(&chksWithMem[i])
		}

		for ; j < numChk; j++ {
			chk, res := queue.Pop()
			require.Equal(t, Result(OK), res)
			checkChunkWithMemoryUsage(t, &chksWithMem[j], chk)
		}
	}

	{
		// Push, push, close, push fail and get `Closed` return value
		queue := NewMPMCQueue(queueLimit)
		queue.Push(&chksWithMem[0])
		queue.Push(&chksWithMem[1])
		queue.Close()
		res := queue.Push(&chksWithMem[1])
		require.Equal(t, Result(Closed), res)
	}

	{
		// Push, push, close, pop, pop, pop fail and get `Closed` return value
		queue := NewMPMCQueue(queueLimit)
		queue.Push(&chksWithMem[0])
		queue.Push(&chksWithMem[1])
		queue.Close()
		chk, res := queue.Pop()
		checkChunkWithMemoryUsage(t, &chksWithMem[0], chk)
		require.Equal(t, Result(OK), res)
		chk, res = queue.Pop()
		checkChunkWithMemoryUsage(t, &chksWithMem[1], chk)
		require.Equal(t, Result(OK), res)
		chk, res = queue.Pop()
		require.Equal(t, (*ChunkWithMemoryUsage)(nil), chk)
		require.Equal(t, Result(Closed), res)
	}

	{
		// Push, push, cancel, push fail and get `Cancelled` return value
		queue := NewMPMCQueue(queueLimit)
		queue.Push(&chksWithMem[0])
		queue.Cancel()
		res := queue.Push(&chksWithMem[1])
		require.Equal(t, Result(Cancelled), res)
	}

	{
		// Push, push, cancel, pop fail and get `Cancelled` return value
		queue := NewMPMCQueue(queueLimit)
		queue.Push(&chksWithMem[0])
		queue.Cancel()
		chk, res := queue.Pop()
		require.Equal(t, (*ChunkWithMemoryUsage)(nil), chk)
		require.Equal(t, Result(Cancelled), res)
	}
}

func TestFullOrEmpty(t *testing.T) {
	numChk := 10
	numRow := 10
	chks, _ := initChunks(numChk, numRow)
	chksWithMem := createChunkWithMemoryUsageSlice(chks)

	{
		// Pop from an empty queue, blocked at one goroutine and
		// should be waked up by another goroutine with (push/close/cancel).
		for i := 0; i < 3; i++ {
			queue := NewMPMCQueue(1)
			waitGroup := sync.WaitGroup{}
			waitGroup.Add(1)
			go func() {
				queue.Pop()
				waitGroup.Done()
			}()

			// Sleep for some time to ensure that wake up action is after the pop.
			time.Sleep(300 * time.Millisecond)
			if i == 0 {
				queue.Push(&chksWithMem[i])
			} else if i == 1 {
				queue.Close()
			} else if i == 2 {
				queue.Cancel()
			}

			waitGroup.Wait()
		}
	}

	{
		// Push chunks into full queue, blocked at one goroutine and
		// should be waked up by another goroutine with (pop/close/cancel).
		for i := 0; i < 3; i++ {
			queue := NewMPMCQueue(1)
			queue.Push(&chksWithMem[0])
			waitGroup := sync.WaitGroup{}
			waitGroup.Add(1)
			go func() {
				queue.Push(&chksWithMem[0])
				waitGroup.Done()
			}()

			// Sleep for some time to ensure that wake up action is after the push.
			time.Sleep(300 * time.Millisecond)
			if i == 0 {
				queue.Pop()
			} else if i == 1 {
				queue.Close()
			} else if i == 2 {
				queue.Cancel()
			}
			waitGroup.Wait()
		}
	}

	{
		// Pop an closed empty queue should get a `Closed` return value.
		queue := NewMPMCQueue(1)
		queue.Close()
		chk, res := queue.Pop()
		require.Equal(t, (*ChunkWithMemoryUsage)(nil), chk)
		require.Equal(t, Result(Closed), res)
	}

	{
		// Pop an cancelled empty queue should get a `Closed` return value.
		queue := NewMPMCQueue(1)
		queue.Cancel()
		chk, res := queue.Pop()
		require.Equal(t, (*ChunkWithMemoryUsage)(nil), chk)
		require.Equal(t, Result(Cancelled), res)
	}

	{
		// Push a closed full queue should get a `Closed` return value.
		queue := NewMPMCQueue(1)
		queue.Push(&chksWithMem[0])
		queue.Close()
		res := queue.Push(&chksWithMem[0])
		require.Equal(t, Result(Closed), res)
	}

	{
		// Push a closed full queue should get a `Cancelled` return value.
		queue := NewMPMCQueue(1)
		queue.Push(&chksWithMem[0])
		queue.Cancel()
		res := queue.Push(&chksWithMem[0])
		require.Equal(t, Result(Cancelled), res)
	}
}
