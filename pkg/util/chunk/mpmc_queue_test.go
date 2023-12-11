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
	chks, _ := initChunks(numChk, numRow)
	chksWithMem := createChunkWithMemoryUsageSlice(chks)
	queue := NewMPMCQueue(100)

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

	// Close the queue
	queue.Push(&chksWithMem[0])
	queue.Push(&chksWithMem[1])
	queue.Pop()
	queue.Close()
	_, res := queue.Pop()
	require.Equal(t, Result(Closed), res)
}

func TestFullOrEmpty(t *testing.T) {
	numChk := 10
	numRow := 10
	chks, _ := initChunks(numChk, numRow)
	chksWithMem := createChunkWithMemoryUsageSlice(chks)
	queue := NewMPMCQueue(100)

	{
		waitGroup := sync.WaitGroup{}
		for i := 0; i < numChk; i++ {
			waitGroup.Add(1)
			go func() {
				queue.Pop()
				waitGroup.Done()
			}()
		}

		// Sleep for some time to ensure that push is after the pop
		time.Sleep(300 * time.Millisecond)
		for i := 0; i < numChk; i++ {
			queue.Push(&chksWithMem[i])
		}
		waitGroup.Wait()
	}

	{
		waitGroup := sync.WaitGroup{}
		for i := 0; i < numChk; i++ {
			waitGroup.Add(1)
			go func() {
				queue.Push(&chksWithMem[0])
				waitGroup.Done()
			}()
		}

		// Sleep for some time to ensure that pop is after the push
		time.Sleep(300 * time.Millisecond)
		for i := 0; i < numChk; i++ {
			queue.Pop()
		}
		waitGroup.Wait()
	}

	queue.Close()
	queue.Pop() // Should not be blocked

	queue = NewMPMCQueue(1)
	queue.Push(&chksWithMem[0])
	queue.Close()
	queue.Push(&chksWithMem[0]) // Should not be blocked
}
