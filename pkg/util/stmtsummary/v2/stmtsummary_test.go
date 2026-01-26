// Copyright 2023 PingCAP, Inc.
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

package stmtsummary

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStmtWindow(t *testing.T) {
	ss := NewStmtSummary4Test(5)
	defer ss.Close()
	ss.Add(GenerateStmtExecInfo4Test("digest1"))
	ss.Add(GenerateStmtExecInfo4Test("digest1"))
	ss.Add(GenerateStmtExecInfo4Test("digest2"))
	ss.Add(GenerateStmtExecInfo4Test("digest2"))
	ss.Add(GenerateStmtExecInfo4Test("digest3"))
	ss.Add(GenerateStmtExecInfo4Test("digest4"))
	ss.Add(GenerateStmtExecInfo4Test("digest5"))
	ss.Add(GenerateStmtExecInfo4Test("digest6"))
	ss.Add(GenerateStmtExecInfo4Test("digest7"))
	// FIFO=1, LRU=4.
	// d1, d2 promoted to LRU (size 2).
	// d3..d7 churn through FIFO. d7 remains.
	// Size = 2 + 1 = 3.
	// Evicted = d3, d4, d5, d6 (4 items).
	require.Equal(t, 3, ss.window.Size())
	require.Equal(t, 4, ss.window.evicted.count())
	require.Equal(t, int64(4), ss.window.evicted.other.ExecCount) // d3, d4, d5, d6
	_, err := json.Marshal(ss.window.evicted.other)
	require.NoError(t, err)
	ss.Clear()
	require.Equal(t, 0, ss.window.Size())
	require.Equal(t, 0, ss.window.evicted.count())
	require.Equal(t, int64(0), ss.window.evicted.other.ExecCount)
}

func TestStmtSummary(t *testing.T) {
	ss := NewStmtSummary4Test(3)
	defer ss.Close()

	w := ss.window
	// FIFO=1, LRU=2.
	ss.Add(GenerateStmtExecInfo4Test("digest1"))
	ss.Add(GenerateStmtExecInfo4Test("digest2")) // evicts d1
	ss.Add(GenerateStmtExecInfo4Test("digest3")) // evicts d2
	ss.Add(GenerateStmtExecInfo4Test("digest4")) // evicts d3
	ss.Add(GenerateStmtExecInfo4Test("digest5")) // evicts d4

	require.Equal(t, 1, w.Size())          // Only d5 in FIFO
	require.Equal(t, 4, w.evicted.count()) // d1, d2, d3, d4 evicted

	ss.rotate(timeNow())

	ss.Add(GenerateStmtExecInfo4Test("digest6"))
	ss.Add(GenerateStmtExecInfo4Test("digest7"))
	w = ss.window
	require.Equal(t, 1, w.Size())          // d7 replaces d6
	require.Equal(t, 1, w.evicted.count()) // d6 evicted (since rotation cleared old evicted?) NO.
	// rotate: creates new window. New window has empty evicted.
	// d6 in FIFO.
	// d7 in FIFO, evicts d6.
	// Evicted count 1.

	ss.Clear()
	require.Equal(t, 0, w.Size())
}

func TestStmtSummaryFlush(t *testing.T) {
	storage := &mockStmtStorage{}
	ss := NewStmtSummary4Test(1000)
	ss.storage = storage

	ss.Add(GenerateStmtExecInfo4Test("digest1"))
	ss.Add(GenerateStmtExecInfo4Test("digest2"))
	ss.Add(GenerateStmtExecInfo4Test("digest3"))

	ss.rotate(timeNow())

	ss.Add(GenerateStmtExecInfo4Test("digest1"))
	ss.Add(GenerateStmtExecInfo4Test("digest2"))
	ss.Add(GenerateStmtExecInfo4Test("digest3"))

	ss.rotate(timeNow())

	ss.Add(GenerateStmtExecInfo4Test("digest1"))
	ss.Add(GenerateStmtExecInfo4Test("digest2"))
	ss.Add(GenerateStmtExecInfo4Test("digest3"))

	ss.Close()

	storage.Lock()
	require.Equal(t, 3, len(storage.windows))
	storage.Unlock()
}

func Test2QBehavior(t *testing.T) {
	// Total 10. FIFO=3, LRU=7.
	ss := NewStmtSummary4Test(10)
	defer ss.Close()

	// 1. Insert d1. Should be in FIFO.
	ss.Add(GenerateStmtExecInfo4Test("digest1"))
	w := ss.window
	require.Equal(t, 1, w.fifo.Size())
	require.Equal(t, 0, w.lru.Size())

	// 2. Insert d1 again. Should promote to LRU.
	ss.Add(GenerateStmtExecInfo4Test("digest1"))
	require.Equal(t, 0, w.fifo.Size())
	require.Equal(t, 1, w.lru.Size())

	// 3. Fill FIFO.
	ss.Add(GenerateStmtExecInfo4Test("d2"))
	ss.Add(GenerateStmtExecInfo4Test("d3"))
	ss.Add(GenerateStmtExecInfo4Test("d4"))
	require.Equal(t, 3, w.fifo.Size())
	require.Equal(t, 1, w.lru.Size())

	// 4. Overflow FIFO.
	ss.Add(GenerateStmtExecInfo4Test("d5"))
	// FIFO has capacity 3. d2, d3, d4 were added.
	// d5 added. d2 should be evicted (First In First Out / LRU behavior of kvcache)
	// FIFO: d3, d4, d5.
	require.Equal(t, 3, w.fifo.Size())
	require.Equal(t, 1, w.evicted.count()) // d2 evicted

	// 5. Promote d3.
	ss.Add(GenerateStmtExecInfo4Test("d3"))
	// FIFO: d4, d5. LRU: d1, d3.
	require.Equal(t, 2, w.fifo.Size())
	require.Equal(t, 2, w.lru.Size())
}
