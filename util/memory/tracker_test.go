// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package memory

import (
	"fmt"
	"sync"
	"testing"
)

type mockQuery struct {
	workerWaitGroup sync.WaitGroup
	numExecutor     int
	numUpdateCount  int
	tracker         *Tracker
}

func (m *mockQuery) run() {
	m.workerWaitGroup.Add(m.numExecutor)
	for i := 0; i < m.numExecutor; i++ {
		go m.runUpdate(i)
	}
	m.workerWaitGroup.Wait()
}

func (m *mockQuery) runUpdate(i int) {
	defer m.workerWaitGroup.Done()
	for cnt := 0; cnt < m.numUpdateCount; cnt++ {
		m.tracker.children[i].Consume(256 << 20) // consume 256MB
	}
}

func newMockQueryTracker(numExecutor, numUpdateCount int) *mockQuery {
	m := &mockQuery{
		numExecutor:    numExecutor,
		numUpdateCount: numUpdateCount,
		tracker:        NewTracker("query", -1),
	}
	for i := 0; i < numExecutor; i++ {
		execTracker := NewTracker(fmt.Sprintf("exec%v", i), -1)
		execTracker.AttachTo(m.tracker)
	}
	return m
}

func BenchmarkConcurrentUpdate(b *testing.B) {
	for i := 0; i < b.N; i++ {
		m := newMockQueryTracker(10, 1000)
		m.run()
	}
}
