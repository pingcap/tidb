// Copyright 2022 PingCAP, Inc.
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
	"sync"
	"testing"
	"time"

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
	require.Equal(t, 5, ss.window.lru.Size())
	require.Equal(t, 2, ss.window.evicted.count())
	require.Equal(t, int64(4), ss.window.evicted.other.ExecCount) // digest1 digest1 digest2 digest2
	ss.Clear()
	require.Equal(t, 0, ss.window.lru.Size())
	require.Equal(t, 0, ss.window.evicted.count())
	require.Equal(t, int64(0), ss.window.evicted.other.ExecCount)
}

func TestStmtSummary(t *testing.T) {
	ss := NewStmtSummary4Test(3)
	defer ss.Close()

	ss.storage = &waitableMockStmtStorage{mockStmtStorage: ss.storage.(*mockStmtStorage)}
	w := ss.window
	ss.Add(GenerateStmtExecInfo4Test("digest1"))
	ss.Add(GenerateStmtExecInfo4Test("digest2"))
	ss.Add(GenerateStmtExecInfo4Test("digest3"))
	ss.Add(GenerateStmtExecInfo4Test("digest4"))
	ss.Add(GenerateStmtExecInfo4Test("digest5"))
	require.Equal(t, 3, w.lru.Size())
	require.Equal(t, 2, w.evicted.count())

	ss.storage.(*waitableMockStmtStorage).Add(1)
	newEnd := w.begin.Add(time.Duration(ss.RefreshInterval()+1) * time.Second)
	timeNow = func() time.Time {
		return newEnd
	}
	ss.storage.(*waitableMockStmtStorage).Wait()

	timeNow = time.Now
	ss.Add(GenerateStmtExecInfo4Test("digest6"))
	ss.Add(GenerateStmtExecInfo4Test("digest7"))
	w = ss.window
	require.Equal(t, 2, w.lru.Size())
	require.Equal(t, 0, w.evicted.count())

	ss.SetEnableInternalQuery(false)
	internalInfo := GenerateStmtExecInfo4Test("digest8")
	internalInfo.IsInternal = true
	ss.Add(internalInfo)
	require.Equal(t, 2, w.lru.Size())
	ss.SetEnableInternalQuery(true)
	ss.Add(internalInfo)
	require.Equal(t, 3, w.lru.Size())
	require.Equal(t, 0, w.evicted.count())
	ss.ClearInternal()
	require.Equal(t, 2, w.lru.Size())
	ss.Clear()
	require.Equal(t, 0, w.lru.Size())
}

type waitableMockStmtStorage struct {
	sync.WaitGroup
	*mockStmtStorage
}

func (s *waitableMockStmtStorage) persist(w *stmtWindow, end time.Time) {
	defer s.Done()
	s.mockStmtStorage.persist(w, end)
}
