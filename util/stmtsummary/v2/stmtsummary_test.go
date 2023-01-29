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
	w := newStmtWindow(time.Unix(0, 0), time.Unix(4070880000, 0), 5)
	w.add(GenerateStmtExecInfo4Test("digest1"))
	w.add(GenerateStmtExecInfo4Test("digest1"))
	w.add(GenerateStmtExecInfo4Test("digest2"))
	w.add(GenerateStmtExecInfo4Test("digest2"))
	w.add(GenerateStmtExecInfo4Test("digest3"))
	w.add(GenerateStmtExecInfo4Test("digest4"))
	w.add(GenerateStmtExecInfo4Test("digest5"))
	w.add(GenerateStmtExecInfo4Test("digest6"))
	w.add(GenerateStmtExecInfo4Test("digest7"))
	require.Equal(t, 5, w.lru.Size())
	require.Equal(t, 2, w.evicted.count())
	require.Equal(t, int64(4), w.evicted.other.ExecCount) // digest1 digest1 digest2 digest2
	w.clear()
	require.Equal(t, 0, w.lru.Size())
	require.Equal(t, 0, w.evicted.count())
	require.Equal(t, int64(0), w.evicted.other.ExecCount)
}

func TestStmtSummary(t *testing.T) {
	ss := NewStmtSummary4Test(3)
	ss.storage = &waitableMockStmtStorage{mockStmtStorage: ss.storage.(*mockStmtStorage)}
	w := ss.window.Load().(*stmtWindow)
	ss.Add(GenerateStmtExecInfo4Test("digest1"))
	ss.Add(GenerateStmtExecInfo4Test("digest2"))
	ss.Add(GenerateStmtExecInfo4Test("digest3"))
	ss.Add(GenerateStmtExecInfo4Test("digest4"))
	ss.Add(GenerateStmtExecInfo4Test("digest5"))
	require.Equal(t, 3, w.lru.Size())
	require.Equal(t, 2, w.evicted.count())

	newEnd := w.end.Add(time.Second)
	timeNow = func() time.Time {
		return newEnd
	}
	ss.storage.(*waitableMockStmtStorage).Add(1)
	ss.Add(GenerateStmtExecInfo4Test("digest6")) // trigger rotate
	timeNow = time.Now
	ss.Add(GenerateStmtExecInfo4Test("digest7"))
	ss.storage.(*waitableMockStmtStorage).Wait()
	w = ss.window.Load().(*stmtWindow)
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

func (s *waitableMockStmtStorage) persist(w *stmtWindow) {
	defer s.Done()
	s.mockStmtStorage.persist(w)
}
