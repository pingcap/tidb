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

	w := ss.window
	ss.Add(GenerateStmtExecInfo4Test("digest1"))
	ss.Add(GenerateStmtExecInfo4Test("digest2"))
	ss.Add(GenerateStmtExecInfo4Test("digest3"))
	ss.Add(GenerateStmtExecInfo4Test("digest4"))
	ss.Add(GenerateStmtExecInfo4Test("digest5"))
	require.Equal(t, 3, w.lru.Size())
	require.Equal(t, 2, w.evicted.count())

	ss.rotate(timeNow())

	ss.Add(GenerateStmtExecInfo4Test("digest6"))
	ss.Add(GenerateStmtExecInfo4Test("digest7"))
	w = ss.window
	require.Equal(t, 2, w.lru.Size())
	require.Equal(t, 0, w.evicted.count())

	ss.Clear()
	require.Equal(t, 0, w.lru.Size())
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
