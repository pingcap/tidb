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
	"time"

	"github.com/pingcap/tidb/pkg/util/stmtsummary"
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
	_, err := json.Marshal(ss.window.evicted.other)
	require.NoError(t, err)
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

func TestStmtSummaryGroupByUser(t *testing.T) {
	ss := NewStmtSummary4Test(100)
	defer ss.Close()

	// Two statements, same digest, different users: without the flag they
	// should merge into one record.
	ss.Add(stmtExecInfoWithUser("digest1", "alice"))
	ss.Add(stmtExecInfoWithUser("digest1", "bob"))
	require.Equal(t, 1, ss.window.lru.Size())

	// Switching the flag on clears the window. Re-emitting produces two rows.
	require.NoError(t, ss.SetGroupByUser(true))
	require.Equal(t, 0, ss.window.lru.Size())
	ss.Add(stmtExecInfoWithUser("digest1", "alice"))
	ss.Add(stmtExecInfoWithUser("digest1", "bob"))
	ss.Add(stmtExecInfoWithUser("digest1", "alice"))
	require.Equal(t, 2, ss.window.lru.Size())

	// Each record should remember the User that groups it so the USER column
	// can be emitted without scanning AuthUsers.
	users := map[string]int64{}
	for _, v := range ss.window.lru.Values() {
		r := v.(*lockedStmtRecord)
		users[r.User] = r.ExecCount
	}
	require.Equal(t, int64(2), users["alice"])
	require.Equal(t, int64(1), users["bob"])

	// Turning the flag off again clears and reverts to single-record merging.
	require.NoError(t, ss.SetGroupByUser(false))
	ss.Add(stmtExecInfoWithUser("digest1", "alice"))
	ss.Add(stmtExecInfoWithUser("digest1", "bob"))
	require.Equal(t, 1, ss.window.lru.Size())
	for _, v := range ss.window.lru.Values() {
		r := v.(*lockedStmtRecord)
		require.Empty(t, r.User) // group_by_user OFF leaves User empty
	}
}

func TestStmtSummaryLogEvicted(t *testing.T) {
	storage := &mockStmtStorage{}
	ss := NewStmtSummary4Test(2)
	ss.storage = storage
	defer ss.Close()
	require.NoError(t, ss.SetLogEvicted(true))

	// With capacity 2, the 3rd and later distinct digests evict older entries
	// and should each land in storage.evicted.
	ss.Add(GenerateStmtExecInfo4Test("digest1"))
	ss.Add(GenerateStmtExecInfo4Test("digest2"))
	ss.Add(GenerateStmtExecInfo4Test("digest3")) // evicts digest1
	ss.Add(GenerateStmtExecInfo4Test("digest4")) // evicts digest2

	// The log is async; wait briefly for drain.
	require.Eventually(t, func() bool {
		storage.Lock()
		defer storage.Unlock()
		return len(storage.evicted) == 2
	}, time.Second, 10*time.Millisecond, "expected 2 evicted records to be logged")

	storage.Lock()
	digests := []string{storage.evicted[0].Digest, storage.evicted[1].Digest}
	storage.Unlock()
	require.ElementsMatch(t, []string{"digest1", "digest2"}, digests)

	// Disable and verify no further log writes.
	require.NoError(t, ss.SetLogEvicted(false))
	ss.Add(GenerateStmtExecInfo4Test("digest5")) // evicts digest3
	time.Sleep(50 * time.Millisecond)
	storage.Lock()
	require.Equal(t, 2, len(storage.evicted))
	storage.Unlock()
}

// stmtExecInfoWithUser returns a StmtExecInfo whose digest and User fields are
// set; everything else is the generic test fixture.
func stmtExecInfoWithUser(digest, user string) *stmtsummary.StmtExecInfo {
	info := GenerateStmtExecInfo4Test(digest)
	info.User = user
	return info
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
