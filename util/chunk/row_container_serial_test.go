// Copyright 2021 PingCAP, Inc.
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
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/memory"
	"github.com/stretchr/testify/require"
)

func TestSpillActionDeadLock(t *testing.T) {
	// Maybe get deadlock if we use two RLock in one goroutine, for oom-action call stack.
	// Now the implement avoids the situation.
	// Goroutine 1: rc.Add() (RLock) -> list.Add() -> tracker.Consume() -> SpillDiskAction -> rc.AlreadySpilledSafeForTest() (RLock)
	// Goroutine 2: ------------------> SpillDiskAction -> new Goroutine to spill -> ------------------
	// new Goroutine created by 2: ---> rc.SpillToDisk (Lock)
	// In golang, RLock will be blocked after try to get Lock. So it will cause deadlock.
	require.Nil(t, failpoint.Enable("github.com/pingcap/tidb/util/chunk/testRowContainerDeadLock", "return(true)"))
	defer func() {
		require.Nil(t, failpoint.Disable("github.com/pingcap/tidb/util/chunk/testRowContainerDeadLock"))
	}()
	sz := 4
	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}
	rc := NewRowContainer(fields, sz)

	chk := NewChunkWithCapacity(fields, sz)
	for i := 0; i < sz; i++ {
		chk.AppendInt64(0, int64(i))
	}
	var tracker *memory.Tracker
	var err error
	tracker = rc.GetMemTracker()
	tracker.SetBytesLimit(1)
	ac := rc.ActionSpillForTest()
	tracker.FallbackOldAndSetNewAction(ac)
	require.False(t, rc.AlreadySpilledSafeForTest())
	go func() {
		time.Sleep(200 * time.Millisecond)
		ac.Action(tracker)
	}()
	err = rc.Add(chk)
	require.NoError(t, err)
	rc.actionSpill.WaitForTest()
	require.True(t, rc.AlreadySpilledSafeForTest())
}

func TestActionBlocked(t *testing.T) {
	sz := 4
	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}
	rc := NewRowContainer(fields, sz)

	chk := NewChunkWithCapacity(fields, sz)
	for i := 0; i < sz; i++ {
		chk.AppendInt64(0, int64(i))
	}
	var tracker *memory.Tracker
	var err error
	// Case 1, test Broadcast in Action.
	tracker = rc.GetMemTracker()
	tracker.SetBytesLimit(1450)
	ac := rc.ActionSpill()
	tracker.FallbackOldAndSetNewAction(ac)
	for i := 0; i < 10; i++ {
		err = rc.Add(chk)
		require.NoError(t, err)
	}

	ac.cond.L.Lock()
	for ac.cond.status == notSpilled ||
		ac.cond.status == spilling {
		ac.cond.Wait()
	}
	ac.cond.L.Unlock()
	ac.cond.L.Lock()
	require.Equal(t, spilledYet, ac.cond.status)
	ac.cond.L.Unlock()
	require.Equal(t, int64(0), tracker.BytesConsumed())
	require.Greater(t, tracker.MaxConsumed(), int64(0))
	require.Greater(t, rc.GetDiskTracker().BytesConsumed(), int64(0))

	// Case 2, test Action will block when spilling.
	rc = NewRowContainer(fields, sz)
	tracker = rc.GetMemTracker()
	ac = rc.ActionSpill()
	starttime := time.Now()
	ac.setStatus(spilling)
	go func() {
		time.Sleep(200 * time.Millisecond)
		ac.setStatus(spilledYet)
		ac.cond.Broadcast()
	}()
	ac.Action(tracker)
	require.GreaterOrEqual(t, time.Since(starttime), 200*time.Millisecond)
}
