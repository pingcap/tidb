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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package memory

import (
	"errors"
	"math/rand"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/stretchr/testify/require"
)

func TestSetLabel(t *testing.T) {
	tracker := NewTracker(1, -1)
	require.Equal(t, 1, tracker.label)
	require.Equal(t, int64(0), tracker.BytesConsumed())
	require.Equal(t, int64(-1), tracker.GetBytesLimit())
	require.Nil(t, tracker.getParent())
	require.Equal(t, 0, len(tracker.mu.children))
	tracker.SetLabel(2)
	require.Equal(t, 2, tracker.label)
	require.Equal(t, int64(0), tracker.BytesConsumed())
	require.Equal(t, int64(-1), tracker.GetBytesLimit())
	require.Nil(t, tracker.getParent())
	require.Equal(t, 0, len(tracker.mu.children))
}

func TestSetLabel2(t *testing.T) {
	tracker := NewTracker(1, -1)
	tracker2 := NewTracker(2, -1)
	tracker2.AttachTo(tracker)
	tracker2.Consume(10)
	require.Equal(t, tracker.BytesConsumed(), int64(10))
	tracker2.SetLabel(10)
	require.Equal(t, tracker.BytesConsumed(), int64(10))
	tracker2.Detach()
	require.Equal(t, tracker.BytesConsumed(), int64(0))
}

func TestConsume(t *testing.T) {
	tracker := NewTracker(1, -1)
	require.Equal(t, int64(0), tracker.BytesConsumed())

	tracker.Consume(100)
	require.Equal(t, int64(100), tracker.BytesConsumed())

	waitGroup := sync.WaitGroup{}
	waitGroup.Add(10)
	for range 10 {
		go func() {
			defer waitGroup.Done()
			tracker.Consume(10)
		}()
	}
	waitGroup.Add(10)
	for range 10 {
		go func() {
			defer waitGroup.Done()
			tracker.Consume(-10)
		}()
	}

	waitGroup.Wait()
	require.Equal(t, int64(100), tracker.BytesConsumed())
}

func TestRelease(t *testing.T) {
	debug.SetGCPercent(-1)
	defer debug.SetGCPercent(100)
	parentTracker := NewGlobalTracker(LabelForGlobalAnalyzeMemory, -1)
	tracker := NewTracker(1, -1)
	tracker.AttachToGlobalTracker(parentTracker)
	require.Equal(t, int64(0), tracker.BytesConsumed())
	EnableGCAwareMemoryTrack.Store(false)
	tracker.Consume(100)
	require.Equal(t, int64(100), tracker.BytesConsumed())
	require.Equal(t, int64(100), parentTracker.BytesConsumed())
	tracker.Release(100)
	require.Equal(t, int64(0), tracker.BytesConsumed())
	require.Equal(t, int64(0), parentTracker.BytesConsumed())
	require.Equal(t, int64(0), tracker.BytesReleased())
	require.Equal(t, int64(0), parentTracker.BytesReleased())

	EnableGCAwareMemoryTrack.Store(true)
	tracker.Consume(100)
	require.Equal(t, int64(100), tracker.BytesConsumed())
	require.Equal(t, int64(100), parentTracker.BytesConsumed())
	tracker.Release(100)
	require.Equal(t, int64(0), tracker.BytesConsumed())
	require.Equal(t, int64(0), parentTracker.BytesConsumed())
	require.Equal(t, int64(0), tracker.BytesReleased())
	require.Equal(t, int64(100), parentTracker.BytesReleased())
	// finalizer func is called async, need to wait for it to be called
	for {
		runtime.GC()
		if parentTracker.BytesReleased() == 0 {
			break
		}
		time.Sleep(time.Millisecond * 5)
	}
	waitGroup := sync.WaitGroup{}
	waitGroup.Add(10)
	for range 10 {
		go func() {
			defer waitGroup.Done()
			tracker.Consume(10)
		}()
	}
	waitGroup.Add(10)
	for range 10 {
		go func() {
			defer waitGroup.Done()
			tracker.Release(10)
		}()
	}
	waitGroup.Wait()
	// finalizer func is called async, need to wait for it to be called
	for {
		runtime.GC()
		if parentTracker.BytesReleased() == 0 {
			break
		}
		time.Sleep(time.Millisecond * 5)
	}
	require.Equal(t, int64(0), tracker.BytesConsumed())
	require.Equal(t, int64(0), parentTracker.BytesConsumed())
	require.Equal(t, int64(0), tracker.BytesReleased())
}

func TestBufferedConsumeAndRelease(t *testing.T) {
	debug.SetGCPercent(-1)
	defer debug.SetGCPercent(100)
	parentTracker := NewGlobalTracker(LabelForGlobalAnalyzeMemory, -1)
	tracker := NewTracker(1, -1)
	tracker.AttachToGlobalTracker(parentTracker)
	require.Equal(t, int64(0), tracker.BytesConsumed())
	EnableGCAwareMemoryTrack.Store(true)
	bufferedMemSize := int64(0)
	tracker.BufferedConsume(&bufferedMemSize, int64(TrackMemWhenExceeds)/2)
	require.Equal(t, int64(0), tracker.BytesConsumed())
	tracker.BufferedConsume(&bufferedMemSize, int64(TrackMemWhenExceeds)/2)
	require.Equal(t, int64(TrackMemWhenExceeds), tracker.BytesConsumed())
	bufferedReleaseSize := int64(0)
	tracker.BufferedRelease(&bufferedReleaseSize, int64(TrackMemWhenExceeds)/2)
	require.Equal(t, int64(TrackMemWhenExceeds), parentTracker.BytesConsumed())
	require.Equal(t, int64(0), parentTracker.BytesReleased())
	tracker.BufferedRelease(&bufferedReleaseSize, int64(TrackMemWhenExceeds)/2)
	require.Equal(t, int64(0), parentTracker.BytesConsumed())
	require.Equal(t, int64(TrackMemWhenExceeds), parentTracker.BytesReleased())
	// finalizer func is called async, need to wait for it to be called
	for {
		runtime.GC()
		if parentTracker.BytesReleased() == 0 {
			break
		}
		time.Sleep(time.Millisecond * 5)
	}
}

func TestOOMAction(t *testing.T) {
	tracker := NewTracker(1, 100)
	// make sure no panic here.
	tracker.Consume(10000)

	tracker = NewTracker(1, 100)
	action := &mockAction{}
	tracker.SetActionOnExceed(action)

	require.False(t, action.called)
	tracker.Consume(10000)
	require.True(t, action.called)

	// test fallback
	action1 := &mockAction{}
	action2 := &mockAction{}
	tracker.SetActionOnExceed(action1)
	tracker.FallbackOldAndSetNewAction(action2)
	require.False(t, action1.called)
	require.False(t, action2.called)
	tracker.Consume(10000)
	require.True(t, action2.called)
	require.False(t, action1.called)
	tracker.Consume(10000)
	require.True(t, action1.called)
	require.True(t, action2.called)

	// test softLimit
	tracker = NewTracker(1, 100)
	action1 = &mockAction{}
	action2 = &mockAction{}
	action3 := &mockAction{}
	tracker.SetActionOnExceed(action1)
	tracker.FallbackOldAndSetNewActionForSoftLimit(action2)
	tracker.FallbackOldAndSetNewActionForSoftLimit(action3)
	require.False(t, action3.called)
	require.False(t, action2.called)
	require.False(t, action1.called)
	tracker.Consume(80)
	require.True(t, action3.called)
	require.False(t, action2.called)
	require.False(t, action1.called)
	tracker.Consume(20)
	require.True(t, action3.called)
	require.True(t, action2.called) // SoftLimit fallback
	require.True(t, action1.called) // HardLimit

	// test fallback
	action1 = &mockAction{}
	action2 = &mockAction{}
	action3 = &mockAction{}
	action4 := &mockAction{}
	action5 := &mockAction{}
	tracker.SetActionOnExceed(action1)
	tracker.FallbackOldAndSetNewAction(action2)
	tracker.FallbackOldAndSetNewAction(action3)
	tracker.FallbackOldAndSetNewAction(action4)
	tracker.FallbackOldAndSetNewAction(action5)
	require.Equal(t, action5, tracker.actionMuForHardLimit.actionOnExceed)
	require.Equal(t, action4, tracker.actionMuForHardLimit.actionOnExceed.GetFallback())
	action4.SetFinished()
	require.Equal(t, action3, tracker.actionMuForHardLimit.actionOnExceed.GetFallback())
	action3.SetFinished()
	action2.SetFinished()
	require.Equal(t, action1, tracker.actionMuForHardLimit.actionOnExceed.GetFallback())
}

type mockAction struct {
	BaseOOMAction
	called   bool
	priority int64
}

func (a *mockAction) Action(t *Tracker) {
	if a.called && a.fallbackAction != nil {
		a.fallbackAction.Action(t)
		return
	}
	a.called = true
}

func (a *mockAction) GetPriority() int64 {
	return a.priority
}

func TestAttachTo(t *testing.T) {
	oldParent := NewTracker(1, -1)
	newParent := NewTracker(2, -1)
	child := NewTracker(3, -1)
	child.Consume(100)
	child.AttachTo(oldParent)
	require.Equal(t, int64(100), child.BytesConsumed())
	require.Equal(t, int64(100), oldParent.BytesConsumed())
	require.Equal(t, oldParent, child.getParent())
	require.Equal(t, 1, len(oldParent.mu.children))
	require.Equal(t, child, oldParent.mu.children[child.label][0])

	child.AttachTo(newParent)
	require.Equal(t, int64(100), child.BytesConsumed())
	require.Equal(t, int64(0), oldParent.BytesConsumed())
	require.Equal(t, int64(100), newParent.BytesConsumed())
	require.Equal(t, newParent, child.getParent())
	require.Equal(t, 1, len(newParent.mu.children))
	require.Equal(t, child, newParent.mu.children[child.label][0])
	require.Equal(t, 0, len(oldParent.mu.children))
}

func TestDetach(t *testing.T) {
	parent := NewTracker(1, -1)
	child := NewTracker(2, -1)
	child.Consume(100)
	child.AttachTo(parent)
	require.Equal(t, int64(100), child.BytesConsumed())
	require.Equal(t, int64(100), parent.BytesConsumed())
	require.Equal(t, 1, len(parent.mu.children))
	require.Equal(t, child, parent.mu.children[child.label][0])

	child.Detach()
	require.Equal(t, int64(100), child.BytesConsumed())
	require.Equal(t, int64(0), parent.BytesConsumed())
	require.Equal(t, 0, len(parent.mu.children))
	require.Nil(t, child.getParent())
}

func TestReplaceChild(t *testing.T) {
	oldChild := NewTracker(1, -1)
	oldChild.Consume(100)
	newChild := NewTracker(2, -1)
	newChild.Consume(500)
	parent := NewTracker(3, -1)

	oldChild.AttachTo(parent)
	require.Equal(t, int64(100), parent.BytesConsumed())

	parent.ReplaceChild(oldChild, newChild)
	require.Equal(t, int64(500), parent.BytesConsumed())
	require.Equal(t, 1, len(parent.mu.children))
	require.Equal(t, newChild, parent.mu.children[newChild.label][0])
	require.Equal(t, parent, newChild.getParent())
	require.Nil(t, oldChild.getParent())

	parent.ReplaceChild(oldChild, nil)
	require.Equal(t, int64(500), parent.BytesConsumed())
	require.Equal(t, 1, len(parent.mu.children))
	require.Equal(t, newChild, parent.mu.children[newChild.label][0])
	require.Equal(t, parent, newChild.getParent())
	require.Nil(t, oldChild.getParent())

	parent.ReplaceChild(newChild, nil)
	require.Equal(t, int64(0), parent.BytesConsumed())
	require.Equal(t, 0, len(parent.mu.children))
	require.Nil(t, newChild.getParent())
	require.Nil(t, oldChild.getParent())

	node1 := NewTracker(1, -1)
	node2 := NewTracker(2, -1)
	node3 := NewTracker(3, -1)
	node2.AttachTo(node1)
	node3.AttachTo(node2)
	node3.Consume(100)
	require.Equal(t, int64(100), node1.BytesConsumed())
	node2.ReplaceChild(node3, nil)
	require.Equal(t, int64(0), node2.BytesConsumed())
	require.Equal(t, int64(0), node1.BytesConsumed())
}

func TestToString(t *testing.T) {
	parent := NewTracker(1, -1)
	child1 := NewTracker(2, 1000)
	child2 := NewTracker(3, -1)
	child3 := NewTracker(4, -1)
	child4 := NewTracker(5, -1)

	child1.AttachTo(parent)
	child2.AttachTo(parent)
	child3.AttachTo(parent)
	child4.AttachTo(parent)

	child1.Consume(100)
	child2.Consume(2 * 1024)
	child3.Consume(3 * 1024 * 1024)
	child4.Consume(4 * 1024 * 1024 * 1024)

	require.Equal(t, parent.String(), `
"1"{
  "consumed": 4.00 GB
  "2"{
    "quota": 1000 Bytes
    "consumed": 100 Bytes
  }
  "3"{
    "consumed": 2 KB
  }
  "4"{
    "consumed": 3 MB
  }
  "5"{
    "consumed": 4 GB
  }
}
`)
}

func TestMaxConsumed(t *testing.T) {
	r := NewTracker(1, -1)
	c1 := NewTracker(2, -1)
	c2 := NewTracker(3, -1)
	cc1 := NewTracker(4, -1)

	c1.AttachTo(r)
	c2.AttachTo(r)
	cc1.AttachTo(c1)

	ts := []*Tracker{r, c1, c2, cc1}
	var consumed, maxConsumed int64
	for range 10 {
		tracker := ts[rand.Intn(len(ts))]
		b := rand.Int63n(1000) - 500
		if consumed+b < 0 {
			b = -consumed
		}
		consumed += b
		tracker.Consume(b)
		maxConsumed = max(maxConsumed, consumed)

		require.Equal(t, consumed, r.BytesConsumed())
		require.Equal(t, maxConsumed, r.MaxConsumed())
	}
}

func TestGlobalTracker(t *testing.T) {
	r := NewGlobalTracker(1, -1)
	c1 := NewTracker(2, -1)
	c2 := NewTracker(3, -1)
	c1.Consume(100)
	c2.Consume(200)

	c1.AttachToGlobalTracker(r)
	c2.AttachToGlobalTracker(r)
	require.Equal(t, int64(300), r.BytesConsumed())
	require.Equal(t, r, c1.getParent())
	require.Equal(t, r, c2.getParent())
	require.Equal(t, 0, len(r.mu.children))

	c1.DetachFromGlobalTracker()
	c2.DetachFromGlobalTracker()
	require.Equal(t, int64(0), r.BytesConsumed())
	require.Nil(t, c1.getParent())
	require.Nil(t, c2.getParent())
	require.Equal(t, 0, len(r.mu.children))

	defer func() {
		v := recover()
		require.Equal(t, "Attach to a non-GlobalTracker", v)
	}()
	commonTracker := NewTracker(4, -1)
	c1.AttachToGlobalTracker(commonTracker)

	c1.AttachTo(commonTracker)
	require.Equal(t, int64(100), commonTracker.BytesConsumed())
	require.Equal(t, 1, len(commonTracker.mu.children))
	require.Equal(t, commonTracker, c1.getParent())

	c1.AttachToGlobalTracker(r)
	require.Equal(t, int64(0), commonTracker.BytesConsumed())
	require.Equal(t, 0, len(commonTracker.mu.children))
	require.Equal(t, int64(100), r.BytesConsumed())
	require.Equal(t, r, c1.getParent())
	require.Equal(t, 0, len(r.mu.children))

	defer func() {
		v := recover()
		require.Equal(t, "Detach from a non-GlobalTracker", v)
	}()
	c2.AttachTo(commonTracker)
	c2.DetachFromGlobalTracker()
}

func parseByteUnit(str string) (int64, error) {
	u := strings.TrimSpace(str)
	switch u {
	case "GB":
		return byteSizeGB, nil
	case "MB":
		return byteSizeMB, nil
	case "KB":
		return byteSizeKB, nil
	case "Bytes":
		return byteSize, nil
	}
	return 0, errors.New("invalid byte unit: " + str)
}

func parseByte(str string) (int64, error) {
	vBuf := make([]byte, 0, len(str))
	uBuf := make([]byte, 0, 2)
	b := int64(0)
	for _, v := range str {
		if (v >= '0' && v <= '9') || v == '.' {
			vBuf = append(vBuf, byte(v))
		} else if v != ' ' {
			uBuf = append(uBuf, byte(v))
		}
	}
	unit, err := parseByteUnit(string(uBuf))
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseFloat(string(vBuf), 64)
	if err != nil {
		return 0, err
	}
	b = int64(v * float64(unit))
	return b, nil
}

func TestFormatBytesWithPrune(t *testing.T) {
	cases := []struct {
		b string
		s string
	}{
		{"0 Bytes", "0 Bytes"},
		{"1 Bytes", "1 Bytes"},
		{"9 Bytes", "9 Bytes"},
		{"10 Bytes", "10 Bytes"},
		{"999 Bytes", "999 Bytes"},
		{"1 KB", "1024 Bytes"},
		{"1.123 KB", "1.12 KB"},
		{"1.023 KB", "1.02 KB"},
		{"1.003 KB", "1.00 KB"},
		{"10.456 KB", "10.5 KB"},
		{"10.956 KB", "11.0 KB"},
		{"999.056 KB", "999.1 KB"},
		{"999.988 KB", "1000.0 KB"},
		{"1.123 MB", "1.12 MB"},
		{"1.023 MB", "1.02 MB"},
		{"1.003 MB", "1.00 MB"},
		{"10.456 MB", "10.5 MB"},
		{"10.956 MB", "11.0 MB"},
		{"999.056 MB", "999.1 MB"},
		{"999.988 MB", "1000.0 MB"},
		{"1.123 GB", "1.12 GB"},
		{"1.023 GB", "1.02 GB"},
		{"1.003 GB", "1.00 GB"},
		{"10.456 GB", "10.5 GB"},
		{"10.956 GB", "11.0 GB"},
		{"9.412345 MB", "9.41 MB"},
		{"10.412345 MB", "10.4 MB"},
		{"5.999 GB", "6.00 GB"},
		{"100.46 KB", "100.5 KB"},
		{"18.399999618530273 MB", "18.4 MB"},
		{"9.15999984741211 MB", "9.16 MB"},
	}
	for _, ca := range cases {
		b, err := parseByte(ca.b)
		require.NoError(t, err)
		result := FormatBytes(b)
		require.Equalf(t, ca.s, result, "input: %v\n", ca.b)
	}
}

func TestErrorCode(t *testing.T) {
	require.Equal(t, errno.ErrMemExceedThreshold, int(terror.ToSQLError(errMemExceedThreshold).Code))
}

func TestOOMActionPriority(t *testing.T) {
	tracker := NewTracker(1, 100)
	// make sure no panic here.
	tracker.Consume(10000)

	tracker = NewTracker(1, 1)
	tracker.actionMuForHardLimit.actionOnExceed = nil
	n := 100
	actions := make([]*mockAction, n)
	for i := range n {
		actions[i] = &mockAction{priority: int64(i)}
	}

	randomShuffle := make([]int, n)
	for i := range n {
		randomShuffle[i] = i
		pos := rand.Int() % (i + 1)
		randomShuffle[i], randomShuffle[pos] = randomShuffle[pos], randomShuffle[i]
	}

	for i := range n {
		tracker.FallbackOldAndSetNewAction(actions[randomShuffle[i]])
	}
	for i := n - 1; i >= 0; i-- {
		tracker.Consume(100)
		for j := n - 1; j >= 0; j-- {
			if j >= i {
				require.True(t, actions[j].called)
			} else {
				require.False(t, actions[j].called)
			}
		}
	}
}

func TestGlobalMemArbitrator(t *testing.T) {
	SetupGlobalMemArbitratorForTest(t.TempDir())
	defer CleanupGlobalMemArbitratorForTest()
	defer func() {
		require.True(t, globalArbitrator.metrics.pools.big.Load() == 0)
		require.True(t, globalArbitrator.metrics.pools.small.Load() == 0)
		require.True(t, globalArbitrator.metrics.pools.intoBig.Load() == 0)
		require.True(t, globalArbitrator.metrics.pools.internal.Load() == 0)
		require.True(t, globalArbitrator.metrics.pools.internalSession.Load() == 0)
	}()

	newRootTracker := func(uid uint64) (t1 *Tracker) {
		t1 = NewTracker(1, -1)
		{
			t1.IsRootTrackerOfSess = true
			t1.Killer = &sqlkiller.SQLKiller{}
			t1.Killer.ConnID.Store(uid)
			t1.SessionID.Store(uid)
		}
		return
	}

	{
		r := newMemStateRecorder(t.TempDir())
		os.RemoveAll(r.baseDir)
		{
			m, err := r.Load()
			require.Error(t, err)
			require.True(t, m == nil)
		}
		src := RuntimeMemStateV1{Version: 1, LastRisk: LastRisk{HeapAlloc: 2, QuotaAlloc: 3}, Magnif: 4, PoolMediumCap: 5}
		r.Store(&src)
		{
			m, err := r.Load()
			require.NoError(t, err)
			require.Equal(t, *m, src)
		}
		{
			f, err := os.OpenFile(r.filePath, os.O_WRONLY, 0666)
			require.NoError(t, err)
			_, err = f.Write([]byte("??????"))
			require.NoError(t, err)
			f.Close()
		}
		{
			m, err := r.Load()
			require.Error(t, err)
			require.True(t, m == nil)
		}
	}
	{ // test implicitly start global mem arbitrator
		ServerMemoryLimitOriginText.Store("10g")
		ServerMemoryLimit.Store(10 << 30)
		AjustGlobalMemArbitratorLimit()
		SetGlobalMemArbitratorSoftLimit("0.88")

		require.True(t, GlobalMemArbitrator() == nil)
		require.True(t, GlobalMemArbitrator().SoftLimit() == 0)

		require.True(t, SetGlobalMemArbitratorWorkMode(ArbitratorModeStandardName))
		m := GlobalMemArbitrator()
		require.True(t, m.execMetrics.Action.UpdateRuntimeMemStats == 1)

		require.True(t, m != nil)
		require.True(t, m.SoftLimit() == uint64(0.88*float64(ServerMemoryLimit.Load())))
		require.True(t, SetGlobalMemArbitratorWorkMode(ArbitratorModeDisableName))
		m.actions.UpdateRuntimeMemStats = func() {
			m.SetRuntimeMemStats(RuntimeMemStats{})
		}
		m.refreshRuntimeMemStats()
		require.True(t, m.execMetrics.Action.UpdateRuntimeMemStats == 2)
		require.True(t, m.limit()-m.softLimit() == m.avoidance.size.Load())

		require.True(t, GlobalMemArbitrator().SoftLimit() == 0)
	}
	{
		require.True(t, SetGlobalMemArbitratorWorkMode(ArbitratorModeStandardName))

		uid := uint64(719)
		t0 := NewTracker(0, -1)   // upstream tracker
		t1 := newRootTracker(uid) // session root
		t2 := NewTracker(1, -1)   // leaf
		t2.AttachTo(t1)
		t1.AttachTo(t0)

		require.True(t, t1.MemArbitrator == nil)
		m := GlobalMemArbitrator()
		// init mem arbitrator for the session tracker
		require.True(t,
			t1.InitMemArbitrator(m, 1<<30, nil, ("test sql 1"), ArbitrationPriorityHigh, false, 1+byteSizeKB, false))
		require.True(t, t1.MemArbitrator != nil)
		require.True(t, t1.MemArbitrator.MemArbitrator == m)
		require.True(t, t1.MemArbitrator.budget.useBig.Load())

		expectCap := int64(1+byteSizeKB) * 1053 / 1000
		require.True(t, m.FindRootPool(t1.MemArbitrator.uid).entry.pool.Capacity() == expectCap)
		require.True(t, m.FindRootPool(t1.MemArbitrator.uid).entry.pool.Allocated() == expectCap)
		require.True(t, t1.MemArbitrator.bigBudgetCap() == expectCap)
		require.True(t, t1.MemArbitrator.bigBudgetUsed() == 0)
		require.True(t, t1.MemArbitrator.bigBudgetGrowThreshold() == expectCap*95/100)
		require.True(t, m.Allocated() == expectCap)
		oriExecMetrics := m.ExecMetrics()
		require.True(t, oriExecMetrics.Task.Succ == 1)
		require.True(t, t1.bytesConsumed == 0)

		t2.Consume(byteSizeKB)
		require.True(t, m.FindRootPool(t1.MemArbitrator.uid).entry.pool.Capacity() == expectCap)
		require.True(t, m.FindRootPool(t1.MemArbitrator.uid).entry.pool.Allocated() == expectCap)
		require.True(t, t1.MemArbitrator.bigBudgetCap() == expectCap)
		require.True(t, t1.MemArbitrator.bigBudgetUsed() == byteSizeKB)
		require.True(t, t1.MemArbitrator.bigBudgetGrowThreshold() == expectCap*95/100)
		require.Equal(t, m.ExecMetrics(), oriExecMetrics)
		require.True(t, t1.bytesConsumed == byteSizeKB)

		t2.Consume(byteSizeMB)
		require.True(t, t1.MemArbitrator.bigBudgetUsed() == byteSizeKB+byteSizeMB)
		require.True(t, t2.maxConsumed.Load() == byteSizeKB+byteSizeMB)
		expectCap = ((byteSizeKB + byteSizeMB) * 2783) >> 10
		require.True(t, t1.MemArbitrator.bigBudgetCap() == expectCap)
		expectMetrics := oriExecMetrics
		expectMetrics.Task.Succ++
		require.True(t, m.execMetrics == expectMetrics)
		require.True(t, m.FindRootPool(t1.MemArbitrator.uid).entry.pool.Capacity() == expectCap)

		t2.Release(byteSizeKB)
		t2.Release(byteSizeMB)

		require.True(t, t1.bytesConsumed == 0)
		require.True(t, t1.MemArbitrator.useBigBudget())
		require.True(t, t1.MemArbitrator.bigBudgetUsed() == 0)
		require.True(t, t1.MemArbitrator.bigBudgetCap() == t1.MemArbitrator.budget.mu.bigB.Pool.capacity())
		require.True(t, t1.MaxConsumed() == byteSizeKB+byteSizeMB)
		require.True(t, m.Allocated() == expectCap)
		require.True(t, m.TaskNum() == 0)
		require.True(t, m.WaitingAllocSize() == 0)
		{
			execMetrics := m.ExecMetrics()
			require.True(t, execMetrics.Task.pairSuccessFail == pairSuccessFail{2, 0})
			require.True(t, execMetrics.Task.SuccByPriority == NumByPriority{})
		}
		require.True(t, t1.Killer.Signal == 0)

		newLimit := int64(5e14)
		m.SetLimit(uint64(newLimit))
		m.SetWorkMode(ArbitratorModeStandard)
		rest := (t1.MemArbitrator.bigBudgetCap() - t1.MemArbitrator.bigBudgetUsed())
		t2.Consume(rest + 1)
		require.Panics(t, func() {
			t2.Consume(newLimit)
		})
		require.True(t, t1.Killer.Signal == sqlkiller.KilledByMemArbitrator)
		{
			execMetrics := m.ExecMetrics()
			require.True(t, execMetrics.Task.pairSuccessFail == pairSuccessFail{3, 1})
			require.True(t, execMetrics.Task.SuccByPriority == NumByPriority{})
		}

		require.True(t, t1.MemArbitration() > 0)
		t1.Detach()
		require.True(t, RemovePoolFromGlobalMemArbitrator(t1.MemArbitrator.uid))
		require.False(t, m.RemoveRootPoolByID(t1.MemArbitrator.uid))
		require.False(t, RemovePoolFromGlobalMemArbitrator(0))
		require.True(t, m.digestProfileCache.num.Load() == 1)

		tx := newRootTracker(uid)
		require.True(t,
			tx.InitMemArbitrator(m, 0, nil, "test sql x", ArbitrationPriorityHigh, false, 0, false))
		require.False(t, tx.MemArbitrator.useBigBudget())
		require.True(t, tx.MemArbitrator.reserveSize == 0)
		require.True(t, tx.MemArbitrator.ctx.PrevMaxMem == 0)
		require.True(t, m.awaitFreePoolUsed() == memPoolQuotaUsage{})
		require.True(t, m.awaitFreePoolCap() == 0)
		tx.Consume(13)
		require.True(t, tx.MemArbitrator.smallBudgetUsed() == 13)
		require.True(t, m.awaitFreePoolUsed() == memPoolQuotaUsage{13, 13})
		require.True(t, m.awaitFreePoolCap() != 0)
		tx.Consume(17)
		require.True(t, tx.MemArbitrator.smallBudgetUsed() == 30)
		require.True(t, m.awaitFreePoolUsed() == memPoolQuotaUsage{30, 30})
		require.True(t, m.awaitFreePoolCap() != 0)
		tx.Detach()
		require.True(t, m.digestProfileCache.num.Load() == 2)
		require.True(t, tx.MemArbitrator.smallBudgetUsed() == 0)
		require.True(t, m.awaitFreePoolUsed() == memPoolQuotaUsage{})
		require.True(t, m.awaitFreePoolCap() != 0)
		m.shrinkAwaitFreePool(0, defAwaitFreePoolShrinkDurMilli+nowUnixMilli())
		require.True(t, m.awaitFreePoolCap() == 0)
		require.False(t, RemovePoolFromGlobalMemArbitrator(tx.MemArbitrator.uid)) // only small budget pool will be used
		tx.MemArbitrator.addSmallBudget(17)
		require.True(t, tx.MemArbitrator.smallBudgetUsed() == 17)
		require.True(t, m.awaitFreePoolUsed() == memPoolQuotaUsage{17, 17})
		tx.Reset()

		oriMaxMem := tx.MaxConsumed()
		tx = newRootTracker(720)
		require.True(t,
			tx.InitMemArbitrator(m, 0, nil, "test sql x", ArbitrationPriorityHigh, false, 0, false))
		require.True(t, !tx.MemArbitrator.useBigBudget())
		require.True(t, tx.MemArbitrator.reserveSize == 0)
		require.True(t, tx.MemArbitrator.ctx.PrevMaxMem == oriMaxMem)
		tx.Consume(7)
		require.True(t, tx.MemArbitrator.smallBudgetUsed() == 7)
		tx.Consume(newLimit/1000 + 1)
		require.True(t, tx.BytesConsumed() == newLimit/1000+1+7)
		require.True(t, tx.MemArbitrator.useBigBudget())
		m.shrinkAwaitFreePool(0, defAwaitFreePoolShrinkDurMilli+nowUnixMilli())
		require.True(t, m.awaitFreePoolCap() == 0)
		require.True(t, RemovePoolFromGlobalMemArbitrator(tx.MemArbitrator.uid))

		tx = newRootTracker(727)
		require.True(t,
			tx.InitMemArbitrator(m, 0, nil, "test sql 1", ArbitrationPriorityHigh, false, 0, false))
		require.True(t, tx.MemArbitrator.useBigBudget())
		require.True(t, tx.MemArbitrator.reserveSize == 0)
		require.Equal(t, t1.MaxConsumed(), tx.MemArbitrator.ctx.PrevMaxMem)
		require.True(t, RemovePoolFromGlobalMemArbitrator(tx.MemArbitrator.uid))
		require.True(t, m.digestProfileCache.num.Load() == 2)
	}

	{
		require.True(t, SetGlobalMemArbitratorWorkMode(ArbitratorModePriorityName))

		trackers := [3]*Tracker{}
		m := GlobalMemArbitrator()
		m.stop()
		m.resetExecMetricsForTest()
		m.restartForTest()
		latestTaskNum := func() int64 {
			m.tasks.Lock()
			defer m.tasks.Unlock()
			return m.TaskNum()
		}

		newLimit := int64(5e14)
		m.SetLimit(uint64(newLimit))
		require.True(t, m != nil)
		oriMetrics := m.ExecMetrics()
		m.stop()
		wg := sync.WaitGroup{}
		for i := range trackers {
			uid := uint64(i + 1)
			trackers[i] = newRootTracker(uid)
			wg.Go(func() {
				require.True(t,
					trackers[i].InitMemArbitrator(m, 1, trackers[i].Killer, "?", ArbitrationPriority(int(ArbitrationPriorityLow)+i), false, newLimit, false))
				trackers[i].Detach()
			})
		}
		for latestTaskNum() != int64(len(trackers)) {
			runtime.Gosched()
		}
		execMetrics := m.ExecMetrics()
		require.Equal(t, NumByPattern{1, 1, 1, 0}, m.TaskNumByPattern())
		require.True(t, m.TaskNum() == 3)
		require.Equal(t, m.WaitingAllocSize(), newLimit*1053/1000*3)

		m.restartForTest()
		wg.Wait()

		// exec by priority order high -> medium -> low
		for i := range trackers {
			require.NoError(t, trackers[i].Killer.HandleSignal())
		}

		execMetrics = m.ExecMetrics()
		for p := range maxArbitrationPriority {
			require.True(t, execMetrics.Task.SuccByPriority[p]-oriMetrics.Task.SuccByPriority[p] == 1)
		}

		for i := range trackers {
			uid := uint64(i + 1)
			trackers[i] = newRootTracker(uid)
			require.True(t,
				trackers[i].InitMemArbitrator(m, 1, trackers[i].Killer, "?", ArbitrationPriority(int(ArbitrationPriorityLow)+i), false, 1, false))
		}
		execMetrics = m.ExecMetrics()
		for p := range maxArbitrationPriority {
			require.True(t, execMetrics.Task.SuccByPriority[p]-oriMetrics.Task.SuccByPriority[p] == 2)
		}

		require.Equal(t, m.privilegedEntry.pool.uid, trackers[0].MemArbitrator.uid)

		m.stop()
		wg = sync.WaitGroup{}
		consumeEvent := make([]int, 0)
		mu := sync.Mutex{}
		for i := range trackers {
			wg.Go(func() {
				defer func() {
					recover()
					trackers[i].Detach()
				}()
				n := 1
				if i == 0 {
					n = 2
				}
				for range n {
					trackers[i].Consume(newLimit)
					{
						mu.Lock()
						consumeEvent = append(consumeEvent, int(trackers[i].MemArbitrator.uid))
						mu.Unlock()
					}
				}
			})
		}
		for latestTaskNum() != 3 {
			runtime.Gosched()
		}
		m.restartForTest()
		wg.Wait()

		{
			require.Equal(t, consumeEvent, []int{1, 3, 2})
			// priority low: canceled
			require.ErrorContains(t, trackers[0].Killer.HandleSignal(), "[executor:8180]Query execution was stopped by the global memory arbitrator [reason=CANCEL(out-of-quota & priority-mode)] [conn=")
			// exec by priority order high -> medium
			require.NoError(t, trackers[1].Killer.HandleSignal())
			require.NoError(t, trackers[2].Killer.HandleSignal())
		}

		for i := range trackers {
			RemovePoolFromGlobalMemArbitrator(trackers[i].SessionID.Load())
		}

		m.stop()
		require.True(t, m.execMetrics.Cancel.PriorityMode == NumByPriority{1, 0, 0})
	}

	{ // interrupt execution
		require.False(t, SetGlobalMemArbitratorWorkMode(ArbitratorModePriorityName))
		m := GlobalMemArbitrator()
		m.stop()
		m.resetExecMetricsForTest()
		m.restartForTest()

		t1 := newRootTracker(13)
		t2 := newRootTracker(17)
		require.True(t,
			t2.InitMemArbitrator(GlobalMemArbitrator(), 0, t2.Killer, "?", ArbitrationPriorityMedium, false, m.limit()/2, false))
		require.True(t,
			t1.InitMemArbitrator(GlobalMemArbitrator(), 0, t1.Killer, "?", ArbitrationPriorityMedium, false, 1, false))
		wg := sync.WaitGroup{}
		wg.Go(func() {
			defer func() {
				recover()
				t1.Detach()
			}()
			t1.Killer.SendKillSignal(sqlkiller.QueryInterrupted)
			t1.Consume(m.limit())
		})
		wg.Wait()
		require.ErrorContains(t, t1.Killer.HandleSignal(), "[executor:1317]Query execution was interrupted")
		RemovePoolFromGlobalMemArbitrator(t1.SessionID.Load())
		RemovePoolFromGlobalMemArbitrator(t2.SessionID.Load())
	}

	{ // oom risk
		require.False(t, SetGlobalMemArbitratorWorkMode(ArbitratorModePriorityName))
		m := GlobalMemArbitrator()
		m.stop()
		m.resetExecMetricsForTest()
		m.SetSoftLimit(0, 0, SoftLimitModeAuto)

		t1 := newRootTracker(19)
		t2 := newRootTracker(23)

		m.restartForTest()
		require.True(t,
			t1.InitMemArbitrator(m, 0, t1.Killer, "?", ArbitrationPriorityMedium, false, 0, false))
		require.True(t,
			t2.InitMemArbitrator(m, 0, t2.Killer, "?", ArbitrationPriorityLow, false, 0, false))
		t1.Consume(m.limit() / 4)
		t2.Consume(m.limit() / 2)
		m.stop()
		m.resetExecMetricsForTest()

		require.True(t, m.allocated() == m.limit()/4+m.limit()/2)
		var mockRuntimeMemStats RuntimeMemStats
		m.actions.UpdateRuntimeMemStats = func() {
			m.SetRuntimeMemStats(mockRuntimeMemStats)
		}
		mockRuntimeMemStats = RuntimeMemStats{
			HeapAlloc:  m.limit(),
			HeapInuse:  m.limit(),
			TotalFree:  0,
			MemOffHeap: 0,
		}
		m.HandleRuntimeStats(mockRuntimeMemStats)
		m.runOneRound()

		{
			execMetrics := m.ExecMetrics()
			require.True(t, execMetrics.Risk.Mem == 1)
			require.True(t, execMetrics.Risk.OOM == 0)
			require.True(t, execMetrics.Action.GC == 1)
			require.True(t, execMetrics.Action.UpdateRuntimeMemStats == 1)
			require.True(t, execMetrics.Action.RecordMemState.Succ == 1)
		}

		mockNow = func() time.Time {
			return m.heapController.memRisk.startTime.t.Add(time.Second)
		}
		mockRuntimeMemStats = RuntimeMemStats{
			HeapAlloc:  m.limit(),
			HeapInuse:  m.limit(),
			TotalFree:  1,
			MemOffHeap: 0,
		}
		m.runOneRound()
		require.Equal(t, NumByPriority{1, 0, 0}, m.ExecMetrics().Risk.OOMKill)
		require.True(t, t2.Killer.Signal == sqlkiller.KilledByMemArbitrator)
		wg := sync.WaitGroup{}
		var err error
		wg.Go(func() {
			defer func() {
				err = recover().(error)
			}()
			t2.Consume(m.limit())
		})
		wg.Wait()
		require.ErrorContains(t, err, "[executor:8180]Query execution was stopped by the global memory arbitrator [reason=KILL(out-of-memory)] [conn=")
		RemovePoolFromGlobalMemArbitrator(t1.SessionID.Load())
		RemovePoolFromGlobalMemArbitrator(t2.SessionID.Load())
		m.runOneRound()

		memState, _ := m.heapController.memStateRecorder.Load()
		require.True(t, memState.Magnif == 1433)
	}
	{
		CleanupGlobalMemArbitratorForTest()
		SetupGlobalMemArbitratorForTest(t.TempDir())

		require.True(t, SetGlobalMemArbitratorWorkMode(ArbitratorModePriorityName))
		m := GlobalMemArbitrator()
		m.SetLimit(1e9)
		m.restartForTest()

		t1 := newRootTracker(29)
		require.True(t,
			t1.InitMemArbitrator(m, 0, t1.Killer, "", ArbitrationPriorityMedium, false, 0, true))
		require.True(t, globalArbitrator.metrics.pools.internal.Load() == 1)
		require.True(t, globalArbitrator.metrics.pools.internalSession.Load() == 0)

		t1.MemArbitrator.ctx.PrevMaxMem = 1 // mock set prev max mem to trigger reserve big budget
		require.True(t, t1.MemArbitrator.state.Load() == memArbitratorStateSmallBudget)
		t1.Consume(1e5)
		require.True(t, t1.bytesConsumed == 1e5)
		require.True(t, t1.MemArbitrator.smallBudgetUsed() == 1e5)
		require.True(t, m.awaitFreePoolUsed().quota == 1e5)
		require.True(t, t1.MemArbitrator.state.Load() == memArbitratorStateSmallBudget)
		require.True(t, globalArbitrator.metrics.pools.small.Load() == 1)
		wg := sync.WaitGroup{}
		mockDebugInject = func() {
			require.True(t, t1.MemArbitrator.state.Load() == memArbitratorStateIntoBigBudget)
			require.True(t, globalArbitrator.metrics.pools.intoBig.Load() == 1)
			require.True(t, globalArbitrator.metrics.pools.small.Load() == 0)
			wg.Go(func() {
				t1.Detach()
				require.True(t, t1.MemArbitrator != nil)
				require.False(t, t1.DetachMemArbitrator())
			})
			for t1.MemArbitrator.state.Load() != memArbitratorStateDown {
				runtime.Gosched()
			}

			mockDebugInject = func() {
				require.True(t, globalArbitrator.metrics.pools.intoBig.Load() == 1)
				require.True(t, m.awaitFreePoolUsed().quota == 0)
				require.True(t, t1.MemArbitrator.useBigBudget())
				require.True(t, t1.MemArbitrator.smallBudgetUsed() == 0)

				used, growThreshold, capacity := t1.MemArbitrator.bigBudgetUsed(), t1.MemArbitrator.bigBudgetGrowThreshold(), t1.MemArbitrator.bigBudgetCap()
				require.True(t, used == t1.bytesConsumed)
				require.True(t, growThreshold == 1e5*0.95)
				require.True(t, capacity == 1e5)
				require.True(t, t1.MemArbitrator.bigBudget().Pool.allocated() == 1e5) // use big budget pool
				require.True(t, globalArbitrator.metrics.pools.internal.Load() == 1)
				require.True(t, globalArbitrator.metrics.pools.internalSession.Load() == 1)
			}
		}
		t1.Consume(1e8)
		wg.Wait()
		require.True(t, globalArbitrator.metrics.pools.internal.Load() == 0)
		require.True(t, globalArbitrator.metrics.pools.internalSession.Load() == 1)

		// all budget released
		require.True(t, globalArbitrator.metrics.pools.big.Load() == 0)
		require.True(t, globalArbitrator.metrics.pools.intoBig.Load() == 0)
		require.True(t, m.awaitFreePoolUsed().quota == 0)
		require.True(t, t1.MemArbitrator.useBigBudget())
		require.True(t, t1.MemArbitrator.smallBudgetUsed() == 0)
		require.True(t, t1.MemArbitrator.bigBudget().Pool.allocated() == 0)
		used, growThreshold, capacity := t1.MemArbitrator.bigBudgetUsed(), t1.MemArbitrator.bigBudgetGrowThreshold(), t1.MemArbitrator.bigBudgetCap()
		require.True(t, used == t1.bytesConsumed)
		require.True(t, growThreshold == 0)
		require.True(t, capacity == 0)

		t1.Consume(1e8)

		// can not use big budget after detach
		require.True(t, t1.bytesConsumed == used+1e8)
		require.True(t, t1.MemArbitrator.bigBudgetGrowThreshold() == 0)
		require.True(t, t1.MemArbitrator.bigBudgetCap() == 0)
		require.True(t, t1.MemArbitrator.bigBudget().Pool.allocated() == 0)

		// mock add small budget
		t1.MemArbitrator.addSmallBudget(1)
		require.True(t, t1.MemArbitrator.smallBudgetUsed() == 1)
		require.True(t, m.awaitFreePoolUsed().quota == 1)

		require.False(t, t1.DetachMemArbitrator())

		// clean small budget
		require.True(t, t1.MemArbitrator.smallBudgetUsed() == 0)
		require.True(t, m.awaitFreePoolUsed().quota == 0)

		RemovePoolFromGlobalMemArbitrator(t1.SessionID.Load())
	}
}
