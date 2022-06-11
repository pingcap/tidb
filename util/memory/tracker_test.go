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
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/util/mathutil"
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
	for i := 0; i < 10; i++ {
		go func() {
			defer waitGroup.Done()
			tracker.Consume(10)
		}()
	}
	waitGroup.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer waitGroup.Done()
			tracker.Consume(-10)
		}()
	}

	waitGroup.Wait()
	require.Equal(t, int64(100), tracker.BytesConsumed())
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
	require.True(t, action1.called)
	require.False(t, action2.called)
	tracker.Consume(10000)
	require.True(t, action1.called)
	require.True(t, action2.called)

	// test softLimit
	tracker = NewTracker(1, 100)
	action1 = &mockAction{}
	action2 = &mockAction{}
	action3 := &mockAction{}
	tracker.FallbackOldAndSetNewActionForSoftLimit(action1)
	tracker.FallbackOldAndSetNewActionForSoftLimit(action2)
	tracker.SetActionOnExceed(action3)
	require.False(t, action1.called)
	require.False(t, action2.called)
	require.False(t, action3.called)
	tracker.Consume(80)
	require.True(t, action1.called)
	require.False(t, action2.called)
	require.False(t, action3.called)
	tracker.Consume(20)
	require.True(t, action1.called)
	require.True(t, action2.called) // SoftLimit fallback
	require.True(t, action3.called) // HardLimit

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
	require.Equal(t, action1, tracker.actionMuForHardLimit.actionOnExceed)
	require.Equal(t, action2, tracker.actionMuForHardLimit.actionOnExceed.GetFallback())
	action2.SetFinished()
	require.Equal(t, action3, tracker.actionMuForHardLimit.actionOnExceed.GetFallback())
	action3.SetFinished()
	action4.SetFinished()
	require.Equal(t, action5, tracker.actionMuForHardLimit.actionOnExceed.GetFallback())
}

type mockAction struct {
	BaseOOMAction
	called   bool
	priority int64
}

func (a *mockAction) SetLogHook(hook func(uint64)) {
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
	for i := 0; i < 10; i++ {
		tracker := ts[rand.Intn(len(ts))]
		b := rand.Int63n(1000) - 500
		if consumed+b < 0 {
			b = -consumed
		}
		consumed += b
		tracker.Consume(b)
		maxConsumed = mathutil.Max(maxConsumed, consumed)

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
		return byteSizeBB, nil
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
	for i := 0; i < n; i++ {
		actions[i] = &mockAction{priority: int64(i)}
	}

	randomShuffle := make([]int, n)
	for i := 0; i < n; i++ {
		randomShuffle[i] = i
		pos := rand.Int() % (i + 1)
		randomShuffle[i], randomShuffle[pos] = randomShuffle[pos], randomShuffle[i]
	}

	for i := 0; i < n; i++ {
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
