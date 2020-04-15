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
	"math/rand"
	"os"
	"sync"
	"testing"

	"github.com/cznic/mathutil"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/v4/errno"
	"github.com/pingcap/tidb/v4/util/logutil"
	"github.com/pingcap/tidb/v4/util/stringutil"
	"github.com/pingcap/tidb/v4/util/testleak"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	logLevel := os.Getenv("log_level")
	logutil.InitLogger(logutil.NewLogConfig(logLevel, logutil.DefaultLogFormat, "", logutil.EmptyFileLogConfig, false))
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct{}

func (s *testSuite) SetUpSuite(c *C)    {}
func (s *testSuite) TearDownSuite(c *C) {}
func (s *testSuite) SetUpTest(c *C)     { testleak.BeforeTest() }
func (s *testSuite) TearDownTest(c *C)  { testleak.AfterTest(c)() }

func (s *testSuite) TestSetLabel(c *C) {
	tracker := NewTracker(stringutil.StringerStr("old label"), -1)
	c.Assert(tracker.label.String(), Equals, "old label")
	c.Assert(tracker.BytesConsumed(), Equals, int64(0))
	c.Assert(tracker.bytesLimit, Equals, int64(-1))
	c.Assert(tracker.parent, IsNil)
	c.Assert(len(tracker.mu.children), Equals, 0)
	tracker.SetLabel(stringutil.StringerStr("new label"))
	c.Assert(tracker.label.String(), Equals, "new label")
	c.Assert(tracker.BytesConsumed(), Equals, int64(0))
	c.Assert(tracker.bytesLimit, Equals, int64(-1))
	c.Assert(tracker.parent, IsNil)
	c.Assert(len(tracker.mu.children), Equals, 0)
}

func (s *testSuite) TestConsume(c *C) {
	tracker := NewTracker(stringutil.StringerStr("tracker"), -1)
	c.Assert(tracker.BytesConsumed(), Equals, int64(0))

	tracker.Consume(100)
	c.Assert(tracker.BytesConsumed(), Equals, int64(100))

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
	c.Assert(tracker.BytesConsumed(), Equals, int64(100))
}

func (s *testSuite) TestOOMAction(c *C) {
	tracker := NewTracker(stringutil.StringerStr("oom tracker"), 100)
	// make sure no panic here.
	tracker.Consume(10000)

	tracker = NewTracker(stringutil.StringerStr("oom tracker"), 100)
	action := &mockAction{}
	tracker.SetActionOnExceed(action)

	c.Assert(action.called, IsFalse)
	tracker.Consume(10000)
	c.Assert(action.called, IsTrue)

	// test fallback
	action1 := &mockAction{}
	action2 := &mockAction{}
	tracker.SetActionOnExceed(action1)
	tracker.FallbackOldAndSetNewAction(action2)
	c.Assert(action1.called, IsFalse)
	c.Assert(action2.called, IsFalse)
	tracker.Consume(10000)
	c.Assert(action1.called, IsFalse)
	c.Assert(action2.called, IsTrue)
	tracker.Consume(10000)
	c.Assert(action1.called, IsTrue)
	c.Assert(action2.called, IsTrue)
}

type mockAction struct {
	called   bool
	fallback ActionOnExceed
}

func (a *mockAction) SetLogHook(hook func(uint64)) {
}

func (a *mockAction) Action(t *Tracker) {
	if a.called && a.fallback != nil {
		a.fallback.Action(t)
		return
	}
	a.called = true
}

func (a *mockAction) SetFallback(fallback ActionOnExceed) {
	a.fallback = fallback
}

func (s *testSuite) TestAttachTo(c *C) {
	oldParent := NewTracker(stringutil.StringerStr("old parent"), -1)
	newParent := NewTracker(stringutil.StringerStr("new parent"), -1)
	child := NewTracker(stringutil.StringerStr("child"), -1)
	child.Consume(100)
	child.AttachTo(oldParent)
	c.Assert(child.BytesConsumed(), Equals, int64(100))
	c.Assert(oldParent.BytesConsumed(), Equals, int64(100))
	c.Assert(child.parent, DeepEquals, oldParent)
	c.Assert(len(oldParent.mu.children), Equals, 1)
	c.Assert(oldParent.mu.children[0], DeepEquals, child)

	child.AttachTo(newParent)
	c.Assert(child.BytesConsumed(), Equals, int64(100))
	c.Assert(oldParent.BytesConsumed(), Equals, int64(0))
	c.Assert(newParent.BytesConsumed(), Equals, int64(100))
	c.Assert(child.parent, DeepEquals, newParent)
	c.Assert(len(newParent.mu.children), Equals, 1)
	c.Assert(newParent.mu.children[0], DeepEquals, child)
	c.Assert(len(oldParent.mu.children), Equals, 0)
}

func (s *testSuite) TestReplaceChild(c *C) {
	oldChild := NewTracker(stringutil.StringerStr("old child"), -1)
	oldChild.Consume(100)
	newChild := NewTracker(stringutil.StringerStr("new child"), -1)
	newChild.Consume(500)
	parent := NewTracker(stringutil.StringerStr("parent"), -1)

	oldChild.AttachTo(parent)
	c.Assert(parent.BytesConsumed(), Equals, int64(100))

	parent.ReplaceChild(oldChild, newChild)
	c.Assert(parent.BytesConsumed(), Equals, int64(500))
	c.Assert(len(parent.mu.children), Equals, 1)
	c.Assert(parent.mu.children[0], DeepEquals, newChild)
	c.Assert(newChild.parent, DeepEquals, parent)
	c.Assert(oldChild.parent, IsNil)

	parent.ReplaceChild(oldChild, nil)
	c.Assert(parent.BytesConsumed(), Equals, int64(500))
	c.Assert(len(parent.mu.children), Equals, 1)
	c.Assert(parent.mu.children[0], DeepEquals, newChild)
	c.Assert(newChild.parent, DeepEquals, parent)
	c.Assert(oldChild.parent, IsNil)

	parent.ReplaceChild(newChild, nil)
	c.Assert(parent.BytesConsumed(), Equals, int64(0))
	c.Assert(len(parent.mu.children), Equals, 0)
	c.Assert(newChild.parent, IsNil)
	c.Assert(oldChild.parent, IsNil)

	node1 := NewTracker(stringutil.StringerStr("Node1"), -1)
	node2 := NewTracker(stringutil.StringerStr("Node2"), -1)
	node3 := NewTracker(stringutil.StringerStr("Node3"), -1)
	node2.AttachTo(node1)
	node3.AttachTo(node2)
	node3.Consume(100)
	c.Assert(node1.BytesConsumed(), Equals, int64(100))
	node2.ReplaceChild(node3, nil)
	c.Assert(node2.BytesConsumed(), Equals, int64(0))
	c.Assert(node1.BytesConsumed(), Equals, int64(0))
}

func (s *testSuite) TestToString(c *C) {
	parent := NewTracker(stringutil.StringerStr("parent"), -1)

	child1 := NewTracker(stringutil.StringerStr("child 1"), 1000)
	child2 := NewTracker(stringutil.StringerStr("child 2"), -1)
	child3 := NewTracker(stringutil.StringerStr("child 3"), -1)
	child4 := NewTracker(stringutil.StringerStr("child 4"), -1)

	child1.AttachTo(parent)
	child2.AttachTo(parent)
	child3.AttachTo(parent)
	child4.AttachTo(parent)

	child1.Consume(100)
	child2.Consume(2 * 1024)
	child3.Consume(3 * 1024 * 1024)
	child4.Consume(4 * 1024 * 1024 * 1024)

	c.Assert(parent.String(), Equals, `
"parent"{
  "consumed": 4.00293168798089 GB
  "child 1"{
    "quota": 1000 Bytes
    "consumed": 100 Bytes
  }
  "child 2"{
    "consumed": 2 KB
  }
  "child 3"{
    "consumed": 3 MB
  }
  "child 4"{
    "consumed": 4 GB
  }
}
`)
}

func (s *testSuite) TestMaxConsumed(c *C) {
	r := NewTracker(stringutil.StringerStr("root"), -1)
	c1 := NewTracker(stringutil.StringerStr("child 1"), -1)
	c2 := NewTracker(stringutil.StringerStr("child 2"), -1)
	cc1 := NewTracker(stringutil.StringerStr("child of child 1"), -1)

	c1.AttachTo(r)
	c2.AttachTo(r)
	cc1.AttachTo(c1)

	ts := []*Tracker{r, c1, c2, cc1}
	var consumed, maxConsumed int64
	for i := 0; i < 10; i++ {
		t := ts[rand.Intn(len(ts))]
		b := rand.Int63n(1000) - 500
		if consumed+b < 0 {
			b = -consumed
		}
		consumed += b
		t.Consume(b)
		maxConsumed = mathutil.MaxInt64(maxConsumed, consumed)

		c.Assert(r.BytesConsumed(), Equals, consumed)
		c.Assert(r.MaxConsumed(), Equals, maxConsumed)
	}
}

func BenchmarkConsume(b *testing.B) {
	tracker := NewTracker(stringutil.StringerStr("root"), -1)
	b.RunParallel(func(pb *testing.PB) {
		childTracker := NewTracker(stringutil.StringerStr("child"), -1)
		childTracker.AttachTo(tracker)
		for pb.Next() {
			childTracker.Consume(256 << 20)
		}
	})
}

func (s *testSuite) TestErrorCode(c *C) {
	c.Assert(int(errMemExceedThreshold.ToSQLError().Code), Equals, errno.ErrMemExceedThreshold)
}
