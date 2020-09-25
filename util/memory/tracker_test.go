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
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/testleak"
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
	tracker := NewTracker(1, -1)
	c.Assert(tracker.label, Equals, 1)
	c.Assert(tracker.BytesConsumed(), Equals, int64(0))
	c.Assert(tracker.bytesLimit, Equals, int64(-1))
	c.Assert(tracker.getParent(), IsNil)
	c.Assert(len(tracker.mu.children), Equals, 0)
	tracker.SetLabel(2)
	c.Assert(tracker.label, Equals, 2)
	c.Assert(tracker.BytesConsumed(), Equals, int64(0))
	c.Assert(tracker.bytesLimit, Equals, int64(-1))
	c.Assert(tracker.getParent(), IsNil)
	c.Assert(len(tracker.mu.children), Equals, 0)
}

func (s *testSuite) TestConsume(c *C) {
	tracker := NewTracker(1, -1)
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
	tracker := NewTracker(1, 100)
	// make sure no panic here.
	tracker.Consume(10000)

	tracker = NewTracker(1, 100)
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
	oldParent := NewTracker(1, -1)
	newParent := NewTracker(2, -1)
	child := NewTracker(3, -1)
	child.Consume(100)
	child.AttachTo(oldParent)
	c.Assert(child.BytesConsumed(), Equals, int64(100))
	c.Assert(oldParent.BytesConsumed(), Equals, int64(100))
	c.Assert(child.getParent(), DeepEquals, oldParent)
	c.Assert(len(oldParent.mu.children), Equals, 1)
	c.Assert(oldParent.mu.children[0], DeepEquals, child)

	child.AttachTo(newParent)
	c.Assert(child.BytesConsumed(), Equals, int64(100))
	c.Assert(oldParent.BytesConsumed(), Equals, int64(0))
	c.Assert(newParent.BytesConsumed(), Equals, int64(100))
	c.Assert(child.getParent(), DeepEquals, newParent)
	c.Assert(len(newParent.mu.children), Equals, 1)
	c.Assert(newParent.mu.children[0], DeepEquals, child)
	c.Assert(len(oldParent.mu.children), Equals, 0)
}

func (s *testSuite) TestDetach(c *C) {
	parent := NewTracker(1, -1)
	child := NewTracker(2, -1)
	child.Consume(100)
	child.AttachTo(parent)
	c.Assert(child.BytesConsumed(), Equals, int64(100))
	c.Assert(parent.BytesConsumed(), Equals, int64(100))
	c.Assert(len(parent.mu.children), Equals, 1)
	c.Assert(parent.mu.children[0], DeepEquals, child)

	child.Detach()
	c.Assert(child.BytesConsumed(), Equals, int64(100))
	c.Assert(parent.BytesConsumed(), Equals, int64(0))
	c.Assert(len(parent.mu.children), Equals, 0)
	c.Assert(child.getParent(), IsNil)
}

func (s *testSuite) TestReplaceChild(c *C) {
	oldChild := NewTracker(1, -1)
	oldChild.Consume(100)
	newChild := NewTracker(2, -1)
	newChild.Consume(500)
	parent := NewTracker(3, -1)

	oldChild.AttachTo(parent)
	c.Assert(parent.BytesConsumed(), Equals, int64(100))

	parent.ReplaceChild(oldChild, newChild)
	c.Assert(parent.BytesConsumed(), Equals, int64(500))
	c.Assert(len(parent.mu.children), Equals, 1)
	c.Assert(parent.mu.children[0], DeepEquals, newChild)
	c.Assert(newChild.getParent(), DeepEquals, parent)
	c.Assert(oldChild.getParent(), IsNil)

	parent.ReplaceChild(oldChild, nil)
	c.Assert(parent.BytesConsumed(), Equals, int64(500))
	c.Assert(len(parent.mu.children), Equals, 1)
	c.Assert(parent.mu.children[0], DeepEquals, newChild)
	c.Assert(newChild.getParent(), DeepEquals, parent)
	c.Assert(oldChild.getParent(), IsNil)

	parent.ReplaceChild(newChild, nil)
	c.Assert(parent.BytesConsumed(), Equals, int64(0))
	c.Assert(len(parent.mu.children), Equals, 0)
	c.Assert(newChild.getParent(), IsNil)
	c.Assert(oldChild.getParent(), IsNil)

	node1 := NewTracker(1, -1)
	node2 := NewTracker(2, -1)
	node3 := NewTracker(3, -1)
	node2.AttachTo(node1)
	node3.AttachTo(node2)
	node3.Consume(100)
	c.Assert(node1.BytesConsumed(), Equals, int64(100))
	node2.ReplaceChild(node3, nil)
	c.Assert(node2.BytesConsumed(), Equals, int64(0))
	c.Assert(node1.BytesConsumed(), Equals, int64(0))
}

func (s *testSuite) TestToString(c *C) {
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

	c.Assert(parent.String(), Equals, `
"1"{
  "consumed": 4.00293168798089 GB
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

func (s *testSuite) TestMaxConsumed(c *C) {
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

func (s *testSuite) TestGlobalTracker(c *C) {
	r := NewGlobalTracker(1, -1)
	c1 := NewTracker(2, -1)
	c2 := NewTracker(3, -1)
	c1.Consume(100)
	c2.Consume(200)

	c1.AttachToGlobalTracker(r)
	c2.AttachToGlobalTracker(r)
	c.Assert(r.BytesConsumed(), Equals, int64(300))
	c.Assert(c1.getParent(), DeepEquals, r)
	c.Assert(c2.getParent(), DeepEquals, r)
	c.Assert(len(r.mu.children), Equals, 0)

	c1.DetachFromGlobalTracker()
	c2.DetachFromGlobalTracker()
	c.Assert(r.BytesConsumed(), Equals, int64(0))
	c.Assert(c1.getParent(), IsNil)
	c.Assert(c2.getParent(), IsNil)
	c.Assert(len(r.mu.children), Equals, 0)

	defer func() {
		v := recover()
		c.Assert(v, Equals, "Attach to a non-GlobalTracker")
	}()
	commonTracker := NewTracker(4, -1)
	c1.AttachToGlobalTracker(commonTracker)

	c1.AttachTo(commonTracker)
	c.Assert(commonTracker.BytesConsumed(), Equals, int64(100))
	c.Assert(len(commonTracker.mu.children), Equals, 1)
	c.Assert(c1.getParent(), DeepEquals, commonTracker)

	c1.AttachToGlobalTracker(r)
	c.Assert(commonTracker.BytesConsumed(), Equals, int64(0))
	c.Assert(len(commonTracker.mu.children), Equals, 0)
	c.Assert(r.BytesConsumed(), Equals, int64(100))
	c.Assert(c1.getParent(), DeepEquals, r)
	c.Assert(len(r.mu.children), Equals, 0)

	defer func() {
		v := recover()
		c.Assert(v, Equals, "Detach from a non-GlobalTracker")
	}()
	c2.AttachTo(commonTracker)
	c2.DetachFromGlobalTracker()

}

func BenchmarkConsume(b *testing.B) {
	tracker := NewTracker(1, -1)
	b.RunParallel(func(pb *testing.PB) {
		childTracker := NewTracker(2, -1)
		childTracker.AttachTo(tracker)
		for pb.Next() {
			childTracker.Consume(256 << 20)
		}
	})
}

func (s *testSuite) TestErrorCode(c *C) {
	c.Assert(int(terror.ToSQLError(errMemExceedThreshold).Code), Equals, errno.ErrMemExceedThreshold)
}
