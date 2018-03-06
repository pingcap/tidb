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
	"os"
	"sync"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/testleak"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	logLevel := os.Getenv("log_level")
	logutil.InitLogger(&logutil.LogConfig{
		Level: logLevel,
	})
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct{}

func (s *testSuite) SetUpSuite(c *C)    {}
func (s *testSuite) TearDownSuite(c *C) {}
func (s *testSuite) SetUpTest(c *C)     { testleak.BeforeTest() }
func (s *testSuite) TearDownTest(c *C)  { testleak.AfterTest(c)() }

func (s *testSuite) TestSetLabel(c *C) {
	tracker := NewTracker("old label", -1)
	c.Assert(tracker.label, Equals, "old label")
	c.Assert(tracker.bytesConsumed, Equals, int64(0))
	c.Assert(tracker.bytesLimit, Equals, int64(-1))
	c.Assert(tracker.parent, IsNil)
	c.Assert(len(tracker.children), Equals, 0)
	tracker.SetLabel("new label")
	c.Assert(tracker.label, Equals, "new label")
	c.Assert(tracker.bytesConsumed, Equals, int64(0))
	c.Assert(tracker.bytesLimit, Equals, int64(-1))
	c.Assert(tracker.parent, IsNil)
	c.Assert(len(tracker.children), Equals, 0)
}

func (s *testSuite) TestConsume(c *C) {
	tracker := NewTracker("tracker", -1)
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
	tracker := NewTracker("oom tracker", 100)
	action := &mockAction{}
	tracker.SetActionOnExceed(action)

	c.Assert(action.called, IsFalse)
	tracker.Consume(10000)
	c.Assert(action.called, IsTrue)
}

type mockAction struct {
	called bool
}

func (a *mockAction) Action(t *Tracker) {
	a.called = true
}

func (s *testSuite) TestAttachTo(c *C) {
	oldParent := NewTracker("old parent", -1)
	newParent := NewTracker("new parent", -1)
	child := NewTracker("child", -1)
	child.Consume(100)
	child.AttachTo(oldParent)
	c.Assert(child.BytesConsumed(), Equals, int64(100))
	c.Assert(oldParent.BytesConsumed(), Equals, int64(100))
	c.Assert(child.parent, DeepEquals, oldParent)
	c.Assert(len(oldParent.children), Equals, 1)
	c.Assert(oldParent.children[0], DeepEquals, child)

	child.AttachTo(newParent)
	c.Assert(child.BytesConsumed(), Equals, int64(100))
	c.Assert(oldParent.BytesConsumed(), Equals, int64(0))
	c.Assert(newParent.BytesConsumed(), Equals, int64(100))
	c.Assert(child.parent, DeepEquals, newParent)
	c.Assert(len(newParent.children), Equals, 1)
	c.Assert(newParent.children[0], DeepEquals, child)
	c.Assert(len(oldParent.children), Equals, 1)
	c.Assert(oldParent.children[0], IsNil)
}

func (s *testSuite) TestReplaceChild(c *C) {
	oldChild := NewTracker("old child", -1)
	oldChild.Consume(100)
	newChild := NewTracker("new child", -1)
	newChild.Consume(500)
	parent := NewTracker("parent", -1)

	oldChild.AttachTo(parent)
	c.Assert(parent.BytesConsumed(), Equals, int64(100))

	parent.ReplaceChild(oldChild, newChild)
	c.Assert(parent.BytesConsumed(), Equals, int64(500))
	c.Assert(len(parent.children), Equals, 1)
	c.Assert(parent.children[0], DeepEquals, newChild)
	c.Assert(newChild.parent, DeepEquals, parent)
	c.Assert(oldChild.parent, IsNil)

	parent.ReplaceChild(oldChild, nil)
	c.Assert(parent.BytesConsumed(), Equals, int64(500))
	c.Assert(len(parent.children), Equals, 1)
	c.Assert(parent.children[0], DeepEquals, newChild)
	c.Assert(newChild.parent, DeepEquals, parent)
	c.Assert(oldChild.parent, IsNil)

	parent.ReplaceChild(newChild, nil)
	c.Assert(parent.BytesConsumed(), Equals, int64(0))
	c.Assert(len(parent.children), Equals, 1)
	c.Assert(parent.children[0], IsNil)
	c.Assert(newChild.parent, IsNil)
	c.Assert(oldChild.parent, IsNil)
}

func (s *testSuite) TestToString(c *C) {
	parent := NewTracker("parent", -1)

	child1 := NewTracker("child 1", 1000)
	child2 := NewTracker("child 2", -1)
	child3 := NewTracker("child 3", -1)
	child4 := NewTracker("child 4", -1)

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

func BenchmarkConsume(b *testing.B) {
	tracker := NewTracker("root", -1)
	b.RunParallel(func(pb *testing.PB) {
		childTracker := NewTracker("child", -1)
		childTracker.AttachTo(tracker)
		for pb.Next() {
			childTracker.Consume(256 << 20)
		}
	})
}
