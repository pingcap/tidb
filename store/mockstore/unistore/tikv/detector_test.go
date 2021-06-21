// Copyright 2019 PingCAP, Inc.
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
// Copyright 2019-present PingCAP, Inc.
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

package tikv

import (
	"testing"
	"time"

	. "github.com/pingcap/check"
	deadlockpb "github.com/pingcap/kvproto/pkg/deadlock"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testDeadlockSuite{})

type testDeadlockSuite struct{}

func (s *testDeadlockSuite) TestDeadlock(c *C) {
	makeDiagCtx := func(key string, resourceGroupTag string) diagnosticContext {
		return diagnosticContext{
			key:              []byte(key),
			resourceGroupTag: []byte(resourceGroupTag),
		}
	}
	checkWaitChainEntry := func(entry *deadlockpb.WaitForEntry, txn, waitForTxn uint64, key, resourceGroupTag string) {
		c.Assert(entry.Txn, Equals, txn)
		c.Assert(entry.WaitForTxn, Equals, waitForTxn)
		c.Assert(string(entry.Key), Equals, key)
		c.Assert(string(entry.ResourceGroupTag), Equals, resourceGroupTag)
	}

	ttl := 50 * time.Millisecond
	expireInterval := 100 * time.Millisecond
	urgentSize := uint64(1)
	detector := NewDetector(ttl, urgentSize, expireInterval)
	err := detector.Detect(1, 2, 100, makeDiagCtx("k1", "tag1"))
	c.Assert(err, IsNil)
	c.Assert(detector.totalSize, Equals, uint64(1))
	err = detector.Detect(2, 3, 200, makeDiagCtx("k2", "tag2"))
	c.Assert(err, IsNil)
	c.Assert(detector.totalSize, Equals, uint64(2))
	err = detector.Detect(3, 1, 300, makeDiagCtx("k3", "tag3"))
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "deadlock")
	c.Assert(len(err.WaitChain), Equals, 3)
	// The order of entries in the wait chain is specific: each item is waiting for the next one.
	checkWaitChainEntry(err.WaitChain[0], 1, 2, "k1", "tag1")
	checkWaitChainEntry(err.WaitChain[1], 2, 3, "k2", "tag2")
	checkWaitChainEntry(err.WaitChain[2], 3, 1, "k3", "tag3")

	c.Assert(detector.totalSize, Equals, uint64(2))
	detector.CleanUp(2)
	list2 := detector.waitForMap[2]
	c.Assert(list2, IsNil)
	c.Assert(detector.totalSize, Equals, uint64(1))

	// After cycle is broken, no deadlock now.
	diagCtx := diagnosticContext{}
	err = detector.Detect(3, 1, 300, diagCtx)
	c.Assert(err, IsNil)
	list3 := detector.waitForMap[3]
	c.Assert(list3.txns.Len(), Equals, 1)
	c.Assert(detector.totalSize, Equals, uint64(2))

	// Different keyHash grows the list.
	err = detector.Detect(3, 1, 400, diagCtx)
	c.Assert(err, IsNil)
	c.Assert(list3.txns.Len(), Equals, 2)
	c.Assert(detector.totalSize, Equals, uint64(3))

	// Same waitFor and key hash doesn't grow the list.
	err = detector.Detect(3, 1, 400, diagCtx)
	c.Assert(err, IsNil)
	c.Assert(list3.txns.Len(), Equals, 2)
	c.Assert(detector.totalSize, Equals, uint64(3))

	detector.CleanUpWaitFor(3, 1, 300)
	c.Assert(list3.txns.Len(), Equals, 1)
	c.Assert(detector.totalSize, Equals, uint64(2))
	detector.CleanUpWaitFor(3, 1, 400)
	c.Assert(detector.totalSize, Equals, uint64(1))
	list3 = detector.waitForMap[3]
	c.Assert(list3, IsNil)

	// after 100ms, all entries expired, detect non exist edges
	time.Sleep(100 * time.Millisecond)
	err = detector.Detect(100, 200, 100, diagCtx)
	c.Assert(err, IsNil)
	c.Assert(detector.totalSize, Equals, uint64(1))
	c.Assert(len(detector.waitForMap), Equals, 1)

	// expired entry should not report deadlock, detect will remove this entry
	// not dependent on expire check interval
	time.Sleep(60 * time.Millisecond)
	err = detector.Detect(200, 100, 200, diagCtx)
	c.Assert(err, IsNil)
	c.Assert(detector.totalSize, Equals, uint64(1))
	c.Assert(len(detector.waitForMap), Equals, 1)
}
