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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"testing"
	"time"

	deadlockpb "github.com/pingcap/kvproto/pkg/deadlock"
	"github.com/stretchr/testify/require"
)

func TestDeadlock(t *testing.T) {
	makeDiagCtx := func(key string, resourceGroupTag string) diagnosticContext {
		return diagnosticContext{
			key:              []byte(key),
			resourceGroupTag: []byte(resourceGroupTag),
		}
	}
	checkWaitChainEntry := func(entry *deadlockpb.WaitForEntry, txn, waitForTxn uint64, key, resourceGroupTag string) {
		require.Equal(t, txn, entry.Txn)
		require.Equal(t, waitForTxn, entry.WaitForTxn)
		require.Equal(t, key, string(entry.Key))
		require.Equal(t, resourceGroupTag, string(entry.ResourceGroupTag))
	}

	ttl := 50 * time.Millisecond
	expireInterval := 100 * time.Millisecond
	urgentSize := uint64(1)
	detector := NewDetector(ttl, urgentSize, expireInterval)
	err := detector.Detect(1, 2, 100, makeDiagCtx("k1", "tag1"))
	require.Nil(t, err)
	require.Equal(t, uint64(1), detector.totalSize)
	err = detector.Detect(2, 3, 200, makeDiagCtx("k2", "tag2"))
	require.Nil(t, err)
	require.Equal(t, uint64(2), detector.totalSize)
	err = detector.Detect(3, 1, 300, makeDiagCtx("k3", "tag3"))
	require.NotNil(t, err)
	require.Equal(t, "deadlock", err.Error())
	require.Equal(t, 3, len(err.WaitChain))
	// The order of entries in the wait chain is specific: each item is waiting for the next one.
	checkWaitChainEntry(err.WaitChain[0], 1, 2, "k1", "tag1")
	checkWaitChainEntry(err.WaitChain[1], 2, 3, "k2", "tag2")
	checkWaitChainEntry(err.WaitChain[2], 3, 1, "k3", "tag3")

	require.Equal(t, uint64(2), detector.totalSize)
	detector.CleanUp(2)
	list2 := detector.waitForMap[2]
	require.Nil(t, list2)
	require.Equal(t, uint64(1), detector.totalSize)

	// After cycle is broken, no deadlock now.
	diagCtx := diagnosticContext{}
	err = detector.Detect(3, 1, 300, diagCtx)
	require.Nil(t, err)
	list3 := detector.waitForMap[3]
	require.Equal(t, 1, list3.txns.Len())
	require.Equal(t, uint64(2), detector.totalSize)

	// Different keyHash grows the list.
	err = detector.Detect(3, 1, 400, diagCtx)
	require.Nil(t, err)
	require.Equal(t, 2, list3.txns.Len())
	require.Equal(t, uint64(3), detector.totalSize)

	// Same waitFor and key hash doesn't grow the list.
	err = detector.Detect(3, 1, 400, diagCtx)
	require.Nil(t, err)
	require.Equal(t, 2, list3.txns.Len())
	require.Equal(t, uint64(3), detector.totalSize)

	detector.CleanUpWaitFor(3, 1, 300)
	require.Equal(t, 1, list3.txns.Len())
	require.Equal(t, uint64(2), detector.totalSize)
	detector.CleanUpWaitFor(3, 1, 400)
	require.Equal(t, uint64(1), detector.totalSize)
	list3 = detector.waitForMap[3]
	require.Nil(t, list3)

	// after 100ms, all entries expired, detect non exist edges
	time.Sleep(100 * time.Millisecond)
	err = detector.Detect(100, 200, 100, diagCtx)
	require.Nil(t, err)
	require.Equal(t, uint64(1), detector.totalSize)
	require.Equal(t, 1, len(detector.waitForMap))

	// expired entry should not report deadlock, detect will remove this entry
	// not dependent on expire check interval
	time.Sleep(60 * time.Millisecond)
	err = detector.Detect(200, 100, 200, diagCtx)
	require.Nil(t, err)
	require.Equal(t, uint64(1), detector.totalSize)
	require.Equal(t, 1, len(detector.waitForMap))
}
