// Copyright 2026 PingCAP, Inc.
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

package mvs

import (
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func installMockTimeForTest(t *testing.T) *MockTimeModule {
	t.Helper()
	module := NewMockTimeModule(mvsUnix(1700000000, 0))
	restore := InstallMockTimeModuleForTest(module)
	t.Cleanup(restore)
	return module
}

func mustHash(mapping map[string]uint32) func([]byte) uint32 {
	return func(data []byte) uint32 {
		if v, ok := mapping[string(data)]; ok {
			return v
		}
		panic("missing hash mapping for " + string(data))
	}
}

func TestConsistentHash_GetNodeWraps(t *testing.T) {
	mapping := map[string]uint32{
		"nodeA#0":  10,
		"nodeB#0":  20,
		"key-low":  5,
		"key-mid":  15,
		"key-high": 25,
	}

	c := NewConsistentHash(1)
	c.hashFunc = mustHash(mapping)

	c.AddNode("nodeA")
	c.AddNode("nodeB")

	if got := c.GetNode([]byte("key-low")); got != "nodeA" {
		t.Fatalf("expected nodeA for key-low, got %q", got)
	}
	if got := c.GetNode([]byte("key-mid")); got != "nodeB" {
		t.Fatalf("expected nodeB for key-mid, got %q", got)
	}
	if got := c.GetNode([]byte("key-high")); got != "nodeA" {
		t.Fatalf("expected nodeA for key-high (wrap), got %q", got)
	}

	if !sort.SliceIsSorted(c.ring, func(i, j int) bool { return c.ring[i].hash < c.ring[j].hash }) {
		t.Fatalf("expected ring to be sorted by hash")
	}
}

func TestConsistentHash_RemoveNodeUpdatesState(t *testing.T) {
	mapping := map[string]uint32{
		"nodeA#0": 10,
		"nodeA#1": 30,
		"nodeB#0": 20,
		"nodeB#1": 40,
	}

	c := NewConsistentHash(2)
	c.hashFunc = mustHash(mapping)

	c.AddNode("nodeA")
	c.AddNode("nodeB")

	if got := c.NodeCount(); got != 2 {
		t.Fatalf("expected 2 nodes, got %d", got)
	}
	if got := c.getVirtualNodes("nodeA"); got != 2 {
		t.Fatalf("expected 2 virtual nodes for nodeA, got %d", got)
	}
	if got := c.getVirtualNodes("nodeB"); got != 2 {
		t.Fatalf("expected 2 virtual nodes for nodeB, got %d", got)
	}

	c.RemoveNode("nodeB")

	if got := c.NodeCount(); got != 1 {
		t.Fatalf("expected 1 node after removal, got %d", got)
	}
	if got := c.getVirtualNodes("nodeB"); got != 0 {
		t.Fatalf("expected 0 virtual nodes for nodeB, got %d", got)
	}
	if len(c.ring) != 2 {
		t.Fatalf("expected ring length 2 after removal, got %d", len(c.ring))
	}

	nodes := c.GetNodes()
	sort.Strings(nodes)
	if !reflect.DeepEqual(nodes, []string{"nodeA"}) {
		t.Fatalf("expected nodes [nodeA], got %v", nodes)
	}

	if !sort.SliceIsSorted(c.ring, func(i, j int) bool { return c.ring[i].hash < c.ring[j].hash }) {
		t.Fatalf("expected ring to remain sorted after removal")
	}
}

func TestConsistentHash_EmptyState(t *testing.T) {
	c := NewConsistentHash(1)

	if got := c.GetNode([]byte("any")); got != "" {
		t.Fatalf("expected empty node for empty ring, got %q", got)
	}
	if got := c.NodeCount(); got != 0 {
		t.Fatalf("expected node count 0, got %d", got)
	}
	if nodes := c.GetNodes(); len(nodes) != 0 {
		t.Fatalf("expected no nodes, got %v", nodes)
	}
}

func TestConsistentHash_RebuildResetsRing(t *testing.T) {
	mapping := map[string]uint32{
		"nodeA#0": 10,
		"nodeA#1": 30,
		"nodeB#0": 20,
		"nodeB#1": 40,
		"nodeC#0": 15,
		"nodeC#1": 35,
		"key":     25,
	}

	c := NewConsistentHash(2)
	c.hashFunc = mustHash(mapping)

	c.AddNode("nodeA")
	c.AddNode("nodeB")

	if got := c.NodeCount(); got != 2 {
		t.Fatalf("expected 2 nodes, got %d", got)
	}
	if got := c.GetNode([]byte("key")); got == "" {
		t.Fatalf("expected non-empty node before rebuild")
	}

	c.Rebuild(map[string]serverInfo{
		"nodeC": {ID: "nodeC"},
	})

	if got := c.NodeCount(); got != 1 {
		t.Fatalf("expected 1 node after rebuild, got %d", got)
	}
	if got := len(c.ring); got != 2 {
		t.Fatalf("expected ring size 2 after rebuild, got %d", got)
	}
	if got := c.GetNode([]byte("key")); got != "nodeC" {
		t.Fatalf("expected key mapped to nodeC after rebuild, got %q", got)
	}
	if !sort.SliceIsSorted(c.ring, func(i, j int) bool { return c.ring[i].hash < c.ring[j].hash }) {
		t.Fatalf("expected ring to be sorted after rebuild")
	}
}

func TestConsistentHash_Rebuild(t *testing.T) {
	mapping := map[string]uint32{
		"nodeA#0": 10,
		"nodeA#1": 30,
		"nodeB#0": 20,
		"nodeB#1": 40,
		"key":     35,
	}

	c := NewConsistentHash(2)
	c.hashFunc = mustHash(mapping)

	c.Rebuild(map[string]serverInfo{
		"nodeA": {ID: "nodeA"},
		"nodeB": {ID: "nodeB"},
	})

	if got := c.NodeCount(); got != 2 {
		t.Fatalf("expected 2 nodes, got %d", got)
	}
	if got := len(c.ring); got != 4 {
		t.Fatalf("expected ring size 4, got %d", got)
	}
	if got := c.GetNode([]byte("key")); got == "" {
		t.Fatalf("expected non-empty node for key")
	}
	if !sort.SliceIsSorted(c.ring, func(i, j int) bool { return c.ring[i].hash < c.ring[j].hash }) {
		t.Fatalf("expected ring to be sorted after rebuild from map")
	}
}

type node struct {
	v  string
	ts int64
}

func (t node) Less(other node) bool {
	return t.ts < other.ts
}

func TestPriorityQueue(t *testing.T) {
	base := time.Time{}.Add(time.Hour * 8192).UnixMilli()
	t.Run("basic ordering and update", func(t *testing.T) {
		pq := &PriorityQueue[node]{}
		pq.Push(node{v: "b", ts: base + int64(2*time.Second/time.Millisecond)})
		pq.Push(node{v: "a", ts: base + int64(time.Second/time.Millisecond)})
		pq.Push(node{v: "c", ts: base + int64(3*time.Second/time.Millisecond)})
		require.True(t, pq.Front().Value.v == "a")

		pq.Update(pq.Front(), node{v: "d", ts: base + int64(5*time.Second/time.Millisecond)})
		pq.Push(node{v: "c", ts: base + int64(500*time.Millisecond/time.Millisecond)})

		popRes := make([]string, 0, 4)
		for pq.Len() > 0 {
			it := pq.Pop()
			popRes = append(popRes, it.Value.v)
		}
		require.Equal(t, []string{"c", "b", "c", "d"}, popRes)
	})

	t.Run("empty and invalid operations", func(t *testing.T) {
		pq := &PriorityQueue[node]{}
		require.Nil(t, pq.Front())
		require.Nil(t, pq.Pop())

		var zero node
		require.Equal(t, zero, pq.Remove(nil))
		pq.Update(nil, node{v: "ignored", ts: base})

		ghost := newItem(node{v: "ghost", ts: base + int64(time.Second/time.Millisecond)})
		pq.Update(ghost, node{v: "still-ghost", ts: base + int64(2*time.Second/time.Millisecond)})
		require.Equal(t, zero, pq.Remove(ghost))
	})

	t.Run("remove and update after removal", func(t *testing.T) {
		pq := &PriorityQueue[node]{}
		itemA := pq.Push(node{v: "a", ts: base + int64(2*time.Second/time.Millisecond)})
		itemB := pq.Push(node{v: "b", ts: base + int64(3*time.Second/time.Millisecond)})
		itemC := pq.Push(node{v: "c", ts: base + int64(time.Second/time.Millisecond)})

		removed := pq.Remove(itemB)
		require.Equal(t, "b", removed.v)

		pq.Update(itemB, node{v: "b2", ts: base + int64(500*time.Millisecond/time.Millisecond)})

		pq.Update(itemA, node{v: "a2", ts: base + int64(4*time.Second/time.Millisecond)})
		pq.Push(node{v: "d", ts: base + int64(1500*time.Millisecond/time.Millisecond)})

		popRes := make([]string, 0, 4)
		for pq.Len() > 0 {
			it := pq.Pop()
			popRes = append(popRes, it.Value.v)
		}
		require.Equal(t, []string{"c", "d", "a2"}, popRes)
		require.NotContains(t, popRes, "b")
		require.NotNil(t, itemC)
	})
}
