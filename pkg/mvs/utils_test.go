package mvs

import (
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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

	if got := c.GetNode("key-low"); got != "nodeA" {
		t.Fatalf("expected nodeA for key-low, got %q", got)
	}
	if got := c.GetNode("key-mid"); got != "nodeB" {
		t.Fatalf("expected nodeB for key-mid, got %q", got)
	}
	if got := c.GetNode("key-high"); got != "nodeA" {
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

	if got := c.GetNode("any"); got != "" {
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
	if got := c.GetNode("key"); got == "" {
		t.Fatalf("expected non-empty node before rebuild")
	}

	c.Rebuild([]string{"nodeC"})

	if got := c.NodeCount(); got != 1 {
		t.Fatalf("expected 1 node after rebuild, got %d", got)
	}
	if got := len(c.ring); got != 2 {
		t.Fatalf("expected ring size 2 after rebuild, got %d", got)
	}
	if got := c.GetNode("key"); got != "nodeC" {
		t.Fatalf("expected key mapped to nodeC after rebuild, got %q", got)
	}
	if !sort.SliceIsSorted(c.ring, func(i, j int) bool { return c.ring[i].hash < c.ring[j].hash }) {
		t.Fatalf("expected ring to be sorted after rebuild")
	}
}

func TestConsistentHash_RebuildFromMap(t *testing.T) {
	mapping := map[string]uint32{
		"nodeA#0": 10,
		"nodeA#1": 30,
		"nodeB#0": 20,
		"nodeB#1": 40,
		"key":     35,
	}

	c := NewConsistentHash(2)
	c.hashFunc = mustHash(mapping)

	nodes := map[string]int{
		"nodeA": 1,
		"nodeB": 2,
	}
	RebuildFromMap(c, nodes)

	if got := c.NodeCount(); got != 2 {
		t.Fatalf("expected 2 nodes, got %d", got)
	}
	if got := len(c.ring); got != 4 {
		t.Fatalf("expected ring size 4, got %d", got)
	}
	if got := c.GetNode("key"); got == "" {
		t.Fatalf("expected non-empty node for key")
	}
	if !sort.SliceIsSorted(c.ring, func(i, j int) bool { return c.ring[i].hash < c.ring[j].hash }) {
		t.Fatalf("expected ring to be sorted after rebuild from map")
	}
}

type node struct {
	v  string
	ts time.Time
}

func (t node) Less(other node) bool {
	return t.ts.Before(other.ts)
}

func TestPriorityQueue(t *testing.T) {
	base := time.Now()
	t.Run("basic ordering and update", func(t *testing.T) {
		pq := &PriorityQueue[node]{}
		pq.Push(node{v: "b", ts: base.Add(2 * time.Second)})
		pq.Push(node{v: "a", ts: base.Add(time.Second)})
		pq.Push(node{v: "c", ts: base.Add(3 * time.Second)})
		require.True(t, pq.Front().Value.v == "a")

		pq.Update(pq.Front(), node{v: "d", ts: base.Add(5 * time.Second)})
		pq.Push(node{v: "c", ts: base.Add(500 * time.Millisecond)})

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

		ghost := NewItem(node{v: "ghost", ts: base.Add(time.Second)})
		pq.Update(ghost, node{v: "still-ghost", ts: base.Add(2 * time.Second)})
		require.Equal(t, zero, pq.Remove(ghost))
	})

	t.Run("remove and update after removal", func(t *testing.T) {
		pq := &PriorityQueue[node]{}
		itemA := pq.Push(node{v: "a", ts: base.Add(2 * time.Second)})
		itemB := pq.Push(node{v: "b", ts: base.Add(3 * time.Second)})
		itemC := pq.Push(node{v: "c", ts: base.Add(time.Second)})

		removed := pq.Remove(itemB)
		require.Equal(t, "b", removed.v)

		pq.Update(itemB, node{v: "b2", ts: base.Add(500 * time.Millisecond)})

		pq.Update(itemA, node{v: "a2", ts: base.Add(4 * time.Second)})
		pq.Push(node{v: "d", ts: base.Add(1500 * time.Millisecond)})

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
