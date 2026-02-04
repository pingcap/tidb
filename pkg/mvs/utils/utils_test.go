package utils

import (
	"container/heap"
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

type node struct {
	v  string
	ts time.Time
}

func (t node) Less(other node) bool {
	return t.ts.Before(other.ts)
}

func TestPriorityQueue(t *testing.T) {
	base := time.Now()
	pq := &PriorityQueue[node]{}
	heap.Init(pq)

	heap.Push(pq, &Item[node]{Value: node{v: "b", ts: base.Add(2 * time.Second)}})
	heap.Push(pq, &Item[node]{Value: node{v: "a", ts: base.Add(time.Second)}})
	item := &Item[node]{Value: node{v: "c", ts: base.Add(3 * time.Second)}}
	heap.Push(pq, item)
	pq.Update(item, node{v: "c", ts: base.Add(500 * time.Millisecond)})
	popRes := make([]string, 0, 3)
	for pq.Len() > 0 {
		it := heap.Pop(pq).(*Item[node])
		popRes = append(popRes, it.Value.v)
	}
	require.Equal(t, []string{"c", "a", "b"}, popRes)
}
