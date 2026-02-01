package utils

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

// ConsistentHash is a consistent hashing ring.
type ConsistentHash struct {
	ring     []virtualNode
	hashFunc func(data []byte) uint32
	replicas int
	mu       sync.RWMutex
}

type virtualNode struct {
	hash uint32
	node string
}

// NewConsistentHash creates a consistent hash instance.
// replicas: number of virtual nodes per real node, suggested 150-300.
func NewConsistentHash(replicas int) *ConsistentHash {
	return &ConsistentHash{
		hashFunc: crc32.ChecksumIEEE,
		replicas: replicas,
	}
}

// AddNode adds a real node (and creates virtual nodes).
func (c *ConsistentHash) AddNode(node string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i := 0; i < c.replicas; i++ {
		// Create a virtual node identifier, e.g. "node1#0", "node1#1".
		virtualKey := node + "#" + strconv.Itoa(i)
		hash := c.hashFunc([]byte(virtualKey))

		c.ring = append(c.ring, virtualNode{hash: hash, node: node})
	}

	// Keep the ring sorted for binary search.
	sort.Slice(c.ring, func(i, j int) bool {
		return c.ring[i].hash < c.ring[j].hash
	})
}

// RemoveNode removes a real node (and all its virtual nodes).
func (c *ConsistentHash) RemoveNode(node string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	newRing := make([]virtualNode, 0, len(c.ring))
	for _, vnode := range c.ring {
		if vnode.node != node {
			newRing = append(newRing, vnode)
		}
	}
	c.ring = newRing
}

// GetNode returns the real node for the given key.
func (c *ConsistentHash) GetNode(key string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.ring) == 0 {
		return ""
	}

	hash := c.hashFunc([]byte(key))

	// Binary search for the first node clockwise.
	idx := sort.Search(len(c.ring), func(i int) bool {
		return c.ring[i].hash >= hash
	})

	// Wrap to the start of the ring if out of range.
	if idx == len(c.ring) {
		idx = 0
	}

	return c.ring[idx].node
}

// GetNodes returns all real nodes.
func (c *ConsistentHash) GetNodes() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	nodeSet := make(map[string]struct{})
	for _, vnode := range c.ring {
		nodeSet[vnode.node] = struct{}{}
	}

	nodes := make([]string, 0, len(nodeSet))
	for node := range nodeSet {
		nodes = append(nodes, node)
	}

	return nodes
}

// NodeCount returns the number of real nodes.
func (c *ConsistentHash) NodeCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	nodeSet := make(map[string]struct{})
	for _, vnode := range c.ring {
		nodeSet[vnode.node] = struct{}{}
	}

	return len(nodeSet)
}

// GetVirtualNodes returns the number of virtual nodes for a given node.
func (c *ConsistentHash) GetVirtualNodes(node string) int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	count := 0
	for _, vnode := range c.ring {
		if vnode.node == node {
			count++
		}
	}

	return count
}
