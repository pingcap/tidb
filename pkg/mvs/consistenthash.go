package utils

import (
	"hash/crc32"
	"sort"
	"strconv"
)

// ConsistentHash is a consistent hashing ring.
type ConsistentHash struct {
	ring     []virtualNode
	hashFunc func(data []byte) uint32
	replicas int
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

// Rebuild rebuilds the consistent hash with the given nodes.
func (c *ConsistentHash) Rebuild(nodes []string) {
	c.ring = make([]virtualNode, 0, len(nodes)*c.replicas)
	for _, node := range nodes {
		c.doInsert(node)
	}
	c.doResort()
}

// RebuildFromMap rebuilds the consistent hash from a template map keyed by node.
func RebuildFromMap[T any](c *ConsistentHash, nodeMap map[string]T) {
	c.ring = make([]virtualNode, 0, len(nodeMap)*c.replicas)
	for node := range nodeMap {
		c.doInsert(node)
	}
	c.doResort()
}

func (c *ConsistentHash) doResort() {
	// Keep the ring sorted for binary search.
	sort.Slice(c.ring, func(i, j int) bool {
		return c.ring[i].hash < c.ring[j].hash
	})
}

func (c *ConsistentHash) doInsert(node string) {
	for i := 0; i < c.replicas; i++ {
		// Create a virtual node identifier, e.g. "node1#0", "node1#1".
		virtualKey := node + "#" + strconv.Itoa(i)
		hash := c.hashFunc([]byte(virtualKey))

		c.ring = append(c.ring, virtualNode{hash: hash, node: node})
	}
}

// AddNode adds a real node (and creates virtual nodes).
func (c *ConsistentHash) AddNode(node string) {
	c.doInsert(node)
	c.doResort()
}

// RemoveNode removes a real node (and all its virtual nodes).
func (c *ConsistentHash) RemoveNode(node string) {
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
	nodeSet := make(map[string]struct{})
	for _, vnode := range c.ring {
		nodeSet[vnode.node] = struct{}{}
	}

	return len(nodeSet)
}

func (c *ConsistentHash) getVirtualNodes(node string) int {
	count := 0
	for _, vnode := range c.ring {
		if vnode.node == node {
			count++
		}
	}

	return count
}
