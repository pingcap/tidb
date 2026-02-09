package mvs

import (
	"hash/crc32"
	"math"
	"sort"
	"strconv"
)

// ConsistentHash is a consistent hashing ring.
// not thread-safe, caller should ensure external synchronization if needed.
type ConsistentHash struct {
	ring     []virtualNode
	data     map[string][]virtualNode
	hashFunc func(data []byte) uint32
	replicas int
}

type virtualNode *virtualNodeImpl

type virtualNodeImpl struct {
	hash uint32
	node string
}

// NewConsistentHash creates a consistent hash instance.
// replicas: number of virtual nodes per real node, suggested 150-300.
func NewConsistentHash(replicas int) *ConsistentHash {
	replicas = min(max(1, replicas), 200)
	return &ConsistentHash{
		data:     make(map[string][]virtualNode),
		hashFunc: crc32.ChecksumIEEE,
		replicas: replicas,
	}
}

// Rebuild rebuilds the consistent hash with the given nodes.
func (c *ConsistentHash) Rebuild(nodes []string) {
	c.ring = make([]virtualNode, 0, len(nodes)*c.replicas)
	c.data = make(map[string][]virtualNode, len(nodes))
	for _, node := range nodes {
		c.doInsert(node)
	}
	c.doResort()
}

// RebuildFromMap rebuilds the consistent hash from a template map keyed by node.
func RebuildFromMap[T any](c *ConsistentHash, nodeMap map[string]T) {
	c.ring = make([]virtualNode, 0, len(nodeMap)*c.replicas)
	c.data = make(map[string][]virtualNode, len(nodeMap))
	for node := range nodeMap {
		c.doInsert(node)
	}
	c.doResort()
}

func (c *ConsistentHash) doResort() {
	// Keep the ring sorted for binary search.
	sort.Slice(c.ring, func(i, j int) bool {
		if c.ring[i].hash == c.ring[j].hash {
			return c.ring[i].node < c.ring[j].node
		}
		return c.ring[i].hash < c.ring[j].hash
	})
}

func (c *ConsistentHash) doInsert(node string) bool {
	if node == "" {
		return false
	}
	if _, ok := c.data[node]; ok {
		return false
	}
	nodes := make([]virtualNode, 0, c.replicas)
	for i := 0; i < c.replicas; i++ {
		// Create a virtual node identifier, e.g. "node1#0", "node1#1".
		virtualKey := node + "#" + strconv.Itoa(i)
		hash := c.hashFunc([]byte(virtualKey))
		hash = min(hash, math.MaxUint32-1) // reserve MaxUint32 for deleted nodes
		n := virtualNode(&virtualNodeImpl{hash: hash, node: node})
		c.ring = append(c.ring, n)
		nodes = append(nodes, n)
	}
	c.data[node] = nodes
	return true
}

// AddNode adds a real node (and creates virtual nodes).
func (c *ConsistentHash) AddNode(node string) bool {
	if c.doInsert(node) {
		c.doResort()
		return true
	}
	return false
}

// RemoveNode removes a real node (and all its virtual nodes).
func (c *ConsistentHash) RemoveNode(node string) {
	vnodes, ok := c.data[node]
	if !ok {
		return
	}
	delete(c.data, node)
	for _, vnode := range vnodes {
		vnode.hash = math.MaxUint32 // mark as deleted
	}
	c.doResort()
	// Trim the deleted nodes at the end of the ring.
	idx := sort.Search(len(c.ring), func(i int) bool {
		return c.ring[i].hash == math.MaxUint32
	})
	c.ring = c.ring[:idx]
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
	nodes := make([]string, 0, len(c.data))
	for node := range c.data {
		nodes = append(nodes, node)
	}

	// sort for stable order in tests
	sort.Strings(nodes)
	return nodes
}

// NodeCount returns the number of real nodes.
func (c *ConsistentHash) NodeCount() int {
	return len(c.data)
}

func (c *ConsistentHash) getVirtualNodes(node string) int {
	return len(c.data[node])
}
