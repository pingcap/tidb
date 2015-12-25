package kv

import (
	"sync"

	"github.com/ngaut/log"
)

// A cache holds a set of reusable objects.
// The slice is a stack (LIFO).
// If more are needed, the cache creates them by calling new.
type cache struct {
	mu    sync.Mutex
	name  string
	saved []MemBuffer
	new   func() MemBuffer
}

func (c *cache) put(x MemBuffer) {
	c.mu.Lock()
	if len(c.saved) < cap(c.saved) {
		c.saved = append(c.saved, x)
	} else {
		log.Warning(c.name, "is full, you may need to increase pool size")
	}
	c.mu.Unlock()
}

func (c *cache) get() MemBuffer {
	c.mu.Lock()
	n := len(c.saved)
	if n == 0 {
		c.mu.Unlock()
		return c.new()
	}
	x := c.saved[n-1]
	c.saved = c.saved[0 : n-1]
	c.mu.Unlock()
	return x
}

func newCache(name string, cap int, f func() MemBuffer) *cache {
	return &cache{name: name, saved: make([]MemBuffer, 0, cap), new: f}
}
