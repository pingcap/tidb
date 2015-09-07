package pool

import (
	"sync"

	"github.com/ngaut/log"
)

// A cache holds a set of reusable objects.
// The slice is a stack (LIFO).
// If more are needed, the cache creates them by calling new.
type Cache struct {
	mu    sync.Mutex
	name  string
	saved []interface{}
	new   func() interface{}
}

func (c *Cache) Put(x interface{}) {
	c.mu.Lock()
	if len(c.saved) < cap(c.saved) {
		c.saved = append(c.saved, x)
	} else {
		log.Warning(c.name, "is full, you may need to increase pool size")
	}
	c.mu.Unlock()
}

func (c *Cache) Get() interface{} {
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

func NewCache(name string, cap int, f func() interface{}) *Cache {
	return &Cache{name: name, saved: make([]interface{}, 0, cap), new: f}
}
