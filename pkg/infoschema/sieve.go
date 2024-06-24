// Copyright 2024 PingCAP, Inc.
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

package infoschema

import (
	"container/list"
	"context"
	"sync"

	"github.com/pingcap/tidb/pkg/infoschema/internal"
)

// entry holds the key and value of a cache entry.
type entry[K comparable, V any] struct {
	key     K
	value   V
	visited bool
	element *list.Element
	size    uint64
}

func (t *entry[K, V]) Size() uint64 {
	if t.size == 0 {
		size := internal.Sizeof(t)
		if size > 0 {
			t.size = uint64(size)
		}
	}
	return t.size
}

// Sieve is an efficient turn-Key eviction algorithm for web caches.
// See blog post https://cachemon.github.io/SIEVE-website/blog/2023/12/17/sieve-is-simpler-than-lru/
// and also the academic paper "SIEVE is simpler than LRU"
type Sieve[K comparable, V any] struct {
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	size     uint64
	capacity uint64
	items    map[K]*entry[K, V]
	ll       *list.List
	hand     *list.Element
}

func newSieve[K comparable, V any](capacity uint64) *Sieve[K, V] {
	ctx, cancel := context.WithCancel(context.Background())

	cache := &Sieve[K, V]{
		ctx:      ctx,
		cancel:   cancel,
		capacity: capacity,
		items:    make(map[K]*entry[K, V]),
		ll:       list.New(),
	}

	return cache
}

func (s *Sieve[K, V]) SetCapacity(capacity uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.capacity = capacity
}

func (s *Sieve[K, V]) Capacity() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.capacity
}

func (s *Sieve[K, V]) Set(key K, value V) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.items[key]; ok {
		e.value = value
		e.visited = true
		return
	}

	for i := 0; s.size > s.capacity && i < 10; i++ {
		s.evict()
	}

	e := &entry[K, V]{
		key:   key,
		value: value,
	}
	s.size += e.Size() // calculate the size first without putting to the list.
	e.element = s.ll.PushFront(key)

	s.items[key] = e
}

func (s *Sieve[K, V]) Get(key K) (value V, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.items[key]; ok {
		e.visited = true
		return e.value, true
	}

	return
}

func (s *Sieve[K, V]) Remove(key K) (ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.items[key]; ok {
		// if the element to be removed is the hand,
		// then move the hand to the previous one.
		if e.element == s.hand {
			s.hand = s.hand.Prev()
		}

		s.removeEntry(e)
		return true
	}

	return false
}

func (s *Sieve[K, V]) Contains(key K) (ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok = s.items[key]
	return
}

func (s *Sieve[K, V]) Peek(key K) (value V, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.items[key]; ok {
		return e.value, true
	}

	return
}

func (s *Sieve[K, V]) Size() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.size
}

func (s *Sieve[K, V]) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.ll.Len()
}

func (s *Sieve[K, V]) Purge() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, e := range s.items {
		s.removeEntry(e)
	}

	s.ll.Init()
}

func (s *Sieve[K, V]) Close() {
	s.Purge()
	s.mu.Lock()
	s.cancel()
	s.mu.Unlock()
}

func (s *Sieve[K, V]) removeEntry(e *entry[K, V]) {
	s.ll.Remove(e.element)
	delete(s.items, e.key)
	s.size -= e.Size()
}

func (s *Sieve[K, V]) evict() {
	o := s.hand
	// if o is nil, then assign it to the tail element in the list
	if o == nil {
		o = s.ll.Back()
	}

	el, ok := s.items[o.Value.(K)]
	if !ok {
		panic("sieve: evicting non-existent element")
	}

	for el.visited {
		el.visited = false
		o = o.Prev()
		if o == nil {
			o = s.ll.Back()
		}

		el, ok = s.items[o.Value.(K)]
		if !ok {
			panic("sieve: evicting non-existent element")
		}
	}

	s.hand = o.Prev()
	s.removeEntry(el)
}
