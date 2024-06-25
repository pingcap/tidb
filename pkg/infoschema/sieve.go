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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/infoschema/internal"
)

// entry holds the key and value of a cache entry.
type entry[K comparable, V any] struct {
	key     K
	value   V
	visited bool
	element *list.Element
	sz      uint64
}

func (t *entry[K, V]) size() uint64 {
	if t.sz == 0 {
		size := internal.Sizeof(t)
		if size > 0 {
			t.sz = uint64(size)
		}
	}
	return t.sz
}

// sieve is an efficient turn-Key eviction algorithm for web caches.
// See blog post https://cachemon.github.io/SIEVE-website/blog/2023/12/17/sieve-is-simpler-than-lru/
// and also the academic paper "SIEVE is simpler than LRU"
type sieve[K comparable, V any] struct {
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	sz     uint64
	cap    uint64
	items  map[K]*entry[K, V]
	ll     *list.List
	hand   *list.Element
}

func newSieve[K comparable, V any](capacity uint64) *sieve[K, V] {
	ctx, cancel := context.WithCancel(context.Background())

	cache := &sieve[K, V]{
		ctx:    ctx,
		cancel: cancel,
		cap:    capacity,
		items:  make(map[K]*entry[K, V]),
		ll:     list.New(),
	}

	return cache
}

func (s *sieve[K, V]) setCapacity(capacity uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cap = capacity
}

func (s *sieve[K, V]) capacity() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cap
}

func (s *sieve[K, V]) set(key K, value V) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.items[key]; ok {
		e.value = value
		e.visited = true
		return
	}

	for i := 0; s.sz > s.cap && i < 10; i++ {
		s.evict()
	}

	failpoint.Inject("forceEvictAll", func() {
		for s.sz > 0 {
			s.evict()
		}
	})

	e := &entry[K, V]{
		key:   key,
		value: value,
	}
	s.sz += e.size() // calculate the size first without putting to the list.
	e.element = s.ll.PushFront(key)

	s.items[key] = e
}

func (s *sieve[K, V]) get(key K) (value V, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.items[key]; ok {
		e.visited = true
		return e.value, true
	}

	return
}

func (s *sieve[K, V]) remove(key K) (ok bool) {
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

func (s *sieve[K, V]) contains(key K) (ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok = s.items[key]
	return
}

func (s *sieve[K, V]) peek(key K) (value V, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.items[key]; ok {
		return e.value, true
	}

	return
}

func (s *sieve[K, V]) size() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.sz
}

func (s *sieve[K, V]) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.ll.Len()
}

func (s *sieve[K, V]) purge() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, e := range s.items {
		s.removeEntry(e)
	}

	s.ll.Init()
}

func (s *sieve[K, V]) close() {
	s.purge()
	s.mu.Lock()
	s.cancel()
	s.mu.Unlock()
}

func (s *sieve[K, V]) removeEntry(e *entry[K, V]) {
	s.ll.Remove(e.element)
	delete(s.items, e.key)
	s.sz -= e.size()
}

func (s *sieve[K, V]) evict() {
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
