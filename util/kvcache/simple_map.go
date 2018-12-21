package kvcache

import (
	"fmt"
	"sync"
)

type SimpleMap struct {
	cache *sync.Map
}

func NewSimpleMapCache() *SimpleMap {
	return &SimpleMap{
		cache: new(sync.Map),
	}
}

func (b *SimpleMap) Put(key interface{}, value interface{}) {
	b.cache.Store(key, value)
}

func (b *SimpleMap) Get(key interface{}) (value interface{}, ok bool) {
	element, ok := b.cache.Load(key)
	return element, ok
}

func (b *SimpleMap) Delete(key string) {
	b.cache.Delete(key)
}

func (b *SimpleMap) Display() {
	b.cache.Range(func(key, value interface{}) bool {
		fmt.Printf("hash: %v, value: %v", key, value)
		return true
	})
}
