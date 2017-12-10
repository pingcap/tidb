// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"sync"

	"github.com/google/btree"
)

// KVBase is an abstract interface for load/save pd cluster data.
type KVBase interface {
	Load(key string) (string, error)
	LoadRange(key, endKey string, limit int) ([]string, error)
	Save(key, value string) error
}

type memoryKV struct {
	sync.RWMutex
	tree *btree.BTree
}

// NewMemoryKV returns an in-memory kvBase for testing.
func NewMemoryKV() KVBase {
	return &memoryKV{
		tree: btree.New(2),
	}
}

type memoryKVItem struct {
	key, value string
}

func (s memoryKVItem) Less(than btree.Item) bool {
	return s.key < than.(memoryKVItem).key
}

func (kv *memoryKV) Load(key string) (string, error) {
	kv.RLock()
	defer kv.RUnlock()
	item := kv.tree.Get(memoryKVItem{key, ""})
	if item == nil {
		return "", nil
	}
	return item.(memoryKVItem).value, nil
}

func (kv *memoryKV) LoadRange(key, endKey string, limit int) ([]string, error) {
	kv.RLock()
	defer kv.RUnlock()
	res := make([]string, 0, limit)
	kv.tree.AscendRange(memoryKVItem{key, ""}, memoryKVItem{endKey, ""}, func(item btree.Item) bool {
		res = append(res, item.(memoryKVItem).value)
		return len(res) < int(limit)
	})
	return res, nil
}

func (kv *memoryKV) Save(key, value string) error {
	kv.Lock()
	defer kv.Unlock()
	kv.tree.ReplaceOrInsert(memoryKVItem{key, value})
	return nil
}
