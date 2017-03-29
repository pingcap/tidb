// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package mvmap

import (
	"bytes"
	"hash"
	"hash/fnv"
)

type entry struct {
	addr   dataAddr
	keyLen uint32
	valLen uint32
	next   entryAddr
}

type entryStore struct {
	slices   [][]entry
	sliceIdx uint32
	sliceLen uint32
}

type dataStore struct {
	slices   [][]byte
	sliceIdx uint32
	sliceLen uint32
}

type entryAddr struct {
	sliceIdx uint32
	offset   uint32
}

type dataAddr struct {
	sliceIdx uint32
	offset   uint32
}

const (
	maxDataSliceLen  = 64 * 1024
	maxEntrySliceLen = 8 * 1024
)

func (ds *dataStore) put(key, value []byte) dataAddr {
	dataLen := uint32(len(key) + len(value))
	if ds.sliceLen != 0 && ds.sliceLen+dataLen > maxDataSliceLen {
		ds.slices = append(ds.slices, make([]byte, 0, maxDataSliceLen))
		ds.sliceLen = 0
		ds.sliceIdx++
	}
	addr := dataAddr{sliceIdx: ds.sliceIdx, offset: ds.sliceLen}
	slice := ds.slices[ds.sliceIdx]
	slice = append(slice, key...)
	slice = append(slice, value...)
	ds.slices[ds.sliceIdx] = slice
	ds.sliceLen += dataLen
	return addr
}

func (ds *dataStore) get(he entry) (key, value []byte) {
	slice := ds.slices[he.addr.sliceIdx]
	valOffset := he.addr.offset + he.keyLen
	return slice[he.addr.offset:valOffset], slice[valOffset : valOffset+he.valLen]
}

var nullEntryAddr = entryAddr{}

func (es *entryStore) put(he entry) entryAddr {
	if es.sliceLen == maxEntrySliceLen {
		es.slices = append(es.slices, make([]entry, 0, maxEntrySliceLen))
		es.sliceLen = 0
		es.sliceIdx++
	}
	addr := entryAddr{sliceIdx: es.sliceIdx, offset: es.sliceLen}
	slice := es.slices[es.sliceIdx]
	slice = append(slice, he)
	es.slices[es.sliceIdx] = slice
	es.sliceLen++
	return addr
}

func (es *entryStore) get(addr entryAddr) entry {
	return es.slices[addr.sliceIdx][addr.offset]
}

// MVMap stores multiple value for a given key with minimum GC overhead.
// A given key can store multiple values.
// It is not thread-safe, should only be used in one goroutine.
type MVMap struct {
	entryStore entryStore
	dataStore  dataStore
	hashTable  map[uint64]entryAddr
	hashFunc   hash.Hash64
}

// NewMVMap creates a new multi-value map.
func NewMVMap() *MVMap {
	m := new(MVMap)
	m.hashTable = make(map[uint64]entryAddr)
	m.hashFunc = fnv.New64()
	m.entryStore.slices = [][]entry{make([]entry, 0, 64)}
	// append first empty entry so zero entry pointer an represent null.
	m.entryStore.put(entry{})
	m.dataStore.slices = [][]byte{make([]byte, 0, 1024)}
	return m
}

// Put puts the key/value pairs to the MVMap, if the key already exists, old value will not be overwritten,
// values are stored in a list.
func (m *MVMap) Put(key, value []byte) {
	hashKey := m.hash(key)
	oldEntryAddr := m.hashTable[hashKey]
	dataAddr := m.dataStore.put(key, value)
	entry := entry{
		addr:   dataAddr,
		keyLen: uint32(len(key)),
		valLen: uint32(len(value)),
		next:   oldEntryAddr,
	}
	newEntryPtr := m.entryStore.put(entry)
	m.hashTable[hashKey] = newEntryPtr
}

// Get gets the values of the key.
func (m *MVMap) Get(key []byte) [][]byte {
	var values [][]byte
	hashKey := m.hash(key)
	entryAddr := m.hashTable[hashKey]
	for entryAddr != nullEntryAddr {
		he := m.entryStore.get(entryAddr)
		entryAddr = he.next
		k, v := m.dataStore.get(he)
		if bytes.Compare(key, k) != 0 {
			continue
		}
		values = append(values, v)
	}
	return values
}

func (m *MVMap) hash(key []byte) uint64 {
	m.hashFunc.Reset()
	m.hashFunc.Write(key)
	return m.hashFunc.Sum64()
}
