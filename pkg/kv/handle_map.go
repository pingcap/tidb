// Copyright 2019 PingCAP, Inc.
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

package kv

import (
	"unsafe"

	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/size"
)

type HandleMap struct {
	ints map[int64]any
	strs map[string]strHandleVal

	// Use two two-dimensional map to fit partitionHandle.
	// The first int64 is for partitionID.
	partitionInts map[int64]map[int64]any
	partitionStrs map[int64]map[string]strHandleVal
}

type strHandleVal struct {
	h   Handle
	val any
}

// SizeofHandleMap presents the memory size of struct HandleMap
const SizeofHandleMap = int64(unsafe.Sizeof(HandleMap{}))

// SizeofStrHandleVal presents the memory size of struct strHandleVal
const SizeofStrHandleVal = int64(unsafe.Sizeof(strHandleVal{}))

// NewHandleMap creates a new map for handle.
func NewHandleMap() *HandleMap {
	return &HandleMap{
		ints: map[int64]any{},
		strs: map[string]strHandleVal{},

		partitionInts: map[int64]map[int64]any{},
		partitionStrs: map[int64]map[string]strHandleVal{},
	}
}

// Get gets a value by a Handle.
func (m *HandleMap) Get(h Handle) (v any, ok bool) {
	ints, strs := m.ints, m.strs
	if ph, ok := h.(PartitionHandle); ok {
		idx := ph.PartitionID
		if (h.IsInt() && m.partitionInts[idx] == nil) ||
			(!h.IsInt() && m.partitionStrs[idx] == nil) {
			return nil, false
		}
		ints, strs = m.partitionInts[idx], m.partitionStrs[idx]
	}
	if h.IsInt() {
		v, ok = ints[h.IntValue()]
	} else {
		var strVal strHandleVal
		strVal, ok = strs[string(h.Encoded())]
		v = strVal.val
	}
	return
}

func calcStrsMemUsage(strs map[string]strHandleVal) int64 {
	res := int64(0)
	for key := range strs {
		res += size.SizeOfString + int64(len(key)) + SizeofStrHandleVal
	}
	return res
}

func calcIntsMemUsage(ints map[int64]any) int64 {
	return int64(len(ints)) * (size.SizeOfInt64 + size.SizeOfInterface)
}

// MemUsage gets the memory usage.
func (m *HandleMap) MemUsage() int64 {
	res := SizeofHandleMap
	res += int64(len(m.partitionInts)) * (size.SizeOfInt64 + size.SizeOfMap)
	for _, v := range m.partitionInts {
		res += calcIntsMemUsage(v)
	}
	res += int64(len(m.partitionStrs)) * (size.SizeOfInt64 + size.SizeOfMap)
	for _, v := range m.partitionStrs {
		res += calcStrsMemUsage(v)
	}
	res += calcIntsMemUsage(m.ints)
	res += calcStrsMemUsage(m.strs)
	return res
}

// Set sets a value with a Handle.
func (m *HandleMap) Set(h Handle, val any) {
	ints, strs := m.ints, m.strs
	if ph, ok := h.(PartitionHandle); ok {
		idx := ph.PartitionID
		if h.IsInt() {
			if m.partitionInts[idx] == nil {
				m.partitionInts[idx] = make(map[int64]any)
			}
			ints = m.partitionInts[idx]
		} else {
			if m.partitionStrs[idx] == nil {
				m.partitionStrs[idx] = make(map[string]strHandleVal)
			}
			strs = m.partitionStrs[idx]
		}
	}
	if h.IsInt() {
		ints[h.IntValue()] = val
	} else {
		strs[string(h.Encoded())] = strHandleVal{
			h:   h,
			val: val,
		}
	}
}

// Delete deletes a entry from the map.
func (m *HandleMap) Delete(h Handle) {
	ints, strs := m.ints, m.strs
	if ph, ok := h.(PartitionHandle); ok {
		idx := ph.PartitionID
		if (h.IsInt() && m.partitionInts[idx] == nil) ||
			(!h.IsInt() && m.partitionStrs[idx] == nil) {
			return
		}
		ints, strs = m.partitionInts[idx], m.partitionStrs[idx]
	}
	if h.IsInt() {
		delete(ints, h.IntValue())
	} else {
		delete(strs, string(h.Encoded()))
	}
}

// Len returns the length of the map.
func (m *HandleMap) Len() int {
	l := len(m.ints) + len(m.strs)
	for _, v := range m.partitionInts {
		l += len(v)
	}
	for _, v := range m.partitionStrs {
		l += len(v)
	}
	return l
}

// Range iterates the HandleMap with fn, the fn returns true to continue, returns false to stop.
func (m *HandleMap) Range(fn func(h Handle, val any) bool) {
	for h, val := range m.ints {
		if !fn(IntHandle(h), val) {
			return
		}
	}
	for _, strVal := range m.strs {
		if !fn(strVal.h, strVal.val) {
			return
		}
	}
	for pid, v := range m.partitionInts {
		for h, val := range v {
			if !fn(NewPartitionHandle(pid, IntHandle(h)), val) {
				return
			}
		}
	}
	for _, v := range m.partitionStrs {
		for _, strVal := range v {
			if !fn(strVal.h, strVal.val) {
				return
			}
		}
	}
}

// MemAwareHandleMap is similar to HandleMap, but it's aware of its memory usage and doesn't support delete.
// It only tracks the actual sizes. Objects that are pointed to by the key or value are not tracked.
// Those should be tracked by the caller.
type MemAwareHandleMap[V any] struct {
	ints memAwareMap[int64, V]
	strs memAwareMap[string, strHandleValue[V]]

	partitionInts map[int64]*memAwareMap[int64, V]
	partitionStrs map[int64]*memAwareMap[string, strHandleValue[V]]
}

type strHandleValue[V any] struct {
	h   Handle
	val V
}

// NewMemAwareHandleMap creates a new map for handle.
func NewMemAwareHandleMap[V any]() (res *MemAwareHandleMap[V]) {
	res = &MemAwareHandleMap[V]{
		partitionInts: map[int64]*memAwareMap[int64, V]{},
		partitionStrs: map[int64]*memAwareMap[string, strHandleValue[V]]{},
	}
	res.ints.Init(make(map[int64]V))
	res.strs.Init(make(map[string]strHandleValue[V]))
	return
}

// Get gets a value by a Handle.
func (m *MemAwareHandleMap[V]) Get(h Handle) (v V, found bool) {
	ints, strs := m.ints.M, m.strs.M
	if ph, ok := h.(PartitionHandle); ok {
		idx := ph.PartitionID
		if h.IsInt() {
			p := m.partitionInts[idx]
			if p == nil {
				return
			}
			ints = p.M
		} else {
			p := m.partitionStrs[idx]
			if p == nil {
				return
			}
			strs = p.M
		}
	}
	if h.IsInt() {
		v, found = ints[h.IntValue()]
	} else {
		strVal, ok := strs[string(h.Encoded())]
		v, found = strVal.val, ok
	}
	return
}

// Set sets a value with a Handle.
func (m *MemAwareHandleMap[V]) Set(h Handle, val V) int64 {
	ints, strs := &m.ints, &m.strs
	if ph, ok := h.(PartitionHandle); ok {
		idx := ph.PartitionID
		if h.IsInt() {
			p := m.partitionInts[idx]
			if p == nil {
				p = newMemAwareMap[int64, V]()
				m.partitionInts[idx] = p
			}
			ints = p
		} else {
			p := m.partitionStrs[idx]
			if p == nil {
				p = newMemAwareMap[string, strHandleValue[V]]()
				m.partitionStrs[idx] = p
			}
			strs = p
		}
	}
	if h.IsInt() {
		return ints.Set(h.IntValue(), val)
	}
	return strs.Set(string(h.Encoded()), strHandleValue[V]{
		h:   h,
		val: val,
	})
}

// Range iterates the MemAwareHandleMap with fn, the fn returns true to continue, returns false to stop.
func (m *MemAwareHandleMap[V]) Range(fn func(h Handle, val V) bool) {
	for h, val := range m.ints.M {
		if !fn(IntHandle(h), val) {
			return
		}
	}
	for _, strVal := range m.strs.M {
		if !fn(strVal.h, strVal.val) {
			return
		}
	}
	for pid, v := range m.partitionInts {
		for h, val := range v.M {
			if !fn(NewPartitionHandle(pid, IntHandle(h)), val) {
				return
			}
		}
	}
	for _, v := range m.partitionStrs {
		for _, strVal := range v.M {
			if !fn(strVal.h, strVal.val) {
				return
			}
		}
	}
}

// PartitionHandle combines a handle and a PartitionID, used to location a row in partitioned table.
type PartitionHandle struct {
	Handle
	PartitionID int64
}

// NewPartitionHandle creates a PartitionHandle from a normal handle and a pid.
func NewPartitionHandle(pid int64, h Handle) PartitionHandle {
	return PartitionHandle{
		Handle:      h,
		PartitionID: pid,
	}
}

// Copy implements the Handle interface.
func (ph PartitionHandle) Copy() Handle {
	return PartitionHandle{
		Handle:      ph.Handle.Copy(),
		PartitionID: ph.PartitionID,
	}
}

// Equal implements the Handle interface.
func (ph PartitionHandle) Equal(h Handle) bool {
	// Compare pid and handle if both sides are `PartitionHandle`.
	if ph2, ok := h.(PartitionHandle); ok {
		return ph.PartitionID == ph2.PartitionID && ph.Handle.Equal(ph2.Handle)
	}

	// Otherwise, use underlying handle to do comparation.
	return ph.Handle.Equal(h)
}

// Compare implements the Handle interface.
func (ph PartitionHandle) Compare(h Handle) int {
	if ph2, ok := h.(PartitionHandle); ok {
		if ph.PartitionID < ph2.PartitionID {
			return -1
		}
		if ph.PartitionID > ph2.PartitionID {
			return 1
		}
		return ph.Handle.Compare(ph2.Handle)
	}
	panic("PartitonHandle compares to non-parition Handle")
}

// MemUsage implements the Handle interface.
func (ph PartitionHandle) MemUsage() uint64 {
	return ph.Handle.MemUsage() + uint64(unsafe.Sizeof(PartitionHandle{}))
}

// ExtraMemSize implements the Handle interface.
func (ph PartitionHandle) ExtraMemSize() uint64 {
	return ph.Handle.ExtraMemSize()
}

type memAwareMap[K comparable, V any] = hack.MemAwareMap[K, V]

func newMemAwareMap[K comparable, V any]() *memAwareMap[K, V] {
	return hack.NewMemAwareMap[K, V](0)
}
