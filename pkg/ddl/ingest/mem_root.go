// Copyright 2022 PingCAP, Inc.
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

package ingest

import (
	"sync"
	"unsafe"
)

// MemRoot is used to track the memory usage for the lightning backfill process.
// TODO(lance6716): change API to prevent TOCTOU.
type MemRoot interface {
	Consume(size int64)
	Release(size int64)
	CheckConsume(size int64) bool
	ConsumeWithTag(tag string, size int64)
	ReleaseWithTag(tag string)

	SetMaxMemoryQuota(quota int64)
	MaxMemoryQuota() int64
	CurrentUsage() int64
	CurrentUsageWithTag(tag string) int64
	RefreshConsumption()
}

var (
	// structSizeBackendCtx is the size of litBackendCtx.
	structSizeBackendCtx int64
	// structSizeEngineInfo is the size of engineInfo.
	structSizeEngineInfo int64
	// structSizeWriterCtx is the size of writerContext.
	structSizeWriterCtx int64
)

func init() {
	structSizeBackendCtx = int64(unsafe.Sizeof(litBackendCtx{}))
	structSizeEngineInfo = int64(unsafe.Sizeof(engineInfo{}))
	structSizeWriterCtx = int64(unsafe.Sizeof(writerContext{}))
}

// memRootImpl is an implementation of MemRoot.
type memRootImpl struct {
	maxLimit      int64
	currUsage     int64
	structSize    map[string]int64
	backendCtxMgr *litBackendCtxMgr
	mu            sync.RWMutex
}

// NewMemRootImpl creates a new memRootImpl.
func NewMemRootImpl(maxQuota int64, bcCtxMgr *litBackendCtxMgr) *memRootImpl {
	return &memRootImpl{
		maxLimit:      maxQuota,
		currUsage:     0,
		structSize:    make(map[string]int64, 10),
		backendCtxMgr: bcCtxMgr,
	}
}

// SetMaxMemoryQuota implements MemRoot.
func (m *memRootImpl) SetMaxMemoryQuota(maxQuota int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.maxLimit = maxQuota
}

// MaxMemoryQuota implements MemRoot.
func (m *memRootImpl) MaxMemoryQuota() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.maxLimit
}

// CurrentUsage implements MemRoot.
func (m *memRootImpl) CurrentUsage() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.currUsage
}

// CurrentUsageWithTag implements MemRoot.
func (m *memRootImpl) CurrentUsageWithTag(tag string) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.structSize[tag]
}

// Consume implements MemRoot.
func (m *memRootImpl) Consume(size int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currUsage += size
}

// Release implements MemRoot.
func (m *memRootImpl) Release(size int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currUsage -= size
}

// ConsumeWithTag implements MemRoot.
func (m *memRootImpl) ConsumeWithTag(tag string, size int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currUsage += size
	if s, ok := m.structSize[tag]; ok {
		m.structSize[tag] = s + size
		return
	}
	m.structSize[tag] = size
}

// CheckConsume implements MemRoot.
func (m *memRootImpl) CheckConsume(size int64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.currUsage+size <= m.maxLimit
}

// ReleaseWithTag implements MemRoot.
func (m *memRootImpl) ReleaseWithTag(tag string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currUsage -= m.structSize[tag]
	delete(m.structSize, tag)
}

// RefreshConsumption implements MemRoot.
func (m *memRootImpl) RefreshConsumption() {
	m.backendCtxMgr.UpdateMemoryUsage()
}
