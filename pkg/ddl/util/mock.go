// Copyright 2023 PingCAP, Inc.
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

package util

import "sync"

// MockGlobalStateEntry is a mock global state entry.
var MockGlobalStateEntry = &MockGlobalState{
	currentOwner: make(map[ownerKey]string),
}

// MockGlobalState is a mock global state.
type MockGlobalState struct {
	currentOwner map[ownerKey]string // store ID + owner_key (ddl/stats/bindinfo) => owner_id
	mu           sync.Mutex
}

type ownerKey struct {
	storeID string
	ownerTp string
}

// OwnerKey returns a mock global state selector with corresponding owner key.
func (m *MockGlobalState) OwnerKey(storeID, tp string) *MockGlobalStateSelector {
	return &MockGlobalStateSelector{m: m, storeID: storeID, ownerKey: tp}
}

// MockGlobalStateSelector is used to get info from global state.
type MockGlobalStateSelector struct {
	m        *MockGlobalState
	storeID  string
	ownerKey string
}

// GetOwner returns the owner.
func (t *MockGlobalStateSelector) GetOwner() string {
	t.m.mu.Lock()
	defer t.m.mu.Unlock()
	k := ownerKey{storeID: t.storeID, ownerTp: t.ownerKey}
	return t.m.currentOwner[k]
}

// SetOwner sets the owner if the owner is empty.
func (t *MockGlobalStateSelector) SetOwner(owner string) bool {
	t.m.mu.Lock()
	defer t.m.mu.Unlock()
	k := ownerKey{storeID: t.storeID, ownerTp: t.ownerKey}
	if t.m.currentOwner[k] == "" {
		t.m.currentOwner[k] = owner
		return true
	}
	return false
}

// UnsetOwner unsets the owner.
func (t *MockGlobalStateSelector) UnsetOwner(owner string) bool {
	t.m.mu.Lock()
	defer t.m.mu.Unlock()
	k := ownerKey{storeID: t.storeID, ownerTp: t.ownerKey}
	if t.m.currentOwner[k] == owner {
		t.m.currentOwner[k] = ""
		return true
	}
	return false
}

// IsOwner returns whether it is the owner.
func (t *MockGlobalStateSelector) IsOwner(owner string) bool {
	t.m.mu.Lock()
	defer t.m.mu.Unlock()
	k := ownerKey{storeID: t.storeID, ownerTp: t.ownerKey}
	return t.m.currentOwner[k] == owner
}
