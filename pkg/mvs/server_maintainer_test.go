// Copyright 2026 PingCAP, Inc.
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

package mvs

import (
	"context"
	"encoding/binary"
	"errors"
	"maps"
	"sync"
	"testing"
)

type mockServerHelper struct {
	mu sync.Mutex

	self    serverInfo
	selfErr error

	allInfos map[string]serverInfo
	allErr   error

	filterFn func(serverInfo) bool

	getServerInfoCalls int
	getAllInfoCalls    int
}

func (m *mockServerHelper) serverFilter(s serverInfo) bool {
	if m.filterFn == nil {
		return true
	}
	return m.filterFn(s)
}

func (m *mockServerHelper) getServerInfo() (serverInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getServerInfoCalls++
	if m.selfErr != nil {
		return serverInfo{}, m.selfErr
	}
	return m.self, nil
}

func (m *mockServerHelper) getAllServerInfo(_ context.Context) (map[string]serverInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getAllInfoCalls++
	if m.allErr != nil {
		return nil, m.allErr
	}

	res := make(map[string]serverInfo, len(m.allInfos))
	maps.Copy(res, m.allInfos)
	return res, nil
}

func newServerConsistentHashForTest(mapping map[string]uint32, helper ServerHelper, selfID string, serverIDs ...string) *ServerConsistentHash {
	sch := NewServerConsistentHash(context.Background(), 1, helper)
	sch.chash.hashFunc = mustHash(mapping)
	sch.ID = selfID
	for _, id := range serverIDs {
		sch.AddServer(serverInfo{ID: id})
	}
	return sch
}

func assertRingNodeCount(t *testing.T, sch *ServerConsistentHash, expected int) {
	t.Helper()
	if got := sch.chash.NodeCount(); got != expected {
		t.Fatalf("expected ring node count %d, got %d", expected, got)
	}
}

func assertRingOwner(t *testing.T, sch *ServerConsistentHash, key []byte, expectedOwner string) {
	t.Helper()
	if got := sch.chash.GetNode(key); got != expectedOwner {
		t.Fatalf("expected %q on %s, got %s", key, expectedOwner, got)
	}
}

func assertAvailableForNode(t *testing.T, sch *ServerConsistentHash, nodeID string, key any, expected bool) {
	t.Helper()
	sch.ID = nodeID
	if got := sch.Available(key); got != expected {
		t.Fatalf("expected Available(%v) on %s = %t, got %t", key, nodeID, expected, got)
	}
}

func TestServerConsistentHashAddRemoveAndAvailable(t *testing.T) {
	mapping := map[string]uint32{
		"nodeA#0": 10,
		"nodeB#0": 20,
		"key-low": 5,
		"key-mid": 15,
	}

	sch := newServerConsistentHashForTest(mapping, &mockServerHelper{}, "nodeB", "nodeA", "nodeB")

	if len(sch.servers) != 2 {
		t.Fatalf("expected 2 servers, got %d", len(sch.servers))
	}
	assertRingNodeCount(t, sch, 2)
	assertRingOwner(t, sch, []byte("key-low"), "nodeA")
	assertRingOwner(t, sch, []byte("key-mid"), "nodeB")
	assertAvailableForNode(t, sch, "nodeB", "key-low", false)
	assertAvailableForNode(t, sch, "nodeB", "key-mid", true)

	sch.RemoveServer("nodeB")

	if len(sch.servers) != 1 {
		t.Fatalf("expected 1 server after remove, got %d", len(sch.servers))
	}
	assertRingNodeCount(t, sch, 1)
	assertRingOwner(t, sch, []byte("key-mid"), "nodeA")
	assertAvailableForNode(t, sch, "nodeB", "key-mid", false)
}

func TestServerConsistentHashAvailableSupportsDifferentKeyTypes(t *testing.T) {
	type testCase struct {
		name     string
		key      any
		hashFunc func(t *testing.T) func([]byte) uint32
	}

	testCases := []testCase{
		{
			name: "int64",
			key:  int64(123456789),
			hashFunc: func(t *testing.T) func([]byte) uint32 {
				key := int64(123456789)
				var keyBytes [8]byte
				binary.BigEndian.PutUint64(keyBytes[:], uint64(key))
				return func(data []byte) uint32 {
					switch string(data) {
					case "nodeA#0":
						return 10
					case "nodeB#0":
						return 20
					case "123456789":
						t.Fatalf("int64 key should use binary bytes instead of decimal text")
					}
					if len(data) == len(keyBytes) && string(data) == string(keyBytes[:]) {
						return 15
					}
					t.Fatalf("unexpected hash input: %v", data)
					return 0
				}
			},
		},
		{
			name: "bytes",
			key:  []byte("key-mid"),
			hashFunc: func(t *testing.T) func([]byte) uint32 {
				return func(data []byte) uint32 {
					switch string(data) {
					case "nodeA#0":
						return 10
					case "nodeB#0":
						return 20
					case "key-mid":
						return 15
					default:
						t.Fatalf("unexpected hash input: %q", data)
						return 0
					}
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sch := NewServerConsistentHash(context.Background(), 1, &mockServerHelper{})
			sch.chash.hashFunc = tc.hashFunc(t)
			sch.AddServer(serverInfo{ID: "nodeA"})
			sch.AddServer(serverInfo{ID: "nodeB"})

			assertAvailableForNode(t, sch, "nodeB", tc.key, true)
			assertAvailableForNode(t, sch, "nodeA", tc.key, false)
		})
	}
}

func TestServerConsistentHashFetchAppliesFilter(t *testing.T) {
	mapping := map[string]uint32{
		"nodeA#0": 10,
		"nodeB#0": 20,
		"key-mid": 15,
	}

	helper := &mockServerHelper{
		allInfos: map[string]serverInfo{
			"nodeA": {ID: "nodeA"},
			"nodeB": {ID: "nodeB"},
		},
		filterFn: func(s serverInfo) bool {
			return s.ID == "nodeA"
		},
	}
	sch := newServerConsistentHashForTest(mapping, helper, "")

	if err := sch.refresh(); err != nil {
		t.Fatalf("fetch failed: %v", err)
	}
	if len(sch.servers) != 1 {
		t.Fatalf("expected 1 server after filter, got %d", len(sch.servers))
	}
	if _, ok := sch.servers["nodeA"]; !ok {
		t.Fatalf("expected nodeA kept by filter")
	}
	assertRingNodeCount(t, sch, 1)
	assertRingOwner(t, sch, []byte("key-mid"), "nodeA")
}

func TestServerConsistentHashFetchNoChange(t *testing.T) {
	mapping := map[string]uint32{
		"nodeA#0": 10,
		"key-mid": 15,
	}

	helper := &mockServerHelper{
		allInfos: map[string]serverInfo{
			"nodeA": {ID: "nodeA"},
		},
	}
	sch := newServerConsistentHashForTest(mapping, helper, "")

	if err := sch.refresh(); err != nil {
		t.Fatalf("first fetch failed: %v", err)
	}
	beforeNodeCount := sch.chash.NodeCount()
	beforeRingSize := len(sch.chash.ring)

	if err := sch.refresh(); err != nil {
		t.Fatalf("second fetch failed: %v", err)
	}
	if got := sch.chash.NodeCount(); got != beforeNodeCount {
		t.Fatalf("expected node count %d, got %d", beforeNodeCount, got)
	}
	if got := len(sch.chash.ring); got != beforeRingSize {
		t.Fatalf("expected ring size %d, got %d", beforeRingSize, got)
	}
	if got := helper.getAllInfoCalls; got != 2 {
		t.Fatalf("expected getAllServerInfo called twice, got %d", got)
	}
	assertRingOwner(t, sch, []byte("key-mid"), "nodeA")
}

func TestServerConsistentHashInit(t *testing.T) {
	mapping := map[string]uint32{
		"nodeA#0": 10,
		"key-mid": 15,
	}

	helper := &mockServerHelper{
		self: serverInfo{ID: "nodeA"},
		allInfos: map[string]serverInfo{
			"nodeA": {ID: "nodeA"},
		},
	}
	sch := newServerConsistentHashForTest(mapping, helper, "")

	if !sch.init() {
		t.Fatalf("init failed")
	}

	if sch.ID != "nodeA" {
		t.Fatalf("expected current ID nodeA, got %s", sch.ID)
	}
	assertRingNodeCount(t, sch, 0)
	if got := helper.getServerInfoCalls; got < 1 {
		t.Fatalf("expected getServerInfo called at least once, got %d", got)
	}
	if got := helper.getAllInfoCalls; got != 0 {
		t.Fatalf("expected getAllServerInfo not called in init, got %d", got)
	}
}

func TestServerConsistentHashInitCanceled(t *testing.T) {
	installMockTimeForTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	helper := &mockServerHelper{
		selfErr: errors.New("boom"),
		allErr:  errors.New("boom"),
	}
	sch := NewServerConsistentHash(ctx, 1, helper)

	start := mvsNow()
	ok := sch.init()
	if ok {
		t.Fatalf("expected init failed when context canceled")
	}
	if mvsSince(start) > 0 {
		t.Fatalf("init should return quickly when context is canceled")
	}
	if got := helper.getServerInfoCalls; got > 1 {
		t.Fatalf("expected at most one getServerInfo call when canceled, got %d", got)
	}
}

func TestServerConsistentHashFetchError(t *testing.T) {
	helper := &mockServerHelper{
		allErr: errors.New("boom"),
	}
	sch := NewServerConsistentHash(context.Background(), 1, helper)

	err := sch.refresh()
	if err == nil {
		t.Fatalf("expected fetch error, got nil")
	}
}
