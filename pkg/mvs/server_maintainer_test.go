package utils

import (
	"context"
	"errors"
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
	for k, v := range m.allInfos {
		res[k] = v
	}
	return res, nil
}

func TestServerConsistentHashAddRemoveAndAvailable(t *testing.T) {
	mapping := map[string]uint32{
		"nodeA#0": 10,
		"nodeB#0": 20,
		"key-low": 5,
		"key-mid": 15,
	}

	sch := NewServerConsistentHash(1, &mockServerHelper{})
	sch.chash.hashFunc = mustHash(mapping)
	sch.ID = "nodeB"

	sch.addServer(serverInfo{ID: "nodeA"})
	sch.addServer(serverInfo{ID: "nodeB"})

	if len(sch.servers) != 2 {
		t.Fatalf("expected 2 servers, got %d", len(sch.servers))
	}
	if got := sch.chash.NodeCount(); got != 2 {
		t.Fatalf("expected ring node count 2, got %d", got)
	}
	if got := sch.ToServerID("key-low"); got != "nodeA" {
		t.Fatalf("expected key-low on nodeA, got %s", got)
	}
	if got := sch.ToServerID("key-mid"); got != "nodeB" {
		t.Fatalf("expected key-mid on nodeB, got %s", got)
	}
	if sch.Available("key-low") {
		t.Fatalf("expected key-low unavailable for nodeB")
	}
	if !sch.Available("key-mid") {
		t.Fatalf("expected key-mid available for nodeB")
	}

	sch.removeServer("nodeB")

	if len(sch.servers) != 1 {
		t.Fatalf("expected 1 server after remove, got %d", len(sch.servers))
	}
	if got := sch.chash.NodeCount(); got != 1 {
		t.Fatalf("expected ring node count 1, got %d", got)
	}
	if got := sch.ToServerID("key-mid"); got != "nodeA" {
		t.Fatalf("expected key-mid on nodeA after remove, got %s", got)
	}
	if sch.Available("key-mid") {
		t.Fatalf("expected key-mid unavailable for nodeB after remove")
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
	sch := NewServerConsistentHash(1, helper)
	sch.chash.hashFunc = mustHash(mapping)

	if err := sch.Refresh(context.Background()); err != nil {
		t.Fatalf("fetch failed: %v", err)
	}
	if len(sch.servers) != 1 {
		t.Fatalf("expected 1 server after filter, got %d", len(sch.servers))
	}
	if _, ok := sch.servers["nodeA"]; !ok {
		t.Fatalf("expected nodeA kept by filter")
	}
	if got := sch.chash.NodeCount(); got != 1 {
		t.Fatalf("expected ring node count 1, got %d", got)
	}
	if got := sch.ToServerID("key-mid"); got != "nodeA" {
		t.Fatalf("expected key-mid on nodeA, got %s", got)
	}
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
	sch := NewServerConsistentHash(1, helper)
	sch.chash.hashFunc = mustHash(mapping)

	if err := sch.Refresh(context.Background()); err != nil {
		t.Fatalf("first fetch failed: %v", err)
	}
	beforeNodeCount := sch.chash.NodeCount()
	beforeRingSize := len(sch.chash.ring)

	if err := sch.Refresh(context.Background()); err != nil {
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
	if got := sch.ToServerID("key-mid"); got != "nodeA" {
		t.Fatalf("expected key-mid on nodeA, got %s", got)
	}
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
	sch := NewServerConsistentHash(1, helper)
	sch.chash.hashFunc = mustHash(mapping)

	sch.init(context.Background())

	if sch.ID != "nodeA" {
		t.Fatalf("expected current ID nodeA, got %s", sch.ID)
	}
	if got := sch.chash.NodeCount(); got != 1 {
		t.Fatalf("expected ring node count 1, got %d", got)
	}
	if got := helper.getServerInfoCalls; got < 1 {
		t.Fatalf("expected getServerInfo called at least once, got %d", got)
	}
	if got := helper.getAllInfoCalls; got < 1 {
		t.Fatalf("expected getAllServerInfo called at least once, got %d", got)
	}
}

func TestServerConsistentHashFetchError(t *testing.T) {
	helper := &mockServerHelper{
		allErr: errors.New("boom"),
	}
	sch := NewServerConsistentHash(1, helper)

	err := sch.Refresh(context.Background())
	if err == nil {
		t.Fatalf("expected fetch error, got nil")
	}
}
