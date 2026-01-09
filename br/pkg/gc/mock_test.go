// Copyright 2025 PingCAP, Inc. Licensed under Apache-2.0.

package gc_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/badger"
	"github.com/pingcap/tidb/br/pkg/gc"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore/lockstore"
	unistoretikv "github.com/pingcap/tidb/pkg/store/mockstore/unistore/tikv"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore/tikv/mvcc"
	"github.com/stretchr/testify/require"
	tikv "github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	pdgc "github.com/tikv/pd/client/clients/gc"
)

// ============================================================================
// Mock implementations
// ============================================================================

// mockCodec implements tikv.Codec interface (partial for testing)
type mockCodec struct {
	tikv.Codec
	keyspaceID tikv.KeyspaceID
}

func (m *mockCodec) GetKeyspaceID() tikv.KeyspaceID {
	return m.keyspaceID
}

// mockStorage implements kv.Storage interface (partial)
type mockStorage struct {
	kv.Storage
	codec *mockCodec
}

func (m *mockStorage) GetCodec() tikv.Codec {
	return m.codec
}

// mockGCStatesClientWithTracking wraps the real GC states client with call tracking
// for test verification. This uses the validated implementation from GCStatesManagerForTest.
//
// Design: Only track call counts, not parameters. Use GetGCState() to query actual state.
type mockGCStatesClientWithTracking struct {
	inner                pdgc.GCStatesClient
	mu                   sync.Mutex
	setGCBarrierCalls    int
	deleteGCBarrierCalls int
	getGCStateCalls      int
}

func (m *mockGCStatesClientWithTracking) GetGCState(ctx context.Context) (pdgc.GCState, error) {
	m.mu.Lock()
	m.getGCStateCalls++
	m.mu.Unlock()
	return m.inner.GetGCState(ctx)
}

func (m *mockGCStatesClientWithTracking) SetGCBarrier(ctx context.Context, barrierID string, barrierTS uint64, ttl time.Duration) (*pdgc.GCBarrierInfo, error) {
	m.mu.Lock()
	m.setGCBarrierCalls++
	m.mu.Unlock()
	return m.inner.SetGCBarrier(ctx, barrierID, barrierTS, ttl)
}

func (m *mockGCStatesClientWithTracking) DeleteGCBarrier(ctx context.Context, barrierID string) (*pdgc.GCBarrierInfo, error) {
	m.mu.Lock()
	m.deleteGCBarrierCalls++
	m.mu.Unlock()
	return m.inner.DeleteGCBarrier(ctx, barrierID)
}

func (m *mockGCStatesClientWithTracking) SetGlobalGCBarrier(ctx context.Context, barrierID string, barrierTS uint64, ttl time.Duration) (*pdgc.GlobalGCBarrierInfo, error) {
	return m.inner.SetGlobalGCBarrier(ctx, barrierID, barrierTS, ttl)
}

func (m *mockGCStatesClientWithTracking) DeleteGlobalGCBarrier(ctx context.Context, barrierID string) (*pdgc.GlobalGCBarrierInfo, error) {
	return m.inner.DeleteGlobalGCBarrier(ctx, barrierID)
}

func (m *mockGCStatesClientWithTracking) GetAllKeyspacesGCStates(ctx context.Context) (pdgc.ClusterGCStates, error) {
	return m.inner.GetAllKeyspacesGCStates(ctx)
}

// mockPDClientWithGCStates implements pd.Client interface using full MockPD
// This provides complete, validated mock behavior with call tracking for assertions.
//
// Design: Only track call counts, not parameters. Use GetGCState() to query actual state.
type mockPDClientWithGCStates struct {
	pd.Client
	mu                 sync.Mutex
	mockPD             *unistoretikv.MockPD
	gcStatesClients    map[uint32]*mockGCStatesClientWithTracking
	updateServiceCalls int
	// Test resources for cleanup
	db      *badger.DB
	dbPath  string
	logPath string
	rm      *unistoretikv.MockRegionManager
}

// createTestDB creates a BadgerDB instance for testing
func createTestDB(t *testing.T) (*badger.DB, string, string, error) {
	dbPath := t.TempDir()
	logPath := t.TempDir()
	subPath := fmt.Sprintf("/%d", 0)
	opts := badger.DefaultOptions
	opts.Dir = filepath.Join(dbPath, subPath)
	opts.ValueDir = filepath.Join(logPath, subPath)
	opts.ManagedTxns = true
	db, err := badger.Open(opts)
	return db, dbPath, logPath, err
}

func newMockPDClientWithGCStates(t *testing.T) *mockPDClientWithGCStates {
	// Create BadgerDB instance
	db, dbPath, logPath, err := createTestDB(t)
	require.NoError(t, err)

	// Create DBBundle
	dbBundle := &mvcc.DBBundle{
		DB:        db,
		LockStore: lockstore.NewMemStore(4096),
	}

	// Create MockRegionManager
	rm, err := unistoretikv.NewMockRegionManager(dbBundle, 1, unistoretikv.RegionOptions{
		StoreAddr:  "127.0.0.1:10086",
		PDAddr:     "127.0.0.1:2379",
		RegionSize: 96 * 1024 * 1024,
	})
	require.NoError(t, err)

	// Create MockPD with the region manager
	mockPD := unistoretikv.NewMockPD(rm)

	return &mockPDClientWithGCStates{
		mockPD:          mockPD,
		gcStatesClients: make(map[uint32]*mockGCStatesClientWithTracking),
		db:              db,
		dbPath:          dbPath,
		logPath:         logPath,
		rm:              rm,
	}
}

// Cleanup cleans up test resources
func (m *mockPDClientWithGCStates) Cleanup() {
	if m.rm != nil {
		_ = m.rm.Close()
	}
	if m.db != nil {
		_ = m.db.Close()
	}
	if m.dbPath != "" {
		_ = os.RemoveAll(m.dbPath)
	}
	if m.logPath != "" {
		_ = os.RemoveAll(m.logPath)
	}
}

func (m *mockPDClientWithGCStates) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	m.mu.Lock()
	m.updateServiceCalls++
	m.mu.Unlock()
	return m.mockPD.UpdateServiceGCSafePoint(ctx, serviceID, ttl, safePoint)
}

func (m *mockPDClientWithGCStates) UpdateGCSafePoint(ctx context.Context, safePoint uint64) (uint64, error) {
	return m.mockPD.UpdateGCSafePoint(ctx, safePoint)
}

func (m *mockPDClientWithGCStates) GetGCStatesClient(keyspaceID uint32) pdgc.GCStatesClient {
	m.mu.Lock()
	defer m.mu.Unlock()

	cli, exists := m.gcStatesClients[keyspaceID]
	if !exists {
		// Wrap the real client with tracking
		cli = &mockGCStatesClientWithTracking{
			inner: m.mockPD.GetGCStatesClient(keyspaceID),
		}
		m.gcStatesClients[keyspaceID] = cli
	}
	return cli
}

// ============================================================================
// State Query Helper Functions
// ============================================================================

// findBarrier finds a barrier by ID in the GC state.
// Returns nil if not found.
func findBarrier(state pdgc.GCState, barrierID string) *pdgc.GCBarrierInfo {
	for _, b := range state.GCBarriers {
		if b.BarrierID == barrierID {
			return b
		}
	}
	return nil
}

// requireBarrier asserts that a barrier exists with the expected TS.
func requireBarrier(t *testing.T, state pdgc.GCState, barrierID string, expectedTS uint64) {
	barrier := findBarrier(state, barrierID)
	require.NotNil(t, barrier, "barrier %q should exist", barrierID)
	require.Equal(t, expectedTS, barrier.BarrierTS, "barrier %q TS mismatch", barrierID)
}

// requireNoBarrier asserts that a barrier does not exist.
func requireNoBarrier(t *testing.T, state pdgc.GCState, barrierID string) {
	barrier := findBarrier(state, barrierID)
	require.Nil(t, barrier, "barrier %q should not exist", barrierID)
}

// ============================================================================
// Simple Mock Manager for Keeper Tests
// ============================================================================

// mockManager implements gc.Manager interface for keeper tests.
// This is a lightweight mock that doesn't depend on MockPD.
type mockManager struct {
	mu                sync.Mutex
	gcSafePoint       uint64
	gcSafePointErr    error
	setSafePointErr   error
	setSafePointCalls int
	deleteCalls       int
}

func newMockManager() *mockManager {
	return &mockManager{}
}

func (m *mockManager) GetGCSafePoint(ctx context.Context) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.gcSafePointErr != nil {
		return 0, m.gcSafePointErr
	}
	return m.gcSafePoint, nil
}

func (m *mockManager) SetServiceSafePoint(ctx context.Context, sp gc.BRServiceSafePoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.setSafePointCalls++
	return m.setSafePointErr
}

func (m *mockManager) DeleteServiceSafePoint(ctx context.Context, sp gc.BRServiceSafePoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleteCalls++
	return nil
}

func (m *mockManager) getSetSafePointCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.setSafePointCalls
}
