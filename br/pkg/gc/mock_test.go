// Copyright 2025 PingCAP, Inc. Licensed under Apache-2.0.

package gc_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/pingcap/badger"
	"github.com/pingcap/tidb/br/pkg/gc"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore/lockstore"
	unistoretikv "github.com/pingcap/tidb/pkg/store/mockstore/unistore/tikv"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore/tikv/mvcc"
	"github.com/stretchr/testify/require"
	tikv "github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	pdgc "github.com/tikv/pd/client/clients/gc"
)

// testKeyspaceID is a non-global keyspace ID used for testing.
const testKeyspaceID = tikv.KeyspaceID(100)

// ============================================================================
// Mock implementations
// ============================================================================

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

type mockPDClient struct {
	pd.Client
	mockPD *unistoretikv.MockPD
}

func (p *mockPDClient) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	return p.mockPD.UpdateServiceGCSafePoint(ctx, serviceID, ttl, safePoint)
}

func (p *mockPDClient) UpdateGCSafePoint(ctx context.Context, safePoint uint64) (uint64, error) {
	return p.mockPD.UpdateGCSafePoint(ctx, safePoint)
}

func (p *mockPDClient) GetGCStatesClient(keyspaceID uint32) pdgc.GCStatesClient {
	return p.mockPD.GetGCStatesClient(keyspaceID)
}

func (p *mockPDClient) GetGCInternalController(keyspaceID uint32) pdgc.InternalController {
	return p.mockPD.GetGCInternalController(keyspaceID)
}

// newTestMockPD creates a fully configured MockPD wrapped in a pd.Client adapter.
// Cleanup is automatically handled via t.Cleanup().
func newTestMockPD(t *testing.T) *mockPDClient {
	db, dbPath, logPath, err := createTestDB(t)
	require.NoError(t, err)

	dbBundle := &mvcc.DBBundle{
		DB:        db,
		LockStore: lockstore.NewMemStore(4096),
	}

	rm, err := unistoretikv.NewMockRegionManager(dbBundle, 1,
		unistoretikv.RegionOptions{
			StoreAddr:  "127.0.0.1:10086",
			PDAddr:     "127.0.0.1:2379",
			RegionSize: 96 * 1024 * 1024,
		})
	require.NoError(t, err)

	mockPD := unistoretikv.NewMockPD(rm)

	// Register cleanup
	t.Cleanup(func() {
		if rm != nil {
			_ = rm.Close()
		}
		if db != nil {
			_ = db.Close()
		}
		if dbPath != "" {
			_ = os.RemoveAll(dbPath)
		}
		if logPath != "" {
			_ = os.RemoveAll(logPath)
		}
	})

	return &mockPDClient{mockPD: mockPD}
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

// getState returns the GC state for the specified keyspace.
// Use tikv.NullspaceID for global mode.
func getState(t *testing.T, ctx context.Context, mockPD *mockPDClient, keyspaceID tikv.KeyspaceID) pdgc.GCState {
	state, err := mockPD.GetGCStatesClient(uint32(keyspaceID)).GetGCState(ctx)
	require.NoError(t, err)
	return state
}

// ============================================================================
// Mock Manager Wrapper for Keeper Tests
// ============================================================================

// mockManager wraps a real gc.Manager (backed by mockPD) with:
// - Call counting for verification
// - Error injection for negative testing
// This provides realistic PD interaction while maintaining test control.
type mockManager struct {
	gc.Manager
	mockPD *mockPDClient

	mu                sync.Mutex
	setSafePointCalls int

	// Error injection
	setSafePointErr error
	gcSafePointErr  error
}

func newMockManagerWrapper(t *testing.T, keyspaceID tikv.KeyspaceID) *mockManager {
	mockPD := newTestMockPD(t)
	mgr := gc.NewManager(mockPD, keyspaceID)
	return &mockManager{
		Manager: mgr,
		mockPD:  mockPD,
	}
}

func (m *mockManager) GetGCSafePoint(ctx context.Context) (uint64, error) {
	m.mu.Lock()
	err := m.gcSafePointErr
	m.mu.Unlock()
	if err != nil {
		return 0, err
	}
	return m.Manager.GetGCSafePoint(ctx)
}

func (m *mockManager) SetServiceSafePoint(ctx context.Context, sp gc.BRServiceSafePoint) error {
	m.mu.Lock()
	m.setSafePointCalls++
	err := m.setSafePointErr
	m.mu.Unlock()
	if err != nil {
		return err
	}
	return m.Manager.SetServiceSafePoint(ctx, sp)
}

func (m *mockManager) DeleteServiceSafePoint(ctx context.Context, sp gc.BRServiceSafePoint) error {
	return m.Manager.DeleteServiceSafePoint(ctx, sp)
}

func (m *mockManager) getSetSafePointCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.setSafePointCalls
}

// setGCSafePoint sets the GC safe point in mockPD for testing.
// This first advances txn safe point, then advances GC safe point.
func (m *mockManager) setGCSafePoint(ctx context.Context, keyspaceID tikv.KeyspaceID, ts uint64) error {
	ctl := m.mockPD.GetGCInternalController(uint32(keyspaceID))
	// First advance txn safe point (GC safe point cannot exceed txn safe point)
	if _, err := ctl.AdvanceTxnSafePoint(ctx, ts); err != nil {
		return err
	}
	// Then advance GC safe point
	if _, err := ctl.AdvanceGCSafePoint(ctx, ts); err != nil {
		return err
	}
	return nil
}
