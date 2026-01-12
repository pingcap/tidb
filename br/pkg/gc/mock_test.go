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

// pdClientAdapter adapts MockPD to pd.Client interface for testing.
// This is a minimal adapter without call tracking - tests should use
// state verification via GetGCStatesClient().GetGCState() instead.
//
// Note: Only implements the 3 methods needed for GC tests:
//   - UpdateServiceGCSafePoint
//   - UpdateGCSafePoint
//   - GetGCStatesClient
// Other pd.Client methods will panic if called (via nil embedding).
type pdClientAdapter struct {
	pd.Client
	mockPD *unistoretikv.MockPD
}

func (p *pdClientAdapter) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	return p.mockPD.UpdateServiceGCSafePoint(ctx, serviceID, ttl, safePoint)
}

func (p *pdClientAdapter) UpdateGCSafePoint(ctx context.Context, safePoint uint64) (uint64, error) {
	return p.mockPD.UpdateGCSafePoint(ctx, safePoint)
}

func (p *pdClientAdapter) GetGCStatesClient(keyspaceID uint32) pdgc.GCStatesClient {
	return p.mockPD.GetGCStatesClient(keyspaceID)
}

// newTestMockPD creates a fully configured MockPD wrapped in a pd.Client adapter.
// Cleanup is automatically handled via t.Cleanup().
func newTestMockPD(t *testing.T) *pdClientAdapter {
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

	return &pdClientAdapter{mockPD: mockPD}
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
//
// Call Tracking (REQUIRED for keeper tests):
//   - setSafePointCalls: Tracks periodic refresh calls in keeper loop
//   - deleteCalls: Tracks deletion calls
//
// Design Note:
//   Keeper tests MUST use call counts to verify periodic behavior
//   (e.g., "does the keeper refresh every N seconds?").
//   This is different from manager tests which use state verification.
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
