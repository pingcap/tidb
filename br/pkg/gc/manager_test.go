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
	"github.com/pingcap/tidb/pkg/config"
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
type mockGCStatesClientWithTracking struct {
	inner                pdgc.GCStatesClient
	mu                   sync.Mutex
	setGCBarrierCalls    int
	deleteGCBarrierCalls int
	lastBarrierID        string
	lastBarrierTS        uint64
	lastTTL              time.Duration
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
	m.lastBarrierID = barrierID
	m.lastBarrierTS = barrierTS
	m.lastTTL = ttl
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
type mockPDClientWithGCStates struct {
	pd.Client
	mu                      sync.Mutex
	mockPD                  *unistoretikv.MockPD
	gcStatesClients         map[uint32]*mockGCStatesClientWithTracking
	updateServiceCalls      int
	lastServiceID           string
	lastTTL                 int64
	lastSafePoint           uint64
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
	m.lastServiceID = serviceID
	m.lastTTL = ttl
	m.lastSafePoint = safePoint
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
// Test Cases
// ============================================================================

func TestNewManager_WithoutKeyspace(t *testing.T) {
	// Setup: no keyspace name configured
	originalCfg := *config.GetGlobalConfig()
	defer config.StoreGlobalConfig(&originalCfg)

	newCfg := originalCfg
	newCfg.KeyspaceName = ""
	config.StoreGlobalConfig(&newCfg)

	pdClient := newMockPDClientWithGCStates(t)
	t.Cleanup(pdClient.Cleanup)
	storage := &mockStorage{
		codec: &mockCodec{keyspaceID: 0},
	}

	// Create GC manager
	mgr, err := gc.NewManager(pdClient, storage)
	require.NoError(t, err)
	require.NotNil(t, mgr)

	// Verify it's globalManager by checking it uses UpdateServiceGCSafePoint
	ctx := context.Background()
	sp := gc.BRServiceSafePoint{
		ID:       "test-service",
		TTL:      60,
		BackupTS: 2334,
	}

	err = mgr.SetServiceSafePoint(ctx, sp)
	require.NoError(t, err)

	// Verify UpdateServiceGCSafePoint was called
	require.Equal(t, 1, pdClient.updateServiceCalls)
	require.Equal(t, "test-service", pdClient.lastServiceID)
	require.Equal(t, int64(60), pdClient.lastTTL)
}

func TestNewManager_WithKeyspace(t *testing.T) {
	// Setup: keyspace name configured
	originalCfg := *config.GetGlobalConfig()
	defer config.StoreGlobalConfig(&originalCfg)

	newCfg := originalCfg
	newCfg.KeyspaceName = "test_keyspace"
	config.StoreGlobalConfig(&newCfg)

	pdClient := newMockPDClientWithGCStates(t)
	t.Cleanup(pdClient.Cleanup)
	keyspaceID := uint32(123)
	storage := &mockStorage{
		codec: &mockCodec{keyspaceID: tikv.KeyspaceID(keyspaceID)},
	}

	// Create GC manager
	mgr, err := gc.NewManager(pdClient, storage)
	require.NoError(t, err)
	require.NotNil(t, mgr)

	// Verify it's keyspaceManager by checking it uses SetGCBarrier
	ctx := context.Background()
	sp := gc.BRServiceSafePoint{
		ID:       "test-service",
		TTL:      60,
		BackupTS: 2335,
	}

	err = mgr.SetServiceSafePoint(ctx, sp)
	require.NoError(t, err)

	// Verify SetGCBarrier was called (keyspace manager behavior)
	gcClient := pdClient.gcStatesClients[keyspaceID]
	require.NotNil(t, gcClient)
	require.Equal(t, 1, gcClient.setGCBarrierCalls)
	require.Equal(t, "test-service", gcClient.lastBarrierID)
	require.Equal(t, uint64(2334), gcClient.lastBarrierTS) // BackupTS - 1
	require.Equal(t, time.Duration(60)*time.Second, gcClient.lastTTL)

	// Verify UpdateServiceGCSafePoint was NOT called
	require.Equal(t, 0, pdClient.updateServiceCalls)
}

func TestGlobalManager_SetServiceSafePoint(t *testing.T) {
	// Setup: no keyspace
	originalCfg := *config.GetGlobalConfig()
	defer config.StoreGlobalConfig(&originalCfg)

	newCfg := originalCfg
	newCfg.KeyspaceName = ""
	config.StoreGlobalConfig(&newCfg)

	pdClient := newMockPDClientWithGCStates(t)
	t.Cleanup(pdClient.Cleanup)
	storage := &mockStorage{
		codec: &mockCodec{keyspaceID: 0},
	}

	mgr, err := gc.NewManager(pdClient, storage)
	require.NoError(t, err)

	ctx := context.Background()

	// Test 1: Set safepoint (TTL > 0)
	sp := gc.BRServiceSafePoint{
		ID:       "br-test",
		TTL:      300,
		BackupTS: 2334,
	}

	err = mgr.SetServiceSafePoint(ctx, sp)
	require.NoError(t, err)
	require.Equal(t, 1, pdClient.updateServiceCalls)
	require.Equal(t, "br-test", pdClient.lastServiceID)
	require.Equal(t, int64(300), pdClient.lastTTL)
	require.Equal(t, sp.BackupTS-1, pdClient.lastSafePoint)

	// Test 2: Delete safepoint (TTL = 0)
	sp.TTL = 0
	err = mgr.SetServiceSafePoint(ctx, sp)
	require.NoError(t, err)
	require.Equal(t, 2, pdClient.updateServiceCalls)
	require.Equal(t, int64(0), pdClient.lastTTL)
}

func TestKeyspaceManager_SetServiceSafePoint(t *testing.T) {
	// Setup: with keyspace
	originalCfg := *config.GetGlobalConfig()
	defer config.StoreGlobalConfig(&originalCfg)

	newCfg := originalCfg
	newCfg.KeyspaceName = "test_ks"
	config.StoreGlobalConfig(&newCfg)

	pdClient := newMockPDClientWithGCStates(t)
	t.Cleanup(pdClient.Cleanup)
	keyspaceID := uint32(456)
	storage := &mockStorage{
		codec: &mockCodec{keyspaceID: tikv.KeyspaceID(keyspaceID)},
	}

	mgr, err := gc.NewManager(pdClient, storage)
	require.NoError(t, err)

	ctx := context.Background()
	gcClient := pdClient.gcStatesClients[keyspaceID]

	// Test 1: Set barrier (TTL > 0)
	sp := gc.BRServiceSafePoint{
		ID:       "br-barrier",
		TTL:      600,
		BackupTS: 3000,
	}

	err = mgr.SetServiceSafePoint(ctx, sp)
	require.NoError(t, err)
	require.Equal(t, 1, gcClient.setGCBarrierCalls)
	require.Equal(t, "br-barrier", gcClient.lastBarrierID)
	require.Equal(t, uint64(2999), gcClient.lastBarrierTS) // BackupTS - 1
	require.Equal(t, time.Duration(600)*time.Second, gcClient.lastTTL)

	// Verify barrier was stored using GetGCState
	state, err := gcClient.GetGCState(ctx)
	require.NoError(t, err)
	require.Len(t, state.GCBarriers, 1)
	require.Equal(t, "br-barrier", state.GCBarriers[0].BarrierID)
	require.Equal(t, uint64(2999), state.GCBarriers[0].BarrierTS)

	// Test 2: Delete barrier (TTL = 0)
	sp.TTL = 0
	err = mgr.SetServiceSafePoint(ctx, sp)
	require.NoError(t, err)
	require.Equal(t, 1, gcClient.deleteGCBarrierCalls)

	// Verify barrier was deleted using GetGCState
	state, err = gcClient.GetGCState(ctx)
	require.NoError(t, err)
	require.Empty(t, state.GCBarriers)
}

func TestStartServiceSafePointKeeper_Global(t *testing.T) {
	// Setup: no keyspace
	originalCfg := *config.GetGlobalConfig()
	defer config.StoreGlobalConfig(&originalCfg)

	newCfg := originalCfg
	newCfg.KeyspaceName = ""
	config.StoreGlobalConfig(&newCfg)

	pdClient := newMockPDClientWithGCStates(t)
	t.Cleanup(pdClient.Cleanup)
	storage := &mockStorage{
		codec: &mockCodec{keyspaceID: 0},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sp := gc.BRServiceSafePoint{
		ID:       "br-keeper",
		TTL:      9, // Short TTL for faster test (update every TTL/3 = 3 seconds)
		BackupTS: 2400,
	}

	// Start keeper using the wrapper function
	err := gc.StartServiceSafePointKeeper(ctx, pdClient, storage, sp)
	require.NoError(t, err)

	// Verify initial call happened
	require.GreaterOrEqual(t, pdClient.updateServiceCalls, 1)

	// Wait for at least one periodic update (TTL/3 = 3 seconds)
	time.Sleep(4 * time.Second)

	// Should have at least 2 calls (initial + 1 periodic)
	require.GreaterOrEqual(t, pdClient.updateServiceCalls, 2)

	// Cancel context to stop keeper
	cancel()
	time.Sleep(100 * time.Millisecond) // Give goroutine time to exit
}

func TestStartServiceSafePointKeeper_Keyspace(t *testing.T) {
	// Setup: with keyspace
	originalCfg := *config.GetGlobalConfig()
	defer config.StoreGlobalConfig(&originalCfg)

	newCfg := originalCfg
	newCfg.KeyspaceName = "test_ks"
	config.StoreGlobalConfig(&newCfg)

	pdClient := newMockPDClientWithGCStates(t)
	t.Cleanup(pdClient.Cleanup)
	keyspaceID := uint32(789)
	storage := &mockStorage{
		codec: &mockCodec{keyspaceID: tikv.KeyspaceID(keyspaceID)},
	}

	mgr, err := gc.NewManager(pdClient, storage)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sp := gc.BRServiceSafePoint{
		ID:       "br-ks-keeper",
		TTL:      9, // Short TTL for faster test
		BackupTS: 2600,
	}

	// Start keeper using the wrapper function
	err = gc.StartServiceSafePointKeeper(ctx, pdClient, storage, sp)
	require.NoError(t, err)

	gcClient := pdClient.gcStatesClients[keyspaceID]
	require.NotNil(t, gcClient)

	// Verify initial SetGCBarrier call
	require.GreaterOrEqual(t, gcClient.setGCBarrierCalls, 1)
	require.Equal(t, "br-ks-keeper", gcClient.lastBarrierID)

	// Wait for at least one periodic update
	time.Sleep(4 * time.Second)

	// Should have at least 2 SetGCBarrier calls
	require.GreaterOrEqual(t, gcClient.setGCBarrierCalls, 2)

	// Cancel context to stop keeper
	cancel()
	time.Sleep(100 * time.Millisecond)

	// Verify DeleteGCBarrier was NOT called by keeper itself
	require.Equal(t, 0, gcClient.deleteGCBarrierCalls)

	// Simulate the cleanup that the caller would do
	sp.TTL = 0
	err = mgr.SetServiceSafePoint(context.Background(), sp)
	require.NoError(t, err)

	// Now DeleteGCBarrier should have been called
	require.Equal(t, 1, gcClient.deleteGCBarrierCalls)
}

func TestKeyspaceManager_ErrorHandling(t *testing.T) {
	// Setup: with keyspace
	originalCfg := *config.GetGlobalConfig()
	defer config.StoreGlobalConfig(&originalCfg)

	newCfg := originalCfg
	newCfg.KeyspaceName = "test_ks"
	config.StoreGlobalConfig(&newCfg)

	pdClient := newMockPDClientWithGCStates(t)
	t.Cleanup(pdClient.Cleanup)
	keyspaceID := uint32(999)
	storage := &mockStorage{
		codec: &mockCodec{keyspaceID: tikv.KeyspaceID(keyspaceID)},
	}

	mgr, err := gc.NewManager(pdClient, storage)
	require.NoError(t, err)

	ctx := context.Background()

	// Test 1: SetGCBarrier with invalid parameters (validated by real implementation)
	// First advance txn safe point to test barrier behind txn safe point error
	gcController := pdClient.mockPD.GetGCInternalController(keyspaceID)
	_, err = gcController.AdvanceTxnSafePoint(ctx, 5000)
	require.NoError(t, err)

	// Now try to set barrier behind txn safe point (should fail)
	sp := gc.BRServiceSafePoint{
		ID:       "br-error",
		TTL:      60,
		BackupTS: 4000, // Behind txn safe point 5000
	}

	err = mgr.SetServiceSafePoint(ctx, sp)
	require.Error(t, err)
	require.Contains(t, err.Error(), "behind")

	// Test 2: SetGCBarrier with valid parameters (should succeed)
	sp2 := gc.BRServiceSafePoint{
		ID:       "br-valid",
		TTL:      60,
		BackupTS: 6000, // Ahead of txn safe point
	}
	err = mgr.SetServiceSafePoint(ctx, sp2)
	require.NoError(t, err)

	// Test 3: Delete barrier (should succeed)
	sp2.TTL = 0
	err = mgr.SetServiceSafePoint(ctx, sp2)
	require.NoError(t, err)

	// Test 4: Delete non-existent barrier (should return nil/no error)
	sp3 := gc.BRServiceSafePoint{
		ID:       "non-existent",
		TTL:      0,
		BackupTS: 7000,
	}
	err = mgr.SetServiceSafePoint(ctx, sp3)
	require.NoError(t, err)
}

func TestAPIWrappers(t *testing.T) {
	originalCfg := *config.GetGlobalConfig()
	defer config.StoreGlobalConfig(&originalCfg)

	// Test 1: Without keyspace - should use globalManager
	newCfg1 := originalCfg
	newCfg1.KeyspaceName = ""
	config.StoreGlobalConfig(&newCfg1)
	pdClient1 := newMockPDClientWithGCStates(t)
	t.Cleanup(pdClient1.Cleanup)
	storage1 := &mockStorage{
		codec: &mockCodec{keyspaceID: 0},
	}

	ctx := context.Background()
	sp := gc.BRServiceSafePoint{
		ID:       "test-wrapper",
		TTL:      120,
		BackupTS: 2800,
	}

	err := gc.SetServiceSafePoint(ctx, pdClient1, storage1, sp)
	require.NoError(t, err)
	require.Equal(t, 1, pdClient1.updateServiceCalls)

	// Test 2: With keyspace - should use keyspaceManager
	newCfg2 := originalCfg
	newCfg2.KeyspaceName = "test_ks"
	config.StoreGlobalConfig(&newCfg2)
	pdClient2 := newMockPDClientWithGCStates(t)
	t.Cleanup(pdClient2.Cleanup)
	keyspaceID := uint32(111)
	storage2 := &mockStorage{
		codec: &mockCodec{keyspaceID: tikv.KeyspaceID(keyspaceID)},
	}

	err = gc.SetServiceSafePoint(ctx, pdClient2, storage2, sp)
	require.NoError(t, err)

	gcClient := pdClient2.gcStatesClients[keyspaceID]
	require.NotNil(t, gcClient)
	require.Equal(t, 1, gcClient.setGCBarrierCalls)
	require.Equal(t, 0, pdClient2.updateServiceCalls) // Should NOT call global API

	// Test 3: StartServiceSafePointKeeper
	ctx3, cancel3 := context.WithCancel(context.Background())
	defer cancel3()

	err = gc.StartServiceSafePointKeeper(ctx3, pdClient2, storage2, sp)
	require.NoError(t, err)

	// Give keeper time to start
	time.Sleep(100 * time.Millisecond)
	require.GreaterOrEqual(t, gcClient.setGCBarrierCalls, 2) // Initial + at least one from wrapper call

	cancel3()
}
