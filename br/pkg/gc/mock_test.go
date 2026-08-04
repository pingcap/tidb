// Copyright 2025 PingCAP, Inc. Licensed under Apache-2.0.

package gc_test

import (
	"context"
	"sync"
	"testing"

	"github.com/pingcap/tidb/br/pkg/gc"
	"github.com/stretchr/testify/require"
	tikv "github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
)

// testKeyspaceID is a non-global keyspace ID used for testing.
const testKeyspaceID = tikv.KeyspaceID(100)

type gcBarrierInfo struct {
	BarrierID string
	BarrierTS uint64
}

type gcState struct {
	GCSafePoint uint64
	GCBarriers  []gcBarrierInfo
}

type mockPDClient struct {
	pd.Client

	mu                sync.Mutex
	gcSafePoints      map[tikv.KeyspaceID]uint64
	serviceSafePoints map[tikv.KeyspaceID]map[string]gcBarrierInfo
}

func (p *mockPDClient) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	return p.updateServiceSafePoint(tikv.NullspaceID, serviceID, ttl, safePoint), nil
}

func (p *mockPDClient) UpdateGCSafePoint(ctx context.Context, safePoint uint64) (uint64, error) {
	return p.updateGCSafePoint(tikv.NullspaceID, safePoint), nil
}

func (p *mockPDClient) UpdateServiceSafePointV2(
	ctx context.Context,
	keyspaceID uint32,
	serviceID string,
	ttl int64,
	safePoint uint64,
) (uint64, error) {
	return p.updateServiceSafePoint(tikv.KeyspaceID(keyspaceID), serviceID, ttl, safePoint), nil
}

func (p *mockPDClient) UpdateGCSafePointV2(ctx context.Context, keyspaceID uint32, safePoint uint64) (uint64, error) {
	return p.updateGCSafePoint(tikv.KeyspaceID(keyspaceID), safePoint), nil
}

func newTestMockPD(t *testing.T) *mockPDClient {
	t.Helper()
	return &mockPDClient{
		gcSafePoints:      make(map[tikv.KeyspaceID]uint64),
		serviceSafePoints: make(map[tikv.KeyspaceID]map[string]gcBarrierInfo),
	}
}

func (p *mockPDClient) updateGCSafePoint(keyspaceID tikv.KeyspaceID, safePoint uint64) uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	if safePoint > p.gcSafePoints[keyspaceID] {
		p.gcSafePoints[keyspaceID] = safePoint
	}
	return p.gcSafePoints[keyspaceID]
}

func (p *mockPDClient) updateServiceSafePoint(
	keyspaceID tikv.KeyspaceID,
	serviceID string,
	ttl int64,
	safePoint uint64,
) uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	serviceSafePoints := p.serviceSafePoints[keyspaceID]
	if serviceSafePoints == nil {
		serviceSafePoints = make(map[string]gcBarrierInfo)
		p.serviceSafePoints[keyspaceID] = serviceSafePoints
	}
	if ttl <= 0 {
		delete(serviceSafePoints, serviceID)
	} else {
		serviceSafePoints[serviceID] = gcBarrierInfo{BarrierID: serviceID, BarrierTS: safePoint}
	}

	var minSafePoint uint64
	for _, serviceSafePoint := range serviceSafePoints {
		if minSafePoint == 0 || serviceSafePoint.BarrierTS < minSafePoint {
			minSafePoint = serviceSafePoint.BarrierTS
		}
	}
	return minSafePoint
}

// ============================================================================
// State Query Helper Functions
// ============================================================================

// findBarrier finds a barrier by ID in the GC state.
// Returns nil if not found.
func findBarrier(state gcState, barrierID string) *gcBarrierInfo {
	for _, b := range state.GCBarriers {
		if b.BarrierID == barrierID {
			return &b
		}
	}
	return nil
}

// requireBarrier asserts that a barrier exists with the expected TS.
func requireBarrier(t *testing.T, state gcState, barrierID string, expectedTS uint64) {
	barrier := findBarrier(state, barrierID)
	require.NotNil(t, barrier, "barrier %q should exist", barrierID)
	require.Equal(t, expectedTS, barrier.BarrierTS, "barrier %q TS mismatch", barrierID)
}

// requireNoBarrier asserts that a barrier does not exist.
func requireNoBarrier(t *testing.T, state gcState, barrierID string) {
	barrier := findBarrier(state, barrierID)
	require.Nil(t, barrier, "barrier %q should not exist", barrierID)
}

// getState returns the GC state for the specified keyspace.
// Use tikv.NullspaceID for global mode.
func getState(ctx context.Context, t *testing.T, mockPD *mockPDClient, keyspaceID tikv.KeyspaceID) gcState {
	t.Helper()
	require.NoError(t, ctx.Err())

	mockPD.mu.Lock()
	defer mockPD.mu.Unlock()
	state := gcState{GCSafePoint: mockPD.gcSafePoints[keyspaceID]}
	for _, serviceSafePoint := range mockPD.serviceSafePoints[keyspaceID] {
		state.GCBarriers = append(state.GCBarriers, serviceSafePoint)
	}
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
	if keyspaceID == tikv.NullspaceID {
		_, err := m.mockPD.UpdateGCSafePoint(ctx, ts)
		return err
	}
	_, err := m.mockPD.UpdateGCSafePointV2(ctx, uint32(keyspaceID), ts)
	return err
}
