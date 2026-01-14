// Copyright 2025 PingCAP, Inc. Licensed under Apache-2.0.

package gc_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/br/pkg/gc"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/stretchr/testify/require"
	tikv "github.com/tikv/client-go/v2/tikv"
)

// ============================================================================
// Test Helper Functions
// ============================================================================

// withKeyspaceConfig temporarily sets keyspace config.
// Cleanup is automatically registered via t.Cleanup.
func withKeyspaceConfig(t *testing.T, keyspaceName string) {
	originalCfg := *config.GetGlobalConfig()
	newCfg := originalCfg
	newCfg.KeyspaceName = keyspaceName
	config.StoreGlobalConfig(&newCfg)
	t.Cleanup(func() {
		config.StoreGlobalConfig(&originalCfg)
	})
}

func TestNewManager(t *testing.T) {
	t.Run("GlobalMode", func(t *testing.T) {
		// Set keyspaceName = "" (global mode)
		withKeyspaceConfig(t, "")

		mockPD := newTestMockPD(t)
		mgr := gc.NewManager(mockPD, tikv.NullspaceID)
		require.NotNil(t, mgr)

		ctx := context.Background()
		sp := gc.BRServiceSafePoint{
			ID:       "br-test-global",
			TTL:      300,
			BackupTS: 1000,
		}
		err := mgr.SetServiceSafePoint(ctx, sp)
		require.NoError(t, err)

		// Verify barrier exists in global state
		requireBarrier(t, getState(t, ctx, mockPD, tikv.NullspaceID), sp.ID, sp.BackupTS-1)
		// Verify barrier does NOT exist in keyspace state
		requireNoBarrier(t, getState(t, ctx, mockPD, testKeyspaceID), sp.ID)
	})

	t.Run("KeyspaceMode", func(t *testing.T) {
		// Set keyspaceName = "test_keyspace" (keyspace mode)
		withKeyspaceConfig(t, "test_keyspace")

		mockPD := newTestMockPD(t)
		mgr := gc.NewManager(mockPD, testKeyspaceID)
		require.NotNil(t, mgr)

		ctx := context.Background()
		sp := gc.BRServiceSafePoint{
			ID:       "br-test-keyspace",
			TTL:      300,
			BackupTS: 1000,
		}
		err := mgr.SetServiceSafePoint(ctx, sp)
		require.NoError(t, err)

		// Verify barrier exists in keyspace state
		requireBarrier(t, getState(t, ctx, mockPD, testKeyspaceID), sp.ID, sp.BackupTS-1)
		// Verify barrier does NOT exist in global state
		requireNoBarrier(t, getState(t, ctx, mockPD, tikv.NullspaceID), sp.ID)
	})
}

func TestGlobalManager(t *testing.T) {
	t.Run("SetServiceSafePoint", func(t *testing.T) {
		withKeyspaceConfig(t, "")

		mockPD := newTestMockPD(t)
		mgr := gc.NewManager(mockPD, tikv.NullspaceID)
		require.NotNil(t, mgr)

		ctx := context.Background()
		sp := gc.BRServiceSafePoint{
			ID:       "br-test",
			TTL:      300,
			BackupTS: 1000,
		}

		err := mgr.SetServiceSafePoint(ctx, sp)
		require.NoError(t, err)

		// Verify barrier exists in global state
		requireBarrier(t, getState(t, ctx, mockPD, tikv.NullspaceID), sp.ID, sp.BackupTS-1)
		// Verify barrier does NOT exist in keyspace state
		requireNoBarrier(t, getState(t, ctx, mockPD, testKeyspaceID), sp.ID)
	})

	t.Run("DeleteServiceSafePoint", func(t *testing.T) {
		withKeyspaceConfig(t, "")

		mockPD := newTestMockPD(t)
		mgr := gc.NewManager(mockPD, tikv.NullspaceID)
		require.NotNil(t, mgr)

		ctx := context.Background()
		sp := gc.BRServiceSafePoint{
			ID:       "br-test",
			TTL:      300,
			BackupTS: 1000,
		}

		// First set the safe point
		err := mgr.SetServiceSafePoint(ctx, sp)
		require.NoError(t, err)

		// Verify barrier exists in global state
		requireBarrier(t, getState(t, ctx, mockPD, tikv.NullspaceID), sp.ID, sp.BackupTS-1)

		// Then delete it
		err = mgr.DeleteServiceSafePoint(ctx, sp)
		require.NoError(t, err)

		// Verify barrier no longer exists
		requireNoBarrier(t, getState(t, ctx, mockPD, tikv.NullspaceID), sp.ID)
	})

	t.Run("GetGCSafePoint", func(t *testing.T) {
		withKeyspaceConfig(t, "")

		mockPD := newTestMockPD(t)
		mgr := gc.NewManager(mockPD, tikv.NullspaceID)
		require.NotNil(t, mgr)

		ctx := context.Background()

		// Get the current GC safe point
		safePoint, err := mgr.GetGCSafePoint(ctx)
		require.NoError(t, err)
		// MockPD returns 0 as initial safe point
		require.Equal(t, uint64(0), safePoint)
	})
}

func TestKeyspaceManager(t *testing.T) {
	t.Run("SetGCBarrier", func(t *testing.T) {
		withKeyspaceConfig(t, "test_keyspace")

		mockPD := newTestMockPD(t)
		mgr := gc.NewManager(mockPD, testKeyspaceID)

		ctx := context.Background()
		sp := gc.BRServiceSafePoint{
			ID:       "br-test-barrier",
			TTL:      300,
			BackupTS: 1000,
		}

		err := mgr.SetServiceSafePoint(ctx, sp)
		require.NoError(t, err)

		// Verify barrier exists in keyspace state
		requireBarrier(t, getState(t, ctx, mockPD, testKeyspaceID), sp.ID, sp.BackupTS-1)
		// Verify barrier does NOT exist in global state
		requireNoBarrier(t, getState(t, ctx, mockPD, tikv.NullspaceID), sp.ID)
	})

	t.Run("SetGCBarrier_ZeroTTL_CallsDelete", func(t *testing.T) {
		withKeyspaceConfig(t, "test_keyspace")

		mockPD := newTestMockPD(t)
		mgr := gc.NewManager(mockPD, testKeyspaceID)

		ctx := context.Background()

		// First set a barrier
		sp := gc.BRServiceSafePoint{
			ID:       "br-test-delete",
			TTL:      300,
			BackupTS: 1000,
		}
		err := mgr.SetServiceSafePoint(ctx, sp)
		require.NoError(t, err)

		// Verify barrier exists in keyspace state
		requireBarrier(t, getState(t, ctx, mockPD, testKeyspaceID), sp.ID, sp.BackupTS-1)

		// Set with TTL=0, should delete
		sp.TTL = 0
		err = mgr.SetServiceSafePoint(ctx, sp)
		require.NoError(t, err)

		// Verify barrier was removed
		requireNoBarrier(t, getState(t, ctx, mockPD, testKeyspaceID), sp.ID)
	})

	t.Run("DeleteGCBarrier", func(t *testing.T) {
		withKeyspaceConfig(t, "test_keyspace")

		mockPD := newTestMockPD(t)
		mgr := gc.NewManager(mockPD, testKeyspaceID)

		ctx := context.Background()

		// First set a barrier
		sp := gc.BRServiceSafePoint{
			ID:       "br-test-to-delete",
			TTL:      300,
			BackupTS: 1000,
		}
		err := mgr.SetServiceSafePoint(ctx, sp)
		require.NoError(t, err)

		// Verify barrier exists in keyspace state
		requireBarrier(t, getState(t, ctx, mockPD, testKeyspaceID), sp.ID, sp.BackupTS-1)

		// Delete the barrier
		err = mgr.DeleteServiceSafePoint(ctx, sp)
		require.NoError(t, err)

		// Verify barrier no longer exists
		requireNoBarrier(t, getState(t, ctx, mockPD, testKeyspaceID), sp.ID)
	})

	t.Run("GetGCSafePoint", func(t *testing.T) {
		withKeyspaceConfig(t, "test_keyspace")

		mockPD := newTestMockPD(t)
		mgr := gc.NewManager(mockPD, testKeyspaceID)

		ctx := context.Background()

		// Get the current GC safe point for the keyspace
		safePoint, err := mgr.GetGCSafePoint(ctx)
		require.NoError(t, err)
		// MockPD returns 0 as initial safe point
		require.Equal(t, uint64(0), safePoint)
	})
}
