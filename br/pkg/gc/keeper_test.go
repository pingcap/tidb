// Copyright 2025 PingCAP, Inc. Licensed under Apache-2.0.

package gc_test

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/gc"
	"github.com/stretchr/testify/require"
)

func TestStartKeeperWithManager(t *testing.T) {
	t.Run("Validation", func(t *testing.T) {
		t.Run("ValidParams", func(t *testing.T) {
			mgr := newMockManager()
			mgr.gcSafePoint = 100

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			sp := gc.BRServiceSafePoint{
				ID:       "br-test",
				TTL:      10,
				BackupTS: 1000,
			}
			err := gc.StartServiceSafePointKeeper(ctx, sp, mgr)
			require.NoError(t, err)

			// Verify initial SetServiceSafePoint was called
			require.Equal(t, 1, mgr.getSetSafePointCalls())
		})

		t.Run("EmptyID", func(t *testing.T) {
			mgr := newMockManager()
			mgr.gcSafePoint = 100

			ctx := context.Background()
			sp := gc.BRServiceSafePoint{
				ID:       "",
				TTL:      10,
				BackupTS: 1000,
			}
			err := gc.StartServiceSafePointKeeper(ctx, sp, mgr)
			require.Error(t, err)
			require.Contains(t, err.Error(), "invalid")
		})

		t.Run("ZeroTTL", func(t *testing.T) {
			mgr := newMockManager()
			mgr.gcSafePoint = 100

			ctx := context.Background()
			sp := gc.BRServiceSafePoint{
				ID:       "br-test",
				TTL:      0,
				BackupTS: 1000,
			}
			err := gc.StartServiceSafePointKeeper(ctx, sp, mgr)
			require.Error(t, err)
			require.Contains(t, err.Error(), "invalid")
		})

		t.Run("NegativeTTL", func(t *testing.T) {
			mgr := newMockManager()
			mgr.gcSafePoint = 100

			ctx := context.Background()
			sp := gc.BRServiceSafePoint{
				ID:       "br-test",
				TTL:      -1,
				BackupTS: 1000,
			}
			err := gc.StartServiceSafePointKeeper(ctx, sp, mgr)
			require.Error(t, err)
			require.Contains(t, err.Error(), "invalid")
		})

		t.Run("BackupTS_BehindSafePoint", func(t *testing.T) {
			mgr := newMockManager()
			mgr.gcSafePoint = 1000 // GC safe point is 1000

			ctx := context.Background()
			sp := gc.BRServiceSafePoint{
				ID:       "br-test",
				TTL:      10,
				BackupTS: 500, // BackupTS is behind GC safe point
			}
			err := gc.StartServiceSafePointKeeper(ctx, sp, mgr)
			require.Error(t, err)
			require.Contains(t, err.Error(), "exceed")
		})

		t.Run("BackupTS_EqualsSafePoint", func(t *testing.T) {
			mgr := newMockManager()
			mgr.gcSafePoint = 1000

			ctx := context.Background()
			sp := gc.BRServiceSafePoint{
				ID:       "br-test",
				TTL:      10,
				BackupTS: 1000, // BackupTS equals GC safe point
			}
			err := gc.StartServiceSafePointKeeper(ctx, sp, mgr)
			require.Error(t, err)
			require.Contains(t, err.Error(), "exceed")
		})
	})

	t.Run("Behavior", func(t *testing.T) {
		t.Run("InitialSetCalled", func(t *testing.T) {
			mgr := newMockManager()
			mgr.gcSafePoint = 100

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			sp := gc.BRServiceSafePoint{
				ID:       "br-test",
				TTL:      10,
				BackupTS: 1000,
			}
			err := gc.StartServiceSafePointKeeper(ctx, sp, mgr)
			require.NoError(t, err)

			// SetServiceSafePoint should be called once immediately
			require.Equal(t, 1, mgr.getSetSafePointCalls())
		})

		t.Run("SetServiceSafePoint_Error_Propagated", func(t *testing.T) {
			mgr := newMockManager()
			mgr.gcSafePoint = 100
			mgr.setSafePointErr = context.DeadlineExceeded

			ctx := context.Background()
			sp := gc.BRServiceSafePoint{
				ID:       "br-test",
				TTL:      10,
				BackupTS: 1000,
			}
			err := gc.StartServiceSafePointKeeper(ctx, sp, mgr)
			require.Error(t, err)
			require.ErrorIs(t, err, context.DeadlineExceeded)
		})

		t.Run("PeriodicRefresh", func(t *testing.T) {
			if testing.Short() {
				t.Skip("skipping periodic test in short mode")
			}

			mgr := newMockManager()
			mgr.gcSafePoint = 100

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// TTL=6s means update interval = 2s (TTL/3)
			sp := gc.BRServiceSafePoint{
				ID:       "br-test",
				TTL:      6,
				BackupTS: 1000,
			}
			err := gc.StartServiceSafePointKeeper(ctx, sp, mgr)
			require.NoError(t, err)

			// Initial call
			require.Equal(t, 1, mgr.getSetSafePointCalls())

			// Wait for at least one periodic refresh (2s interval + buffer)
			time.Sleep(3 * time.Second)

			// Should have at least 2 calls (initial + 1 periodic)
			require.GreaterOrEqual(t, mgr.getSetSafePointCalls(), 2)
		})

		t.Run("ContextCancel_Exits", func(t *testing.T) {
			mgr := newMockManager()
			mgr.gcSafePoint = 100

			ctx, cancel := context.WithCancel(context.Background())

			sp := gc.BRServiceSafePoint{
				ID:       "br-test",
				TTL:      300, // Long TTL = 100s interval
				BackupTS: 1000,
			}
			err := gc.StartServiceSafePointKeeper(ctx, sp, mgr)
			require.NoError(t, err)

			initialCalls := mgr.getSetSafePointCalls()
			require.Equal(t, 1, initialCalls)

			// Cancel context
			cancel()

			// Give goroutine time to exit
			time.Sleep(100 * time.Millisecond)

			// No additional calls should happen after cancel
			// (The goroutine should have exited)
			finalCalls := mgr.getSetSafePointCalls()
			require.Equal(t, initialCalls, finalCalls)
		})

		t.Run("GetGCSafePoint_Error_Ignored", func(t *testing.T) {
			mgr := newMockManager()
			mgr.gcSafePoint = 100
			mgr.gcSafePointErr = context.DeadlineExceeded // Error when getting GC safe point

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			sp := gc.BRServiceSafePoint{
				ID:       "br-test",
				TTL:      10,
				BackupTS: 1000,
			}
			// Should NOT return error because GetGCSafePoint error is ignored
			// in CheckGCSafePoint (returns nil on error)
			err := gc.StartServiceSafePointKeeper(ctx, sp, mgr)
			require.NoError(t, err)
		})
	})
}

func TestCheckGCSafePoint(t *testing.T) {
	t.Run("TS_GreaterThan_SafePoint", func(t *testing.T) {
		mgr := newMockManager()
		mgr.gcSafePoint = 100

		err := gc.CheckGCSafePoint(context.Background(), mgr, 200)
		require.NoError(t, err)
	})

	t.Run("TS_Equals_SafePoint", func(t *testing.T) {
		mgr := newMockManager()
		mgr.gcSafePoint = 100

		err := gc.CheckGCSafePoint(context.Background(), mgr, 100)
		require.Error(t, err)
		require.Contains(t, err.Error(), "exceed")
	})

	t.Run("TS_LessThan_SafePoint", func(t *testing.T) {
		mgr := newMockManager()
		mgr.gcSafePoint = 100

		err := gc.CheckGCSafePoint(context.Background(), mgr, 50)
		require.Error(t, err)
		require.Contains(t, err.Error(), "exceed")
	})

	t.Run("GetGCSafePoint_Error_Ignored", func(t *testing.T) {
		mgr := newMockManager()
		mgr.gcSafePointErr = context.DeadlineExceeded

		// Error should be ignored, return nil
		err := gc.CheckGCSafePoint(context.Background(), mgr, 100)
		require.NoError(t, err)
	})
}
