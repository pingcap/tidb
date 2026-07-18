// Copyright 2025 PingCAP, Inc. Licensed under Apache-2.0.

package gc_test

import (
	"context"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/gc"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	tikv "github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestMakeSafePointID(t *testing.T) {
	t.Run("Format", func(t *testing.T) {
		id := gc.MakeSafePointID()

		// Should match "br-{uuid}" pattern
		// UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
		pattern := `^br-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`
		matched, err := regexp.MatchString(pattern, id)
		require.NoError(t, err)
		require.True(t, matched, "ID %q should match pattern %q", id, pattern)
	})

	t.Run("Uniqueness", func(t *testing.T) {
		ids := make(map[string]bool)
		count := 100

		for i := 0; i < count; i++ {
			id := gc.MakeSafePointID()
			require.False(t, ids[id], "duplicate ID generated: %s", id)
			ids[id] = true
		}
	})

	t.Run("Concurrent_Uniqueness", func(t *testing.T) {
		var mu sync.Mutex
		ids := make(map[string]bool)
		count := 100

		var wg sync.WaitGroup
		for i := 0; i < count; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				id := gc.MakeSafePointID()
				mu.Lock()
				defer mu.Unlock()
				require.False(t, ids[id], "duplicate ID generated: %s", id)
				ids[id] = true
			}()
		}
		wg.Wait()
	})
}

func TestBRServiceSafePoint_MarshalLogObject(t *testing.T) {
	t.Run("NormalValues", func(t *testing.T) {
		sp := gc.BRServiceSafePoint{
			ID:       "br-test-id",
			TTL:      300,
			BackupTS: 1000,
		}

		core, logs := observer.New(zapcore.InfoLevel)
		logger := zap.New(core)

		logger.Info("test", zap.Object("safepoint", sp))

		require.Equal(t, 1, logs.Len())
		entry := logs.All()[0]

		// Check the context fields
		fields := entry.ContextMap()
		spFields, ok := fields["safepoint"].(map[string]any)
		require.True(t, ok, "safepoint field should be a map")

		require.Equal(t, "br-test-id", spFields["ID"])
		require.Equal(t, "5m0s", spFields["TTL"]) // 300 seconds = 5m0s
		require.Equal(t, uint64(1000), spFields["BackupTS"])
	})

	t.Run("ZeroValues", func(t *testing.T) {
		sp := gc.BRServiceSafePoint{
			ID:       "",
			TTL:      0,
			BackupTS: 0,
		}

		core, logs := observer.New(zapcore.InfoLevel)
		logger := zap.New(core)

		// Should not panic
		logger.Info("test", zap.Object("safepoint", sp))

		require.Equal(t, 1, logs.Len())
		entry := logs.All()[0]

		fields := entry.ContextMap()
		spFields, ok := fields["safepoint"].(map[string]any)
		require.True(t, ok, "safepoint field should be a map")

		require.Equal(t, "", spFields["ID"])
		require.Equal(t, "0s", spFields["TTL"])
		require.Equal(t, uint64(0), spFields["BackupTS"])
	})

	t.Run("LargeTTL", func(t *testing.T) {
		sp := gc.BRServiceSafePoint{
			ID:       "br-test",
			TTL:      86400, // 24 hours
			BackupTS: 1000,
		}

		core, logs := observer.New(zapcore.InfoLevel)
		logger := zap.New(core)

		logger.Info("test", zap.Object("safepoint", sp))

		require.Equal(t, 1, logs.Len())
		entry := logs.All()[0]

		fields := entry.ContextMap()
		spFields, ok := fields["safepoint"].(map[string]any)
		require.True(t, ok)

		require.Equal(t, "24h0m0s", spFields["TTL"])
	})
}

// ============================================================================
// SafepointKeeperSuite - parameterized tests for keyspace scope
// ============================================================================

// SafepointKeeperSuite tests safepoint keeper and GC safepoint operations.
// Each test method runs for both Global (NullspaceID) and Keyspace scopes.
type SafepointKeeperSuite struct {
	suite.Suite
	keyspaceID tikv.KeyspaceID
	mgr        *mockManager
	ctx        context.Context
	cancel     context.CancelFunc
}

// SetupTest runs before each test method.
func (s *SafepointKeeperSuite) SetupTest() {
	s.mgr = newMockManagerWrapper(s.T(), s.keyspaceID)
	s.ctx, s.cancel = context.WithCancel(context.Background())
}

// TearDownTest runs after each test method.
func (s *SafepointKeeperSuite) TearDownTest() {
	s.cancel()
}

// otherKeyspaceID returns the "other" keyspace for isolation checks.
func (s *SafepointKeeperSuite) otherKeyspaceID() tikv.KeyspaceID {
	if s.keyspaceID == tikv.NullspaceID {
		return testKeyspaceID
	}
	return tikv.NullspaceID
}

// requireBarrierIsolation verifies barrier exists in this keyspace and not in others.
func (s *SafepointKeeperSuite) requireBarrierIsolation(sp gc.BRServiceSafePoint) {
	requireBarrier(s.T(), getState(s.ctx, s.T(), s.mgr.mockPD, s.keyspaceID), sp.ID, sp.BackupTS-1)
	requireNoBarrier(s.T(), getState(s.ctx, s.T(), s.mgr.mockPD, s.otherKeyspaceID()), sp.ID)
}

// setGCSafePoint sets the GC safe point for the current keyspace.
func (s *SafepointKeeperSuite) setGCSafePoint(ts uint64) {
	s.Require().NoError(s.mgr.setGCSafePoint(s.ctx, s.keyspaceID, ts))
}

// ============================================================================
// Keeper Validation Tests
// ============================================================================

func (s *SafepointKeeperSuite) TestValidation_ValidParams() {
	sp := gc.BRServiceSafePoint{
		ID:       "br-test",
		TTL:      10,
		BackupTS: 1000,
	}
	err := gc.StartServiceSafePointKeeper(s.ctx, sp, s.mgr)
	s.Require().NoError(err)
	s.Require().Equal(1, s.mgr.getSetSafePointCalls())
	s.requireBarrierIsolation(sp)
}

func (s *SafepointKeeperSuite) TestValidation_EmptyID() {
	sp := gc.BRServiceSafePoint{
		ID:       "",
		TTL:      10,
		BackupTS: 1000,
	}
	err := gc.StartServiceSafePointKeeper(s.ctx, sp, s.mgr)
	s.Require().Error(err)
	s.Require().Contains(err.Error(), "invalid")
}

func (s *SafepointKeeperSuite) TestValidation_ZeroTTL() {
	sp := gc.BRServiceSafePoint{
		ID:       "br-test",
		TTL:      0,
		BackupTS: 1000,
	}
	err := gc.StartServiceSafePointKeeper(s.ctx, sp, s.mgr)
	s.Require().Error(err)
	s.Require().Contains(err.Error(), "invalid")
}

func (s *SafepointKeeperSuite) TestValidation_NegativeTTL() {
	sp := gc.BRServiceSafePoint{
		ID:       "br-test",
		TTL:      -1,
		BackupTS: 1000,
	}
	err := gc.StartServiceSafePointKeeper(s.ctx, sp, s.mgr)
	s.Require().Error(err)
	s.Require().Contains(err.Error(), "invalid")
}

func (s *SafepointKeeperSuite) TestValidation_BackupTSBehindSafePoint() {
	s.setGCSafePoint(1000)

	sp := gc.BRServiceSafePoint{
		ID:       "br-test",
		TTL:      10,
		BackupTS: 500, // BackupTS is behind GC safe point
	}
	err := gc.StartServiceSafePointKeeper(s.ctx, sp, s.mgr)
	s.Require().Error(err)
	s.Require().Contains(err.Error(), "exceed")
}

func (s *SafepointKeeperSuite) TestValidation_BackupTSEqualsSafePoint() {
	s.setGCSafePoint(1000)

	sp := gc.BRServiceSafePoint{
		ID:       "br-test",
		TTL:      10,
		BackupTS: 1000, // BackupTS equals GC safe point
	}
	err := gc.StartServiceSafePointKeeper(s.ctx, sp, s.mgr)
	s.Require().Error(err)
	s.Require().Contains(err.Error(), "exceed")
}

// ============================================================================
// Keeper Behavior Tests
// ============================================================================

func (s *SafepointKeeperSuite) TestBehavior_InitialSetCalled() {
	sp := gc.BRServiceSafePoint{
		ID:       "br-test",
		TTL:      10,
		BackupTS: 1000,
	}
	err := gc.StartServiceSafePointKeeper(s.ctx, sp, s.mgr)
	s.Require().NoError(err)
	s.Require().Equal(1, s.mgr.getSetSafePointCalls())
	s.requireBarrierIsolation(sp)
}

func (s *SafepointKeeperSuite) TestBehavior_SetServiceSafePointErrorPropagated() {
	s.mgr.setSafePointErr = context.DeadlineExceeded

	sp := gc.BRServiceSafePoint{
		ID:       "br-test",
		TTL:      10,
		BackupTS: 1000,
	}
	err := gc.StartServiceSafePointKeeper(s.ctx, sp, s.mgr)
	s.Require().Error(err)
	s.Require().ErrorIs(err, context.DeadlineExceeded)
}

func (s *SafepointKeeperSuite) TestBehavior_PeriodicRefresh() {
	if testing.Short() {
		s.T().Skip("skipping periodic test in short mode")
	}

	// TTL=3s means update interval = 1s (TTL/3)
	sp := gc.BRServiceSafePoint{
		ID:       "br-test",
		TTL:      3,
		BackupTS: 1000,
	}
	err := gc.StartServiceSafePointKeeper(s.ctx, sp, s.mgr)
	s.Require().NoError(err)
	s.Require().Equal(1, s.mgr.getSetSafePointCalls())
	s.requireBarrierIsolation(sp)

	// Wait for at least one periodic refresh (1s interval + buffer)
	s.Require().Eventually(func() bool {
		return s.mgr.getSetSafePointCalls() >= 2
	}, 2*time.Second, 25*time.Millisecond)

	// Verify barrier still exists after refresh
	s.requireBarrierIsolation(sp)
}

func (s *SafepointKeeperSuite) TestBehavior_ContextCancelExits() {
	sp := gc.BRServiceSafePoint{
		ID:       "br-test",
		TTL:      300, // Long TTL = 100s interval
		BackupTS: 1000,
	}
	err := gc.StartServiceSafePointKeeper(s.ctx, sp, s.mgr)
	s.Require().NoError(err)

	initialCalls := s.mgr.getSetSafePointCalls()
	s.Require().Equal(1, initialCalls)

	// Cancel context
	s.cancel()

	// Give goroutine time to exit
	time.Sleep(100 * time.Millisecond)

	// No additional calls should happen after cancel
	finalCalls := s.mgr.getSetSafePointCalls()
	s.Require().Equal(initialCalls, finalCalls)
}

func (s *SafepointKeeperSuite) TestBehavior_GetGCSafePointErrorIgnored() {
	s.mgr.gcSafePointErr = context.DeadlineExceeded

	sp := gc.BRServiceSafePoint{
		ID:       "br-test",
		TTL:      10,
		BackupTS: 1000,
	}
	// Should NOT return error because GetGCSafePoint error is ignored
	// in CheckGCSafePoint (returns nil on error)
	err := gc.StartServiceSafePointKeeper(s.ctx, sp, s.mgr)
	s.Require().NoError(err)
}

// ============================================================================
// CheckGCSafePoint Tests
// ============================================================================

func (s *SafepointKeeperSuite) TestCheckGCSafePoint_TSGreaterThanSafePoint() {
	s.setGCSafePoint(100)

	err := gc.CheckGCSafePoint(s.ctx, s.mgr, 200)
	s.Require().NoError(err)
}

func (s *SafepointKeeperSuite) TestCheckGCSafePoint_TSEqualsSafePoint() {
	s.setGCSafePoint(100)

	err := gc.CheckGCSafePoint(s.ctx, s.mgr, 100)
	s.Require().Error(err)
	s.Require().Contains(err.Error(), "exceed")
}

func (s *SafepointKeeperSuite) TestCheckGCSafePoint_TSLessThanSafePoint() {
	s.setGCSafePoint(100)

	err := gc.CheckGCSafePoint(s.ctx, s.mgr, 50)
	s.Require().Error(err)
	s.Require().Contains(err.Error(), "exceed")
}

func (s *SafepointKeeperSuite) TestCheckGCSafePoint_ErrorIgnored() {
	s.mgr.gcSafePointErr = context.DeadlineExceeded

	// Error should be ignored, return nil
	err := gc.CheckGCSafePoint(s.ctx, s.mgr, 100)
	s.Require().NoError(err)
}

// ============================================================================
// Suite Entry Points
// ============================================================================

func TestGlobalSafepointKeeper(t *testing.T) {
	suite.Run(t, &SafepointKeeperSuite{keyspaceID: tikv.NullspaceID})
}

func TestKeyspaceSafepointKeeper(t *testing.T) {
	suite.Run(t, &SafepointKeeperSuite{keyspaceID: testKeyspaceID})
}
