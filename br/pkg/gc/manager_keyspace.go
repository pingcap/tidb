// Copyright 2025 PingCAP, Inc. Licensed under Apache-2.0.

package gc

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/clients/gc"
	"go.uber.org/zap"
)

// keyspaceManager implements Manager using the per-keyspace GC barrier mechanism.
// It uses the new pd.Client.GetGCStatesClient(keyspaceID).SetGCBarrier API.
type keyspaceManager struct {
	pdClient   pd.Client
	keyspaceID tikv.KeyspaceID
	gcClient   gc.GCStatesClient
}

// Ensure keyspaceManager implements Manager interface.
var _ Manager = (*keyspaceManager)(nil)

// newKeyspaceManager creates a new keyspaceManager instance.
func newKeyspaceManager(pdClient pd.Client, keyspaceID tikv.KeyspaceID) *keyspaceManager {
	// Get keyspace-specific GC states client
	// KeyspaceID is bound to this client, all operations will automatically target this keyspace
	gcClient := pdClient.GetGCStatesClient(uint32(keyspaceID))

	return &keyspaceManager{
		pdClient:   pdClient,
		keyspaceID: keyspaceID,
		gcClient:   gcClient,
	}
}

// GetGCSafePoint returns the current GC safe point for this keyspace.
func (m *keyspaceManager) GetGCSafePoint(ctx context.Context) (uint64, error) {
	state, err := m.gcClient.GetGCState(ctx)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return state.GCSafePoint, nil
}

// SetServiceSafePoint sets the keyspace GC barrier using SetGCBarrier API.
// If sp.TTL <= 0, it calls DeleteGCBarrier to remove the barrier (same as unified manager behavior).
func (m *keyspaceManager) SetServiceSafePoint(ctx context.Context, sp BRServiceSafePoint) error {
	log.Debug("set keyspace GC barrier",
		zap.Uint32("keyspaceID", uint32(m.keyspaceID)),
		zap.Object("safePoint", sp))

	// Handle deletion case (TTL <= 0), same as unified manager behavior
	if sp.TTL <= 0 {
		return m.DeleteServiceSafePoint(ctx, sp)
	}

	// Convert TTL from int64 seconds to time.Duration
	ttlDuration := time.Duration(sp.TTL) * time.Second

	// Set or update the barrier
	// barrierTS = BackupTS - 1 (same as UpdateServiceGCSafePoint)
	barrierInfo, err := m.gcClient.SetGCBarrier(ctx, sp.ID, sp.BackupTS-1, ttlDuration)
	if err != nil {
		return errors.Trace(err)
	}

	// Integration tests use this to distinguish global vs keyspace GC protection.
	failpoint.Inject("hint-gc-keyspace-set-barrier", func(v failpoint.Value) {
		if sigFile, ok := v.(string); ok {
			// Include keyspaceID so the test can sanity-check scope if needed.
			content := fmt.Sprintf("keyspace=%d\nid=%s\n", uint32(m.keyspaceID), sp.ID)
			if writeErr := os.WriteFile(sigFile, []byte(content), 0o644); writeErr != nil {
				log.Warn("failed to write failpoint signal file", zap.Error(writeErr), zap.String("file", sigFile))
			}
		}
		// Provide a small observation window for test scripts.
		time.Sleep(3 * time.Second)
	})

	log.Debug("set keyspace GC barrier succeeded",
		zap.Uint32("keyspaceID", uint32(m.keyspaceID)),
		zap.String("barrierID", barrierInfo.BarrierID),
		zap.Uint64("barrierTS", barrierInfo.BarrierTS),
		zap.Duration("TTL", barrierInfo.TTL))

	return nil
}

// DeleteServiceSafePoint removes the keyspace GC barrier.
func (m *keyspaceManager) DeleteServiceSafePoint(ctx context.Context, sp BRServiceSafePoint) error {
	_, err := m.gcClient.DeleteGCBarrier(ctx, sp.ID)
	if err != nil {
		return errors.Trace(err)
	}
	failpoint.Inject("hint-gc-keyspace-delete-barrier", func(v failpoint.Value) {
		if sigFile, ok := v.(string); ok {
			content := fmt.Sprintf("keyspace=%d\nid=%s\n", uint32(m.keyspaceID), sp.ID)
			if writeErr := os.WriteFile(sigFile, []byte(content), 0o644); writeErr != nil {
				log.Warn("failed to write failpoint signal file", zap.Error(writeErr), zap.String("file", sigFile))
			}
		}
	})
	log.Debug("deleted keyspace GC barrier",
		zap.Uint32("keyspaceID", uint32(m.keyspaceID)),
		zap.String("barrierID", sp.ID))
	return nil
}
