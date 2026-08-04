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
	"go.uber.org/zap"
)

// keyspaceManager implements Manager using the per-keyspace GC safe point mechanism.
// release-8.5 exposes it through the V2 safe point APIs.
type keyspaceManager struct {
	pdClient   pd.Client
	keyspaceID tikv.KeyspaceID
}

// Ensure keyspaceManager implements Manager interface.
var _ Manager = (*keyspaceManager)(nil)

// newKeyspaceManager creates a new keyspaceManager instance.
func newKeyspaceManager(pdClient pd.Client, keyspaceID tikv.KeyspaceID) *keyspaceManager {
	return &keyspaceManager{
		pdClient:   pdClient,
		keyspaceID: keyspaceID,
	}
}

// GetGCSafePoint returns the current GC safe point for this keyspace.
func (m *keyspaceManager) GetGCSafePoint(ctx context.Context) (uint64, error) {
	safePoint, err := m.pdClient.UpdateGCSafePointV2(ctx, uint32(m.keyspaceID), 0)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return safePoint, nil
}

// SetServiceSafePoint sets the service safe point for this keyspace.
// If sp.TTL <= 0, it removes the service safe point.
func (m *keyspaceManager) SetServiceSafePoint(ctx context.Context, sp BRServiceSafePoint) error {
	log.Debug("set keyspace GC service safe point",
		zap.Uint32("keyspaceID", uint32(m.keyspaceID)),
		zap.Object("safePoint", sp))

	// Handle deletion case (TTL <= 0), same as unified manager behavior
	if sp.TTL <= 0 {
		return m.DeleteServiceSafePoint(ctx, sp)
	}

	lastSafePoint, err := m.pdClient.UpdateServiceSafePointV2(
		ctx, uint32(m.keyspaceID), sp.ID, sp.TTL, sp.BackupTS-1,
	)
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

	if lastSafePoint > sp.BackupTS-1 {
		log.Warn("service GC safe point lost, we may fail to back up if GC lifetime isn't long enough",
			zap.Uint64("lastSafePoint", lastSafePoint),
			zap.Object("safePoint", sp),
		)
	}

	log.Debug("set keyspace GC service safe point succeeded",
		zap.Uint32("keyspaceID", uint32(m.keyspaceID)),
		zap.String("serviceID", sp.ID),
		zap.Uint64("safePoint", sp.BackupTS-1),
		zap.Int64("TTL", sp.TTL))

	return nil
}

// DeleteServiceSafePoint removes the service safe point from this keyspace.
func (m *keyspaceManager) DeleteServiceSafePoint(ctx context.Context, sp BRServiceSafePoint) error {
	_, err := m.pdClient.UpdateServiceSafePointV2(ctx, uint32(m.keyspaceID), sp.ID, 0, 0)
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
	log.Debug("deleted keyspace GC service safe point",
		zap.Uint32("keyspaceID", uint32(m.keyspaceID)),
		zap.String("serviceID", sp.ID))
	return nil
}
