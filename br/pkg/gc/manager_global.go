// Copyright 2025 PingCAP, Inc. Licensed under Apache-2.0.

package gc

import (
	"context"
	"os"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

// globalManager implements Manager using the global GC safepoint mechanism.
// It uses the deprecated pd.Client.UpdateServiceGCSafePoint API for backward compatibility.
type globalManager struct {
	pdClient pd.Client
}

// Ensure globalManager implements Manager interface.
var _ Manager = (*globalManager)(nil)

// newGlobalManager creates a new globalManager instance.
func newGlobalManager(pdClient pd.Client) *globalManager {
	return &globalManager{
		pdClient: pdClient,
	}
}

// GetGCSafePoint returns the current GC safe point.
func (m *globalManager) GetGCSafePoint(ctx context.Context) (uint64, error) {
	safePoint, err := m.pdClient.UpdateGCSafePoint(ctx, 0)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return safePoint, nil
}

// SetServiceSafePoint sets the global service safe point using the deprecated API.
// This maintains backward compatibility with existing BR behavior.
func (m *globalManager) SetServiceSafePoint(ctx context.Context, sp BRServiceSafePoint) error {
	log.Debug("update PD safePoint limit with TTL", zap.Object("safePoint", sp))

	lastSafePoint, err := m.pdClient.UpdateServiceGCSafePoint(ctx, sp.ID, sp.TTL, sp.BackupTS-1)
	if err == nil {
		// Integration tests use this to distinguish global vs keyspace GC protection.
		failpoint.Inject("hint-gc-global-set-safepoint", func(v failpoint.Value) {
			if sigFile, ok := v.(string); ok {
				// Write the service ID so the test can match PD output precisely.
				if writeErr := os.WriteFile(sigFile, []byte(sp.ID), 0o644); writeErr != nil {
					log.Warn("failed to write failpoint signal file", zap.Error(writeErr), zap.String("file", sigFile))
				}
			}
			// Provide a small observation window for test scripts.
			time.Sleep(3 * time.Second)
		})
	}
	if lastSafePoint > sp.BackupTS-1 && sp.TTL > 0 {
		log.Warn("service GC safe point lost, we may fail to back up if GC lifetime isn't long enough",
			zap.Uint64("lastSafePoint", lastSafePoint),
			zap.Object("safePoint", sp),
		)
	}
	return errors.Trace(err)
}

// DeleteServiceSafePoint removes the service safe point by setting TTL to 0.
func (m *globalManager) DeleteServiceSafePoint(ctx context.Context, sp BRServiceSafePoint) error {
	// Setting TTL to 0 effectively removes the service safe point
	_, err := m.pdClient.UpdateServiceGCSafePoint(ctx, sp.ID, 0, 0)
	if err == nil {
		failpoint.Inject("hint-gc-global-delete-safepoint", func(v failpoint.Value) {
			if sigFile, ok := v.(string); ok {
				if writeErr := os.WriteFile(sigFile, []byte(sp.ID), 0o644); writeErr != nil {
					log.Warn("failed to write failpoint signal file", zap.Error(writeErr), zap.String("file", sigFile))
				}
			}
		})
	}
	return errors.Trace(err)
}
