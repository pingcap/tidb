// Copyright 2025 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

// globalGCManager implements GCSafePointManager using the global GC safepoint mechanism.
// It uses the deprecated pd.Client.UpdateServiceGCSafePoint API for backward compatibility.
type globalGCManager struct {
	pdClient pd.Client
}

// Ensure globalGCManager implements GCSafePointManager interface.
var _ GCSafePointManager = (*globalGCManager)(nil)

// newGlobalGCManager creates a new GlobalGCManager instance.
func newGlobalGCManager(pdClient pd.Client) *globalGCManager {
	return &globalGCManager{
		pdClient: pdClient,
	}
}

// GetGCSafePoint returns the current GC safe point.
func (m *globalGCManager) GetGCSafePoint(ctx context.Context) (uint64, error) {
	safePoint, err := m.pdClient.UpdateGCSafePoint(ctx, 0)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return safePoint, nil
}

// SetServiceSafePoint sets the global service safe point using the deprecated API.
// This maintains backward compatibility with existing BR behavior.
func (m *globalGCManager) SetServiceSafePoint(ctx context.Context, sp BRServiceSafePoint) error {
	log.Debug("update PD safePoint limit with TTL", zap.Object("safePoint", sp))

	lastSafePoint, err := m.pdClient.UpdateServiceGCSafePoint(ctx, sp.ID, sp.TTL, sp.BackupTS-1)
	if lastSafePoint > sp.BackupTS-1 && sp.TTL > 0 {
		log.Warn("service GC safe point lost, we may fail to back up if GC lifetime isn't long enough",
			zap.Uint64("lastSafePoint", lastSafePoint),
			zap.Object("safePoint", sp),
		)
	}
	return errors.Trace(err)
}

// DeleteServiceSafePoint removes the service safe point by setting TTL to 0.
func (m *globalGCManager) DeleteServiceSafePoint(ctx context.Context, sp BRServiceSafePoint) error {
	// Setting TTL to 0 effectively removes the service safe point
	_, err := m.pdClient.UpdateServiceGCSafePoint(ctx, sp.ID, 0, 0)
	return errors.Trace(err)
}
