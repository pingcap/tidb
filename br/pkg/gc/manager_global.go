// Copyright 2025 PingCAP, Inc. Licensed under Apache-2.0.

package gc

import (
	"context"

	"github.com/pingcap/errors"
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
	return errors.Trace(err)
}

// UpdateServiceSafePointGlobal updates the service safe point using globalManager.
// NOTE: This does NOT support keyspace. Use SetServiceSafePoint with storage for keyspace support.
func UpdateServiceSafePointGlobal(ctx context.Context, pdClient pd.Client, sp BRServiceSafePoint) error {
	mgr := newGlobalManager(pdClient)
	return mgr.SetServiceSafePoint(ctx, sp)
}

// StartServiceSafePointKeeperGlobal starts a keeper using globalManager.
// NOTE: This does NOT support keyspace. Use StartServiceSafePointKeeper with storage for keyspace support.
func StartServiceSafePointKeeperGlobal(ctx context.Context, pdClient pd.Client, sp BRServiceSafePoint) error {
	mgr := newGlobalManager(pdClient)
	return StartKeeperWithManager(ctx, sp, mgr)
}
