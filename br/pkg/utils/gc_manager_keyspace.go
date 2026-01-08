// Copyright 2025 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/clients/gc"
	"go.uber.org/zap"
)

// keyspaceGCManager implements GCSafePointManager using the per-keyspace GC barrier mechanism.
// It uses the new pd.Client.GetGCStatesClient(keyspaceID).SetGCBarrier API.
type keyspaceGCManager struct {
	pdClient   pd.Client
	keyspaceID uint32
	gcClient   gc.GCStatesClient
}

// Ensure keyspaceGCManager implements GCSafePointManager interface.
var _ GCSafePointManager = (*keyspaceGCManager)(nil)

// newKeyspaceGCManager creates a new KeyspaceGCManager instance.
func newKeyspaceGCManager(pdClient pd.Client, keyspaceID uint32) (*keyspaceGCManager, error) {
	// Get keyspace-specific GC states client
	// KeyspaceID is bound to this client, all operations will automatically target this keyspace
	gcClient := pdClient.GetGCStatesClient(keyspaceID)

	return &keyspaceGCManager{
		pdClient:   pdClient,
		keyspaceID: keyspaceID,
		gcClient:   gcClient,
	}, nil
}

// UpdateServiceSafePoint updates the keyspace GC barrier using SetGCBarrier API.
// If sp.TTL is 0, it calls DeleteGCBarrier to remove the barrier.
func (m *keyspaceGCManager) UpdateServiceSafePoint(ctx context.Context, sp BRServiceSafePoint) error {
	log.Debug("update keyspace GC barrier",
		zap.Uint32("keyspaceID", m.keyspaceID),
		zap.Object("safePoint", sp))

	// Handle deletion case (TTL = 0)
	if sp.TTL <= 0 {
		// Delete the barrier
		_, err := m.gcClient.DeleteGCBarrier(ctx, sp.ID)
		if err != nil {
			return errors.Trace(err)
		}
		log.Debug("deleted keyspace GC barrier",
			zap.Uint32("keyspaceID", m.keyspaceID),
			zap.String("barrierID", sp.ID))
		return nil
	}

	// Convert TTL from int64 seconds to time.Duration
	ttlDuration := time.Duration(sp.TTL) * time.Second

	// Set or update the barrier
	// barrierTS = BackupTS - 1 (same as UpdateServiceGCSafePoint)
	barrierInfo, err := m.gcClient.SetGCBarrier(ctx, sp.ID, sp.BackupTS-1, ttlDuration)
	if err != nil {
		return errors.Trace(err)
	}

	log.Debug("set keyspace GC barrier",
		zap.Uint32("keyspaceID", m.keyspaceID),
		zap.String("barrierID", barrierInfo.BarrierID),
		zap.Uint64("barrierTS", barrierInfo.BarrierTS),
		zap.Duration("TTL", barrierInfo.TTL))

	return nil
}

// StartServiceSafePointKeeper starts a goroutine to periodically update the keyspace GC barrier.
// The keeper will run until the context is canceled.
// Barrier cleanup is handled by the caller setting TTL=0, similar to the unified GC manager.
func (m *keyspaceGCManager) StartServiceSafePointKeeper(ctx context.Context, sp BRServiceSafePoint) error {
	if sp.ID == "" || sp.TTL <= 0 {
		return errors.Annotatef(errors.New("invalid service safe point"), "invalid service safe point %v", sp)
	}

	// Check GC safe point first (reuse existing check)
	if err := CheckGCSafePoint(ctx, m.pdClient, sp.BackupTS); err != nil {
		return errors.Trace(err)
	}

	// Set initial barrier immediately to cover the gap between starting
	// update goroutine and updating the barrier.
	if err := m.UpdateServiceSafePoint(ctx, sp); err != nil {
		return errors.Trace(err)
	}

	// Calculate update interval (TTL / 3, same as existing keeper logic)
	updateGapTime := time.Duration(sp.TTL) * time.Second / preUpdateServiceSafePointFactor
	updateTick := time.NewTicker(updateGapTime)
	checkTick := time.NewTicker(checkGCSafePointGapTime)

	go func() {
		defer updateTick.Stop()
		defer checkTick.Stop()
		for {
			select {
			case <-ctx.Done():
				log.Debug("keyspace GC barrier keeper exited",
					zap.Uint32("keyspaceID", m.keyspaceID))
				return

			case <-updateTick.C:
				// Periodically refresh the barrier to extend its TTL
				if err := m.UpdateServiceSafePoint(ctx, sp); err != nil {
					log.Warn("failed to update keyspace GC barrier, backup may fail if gc triggered",
						zap.Uint32("keyspaceID", m.keyspaceID),
						zap.Error(err))
				}

			case <-checkTick.C:
				// Periodically check if GC has advanced past our backup TS
				// This is a safety mechanism - if this check fails, we panic to prevent data loss
				if err := CheckGCSafePoint(ctx, m.pdClient, sp.BackupTS); err != nil {
					log.Panic("cannot pass gc safe point check, aborting",
						zap.Uint32("keyspaceID", m.keyspaceID),
						zap.Error(err),
						zap.Object("safePoint", sp))
				}
			}
		}
	}()

	return nil
}
