// Copyright 2025 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	pd "github.com/tikv/pd/client"
)

// GCSafePointManager abstracts GC operations, supporting both global and keyspace-level GC.
type GCSafePointManager interface {
	// UpdateServiceSafePoint updates the service safe point.
	// For UnifiedGCManager: calls pd.Client.UpdateServiceGCSafePoint (deprecated API)
	// For KeyspaceGCManager: calls GCStatesClient.SetGCBarrier
	UpdateServiceSafePoint(ctx context.Context, sp BRServiceSafePoint) error

	// StartServiceSafePointKeeper starts a goroutine to periodically update the service safe point.
	// The keeper will run until the context is canceled.
	StartServiceSafePointKeeper(ctx context.Context, sp BRServiceSafePoint) error
}

func NewGCSafePointManager(pdClient pd.Client, storage kv.Storage) (GCSafePointManager, error) {
	keyspaceName := config.GetGlobalKeyspaceName()

	if keyspaceName == "" {
		return newUnifiedGCManager(pdClient), nil
	}

	codec := storage.GetCodec()
	keyspaceID := uint32(codec.GetKeyspaceID())

	return newKeyspaceGCManager(pdClient, keyspaceID)
}

// StartServiceSafePointKeeperWithStorage is the storage-aware wrapper for StartServiceSafePointKeeper.
// This is the new recommended function that should be used by all BR tasks.
func StartServiceSafePointKeeperWithStorage(
	ctx context.Context,
	pdClient pd.Client,
	storage kv.Storage,
	sp BRServiceSafePoint,
) error {
	mgr, err := NewGCSafePointManager(pdClient, storage)
	if err != nil {
		return errors.Trace(err)
	}
	return mgr.StartServiceSafePointKeeper(ctx, sp)
}

// UpdateServiceSafePointWithStorage is the storage-aware wrapper for UpdateServiceSafePoint.
// This is the new recommended function that should be used by all BR tasks.
func UpdateServiceSafePointWithStorage(
	ctx context.Context,
	pdClient pd.Client,
	storage kv.Storage,
	sp BRServiceSafePoint,
) error {
	mgr, err := NewGCSafePointManager(pdClient, storage)
	if err != nil {
		return errors.Trace(err)
	}
	return mgr.UpdateServiceSafePoint(ctx, sp)
}
