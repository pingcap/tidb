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
	// GetGCSafePoint returns the current GC safe point.
	GetGCSafePoint(ctx context.Context) (uint64, error)

	// SetServiceSafePoint sets the service safe point with TTL.
	// If TTL <= 0, it removes the service safe point.
	SetServiceSafePoint(ctx context.Context, sp BRServiceSafePoint) error

	// DeleteServiceSafePoint removes the service safe point.
	DeleteServiceSafePoint(ctx context.Context, id string) error
}

func NewGCSafePointManager(pdClient pd.Client, storage kv.Storage) (GCSafePointManager, error) {
	keyspaceName := config.GetGlobalKeyspaceName()

	if keyspaceName == "" {
		return newGlobalGCManager(pdClient), nil
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
	return StartServiceSafePointKeeper(ctx, sp, mgr)
}

// SetServiceSafePointWithStorage is the storage-aware wrapper for SetServiceSafePoint.
// This is the new recommended function that should be used by all BR tasks.
func SetServiceSafePointWithStorage(
	ctx context.Context,
	pdClient pd.Client,
	storage kv.Storage,
	sp BRServiceSafePoint,
) error {
	mgr, err := NewGCSafePointManager(pdClient, storage)
	if err != nil {
		return errors.Trace(err)
	}
	return mgr.SetServiceSafePoint(ctx, sp)
}
