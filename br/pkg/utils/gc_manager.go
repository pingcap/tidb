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
	DeleteServiceSafePoint(ctx context.Context, sp BRServiceSafePoint) error
}

func NewGCSafePointManager(pdClient pd.Client, storage kv.Storage) (GCSafePointManager, error) {
	keyspaceName := config.GetGlobalKeyspaceName()

	if keyspaceName == "" {
		return newGlobalGCManager(pdClient), nil
	}

	if storage == nil {
		panic("storage is required for keyspace mode")
	}

	codec := storage.GetCodec()
	keyspaceID := uint32(codec.GetKeyspaceID())

	return newKeyspaceGCManager(pdClient, keyspaceID)
}

// StartServiceSafePointKeeper starts a goroutine to periodically update the service safe point.
// It creates the appropriate GCSafePointManager based on the storage configuration.
func StartServiceSafePointKeeper(
	ctx context.Context,
	pdClient pd.Client,
	storage kv.Storage,
	sp BRServiceSafePoint,
) error {
	mgr, err := NewGCSafePointManager(pdClient, storage)
	if err != nil {
		return errors.Trace(err)
	}
	return StartServiceSafePointKeeperInner(ctx, sp, mgr)
}

// SetServiceSafePoint sets the service safe point with the appropriate GCSafePointManager.
func SetServiceSafePoint(
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

// DeleteServiceSafePoint removes the service safe point with the appropriate GCSafePointManager.
func DeleteServiceSafePoint(
	ctx context.Context,
	pdClient pd.Client,
	storage kv.Storage,
	sp BRServiceSafePoint,
) error {
	mgr, err := NewGCSafePointManager(pdClient, storage)
	if err != nil {
		return errors.Trace(err)
	}
	return mgr.DeleteServiceSafePoint(ctx, sp)
}

// The following functions use globalGCManager directly.
// They do NOT support keyspace and are used by operator/advancer which don't have access to storage.

// UpdateServiceSafePointGlobal updates the service safe point using globalGCManager.
// NOTE: This does NOT support keyspace. Use SetServiceSafePoint with storage for keyspace support.
func UpdateServiceSafePointGlobal(ctx context.Context, pdClient pd.Client, sp BRServiceSafePoint) error {
	mgr := newGlobalGCManager(pdClient)
	return mgr.SetServiceSafePoint(ctx, sp)
}

// StartServiceSafePointKeeperGlobal starts a keeper using globalGCManager.
// NOTE: This does NOT support keyspace. Use StartServiceSafePointKeeper with storage for keyspace support.
func StartServiceSafePointKeeperGlobal(ctx context.Context, pdClient pd.Client, sp BRServiceSafePoint) error {
	mgr := newGlobalGCManager(pdClient)
	return StartServiceSafePointKeeperInner(ctx, sp, mgr)
}
