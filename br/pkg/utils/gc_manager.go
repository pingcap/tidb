// Copyright 2025 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
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

// CheckGCSafePoint checks whether the ts is older than GC safepoint.
// Note: It ignores errors other than exceed GC safepoint.
func CheckGCSafePoint(ctx context.Context, pdClient pd.Client, ts uint64) error {
	// TODO: use PDClient.GetGCSafePoint instead once PD client exports it.
	safePoint, err := getGCSafePoint(ctx, pdClient)
	if err != nil {
		log.Warn("fail to get GC safe point", zap.Error(err))
		return nil
	}
	if ts <= safePoint {
		return errors.Annotatef(berrors.ErrBackupGCSafepointExceeded, "GC safepoint %d exceed TS %d", safePoint, ts)
	}
	return nil
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
