// Copyright 2025 PingCAP, Inc. Licensed under Apache-2.0.

package gc

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	pd "github.com/tikv/pd/client"
)

// StartServiceSafePointKeeper starts a goroutine to periodically update the service safe point.
// It creates the appropriate Manager based on the storage configuration.
func StartServiceSafePointKeeper(
	ctx context.Context,
	pdClient pd.Client,
	storage kv.Storage,
	sp BRServiceSafePoint,
) error {
	mgr, err := NewManager(pdClient, storage)
	if err != nil {
		return errors.Trace(err)
	}
	return StartKeeperWithManager(ctx, sp, mgr)
}

// SetServiceSafePoint sets the service safe point with the appropriate Manager.
func SetServiceSafePoint(
	ctx context.Context,
	pdClient pd.Client,
	storage kv.Storage,
	sp BRServiceSafePoint,
) error {
	mgr, err := NewManager(pdClient, storage)
	if err != nil {
		return errors.Trace(err)
	}
	return mgr.SetServiceSafePoint(ctx, sp)
}

// DeleteServiceSafePoint removes the service safe point with the appropriate Manager.
func DeleteServiceSafePoint(
	ctx context.Context,
	pdClient pd.Client,
	storage kv.Storage,
	sp BRServiceSafePoint,
) error {
	mgr, err := NewManager(pdClient, storage)
	if err != nil {
		return errors.Trace(err)
	}
	return mgr.DeleteServiceSafePoint(ctx, sp)
}

// CheckGCSafePoint checks whether the ts is older than GC safepoint.
// Note: It ignores errors other than exceed GC safepoint.
func CheckGCSafePoint(
	ctx context.Context,
	pdClient pd.Client,
	storage kv.Storage,
	ts uint64,
) error {
	mgr, err := NewManager(pdClient, storage)
	if err != nil {
		return errors.Trace(err)
	}
	return checkSafePointByManager(ctx, mgr, ts)
}

// CheckGCSafePointWithManager checks whether ts is older than GC safepoint using the provided Manager.
// Note: It ignores errors other than exceed GC safepoint, same as CheckGCSafePoint.
func CheckGCSafePointWithManager(ctx context.Context, mgr Manager, ts uint64) error {
	return checkSafePointByManager(ctx, mgr, ts)
}

// The following functions use globalManager directly.
// They do NOT support keyspace and are used by operator/advancer which don't have access to storage.

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
