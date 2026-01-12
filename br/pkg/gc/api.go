// Copyright 2025 PingCAP, Inc. Licensed under Apache-2.0.

package gc

import (
	"context"

	pd "github.com/tikv/pd/client"
)

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
