// Copyright 2025 PingCAP, Inc. Licensed under Apache-2.0.

package gc

import (
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	pd "github.com/tikv/pd/client"
)

// NewManager creates a GC Manager based on the storage configuration.
// If keyspace is configured, it returns a keyspace-aware manager.
// Otherwise, it returns a global manager.
func NewManager(pdClient pd.Client, storage kv.Storage) (Manager, error) {
	keyspaceName := config.GetGlobalKeyspaceName()

	if keyspaceName == "" {
		return newGlobalManager(pdClient), nil
	}

	if storage == nil {
		panic("storage is required for keyspace mode")
	}

	codec := storage.GetCodec()
	keyspaceID := uint32(codec.GetKeyspaceID())

	return newKeyspaceManager(pdClient, keyspaceID)
}
