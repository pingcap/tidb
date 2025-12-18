package core

import (
	"sync"

	"github.com/pingcap/tidb/pkg/domain"
)

func init() {
	// Set the callback to allow domain package to set the global location resolver
	domain.SetGlobalLocationResolver = func(resolver any) {
		if lr, ok := resolver.(LocationResolver); ok {
			SetLocationResolver(lr)
		}
	}
}

// LocationResolver is the interface for resolving the store address for a given table/partition.
// This is used to determine which TiDB node should execute a query based on data location.
type LocationResolver interface {
	// ResolveTableLocation returns the store address for a given table.
	// tableID: the ID of the table
	// Returns the store address (e.g., "192.168.1.100:10080") or empty string if unknown.
	ResolveTableLocation(tableID int64) string

	// ResolvePartitionLocation returns the store address for a given partition.
	// tableID: the ID of the table
	// partitionID: the ID of the partition (0 for non-partitioned tables)
	// Returns the store address (e.g., "192.168.1.100:10080") or empty string if unknown.
	ResolvePartitionLocation(tableID int64, partitionID int64) string

	// ResolveKeyLocation returns the store address for a specific key.
	// This is used for point queries where we know the exact key.
	// tableID: the ID of the table
	// partitionID: the ID of the partition (0 for non-partitioned tables)
	// key: the encoded key bytes
	// Returns the store address or empty string if unknown.
	ResolveKeyLocation(tableID int64, partitionID int64, key []byte) string

	// IsLocalStore checks if the given store address is the local TiDB node.
	IsLocalStore(storeAddr string) bool

	// GetLocalStoreAddr returns the address of the local TiDB node.
	GetLocalStoreAddr() string
}

// Global location resolver instance
var (
	globalLocationResolver   LocationResolver
	globalLocationResolverMu sync.RWMutex
)

// GetLocationResolver returns the global location resolver instance.
// Returns nil if no resolver has been set.
func GetLocationResolver() LocationResolver {
	globalLocationResolverMu.RLock()
	defer globalLocationResolverMu.RUnlock()
	return globalLocationResolver
}

// SetLocationResolver sets the global location resolver instance.
// This should be called during initialization before any queries are executed.
func SetLocationResolver(resolver LocationResolver) {
	globalLocationResolverMu.Lock()
	defer globalLocationResolverMu.Unlock()
	globalLocationResolver = resolver
}
