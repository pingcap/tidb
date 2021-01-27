package config

import (
	"fmt"
	"time"

	"google.golang.org/grpc/encoding/gzip"
)

const (
	// DefStoreLivenessTimeout is the default value for store liveness timeout.
	DefStoreLivenessTimeout = "5s"
)

// TiKVClient is the config for tikv client.
type TiKVClient struct {
	// GrpcConnectionCount is the max gRPC connections that will be established
	// with each tikv-server.
	GrpcConnectionCount uint `toml:"grpc-connection-count" json:"grpc-connection-count"`
	// After a duration of this time in seconds if the client doesn't see any activity it pings
	// the server to see if the transport is still alive.
	GrpcKeepAliveTime uint `toml:"grpc-keepalive-time" json:"grpc-keepalive-time"`
	// After having pinged for keepalive check, the client waits for a duration of Timeout in seconds
	// and if no activity is seen even after that the connection is closed.
	GrpcKeepAliveTimeout uint `toml:"grpc-keepalive-timeout" json:"grpc-keepalive-timeout"`
	// GrpcCompressionType is the compression type for gRPC channel: none or gzip.
	GrpcCompressionType string `toml:"grpc-compression-type" json:"grpc-compression-type"`
	// CommitTimeout is the max time which command 'commit' will wait.
	CommitTimeout string      `toml:"commit-timeout" json:"commit-timeout"`
	AsyncCommit   AsyncCommit `toml:"async-commit" json:"async-commit"`
	// MaxBatchSize is the max batch size when calling batch commands API.
	MaxBatchSize uint `toml:"max-batch-size" json:"max-batch-size"`
	// If TiKV load is greater than this, TiDB will wait for a while to avoid little batch.
	OverloadThreshold uint `toml:"overload-threshold" json:"overload-threshold"`
	// MaxBatchWaitTime in nanosecond is the max wait time for batch.
	MaxBatchWaitTime time.Duration `toml:"max-batch-wait-time" json:"max-batch-wait-time"`
	// BatchWaitSize is the max wait size for batch.
	BatchWaitSize uint `toml:"batch-wait-size" json:"batch-wait-size"`
	// EnableChunkRPC indicate the data encode in chunk format for coprocessor requests.
	EnableChunkRPC bool `toml:"enable-chunk-rpc" json:"enable-chunk-rpc"`
	// If a Region has not been accessed for more than the given duration (in seconds), it
	// will be reloaded from the PD.
	RegionCacheTTL uint `toml:"region-cache-ttl" json:"region-cache-ttl"`
	// If a store has been up to the limit, it will return error for successive request to
	// prevent the store occupying too much token in dispatching level.
	StoreLimit int64 `toml:"store-limit" json:"store-limit"`
	// StoreLivenessTimeout is the timeout for store liveness check request.
	StoreLivenessTimeout string           `toml:"store-liveness-timeout" json:"store-liveness-timeout"`
	CoprCache            CoprocessorCache `toml:"copr-cache" json:"copr-cache"`
	// TTLRefreshedTxnSize controls whether a transaction should update its TTL or not.
	TTLRefreshedTxnSize int64 `toml:"ttl-refreshed-txn-size" json:"ttl-refreshed-txn-size"`
}

// AsyncCommit is the config for the async commit feature. The switch to enable it is a system variable.
type AsyncCommit struct {
	// Use async commit only if the number of keys does not exceed KeysLimit.
	KeysLimit uint `toml:"keys-limit" json:"keys-limit"`
	// Use async commit only if the total size of keys does not exceed TotalKeySizeLimit.
	TotalKeySizeLimit uint64 `toml:"total-key-size-limit" json:"total-key-size-limit"`
	// The following two fields should never be modified by the user, so tags are not provided
	// on purpose.
	// The duration within which is safe for async commit or 1PC to commit with an old schema.
	// It is only changed in tests.
	// TODO: 1PC is not part of async commit. These two fields should be moved to a more suitable
	// place.
	SafeWindow time.Duration
	// The duration in addition to SafeWindow to make DDL safe.
	AllowedClockDrift time.Duration
}

// CoprocessorCache is the config for coprocessor cache.
type CoprocessorCache struct {
	// Whether to enable the copr cache. The copr cache saves the result from TiKV Coprocessor in the memory and
	// reuses the result when corresponding data in TiKV is unchanged, on a region basis.
	Enable bool `toml:"enable" json:"enable"`
	// The capacity in MB of the cache.
	CapacityMB float64 `toml:"capacity-mb" json:"capacity-mb"`
	// Only cache requests that containing small number of ranges. May to be changed in future.
	AdmissionMaxRanges uint64 `toml:"admission-max-ranges" json:"admission-max-ranges"`
	// Only cache requests whose result set is small.
	AdmissionMaxResultMB float64 `toml:"admission-max-result-mb" json:"admission-max-result-mb"`
	// Only cache requests takes notable time to process.
	AdmissionMinProcessMs uint64 `toml:"admission-min-process-ms" json:"admission-min-process-ms"`
}

// DefaultTiKVClient returns default config for TiKVClient.
func DefaultTiKVClient() TiKVClient {
	return TiKVClient{
		GrpcConnectionCount:  4,
		GrpcKeepAliveTime:    10,
		GrpcKeepAliveTimeout: 3,
		GrpcCompressionType:  "none",
		CommitTimeout:        "41s",
		AsyncCommit: AsyncCommit{
			// FIXME: Find an appropriate default limit.
			KeysLimit:         256,
			TotalKeySizeLimit: 4 * 1024, // 4 KiB
			SafeWindow:        2 * time.Second,
			AllowedClockDrift: 500 * time.Millisecond,
		},

		MaxBatchSize:      128,
		OverloadThreshold: 200,
		MaxBatchWaitTime:  0,
		BatchWaitSize:     8,

		EnableChunkRPC: true,

		RegionCacheTTL:       600,
		StoreLimit:           0,
		StoreLivenessTimeout: DefStoreLivenessTimeout,

		TTLRefreshedTxnSize: 32 * 1024 * 1024,

		CoprCache: CoprocessorCache{
			Enable:                true,
			CapacityMB:            1000,
			AdmissionMaxRanges:    500,
			AdmissionMaxResultMB:  10,
			AdmissionMinProcessMs: 5,
		},
	}
}

// Valid checks if this config is valid.
func (config *TiKVClient) Valid() error {
	if config.GrpcConnectionCount == 0 {
		return fmt.Errorf("grpc-connection-count should be greater than 0")
	}
	if config.GrpcCompressionType != "none" && config.GrpcCompressionType != gzip.Name {
		return fmt.Errorf("grpc-compression-type should be none or %s, but got %s", gzip.Name, config.GrpcCompressionType)
	}
	return nil
}
