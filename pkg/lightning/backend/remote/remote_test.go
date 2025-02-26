package remote

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/stretchr/testify/require"
)

func TestNewBackendConfig(t *testing.T) {
	cfg := &config.Config{
		TaskID: 123,
		TiDB: config.DBStore{
			PdAddr: "127.0.0.1:2379",
		},
		TikvImporter: config.TikvImporter{
			RemoteWorkerAddr: "127.0.0.1:8287",
			SortedKVDir:      "/tmp/sorted-kv",
			ChunkCacheDir:    "/tmp/chunk-cache",
		},
		Checkpoint: config.Checkpoint{
			Enable: true,
		},
		Conflict: config.Conflict{
			Strategy: config.ErrorOnDup,
		},
	}

	bc := NewBackendConfig(cfg, "test_keyspace", "test_group", "test_type")
	require.Equal(t, cfg.TaskID, bc.TaskID)
	require.Equal(t, cfg.TiDB.PdAddr, bc.PdAddr)
	require.Equal(t, cfg.TikvImporter.RemoteWorkerAddr, bc.RemoteWorkerAddr)
	require.Equal(t, "test_keyspace", bc.KeyspaceName)
	require.Equal(t, "test_group", bc.ResourceGroupName)
	require.Equal(t, "test_type", bc.TaskType)
	require.Equal(t, cfg.TikvImporter.SortedKVDir, bc.SortedKVDir)
	require.True(t, bc.CheckpointEnabled)
	require.True(t, bc.DupeDetectEnabled)
	require.True(t, bc.DuplicateDetectOpt.ReportErrOnDup)
}

func TestLoadDataStates(t *testing.T) {
	states := &LoadDataStates{
		Canceled:        false,
		Finished:        true,
		Error:           "",
		CreatedFiles:    10,
		IngestedRegions: 5,
		TotalKVs:        1000,
		DuplicateEntries: []duplicateEntry{
			{
				Key:    "key1",
				Values: []string{"val1", "val2"},
			},
		},
	}

	require.True(t, states.hasDuplicateEntries())

	emptyStates := &LoadDataStates{}
	require.False(t, emptyStates.hasDuplicateEntries())
}

func TestBackendInterface(t *testing.T) {
	b := &Backend{}

	// Test interface method implementations
	require.Equal(t, time.Duration(0), b.RetryImportDelay())
	require.True(t, b.ShouldPostProcess())
	require.Nil(t, b.EngineFileSizes())

	err := b.ResetEngine(context.Background(), uuid.New())
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot reset an engine")
}
