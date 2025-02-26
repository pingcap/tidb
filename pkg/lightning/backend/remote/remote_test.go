package remote

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/common"
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

func TestRemoteWorkerAPI(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "POST":
			if r.URL.Query().Get("build") == "true" {
				w.WriteHeader(http.StatusOK)
				return
			}
			w.WriteHeader(http.StatusOK)
		case "GET":
			states := &LoadDataStates{
				Finished:        true,
				CreatedFiles:    2,
				IngestedRegions: 1,
				TotalKVs:        100,
			}
			json.NewEncoder(w).Encode(states)
		}
	}))
	defer server.Close()

	ctx := context.Background()
	tls := &common.TLS{}
	cfg := &BackendConfig{
		TaskID:           1,
		RemoteWorkerAddr: server.URL,
		PdAddr:           "127.0.0.1:2379",
		SortedKVDir:      t.TempDir(),
		ChunkSize:        32 * 1024 * 1024,
	}

	b, err := NewBackend(ctx, tls, cfg, nil)
	require.NoError(t, err)
	defer b.Close()

	engineUUID := uuid.New()
	engineCfg := &backend.EngineConfig{}

	err = b.OpenEngine(ctx, engineCfg, engineUUID)
	require.NoError(t, err)

	err = b.ImportEngine(ctx, engineUUID, 96*1024*1024, 1000000)
	require.NoError(t, err)
}
