// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package remote

import (
	"context"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/errormanager"
	"github.com/pingcap/tidb/pkg/lightning/log"
	sd "github.com/tikv/pd/client/servicediscovery"
)

/*
    Remote Backend is a backend that sends KV pairs to remote worker using HTTP API that using in TiDB Cloud.
	The remote worker will sort the KV pairs and encode them into SST files, then ingest SST files into TiKV.

	Remote worker API:

	1. init task:
		POST /load_data?cluster_id=%d&task_id=%s&start_ts=%d&commit_ts=%d

	2. put chunk:
		PUT /load_data?cluster_id=%d&task_id=%s&writer_id=%d&chunk_id=%d

		key_len(2) + key(key_len) + val_len(4) + value(val_len) + row_id_len(2) + row_id(row_id_len)
		key_len(2) + key(key_len) + val_len(4) + value(val_len) + row_id_len(2) + row_id(row_id_len)
		...

	3. flush:
		POST /load_data?cluster_id=%d&task_id=%s&writer_id=%dflush=true

	4. build:
		POST /load_data?cluster_id=%d&task_id=%s&build=true&compression=zstd&split_size=%d&split_keys=%d

	5. get task states:
		GET /load_data?cluster_id=%d&task_id=%s

		{"canceled": false, "finished": false, "error": "", "created-files": 10, "ingested-regions": 3}

	6. clean up task:
		DELETE /load_data?cluster_id=%d&task_id=%s
*/

const (
	initTaskURL    = "%s/load_data?cluster_id=%d&task_id=%s&start_ts=%d&commit_ts=%d&data_size=%d"
	putChunkURL    = "%s/load_data?cluster_id=%d&task_id=%s&writer_id=%d&chunk_id=%d"
	flushURL       = "%s/load_data?cluster_id=%d&task_id=%s&writer_id=%d&flush=true"
	buildURL       = "%s/load_data?cluster_id=%d&task_id=%s&build=true&compression=zstd&split_size=%d&split_keys=%d"
	queryTaskURL   = "%s/load_data?cluster_id=%d&task_id=%s"
	cleanUpTaskURL = "%s/load_data?cluster_id=%d&task_id=%s"
)

// NewBackend creates a new remote backend instance.
func NewBackend(
	_ context.Context,
	_ *common.TLS,
	_ *BackendConfig,
	_ sd.ServiceDiscovery,
) (backend.Backend, error) {
	return &Backend{}, nil
}

// BackendConfig is the config for remote backend.
type BackendConfig struct {
	TaskID              int64
	PdAddr              string
	RemoteWorkerAddr    string
	KeyspaceName        string
	SortedKVDir         string
	ChunkSize           int
	ChunkCacheDir       string
	CheckpointEnabled   bool
	DuplicateResolution config.DuplicateResolutionAlgorithm
	ResourceGroupName   string
	TaskType            string
	DupeDetectEnabled   bool
	DuplicateDetectOpt  common.DupDetectOpt
}

// NewBackendConfig creates a new BackendConfig.
func NewBackendConfig(cfg *config.Config, keyspaceName, resourceGroupName, taskType string) *BackendConfig {
	return &BackendConfig{
		TaskID:              cfg.TaskID,
		PdAddr:              cfg.TiDB.PdAddr,
		RemoteWorkerAddr:    cfg.TikvImporter.RemoteWorkerAddr,
		KeyspaceName:        keyspaceName,
		ResourceGroupName:   resourceGroupName,
		TaskType:            taskType,
		SortedKVDir:         cfg.TikvImporter.SortedKVDir,
		CheckpointEnabled:   cfg.Checkpoint.Enable,
		DuplicateResolution: cfg.TikvImporter.DuplicateResolution,
		DupeDetectEnabled:   cfg.Conflict.Strategy != config.NoneOnDup,
		DuplicateDetectOpt:  common.DupDetectOpt{ReportErrOnDup: cfg.Conflict.Strategy == config.ErrorOnDup},
		ChunkSize:           cfg.TikvImporter.RemoteChunkSize,
		ChunkCacheDir:       cfg.TikvImporter.RemoteChunkCacheDir,
	}
}

// Backend is a remote backend that sends KV pairs to remote worker.
type Backend struct {
}

// Close the connection to the backend.
func (*Backend) Close() {
}

// RetryImportDelay returns the duration to sleep when retrying an import
func (*Backend) RetryImportDelay() time.Duration {
	return 0
}

// ShouldPostProcess returns whether KV-specific post-processing should be
// performed for this backend. Post-processing includes checksum and analyze.
func (*Backend) ShouldPostProcess() bool {
	return true
}

// OpenEngine opens an engine for writing.
func (*Backend) OpenEngine(_ context.Context, _ *backend.EngineConfig, _ uuid.UUID) error {
	return nil
}

// CloseEngine closes backend engine by uuid.
func (*Backend) CloseEngine(_ context.Context, _ *backend.EngineConfig, _ uuid.UUID) error {
	return nil
}

// ImportEngine imports an engine to TiKV.
func (*Backend) ImportEngine(_ context.Context, _ uuid.UUID, _, _ int64) error {
	return nil
}

// GetImportedKVCount returns the number of imported KV pairs of some engine.
func (*Backend) GetImportedKVCount(_ uuid.UUID) int64 {
	return 0
}

// CleanupEngine cleanup the engine and reclaim the space.
func (*Backend) CleanupEngine(_ context.Context, _ uuid.UUID) error {
	return nil
}

// FlushEngine ensures all KV pairs written to an open engine has been
// synchronized, such that kill-9'ing Lightning afterwards and resuming from
// checkpoint can recover the exact same content.
//
// This method is only relevant for local backend, and is no-op for all
// other backends.
func (*Backend) FlushEngine(_ context.Context, _ uuid.UUID) error {
	return nil
}

// FlushAllEngines performs FlushEngine on all opened engines. This is a
// very expensive operation and should only be used in some rare situation
// (e.g. preparing to resolve a disk quota violation).
// This method is only relevant for local backend, and is no-op for remote backend.
func (*Backend) FlushAllEngines(_ context.Context) error {
	return nil
}

// EngineFileSizes obtains the size occupied locally of all engines managed
// by this backend. This method is used to compute disk quota.
// It can return nil if the content are all stored remotely.
func (*Backend) EngineFileSizes() []backend.EngineFileSize {
	return nil
}

// ResetEngine clears all written KV pairs in this opened engine.
func (*Backend) ResetEngine(_ context.Context, _ uuid.UUID) error {
	return errors.New("cannot reset an engine in remote backend")
}

// LocalWriter obtains a thread-local EngineWriter for writing rows into the given engine.
func (*Backend) LocalWriter(_ context.Context, _ *backend.LocalWriterConfig, _ uuid.UUID) (backend.EngineWriter, error) {
	return &writer{}, nil
}

type engine struct {
	logger log.Logger
	// task id in remote worker
	loadDataTaskID string
	addr           string
	clusterID      uint64

	httpClient *http.Client
}

// writer implments the EngineWriter interface
type writer struct {
}

func (*writer) AppendRows(
	_ context.Context,
	_ []string,
	_ encode.Rows,
) error {
	return nil
}

func (*writer) IsSynced() bool {
	return true
}

func (w *writer) Close(_ context.Context) (backend.ChunkFlushStatus, error) {
	return w, nil
}

func (*writer) Flushed() bool {
	return false
}

// GetDupeController returns a new dupe controller.
func (*Backend) GetDupeController(dupeConcurrency int, errorMgr *errormanager.ErrorManager) *local.DupeController {
	return local.NewDupeControllerForRemoteBackend(dupeConcurrency, errorMgr, nil, nil, nil, nil, nil, nil, "", "", false)
}
