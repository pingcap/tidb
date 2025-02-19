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
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/docker/go-units"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/errormanager"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/metric"
	"github.com/tikv/client-go/v2/oracle"
	tikvclient "github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
	sd "github.com/tikv/pd/client/servicediscovery"
	"go.uber.org/zap"
	"google.golang.org/grpc"
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

const (
	pdCliMaxMsgSize = int(128 * units.MiB) // pd.ScanRegion may return a large response
	duplicateDBName = "duplicates"
	maxConnsPerHost = 10
)

var (
	maxCallMsgSize = []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(pdCliMaxMsgSize)),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(pdCliMaxMsgSize)),
	}
)

// LoadDataStates is json data that returned by remote server GET API.
type LoadDataStates struct {
	Canceled        bool   `json:"canceled"`
	Finished        bool   `json:"finished"`
	Error           string `json:"error"`
	CreatedFiles    int    `json:"created-files"`
	IngestedRegions int    `json:"ingested-regions"`
	TotalKVs        int64  `json:"total-kvs"`

	DuplicateEntries []duplicateEntry `json:"duplicated-entries"`
}

type duplicateEntry struct {
	Key    string   `json:"key"`
	Values []string `json:"values"`
}

func (s *LoadDataStates) hasDuplicateEntries() bool {
	return len(s.DuplicateEntries) > 0
}

// NewBackend creates a new remote backend instance.
func NewBackend(
	ctx context.Context,
	tls *common.TLS,
	cfg *BackendConfig,
	pdSvcDiscovery sd.ServiceDiscovery,
) (backend.Backend, error) {
	localFile := cfg.SortedKVDir
	shouldCreate := true
	if cfg.CheckpointEnabled {
		if info, err := os.Stat(localFile); err != nil {
			if !os.IsNotExist(err) {
				return nil, err
			}
		} else if info.IsDir() {
			shouldCreate = false
		}
	}
	if shouldCreate {
		err := os.Mkdir(localFile, 0o700)
		if err != nil {
			return nil, common.ErrInvalidSortedKVDir.Wrap(err).GenWithStackByArgs(localFile)
		}
	}
	var (
		duplicateDB *pebble.DB
		err         error
	)
	keyAdapter := common.KeyAdapter(common.NoopKeyAdapter{})
	if cfg.DupeDetectEnabled {
		duplicateDB, err = local.OpenDuplicateDB(localFile)
		if err != nil {
			return nil, common.ErrOpenDuplicateDB.Wrap(err).GenWithStackByArgs()
		}
		keyAdapter = common.DupDetectKeyAdapter{}
	}

	var (
		pdCli        pd.Client
		spkv         *tikvclient.EtcdSafePointKV
		pdCliForTiKV *tikvclient.CodecPDClient
		rpcCli       tikvclient.Client
		tikvCli      *tikvclient.KVStore
	)
	defer func() {
		if err == nil {
			return
		}
		if tikvCli != nil {
			// tikvCli uses pdCliForTiKV(which wraps pdCli) , spkv and rpcCli, so
			// close tikvCli will close all of them.
			_ = tikvCli.Close()
		} else {
			if rpcCli != nil {
				_ = rpcCli.Close()
			}
			if spkv != nil {
				_ = spkv.Close()
			}
			// pdCliForTiKV wraps pdCli, so we only need close pdCli
			if pdCli != nil {
				pdCli.Close()
			}
		}
	}()

	var pdAddrs []string
	if pdSvcDiscovery != nil {
		pdAddrs = pdSvcDiscovery.GetServiceURLs()
		// TODO(lance6716): if PD client can support creating a client with external
		// service discovery, we can directly pass pdSvcDiscovery.
	} else {
		pdAddrs = strings.Split(cfg.PdAddr, ",")
	}
	pdCli, err = pd.NewClientWithContext(
		ctx, caller.Component("lightning-remote-backend"), pdAddrs, tls.ToPDSecurityOption(),
		opt.WithGRPCDialOptions(maxCallMsgSize...),
		// If the time too short, we may scatter a region many times, because
		// the interface `ScatterRegions` may time out.
		opt.WithCustomTimeoutOption(60*time.Second),
	)
	if err != nil {
		return nil, common.NormalizeOrWrapErr(common.ErrCreatePDClient, err)
	}
	spkv, err = tikvclient.NewEtcdSafePointKV(pdAddrs, tls.TLSConfig())
	if err != nil {
		return nil, common.ErrCreateKVClient.Wrap(err).GenWithStackByArgs()
	}

	if cfg.KeyspaceName == "" {
		pdCliForTiKV = tikvclient.NewCodecPDClient(tikvclient.ModeTxn, pdCli)
	} else {
		pdCliForTiKV, err = tikvclient.NewCodecPDClientWithKeyspace(tikvclient.ModeTxn, pdCli, cfg.KeyspaceName)
		if err != nil {
			return nil, common.ErrCreatePDClient.Wrap(err).GenWithStackByArgs()
		}
	}

	tikvCodec := pdCliForTiKV.GetCodec()
	rpcCli = tikvclient.NewRPCClient(tikvclient.WithSecurity(tls.ToTiKVSecurityConfig()), tikvclient.WithCodec(tikvCodec))
	tikvCli, err = tikvclient.NewKVStore("lightning-remote-backend", pdCliForTiKV, spkv, rpcCli)
	if err != nil {
		return nil, common.ErrCreateKVClient.Wrap(err).GenWithStackByArgs()
	}

	httpClient := http.DefaultClient
	if tlsConfig := tls.TLSConfig(); tlsConfig != nil {
		httpClient = &http.Client{
			Transport: &http.Transport{
				MaxConnsPerHost: maxConnsPerHost,
				TLSClientConfig: tlsConfig,
			},
		}
	}

	keyspace := pdCliForTiKV.GetCodec().GetKeyspace()
	keyspaceID := pdCliForTiKV.GetCodec().GetKeyspaceID()
	remote := &Backend{
		workerAddr: cfg.RemoteWorkerAddr,
		pdCli:      pdCli,
		tls:        tls,
		pdAddr:     cfg.PdAddr,
		keyspace:   keyspace,
		keyspaceID: uint32(keyspaceID),
		logger:     log.FromContext(ctx),
		httpClient: httpClient,

		tikvCodec:     tikvCodec,
		tikvCli:       tikvCli,
		duplicateDB:   duplicateDB,
		keyAdapter:    keyAdapter,
		localStoreDir: localFile,

		importTaskID:      cfg.TaskID,
		chunkSize:         cfg.ChunkSize,
		chunkCacheDir:     cfg.ChunkCacheDir,
		chunkCacheInMem:   cfg.ChunkCacheInMem,
		checkpointEnabled: cfg.CheckpointEnabled,
		dupeDetectEnabled: cfg.DupeDetectEnabled,
		reportErrOnDup:    cfg.DuplicateDetectOpt.ReportErrOnDup,
	}

	if m, ok := metric.FromContext(ctx); ok {
		remote.metrics = m
	}

	return remote, nil
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
	ChunkCacheInMem     bool
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
		ChunkCacheDir:       cfg.TikvImporter.ChunkCacheDir,
		ChunkCacheInMem:     cfg.TikvImporter.ChunkCacheInMem,
	}
}

// Backend is a remote backend that sends KV pairs to remote worker.
type Backend struct {
	workerAddr string
	pdCli      pd.Client
	tls        *common.TLS
	pdAddr     string
	metrics    *metric.Metrics
	engines    sync.Map
	logger     log.Logger
	httpClient *http.Client
	keyspace   []byte
	keyspaceID uint32

	tikvCodec   tikvclient.Codec
	duplicateDB *pebble.DB
	tikvCli     *tikvclient.KVStore
	keyAdapter  common.KeyAdapter

	importTaskID      int64
	chunkSize         int
	dupeDetectEnabled bool
	reportErrOnDup    bool
	checkpointEnabled bool
	localStoreDir     string
	chunkCacheDir     string
	chunkCacheInMem   bool
}

// Close the connection to the backend.
func (b *Backend) Close() {
	if b.duplicateDB != nil {
		// Check if there are duplicates that are not collected.
		iter, err := b.duplicateDB.NewIter(&pebble.IterOptions{})
		if err != nil {
			b.logger.Panic("fail to create iterator")
		}
		hasDuplicates := iter.First()
		allIsWell := true
		if err := iter.Error(); err != nil {
			b.logger.Warn("iterate duplicate db failed", zap.Error(err))
			allIsWell = false
		}
		if err := iter.Close(); err != nil {
			b.logger.Warn("close duplicate db iter failed", zap.Error(err))
			allIsWell = false
		}
		if err := b.duplicateDB.Close(); err != nil {
			b.logger.Warn("close duplicate db failed", zap.Error(err))
			allIsWell = false
		}
		// If checkpoint is disabled, or we don't detect any duplicate, then this duplicate
		// db dir will be useless, so we clean up this dir.
		if allIsWell && (!b.checkpointEnabled || !hasDuplicates) {
			if err := os.RemoveAll(filepath.Join(b.localStoreDir, duplicateDBName)); err != nil {
				b.logger.Warn("remove duplicate db file failed", zap.Error(err))
			}
		}
		b.duplicateDB = nil
	}

	// if checkpoint is disable or we finish load all data successfully, then files in this
	// dir will be useless, so we clean up this dir and all files in it.
	if !b.checkpointEnabled || common.IsEmptyDir(b.localStoreDir) {
		err := os.RemoveAll(b.localStoreDir)
		if err != nil {
			b.logger.Warn("remove local db file failed", zap.Error(err))
		}
	}
	_ = b.tikvCli.Close()
	b.pdCli.Close()
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
func (b *Backend) OpenEngine(ctx context.Context, cfg *backend.EngineConfig, engineUUID uuid.UUID) error {
	physical, logical, err := b.pdCli.GetTS(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	ts := oracle.ComposeTS(physical, logical)

	loadDataTaskID := genLoadDataTaskID(b.keyspaceID, b.importTaskID, cfg)
	e, _ := b.engines.LoadOrStore(engineUUID, &engine{
		ctx:            ctx,
		logger:         log.With(zap.String("loadDataTaskID", loadDataTaskID)),
		loadDataTaskID: loadDataTaskID,
		ts:             ts,
		tbl:            cfg.TableInfo,
		addr:           b.workerAddr,
		clusterID:      b.pdCli.GetClusterID(ctx),
		writers:        sync.Map{},
		httpClient:     b.httpClient,
		backend:        b,
	})
	engine := e.(*engine)
	if cfg.Remote.RecoverFromCheckpoint {
		exist, err := b.ensureTaskExists(ctx, loadDataTaskID)
		if err != nil {
			return errors.Trace(err)
		}
		if !exist {
			b.logger.Error("task not found in remote", zap.String("loadDataTaskID", loadDataTaskID))
			return common.ErrLoadDataTaskNotFound.FastGenByArgs(loadDataTaskID)
		}
	}

	if engine.loadDataTaskID == loadDataTaskID {
		// newly created engine.
		err = b.loadDataInit(ctx, engine, cfg.Remote.EstimatedDataSize)
		if err != nil {
			b.engines.Delete(engineUUID)
			return errors.Trace(err)
		}
	}
	return nil
}

func (b *Backend) loadDataInit(ctx context.Context, engine *engine, dataSize int64) error {
	for {
		url := fmt.Sprintf(initTaskURL,
			engine.addr,
			engine.clusterID,
			engine.loadDataTaskID,
			engine.ts,
			engine.ts+1,
			dataSize,
		)
		client := *b.httpClient
		client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		}

		resp, err := sendRequestWithRetry(ctx, &client, "POST", url, nil)
		if err != nil {
			return errors.Trace(err)
		}

		if resp.StatusCode == http.StatusFound {
			engine.addr = parseRemoteWorkerURL(resp, b.tls.TLSConfig() != nil)
			engine.logger.Info("redirect to remote worker",
				zap.String("worker addr", engine.addr))
			_ = resp.Body.Close()
			continue
		}

		if resp.StatusCode != http.StatusOK {
			msg, err := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			// If the task exists, we can continue to import data.
			if err == nil && strings.TrimSpace(string(msg)) == taskExitsMsg {
				engine.logger.Info("load data task has inited in remote worker",
					zap.String("worker addr", engine.addr))
				return nil
			}
			engine.logger.Error("failed to init load data task",
				zap.String("status", resp.Status),
				zap.String("msg", string(msg)))
			return common.ErrRequestRemoteWorker.FastGenByArgs(resp.StatusCode, string(msg))
		}
		_ = resp.Body.Close()
		return nil
	}
}

// CloseEngine closes backend engine by uuid.
func (b *Backend) CloseEngine(ctx context.Context, cfg *backend.EngineConfig, engineUUID uuid.UUID) error {
	engine, err := b.getEngine(engineUUID)
	if err != nil {
		loadDataTaskID := genLoadDataTaskID(b.keyspaceID, b.importTaskID, cfg)
		exist, err := b.ensureTaskExists(ctx, loadDataTaskID)
		if err != nil {
			return errors.Trace(err)
		}
		if !exist {
			return common.ErrLoadDataTaskNotFound.FastGenByArgs(loadDataTaskID)
		}
		err = b.OpenEngine(ctx, cfg, engineUUID)
		if err != nil {
			return errors.Trace(err)
		}
		engine, err = b.getEngine(engineUUID)
		if err != nil {
			return errors.Trace(err)
		}
	}

	err = engine.flush(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	engine.writers.Range(func(key, _ any) bool {
		sender := key.(*writer).chunkSender
		err = sender.close()
		if err != nil {
			engine.logger.Warn("failed to close chunk sender", zap.Error(err), zap.Uint64("sender", sender.id))
		}
		return true
	})

	return errors.Trace(err)
}

func (b *Backend) ensureTaskExists(ctx context.Context, loadDataTaskID string) (bool, error) {
	clusterID := b.pdCli.GetClusterID(ctx)
	addr := b.workerAddr

	for {
		url := fmt.Sprintf(queryTaskURL, addr, clusterID, loadDataTaskID)
		client := *b.httpClient
		client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		}

		resp, err := sendRequestWithRetry(ctx, &client, "GET", url, nil)
		if err != nil {
			return false, err
		}

		if resp.StatusCode == http.StatusFound {
			addr = parseRemoteWorkerURL(resp, b.tls.TLSConfig() != nil)
			b.logger.Info("redirect to remote worker",
				zap.String("worker addr", addr),
				zap.String("loadDataTaskID", loadDataTaskID))
			_ = resp.Body.Close()
			continue
		}
		if resp.StatusCode != http.StatusOK {
			if resp.StatusCode == http.StatusNotFound {
				return false, nil
			}
			msg, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			b.logger.Warn("failed to get task", zap.String("status", resp.Status),
				zap.String("msg", string(msg)),
				zap.String("loadDataTaskID", loadDataTaskID))
			return false, common.ErrRequestRemoteWorker.FastGenByArgs(resp.Status, string(msg))
		}
		_ = resp.Body.Close()
		return true, nil
	}
}

// ImportEngine imports an engine to TiKV.
func (b *Backend) ImportEngine(ctx context.Context, engineUUID uuid.UUID, regionSplitSize, regionSplitKeys int64) error {
	engine, err := b.getEngine(engineUUID)
	if err != nil {
		return errors.Trace(err)
	}
	err = b.loadDataBuild(ctx, engine, regionSplitSize, regionSplitKeys)
	if err != nil {
		return errors.Trace(err)
	}
	start := time.Now()
	ticker := time.NewTicker(time.Second * 1)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			states, err := b.loadDataGetStates(ctx, engine)
			if err != nil {
				return errors.Trace(err)
			}
			if states.Canceled {
				engine.logger.Error("load data task is canceled", zap.String("error", states.Error))
				return common.ErrLoadDataTaskCanceled.FastGenByArgs(engine.loadDataTaskID, states.Error)
			} else if states.Finished {
				if states.hasDuplicateEntries() {
					err = b.handleDuplicateEntries(ctx, engine, states)
					if err != nil {
						return errors.Trace(err)
					}
				}
				engine.importedKVs.Store(states.TotalKVs)
				b.logger.Info("load data task is finished",
					zap.String("db", engine.tbl.DB),
					zap.String("table", engine.tbl.Name),
					zap.Int64("imported kvs", states.TotalKVs),
					zap.Duration("elapsed", time.Since(start)))
				return nil
			} else {
				b.logger.Info(
					"get load data task states",
					zap.String("db", engine.tbl.DB),
					zap.String("table", engine.tbl.Name),
					zap.Int64("total kvs", states.TotalKVs),
					zap.Int("created files", states.CreatedFiles),
					zap.Int("ingested regions", states.IngestedRegions))
			}
		}
	}
}

// GetImportedKVCount returns the number of imported KV pairs of some engine.
func (b *Backend) GetImportedKVCount(engineUUID uuid.UUID) int64 {
	v, ok := b.engines.Load(engineUUID)
	if !ok {
		return 0
	}
	e := v.(*engine)
	return e.importedKVs.Load()
}

func (b *Backend) handleDuplicateEntries(_ context.Context, engine *engine, states *LoadDataStates) error {
	if !b.dupeDetectEnabled {
		return nil
	}

	if b.reportErrOnDup {
		dupKey, err := hex.DecodeString(states.DuplicateEntries[0].Key)
		if err != nil {
			engine.logger.Warn("failed to decode key", zap.String("key", states.DuplicateEntries[0].Key), zap.Error(err))
		} else {
			dupKey, err = b.tikvCodec.DecodeKey(dupKey)
			if err != nil {
				engine.logger.Warn("failed to decode key", zap.String("key", states.DuplicateEntries[0].Key), zap.Error(err))
			}
		}

		dupVal, err := hex.DecodeString(states.DuplicateEntries[0].Values[0])
		if err != nil {
			engine.logger.Warn("failed to decode value", zap.String("value", states.DuplicateEntries[0].Values[0]), zap.Error(err))
		}

		return common.ErrFoundDuplicateKeys.FastGenByArgs(dupKey, dupVal)
	}

	writeBatch := b.duplicateDB.NewBatch()
	writeBatchSize := int64(0)
	for _, entry := range states.DuplicateEntries {
		if len(entry.Values) == 0 {
			continue
		}
		rawKey, err := hex.DecodeString(entry.Key)
		if err != nil {
			return errors.Trace(err)
		}
		for i, value := range entry.Values {
			// append index to rawKey to make it unique.
			encodedRawKey := b.keyAdapter.Encode(nil, rawKey, common.EncodeIntRowID(int64(i)))

			rawValue, err := hex.DecodeString(value)
			if err != nil {
				return errors.Trace(err)
			}
			err = writeBatch.Set(encodedRawKey, rawValue, nil)
			if err != nil {
				return errors.Trace(err)
			}
			writeBatchSize += int64(len(encodedRawKey) + len(rawValue))
			if writeBatchSize >= maxDuplicateBatchSize {
				err = writeBatch.Commit(nil)
				if err != nil {
					return errors.Trace(err)
				}
				writeBatch = b.duplicateDB.NewBatch()
				writeBatchSize = 0
			}
		}
	}

	if writeBatchSize > 0 {
		err := writeBatch.Commit(nil)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (b *Backend) allocateWriterID(ctx context.Context) (uint64, error) {
	physical, logical, err := b.pdCli.GetTS(ctx)
	if err != nil {
		return 0, err
	}
	ts := oracle.ComposeTS(physical, logical)
	return ts, nil
}

func (b *Backend) loadDataBuild(ctx context.Context, engine *engine, splitSize, splitKeys int64) error {
	url := fmt.Sprintf(buildURL, engine.addr, engine.clusterID, engine.loadDataTaskID, splitSize, splitKeys)

	_, err := sendRequest(ctx, b.httpClient, "POST", url, nil)
	return errors.Trace(err)
}

func (b *Backend) loadDataGetStates(ctx context.Context, engine *engine) (*LoadDataStates, error) {
	url := fmt.Sprintf(queryTaskURL, engine.addr, engine.clusterID, engine.loadDataTaskID)
	data, err := sendRequest(ctx, b.httpClient, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	state := new(LoadDataStates)
	err = json.Unmarshal(data, state)
	if err != nil {
		return nil, err
	}
	return state, nil
}

// CleanupEngine cleanup the engine and reclaim the space.
func (b *Backend) CleanupEngine(ctx context.Context, engineUUID uuid.UUID) error {
	engine, err := b.getEngine(engineUUID)
	if err != nil {
		return errors.Trace(err)
	}
	return b.loadDataCleanUp(ctx, engine)
}

func (b *Backend) loadDataCleanUp(ctx context.Context, engine *engine) error {
	url := fmt.Sprintf(cleanUpTaskURL, engine.addr, engine.clusterID, engine.loadDataTaskID)
	_, err := sendRequest(ctx, b.httpClient, "DELETE", url, nil)
	return errors.Trace(err)
}

// FlushEngine ensures all KV pairs written to an open engine has been
// synchronized, such that kill-9'ing Lightning afterwards and resuming from
// checkpoint can recover the exact same content.
//
// This method is only relevant for local backend, and is no-op for all
// other backends.
func (b *Backend) FlushEngine(ctx context.Context, engineUUID uuid.UUID) error {
	engine, err := b.getEngine(engineUUID)
	if err != nil {
		return errors.Trace(err)
	}
	return engine.flush(ctx)
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
func (b *Backend) LocalWriter(ctx context.Context, _ *backend.LocalWriterConfig, engineUUID uuid.UUID) (backend.EngineWriter, error) {
	engine, err := b.getEngine(engineUUID)
	if err != nil {
		return nil, err
	}
	// If lightning pod is restarted, there is no guarantee that the chunk generated is same as before, so we need to
	// allocate a new sender id instead of using writer's id.
	writerID, err := b.allocateWriterID(ctx)
	if err != nil {
		return nil, err
	}
	cache, err := newChunksCache(engine.loadDataTaskID, writerID, b.chunkCacheDir, b.chunkCacheInMem)
	if err != nil {
		return nil, err
	}
	writer := &writer{
		e:           engine,
		keyspace:    b.keyspace,
		chunkSender: newChunkSender(ctx, writerID, engine, cache),
	}
	engine.writers.Store(writer, struct{}{})
	return writer, nil
}

func (b *Backend) getEngine(engineUUID uuid.UUID) (*engine, error) {
	v, ok := b.engines.Load(engineUUID)
	if !ok {
		return nil, errors.Errorf("cannot find engine for %s", engineUUID.String())
	}
	return v.(*engine), nil
}

type engine struct {
	ctx            context.Context
	logger         log.Logger
	loadDataTaskID string
	ts             uint64
	tbl            *checkpoints.TidbTableInfo
	writers        sync.Map
	addr           string
	clusterID      uint64

	httpClient  *http.Client
	backend     *Backend
	importedKVs atomic.Int64
}

func (e *engine) flush(ctx context.Context) error {
	var err error
	e.writers.Range(func(key, _ any) bool {
		w := key.(*writer)
		err = w.addChunk(ctx)
		if err != nil {
			return false
		}

		err = w.chunkSender.flush(ctx)
		return err == nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(err)
}

// writer implments the EngineWriter interface
type writer struct {
	e           *engine
	chunkSender *chunkSender
	lastChunkID uint64

	buf      []byte
	keyspace []byte
}

const batchSize int = 8 * 1024 * 1024

func (w *writer) AppendRows(
	ctx context.Context,
	_ []string,
	rows encode.Rows,
) error {
	kvs := kv.Rows2KvPairs(rows)
	if len(kvs) == 0 {
		return nil
	}
	lenBuf := make([]byte, 4)
	for _, pair := range kvs {
		binary.LittleEndian.PutUint16(lenBuf[:2], uint16(len(pair.Key)+len(w.keyspace)))
		if len(w.keyspace) != 0 {
			w.buf = append(w.buf, lenBuf[:2]...)
			w.buf = append(w.buf, w.keyspace...)
		}
		w.buf = append(w.buf, pair.Key...)
		binary.LittleEndian.PutUint32(lenBuf, uint32(len(pair.Val)))
		w.buf = append(w.buf, lenBuf...)
		w.buf = append(w.buf, pair.Val...)
		binary.LittleEndian.PutUint16(lenBuf, uint16(len(pair.RowID)))
		w.buf = append(w.buf, lenBuf[:2]...)
		w.buf = append(w.buf, pair.RowID...)
	}
	if len(w.buf) > w.e.backend.chunkSize {
		return w.addChunk(ctx)
	}
	return nil
}

func (w *writer) addChunk(ctx context.Context) error {
	if len(w.buf) == 0 {
		// nothing to do
		return nil
	}

	chunkID, err := w.chunkSender.putChunk(ctx, w.buf)
	if err != nil {
		return errors.Trace(err)
	}
	w.lastChunkID = chunkID
	w.buf = w.buf[:0]

	return nil
}

func (w *writer) IsSynced() bool {
	return len(w.buf) == 0 && w.lastChunkID <= w.chunkSender.getFlushedChunkID()
}

func (w *writer) Close(ctx context.Context) (backend.ChunkFlushStatus, error) {
	if len(w.buf) > 0 {
		err := w.addChunk(ctx)
		if err != nil {
			return w, err
		}
	}
	w.buf = nil
	return w, nil
}

func (w *writer) Flushed() bool {
	return w.lastChunkID <= w.chunkSender.getFlushedChunkID()
}

// GetDupeController returns a new dupe controller.
func (b *Backend) GetDupeController(dupeConcurrency int, errorMgr *errormanager.ErrorManager) *local.DupeController {
	return local.NewDupeController(dupeConcurrency, errorMgr, nil, b.tikvCli, b.tikvCodec, b.duplicateDB, b.keyAdapter, nil, "", "", false)
}
