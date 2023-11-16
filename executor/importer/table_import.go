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

package importer

import (
	"context"
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	verify "github.com/pingcap/tidb/br/pkg/lightning/verification"
	tidb "github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/executor/asyncloaddata"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/tikv/client-go/v2/util"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	// CheckDiskQuotaInterval is the default time interval to check disk quota.
	// TODO: make it dynamically adjusting according to the speed of import and the disk size.
	CheckDiskQuotaInterval = 10 * time.Second

	// defaultMaxEngineSize is the default max engine size in bytes.
	// we make it 5 times larger than lightning default engine size to reduce range overlap, especially for index,
	// since we have an index engine per distributed subtask.
	// for 1TiB data, we can divide it into 2 engines that runs on 2 TiDB. it can have a good balance between
	// range overlap and sort speed in one of our test of:
	// 	- 10 columns, PK + 6 secondary index 2 of which is mv index
	//	- 1.05 KiB per row, 527 MiB per file, 1024000000 rows, 1 TiB total
	//
	// it might not be the optimal value for other cases.
	defaultMaxEngineSize = int64(5 * config.DefaultBatchSize)
)

// prepareSortDir creates a new directory for import, remove previous sort directory if exists.
func prepareSortDir(e *LoadDataController, id string, tidbCfg *tidb.Config) (string, error) {
	sortPathSuffix := "import-" + strconv.Itoa(int(tidbCfg.Port))
	importDir := filepath.Join(tidbCfg.TempDir, sortPathSuffix)
	sortDir := filepath.Join(importDir, id)

	if info, err := os.Stat(importDir); err != nil || !info.IsDir() {
		if err != nil && !os.IsNotExist(err) {
			e.logger.Error("stat import dir failed", zap.String("import_dir", importDir), zap.Error(err))
			return "", errors.Trace(err)
		}
		if info != nil && !info.IsDir() {
			e.logger.Warn("import dir is not a dir, remove it", zap.String("import_dir", importDir))
			if err := os.RemoveAll(importDir); err != nil {
				return "", errors.Trace(err)
			}
		}
		e.logger.Info("import dir not exists, create it", zap.String("import_dir", importDir))
		if err := os.MkdirAll(importDir, 0o700); err != nil {
			e.logger.Error("failed to make dir", zap.String("import_dir", importDir), zap.Error(err))
			return "", errors.Trace(err)
		}
	}

	// todo: remove this after we support checkpoint
	if _, err := os.Stat(sortDir); err != nil {
		if !os.IsNotExist(err) {
			e.logger.Error("stat sort dir failed", zap.String("sort_dir", sortDir), zap.Error(err))
			return "", errors.Trace(err)
		}
	} else {
		e.logger.Warn("sort dir already exists, remove it", zap.String("sort_dir", sortDir))
		if err := os.RemoveAll(sortDir); err != nil {
			return "", errors.Trace(err)
		}
	}
	return sortDir, nil
}

// GetCachedKVStoreFrom gets a cached kv store from PD address.
// Callers should NOT close the kv store.
func GetCachedKVStoreFrom(pdAddr string, tls *common.TLS) (tidbkv.Storage, error) {
	// the kv store we get is a cached store, so we can't close it.
	kvStore, err := GetKVStore(fmt.Sprintf("tikv://%s?disableGC=true", pdAddr), tls.ToTiKVSecurityConfig())
	if err != nil {
		return nil, errors.Trace(err)
	}
	return kvStore, nil
}

// NewTableImporter creates a new table importer.
func NewTableImporter(param *JobImportParam, e *LoadDataController, id string) (ti *TableImporter, err error) {
	idAlloc := kv.NewPanickingAllocators(0)
	tbl, err := tables.TableFromMeta(idAlloc, e.Table.Meta())
	if err != nil {
		return nil, errors.Annotatef(err, "failed to tables.TableFromMeta %s", e.Table.Meta().Name)
	}

	tidbCfg := tidb.GetGlobalConfig()
	// todo: we only need to prepare this once on each node(we might call it 3 times in distribution framework)
	dir, err := prepareSortDir(e, id, tidbCfg)
	if err != nil {
		return nil, err
	}

	hostPort := net.JoinHostPort("127.0.0.1", strconv.Itoa(int(tidbCfg.Status.StatusPort)))
	tls, err := common.NewTLS(
		tidbCfg.Security.ClusterSSLCA,
		tidbCfg.Security.ClusterSSLCert,
		tidbCfg.Security.ClusterSSLKey,
		hostPort,
		nil, nil, nil,
	)
	if err != nil {
		return nil, err
	}

	var rLimit local.Rlim_t
	rLimit, err = local.GetSystemRLimit()
	if err != nil {
		return nil, err
	}
	if rLimit < 0 {
		rLimit = math.MaxInt32
	}
	// no need to close kvStore, since it's a cached store.
	// todo: use a real region size getter
	litConfig := &config.Config{
		TiDB: config.DBStore{
			PdAddr: tidbCfg.Path,
		},
		TikvImporter: config.TikvImporter{
			SortedKVDir:             dir,
			RangeConcurrency:        16,
			EngineMemCacheSize:      config.DefaultEngineMemCacheSize,
			LocalWriterMemCacheSize: config.DefaultLocalWriterMemCacheSize,
			DuplicateResolution:     config.DupeResAlgNone,
			SendKVPairs:             4096,
		},
		App: config.Lightning{
			CheckRequirements: true,
		},
		Checkpoint: config.Checkpoint{
			Enable: false,
		},
		Cron: config.Cron{
			SwitchMode: config.Duration{Duration: 0},
		},
	}
	localBackend, err := local.NewLocalBackend(param.GroupCtx, tls, litConfig, nil, int(rLimit), nil)
	if err != nil {
		return nil, err
	}

	realLocalBackend := localBackend.Inner().(*local.Local)

	return &TableImporter{
		JobImportParam:     param,
		LoadDataController: e,
		id:                 id,
		backend:            realLocalBackend,
		tableInfo: &checkpoints.TidbTableInfo{
			ID:   e.Table.Meta().ID,
			Name: e.Table.Meta().Name.O,
			Core: e.Table.Meta(),
		},
		encTable: tbl,
		dbID:     e.DBID,
		logger:   e.logger.With(zap.String("import-id", id)),
		// this is the value we use for 50TiB data parallel import.
		// this might not be the optimal value.
		// todo: use different default for single-node import and distributed import.
		regionSplitSize: 2 * int64(config.SplitRegionSize),
		regionSplitKeys: 2 * int64(config.SplitRegionKeys),
		diskQuota:       adjustDiskQuota(int64(e.DiskQuota), dir, e.logger),
		diskQuotaLock:   new(sync.RWMutex),
	}, nil
}

// TableImporter is a table importer.
type TableImporter struct {
	*JobImportParam
	*LoadDataController
	// id is the unique id for this importer.
	// it's the task id if we are running in distributed framework, else it's an
	// uuid. we use this id to create a unique directory for this importer.
	id        string
	backend   *local.Local
	tableInfo *checkpoints.TidbTableInfo
	// this table has a separate id allocator used to record the max row id allocated.
	encTable table.Table
	dbID     int64

	// the kv store we get is a cached store, so we can't close it.
	logger          *zap.Logger
	regionSplitSize int64
	regionSplitKeys int64
	diskQuota       int64
	diskQuotaLock   *sync.RWMutex

	rowCh chan QueryRow
}

func (ti *TableImporter) getKVEncoder(chunk *checkpoints.ChunkCheckpoint) (KVEncoder, error) {
	cfg := &encode.EncodingConfig{
		SessionOptions: encode.SessionOptions{
			SQLMode:        ti.SQLMode,
			Timestamp:      chunk.Timestamp,
			SysVars:        ti.ImportantSysVars,
			AutoRandomSeed: chunk.Chunk.PrevRowIDMax,
		},
		Path:   chunk.FileMeta.Path,
		Table:  ti.encTable,
		Logger: log.Logger{Logger: ti.logger.With(zap.String("path", chunk.FileMeta.Path))},
	}
	return NewTableKVEncoder(cfg, ti)
}

// OpenIndexEngine opens an index engine.
func (ti *TableImporter) OpenIndexEngine(ctx context.Context, engineID int32) (*backend.EngineConfig, *backend.OpenedEngine, error) {
	idxEngineCfg := &backend.EngineConfig{
		TableInfo: ti.tableInfo,
	}
	// todo: getTotalRawFileSize returns size of all data files, but in distributed framework,
	// we create one index engine for each engine, should reflect this in the future.
	idxEngineCfg.Local = &backend.LocalEngineConfig{
		Compact:            true,
		CompactConcurrency: 4,
		CompactThreshold:   32 * units.GiB,
	}
	fullTableName := ti.fullTableName()
	// todo: cleanup all engine data on any error since we don't support checkpoint for now
	// some return path, didn't make sure all data engine and index engine are cleaned up.
	// maybe we can add this in upper level to clean the whole local-sort directory
	mgr := backend.MakeBackend(ti.backend)
	engine, err := mgr.OpenEngine(ctx, idxEngineCfg, fullTableName, engineID)
	return idxEngineCfg, engine, err
}

// OpenDataEngine opens a data engine.
func (ti *TableImporter) OpenDataEngine(ctx context.Context, engineID int32) (*backend.EngineConfig, *backend.OpenedEngine, error) {
	dataEngineCfg := &backend.EngineConfig{
		TableInfo: ti.tableInfo,
	}
	// todo: support checking IsRowOrdered later.
	// also see test result here: https://github.com/pingcap/tidb/pull/47147
	//if ti.tableMeta.IsRowOrdered {
	//	dataEngineCfg.Local.Compact = true
	//	dataEngineCfg.Local.CompactConcurrency = 4
	//	dataEngineCfg.Local.CompactThreshold = local.CompactionUpperThreshold
	//}
	mgr := backend.MakeBackend(ti.backend)
	engine, err := mgr.OpenEngine(ctx, dataEngineCfg, ti.fullTableName(), engineID)
	return dataEngineCfg, engine, err
}

// FullTableName return FQDN of the table.
func (ti *TableImporter) fullTableName() string {
	return common.UniqueTable(ti.DBName, ti.Table.Meta().Name.O)
}

// Close implements the io.Closer interface.
func (ti *TableImporter) Close() error {
	ti.backend.Close()
	return nil
}

// Allocators returns allocators used to record max used ID, i.e. PanickingAllocators.
func (ti *TableImporter) Allocators() autoid.Allocators {
	return ti.encTable.Allocators(nil)
}

// SetSelectedRowCh sets the channel to receive selected rows.
func (ti *TableImporter) SetSelectedRowCh(ch chan QueryRow) {
	ti.rowCh = ch
}

func (ti *TableImporter) closeAndCleanupEngine(engine *backend.OpenedEngine, engineCfg *backend.EngineConfig) {
	// outer context might be done, so we create a new context here.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	closedEngine, err := engine.Close(ctx, engineCfg)
	if err != nil {
		ti.logger.Error("close engine failed", zap.Error(err))
		return
	}
	if err = closedEngine.Cleanup(ctx); err != nil {
		ti.logger.Error("cleanup engine failed", zap.Error(err))
	}
}

// ImportSelectedRows imports selected rows.
func (ti *TableImporter) ImportSelectedRows(ctx context.Context, se sessionctx.Context) (*JobImportResult, error) {
	var (
		err                           error
		dataEngine, indexEngine       *backend.OpenedEngine
		dataEngineCfg, indexEngineCfg *backend.EngineConfig
	)
	defer func() {
		if dataEngine != nil {
			ti.closeAndCleanupEngine(dataEngine, dataEngineCfg)
		}
		if indexEngine != nil {
			ti.closeAndCleanupEngine(indexEngine, indexEngineCfg)
		}
	}()
	dataEngineCfg, dataEngine, err = ti.OpenDataEngine(ctx, 1)
	if err != nil {
		return nil, err
	}
	indexEngineCfg, indexEngine, err = ti.OpenIndexEngine(ctx, -1)
	if err != nil {
		return nil, err
	}

	var (
		mu         sync.Mutex
		checksum   verify.KVChecksum
		colSizeMap = make(map[int64]int64)
	)
	eg, egCtx := errgroup.WithContext(ctx)
	for i := 0; i < int(ti.ThreadCnt); i++ {
		eg.Go(func() error {
			chunkCheckpoint := checkpoints.ChunkCheckpoint{}
			progress := asyncloaddata.NewProgress(false)
			defer func() {
				mu.Lock()
				defer mu.Unlock()
				checksum.Add(&chunkCheckpoint.Checksum)
				for k, v := range progress.GetColSize() {
					colSizeMap[k] += v
				}
			}()
			return ProcessChunk(egCtx, &chunkCheckpoint, ti, dataEngine, indexEngine, progress, ti.logger)
		})
	}
	if err = eg.Wait(); err != nil {
		return nil, err
	}

	closedDataEngine, err := dataEngine.Close(ctx, dataEngineCfg)
	if err != nil {
		return nil, err
	}
	failpoint.Inject("mockImportFromSelectErr", func() {
		failpoint.Return(nil, errors.New("mock import from select error"))
	})
	if err = closedDataEngine.Import(ctx, ti.regionSplitSize, ti.regionSplitKeys); err != nil {
		return nil, err
	}
	dataKVCount := ti.backend.GetImportedKVCount(dataEngine.GetEngineUUID())

	closedIndexEngine, err := indexEngine.Close(ctx, indexEngineCfg)
	if err != nil {
		return nil, err
	}
	if err = closedIndexEngine.Import(ctx, ti.regionSplitSize, ti.regionSplitKeys); err != nil {
		return nil, err
	}

	allocators := ti.Allocators()
	maxIDs := map[autoid.AllocatorType]int64{
		autoid.RowIDAllocType:    allocators.Get(autoid.RowIDAllocType).Base(),
		autoid.AutoIncrementType: allocators.Get(autoid.AutoIncrementType).Base(),
		autoid.AutoRandomType:    allocators.Get(autoid.AutoRandomType).Base(),
	}
	if err = postProcess(ctx, se, maxIDs, ti.Plan, checksum, ti.logger); err != nil {
		return nil, err
	}

	return &JobImportResult{
		Affected:   uint64(dataKVCount),
		ColSizeMap: colSizeMap,
	}, nil
}

func adjustDiskQuota(diskQuota int64, sortDir string, logger *zap.Logger) int64 {
	sz, err := common.GetStorageSize(sortDir)
	if err != nil {
		logger.Warn("failed to get storage size", zap.Error(err))
		if diskQuota != 0 {
			return diskQuota
		}
		logger.Info("use default quota instead", zap.Int64("quota", int64(DefaultDiskQuota)))
		return int64(DefaultDiskQuota)
	}

	maxDiskQuota := int64(float64(sz.Capacity) * 0.8)
	switch {
	case diskQuota == 0:
		logger.Info("use 0.8 of the storage size as default disk quota",
			zap.String("quota", units.HumanSize(float64(maxDiskQuota))))
		return maxDiskQuota
	case diskQuota > maxDiskQuota:
		logger.Warn("disk quota is larger than 0.8 of the storage size, use 0.8 of the storage size instead",
			zap.String("quota", units.HumanSize(float64(maxDiskQuota))))
		return maxDiskQuota
	default:
		return diskQuota
	}
}

// postProcess does the post-processing for the task.
func postProcess(
	ctx context.Context,
	se sessionctx.Context,
	maxIDs map[autoid.AllocatorType]int64,
	plan *Plan,
	localChecksum verify.KVChecksum,
	logger *zap.Logger,
) (err error) {
	callLog := log.BeginTask(logger.With(zap.Any("checksum", localChecksum)), "post process")
	defer func() {
		callLog.End(zap.ErrorLevel, err)
	}()

	if err = RebaseAllocatorBases(ctx, maxIDs, plan, logger); err != nil {
		return err
	}

	return VerifyChecksum(ctx, plan, localChecksum, se, logger)
}

// RebaseAllocatorBases rebase the allocator bases to the maxIDs.
func RebaseAllocatorBases(ctx context.Context, maxIDs map[autoid.AllocatorType]int64, plan *Plan, logger *zap.Logger) (err error) {
	callLog := log.BeginTask(logger.With(zap.Any("max-ids", maxIDs)), "rebase allocators")
	defer func() {
		callLog.End(zap.ErrorLevel, err)
	}()

	if !common.TableHasAutoID(plan.DesiredTableInfo) {
		return nil
	}

	tidbCfg := tidb.GetGlobalConfig()
	hostPort := net.JoinHostPort("127.0.0.1", strconv.Itoa(int(tidbCfg.Status.StatusPort)))
	tls, err2 := common.NewTLS(
		tidbCfg.Security.ClusterSSLCA,
		tidbCfg.Security.ClusterSSLCert,
		tidbCfg.Security.ClusterSSLKey,
		hostPort,
		nil, nil, nil,
	)
	if err2 != nil {
		return err2
	}

	// no need to close kvStore, since it's a cached store.
	kvStore, err2 := GetCachedKVStoreFrom(tidbCfg.Path, tls)
	if err2 != nil {
		return errors.Trace(err2)
	}
	err = common.RebaseTableAllocators(ctx, maxIDs, kvStore, plan.DBID, plan.DesiredTableInfo)
	return errors.Trace(err)
}

type autoIDRequirement struct {
	store   tidbkv.Storage
	etcdCli *clientv3.Client
}

func (r *autoIDRequirement) Store() tidbkv.Storage {
	return r.store
}

func (r *autoIDRequirement) GetEtcdClient() *clientv3.Client {
	return r.etcdCli
}

// VerifyChecksum verify the checksum of the table.
func VerifyChecksum(ctx context.Context, plan *Plan, localChecksum verify.KVChecksum, se sessionctx.Context, logger *zap.Logger) error {
	if plan.Checksum == config.OpLevelOff {
		return nil
	}
	logger.Info("local checksum", zap.Object("checksum", &localChecksum))

	failpoint.Inject("waitCtxDone", func() {
		<-ctx.Done()
	})

	remoteChecksum, err := checksumTable(ctx, se, plan, logger)
	if err != nil {
		if plan.Checksum != config.OpLevelOptional {
			return err
		}
		logger.Warn("checksumTable failed, will skip this error and go on", zap.Error(err))
	}
	if remoteChecksum != nil {
		if !remoteChecksum.IsEqual(&localChecksum) {
			err2 := common.ErrChecksumMismatch.GenWithStackByArgs(
				remoteChecksum.Checksum, localChecksum.Sum(),
				remoteChecksum.TotalKVs, localChecksum.SumKVS(),
				remoteChecksum.TotalBytes, localChecksum.SumSize(),
			)
			if plan.Checksum == config.OpLevelOptional {
				logger.Warn("verify checksum failed, but checksum is optional, will skip it", zap.Error(err2))
				err2 = nil
			}
			return err2
		}
		logger.Info("checksum pass", zap.Object("local", &localChecksum))
	}
	return nil
}

func checksumTable(ctx context.Context, se sessionctx.Context, plan *Plan, logger *zap.Logger) (*local.RemoteChecksum, error) {
	var (
		tableName                    = common.UniqueTable(plan.DBName, plan.TableInfo.Name.L)
		sql                          = "ADMIN CHECKSUM TABLE " + tableName
		maxErrorRetryCount           = 3
		distSQLScanConcurrencyFactor = 1
		remoteChecksum               *local.RemoteChecksum
		txnErr                       error
	)

	ctx = util.WithInternalSourceType(ctx, tidbkv.InternalImportInto)
	for i := 0; i < maxErrorRetryCount; i++ {
		txnErr = func() error {
			// increase backoff weight
			if err := setBackoffWeight(se, plan, logger); err != nil {
				logger.Warn("set tidb_backoff_weight failed", zap.Error(err))
			}

			distSQLScanConcurrency := se.GetSessionVars().DistSQLScanConcurrency()
			se.GetSessionVars().SetDistSQLScanConcurrency(mathutil.Max(distSQLScanConcurrency/distSQLScanConcurrencyFactor, 4))
			defer func() {
				se.GetSessionVars().SetDistSQLScanConcurrency(distSQLScanConcurrency)
			}()

			// TODO: add resource group name

			rs, err := sqlexec.ExecSQL(ctx, se, sql)
			if err != nil {
				return err
			}
			if len(rs) < 1 {
				return errors.New("empty checksum result")
			}

			failpoint.Inject("errWhenChecksum", func() {
				if i == 0 {
					failpoint.Return(errors.New("occur an error when checksum, coprocessor task terminated due to exceeding the deadline"))
				}
			})

			// ADMIN CHECKSUM TABLE <schema>.<table>  example.
			// 	mysql> admin checksum table test.t;
			// +---------+------------+---------------------+-----------+-------------+
			// | Db_name | Table_name | Checksum_crc64_xor  | Total_kvs | Total_bytes |
			// +---------+------------+---------------------+-----------+-------------+
			// | test    | t          | 8520875019404689597 |   7296873 |   357601387 |
			// +---------+------------+-------------
			remoteChecksum = &local.RemoteChecksum{
				Schema:     rs[0].GetString(0),
				Table:      rs[0].GetString(1),
				Checksum:   rs[0].GetUint64(2),
				TotalKVs:   rs[0].GetUint64(3),
				TotalBytes: rs[0].GetUint64(4),
			}
			return nil
		}()
		if !common.IsRetryableError(txnErr) {
			break
		}
		distSQLScanConcurrencyFactor *= 2
		logger.Warn("retry checksum table", zap.Int("retry count", i+1), zap.Error(txnErr))
	}
	return remoteChecksum, txnErr
}

func setBackoffWeight(se sessionctx.Context, plan *Plan, logger *zap.Logger) error {
	backoffWeight := 30
	if val, ok := plan.ImportantSysVars[variable.TiDBBackOffWeight]; ok {
		if weight, err := strconv.Atoi(val); err == nil && weight > backoffWeight {
			backoffWeight = weight
		}
	}
	logger.Info("set backoff weight", zap.Int("weight", backoffWeight))
	return se.GetSessionVars().SetSystemVar(variable.TiDBBackOffWeight, strconv.Itoa(backoffWeight))
}
