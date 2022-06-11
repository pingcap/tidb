// Copyright 2020 PingCAP, Inc.
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

package local

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/coreos/go-semver/semver"
	"github.com/docker/go-units"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/errorpb"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/errormanager"
	"github.com/pingcap/tidb/br/pkg/lightning/glue"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/manual"
	"github.com/pingcap/tidb/br/pkg/lightning/metric"
	"github.com/pingcap/tidb/br/pkg/lightning/tikv"
	"github.com/pingcap/tidb/br/pkg/lightning/worker"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	split "github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/version"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/mathutil"
	tikverror "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/oracle"
	tikvclient "github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/atomic"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

const (
	dialTimeout             = 5 * time.Minute
	maxRetryTimes           = 5
	defaultRetryBackoffTime = 3 * time.Second
	// maxWriteAndIngestRetryTimes is the max retry times for write and ingest.
	// A large retry times is for tolerating tikv cluster failures.
	maxWriteAndIngestRetryTimes = 30
	maxRetryBackoffTime         = 30 * time.Second

	gRPCKeepAliveTime    = 10 * time.Minute
	gRPCKeepAliveTimeout = 5 * time.Minute
	gRPCBackOffMaxDelay  = 10 * time.Minute

	// The max ranges count in a batch to split and scatter.
	maxBatchSplitRanges = 4096

	propRangeIndex = "tikv.range_index"

	defaultPropSizeIndexDistance = 4 * units.MiB
	defaultPropKeysIndexDistance = 40 * 1024

	// the lower threshold of max open files for pebble db.
	openFilesLowerThreshold = 128

	duplicateDBName = "duplicates"
	scanRegionLimit = 128
)

var (
	// Local backend is compatible with TiDB [4.0.0, NextMajorVersion).
	localMinTiDBVersion = *semver.New("4.0.0")
	localMinTiKVVersion = *semver.New("4.0.0")
	localMinPDVersion   = *semver.New("4.0.0")
	localMaxTiDBVersion = version.NextMajorVersion()
	localMaxTiKVVersion = version.NextMajorVersion()
	localMaxPDVersion   = version.NextMajorVersion()
	tiFlashMinVersion   = *semver.New("4.0.5")

	errorEngineClosed = errors.New("engine is closed")
)

// ImportClientFactory is factory to create new import client for specific store.
type ImportClientFactory interface {
	Create(ctx context.Context, storeID uint64) (sst.ImportSSTClient, error)
	Close()
}

type importClientFactoryImpl struct {
	conns          *common.GRPCConns
	splitCli       split.SplitClient
	tls            *common.TLS
	tcpConcurrency int
}

func newImportClientFactoryImpl(splitCli split.SplitClient, tls *common.TLS, tcpConcurrency int) *importClientFactoryImpl {
	return &importClientFactoryImpl{
		conns:          common.NewGRPCConns(),
		splitCli:       splitCli,
		tls:            tls,
		tcpConcurrency: tcpConcurrency,
	}
}

func (f *importClientFactoryImpl) makeConn(ctx context.Context, storeID uint64) (*grpc.ClientConn, error) {
	store, err := f.splitCli.GetStore(ctx, storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	opt := grpc.WithInsecure()
	if f.tls.TLSConfig() != nil {
		opt = grpc.WithTransportCredentials(credentials.NewTLS(f.tls.TLSConfig()))
	}
	ctx, cancel := context.WithTimeout(ctx, dialTimeout)

	bfConf := backoff.DefaultConfig
	bfConf.MaxDelay = gRPCBackOffMaxDelay
	// we should use peer address for tiflash. for tikv, peer address is empty
	addr := store.GetPeerAddress()
	if addr == "" {
		addr = store.GetAddress()
	}
	conn, err := grpc.DialContext(
		ctx,
		addr,
		opt,
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: bfConf}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                gRPCKeepAliveTime,
			Timeout:             gRPCKeepAliveTimeout,
			PermitWithoutStream: true,
		}),
	)
	cancel()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return conn, nil
}

func (f *importClientFactoryImpl) getGrpcConn(ctx context.Context, storeID uint64) (*grpc.ClientConn, error) {
	return f.conns.GetGrpcConn(ctx, storeID, f.tcpConcurrency,
		func(ctx context.Context) (*grpc.ClientConn, error) {
			return f.makeConn(ctx, storeID)
		})
}

func (f *importClientFactoryImpl) Create(ctx context.Context, storeID uint64) (sst.ImportSSTClient, error) {
	conn, err := f.getGrpcConn(ctx, storeID)
	if err != nil {
		return nil, err
	}
	return sst.NewImportSSTClient(conn), nil
}

func (f *importClientFactoryImpl) Close() {
	f.conns.Close()
}

// Range record start and end key for localStoreDir.DB
// so we can write it to tikv in streaming
type Range struct {
	start []byte
	end   []byte
}

type local struct {
	engines sync.Map // sync version of map[uuid.UUID]*Engine

	pdCtl    *pdutil.PdController
	splitCli split.SplitClient
	tikvCli  *tikvclient.KVStore
	tls      *common.TLS
	pdAddr   string
	g        glue.Glue

	localStoreDir string

	rangeConcurrency  *worker.Pool
	ingestConcurrency *worker.Pool
	batchWriteKVPairs int
	checkpointEnabled bool

	dupeConcurrency int
	maxOpenFiles    int

	engineMemCacheSize      int
	localWriterMemCacheSize int64
	supportMultiIngest      bool

	checkTiKVAvaliable  bool
	duplicateDetection  bool
	duplicateDB         *pebble.DB
	keyAdapter          KeyAdapter
	errorMgr            *errormanager.ErrorManager
	importClientFactory ImportClientFactory

	bufferPool *membuf.Pool
}

func openDuplicateDB(storeDir string) (*pebble.DB, error) {
	dbPath := filepath.Join(storeDir, duplicateDBName)
	// TODO: Optimize the opts for better write.
	opts := &pebble.Options{
		TablePropertyCollectors: []func() pebble.TablePropertyCollector{
			newRangePropertiesCollector,
		},
	}
	return pebble.Open(dbPath, opts)
}

// NewLocalBackend creates new connections to tikv.
func NewLocalBackend(
	ctx context.Context,
	tls *common.TLS,
	cfg *config.Config,
	g glue.Glue,
	maxOpenFiles int,
	errorMgr *errormanager.ErrorManager,
) (backend.Backend, error) {
	localFile := cfg.TikvImporter.SortedKVDir
	rangeConcurrency := cfg.TikvImporter.RangeConcurrency

	pdCtl, err := pdutil.NewPdController(ctx, cfg.TiDB.PdAddr, tls.TLSConfig(), tls.ToPDSecurityOption())
	if err != nil {
		return backend.MakeBackend(nil), common.NormalizeOrWrapErr(common.ErrCreatePDClient, err)
	}
	splitCli := split.NewSplitClient(pdCtl.GetPDClient(), tls.TLSConfig(), false)

	shouldCreate := true
	if cfg.Checkpoint.Enable {
		if info, err := os.Stat(localFile); err != nil {
			if !os.IsNotExist(err) {
				return backend.MakeBackend(nil), err
			}
		} else if info.IsDir() {
			shouldCreate = false
		}
	}

	if shouldCreate {
		err = os.Mkdir(localFile, 0o700)
		if err != nil {
			return backend.MakeBackend(nil), common.ErrInvalidSortedKVDir.Wrap(err).GenWithStackByArgs(localFile)
		}
	}

	var duplicateDB *pebble.DB
	if cfg.TikvImporter.DuplicateResolution != config.DupeResAlgNone {
		duplicateDB, err = openDuplicateDB(localFile)
		if err != nil {
			return backend.MakeBackend(nil), common.ErrOpenDuplicateDB.Wrap(err).GenWithStackByArgs()
		}
	}

	// The following copies tikv.NewTxnClient without creating yet another pdClient.
	spkv, err := tikvclient.NewEtcdSafePointKV(strings.Split(cfg.TiDB.PdAddr, ","), tls.TLSConfig())
	if err != nil {
		return backend.MakeBackend(nil), common.ErrCreateKVClient.Wrap(err).GenWithStackByArgs()
	}
	rpcCli := tikvclient.NewRPCClient(tikvclient.WithSecurity(tls.ToTiKVSecurityConfig()))
	pdCliForTiKV := &tikvclient.CodecPDClient{Client: pdCtl.GetPDClient()}
	tikvCli, err := tikvclient.NewKVStore("lightning-local-backend", pdCliForTiKV, spkv, rpcCli)
	if err != nil {
		return backend.MakeBackend(nil), common.ErrCreateKVClient.Wrap(err).GenWithStackByArgs()
	}
	importClientFactory := newImportClientFactoryImpl(splitCli, tls, rangeConcurrency)
	duplicateDetection := cfg.TikvImporter.DuplicateResolution != config.DupeResAlgNone
	keyAdapter := KeyAdapter(noopKeyAdapter{})
	if duplicateDetection {
		keyAdapter = dupDetectKeyAdapter{}
	}
	local := &local{
		engines:  sync.Map{},
		pdCtl:    pdCtl,
		splitCli: splitCli,
		tikvCli:  tikvCli,
		tls:      tls,
		pdAddr:   cfg.TiDB.PdAddr,
		g:        g,

		localStoreDir:     localFile,
		rangeConcurrency:  worker.NewPool(ctx, rangeConcurrency, "range"),
		ingestConcurrency: worker.NewPool(ctx, rangeConcurrency*2, "ingest"),
		dupeConcurrency:   rangeConcurrency * 2,
		batchWriteKVPairs: cfg.TikvImporter.SendKVPairs,
		checkpointEnabled: cfg.Checkpoint.Enable,
		maxOpenFiles:      mathutil.Max(maxOpenFiles, openFilesLowerThreshold),

		engineMemCacheSize:      int(cfg.TikvImporter.EngineMemCacheSize),
		localWriterMemCacheSize: int64(cfg.TikvImporter.LocalWriterMemCacheSize),
		duplicateDetection:      duplicateDetection,
		checkTiKVAvaliable:      cfg.App.CheckRequirements,
		duplicateDB:             duplicateDB,
		keyAdapter:              keyAdapter,
		errorMgr:                errorMgr,
		importClientFactory:     importClientFactory,
		bufferPool:              membuf.NewPool(membuf.WithAllocator(manual.Allocator{})),
	}
	if err = local.checkMultiIngestSupport(ctx); err != nil {
		return backend.MakeBackend(nil), common.ErrCheckMultiIngest.Wrap(err).GenWithStackByArgs()
	}

	return backend.MakeBackend(local), nil
}

func (local *local) checkMultiIngestSupport(ctx context.Context) error {
	stores, err := local.pdCtl.GetPDClient().GetAllStores(ctx, pd.WithExcludeTombstone())
	if err != nil {
		return errors.Trace(err)
	}

	hasTiFlash := false
	for _, s := range stores {
		if s.State == metapb.StoreState_Up && version.IsTiFlash(s) {
			hasTiFlash = true
			break
		}
	}

	for _, s := range stores {
		// skip stores that are not online
		if s.State != metapb.StoreState_Up || version.IsTiFlash(s) {
			continue
		}
		var err error
		for i := 0; i < maxRetryTimes; i++ {
			if i > 0 {
				select {
				case <-time.After(100 * time.Millisecond):
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			client, err1 := local.getImportClient(ctx, s.Id)
			if err1 != nil {
				err = err1
				log.L().Warn("get import client failed", zap.Error(err), zap.String("store", s.Address))
				continue
			}
			_, err = client.MultiIngest(ctx, &sst.MultiIngestRequest{})
			if err == nil {
				break
			}
			if st, ok := status.FromError(err); ok {
				if st.Code() == codes.Unimplemented {
					log.L().Info("multi ingest not support", zap.Any("unsupported store", s))
					local.supportMultiIngest = false
					return nil
				}
			}
			log.L().Warn("check multi ingest support failed", zap.Error(err), zap.String("store", s.Address),
				zap.Int("retry", i))
		}
		if err != nil {
			// if the cluster contains no TiFlash store, we don't need the multi-ingest feature,
			// so in this condition, downgrade the logic instead of return an error.
			if hasTiFlash {
				return errors.Trace(err)
			}
			log.L().Warn("check multi failed all retry, fallback to false", log.ShortError(err))
			local.supportMultiIngest = false
			return nil
		}
	}

	local.supportMultiIngest = true
	log.L().Info("multi ingest support")
	return nil
}

// rlock read locks a local file and returns the Engine instance if it exists.
func (local *local) rLockEngine(engineId uuid.UUID) *Engine {
	if e, ok := local.engines.Load(engineId); ok {
		engine := e.(*Engine)
		engine.rLock()
		return engine
	}
	return nil
}

// lock locks a local file and returns the Engine instance if it exists.
func (local *local) lockEngine(engineID uuid.UUID, state importMutexState) *Engine {
	if e, ok := local.engines.Load(engineID); ok {
		engine := e.(*Engine)
		engine.lock(state)
		return engine
	}
	return nil
}

// tryRLockAllEngines tries to read lock all engines, return all `Engine`s that are successfully locked.
func (local *local) tryRLockAllEngines() []*Engine {
	var allEngines []*Engine
	local.engines.Range(func(k, v interface{}) bool {
		engine := v.(*Engine)
		// skip closed engine
		if engine.tryRLock() {
			if !engine.closed.Load() {
				allEngines = append(allEngines, engine)
			} else {
				engine.rUnlock()
			}
		}
		return true
	})
	return allEngines
}

// lockAllEnginesUnless tries to lock all engines, unless those which are already locked in the
// state given by ignoreStateMask. Returns the list of locked engines.
func (local *local) lockAllEnginesUnless(newState, ignoreStateMask importMutexState) []*Engine {
	var allEngines []*Engine
	local.engines.Range(func(k, v interface{}) bool {
		engine := v.(*Engine)
		if engine.lockUnless(newState, ignoreStateMask) {
			allEngines = append(allEngines, engine)
		}
		return true
	})
	return allEngines
}

// Close the local backend.
func (local *local) Close() {
	allEngines := local.lockAllEnginesUnless(importMutexStateClose, 0)
	local.engines = sync.Map{}

	for _, engine := range allEngines {
		engine.Close()
		engine.unlock()
	}
	local.importClientFactory.Close()
	local.bufferPool.Destroy()

	if local.duplicateDB != nil {
		// Check if there are duplicates that are not collected.
		iter := local.duplicateDB.NewIter(&pebble.IterOptions{})
		hasDuplicates := iter.First()
		allIsWell := true
		if err := iter.Error(); err != nil {
			log.L().Warn("iterate duplicate db failed", zap.Error(err))
			allIsWell = false
		}
		if err := iter.Close(); err != nil {
			log.L().Warn("close duplicate db iter failed", zap.Error(err))
			allIsWell = false
		}
		if err := local.duplicateDB.Close(); err != nil {
			log.L().Warn("close duplicate db failed", zap.Error(err))
			allIsWell = false
		}
		// If checkpoint is disabled, or we don't detect any duplicate, then this duplicate
		// db dir will be useless, so we clean up this dir.
		if allIsWell && (!local.checkpointEnabled || !hasDuplicates) {
			if err := os.RemoveAll(filepath.Join(local.localStoreDir, duplicateDBName)); err != nil {
				log.L().Warn("remove duplicate db file failed", zap.Error(err))
			}
		}
		local.duplicateDB = nil
	}

	// if checkpoint is disable or we finish load all data successfully, then files in this
	// dir will be useless, so we clean up this dir and all files in it.
	if !local.checkpointEnabled || common.IsEmptyDir(local.localStoreDir) {
		err := os.RemoveAll(local.localStoreDir)
		if err != nil {
			log.L().Warn("remove local db file failed", zap.Error(err))
		}
	}

	local.tikvCli.Close()
	local.pdCtl.Close()
}

// FlushEngine ensure the written data is saved successfully, to make sure no data lose after restart
func (local *local) FlushEngine(ctx context.Context, engineID uuid.UUID) error {
	engine := local.rLockEngine(engineID)

	// the engine cannot be deleted after while we've acquired the lock identified by UUID.
	if engine == nil {
		return errors.Errorf("engine '%s' not found", engineID)
	}
	defer engine.rUnlock()
	if engine.closed.Load() {
		return nil
	}
	return engine.flushEngineWithoutLock(ctx)
}

func (local *local) FlushAllEngines(parentCtx context.Context) (err error) {
	allEngines := local.tryRLockAllEngines()
	defer func() {
		for _, engine := range allEngines {
			engine.rUnlock()
		}
	}()

	eg, ctx := errgroup.WithContext(parentCtx)
	for _, engine := range allEngines {
		e := engine
		eg.Go(func() error {
			return e.flushEngineWithoutLock(ctx)
		})
	}
	return eg.Wait()
}

func (local *local) RetryImportDelay() time.Duration {
	return defaultRetryBackoffTime
}

func (local *local) ShouldPostProcess() bool {
	return true
}

func (local *local) openEngineDB(engineUUID uuid.UUID, readOnly bool) (*pebble.DB, error) {
	opt := &pebble.Options{
		MemTableSize: local.engineMemCacheSize,
		// the default threshold value may cause write stall.
		MemTableStopWritesThreshold: 8,
		MaxConcurrentCompactions:    16,
		// set threshold to half of the max open files to avoid trigger compaction
		L0CompactionThreshold: math.MaxInt32,
		L0StopWritesThreshold: math.MaxInt32,
		LBaseMaxBytes:         16 * units.TiB,
		MaxOpenFiles:          local.maxOpenFiles,
		DisableWAL:            true,
		ReadOnly:              readOnly,
		TablePropertyCollectors: []func() pebble.TablePropertyCollector{
			newRangePropertiesCollector,
		},
	}
	// set level target file size to avoid pebble auto triggering compaction that split ingest SST files into small SST.
	opt.Levels = []pebble.LevelOptions{
		{
			TargetFileSize: 16 * units.GiB,
		},
	}

	dbPath := filepath.Join(local.localStoreDir, engineUUID.String())
	db, err := pebble.Open(dbPath, opt)
	return db, errors.Trace(err)
}

// OpenEngine must be called with holding mutex of Engine.
func (local *local) OpenEngine(ctx context.Context, cfg *backend.EngineConfig, engineUUID uuid.UUID) error {
	engineCfg := backend.LocalEngineConfig{}
	if cfg.Local != nil {
		engineCfg = *cfg.Local
	}
	db, err := local.openEngineDB(engineUUID, false)
	if err != nil {
		return err
	}

	sstDir := engineSSTDir(local.localStoreDir, engineUUID)
	if err := os.RemoveAll(sstDir); err != nil {
		return errors.Trace(err)
	}
	if !common.IsDirExists(sstDir) {
		if err := os.Mkdir(sstDir, 0o750); err != nil {
			return errors.Trace(err)
		}
	}
	engineCtx, cancel := context.WithCancel(ctx)

	e, _ := local.engines.LoadOrStore(engineUUID, &Engine{
		UUID:               engineUUID,
		sstDir:             sstDir,
		sstMetasChan:       make(chan metaOrFlush, 64),
		ctx:                engineCtx,
		cancel:             cancel,
		config:             engineCfg,
		tableInfo:          cfg.TableInfo,
		duplicateDetection: local.duplicateDetection,
		duplicateDB:        local.duplicateDB,
		errorMgr:           local.errorMgr,
		keyAdapter:         local.keyAdapter,
	})
	engine := e.(*Engine)
	engine.db = db
	engine.sstIngester = dbSSTIngester{e: engine}
	if err = engine.loadEngineMeta(); err != nil {
		return errors.Trace(err)
	}
	if err = local.allocateTSIfNotExists(ctx, engine); err != nil {
		return errors.Trace(err)
	}
	engine.wg.Add(1)
	go engine.ingestSSTLoop()
	return nil
}

func (local *local) allocateTSIfNotExists(ctx context.Context, engine *Engine) error {
	if engine.TS > 0 {
		return nil
	}
	physical, logical, err := local.pdCtl.GetPDClient().GetTS(ctx)
	if err != nil {
		return err
	}
	ts := oracle.ComposeTS(physical, logical)
	engine.TS = ts
	return engine.saveEngineMeta()
}

// CloseEngine closes backend engine by uuid.
func (local *local) CloseEngine(ctx context.Context, cfg *backend.EngineConfig, engineUUID uuid.UUID) error {
	// flush mem table to storage, to free memory,
	// ask others' advise, looks like unnecessary, but with this we can control memory precisely.
	engineI, ok := local.engines.Load(engineUUID)
	if !ok {
		// recovery mode, we should reopen this engine file
		db, err := local.openEngineDB(engineUUID, true)
		if err != nil {
			return err
		}
		engine := &Engine{
			UUID:               engineUUID,
			db:                 db,
			sstMetasChan:       make(chan metaOrFlush),
			tableInfo:          cfg.TableInfo,
			keyAdapter:         local.keyAdapter,
			duplicateDetection: local.duplicateDetection,
			duplicateDB:        local.duplicateDB,
			errorMgr:           local.errorMgr,
		}
		engine.sstIngester = dbSSTIngester{e: engine}
		if err = engine.loadEngineMeta(); err != nil {
			return err
		}
		local.engines.Store(engineUUID, engine)
		return nil
	}

	engine := engineI.(*Engine)
	engine.rLock()
	if engine.closed.Load() {
		engine.rUnlock()
		return nil
	}

	err := engine.flushEngineWithoutLock(ctx)
	engine.rUnlock()

	// use mutex to make sure we won't close sstMetasChan while other routines
	// trying to do flush.
	engine.lock(importMutexStateClose)
	engine.closed.Store(true)
	close(engine.sstMetasChan)
	engine.unlock()
	if err != nil {
		return errors.Trace(err)
	}
	engine.wg.Wait()
	return engine.ingestErr.Get()
}

func (local *local) getImportClient(ctx context.Context, storeID uint64) (sst.ImportSSTClient, error) {
	return local.importClientFactory.Create(ctx, storeID)
}

type rangeStats struct {
	count      int64
	totalBytes int64
}

// WriteToTiKV writer engine key-value pairs to tikv and return the sst meta generated by tikv.
// we don't need to do cleanup for the pairs written to tikv if encounters an error,
// tikv will takes the responsibility to do so.
func (local *local) WriteToTiKV(
	ctx context.Context,
	engine *Engine,
	region *split.RegionInfo,
	start, end []byte,
	regionSplitSize int64,
	regionSplitKeys int64,
) ([]*sst.SSTMeta, Range, rangeStats, error) {
	failpoint.Inject("WriteToTiKVNotEnoughDiskSpace", func(_ failpoint.Value) {
		failpoint.Return(nil, Range{}, rangeStats{},
			errors.Errorf("The available disk of TiKV (%s) only left %d, and capacity is %d", "", 0, 0))
	})
	if local.checkTiKVAvaliable {
		for _, peer := range region.Region.GetPeers() {
			var e error
			for i := 0; i < maxRetryTimes; i++ {
				store, err := local.pdCtl.GetStoreInfo(ctx, peer.StoreId)
				if err != nil {
					e = err
					continue
				}
				if store.Status.Capacity > 0 {
					// The available disk percent of TiKV
					ratio := store.Status.Available * 100 / store.Status.Capacity
					if ratio < 10 {
						return nil, Range{}, rangeStats{}, errors.Errorf("The available disk of TiKV (%s) only left %d, and capacity is %d",
							store.Store.Address, store.Status.Available, store.Status.Capacity)
					}
				}
				break
			}
			if e != nil {
				log.L().Error("failed to get StoreInfo from pd http api", zap.Error(e))
			}
		}
	}
	begin := time.Now()
	regionRange := intersectRange(region.Region, Range{start: start, end: end})
	opt := &pebble.IterOptions{LowerBound: regionRange.start, UpperBound: regionRange.end}
	iter := engine.newKVIter(ctx, opt)
	defer iter.Close()

	stats := rangeStats{}

	iter.First()
	if iter.Error() != nil {
		return nil, Range{}, stats, errors.Annotate(iter.Error(), "failed to read the first key")
	}
	if !iter.Valid() {
		log.L().Info("keys within region is empty, skip ingest", logutil.Key("start", start),
			logutil.Key("regionStart", region.Region.StartKey), logutil.Key("end", end),
			logutil.Key("regionEnd", region.Region.EndKey))
		return nil, regionRange, stats, nil
	}
	firstKey := codec.EncodeBytes([]byte{}, iter.Key())
	iter.Last()
	if iter.Error() != nil {
		return nil, Range{}, stats, errors.Annotate(iter.Error(), "failed to seek to the last key")
	}
	lastKey := codec.EncodeBytes([]byte{}, iter.Key())

	u := uuid.New()
	meta := &sst.SSTMeta{
		Uuid:        u[:],
		RegionId:    region.Region.GetId(),
		RegionEpoch: region.Region.GetRegionEpoch(),
		Range: &sst.Range{
			Start: firstKey,
			End:   lastKey,
		},
	}

	leaderID := region.Leader.GetId()
	clients := make([]sst.ImportSST_WriteClient, 0, len(region.Region.GetPeers()))
	requests := make([]*sst.WriteRequest, 0, len(region.Region.GetPeers()))
	for _, peer := range region.Region.GetPeers() {
		cli, err := local.getImportClient(ctx, peer.StoreId)
		if err != nil {
			return nil, Range{}, stats, err
		}

		wstream, err := cli.Write(ctx)
		if err != nil {
			return nil, Range{}, stats, errors.Trace(err)
		}

		// Bind uuid for this write request
		req := &sst.WriteRequest{
			Chunk: &sst.WriteRequest_Meta{
				Meta: meta,
			},
		}
		if err = wstream.Send(req); err != nil {
			return nil, Range{}, stats, errors.Trace(err)
		}
		req.Chunk = &sst.WriteRequest_Batch{
			Batch: &sst.WriteBatch{
				CommitTs: engine.TS,
			},
		}
		clients = append(clients, wstream)
		requests = append(requests, req)
	}

	bytesBuf := local.bufferPool.NewBuffer()
	defer bytesBuf.Destroy()
	pairs := make([]*sst.Pair, 0, local.batchWriteKVPairs)
	count := 0
	size := int64(0)
	totalCount := int64(0)
	firstLoop := true
	// if region-split-size <= 96MiB, we bump the threshold a bit to avoid too many retry split
	// because the range-properties is not 100% accurate
	regionMaxSize := regionSplitSize
	if regionSplitSize <= int64(config.SplitRegionSize) {
		regionMaxSize = regionSplitSize * 4 / 3
	}

	for iter.First(); iter.Valid(); iter.Next() {
		size += int64(len(iter.Key()) + len(iter.Value()))
		// here we reuse the `*sst.Pair`s to optimize object allocation
		if firstLoop {
			pair := &sst.Pair{
				Key:   bytesBuf.AddBytes(iter.Key()),
				Value: bytesBuf.AddBytes(iter.Value()),
			}
			pairs = append(pairs, pair)
		} else {
			pairs[count].Key = bytesBuf.AddBytes(iter.Key())
			pairs[count].Value = bytesBuf.AddBytes(iter.Value())
		}
		count++
		totalCount++

		if count >= local.batchWriteKVPairs {
			for i := range clients {
				requests[i].Chunk.(*sst.WriteRequest_Batch).Batch.Pairs = pairs[:count]
				if err := clients[i].Send(requests[i]); err != nil {
					return nil, Range{}, stats, errors.Trace(err)
				}
			}
			count = 0
			bytesBuf.Reset()
			firstLoop = false
		}
		if size >= regionMaxSize || totalCount >= regionSplitKeys {
			break
		}
	}

	if iter.Error() != nil {
		return nil, Range{}, stats, errors.Trace(iter.Error())
	}

	if count > 0 {
		for i := range clients {
			requests[i].Chunk.(*sst.WriteRequest_Batch).Batch.Pairs = pairs[:count]
			if err := clients[i].Send(requests[i]); err != nil {
				return nil, Range{}, stats, errors.Trace(err)
			}
		}
	}

	var leaderPeerMetas []*sst.SSTMeta
	for i, wStream := range clients {
		resp, closeErr := wStream.CloseAndRecv()
		if closeErr != nil {
			return nil, Range{}, stats, errors.Trace(closeErr)
		}
		if resp.Error != nil {
			return nil, Range{}, stats, errors.New(resp.Error.Message)
		}
		if leaderID == region.Region.Peers[i].GetId() {
			leaderPeerMetas = resp.Metas
			log.L().Debug("get metas after write kv stream to tikv", zap.Reflect("metas", leaderPeerMetas))
		}
	}

	// if there is not leader currently, we should directly return an error
	if len(leaderPeerMetas) == 0 {
		log.L().Warn("write to tikv no leader", logutil.Region(region.Region), logutil.Leader(region.Leader),
			zap.Uint64("leader_id", leaderID), logutil.SSTMeta(meta),
			zap.Int64("kv_pairs", totalCount), zap.Int64("total_bytes", size))
		return nil, Range{}, stats, errors.Errorf("write to tikv with no leader returned, region '%d', leader: %d",
			region.Region.Id, leaderID)
	}

	log.L().Debug("write to kv", zap.Reflect("region", region), zap.Uint64("leader", leaderID),
		zap.Reflect("meta", meta), zap.Reflect("return metas", leaderPeerMetas),
		zap.Int64("kv_pairs", totalCount), zap.Int64("total_bytes", size),
		zap.Int64("buf_size", bytesBuf.TotalSize()),
		zap.Stringer("takeTime", time.Since(begin)))

	finishedRange := regionRange
	if iter.Valid() && iter.Next() {
		firstKey := append([]byte{}, iter.Key()...)
		finishedRange = Range{start: regionRange.start, end: firstKey}
		log.L().Info("write to tikv partial finish", zap.Int64("count", totalCount),
			zap.Int64("size", size), logutil.Key("startKey", regionRange.start), logutil.Key("endKey", regionRange.end),
			logutil.Key("remainStart", firstKey), logutil.Key("remainEnd", regionRange.end),
			logutil.Region(region.Region), logutil.Leader(region.Leader))
	}
	stats.count = totalCount
	stats.totalBytes = size

	return leaderPeerMetas, finishedRange, stats, nil
}

func (local *local) Ingest(ctx context.Context, metas []*sst.SSTMeta, region *split.RegionInfo) (*sst.IngestResponse, error) {
	leader := region.Leader
	if leader == nil {
		leader = region.Region.GetPeers()[0]
	}

	cli, err := local.getImportClient(ctx, leader.StoreId)
	if err != nil {
		return nil, err
	}
	reqCtx := &kvrpcpb.Context{
		RegionId:    region.Region.GetId(),
		RegionEpoch: region.Region.GetRegionEpoch(),
		Peer:        leader,
	}

	if !local.supportMultiIngest {
		if len(metas) != 1 {
			return nil, errors.New("batch ingest is not support")
		}
		req := &sst.IngestRequest{
			Context: reqCtx,
			Sst:     metas[0],
		}
		resp, err := cli.Ingest(ctx, req)
		return resp, errors.Trace(err)
	}

	req := &sst.MultiIngestRequest{
		Context: reqCtx,
		Ssts:    metas,
	}
	resp, err := cli.MultiIngest(ctx, req)
	return resp, errors.Trace(err)
}

func splitRangeBySizeProps(fullRange Range, sizeProps *sizeProperties, sizeLimit int64, keysLimit int64) []Range {
	ranges := make([]Range, 0, sizeProps.totalSize/uint64(sizeLimit))
	curSize := uint64(0)
	curKeys := uint64(0)
	curKey := fullRange.start

	sizeProps.iter(func(p *rangeProperty) bool {
		if bytes.Compare(p.Key, curKey) <= 0 {
			return true
		}
		if bytes.Compare(p.Key, fullRange.end) > 0 {
			return false
		}
		curSize += p.Size
		curKeys += p.Keys
		if int64(curSize) >= sizeLimit || int64(curKeys) >= keysLimit {
			ranges = append(ranges, Range{start: curKey, end: p.Key})
			curKey = p.Key
			curSize = 0
			curKeys = 0
		}
		return true
	})

	if bytes.Compare(curKey, fullRange.end) < 0 {
		// If the remaining range is too small, append it to last range.
		if len(ranges) > 0 && curKeys == 0 {
			ranges[len(ranges)-1].end = fullRange.end
		} else {
			ranges = append(ranges, Range{start: curKey, end: fullRange.end})
		}
	}
	return ranges
}

func (local *local) readAndSplitIntoRange(ctx context.Context, engine *Engine, regionSplitSize int64, regionSplitKeys int64) ([]Range, error) {
	iter := engine.newKVIter(ctx, &pebble.IterOptions{})
	defer iter.Close()

	iterError := func(e string) error {
		err := iter.Error()
		if err != nil {
			return errors.Annotate(err, e)
		}
		return errors.New(e)
	}

	var firstKey, lastKey []byte
	if iter.First() {
		firstKey = append([]byte{}, iter.Key()...)
	} else {
		return nil, iterError("could not find first pair")
	}
	if iter.Last() {
		lastKey = append([]byte{}, iter.Key()...)
	} else {
		return nil, iterError("could not find last pair")
	}
	endKey := nextKey(lastKey)

	engineFileTotalSize := engine.TotalSize.Load()
	engineFileLength := engine.Length.Load()

	// <= 96MB no need to split into range
	if engineFileTotalSize <= regionSplitSize && engineFileLength <= regionSplitKeys {
		ranges := []Range{{start: firstKey, end: endKey}}
		return ranges, nil
	}

	logger := log.With(zap.Stringer("engine", engine.UUID))
	sizeProps, err := getSizeProperties(logger, engine.db, local.keyAdapter)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ranges := splitRangeBySizeProps(Range{start: firstKey, end: endKey}, sizeProps,
		regionSplitSize, regionSplitKeys)

	logger.Info("split engine key ranges",
		zap.Int64("totalSize", engineFileTotalSize), zap.Int64("totalCount", engineFileLength),
		logutil.Key("firstKey", firstKey), logutil.Key("lastKey", lastKey),
		zap.Int("ranges", len(ranges)))

	return ranges, nil
}

func (local *local) writeAndIngestByRange(
	ctxt context.Context,
	engine *Engine,
	start, end []byte,
	regionSplitSize int64,
	regionSplitKeys int64,
) error {
	ito := &pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	}

	iter := engine.newKVIter(ctxt, ito)
	defer iter.Close()
	// Needs seek to first because NewIter returns an iterator that is unpositioned
	hasKey := iter.First()
	if iter.Error() != nil {
		return errors.Annotate(iter.Error(), "failed to read the first key")
	}
	if !hasKey {
		log.L().Info("There is no pairs in iterator",
			logutil.Key("start", start),
			logutil.Key("end", end))
		engine.finishedRanges.add(Range{start: start, end: end})
		return nil
	}
	pairStart := append([]byte{}, iter.Key()...)
	iter.Last()
	if iter.Error() != nil {
		return errors.Annotate(iter.Error(), "failed to seek to the last key")
	}
	pairEnd := append([]byte{}, iter.Key()...)

	var regions []*split.RegionInfo
	var err error
	ctx, cancel := context.WithCancel(ctxt)
	defer cancel()

WriteAndIngest:
	for retry := 0; retry < maxRetryTimes; {
		if retry != 0 {
			select {
			case <-time.After(time.Second):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		startKey := codec.EncodeBytes([]byte{}, pairStart)
		endKey := codec.EncodeBytes([]byte{}, nextKey(pairEnd))
		regions, err = split.PaginateScanRegion(ctx, local.splitCli, startKey, endKey, scanRegionLimit)
		if err != nil || len(regions) == 0 {
			log.L().Warn("scan region failed", log.ShortError(err), zap.Int("region_len", len(regions)),
				logutil.Key("startKey", startKey), logutil.Key("endKey", endKey), zap.Int("retry", retry))
			retry++
			continue WriteAndIngest
		}

		for _, region := range regions {
			log.L().Debug("get region", zap.Int("retry", retry), zap.Binary("startKey", startKey),
				zap.Binary("endKey", endKey), zap.Uint64("id", region.Region.GetId()),
				zap.Stringer("epoch", region.Region.GetRegionEpoch()), zap.Binary("start", region.Region.GetStartKey()),
				zap.Binary("end", region.Region.GetEndKey()), zap.Reflect("peers", region.Region.GetPeers()))

			w := local.ingestConcurrency.Apply()
			err = local.writeAndIngestPairs(ctx, engine, region, pairStart, end, regionSplitSize, regionSplitKeys)
			local.ingestConcurrency.Recycle(w)
			if err != nil {
				if !common.IsRetryableError(err) {
					return err
				}
				_, regionStart, _ := codec.DecodeBytes(region.Region.StartKey, []byte{})
				// if we have at least succeeded one region, retry without increasing the retry count
				if bytes.Compare(regionStart, pairStart) > 0 {
					pairStart = regionStart
				} else {
					retry++
				}
				log.L().Info("retry write and ingest kv pairs", logutil.Key("startKey", pairStart),
					logutil.Key("endKey", end), log.ShortError(err), zap.Int("retry", retry))
				continue WriteAndIngest
			}
		}

		return err
	}

	return err
}

type retryType int

const (
	retryNone retryType = iota
	retryWrite
	retryIngest
)

func (local *local) writeAndIngestPairs(
	ctx context.Context,
	engine *Engine,
	region *split.RegionInfo,
	start, end []byte,
	regionSplitSize int64,
	regionSplitKeys int64,
) error {
	var err error

loopWrite:
	for i := 0; i < maxRetryTimes; i++ {
		var metas []*sst.SSTMeta
		var finishedRange Range
		var rangeStats rangeStats
		metas, finishedRange, rangeStats, err = local.WriteToTiKV(ctx, engine, region, start, end, regionSplitSize, regionSplitKeys)
		if err != nil {
			if !common.IsRetryableError(err) {
				return err
			}

			log.L().Warn("write to tikv failed", log.ShortError(err), zap.Int("retry", i))
			continue loopWrite
		}

		if len(metas) == 0 {
			return nil
		}

		batch := 1
		if local.supportMultiIngest {
			batch = len(metas)
		}

		for i := 0; i < len(metas); i += batch {
			start := i * batch
			end := mathutil.Min((i+1)*batch, len(metas))
			ingestMetas := metas[start:end]
			errCnt := 0
			for errCnt < maxRetryTimes {
				log.L().Debug("ingest meta", zap.Reflect("meta", ingestMetas))
				var resp *sst.IngestResponse
				failpoint.Inject("FailIngestMeta", func(val failpoint.Value) {
					// only inject the error once
					switch val.(string) {
					case "notleader":
						resp = &sst.IngestResponse{
							Error: &errorpb.Error{
								NotLeader: &errorpb.NotLeader{
									RegionId: region.Region.Id,
									Leader:   region.Leader,
								},
							},
						}
					case "epochnotmatch":
						resp = &sst.IngestResponse{
							Error: &errorpb.Error{
								EpochNotMatch: &errorpb.EpochNotMatch{
									CurrentRegions: []*metapb.Region{region.Region},
								},
							},
						}
					}
					if resp != nil {
						err = nil
					}
				})
				if resp == nil {
					resp, err = local.Ingest(ctx, ingestMetas, region)
				}
				if err != nil {
					if common.IsContextCanceledError(err) {
						return err
					}
					log.L().Warn("ingest failed", log.ShortError(err), logutil.SSTMetas(ingestMetas),
						logutil.Region(region.Region), logutil.Leader(region.Leader))
					errCnt++
					continue
				}

				var retryTy retryType
				var newRegion *split.RegionInfo
				retryTy, newRegion, err = local.isIngestRetryable(ctx, resp, region, ingestMetas)
				if common.IsContextCanceledError(err) {
					return err
				}
				if err == nil {
					// ingest next meta
					break
				}
				switch retryTy {
				case retryNone:
					log.L().Warn("ingest failed noretry", log.ShortError(err), logutil.SSTMetas(ingestMetas),
						logutil.Region(region.Region), logutil.Leader(region.Leader))
					// met non-retryable error retry whole Write procedure
					return err
				case retryWrite:
					region = newRegion
					continue loopWrite
				case retryIngest:
					region = newRegion
					continue
				}
			}
		}

		if err != nil {
			log.L().Warn("write and ingest region, will retry import full range", log.ShortError(err),
				logutil.Region(region.Region), logutil.Key("start", start),
				logutil.Key("end", end))
		} else {
			engine.importedKVSize.Add(rangeStats.totalBytes)
			engine.importedKVCount.Add(rangeStats.count)
			engine.finishedRanges.add(finishedRange)
			metric.BytesCounter.WithLabelValues(metric.BytesStateImported).Add(float64(rangeStats.totalBytes))
		}
		return errors.Trace(err)
	}

	return errors.Trace(err)
}

func (local *local) writeAndIngestByRanges(ctx context.Context, engine *Engine, ranges []Range, regionSplitSize int64, regionSplitKeys int64) error {
	if engine.Length.Load() == 0 {
		// engine is empty, this is likes because it's a index engine but the table contains no index
		log.L().Info("engine contains no data", zap.Stringer("uuid", engine.UUID))
		return nil
	}
	log.L().Debug("the ranges Length write to tikv", zap.Int("Length", len(ranges)))

	var allErrLock sync.Mutex
	var allErr error
	var wg sync.WaitGroup
	metErr := atomic.NewBool(false)

	for _, r := range ranges {
		startKey := r.start
		endKey := r.end
		w := local.rangeConcurrency.Apply()
		// if meet error here, skip try more here to allow fail fast.
		if metErr.Load() {
			local.rangeConcurrency.Recycle(w)
			break
		}
		wg.Add(1)
		go func(w *worker.Worker) {
			defer func() {
				local.rangeConcurrency.Recycle(w)
				wg.Done()
			}()
			var err error
			// max retry backoff time: 2+4+8+16+30*26=810s
			backOffTime := time.Second
			for i := 0; i < maxWriteAndIngestRetryTimes; i++ {
				err = local.writeAndIngestByRange(ctx, engine, startKey, endKey, regionSplitSize, regionSplitKeys)
				if err == nil || common.IsContextCanceledError(err) {
					return
				}
				if !common.IsRetryableError(err) {
					break
				}
				log.L().Warn("write and ingest by range failed",
					zap.Int("retry time", i+1), log.ShortError(err))
				backOffTime *= 2
				if backOffTime > maxRetryBackoffTime {
					backOffTime = maxRetryBackoffTime
				}
				select {
				case <-time.After(backOffTime):
				case <-ctx.Done():
					return
				}
			}

			allErrLock.Lock()
			allErr = multierr.Append(allErr, err)
			allErrLock.Unlock()
			if err != nil {
				metErr.Store(true)
			}
		}(w)
	}

	// wait for all sub tasks finish to avoid panic. if we return on the first error,
	// the outer tasks may close the pebble db but some sub tasks still read from the db
	wg.Wait()
	if allErr == nil {
		return ctx.Err()
	}
	return allErr
}

func (local *local) ImportEngine(ctx context.Context, engineUUID uuid.UUID, regionSplitSize, regionSplitKeys int64) error {
	lf := local.lockEngine(engineUUID, importMutexStateImport)
	if lf == nil {
		// skip if engine not exist. See the comment of `CloseEngine` for more detail.
		return nil
	}
	defer lf.unlock()

	lfTotalSize := lf.TotalSize.Load()
	lfLength := lf.Length.Load()
	if lfTotalSize == 0 {
		log.L().Info("engine contains no kv, skip import", zap.Stringer("engine", engineUUID))
		return nil
	}
	kvRegionSplitSize, kvRegionSplitKeys, err := getRegionSplitSizeKeys(ctx, local.pdCtl.GetPDClient(), local.tls)
	if err == nil {
		if kvRegionSplitSize > regionSplitSize {
			regionSplitSize = kvRegionSplitSize
		}
		if kvRegionSplitKeys > regionSplitKeys {
			regionSplitKeys = kvRegionSplitKeys
		}
	} else {
		log.L().Warn("fail to get region split keys and size", zap.Error(err))
	}

	// split sorted file into range by 96MB size per file
	ranges, err := local.readAndSplitIntoRange(ctx, lf, regionSplitSize, regionSplitKeys)
	if err != nil {
		return err
	}

	log.L().Info("start import engine", zap.Stringer("uuid", engineUUID),
		zap.Int("ranges", len(ranges)), zap.Int64("count", lfLength), zap.Int64("size", lfTotalSize))
	for {
		unfinishedRanges := lf.unfinishedRanges(ranges)
		if len(unfinishedRanges) == 0 {
			break
		}
		log.L().Info("import engine unfinished ranges", zap.Int("count", len(unfinishedRanges)))

		// if all the kv can fit in one region, skip split regions. TiDB will split one region for
		// the table when table is created.
		needSplit := len(unfinishedRanges) > 1 || lfTotalSize > regionSplitSize || lfLength > regionSplitKeys
		// split region by given ranges
		for i := 0; i < maxRetryTimes; i++ {
			err = local.SplitAndScatterRegionInBatches(ctx, unfinishedRanges, lf.tableInfo, needSplit, regionSplitSize, maxBatchSplitRanges)
			if err == nil || common.IsContextCanceledError(err) {
				break
			}

			log.L().Warn("split and scatter failed in retry", zap.Stringer("uuid", engineUUID),
				log.ShortError(err), zap.Int("retry", i))
		}
		if err != nil {
			log.L().Error("split & scatter ranges failed", zap.Stringer("uuid", engineUUID), log.ShortError(err))
			return err
		}

		// start to write to kv and ingest
		err = local.writeAndIngestByRanges(ctx, lf, unfinishedRanges, regionSplitSize, regionSplitKeys)
		if err != nil {
			log.L().Error("write and ingest engine failed", log.ShortError(err))
			return err
		}
	}

	log.L().Info("import engine success", zap.Stringer("uuid", engineUUID),
		zap.Int64("size", lfTotalSize), zap.Int64("kvs", lfLength),
		zap.Int64("importedSize", lf.importedKVSize.Load()), zap.Int64("importedCount", lf.importedKVCount.Load()))
	return nil
}

func (local *local) CollectLocalDuplicateRows(ctx context.Context, tbl table.Table, tableName string, opts *kv.SessionOptions) (hasDupe bool, err error) {
	logger := log.With(zap.String("table", tableName)).Begin(zap.InfoLevel, "[detect-dupe] collect local duplicate keys")
	defer func() {
		logger.End(zap.ErrorLevel, err)
	}()

	atomicHasDupe := atomic.NewBool(false)
	duplicateManager, err := NewDuplicateManager(tbl, tableName, local.splitCli, local.tikvCli,
		local.errorMgr, opts, local.dupeConcurrency, atomicHasDupe)
	if err != nil {
		return false, errors.Trace(err)
	}
	if err := duplicateManager.CollectDuplicateRowsFromDupDB(ctx, local.duplicateDB, local.keyAdapter); err != nil {
		return false, errors.Trace(err)
	}
	return atomicHasDupe.Load(), nil
}

func (local *local) CollectRemoteDuplicateRows(ctx context.Context, tbl table.Table, tableName string, opts *kv.SessionOptions) (hasDupe bool, err error) {
	logger := log.With(zap.String("table", tableName)).Begin(zap.InfoLevel, "[detect-dupe] collect remote duplicate keys")
	defer func() {
		logger.End(zap.ErrorLevel, err)
	}()

	atomicHasDupe := atomic.NewBool(false)
	duplicateManager, err := NewDuplicateManager(tbl, tableName, local.splitCli, local.tikvCli,
		local.errorMgr, opts, local.dupeConcurrency, atomicHasDupe)
	if err != nil {
		return false, errors.Trace(err)
	}
	if err := duplicateManager.CollectDuplicateRowsFromTiKV(ctx, local.importClientFactory); err != nil {
		return false, errors.Trace(err)
	}
	return atomicHasDupe.Load(), nil
}

func (local *local) ResolveDuplicateRows(ctx context.Context, tbl table.Table, tableName string, algorithm config.DuplicateResolutionAlgorithm) (err error) {
	logger := log.With(zap.String("table", tableName)).Begin(zap.InfoLevel, "[resolve-dupe] resolve duplicate rows")
	defer func() {
		logger.End(zap.ErrorLevel, err)
	}()

	switch algorithm {
	case config.DupeResAlgRecord, config.DupeResAlgNone:
		logger.Warn("[resolve-dupe] skipping resolution due to selected algorithm. this table will become inconsistent!", zap.Stringer("algorithm", algorithm))
		return nil
	case config.DupeResAlgRemove:
		break
	default:
		panic(fmt.Sprintf("[resolve-dupe] unknown resolution algorithm %v", algorithm))
	}

	// TODO: reuse the *kv.SessionOptions from NewEncoder for picking the correct time zone.
	decoder, err := kv.NewTableKVDecoder(tbl, tableName, &kv.SessionOptions{
		SQLMode: mysql.ModeStrictAllTables,
	})
	if err != nil {
		return err
	}

	errLimiter := rate.NewLimiter(1, 1)
	pool := utils.NewWorkerPool(uint(local.dupeConcurrency), "resolve duplicate rows")
	err = local.errorMgr.ResolveAllConflictKeys(
		ctx, tableName, pool,
		func(ctx context.Context, handleRows [][2][]byte) error {
			for {
				err := local.deleteDuplicateRows(ctx, logger, handleRows, decoder)
				if err == nil {
					return nil
				}
				if log.IsContextCanceledError(err) {
					return err
				}
				if !tikverror.IsErrWriteConflict(errors.Cause(err)) {
					logger.Warn("delete duplicate rows encounter error", log.ShortError(err))
				}
				if err = errLimiter.Wait(ctx); err != nil {
					return err
				}
			}
		},
	)
	return errors.Trace(err)
}

func (local *local) deleteDuplicateRows(ctx context.Context, logger *log.Task, handleRows [][2][]byte, decoder *kv.TableKVDecoder) (err error) {
	// Starts a Delete transaction.
	txn, err := local.tikvCli.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = txn.Commit(ctx)
		} else {
			if rollbackErr := txn.Rollback(); rollbackErr != nil {
				logger.Warn("failed to rollback transaction", zap.Error(rollbackErr))
			}
		}
	}()

	deleteKey := func(key []byte) error {
		logger.Debug("[resolve-dupe] will delete key", logutil.Key("key", key))
		return txn.Delete(key)
	}

	// Collect all rows & index keys into the deletion transaction.
	// (if the number of duplicates is small this should fit entirely in memory)
	// (Txn's MemBuf's bufferSizeLimit is currently infinity)
	for _, handleRow := range handleRows {
		logger.Debug("[resolve-dupe] found row to resolve",
			logutil.Key("handle", handleRow[0]),
			logutil.Key("row", handleRow[1]))

		if err := deleteKey(handleRow[0]); err != nil {
			return err
		}

		handle, err := decoder.DecodeHandleFromRowKey(handleRow[0])
		if err != nil {
			return err
		}

		err = decoder.IterRawIndexKeys(handle, handleRow[1], deleteKey)
		if err != nil {
			return err
		}
	}

	logger.Debug("[resolve-dupe] number of KV pairs to be deleted", zap.Int("count", txn.Len()))
	return nil
}

func (local *local) ResetEngine(ctx context.Context, engineUUID uuid.UUID) error {
	// the only way to reset the engine + reclaim the space is to delete and reopen it 🤷
	localEngine := local.lockEngine(engineUUID, importMutexStateClose)
	if localEngine == nil {
		log.L().Warn("could not find engine in cleanupEngine", zap.Stringer("uuid", engineUUID))
		return nil
	}
	defer localEngine.unlock()
	if err := localEngine.Close(); err != nil {
		return err
	}
	if err := localEngine.Cleanup(local.localStoreDir); err != nil {
		return err
	}
	db, err := local.openEngineDB(engineUUID, false)
	if err == nil {
		localEngine.db = db
		localEngine.engineMeta = engineMeta{}
		if !common.IsDirExists(localEngine.sstDir) {
			if err := os.Mkdir(localEngine.sstDir, 0o750); err != nil {
				return errors.Trace(err)
			}
		}
		if err = local.allocateTSIfNotExists(ctx, localEngine); err != nil {
			return errors.Trace(err)
		}
	}
	localEngine.pendingFileSize.Store(0)
	localEngine.finishedRanges.reset()

	return err
}

func (local *local) CleanupEngine(ctx context.Context, engineUUID uuid.UUID) error {
	localEngine := local.lockEngine(engineUUID, importMutexStateClose)
	// release this engine after import success
	if localEngine == nil {
		log.L().Warn("could not find engine in cleanupEngine", zap.Stringer("uuid", engineUUID))
		return nil
	}
	defer localEngine.unlock()

	// since closing the engine causes all subsequent operations on it panic,
	// we make sure to delete it from the engine map before calling Close().
	// (note that Close() returning error does _not_ mean the pebble DB
	// remains open/usable.)
	local.engines.Delete(engineUUID)
	err := localEngine.Close()
	if err != nil {
		return err
	}
	err = localEngine.Cleanup(local.localStoreDir)
	if err != nil {
		return err
	}
	localEngine.TotalSize.Store(0)
	localEngine.Length.Store(0)
	return nil
}

func (local *local) CheckRequirements(ctx context.Context, checkCtx *backend.CheckCtx) error {
	// TODO: support lightning via SQL
	db, _ := local.g.GetDB()
	versionStr, err := version.FetchVersion(ctx, db)
	if err != nil {
		return errors.Trace(err)
	}
	if err := checkTiDBVersion(ctx, versionStr, localMinTiDBVersion, localMaxTiDBVersion); err != nil {
		return err
	}
	if err := tikv.CheckPDVersion(ctx, local.tls, local.pdAddr, localMinPDVersion, localMaxPDVersion); err != nil {
		return err
	}
	if err := tikv.CheckTiKVVersion(ctx, local.tls, local.pdAddr, localMinTiKVVersion, localMaxTiKVVersion); err != nil {
		return err
	}

	serverInfo := version.ParseServerInfo(versionStr)
	return checkTiFlashVersion(ctx, local.g, checkCtx, *serverInfo.ServerVersion)
}

func checkTiDBVersion(_ context.Context, versionStr string, requiredMinVersion, requiredMaxVersion semver.Version) error {
	return version.CheckTiDBVersion(versionStr, requiredMinVersion, requiredMaxVersion)
}

var tiFlashReplicaQuery = "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TIFLASH_REPLICA WHERE REPLICA_COUNT > 0;"

type tblName struct {
	schema string
	name   string
}

type tblNames []tblName

func (t tblNames) String() string {
	var b strings.Builder
	b.WriteByte('[')
	for i, n := range t {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(common.UniqueTable(n.schema, n.name))
	}
	b.WriteByte(']')
	return b.String()
}

// check TiFlash replicas.
// local backend doesn't support TiFlash before tidb v4.0.5
func checkTiFlashVersion(ctx context.Context, g glue.Glue, checkCtx *backend.CheckCtx, tidbVersion semver.Version) error {
	if tidbVersion.Compare(tiFlashMinVersion) >= 0 {
		return nil
	}

	res, err := g.GetSQLExecutor().QueryStringsWithLog(ctx, tiFlashReplicaQuery, "fetch tiflash replica info", log.L())
	if err != nil {
		return errors.Annotate(err, "fetch tiflash replica info failed")
	}

	tiFlashTablesMap := make(map[tblName]struct{}, len(res))
	for _, tblInfo := range res {
		name := tblName{schema: tblInfo[0], name: tblInfo[1]}
		tiFlashTablesMap[name] = struct{}{}
	}

	tiFlashTables := make(tblNames, 0)
	for _, dbMeta := range checkCtx.DBMetas {
		for _, tblMeta := range dbMeta.Tables {
			if len(tblMeta.DataFiles) == 0 {
				continue
			}
			name := tblName{schema: tblMeta.DB, name: tblMeta.Name}
			if _, ok := tiFlashTablesMap[name]; ok {
				tiFlashTables = append(tiFlashTables, name)
			}
		}
	}

	if len(tiFlashTables) > 0 {
		helpInfo := "Please either upgrade TiDB to version >= 4.0.5 or add TiFlash replica after load data."
		return errors.Errorf("lightning local backend doesn't support TiFlash in this TiDB version. conflict tables: %s. "+helpInfo, tiFlashTables)
	}
	return nil
}

func (local *local) FetchRemoteTableModels(ctx context.Context, schemaName string) ([]*model.TableInfo, error) {
	return tikv.FetchRemoteTableModelsFromTLS(ctx, local.tls, schemaName)
}

func (local *local) MakeEmptyRows() kv.Rows {
	return kv.MakeRowsFromKvPairs(nil)
}

func (local *local) NewEncoder(tbl table.Table, options *kv.SessionOptions) (kv.Encoder, error) {
	return kv.NewTableKVEncoder(tbl, options)
}

func engineSSTDir(storeDir string, engineUUID uuid.UUID) string {
	return filepath.Join(storeDir, engineUUID.String()+".sst")
}

func (local *local) LocalWriter(ctx context.Context, cfg *backend.LocalWriterConfig, engineUUID uuid.UUID) (backend.EngineWriter, error) {
	e, ok := local.engines.Load(engineUUID)
	if !ok {
		return nil, errors.Errorf("could not find engine for %s", engineUUID.String())
	}
	engine := e.(*Engine)
	return openLocalWriter(cfg, engine, local.localWriterMemCacheSize, local.bufferPool.NewBuffer())
}

func openLocalWriter(cfg *backend.LocalWriterConfig, engine *Engine, cacheSize int64, kvBuffer *membuf.Buffer) (*Writer, error) {
	w := &Writer{
		engine:             engine,
		memtableSizeLimit:  cacheSize,
		kvBuffer:           kvBuffer,
		isKVSorted:         cfg.IsKVSorted,
		isWriteBatchSorted: true,
	}
	// pre-allocate a long enough buffer to avoid a lot of runtime.growslice
	// this can help save about 3% of CPU.
	if !w.isKVSorted {
		w.writeBatch = make([]common.KvPair, units.MiB)
	}
	engine.localWriters.Store(w, nil)
	return w, nil
}

func (local *local) isIngestRetryable(
	ctx context.Context,
	resp *sst.IngestResponse,
	region *split.RegionInfo,
	metas []*sst.SSTMeta,
) (retryType, *split.RegionInfo, error) {
	if resp.GetError() == nil {
		return retryNone, nil, nil
	}

	getRegion := func() (*split.RegionInfo, error) {
		for i := 0; ; i++ {
			newRegion, err := local.splitCli.GetRegion(ctx, region.Region.GetStartKey())
			if err != nil {
				return nil, errors.Trace(err)
			}
			if newRegion != nil {
				return newRegion, nil
			}
			log.L().Warn("get region by key return nil, will retry", logutil.Region(region.Region), logutil.Leader(region.Leader),
				zap.Int("retry", i))
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Second):
			}
		}
	}

	var newRegion *split.RegionInfo
	var err error
	switch errPb := resp.GetError(); {
	case errPb.NotLeader != nil:
		if newLeader := errPb.GetNotLeader().GetLeader(); newLeader != nil {
			newRegion = &split.RegionInfo{
				Leader: newLeader,
				Region: region.Region,
			}
		} else {
			newRegion, err = getRegion()
			if err != nil {
				return retryNone, nil, errors.Trace(err)
			}
		}
		// TODO: because in some case, TiKV may return retryable error while the ingest is succeeded.
		// Thus directly retry ingest may cause TiKV panic. So always return retryWrite here to avoid
		// this issue.
		// See: https://github.com/tikv/tikv/issues/9496
		return retryWrite, newRegion, common.ErrKVNotLeader.GenWithStack(errPb.GetMessage())
	case errPb.EpochNotMatch != nil:
		if currentRegions := errPb.GetEpochNotMatch().GetCurrentRegions(); currentRegions != nil {
			var currentRegion *metapb.Region
			for _, r := range currentRegions {
				if insideRegion(r, metas) {
					currentRegion = r
					break
				}
			}
			if currentRegion != nil {
				var newLeader *metapb.Peer
				for _, p := range currentRegion.Peers {
					if p.GetStoreId() == region.Leader.GetStoreId() {
						newLeader = p
						break
					}
				}
				if newLeader != nil {
					newRegion = &split.RegionInfo{
						Leader: newLeader,
						Region: currentRegion,
					}
				}
			}
		}
		retryTy := retryNone
		if newRegion != nil {
			retryTy = retryWrite
		}
		return retryTy, newRegion, common.ErrKVEpochNotMatch.GenWithStack(errPb.GetMessage())
	case strings.Contains(errPb.Message, "raft: proposal dropped"):
		// TODO: we should change 'Raft raft: proposal dropped' to a error type like 'NotLeader'
		newRegion, err = getRegion()
		if err != nil {
			return retryNone, nil, errors.Trace(err)
		}
		return retryWrite, newRegion, errors.New(errPb.GetMessage())
	case errPb.ServerIsBusy != nil:
		return retryNone, nil, common.ErrKVServerIsBusy.GenWithStack(errPb.GetMessage())
	case errPb.RegionNotFound != nil:
		return retryNone, nil, common.ErrKVRegionNotFound.GenWithStack(errPb.GetMessage())
	}
	return retryNone, nil, errors.Errorf("non-retryable error: %s", resp.GetError().GetMessage())
}

// return the smallest []byte that is bigger than current bytes.
// special case when key is empty, empty bytes means infinity in our context, so directly return itself.
func nextKey(key []byte) []byte {
	if len(key) == 0 {
		return []byte{}
	}

	// in tikv <= 4.x, tikv will truncate the row key, so we should fetch the next valid row key
	// See: https://github.com/tikv/tikv/blob/f7f22f70e1585d7ca38a59ea30e774949160c3e8/components/raftstore/src/coprocessor/split_observer.rs#L36-L41
	if tablecodec.IsRecordKey(key) {
		tableID, handle, _ := tablecodec.DecodeRecordKey(key)
		nextHandle := handle.Next()
		// int handle overflow, use the next table prefix as nextKey
		if nextHandle.Compare(handle) <= 0 {
			return tablecodec.EncodeTablePrefix(tableID + 1)
		}
		return tablecodec.EncodeRowKeyWithHandle(tableID, nextHandle)
	}

	// if key is an index, directly append a 0x00 to the key.
	res := make([]byte, 0, len(key)+1)
	res = append(res, key...)
	res = append(res, 0)
	return res
}

func (local *local) EngineFileSizes() (res []backend.EngineFileSize) {
	local.engines.Range(func(k, v interface{}) bool {
		engine := v.(*Engine)
		res = append(res, engine.getEngineFileSize())
		return true
	})
	return
}

var getSplitConfFromStoreFunc = getSplitConfFromStore

// return region split size, region split keys, error
func getSplitConfFromStore(ctx context.Context, host string, tls *common.TLS) (int64, int64, error) {
	var (
		nested struct {
			Coprocessor struct {
				RegionSplitSize string `json:"region-split-size"`
				RegionSplitKeys int64  `json:"region-split-keys"`
			} `json:"coprocessor"`
		}
	)
	if err := tls.WithHost(host).GetJSON(ctx, "/config", &nested); err != nil {
		return 0, 0, errors.Trace(err)
	}
	splitSize, err := units.FromHumanSize(nested.Coprocessor.RegionSplitSize)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}

	return splitSize, nested.Coprocessor.RegionSplitKeys, nil
}

// return region split size, region split keys, error
func getRegionSplitSizeKeys(ctx context.Context, cli pd.Client, tls *common.TLS) (int64, int64, error) {
	stores, err := cli.GetAllStores(ctx, pd.WithExcludeTombstone())
	if err != nil {
		return 0, 0, err
	}
	for _, store := range stores {
		if store.StatusAddress == "" || version.IsTiFlash(store) {
			continue
		}
		serverInfo := infoschema.ServerInfo{
			Address:    store.Address,
			StatusAddr: store.StatusAddress,
		}
		serverInfo.ResolveLoopBackAddr()
		regionSplitSize, regionSplitKeys, err := getSplitConfFromStoreFunc(ctx, serverInfo.StatusAddr, tls)
		if err == nil {
			return regionSplitSize, regionSplitKeys, nil
		}
		log.L().Warn("get region split size and keys failed", zap.Error(err), zap.String("store", serverInfo.StatusAddr))
	}
	return 0, 0, errors.New("get region split size and keys failed")
}
