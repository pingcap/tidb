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

package local

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/docker/go-units"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/manual"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/oracle"
	tikvclient "github.com/tikv/client-go/v2/tikv"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	// RunInTest indicates whether the current process is running in test.
	RunInTest bool
	// LastAlloc is the last ID allocator.
	LastAlloc manual.Allocator
)

// StoreHelper have some api to help encode or store KV data
type StoreHelper interface {
	GetTS(ctx context.Context) (physical, logical int64, err error)
	GetTiKVCodec() tikvclient.Codec
}

// engineManager manages all engines, either local or external.
type engineManager struct {
	BackendConfig
	StoreHelper
	engines        sync.Map // sync version of map[uuid.UUID]*Engine
	externalEngine map[uuid.UUID]common.Engine
	bufferPool     *membuf.Pool
	duplicateDB    *pebble.DB
	keyAdapter     common.KeyAdapter
	logger         log.Logger
}

func newEngineManager(config BackendConfig, storeHelper StoreHelper, logger log.Logger) (_ *engineManager, err error) {
	var duplicateDB *pebble.DB
	defer func() {
		if err != nil && duplicateDB != nil {
			_ = duplicateDB.Close()
		}
	}()

	if err = prepareSortDir(config); err != nil {
		return nil, err
	}

	keyAdapter := common.KeyAdapter(common.NoopKeyAdapter{})
	if config.DupeDetectEnabled {
		duplicateDB, err = openDuplicateDB(config.LocalStoreDir)
		if err != nil {
			return nil, common.ErrOpenDuplicateDB.Wrap(err).GenWithStackByArgs()
		}
		keyAdapter = common.DupDetectKeyAdapter{}
	}
	alloc := manual.Allocator{}
	if RunInTest {
		alloc.RefCnt = new(atomic.Int64)
		LastAlloc = alloc
	}
	return &engineManager{
		BackendConfig:  config,
		StoreHelper:    storeHelper,
		engines:        sync.Map{},
		externalEngine: map[uuid.UUID]common.Engine{},
		bufferPool:     membuf.NewPool(membuf.WithAllocator(alloc)),
		duplicateDB:    duplicateDB,
		keyAdapter:     keyAdapter,
		logger:         logger,
	}, nil
}

// rlock read locks a local file and returns the Engine instance if it exists.
func (em *engineManager) rLockEngine(engineID uuid.UUID) *Engine {
	if e, ok := em.engines.Load(engineID); ok {
		engine := e.(*Engine)
		engine.rLock()
		return engine
	}
	return nil
}

// lock locks a local file and returns the Engine instance if it exists.
func (em *engineManager) lockEngine(engineID uuid.UUID, state importMutexState) *Engine {
	if e, ok := em.engines.Load(engineID); ok {
		engine := e.(*Engine)
		engine.lock(state)
		return engine
	}
	return nil
}

// tryRLockAllEngines tries to read lock all engines, return all `Engine`s that are successfully locked.
func (em *engineManager) tryRLockAllEngines() []*Engine {
	var allEngines []*Engine
	em.engines.Range(func(_, v any) bool {
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
func (em *engineManager) lockAllEnginesUnless(newState, ignoreStateMask importMutexState) []*Engine {
	var allEngines []*Engine
	em.engines.Range(func(_, v any) bool {
		engine := v.(*Engine)
		if engine.lockUnless(newState, ignoreStateMask) {
			allEngines = append(allEngines, engine)
		}
		return true
	})
	return allEngines
}

// flushEngine ensure the written data is saved successfully, to make sure no data lose after restart
func (em *engineManager) flushEngine(ctx context.Context, engineID uuid.UUID) error {
	engine := em.rLockEngine(engineID)

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

// flushAllEngines flush all engines.
func (em *engineManager) flushAllEngines(parentCtx context.Context) (err error) {
	allEngines := em.tryRLockAllEngines()
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

func (em *engineManager) openEngineDB(engineUUID uuid.UUID, readOnly bool) (*pebble.DB, error) {
	opt := &pebble.Options{
		MemTableSize: uint64(em.MemTableSize),
		// the default threshold value may cause write stall.
		MemTableStopWritesThreshold: 8,
		MaxConcurrentCompactions:    func() int { return 16 },
		// set threshold to half of the max open files to avoid trigger compaction
		L0CompactionThreshold: math.MaxInt32,
		L0StopWritesThreshold: math.MaxInt32,
		LBaseMaxBytes:         16 * units.TiB,
		MaxOpenFiles:          em.MaxOpenFiles,
		DisableWAL:            true,
		ReadOnly:              readOnly,
		TablePropertyCollectors: []func() pebble.TablePropertyCollector{
			newRangePropertiesCollector,
		},
		DisableAutomaticCompactions: em.DisableAutomaticCompactions,
	}
	// set level target file size to avoid pebble auto triggering compaction that split ingest SST files into small SST.
	opt.Levels = []pebble.LevelOptions{
		{
			TargetFileSize: 16 * units.GiB,
			BlockSize:      em.BlockSize,
		},
	}

	dbPath := filepath.Join(em.LocalStoreDir, engineUUID.String())
	db, err := pebble.Open(dbPath, opt)
	return db, errors.Trace(err)
}

// openEngine must be called with holding mutex of Engine.
func (em *engineManager) openEngine(ctx context.Context, cfg *backend.EngineConfig, engineUUID uuid.UUID) error {
	db, err := em.openEngineDB(engineUUID, false)
	if err != nil {
		return err
	}

	sstDir := engineSSTDir(em.LocalStoreDir, engineUUID)
	if !cfg.KeepSortDir {
		if err := os.RemoveAll(sstDir); err != nil {
			return errors.Trace(err)
		}
	}
	if !common.IsDirExists(sstDir) {
		if err := os.Mkdir(sstDir, 0o750); err != nil {
			return errors.Trace(err)
		}
	}
	engineCtx, cancel := context.WithCancel(ctx)

	e, _ := em.engines.LoadOrStore(engineUUID, &Engine{
		UUID:               engineUUID,
		sstDir:             sstDir,
		sstMetasChan:       make(chan metaOrFlush, 64),
		ctx:                engineCtx,
		cancel:             cancel,
		config:             cfg.Local,
		tableInfo:          cfg.TableInfo,
		duplicateDetection: em.DupeDetectEnabled,
		dupDetectOpt:       em.DuplicateDetectOpt,
		duplicateDB:        em.duplicateDB,
		keyAdapter:         em.keyAdapter,
		logger:             log.FromContext(ctx),
	})
	engine := e.(*Engine)
	engine.lock(importMutexStateOpen)
	defer engine.unlock()
	engine.db.Store(db)
	engine.sstIngester = dbSSTIngester{e: engine}
	if err = engine.loadEngineMeta(); err != nil {
		return errors.Trace(err)
	}
	if engine.TS == 0 && cfg.TS > 0 {
		engine.TS = cfg.TS
		// we don't saveEngineMeta here, we can rely on the caller use the same TS to
		// open the engine again.
	}
	if err = em.allocateTSIfNotExists(ctx, engine); err != nil {
		return errors.Trace(err)
	}
	engine.wg.Add(1)
	go engine.ingestSSTLoop()
	return nil
}

// closeEngine closes backend engine by uuid.
func (em *engineManager) closeEngine(
	ctx context.Context,
	cfg *backend.EngineConfig,
	engineUUID uuid.UUID,
) (errRet error) {
	if externalCfg := cfg.External; externalCfg != nil {
		storeBackend, err := storage.ParseBackend(externalCfg.StorageURI, nil)
		if err != nil {
			return err
		}
		store, err := storage.NewWithDefaultOpt(ctx, storeBackend)
		if err != nil {
			return err
		}
		defer func() {
			if errRet != nil {
				store.Close()
			}
		}()
		ts := cfg.TS
		if ts == 0 {
			physical, logical, err := em.GetTS(ctx)
			if err != nil {
				return err
			}
			ts = oracle.ComposeTS(physical, logical)
		}
		externalEngine := external.NewExternalEngine(
			store,
			externalCfg.DataFiles,
			externalCfg.StatFiles,
			externalCfg.StartKey,
			externalCfg.EndKey,
			externalCfg.SplitKeys,
			externalCfg.RegionSplitSize,
			em.keyAdapter,
			em.DupeDetectEnabled,
			em.duplicateDB,
			em.DuplicateDetectOpt,
			em.WorkerConcurrency,
			ts,
			externalCfg.TotalFileSize,
			externalCfg.TotalKVCount,
			externalCfg.CheckHotspot,
		)
		em.externalEngine[engineUUID] = externalEngine
		return nil
	}

	// flush mem table to storage, to free memory,
	// ask others' advise, looks like unnecessary, but with this we can control memory precisely.
	engineI, ok := em.engines.Load(engineUUID)
	if !ok {
		// recovery mode, we should reopen this engine file
		db, err := em.openEngineDB(engineUUID, true)
		if err != nil {
			return err
		}
		engine := &Engine{
			UUID:               engineUUID,
			sstMetasChan:       make(chan metaOrFlush),
			tableInfo:          cfg.TableInfo,
			keyAdapter:         em.keyAdapter,
			duplicateDetection: em.DupeDetectEnabled,
			dupDetectOpt:       em.DuplicateDetectOpt,
			duplicateDB:        em.duplicateDB,
			logger:             log.FromContext(ctx),
		}
		engine.db.Store(db)
		engine.sstIngester = dbSSTIngester{e: engine}
		if err = engine.loadEngineMeta(); err != nil {
			return errors.Trace(err)
		}
		em.engines.Store(engineUUID, engine)
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

// getImportedKVCount returns the number of imported KV pairs of some engine.
func (em *engineManager) getImportedKVCount(engineUUID uuid.UUID) int64 {
	v, ok := em.engines.Load(engineUUID)
	if !ok {
		// we get it after import, but before clean up, so this should not happen
		// todo: return error
		return 0
	}
	e := v.(*Engine)
	return e.importedKVCount.Load()
}

// getExternalEngineKVStatistics returns kv statistics of some engine.
func (em *engineManager) getExternalEngineKVStatistics(engineUUID uuid.UUID) (
	totalKVSize int64, totalKVCount int64) {
	v, ok := em.externalEngine[engineUUID]
	if !ok {
		return 0, 0
	}
	return v.ImportedStatistics()
}

// resetEngine reset the engine and reclaim the space.
func (em *engineManager) resetEngine(
	ctx context.Context,
	engineUUID uuid.UUID,
	skipAllocTS bool,
) error {
	// the only way to reset the engine + reclaim the space is to delete and reopen it 🤷
	localEngine := em.lockEngine(engineUUID, importMutexStateClose)
	if localEngine == nil {
		if engineI, ok := em.externalEngine[engineUUID]; ok {
			extEngine := engineI.(*external.Engine)
			return extEngine.Reset()
		}

		log.FromContext(ctx).Warn("could not find engine in cleanupEngine", zap.Stringer("uuid", engineUUID))
		return nil
	}
	defer localEngine.unlock()
	if err := localEngine.Close(); err != nil {
		return err
	}
	if err := localEngine.Cleanup(em.LocalStoreDir); err != nil {
		return err
	}
	db, err := em.openEngineDB(engineUUID, false)
	if err == nil {
		localEngine.db.Store(db)
		localEngine.engineMeta = engineMeta{}
		if !common.IsDirExists(localEngine.sstDir) {
			if err := os.Mkdir(localEngine.sstDir, 0o750); err != nil {
				return errors.Trace(err)
			}
		}
		if !skipAllocTS {
			if err = em.allocateTSIfNotExists(ctx, localEngine); err != nil {
				return errors.Trace(err)
			}
		}
	}
	localEngine.pendingFileSize.Store(0)

	return err
}

func (em *engineManager) allocateTSIfNotExists(ctx context.Context, engine *Engine) error {
	if engine.TS > 0 {
		return nil
	}
	physical, logical, err := em.GetTS(ctx)
	if err != nil {
		return err
	}
	ts := oracle.ComposeTS(physical, logical)
	engine.TS = ts
	return engine.saveEngineMeta()
}

// cleanupEngine cleanup the engine and reclaim the space.
func (em *engineManager) cleanupEngine(ctx context.Context, engineUUID uuid.UUID) error {
	localEngine := em.lockEngine(engineUUID, importMutexStateClose)
	// release this engine after import success
	if localEngine == nil {
		if extEngine, ok := em.externalEngine[engineUUID]; ok {
			retErr := extEngine.Close()
			delete(em.externalEngine, engineUUID)
			return retErr
		}
		log.FromContext(ctx).Warn("could not find engine in cleanupEngine", zap.Stringer("uuid", engineUUID))
		return nil
	}
	defer localEngine.unlock()

	// since closing the engine causes all subsequent operations on it panic,
	// we make sure to delete it from the engine map before calling Close().
	// (note that Close() returning error does _not_ mean the pebble DB
	// remains open/usable.)
	em.engines.Delete(engineUUID)
	err := localEngine.Close()
	if err != nil {
		return err
	}
	err = localEngine.Cleanup(em.LocalStoreDir)
	if err != nil {
		return err
	}
	localEngine.TotalSize.Store(0)
	localEngine.Length.Store(0)
	return nil
}

// LocalWriter returns a new local writer.
func (em *engineManager) localWriter(_ context.Context, cfg *backend.LocalWriterConfig, engineUUID uuid.UUID) (backend.EngineWriter, error) {
	e, ok := em.engines.Load(engineUUID)
	if !ok {
		return nil, errors.Errorf("could not find engine for %s", engineUUID.String())
	}
	engine := e.(*Engine)
	return openLocalWriter(cfg, engine, em.GetTiKVCodec(), em.LocalWriterMemCacheSize, em.bufferPool.NewBuffer())
}

func (em *engineManager) engineFileSizes() (res []backend.EngineFileSize) {
	em.engines.Range(func(_, v any) bool {
		engine := v.(*Engine)
		res = append(res, engine.getEngineFileSize())
		return true
	})
	return
}

func (em *engineManager) close() {
	for _, e := range em.externalEngine {
		_ = e.Close()
	}
	em.externalEngine = map[uuid.UUID]common.Engine{}
	allLocalEngines := em.lockAllEnginesUnless(importMutexStateClose, 0)
	for _, e := range allLocalEngines {
		_ = e.Close()
		e.unlock()
	}
	em.engines = sync.Map{}
	em.bufferPool.Destroy()

	if em.duplicateDB != nil {
		// Check if there are duplicates that are not collected.
		iter, err := em.duplicateDB.NewIter(&pebble.IterOptions{})
		if err != nil {
			em.logger.Panic("fail to create iterator")
		}
		hasDuplicates := iter.First()
		allIsWell := true
		if err := iter.Error(); err != nil {
			em.logger.Warn("iterate duplicate db failed", zap.Error(err))
			allIsWell = false
		}
		if err := iter.Close(); err != nil {
			em.logger.Warn("close duplicate db iter failed", zap.Error(err))
			allIsWell = false
		}
		if err := em.duplicateDB.Close(); err != nil {
			em.logger.Warn("close duplicate db failed", zap.Error(err))
			allIsWell = false
		}
		// If checkpoint is disabled, or we don't detect any duplicate, then this duplicate
		// db dir will be useless, so we clean up this dir.
		if allIsWell && (!em.CheckpointEnabled || !hasDuplicates) {
			if err := os.RemoveAll(filepath.Join(em.LocalStoreDir, duplicateDBName)); err != nil {
				em.logger.Warn("remove duplicate db file failed", zap.Error(err))
			}
		}
		em.duplicateDB = nil
	}

	// if checkpoint is disabled, or we finish load all data successfully, then files in this
	// dir will be useless, so we clean up this dir and all files in it.
	if !em.CheckpointEnabled || common.IsEmptyDir(em.LocalStoreDir) {
		err := os.RemoveAll(em.LocalStoreDir)
		if err != nil {
			em.logger.Warn("remove local db file failed", zap.Error(err))
		}
	}
}

func (em *engineManager) getExternalEngine(uuid uuid.UUID) (common.Engine, bool) {
	e, ok := em.externalEngine[uuid]
	return e, ok
}

func (em *engineManager) totalMemoryConsume() int64 {
	var memConsume int64
	em.engines.Range(func(_, v any) bool {
		e := v.(*Engine)
		if e != nil {
			memConsume += e.TotalMemorySize()
		}
		return true
	})
	return memConsume + em.bufferPool.TotalSize()
}

func (em *engineManager) getDuplicateDB() *pebble.DB {
	return em.duplicateDB
}

func (em *engineManager) getKeyAdapter() common.KeyAdapter {
	return em.keyAdapter
}

func (em *engineManager) getBufferPool() *membuf.Pool {
	return em.bufferPool
}

// only used in tests
type slowCreateFS struct {
	vfs.FS
}

// WaitRMFolderChForTest is a channel for testing.
var WaitRMFolderChForTest = make(chan struct{})

func (s slowCreateFS) Create(name string) (vfs.File, error) {
	if strings.Contains(name, "temporary") {
		select {
		case <-WaitRMFolderChForTest:
		case <-time.After(1 * time.Second):
			logutil.BgLogger().Info("no one removes folder")
		}
	}
	return s.FS.Create(name)
}

func openDuplicateDB(storeDir string) (*pebble.DB, error) {
	dbPath := filepath.Join(storeDir, duplicateDBName)
	// TODO: Optimize the opts for better write.
	opts := &pebble.Options{
		TablePropertyCollectors: []func() pebble.TablePropertyCollector{
			newRangePropertiesCollector,
		},
	}
	failpoint.Inject("slowCreateFS", func() {
		opts.FS = slowCreateFS{vfs.Default}
	})
	return pebble.Open(dbPath, opts)
}

func prepareSortDir(config BackendConfig) error {
	shouldCreate := true
	if config.CheckpointEnabled {
		if info, err := os.Stat(config.LocalStoreDir); err != nil {
			if !os.IsNotExist(err) {
				return err
			}
		} else if info.IsDir() {
			shouldCreate = false
		}
	}

	if shouldCreate {
		err := os.Mkdir(config.LocalStoreDir, 0o700)
		if err != nil {
			return common.ErrInvalidSortedKVDir.Wrap(err).GenWithStackByArgs(config.LocalStoreDir)
		}
	}
	return nil
}
