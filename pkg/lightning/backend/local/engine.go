// Copyright 2021 PingCAP, Inc.
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
	"container/heap"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"
	"unsafe"

	"github.com/cockroachdb/pebble"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/pkg/ingestor/engineapi"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/membuf"
	"github.com/pingcap/tidb/pkg/meta/model"
	tidblogutil "github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	engineMetaKey      = []byte{0, 'm', 'e', 't', 'a'}
	normalIterStartKey = []byte{1}
)

type importMutexState uint32

const (
	importMutexStateImport importMutexState = 1 << iota
	importMutexStateClose
	// importMutexStateReadLock is a special state because in this state we lock engine with read lock
	// and add isImportingAtomic with this value. In other state, we directly store with the state value.
	// so this must always the last value of this enum.
	importMutexStateReadLock
	// we need to lock the engine when it's open as we do when it's close, otherwise GetEngienSize may race with OpenEngine
	importMutexStateOpen
)

const (
	// DupDetectDirSuffix is used by pre-deduplication to store the encoded index KV.
	DupDetectDirSuffix = ".dupdetect"
	// DupResultDirSuffix is used by pre-deduplication to store the duplicated row ID.
	DupResultDirSuffix = ".dupresult"
)

// engineMeta contains some field that is necessary to continue the engine restore/import process.
// These field should be written to disk when we update chunk checkpoint
type engineMeta struct {
	TS uint64 `json:"ts"`
	// Length is the number of KV pairs stored by the engine.
	Length atomic.Int64 `json:"length"`
	// TotalSize is the total pre-compressed KV byte size stored by engine.
	TotalSize atomic.Int64 `json:"total_size"`
}

// Engine is a local engine.
type Engine struct {
	engineMeta
	closed       atomic.Bool
	db           atomic.Pointer[pebble.DB]
	UUID         uuid.UUID
	localWriters sync.Map

	regionSplitSize      int64
	regionSplitKeyCnt    int64
	regionSplitKeysCache [][]byte

	// isImportingAtomic is an atomic variable indicating whether this engine is importing.
	// This should not be used as a "spin lock" indicator.
	isImportingAtomic atomic.Uint32
	// flush and ingest sst hold the rlock, other operation hold the wlock.
	mutex sync.RWMutex

	ctx          context.Context
	cancel       context.CancelFunc
	sstDir       string
	sstMetasChan chan metaOrFlush
	ingestErr    common.OnceError
	wg           sync.WaitGroup
	sstIngester  sstIngester

	// sst seq lock
	seqLock sync.Mutex
	// seq number for incoming sst meta
	nextSeq int32
	// max seq of sst metas ingested into pebble
	finishedMetaSeq atomic.Int32

	config    backend.LocalEngineConfig
	tableInfo *checkpoints.TidbTableInfo

	dupDetectOpt common.DupDetectOpt

	// total size of SST files waiting to be ingested
	pendingFileSize atomic.Int64

	// statistics for pebble kv iter.
	importedKVSize  atomic.Int64
	importedKVCount atomic.Int64

	keyAdapter         common.KeyAdapter
	duplicateDetection bool
	duplicateDB        *pebble.DB

	logger log.Logger
}

var _ engineapi.Engine = (*Engine)(nil)

func (e *Engine) setError(err error) {
	if err != nil {
		e.ingestErr.Set(err)
		e.cancel()
	}
}

func (e *Engine) getDB() *pebble.DB {
	return e.db.Load()
}

// Close closes the engine and release all resources.
func (e *Engine) Close() error {
	e.logger.Debug("closing local engine", zap.Stringer("engine", e.UUID), zap.Stack("stack"))
	db := e.getDB()
	if db == nil {
		return nil
	}
	err := errors.Trace(db.Close())
	e.db.Store(nil)
	return err
}

// Cleanup remove meta, db and duplicate detection files
func (e *Engine) Cleanup(dataDir string) error {
	if err := os.RemoveAll(e.sstDir); err != nil {
		return errors.Trace(err)
	}
	uuid := e.UUID.String()
	if err := os.RemoveAll(filepath.Join(dataDir, uuid+DupDetectDirSuffix)); err != nil {
		return errors.Trace(err)
	}
	if err := os.RemoveAll(filepath.Join(dataDir, uuid+DupResultDirSuffix)); err != nil {
		return errors.Trace(err)
	}

	dbPath := filepath.Join(dataDir, uuid)
	return errors.Trace(os.RemoveAll(dbPath))
}

// Exist checks if db folder existing (meta sometimes won't flush before lightning exit)
func (e *Engine) Exist(dataDir string) error {
	dbPath := filepath.Join(dataDir, e.UUID.String())
	if _, err := os.Stat(dbPath); err != nil {
		return err
	}
	return nil
}

// finishWrite flushes all data and waits for the background routine to finish.
// after calling this method, the engine cannot be written anymore. the underlying
// pebble DB is still kept open for reading.
func (e *Engine) finishWrite(ctx context.Context) error {
	e.rLock()
	if e.closed.Load() {
		e.rUnlock()
		return nil
	}

	err := e.flushEngineWithoutLock(ctx)
	e.rUnlock()

	// use mutex to make sure we won't close sstMetasChan while other routines
	// trying to do flush.
	e.lock(importMutexStateClose)
	e.closed.Store(true)
	close(e.sstMetasChan)
	e.unlock()
	if err != nil {
		return errors.Trace(err)
	}
	e.wg.Wait()
	return e.ingestErr.Get()
}

func isStateLocked(state importMutexState) bool {
	return state&(importMutexStateClose|importMutexStateImport) != 0
}

func (e *Engine) isLocked() bool {
	// the engine is locked only in import or close state.
	return isStateLocked(importMutexState(e.isImportingAtomic.Load()))
}

// rLock locks the local file with shard read state. Only used for flush and ingest SST files.
func (e *Engine) rLock() {
	e.mutex.RLock()
	e.isImportingAtomic.Add(uint32(importMutexStateReadLock))
}

func (e *Engine) rUnlock() {
	if e == nil {
		return
	}

	e.isImportingAtomic.Sub(uint32(importMutexStateReadLock))
	e.mutex.RUnlock()
}

// lock locks the local file for importing.
func (e *Engine) lock(state importMutexState) {
	e.mutex.Lock()
	e.isImportingAtomic.Store(uint32(state))
}

// lockUnless tries to lock the local file unless it is already locked into the state given by
// ignoreStateMask. Returns whether the lock is successful.
func (e *Engine) lockUnless(newState, ignoreStateMask importMutexState) bool {
	curState := e.isImportingAtomic.Load()
	if curState&uint32(ignoreStateMask) != 0 {
		return false
	}
	e.lock(newState)
	return true
}

// tryRLock tries to read-lock the local file unless it is already write locked.
// Returns whether the lock is successful.
func (e *Engine) tryRLock() bool {
	curState := e.isImportingAtomic.Load()
	// engine is in import/close state.
	if isStateLocked(importMutexState(curState)) {
		return false
	}
	e.rLock()
	return true
}

func (e *Engine) unlock() {
	if e == nil {
		return
	}
	e.isImportingAtomic.Store(0)
	e.mutex.Unlock()
}

var sizeOfKVPair = int64(unsafe.Sizeof(common.KvPair{}))

// TotalMemorySize returns the total memory size of the engine.
func (e *Engine) TotalMemorySize() int64 {
	var memSize int64
	e.localWriters.Range(func(k, _ any) bool {
		w := k.(*Writer)
		if w.kvBuffer != nil {
			w.Lock()
			memSize += w.kvBuffer.TotalSize()
			w.Unlock()
		}
		w.Lock()
		memSize += sizeOfKVPair * int64(cap(w.writeBatch))
		w.Unlock()
		return true
	})
	return memSize
}

// KVStatistics returns the total kv size and total kv count.
func (e *Engine) KVStatistics() (totalSize int64, totalKVCount int64) {
	return e.TotalSize.Load(), e.Length.Load()
}

// ImportedStatistics returns the imported kv size and imported kv count.
func (e *Engine) ImportedStatistics() (importedSize int64, importedKVCount int64) {
	return e.importedKVSize.Load(), e.importedKVCount.Load()
}

// ConflictInfo implements common.Engine.
func (*Engine) ConflictInfo() engineapi.ConflictInfo {
	return engineapi.ConflictInfo{}
}

// ID is the identifier of an engine.
func (e *Engine) ID() string {
	return e.UUID.String()
}

// GetKeyRange implements common.Engine.
func (e *Engine) GetKeyRange() (startKey []byte, endKey []byte, err error) {
	firstLey, lastKey, err := e.GetFirstAndLastKey(nil, nil)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return firstLey, nextKey(lastKey), nil
}

// GetRegionSplitKeys implements common.Engine.
func (e *Engine) GetRegionSplitKeys() ([][]byte, error) {
	return e.getRegionSplitKeys(e.regionSplitSize, e.regionSplitKeyCnt)
}

func (e *Engine) getRegionSplitKeys(regionSplitSize, regionSplitKeyCnt int64) ([][]byte, error) {
	sizeProps, err := getSizePropertiesFn(e.logger, e.getDB(), e.keyAdapter)
	if err != nil {
		return nil, errors.Trace(err)
	}
	startKey, endKey, err := e.GetKeyRange()
	if err != nil {
		return nil, errors.Trace(err)
	}

	ranges := splitRangeBySizeProps(
		engineapi.Range{Start: startKey, End: endKey},
		sizeProps,
		regionSplitSize,
		regionSplitKeyCnt,
	)
	keys := make([][]byte, 0, len(ranges)+1)
	for _, r := range ranges {
		keys = append(keys, r.Start)
	}
	keys = append(keys, ranges[len(ranges)-1].End)
	e.regionSplitKeysCache = keys
	return keys, nil
}


func (e *Engine) getEngineFileSize() backend.EngineFileSize {
	db := e.getDB()

	var total pebble.LevelMetrics
	if db != nil {
		metrics := db.Metrics()
		total = metrics.Total()
	}
	var memSize int64
	e.localWriters.Range(func(k, _ any) bool {
		w := k.(*Writer)
		memSize += int64(w.EstimatedSize())
		return true
	})

	pendingSize := e.pendingFileSize.Load()
	// TODO: should also add the in-processing compaction sst writer size into MemSize
	return backend.EngineFileSize{
		UUID:        e.UUID,
		DiskSize:    total.Size + pendingSize,
		MemSize:     memSize,
		IsImporting: e.isLocked(),
	}
}

// either a sstMeta or a flush message
type metaOrFlush struct {
	meta    *sstMeta
	flushCh chan struct{}
}

type metaSeq struct {
	// the sequence for this flush message, a flush call can return only if
	// all the other flush will lower `flushSeq` are done
	flushSeq int32
	// the max sstMeta sequence number in this flush, after the flush is done (all SSTs are ingested),
	// we can save chunks will a lower meta sequence number safely.
	metaSeq int32
}

type metaSeqHeap struct {
	arr []metaSeq
}

// Len returns the number of items in the priority queue.
func (h *metaSeqHeap) Len() int {
	return len(h.arr)
}

// Less reports whether the item in the priority queue with
func (h *metaSeqHeap) Less(i, j int) bool {
	return h.arr[i].flushSeq < h.arr[j].flushSeq
}

// Swap swaps the items at the passed indices.
func (h *metaSeqHeap) Swap(i, j int) {
	h.arr[i], h.arr[j] = h.arr[j], h.arr[i]
}

// Push pushes the item onto the priority queue.
func (h *metaSeqHeap) Push(x any) {
	h.arr = append(h.arr, x.(metaSeq))
}

// Pop removes the minimum item (according to Less) from the priority queue
func (h *metaSeqHeap) Pop() any {
	item := h.arr[len(h.arr)-1]
	h.arr = h.arr[:len(h.arr)-1]
	return item
}

func (e *Engine) ingestSSTLoop() {
	defer e.wg.Done()

	type flushSeq struct {
		seq int32
		ch  chan struct{}
	}

	seq := atomic.NewInt32(0)
	finishedSeq := atomic.NewInt32(0)
	var seqLock sync.Mutex
	// a flush is finished iff all the compaction&ingest tasks with a lower seq number are finished.
	flushQueue := make([]flushSeq, 0)
	// inSyncSeqs is a heap that stores all the finished compaction tasks whose seq is bigger than `finishedSeq + 1`
	// this mean there are still at lease one compaction task with a lower seq unfinished.
	inSyncSeqs := &metaSeqHeap{arr: make([]metaSeq, 0)}

	type metaAndSeq struct {
		metas []*sstMeta
		seq   int32
	}

	concurrency := e.config.CompactConcurrency
	// when compaction is disabled, ingest is an serial action, so 1 routine is enough
	if !e.config.Compact {
		concurrency = 1
	}
	metaChan := make(chan metaAndSeq, concurrency)
	for range concurrency {
		e.wg.Add(1)
		go func() {
			defer func() {
				if e.ingestErr.Get() != nil {
					seqLock.Lock()
					for _, f := range flushQueue {
						f.ch <- struct{}{}
					}
					flushQueue = flushQueue[:0]
					seqLock.Unlock()
				}
				e.wg.Done()
			}()
			for {
				select {
				case <-e.ctx.Done():
					return
				case metas, ok := <-metaChan:
					if !ok {
						return
					}
					ingestMetas := metas.metas
					if e.config.Compact {
						newMeta, err := e.sstIngester.mergeSSTs(metas.metas, e.sstDir, e.config.BlockSize)
						if err != nil {
							e.setError(err)
							return
						}
						ingestMetas = []*sstMeta{newMeta}
					}
					// batchIngestSSTs will change ingestMetas' order, so we record the max seq here
					metasMaxSeq := ingestMetas[len(ingestMetas)-1].seq

					if err := e.batchIngestSSTs(ingestMetas); err != nil {
						e.setError(err)
						return
					}
					seqLock.Lock()
					finSeq := finishedSeq.Load()
					if metas.seq == finSeq+1 {
						finSeq = metas.seq
						finMetaSeq := metasMaxSeq
						for len(inSyncSeqs.arr) > 0 {
							if inSyncSeqs.arr[0].flushSeq != finSeq+1 {
								break
							}
							finSeq++
							finMetaSeq = inSyncSeqs.arr[0].metaSeq
							heap.Remove(inSyncSeqs, 0)
						}

						var flushChans []chan struct{}
						for _, seq := range flushQueue {
							if seq.seq > finSeq {
								break
							}
							flushChans = append(flushChans, seq.ch)
						}
						flushQueue = flushQueue[len(flushChans):]
						finishedSeq.Store(finSeq)
						e.finishedMetaSeq.Store(finMetaSeq)
						seqLock.Unlock()
						for _, c := range flushChans {
							c <- struct{}{}
						}
					} else {
						heap.Push(inSyncSeqs, metaSeq{flushSeq: metas.seq, metaSeq: metasMaxSeq})
						seqLock.Unlock()
					}
				}
			}
		}()
	}

	compactAndIngestSSTs := func(metas []*sstMeta) {
		if len(metas) > 0 {
			seqLock.Lock()
			metaSeq := seq.Add(1)
			seqLock.Unlock()
			select {
			case <-e.ctx.Done():
			case metaChan <- metaAndSeq{metas: metas, seq: metaSeq}:
			}
		}
	}

	pendingMetas := make([]*sstMeta, 0, 16)
	totalSize := int64(0)
	metasTmp := make([]*sstMeta, 0)
	addMetas := func() {
		if len(metasTmp) == 0 {
			return
		}
		metas := metasTmp
		metasTmp = make([]*sstMeta, 0, len(metas))
		if !e.config.Compact {
			compactAndIngestSSTs(metas)
			return
		}
		for _, m := range metas {
			if m.totalCount > 0 {
				pendingMetas = append(pendingMetas, m)
				totalSize += m.totalSize
				if totalSize >= e.config.CompactThreshold {
					compactMetas := pendingMetas
					pendingMetas = make([]*sstMeta, 0, len(pendingMetas))
					totalSize = 0
					compactAndIngestSSTs(compactMetas)
				}
			}
		}
	}
readMetaLoop:
	for {
		closed := false
		select {
		case <-e.ctx.Done():
			close(metaChan)
			return
		case m, ok := <-e.sstMetasChan:
			if !ok {
				closed = true
				break
			}
			if m.flushCh != nil {
				// meet a flush event, we should trigger a ingest task if there are pending metas,
				// and then waiting for all the running flush tasks to be done.
				if len(metasTmp) > 0 {
					addMetas()
				}
				if len(pendingMetas) > 0 {
					seqLock.Lock()
					metaSeq := seq.Add(1)
					flushQueue = append(flushQueue, flushSeq{ch: m.flushCh, seq: metaSeq})
					seqLock.Unlock()
					select {
					case metaChan <- metaAndSeq{metas: pendingMetas, seq: metaSeq}:
					case <-e.ctx.Done():
						close(metaChan)
						return
					}

					pendingMetas = make([]*sstMeta, 0, len(pendingMetas))
					totalSize = 0
				} else {
					// none remaining metas needed to be ingested
					seqLock.Lock()
					curSeq := seq.Load()
					finSeq := finishedSeq.Load()
					// if all pending SST files are written, directly do a db.Flush
					if curSeq == finSeq {
						seqLock.Unlock()
						m.flushCh <- struct{}{}
					} else {
						// waiting for pending compaction tasks
						flushQueue = append(flushQueue, flushSeq{ch: m.flushCh, seq: curSeq})
						seqLock.Unlock()
					}
				}
				continue readMetaLoop
			}
			metasTmp = append(metasTmp, m.meta)
			// try to drain all the sst meta from the chan to make sure all the SSTs are processed before handle a flush msg.
			if len(e.sstMetasChan) > 0 {
				continue readMetaLoop
			}

			addMetas()
		}
		if closed {
			compactAndIngestSSTs(pendingMetas)
			close(metaChan)
			return
		}
	}
}

func (e *Engine) addSST(ctx context.Context, m *sstMeta) (int32, error) {
	// set pending size after SST file is generated
	e.pendingFileSize.Add(m.fileSize)
	// make sure sstMeta is sent into the chan in order
	e.seqLock.Lock()
	defer e.seqLock.Unlock()
	e.nextSeq++
	seq := e.nextSeq
	m.seq = seq
	select {
	case e.sstMetasChan <- metaOrFlush{meta: m}:
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-e.ctx.Done():
	}
	return seq, e.ingestErr.Get()
}

func (e *Engine) batchIngestSSTs(metas []*sstMeta) error {
	if len(metas) == 0 {
		return nil
	}
	slices.SortFunc(metas, func(i, j *sstMeta) int {
		return bytes.Compare(i.minKey, j.minKey)
	})

	// non overlapping sst is grouped, and ingested in that order
	metaLevels := make([][]*sstMeta, 0)
	for _, meta := range metas {
		inserted := false
		for i, l := range metaLevels {
			if bytes.Compare(l[len(l)-1].maxKey, meta.minKey) >= 0 {
				continue
			}
			metaLevels[i] = append(l, meta)
			inserted = true
			break
		}
		if !inserted {
			metaLevels = append(metaLevels, []*sstMeta{meta})
		}
	}

	for _, l := range metaLevels {
		if err := e.ingestSSTs(l); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) ingestSSTs(metas []*sstMeta) error {
	// use raw RLock to avoid change the lock state during flushing.
	e.mutex.RLock()
	defer e.mutex.RUnlock()
	if e.closed.Load() {
		return errorEngineClosed
	}
	totalSize := int64(0)
	totalCount := int64(0)
	fileSize := int64(0)
	for _, m := range metas {
		totalSize += m.totalSize
		totalCount += m.totalCount
		fileSize += m.fileSize
	}
	e.logger.Info("write data to local DB",
		zap.Int64("size", totalSize),
		zap.Int64("kvs", totalCount),
		zap.Int("files", len(metas)),
		zap.Int64("sstFileSize", fileSize),
		zap.String("file", metas[0].path),
		logutil.Key("firstKey", metas[0].minKey),
		logutil.Key("lastKey", metas[len(metas)-1].maxKey))
	if err := e.sstIngester.ingest(metas); err != nil {
		return errors.Trace(err)
	}
	count := int64(0)
	size := int64(0)
	for _, m := range metas {
		count += m.totalCount
		size += m.totalSize
	}
	e.Length.Add(count)
	e.TotalSize.Add(size)
	return nil
}

func (e *Engine) flushLocalWriters(parentCtx context.Context) error {
	eg, ctx := errgroup.WithContext(parentCtx)
	e.localWriters.Range(func(k, _ any) bool {
		eg.Go(func() error {
			w := k.(*Writer)
			return w.flush(ctx)
		})
		return true
	})
	return eg.Wait()
}

func (e *Engine) flushEngineWithoutLock(ctx context.Context) error {
	if err := e.flushLocalWriters(ctx); err != nil {
		return err
	}
	flushChan := make(chan struct{}, 1)
	select {
	case e.sstMetasChan <- metaOrFlush{flushCh: flushChan}:
	case <-ctx.Done():
		return ctx.Err()
	case <-e.ctx.Done():
		return e.ctx.Err()
	}

	select {
	case <-flushChan:
	case <-ctx.Done():
		return ctx.Err()
	case <-e.ctx.Done():
		return e.ctx.Err()
	}
	if err := e.ingestErr.Get(); err != nil {
		return errors.Trace(err)
	}
	if err := e.saveEngineMeta(); err != nil {
		return err
	}

	flushFinishedCh, err := e.getDB().AsyncFlush()
	if err != nil {
		return errors.Trace(err)
	}
	select {
	case <-flushFinishedCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-e.ctx.Done():
		return e.ctx.Err()
	}
}

func saveEngineMetaToDB(meta *engineMeta, db *pebble.DB) error {
	jsonBytes, err := json.Marshal(meta)
	if err != nil {
		return errors.Trace(err)
	}
	// note: we can't set Sync to true since we disabled WAL.
	return db.Set(engineMetaKey, jsonBytes, &pebble.WriteOptions{Sync: false})
}

// saveEngineMeta saves the metadata about the DB into the DB itself.
// This method should be followed by a Flush to ensure the data is actually synchronized
func (e *Engine) saveEngineMeta() error {
	e.logger.Debug("save engine meta", zap.Stringer("uuid", e.UUID), zap.Int64("count", e.Length.Load()),
		zap.Int64("size", e.TotalSize.Load()))
	return errors.Trace(saveEngineMetaToDB(&e.engineMeta, e.getDB()))
}

func (e *Engine) loadEngineMeta() error {
	jsonBytes, closer, err := e.getDB().Get(engineMetaKey)
	if err != nil {
		if err == pebble.ErrNotFound {
			e.logger.Debug("local db missing engine meta", zap.Stringer("uuid", e.UUID), log.ShortError(err))
			return nil
		}
		return err
	}
	//nolint: errcheck
	defer closer.Close()

	if err = json.Unmarshal(jsonBytes, &e.engineMeta); err != nil {
		e.logger.Warn("local db failed to deserialize meta", zap.Stringer("uuid", e.UUID), zap.ByteString("content", jsonBytes), zap.Error(err))
		return err
	}
	e.logger.Debug("load engine meta", zap.Stringer("uuid", e.UUID), zap.Int64("count", e.Length.Load()),
		zap.Int64("size", e.TotalSize.Load()))
	return nil
}

func (e *Engine) newKVIter(ctx context.Context, opts *pebble.IterOptions, buf *membuf.Buffer) IngestLocalEngineIter {
	if bytes.Compare(opts.LowerBound, normalIterStartKey) < 0 {
		newOpts := *opts
		newOpts.LowerBound = normalIterStartKey
		opts = &newOpts
	}
	if !e.duplicateDetection {
		iter, err := e.getDB().NewIter(opts)
		if err != nil {
			e.logger.Panic("fail to create iterator")
			return nil
		}
		return &pebbleIter{Iterator: iter, buf: buf}
	}
	logger := log.Wrap(tidblogutil.Logger(ctx)).With(
		zap.String("table", common.UniqueTable(e.tableInfo.DB, e.tableInfo.Name)),
		zap.Int64("tableID", e.tableInfo.ID),
		zap.Stringer("engineUUID", e.UUID))
	return newDupDetectIter(
		e.getDB(),
		e.keyAdapter,
		opts,
		e.duplicateDB,
		logger,
		e.dupDetectOpt,
		buf,
	)
}

var _ engineapi.IngestData = (*Engine)(nil)

// GetFirstAndLastKey reads the first and last key in range [lowerBound, upperBound)
// in the engine. Empty upperBound means unbounded.
func (e *Engine) GetFirstAndLastKey(lowerBound, upperBound []byte) (firstKey, lastKey []byte, err error) {
	if len(upperBound) == 0 {
		// we use empty slice for unbounded upper bound, but it means max value in pebble
		// so reset to nil
		upperBound = nil
	}
	opt := &pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	}
	failpoint.Inject("mockGetFirstAndLastKey", func() {
		failpoint.Return(lowerBound, upperBound, nil)
	})

	iter := e.newKVIter(context.Background(), opt, nil)
	//nolint: errcheck
	defer iter.Close()
	// Needs seek to first because NewIter returns an iterator that is unpositioned
	hasKey := iter.First()
	if iter.Error() != nil {
		return nil, nil, errors.Annotate(iter.Error(), "failed to read the first key")
	}
	if !hasKey {
		return nil, nil, nil
	}
	firstKey = slices.Clone(iter.Key())
	iter.Last()
	if iter.Error() != nil {
		return nil, nil, errors.Annotate(iter.Error(), "failed to seek to the last key")
	}
	lastKey = slices.Clone(iter.Key())
	return firstKey, lastKey, nil
}

// NewIter implements IngestData interface.
func (e *Engine) NewIter(
	ctx context.Context,
	lowerBound, upperBound []byte,
	bufPool *membuf.Pool,
) engineapi.ForwardIter {
	return e.newKVIter(
		ctx,
		&pebble.IterOptions{LowerBound: lowerBound, UpperBound: upperBound},
		bufPool.NewBuffer(),
	)
}

// GetTS implements IngestData interface.
func (e *Engine) GetTS() uint64 {
	return e.TS
}

// IncRef implements IngestData interface.
func (*Engine) IncRef() {}

// DecRef implements IngestData interface.
func (*Engine) DecRef() {}

// Finish implements IngestData interface.
func (e *Engine) Finish(totalBytes, totalCount int64) {
	e.importedKVSize.Add(totalBytes)
	e.importedKVCount.Add(totalCount)
}

// LoadIngestData return (local) Engine itself because Engine has implemented
// IngestData interface.
func (e *Engine) LoadIngestData(
	ctx context.Context,
	outCh chan<- engineapi.DataAndRanges,
) (err error) {
	jobRangeKeys := e.regionSplitKeysCache
	// when the region is large, we need to split to smaller job ranges to increase
	// the concurrency.
	if jobRangeKeys == nil || e.regionSplitSize > 2*int64(config.SplitRegionSize) {
		e.regionSplitKeysCache = nil
		jobRangeKeys, err = e.getRegionSplitKeys(
			int64(config.SplitRegionSize), int64(config.SplitRegionKeys),
		)
		if err != nil {
			return errors.Trace(err)
		}
	}

	prev := jobRangeKeys[0]
	for i := 1; i < len(jobRangeKeys); i++ {
		cur := jobRangeKeys[i]
		select {
		case <-ctx.Done():
			return ctx.Err()
		case outCh <- engineapi.DataAndRanges{
			Data:         e,
			SortedRanges: []engineapi.Range{{Start: prev, End: cur}},
		}:
		}
		prev = cur
	}
	return nil
}

type sstMeta struct {
	path       string
	minKey     []byte
	maxKey     []byte
	totalSize  int64
	totalCount int64
	// used for calculate disk-quota
	fileSize int64
	seq      int32
}

// Writer is used to write data into a SST file.
type Writer struct {
	sync.Mutex
	engine            *Engine
	memtableSizeLimit int64

	// if the KVs are append in order, we can directly write the into SST file,
	// else we must first store them in writeBatch and then batch flush into SST file.
	isKVSorted bool
	writer     atomic.Pointer[sstWriter]
	writerSize atomic.Uint64

	// bytes buffer for writeBatch
	kvBuffer   *membuf.Buffer
	writeBatch []common.KvPair
	// if the kvs in writeBatch are in order, we can avoid doing a `sort.Slice` which
	// is quite slow. in our bench, the sort operation eats about 5% of total CPU
	isWriteBatchSorted bool
	sortedKeyBuf       []byte

	batchCount int
	batchSize  atomic.Int64

	lastMetaSeq int32

	tikvCodec tikv.Codec
}

func (w *Writer) appendRowsSorted(kvs []common.KvPair) (err error) {
	writer := w.writer.Load()
	if writer == nil {
		writer, err = w.createSSTWriter()
		if err != nil {
			return errors.Trace(err)
		}
		w.writer.Store(writer)
	}

	keyAdapter := w.engine.keyAdapter
	totalKeySize := 0
	for i := range kvs {
		keySize := keyAdapter.EncodedLen(kvs[i].Key, kvs[i].RowID)
		w.batchSize.Add(int64(keySize + len(kvs[i].Val)))
		totalKeySize += keySize
	}
	w.batchCount += len(kvs)
	// NoopKeyAdapter doesn't really change the key,
	// skipping the encoding to avoid unnecessary alloc and copy.
	if _, ok := keyAdapter.(common.NoopKeyAdapter); !ok {
		if cap(w.sortedKeyBuf) < totalKeySize {
			w.sortedKeyBuf = make([]byte, totalKeySize)
		}
		buf := w.sortedKeyBuf[:0]
		newKvs := make([]common.KvPair, len(kvs))
		for i := range kvs {
			buf = keyAdapter.Encode(buf, kvs[i].Key, kvs[i].RowID)
			newKvs[i] = common.KvPair{Key: buf, Val: kvs[i].Val}
			buf = buf[len(buf):]
		}
		kvs = newKvs
	}
	if err := writer.writeKVs(kvs); err != nil {
		return err
	}
	w.writerSize.Store(writer.writer.EstimatedSize())
	return nil
}

func (w *Writer) appendRowsUnsorted(ctx context.Context, kvs []common.KvPair) error {
	l := len(w.writeBatch)
	cnt := w.batchCount
	var lastKey []byte
	if cnt > 0 {
		lastKey = w.writeBatch[cnt-1].Key
	}
	keyAdapter := w.engine.keyAdapter
	for _, pair := range kvs {
		if w.isWriteBatchSorted && bytes.Compare(lastKey, pair.Key) > 0 {
			w.isWriteBatchSorted = false
		}
		lastKey = pair.Key
		w.batchSize.Add(int64(len(pair.Key) + len(pair.Val)))
		buf := w.kvBuffer.AllocBytes(keyAdapter.EncodedLen(pair.Key, pair.RowID))
		key := keyAdapter.Encode(buf[:0], pair.Key, pair.RowID)
		val := w.kvBuffer.AddBytes(pair.Val)
		if cnt < l {
			w.writeBatch[cnt].Key = key
			w.writeBatch[cnt].Val = val
		} else {
			w.writeBatch = append(w.writeBatch, common.KvPair{Key: key, Val: val})
		}
		cnt++
	}
	w.batchCount = cnt

	if w.batchSize.Load() > w.memtableSizeLimit {
		if err := w.flushKVs(ctx); err != nil {
			return err
		}
	}
	return nil
}

// AppendRows appends rows to the SST file.
func (w *Writer) AppendRows(ctx context.Context, columnNames []string, rows encode.Rows) error {
	kvs := kv.Rows2KvPairs(rows)
	if len(kvs) == 0 {
		return nil
	}

	if w.engine.closed.Load() {
		return errorEngineClosed
	}

	for i := range kvs {
		kvs[i].Key = w.tikvCodec.EncodeKey(kvs[i].Key)
	}

	w.Lock()
	defer w.Unlock()

	// if chunk has _tidb_rowid field, we can't ensure that the rows are sorted.
	if w.isKVSorted && w.writer.Load() == nil {
		for _, c := range columnNames {
			if c == model.ExtraHandleName.L {
				w.isKVSorted = false
			}
		}
	}

	if w.isKVSorted {
		return w.appendRowsSorted(kvs)
	}
	return w.appendRowsUnsorted(ctx, kvs)
}

func (w *Writer) flush(ctx context.Context) error {
	w.Lock()
	defer w.Unlock()
	if w.batchCount == 0 {
		return nil
	}

	if len(w.writeBatch) > 0 {
		if err := w.flushKVs(ctx); err != nil {
			return errors.Trace(err)
		}
	}

	writer := w.writer.Load()
	if writer != nil {
		meta, err := writer.close()
		if err != nil {
			return errors.Trace(err)
		}
		w.writer.Store(nil)
		w.writerSize.Store(0)
		w.batchCount = 0
		if meta != nil && meta.totalSize > 0 {
			return w.addSST(ctx, meta)
		}
	}

	return nil
}

// EstimatedSize returns the estimated size of the SST file.
func (w *Writer) EstimatedSize() uint64 {
	if size := w.writerSize.Load(); size > 0 {
		return size
	}
	// if kvs are still in memory, only calculate half of the total size
	// in our tests, SST file size is about 50% of the raw kv size
	return uint64(w.batchSize.Load()) / 2
}

type flushStatus struct {
	local *Engine
	seq   int32
}

// Flushed implements common.ChunkFlushStatus.
func (f flushStatus) Flushed() bool {
	return f.seq <= f.local.finishedMetaSeq.Load()
}

// Close implements common.ChunkFlushStatus.
func (w *Writer) Close(ctx context.Context) (common.ChunkFlushStatus, error) {
	defer w.kvBuffer.Destroy()
	defer w.engine.localWriters.Delete(w)
	err := w.flush(ctx)
	// FIXME: in theory this line is useless, but In our benchmark with go1.15
	// this can resolve the memory consistently increasing issue.
	// maybe this is a bug related to go GC mechanism.
	w.writeBatch = nil
	return flushStatus{local: w.engine, seq: w.lastMetaSeq}, err
}

// IsSynced implements common.ChunkFlushStatus.
func (w *Writer) IsSynced() bool {
	return w.batchCount == 0 && w.lastMetaSeq <= w.engine.finishedMetaSeq.Load()
}

func (w *Writer) flushKVs(ctx context.Context) error {
	writer, err := w.createSSTWriter()
	if err != nil {
		return errors.Trace(err)
	}
	if !w.isWriteBatchSorted {
		slices.SortFunc(w.writeBatch[:w.batchCount], func(i, j common.KvPair) int {
			return bytes.Compare(i.Key, j.Key)
		})
		w.isWriteBatchSorted = true
	}

	err = writer.writeKVs(w.writeBatch[:w.batchCount])
	if err != nil {
		return errors.Trace(err)
	}
	meta, err := writer.close()
	if err != nil {
		return errors.Trace(err)
	}

	failpoint.Inject("orphanWriterGoRoutine", func() {
		_ = common.KillMySelf()
		// mimic we meet context cancel error when `addSST`
		<-ctx.Done()
		time.Sleep(5 * time.Second)
		failpoint.Return(errors.Trace(ctx.Err()))
	})

	err = w.addSST(ctx, meta)
	if err != nil {
		return errors.Trace(err)
	}

	w.batchSize.Store(0)
	w.batchCount = 0
	w.kvBuffer.Reset()
	return nil
}

func (w *Writer) addSST(ctx context.Context, meta *sstMeta) error {
	seq, err := w.engine.addSST(ctx, meta)
	if err != nil {
		return err
	}
	w.lastMetaSeq = seq
	return nil
}

func (w *Writer) createSSTWriter() (*sstWriter, error) {
	path := filepath.Join(w.engine.sstDir, uuid.New().String()+".sst")
	writer, err := newSSTWriter(path, w.engine.config.BlockSize)
	if err != nil {
		return nil, err
	}
	sw := &sstWriter{sstMeta: &sstMeta{path: path}, writer: writer, logger: w.engine.logger}
	return sw, nil
}

