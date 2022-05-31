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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/google/btree"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/errormanager"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/hack"
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

type syncedRanges struct {
	sync.Mutex
	ranges []Range
}

func (r *syncedRanges) add(g Range) {
	r.Lock()
	r.ranges = append(r.ranges, g)
	r.Unlock()
}

func (r *syncedRanges) reset() {
	r.Lock()
	r.ranges = r.ranges[:0]
	r.Unlock()
}

type Engine struct {
	engineMeta
	closed       atomic.Bool
	db           *pebble.DB
	UUID         uuid.UUID
	localWriters sync.Map

	// isImportingAtomic is an atomic variable indicating whether this engine is importing.
	// This should not be used as a "spin lock" indicator.
	isImportingAtomic atomic.Uint32
	// flush and ingest sst hold the rlock, other operation hold the wlock.
	mutex sync.RWMutex

	ctx            context.Context
	cancel         context.CancelFunc
	sstDir         string
	sstMetasChan   chan metaOrFlush
	ingestErr      common.OnceError
	wg             sync.WaitGroup
	sstIngester    sstIngester
	finishedRanges syncedRanges

	// sst seq lock
	seqLock sync.Mutex
	// seq number for incoming sst meta
	nextSeq int32
	// max seq of sst metas ingested into pebble
	finishedMetaSeq atomic.Int32

	config    backend.LocalEngineConfig
	tableInfo *checkpoints.TidbTableInfo

	// total size of SST files waiting to be ingested
	pendingFileSize atomic.Int64

	// statistics for pebble kv iter.
	importedKVSize  atomic.Int64
	importedKVCount atomic.Int64

	keyAdapter         KeyAdapter
	duplicateDetection bool
	duplicateDB        *pebble.DB
	errorMgr           *errormanager.ErrorManager
}

func (e *Engine) setError(err error) {
	if err != nil {
		e.ingestErr.Set(err)
		e.cancel()
	}
}

func (e *Engine) Close() error {
	log.L().Debug("closing local engine", zap.Stringer("engine", e.UUID), zap.Stack("stack"))
	if e.db == nil {
		return nil
	}
	err := errors.Trace(e.db.Close())
	e.db = nil
	return err
}

// Cleanup remove meta and db files
func (e *Engine) Cleanup(dataDir string) error {
	if err := os.RemoveAll(e.sstDir); err != nil {
		return errors.Trace(err)
	}

	dbPath := filepath.Join(dataDir, e.UUID.String())
	return os.RemoveAll(dbPath)
}

// Exist checks if db folder existing (meta sometimes won't flush before lightning exit)
func (e *Engine) Exist(dataDir string) error {
	dbPath := filepath.Join(dataDir, e.UUID.String())
	if _, err := os.Stat(dbPath); err != nil {
		return err
	}
	return nil
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

type rangeOffsets struct {
	Size uint64
	Keys uint64
}

type rangeProperty struct {
	Key []byte
	rangeOffsets
}

func (r *rangeProperty) Less(than btree.Item) bool {
	ta := than.(*rangeProperty)
	return bytes.Compare(r.Key, ta.Key) < 0
}

var _ btree.Item = &rangeProperty{}

type rangeProperties []rangeProperty

func (r rangeProperties) Encode() []byte {
	b := make([]byte, 0, 1024)
	idx := 0
	for _, p := range r {
		b = append(b, 0, 0, 0, 0)
		binary.BigEndian.PutUint32(b[idx:], uint32(len(p.Key)))
		idx += 4
		b = append(b, p.Key...)
		idx += len(p.Key)

		b = append(b, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.BigEndian.PutUint64(b[idx:], p.Size)
		idx += 8

		b = append(b, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.BigEndian.PutUint64(b[idx:], p.Keys)
		idx += 8
	}
	return b
}

type RangePropertiesCollector struct {
	props               rangeProperties
	lastOffsets         rangeOffsets
	lastKey             []byte
	currentOffsets      rangeOffsets
	propSizeIdxDistance uint64
	propKeysIdxDistance uint64
}

func newRangePropertiesCollector() pebble.TablePropertyCollector {
	return &RangePropertiesCollector{
		props:               make([]rangeProperty, 0, 1024),
		propSizeIdxDistance: defaultPropSizeIndexDistance,
		propKeysIdxDistance: defaultPropKeysIndexDistance,
	}
}

func (c *RangePropertiesCollector) sizeInLastRange() uint64 {
	return c.currentOffsets.Size - c.lastOffsets.Size
}

func (c *RangePropertiesCollector) keysInLastRange() uint64 {
	return c.currentOffsets.Keys - c.lastOffsets.Keys
}

func (c *RangePropertiesCollector) insertNewPoint(key []byte) {
	c.lastOffsets = c.currentOffsets
	c.props = append(c.props, rangeProperty{Key: append([]byte{}, key...), rangeOffsets: c.currentOffsets})
}

// Add implements `pebble.TablePropertyCollector`.
// Add implements `TablePropertyCollector.Add`.
func (c *RangePropertiesCollector) Add(key pebble.InternalKey, value []byte) error {
	if key.Kind() != pebble.InternalKeyKindSet || bytes.Equal(key.UserKey, engineMetaKey) {
		return nil
	}
	c.currentOffsets.Size += uint64(len(value)) + uint64(len(key.UserKey))
	c.currentOffsets.Keys++
	if len(c.lastKey) == 0 || c.sizeInLastRange() >= c.propSizeIdxDistance ||
		c.keysInLastRange() >= c.propKeysIdxDistance {
		c.insertNewPoint(key.UserKey)
	}
	c.lastKey = append(c.lastKey[:0], key.UserKey...)
	return nil
}

func (c *RangePropertiesCollector) Finish(userProps map[string]string) error {
	if c.sizeInLastRange() > 0 || c.keysInLastRange() > 0 {
		c.insertNewPoint(c.lastKey)
	}

	userProps[propRangeIndex] = string(c.props.Encode())
	return nil
}

func (c *RangePropertiesCollector) Name() string {
	return propRangeIndex
}

type sizeProperties struct {
	totalSize    uint64
	indexHandles *btree.BTree
}

func newSizeProperties() *sizeProperties {
	return &sizeProperties{indexHandles: btree.New(32)}
}

func (s *sizeProperties) add(item *rangeProperty) {
	if old := s.indexHandles.ReplaceOrInsert(item); old != nil {
		o := old.(*rangeProperty)
		item.Keys += o.Keys
		item.Size += o.Size
	}
}

func (s *sizeProperties) addAll(props rangeProperties) {
	prevRange := rangeOffsets{}
	for _, r := range props {
		s.add(&rangeProperty{
			Key:          r.Key,
			rangeOffsets: rangeOffsets{Keys: r.Keys - prevRange.Keys, Size: r.Size - prevRange.Size},
		})
		prevRange = r.rangeOffsets
	}
	if len(props) > 0 {
		s.totalSize += props[len(props)-1].Size
	}
}

// iter the tree until f return false
func (s *sizeProperties) iter(f func(p *rangeProperty) bool) {
	s.indexHandles.Ascend(func(i btree.Item) bool {
		prop := i.(*rangeProperty)
		return f(prop)
	})
}

func decodeRangeProperties(data []byte, keyAdapter KeyAdapter) (rangeProperties, error) {
	r := make(rangeProperties, 0, 16)
	for len(data) > 0 {
		if len(data) < 4 {
			return nil, io.ErrUnexpectedEOF
		}
		keyLen := int(binary.BigEndian.Uint32(data[:4]))
		data = data[4:]
		if len(data) < keyLen+8*2 {
			return nil, io.ErrUnexpectedEOF
		}
		key := data[:keyLen]
		data = data[keyLen:]
		size := binary.BigEndian.Uint64(data[:8])
		keys := binary.BigEndian.Uint64(data[8:])
		data = data[16:]
		if !bytes.Equal(key, engineMetaKey) {
			userKey, err := keyAdapter.Decode(nil, key)
			if err != nil {
				return nil, errors.Annotate(err, "failed to decode key with keyAdapter")
			}
			r = append(r, rangeProperty{Key: userKey, rangeOffsets: rangeOffsets{Size: size, Keys: keys}})
		}
	}

	return r, nil
}

func getSizeProperties(logger log.Logger, db *pebble.DB, keyAdapter KeyAdapter) (*sizeProperties, error) {
	sstables, err := db.SSTables(pebble.WithProperties())
	if err != nil {
		logger.Warn("get sst table properties failed", log.ShortError(err))
		return nil, errors.Trace(err)
	}

	sizeProps := newSizeProperties()
	for _, level := range sstables {
		for _, info := range level {
			if prop, ok := info.Properties.UserProperties[propRangeIndex]; ok {
				data := hack.Slice(prop)
				rangeProps, err := decodeRangeProperties(data, keyAdapter)
				if err != nil {
					logger.Warn("decodeRangeProperties failed",
						zap.Stringer("fileNum", info.FileNum), log.ShortError(err))
					return nil, errors.Trace(err)
				}
				sizeProps.addAll(rangeProps)
			}
		}
	}

	return sizeProps, nil
}

func (e *Engine) getEngineFileSize() backend.EngineFileSize {
	e.mutex.RLock()
	db := e.db
	e.mutex.RUnlock()

	var total pebble.LevelMetrics
	if db != nil {
		metrics := db.Metrics()
		total = metrics.Total()
	}
	var memSize int64
	e.localWriters.Range(func(k, v interface{}) bool {
		w := k.(*Writer)
		if w.writer != nil {
			memSize += int64(w.writer.writer.EstimatedSize())
		} else {
			// if kvs are still in memory, only calculate half of the total size
			// in our tests, SST file size is about 50% of the raw kv size
			memSize += w.batchSize / 2
		}

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

func (h *metaSeqHeap) Len() int {
	return len(h.arr)
}

func (h *metaSeqHeap) Less(i, j int) bool {
	return h.arr[i].flushSeq < h.arr[j].flushSeq
}

func (h *metaSeqHeap) Swap(i, j int) {
	h.arr[i], h.arr[j] = h.arr[j], h.arr[i]
}

func (h *metaSeqHeap) Push(x interface{}) {
	h.arr = append(h.arr, x.(metaSeq))
}

func (h *metaSeqHeap) Pop() interface{} {
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
	for i := 0; i < concurrency; i++ {
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
						newMeta, err := e.sstIngester.mergeSSTs(metas.metas, e.sstDir)
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
							if inSyncSeqs.arr[0].flushSeq == finSeq+1 {
								finSeq++
								finMetaSeq = inSyncSeqs.arr[0].metaSeq
								heap.Remove(inSyncSeqs, 0)
							} else {
								break
							}
						}

						var flushChans []chan struct{}
						for _, seq := range flushQueue {
							if seq.seq <= finSeq {
								flushChans = append(flushChans, seq.ch)
							} else {
								break
							}
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
	sort.Slice(metas, func(i, j int) bool {
		return bytes.Compare(metas[i].minKey, metas[j].minKey) < 0
	})

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
	log.L().Info("write data to local DB",
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
	e.localWriters.Range(func(k, v interface{}) bool {
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

	flushFinishedCh, err := e.db.AsyncFlush()
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
	log.L().Debug("save engine meta", zap.Stringer("uuid", e.UUID), zap.Int64("count", e.Length.Load()),
		zap.Int64("size", e.TotalSize.Load()))
	return errors.Trace(saveEngineMetaToDB(&e.engineMeta, e.db))
}

func (e *Engine) loadEngineMeta() error {
	jsonBytes, closer, err := e.db.Get(engineMetaKey)
	if err != nil {
		if err == pebble.ErrNotFound {
			log.L().Debug("local db missing engine meta", zap.Stringer("uuid", e.UUID), log.ShortError(err))
			return nil
		}
		return err
	}
	defer closer.Close()

	if err = json.Unmarshal(jsonBytes, &e.engineMeta); err != nil {
		log.L().Warn("local db failed to deserialize meta", zap.Stringer("uuid", e.UUID), zap.ByteString("content", jsonBytes), zap.Error(err))
		return err
	}
	log.L().Debug("load engine meta", zap.Stringer("uuid", e.UUID), zap.Int64("count", e.Length.Load()),
		zap.Int64("size", e.TotalSize.Load()))
	return nil
}

// sortAndMergeRanges sort the ranges and merge range that overlaps with each other into a single range.
func sortAndMergeRanges(ranges []Range) []Range {
	if len(ranges) == 0 {
		return ranges
	}

	sort.Slice(ranges, func(i, j int) bool {
		return bytes.Compare(ranges[i].start, ranges[j].start) < 0
	})

	curEnd := ranges[0].end
	i := 0
	for j := 1; j < len(ranges); j++ {
		if bytes.Compare(curEnd, ranges[j].start) >= 0 {
			if bytes.Compare(curEnd, ranges[j].end) < 0 {
				curEnd = ranges[j].end
			}
		} else {
			ranges[i].end = curEnd
			i++
			ranges[i].start = ranges[j].start
			curEnd = ranges[j].end
		}
	}
	ranges[i].end = curEnd
	return ranges[:i+1]
}

func filterOverlapRange(ranges []Range, finishedRanges []Range) []Range {
	if len(ranges) == 0 || len(finishedRanges) == 0 {
		return ranges
	}

	result := make([]Range, 0)
	for _, r := range ranges {
		start := r.start
		end := r.end
		for len(finishedRanges) > 0 && bytes.Compare(finishedRanges[0].start, end) < 0 {
			fr := finishedRanges[0]
			if bytes.Compare(fr.start, start) > 0 {
				result = append(result, Range{start: start, end: fr.start})
			}
			if bytes.Compare(fr.end, start) > 0 {
				start = fr.end
			}
			if bytes.Compare(fr.end, end) > 0 {
				break
			}
			finishedRanges = finishedRanges[1:]
		}
		if bytes.Compare(start, end) < 0 {
			result = append(result, Range{start: start, end: end})
		}
	}
	return result
}

func (e *Engine) unfinishedRanges(ranges []Range) []Range {
	e.finishedRanges.Lock()
	defer e.finishedRanges.Unlock()

	e.finishedRanges.ranges = sortAndMergeRanges(e.finishedRanges.ranges)

	return filterOverlapRange(ranges, e.finishedRanges.ranges)
}

func (e *Engine) newKVIter(ctx context.Context, opts *pebble.IterOptions) Iter {
	if bytes.Compare(opts.LowerBound, normalIterStartKey) < 0 {
		newOpts := *opts
		newOpts.LowerBound = normalIterStartKey
		opts = &newOpts
	}
	if !e.duplicateDetection {
		return pebbleIter{Iterator: e.db.NewIter(opts)}
	}
	logger := log.With(
		zap.String("table", common.UniqueTable(e.tableInfo.DB, e.tableInfo.Name)),
		zap.Int64("tableID", e.tableInfo.ID),
		zap.Stringer("engineUUID", e.UUID))
	return newDupDetectIter(ctx, e.db, e.keyAdapter, opts, e.duplicateDB, logger)
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

type Writer struct {
	sync.Mutex
	engine            *Engine
	memtableSizeLimit int64

	// if the KVs are append in order, we can directly write the into SST file,
	// else we must first store them in writeBatch and then batch flush into SST file.
	isKVSorted bool
	writer     *sstWriter

	// bytes buffer for writeBatch
	kvBuffer   *membuf.Buffer
	writeBatch []common.KvPair
	// if the kvs in writeBatch are in order, we can avoid doing a `sort.Slice` which
	// is quite slow. in our bench, the sort operation eats about 5% of total CPU
	isWriteBatchSorted bool
	sortedKeyBuf       []byte

	batchCount int
	batchSize  int64

	lastMetaSeq int32
}

func (w *Writer) appendRowsSorted(kvs []common.KvPair) error {
	if w.writer == nil {
		writer, err := w.createSSTWriter()
		if err != nil {
			return errors.Trace(err)
		}
		w.writer = writer
	}

	keyAdapter := w.engine.keyAdapter
	totalKeySize := 0
	for i := 0; i < len(kvs); i++ {
		keySize := keyAdapter.EncodedLen(kvs[i].Key)
		w.batchSize += int64(keySize + len(kvs[i].Val))
		totalKeySize += keySize
	}
	w.batchCount += len(kvs)
	// noopKeyAdapter doesn't really change the key,
	// skipping the encoding to avoid unnecessary alloc and copy.
	if _, ok := keyAdapter.(noopKeyAdapter); !ok {
		if cap(w.sortedKeyBuf) < totalKeySize {
			w.sortedKeyBuf = make([]byte, totalKeySize)
		}
		buf := w.sortedKeyBuf[:0]
		newKvs := make([]common.KvPair, len(kvs))
		for i := 0; i < len(kvs); i++ {
			buf = keyAdapter.Encode(buf, kvs[i].Key, kvs[i].RowID)
			newKvs[i] = common.KvPair{Key: buf, Val: kvs[i].Val}
			buf = buf[len(buf):]
		}
		kvs = newKvs
	}
	return w.writer.writeKVs(kvs)
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
		w.batchSize += int64(len(pair.Key) + len(pair.Val))
		buf := w.kvBuffer.AllocBytes(keyAdapter.EncodedLen(pair.Key))
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

	if w.batchSize > w.memtableSizeLimit {
		if err := w.flushKVs(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) AppendRows(ctx context.Context, tableName string, columnNames []string, rows kv.Rows) error {
	kvs := kv.KvPairsFromRows(rows)
	if len(kvs) == 0 {
		return nil
	}

	if w.engine.closed.Load() {
		return errorEngineClosed
	}

	w.Lock()
	defer w.Unlock()

	// if chunk has _tidb_rowid field, we can't ensure that the rows are sorted.
	if w.isKVSorted && w.writer == nil {
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

	if w.writer != nil {
		meta, err := w.writer.close()
		if err != nil {
			return errors.Trace(err)
		}
		w.writer = nil
		w.batchCount = 0
		if meta != nil && meta.totalSize > 0 {
			return w.addSST(ctx, meta)
		}
	}

	return nil
}

type flushStatus struct {
	local *Engine
	seq   int32
}

func (f flushStatus) Flushed() bool {
	return f.seq <= f.local.finishedMetaSeq.Load()
}

func (w *Writer) Close(ctx context.Context) (backend.ChunkFlushStatus, error) {
	defer w.kvBuffer.Destroy()
	defer w.engine.localWriters.Delete(w)
	err := w.flush(ctx)
	// FIXME: in theory this line is useless, but In our benchmark with go1.15
	// this can resolve the memory consistently increasing issue.
	// maybe this is a bug related to go GC mechanism.
	w.writeBatch = nil
	return flushStatus{local: w.engine, seq: w.lastMetaSeq}, err
}

func (w *Writer) IsSynced() bool {
	return w.batchCount == 0 && w.lastMetaSeq <= w.engine.finishedMetaSeq.Load()
}

func (w *Writer) flushKVs(ctx context.Context) error {
	writer, err := w.createSSTWriter()
	if err != nil {
		return errors.Trace(err)
	}
	if !w.isWriteBatchSorted {
		sort.Slice(w.writeBatch[:w.batchCount], func(i, j int) bool {
			return bytes.Compare(w.writeBatch[i].Key, w.writeBatch[j].Key) < 0
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
	err = w.addSST(ctx, meta)
	if err != nil {
		return errors.Trace(err)
	}

	w.batchSize = 0
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
	writer, err := newSSTWriter(path)
	if err != nil {
		return nil, err
	}
	sw := &sstWriter{sstMeta: &sstMeta{path: path}, writer: writer}
	return sw, nil
}

var errorUnorderedSSTInsertion = errors.New("inserting KVs into SST without order")

type sstWriter struct {
	*sstMeta
	writer *sstable.Writer
}

func newSSTWriter(path string) (*sstable.Writer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, errors.Trace(err)
	}
	writer := sstable.NewWriter(f, sstable.WriterOptions{
		TablePropertyCollectors: []func() pebble.TablePropertyCollector{
			newRangePropertiesCollector,
		},
		BlockSize: 16 * 1024,
	})
	return writer, nil
}

func (sw *sstWriter) writeKVs(kvs []common.KvPair) error {
	if len(kvs) == 0 {
		return nil
	}
	if len(sw.minKey) == 0 {
		sw.minKey = append([]byte{}, kvs[0].Key...)
	}
	if bytes.Compare(kvs[0].Key, sw.maxKey) <= 0 {
		return errorUnorderedSSTInsertion
	}

	internalKey := sstable.InternalKey{
		Trailer: uint64(sstable.InternalKeyKindSet),
	}
	var lastKey []byte
	for _, p := range kvs {
		if bytes.Equal(p.Key, lastKey) {
			log.L().Warn("duplicated key found, skip write", logutil.Key("key", p.Key))
			continue
		}
		internalKey.UserKey = p.Key
		if err := sw.writer.Add(internalKey, p.Val); err != nil {
			return errors.Trace(err)
		}
		sw.totalSize += int64(len(p.Key)) + int64(len(p.Val))
		lastKey = p.Key
	}
	sw.totalCount += int64(len(kvs))
	sw.maxKey = append(sw.maxKey[:0], lastKey...)
	return nil
}

func (sw *sstWriter) close() (*sstMeta, error) {
	if err := sw.writer.Close(); err != nil {
		return nil, errors.Trace(err)
	}
	meta, err := sw.writer.Metadata()
	if err != nil {
		return nil, errors.Trace(err)
	}
	sw.fileSize = int64(meta.Size)
	return sw.sstMeta, nil
}

type sstIter struct {
	name   string
	key    []byte
	val    []byte
	iter   sstable.Iterator
	reader *sstable.Reader
	valid  bool
}

func (i *sstIter) Close() error {
	if err := i.iter.Close(); err != nil {
		return errors.Trace(err)
	}
	err := i.reader.Close()
	return errors.Trace(err)
}

type sstIterHeap struct {
	iters []*sstIter
}

func (h *sstIterHeap) Len() int {
	return len(h.iters)
}

func (h *sstIterHeap) Less(i, j int) bool {
	return bytes.Compare(h.iters[i].key, h.iters[j].key) < 0
}

func (h *sstIterHeap) Swap(i, j int) {
	h.iters[i], h.iters[j] = h.iters[j], h.iters[i]
}

func (h *sstIterHeap) Push(x interface{}) {
	h.iters = append(h.iters, x.(*sstIter))
}

func (h *sstIterHeap) Pop() interface{} {
	item := h.iters[len(h.iters)-1]
	h.iters = h.iters[:len(h.iters)-1]
	return item
}

func (h *sstIterHeap) Next() ([]byte, []byte, error) {
	for {
		if len(h.iters) == 0 {
			return nil, nil, nil
		}

		iter := h.iters[0]
		if iter.valid {
			iter.valid = false
			return iter.key, iter.val, iter.iter.Error()
		}

		var k *pebble.InternalKey
		k, iter.val = iter.iter.Next()
		if k != nil {
			iter.key = k.UserKey
			iter.valid = true
			heap.Fix(h, 0)
		} else {
			err := iter.Close()
			heap.Remove(h, 0)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
		}
	}
}

// sstIngester is a interface used to merge and ingest SST files.
// it's a interface mainly used for test convenience
type sstIngester interface {
	mergeSSTs(metas []*sstMeta, dir string) (*sstMeta, error)
	ingest([]*sstMeta) error
}

type dbSSTIngester struct {
	e *Engine
}

func (i dbSSTIngester) mergeSSTs(metas []*sstMeta, dir string) (*sstMeta, error) {
	if len(metas) == 0 {
		return nil, errors.New("sst metas is empty")
	} else if len(metas) == 1 {
		return metas[0], nil
	}

	start := time.Now()
	newMeta := &sstMeta{
		seq: metas[len(metas)-1].seq,
	}
	mergeIter := &sstIterHeap{
		iters: make([]*sstIter, 0, len(metas)),
	}

	for _, p := range metas {
		f, err := os.Open(p.path)
		if err != nil {
			return nil, errors.Trace(err)
		}
		reader, err := sstable.NewReader(f, sstable.ReaderOptions{})
		if err != nil {
			return nil, errors.Trace(err)
		}
		iter, err := reader.NewIter(nil, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
		key, val := iter.Next()
		if key == nil {
			continue
		}
		if iter.Error() != nil {
			return nil, errors.Trace(iter.Error())
		}
		mergeIter.iters = append(mergeIter.iters, &sstIter{
			name:   p.path,
			iter:   iter,
			key:    key.UserKey,
			val:    val,
			reader: reader,
			valid:  true,
		})
		newMeta.totalSize += p.totalSize
		newMeta.totalCount += p.totalCount
	}
	heap.Init(mergeIter)

	name := filepath.Join(dir, fmt.Sprintf("%s.sst", uuid.New()))
	writer, err := newSSTWriter(name)
	if err != nil {
		return nil, errors.Trace(err)
	}
	newMeta.path = name

	internalKey := sstable.InternalKey{
		Trailer: uint64(sstable.InternalKeyKindSet),
	}
	key, val, err := mergeIter.Next()
	if err != nil {
		return nil, err
	}
	if key == nil {
		return nil, errors.New("all ssts are empty!")
	}
	newMeta.minKey = append(newMeta.minKey[:0], key...)
	lastKey := make([]byte, 0)
	for {
		if bytes.Equal(lastKey, key) {
			log.L().Warn("duplicated key found, skipped", zap.Binary("key", lastKey))
			newMeta.totalCount--
			newMeta.totalSize -= int64(len(key) + len(val))

			goto nextKey
		}
		internalKey.UserKey = key
		err = writer.Add(internalKey, val)
		if err != nil {
			return nil, err
		}
		lastKey = append(lastKey[:0], key...)
	nextKey:
		key, val, err = mergeIter.Next()
		if err != nil {
			return nil, err
		}
		if key == nil {
			break
		}
	}
	err = writer.Close()
	if err != nil {
		return nil, errors.Trace(err)
	}
	meta, err := writer.Metadata()
	if err != nil {
		return nil, errors.Trace(err)
	}
	newMeta.maxKey = lastKey
	newMeta.fileSize = int64(meta.Size)

	dur := time.Since(start)
	log.L().Info("compact sst", zap.Int("fileCount", len(metas)), zap.Int64("size", newMeta.totalSize),
		zap.Int64("count", newMeta.totalCount), zap.Duration("cost", dur), zap.String("file", name))

	// async clean raw SSTs.
	go func() {
		totalSize := int64(0)
		for _, m := range metas {
			totalSize += m.fileSize
			if err := os.Remove(m.path); err != nil {
				log.L().Warn("async cleanup sst file failed", zap.Error(err))
			}
		}
		// decrease the pending size after clean up
		i.e.pendingFileSize.Sub(totalSize)
	}()

	return newMeta, err
}

func (i dbSSTIngester) ingest(metas []*sstMeta) error {
	if len(metas) == 0 {
		return nil
	}
	paths := make([]string, 0, len(metas))
	for _, m := range metas {
		paths = append(paths, m.path)
	}
	if i.e.db == nil {
		return errorEngineClosed
	}
	return i.e.db.Ingest(paths)
}
