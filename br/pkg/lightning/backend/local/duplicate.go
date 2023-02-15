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
	"context"
	"fmt"
	"io"
	"math"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/docker/go-units"
	"github.com/google/btree"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/errormanager"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/metric"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/distsql"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/ranger"
	tikverror "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

const (
	maxDupCollectAttemptTimes       = 5
	defaultRecordConflictErrorBatch = 1024
)

type pendingIndexHandles struct {
	// all 4 slices should have exactly the same length.
	// we use a struct-of-arrays instead of array-of-structs
	// so that the rawHandles can be directly given to the BatchGetRequest.
	dataConflictInfos []errormanager.DataConflictInfo
	indexNames        []string
	handles           []tidbkv.Handle
	rawHandles        [][]byte
}

// makePendingIndexHandlesWithCapacity makes the pendingIndexHandles struct-of-arrays with the given
// capacity for every internal array.
func makePendingIndexHandlesWithCapacity(capacity int) pendingIndexHandles {
	return pendingIndexHandles{
		dataConflictInfos: make([]errormanager.DataConflictInfo, 0, capacity),
		indexNames:        make([]string, 0, capacity),
		handles:           make([]tidbkv.Handle, 0, capacity),
		rawHandles:        make([][]byte, 0, capacity),
	}
}

// append pushes the item (no copying) to the end of the indexHandles.
func (indexHandles *pendingIndexHandles) append(
	conflictInfo errormanager.DataConflictInfo,
	indexName string,
	handle tidbkv.Handle,
	rawHandle []byte,
) {
	indexHandles.dataConflictInfos = append(indexHandles.dataConflictInfos, conflictInfo)
	indexHandles.indexNames = append(indexHandles.indexNames, indexName)
	indexHandles.handles = append(indexHandles.handles, handle)
	indexHandles.rawHandles = append(indexHandles.rawHandles, rawHandle)
}

// truncate resets all arrays in indexHandles to length zero, but keeping the allocated capacity.
func (indexHandles *pendingIndexHandles) truncate() {
	indexHandles.dataConflictInfos = indexHandles.dataConflictInfos[:0]
	indexHandles.indexNames = indexHandles.indexNames[:0]
	indexHandles.handles = indexHandles.handles[:0]
	indexHandles.rawHandles = indexHandles.rawHandles[:0]
}

// Len implements sort.Interface.
func (indexHandles *pendingIndexHandles) Len() int {
	return len(indexHandles.rawHandles)
}

// Less implements sort.Interface.
func (indexHandles *pendingIndexHandles) Less(i, j int) bool {
	return bytes.Compare(indexHandles.rawHandles[i], indexHandles.rawHandles[j]) < 0
}

// Swap implements sort.Interface.
func (indexHandles *pendingIndexHandles) Swap(i, j int) {
	indexHandles.handles[i], indexHandles.handles[j] = indexHandles.handles[j], indexHandles.handles[i]
	indexHandles.indexNames[i], indexHandles.indexNames[j] = indexHandles.indexNames[j], indexHandles.indexNames[i]
	indexHandles.dataConflictInfos[i], indexHandles.dataConflictInfos[j] = indexHandles.dataConflictInfos[j], indexHandles.dataConflictInfos[i]
	indexHandles.rawHandles[i], indexHandles.rawHandles[j] = indexHandles.rawHandles[j], indexHandles.rawHandles[i]
}

type pendingKeyRange tidbkv.KeyRange

func (kr pendingKeyRange) Less(other btree.Item) bool {
	return bytes.Compare(kr.EndKey, other.(pendingKeyRange).EndKey) < 0
}

type pendingKeyRanges struct {
	mu   sync.Mutex
	tree *btree.BTree
}

func newPendingKeyRanges(keyRange tidbkv.KeyRange) *pendingKeyRanges {
	tree := btree.New(32)
	tree.ReplaceOrInsert(pendingKeyRange(keyRange))
	return &pendingKeyRanges{tree: tree}
}

func (p *pendingKeyRanges) list() []tidbkv.KeyRange {
	p.mu.Lock()
	defer p.mu.Unlock()

	var keyRanges []tidbkv.KeyRange
	p.tree.Ascend(func(item btree.Item) bool {
		keyRanges = append(keyRanges, tidbkv.KeyRange(item.(pendingKeyRange)))
		return true
	})
	return keyRanges
}

func (p *pendingKeyRanges) empty() bool {
	return p.tree.Len() == 0
}

func (p *pendingKeyRanges) finish(keyRange tidbkv.KeyRange) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var (
		pendingAdd    []btree.Item
		pendingRemove []btree.Item
	)
	startKey := keyRange.StartKey
	endKey := keyRange.EndKey
	p.tree.AscendGreaterOrEqual(
		pendingKeyRange(tidbkv.KeyRange{EndKey: startKey}),
		func(item btree.Item) bool {
			kr := item.(pendingKeyRange)
			if bytes.Compare(startKey, kr.EndKey) >= 0 {
				return true
			}
			if bytes.Compare(endKey, kr.StartKey) <= 0 {
				return false
			}
			pendingRemove = append(pendingRemove, kr)
			if bytes.Compare(startKey, kr.StartKey) > 0 {
				pendingAdd = append(pendingAdd,
					pendingKeyRange(tidbkv.KeyRange{
						StartKey: kr.StartKey,
						EndKey:   startKey,
					}),
				)
			}
			if bytes.Compare(endKey, kr.EndKey) < 0 {
				pendingAdd = append(pendingAdd,
					pendingKeyRange(tidbkv.KeyRange{
						StartKey: endKey,
						EndKey:   kr.EndKey,
					}),
				)
			}
			return true
		},
	)
	for _, item := range pendingRemove {
		p.tree.Delete(item)
	}
	for _, item := range pendingAdd {
		p.tree.ReplaceOrInsert(item)
	}
}

// physicalTableIDs returns all physical table IDs associated with the tableInfo.
// A partitioned table can have multiple physical table IDs.
func physicalTableIDs(tableInfo *model.TableInfo) []int64 {
	if tableInfo.Partition != nil {
		defs := tableInfo.Partition.Definitions
		tids := make([]int64, 1, len(defs)+1)
		tids[0] = tableInfo.ID
		for _, def := range defs {
			tids = append(tids, def.ID)
		}
		return tids
	}
	return []int64{tableInfo.ID}
}

// tableHandleKeyRanges returns all key ranges associated with the tableInfo.
func tableHandleKeyRanges(tableInfo *model.TableInfo) (*tidbkv.KeyRanges, error) {
	ranges := ranger.FullIntRange(false)
	if tableInfo.IsCommonHandle {
		ranges = ranger.FullRange()
	}
	tableIDs := physicalTableIDs(tableInfo)
	return distsql.TableHandleRangesToKVRanges(nil, tableIDs, tableInfo.IsCommonHandle, ranges, nil)
}

// tableIndexKeyRanges returns all key ranges associated with the tableInfo and indexInfo.
func tableIndexKeyRanges(tableInfo *model.TableInfo, indexInfo *model.IndexInfo) (*tidbkv.KeyRanges, error) {
	tableIDs := physicalTableIDs(tableInfo)
	return distsql.IndexRangesToKVRangesForTables(nil, tableIDs, indexInfo.ID, ranger.FullRange(), nil)
}

// DupKVStream is a streaming interface for collecting duplicate key-value pairs.
type DupKVStream interface {
	// Next returns the next key-value pair or any error it encountered.
	// At the end of the stream, the error is io.EOF.
	Next() (key, val []byte, err error)
	// Close closes the stream.
	Close() error
}

// LocalDupKVStream implements the interface of DupKVStream.
// It collects duplicate key-value pairs from a pebble.DB.
//
//goland:noinspection GoNameStartsWithPackageName
type LocalDupKVStream struct {
	iter Iter
}

// NewLocalDupKVStream creates a new LocalDupKVStream with the given duplicate db and key range.
func NewLocalDupKVStream(dupDB *pebble.DB, keyAdapter KeyAdapter, keyRange tidbkv.KeyRange) *LocalDupKVStream {
	opts := &pebble.IterOptions{
		LowerBound: keyRange.StartKey,
		UpperBound: keyRange.EndKey,
	}
	iter := newDupDBIter(dupDB, keyAdapter, opts)
	iter.First()
	return &LocalDupKVStream{iter: iter}
}

func (s *LocalDupKVStream) Next() (key, val []byte, err error) {
	if !s.iter.Valid() {
		err = s.iter.Error()
		if err == nil {
			err = io.EOF
		}
		return
	}
	key = append(key, s.iter.Key()...)
	val = append(val, s.iter.Value()...)
	s.iter.Next()
	return
}

func (s *LocalDupKVStream) Close() error {
	return s.iter.Close()
}

type regionError struct {
	inner *errorpb.Error
}

func (r regionError) Error() string {
	return r.inner.String()
}

// RemoteDupKVStream implements the interface of DupKVStream.
// It collects duplicate key-value pairs from a TiKV region.
type RemoteDupKVStream struct {
	cli    import_sstpb.ImportSST_DuplicateDetectClient
	kvs    []*import_sstpb.KvPair
	atEOF  bool
	cancel context.CancelFunc
}

func getDupDetectClient(
	ctx context.Context,
	region *split.RegionInfo,
	keyRange tidbkv.KeyRange,
	importClientFactory ImportClientFactory,
) (import_sstpb.ImportSST_DuplicateDetectClient, error) {
	leader := region.Leader
	if leader == nil {
		leader = region.Region.GetPeers()[0]
	}
	importClient, err := importClientFactory.Create(ctx, leader.GetStoreId())
	if err != nil {
		return nil, errors.Trace(err)
	}
	reqCtx := &kvrpcpb.Context{
		RegionId:    region.Region.GetId(),
		RegionEpoch: region.Region.GetRegionEpoch(),
		Peer:        leader,
	}
	req := &import_sstpb.DuplicateDetectRequest{
		Context:  reqCtx,
		StartKey: keyRange.StartKey,
		EndKey:   keyRange.EndKey,
	}
	cli, err := importClient.DuplicateDetect(ctx, req)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return cli, nil
}

// NewRemoteDupKVStream creates a new RemoteDupKVStream.
func NewRemoteDupKVStream(
	ctx context.Context,
	region *split.RegionInfo,
	keyRange tidbkv.KeyRange,
	importClientFactory ImportClientFactory,
) (*RemoteDupKVStream, error) {
	subCtx, cancel := context.WithCancel(ctx)
	cli, err := getDupDetectClient(subCtx, region, keyRange, importClientFactory)
	if err != nil {
		cancel()
		return nil, errors.Trace(err)
	}
	s := &RemoteDupKVStream{cli: cli, cancel: cancel}
	// call tryRecv to see if there are some region errors.
	if err := s.tryRecv(); err != nil && errors.Cause(err) != io.EOF {
		cancel()
		return nil, errors.Trace(err)
	}
	return s, nil
}

func (s *RemoteDupKVStream) tryRecv() error {
	resp, err := s.cli.Recv()
	if err != nil {
		if errors.Cause(err) == io.EOF {
			s.atEOF = true
			err = io.EOF
		}
		return err
	}
	if resp.RegionError != nil {
		return errors.Cause(regionError{inner: resp.RegionError})
	}
	if resp.KeyError != nil {
		return errors.Errorf("meet key error in duplicate detect response: %s", resp.KeyError.Message)
	}
	s.kvs = resp.Pairs
	return nil
}

func (s *RemoteDupKVStream) Next() (key, val []byte, err error) {
	for len(s.kvs) == 0 {
		if s.atEOF {
			return nil, nil, io.EOF
		}
		if err := s.tryRecv(); err != nil {
			return nil, nil, errors.Trace(err)
		}
	}
	key, val = s.kvs[0].Key, s.kvs[0].Value
	s.kvs = s.kvs[1:]
	return
}

func (s *RemoteDupKVStream) Close() error {
	s.cancel()
	return nil
}

// DuplicateManager provides methods to collect and decode duplicated KV pairs into row data. The results
// are stored into the errorMgr.
type DuplicateManager struct {
	tbl         table.Table
	tableName   string
	splitCli    split.SplitClient
	tikvCli     *tikv.KVStore
	errorMgr    *errormanager.ErrorManager
	decoder     *kv.TableKVDecoder
	logger      log.Logger
	concurrency int
	hasDupe     *atomic.Bool
	indexID     int64
	metrics     *metric.Metrics
}

// NewDuplicateManager creates a new DuplicateManager.
func NewDuplicateManager(
	tbl table.Table,
	tableName string,
	splitCli split.SplitClient,
	tikvCli *tikv.KVStore,
	errMgr *errormanager.ErrorManager,
	sessOpts *kv.SessionOptions,
	concurrency int,
	hasDupe *atomic.Bool,
	logger log.Logger,
	metrics *metric.Metrics,
) (*DuplicateManager, error) {
	logger = logger.With(zap.String("tableName", tableName))
	decoder, err := kv.NewTableKVDecoder(tbl, tableName, sessOpts, logger)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &DuplicateManager{
		tbl:         tbl,
		tableName:   tableName,
		splitCli:    splitCli,
		tikvCli:     tikvCli,
		errorMgr:    errMgr,
		decoder:     decoder,
		logger:      logger,
		concurrency: concurrency,
		hasDupe:     hasDupe,
		indexID:     sessOpts.IndexID,
		metrics:     metrics,
	}, nil
}

// RecordDataConflictError records data conflicts to errorMgr. The key received from stream must be a row key.
func (m *DuplicateManager) RecordDataConflictError(ctx context.Context, stream DupKVStream) error {
	//nolint: errcheck
	defer stream.Close()
	var dataConflictInfos []errormanager.DataConflictInfo
	for {
		key, val, err := stream.Next()
		if errors.Cause(err) == io.EOF {
			break
		}
		if err != nil {
			return errors.Trace(err)
		}
		m.hasDupe.Store(true)

		h, err := m.decoder.DecodeHandleFromRowKey(key)
		if err != nil {
			return errors.Trace(err)
		}
		conflictInfo := errormanager.DataConflictInfo{
			RawKey:   key,
			RawValue: val,
			KeyData:  h.String(),
			Row:      m.decoder.DecodeRawRowDataAsStr(h, val),
		}
		dataConflictInfos = append(dataConflictInfos, conflictInfo)
		if len(dataConflictInfos) >= defaultRecordConflictErrorBatch {
			if err := m.errorMgr.RecordDataConflictError(ctx, m.logger, m.tableName, dataConflictInfos); err != nil {
				return errors.Trace(err)
			}
			dataConflictInfos = dataConflictInfos[:0]
		}
	}
	if len(dataConflictInfos) > 0 {
		if err := m.errorMgr.RecordDataConflictError(ctx, m.logger, m.tableName, dataConflictInfos); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (m *DuplicateManager) saveIndexHandles(ctx context.Context, handles pendingIndexHandles) error {
	snapshot := m.tikvCli.GetSnapshot(math.MaxUint64)
	batchGetMap, err := snapshot.BatchGet(ctx, handles.rawHandles)
	if err != nil {
		return errors.Trace(err)
	}

	rawRows := make([][]byte, handles.Len())
	for i, rawHandle := range handles.rawHandles {
		rawValue, ok := batchGetMap[string(hack.String(rawHandle))]
		if ok {
			rawRows[i] = rawValue
			handles.dataConflictInfos[i].Row = m.decoder.DecodeRawRowDataAsStr(handles.handles[i], rawValue)
		} else {
			m.logger.Warn("[detect-dupe] can not found row data corresponding to the handle",
				logutil.Key("rawHandle", rawHandle))
		}
	}

	err = m.errorMgr.RecordIndexConflictError(ctx, m.logger, m.tableName,
		handles.indexNames, handles.dataConflictInfos, handles.rawHandles, rawRows)
	return errors.Trace(err)
}

// RecordIndexConflictError records index conflicts to errorMgr. The key received from stream must be an index key.
func (m *DuplicateManager) RecordIndexConflictError(ctx context.Context, stream DupKVStream, tableID int64, indexInfo *model.IndexInfo) error {
	//nolint: errcheck
	defer stream.Close()
	indexHandles := makePendingIndexHandlesWithCapacity(0)
	for {
		key, val, err := stream.Next()
		if errors.Cause(err) == io.EOF {
			break
		}
		if err != nil {
			return errors.Trace(err)
		}
		m.hasDupe.Store(true)

		h, err := m.decoder.DecodeHandleFromIndex(indexInfo, key, val)
		if err != nil {
			return errors.Trace(err)
		}
		conflictInfo := errormanager.DataConflictInfo{
			RawKey:   key,
			RawValue: val,
			KeyData:  h.String(),
		}
		indexHandles.append(conflictInfo, indexInfo.Name.O,
			h, tablecodec.EncodeRowKeyWithHandle(tableID, h))

		if indexHandles.Len() >= defaultRecordConflictErrorBatch {
			if err := m.saveIndexHandles(ctx, indexHandles); err != nil {
				return errors.Trace(err)
			}
			indexHandles.truncate()
		}
	}
	if indexHandles.Len() > 0 {
		if err := m.saveIndexHandles(ctx, indexHandles); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// BuildDuplicateTaskForTest is only used for test.
var BuildDuplicateTaskForTest = func(m *DuplicateManager) ([]dupTask, error) {
	return m.buildDupTasks(m.RecordDataConflictError, m.RecordIndexConflictError)
}

type (
	DataConflictErrorHandler  func(ctx context.Context, stream DupKVStream) error
	IndexConflictErrorHandler func(ctx context.Context, stream DupKVStream, tableID int64, indexInfo *model.IndexInfo) error
)

type dupTask struct {
	tidbkv.KeyRange
	tableID                   int64
	indexInfo                 *model.IndexInfo
	dataConflictErrorHandler  DataConflictErrorHandler
	indexConflictErrorHandler IndexConflictErrorHandler
}

func (m *DuplicateManager) buildDupTasks(
	dataConflictHandler DataConflictErrorHandler,
	indexConflictHandler IndexConflictErrorHandler,
) ([]dupTask, error) {
	if m.indexID != 0 {
		return m.buildIndexDupTasks()
	}
	keyRanges, err := tableHandleKeyRanges(m.tbl.Meta())
	if err != nil {
		return nil, errors.Trace(err)
	}
	tasks := make([]dupTask, 0, keyRanges.TotalRangeNum()*(1+len(m.tbl.Meta().Indices)))
	putToTaskFunc := func(ranges []tidbkv.KeyRange, indexInfo *model.IndexInfo) {
		if len(ranges) == 0 {
			return
		}
		tid := tablecodec.DecodeTableID(ranges[0].StartKey)
		for _, r := range ranges {
			tasks = append(tasks, dupTask{
				KeyRange:                  r,
				tableID:                   tid,
				indexInfo:                 indexInfo,
				dataConflictErrorHandler:  dataConflictHandler,
				indexConflictErrorHandler: indexConflictHandler,
			})
		}
	}
	keyRanges.ForEachPartition(func(ranges []tidbkv.KeyRange) {
		putToTaskFunc(ranges, nil)
	})
	for _, indexInfo := range m.tbl.Meta().Indices {
		if indexInfo.State != model.StatePublic {
			continue
		}
		keyRanges, err = tableIndexKeyRanges(m.tbl.Meta(), indexInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}
		keyRanges.ForEachPartition(func(ranges []tidbkv.KeyRange) {
			putToTaskFunc(ranges, indexInfo)
		})
	}
	return tasks, nil
}

func (m *DuplicateManager) buildIndexDupTasks() ([]dupTask, error) {
	for _, indexInfo := range m.tbl.Meta().Indices {
		if m.indexID != indexInfo.ID {
			continue
		}
		keyRanges, err := tableIndexKeyRanges(m.tbl.Meta(), indexInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}
		tasks := make([]dupTask, 0, keyRanges.TotalRangeNum())
		keyRanges.ForEachPartition(func(ranges []tidbkv.KeyRange) {
			if len(ranges) == 0 {
				return
			}
			tid := tablecodec.DecodeTableID(ranges[0].StartKey)
			for _, r := range ranges {
				tasks = append(tasks, dupTask{
					KeyRange: r,
					tableID:  tid,
				})
			}
		})
		return tasks, nil
	}
	return nil, nil
}

func (m *DuplicateManager) splitLocalDupTaskByKeys(
	task dupTask,
	dupDB *pebble.DB,
	keyAdapter KeyAdapter,
	sizeLimit int64,
	keysLimit int64,
) ([]dupTask, error) {
	sizeProps, err := getSizeProperties(m.logger, dupDB, keyAdapter)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ranges := splitRangeBySizeProps(Range{start: task.StartKey, end: task.EndKey}, sizeProps, sizeLimit, keysLimit)
	newDupTasks := make([]dupTask, 0, len(ranges))
	for _, r := range ranges {
		newDupTasks = append(newDupTasks, dupTask{
			KeyRange: tidbkv.KeyRange{
				StartKey: r.start,
				EndKey:   r.end,
			},
			tableID:                   task.tableID,
			indexInfo:                 task.indexInfo,
			indexConflictErrorHandler: task.indexConflictErrorHandler,
			dataConflictErrorHandler:  task.dataConflictErrorHandler,
		})
	}
	return newDupTasks, nil
}

func (m *DuplicateManager) buildLocalDupTasks(
	dupDB *pebble.DB,
	keyAdapter KeyAdapter,
	dataConflictErrorHandler DataConflictErrorHandler,
	indexConflictErrorHandler IndexConflictErrorHandler,
) ([]dupTask, error) {
	tasks, err := m.buildDupTasks(dataConflictErrorHandler, indexConflictErrorHandler)
	if err != nil {
		return nil, errors.Trace(err)
	}
	//nolint: prealloc
	var newTasks []dupTask
	for _, task := range tasks {
		// FIXME: Do not hardcode sizeLimit and keysLimit.
		subTasks, err := m.splitLocalDupTaskByKeys(task, dupDB, keyAdapter, 32*units.MiB, 1*units.MiB)
		if err != nil {
			return nil, errors.Trace(err)
		}
		newTasks = append(newTasks, subTasks...)
	}
	return newTasks, nil
}

// CollectDuplicateRowsFromDupDB collects duplicates from the duplicate DB and records all duplicate row info into errorMgr.
func (m *DuplicateManager) CollectDuplicateRowsFromDupDB(ctx context.Context, dupDB *pebble.DB, keyAdapter KeyAdapter) error {
	return m.iterDuplicateRowsFromDupDB(ctx,
		"collect duplicate rows from local duplicate db",
		dupDB, keyAdapter, m.RecordDataConflictError, m.RecordIndexConflictError)
}

func (m *DuplicateManager) iterDuplicateRowsFromDupDB(
	ctx context.Context,
	action string,
	dupDB *pebble.DB,
	keyAdapter KeyAdapter,
	dataConflictErrorHandler DataConflictErrorHandler,
	indexConflictErrorHandler IndexConflictErrorHandler,
) error {
	tasks, err := m.buildLocalDupTasks(dupDB, keyAdapter, dataConflictErrorHandler, indexConflictErrorHandler)
	if err != nil {
		return errors.Trace(err)
	}

	finishedTasks := atomic.NewInt64(0)
	totalTasks := len(tasks)
	var wg sync.WaitGroup
	wg.Add(1)
	finished := make(chan struct{})
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Minute)
		for {
			select {
			case <-ticker.C:
				m.logger.Info(action+", progress",
					zap.String("progress", fmt.Sprintf("%.1f%%", float64(finishedTasks.Load())/float64(totalTasks)*100)),
					zap.Int64("finished", finishedTasks.Load()), zap.Int("total", totalTasks))
			case <-ctx.Done():
				return
			case <-finished:
				return
			}
		}
	}()

	pool := utils.NewWorkerPool(uint(m.concurrency), action)
	g, gCtx := errgroup.WithContext(ctx)
	for _, task := range tasks {
		task := task
		pool.ApplyOnErrorGroup(g, func() error {
			if err := common.Retry(action, m.logger, func() error {
				stream := NewLocalDupKVStream(dupDB, keyAdapter, task.KeyRange)
				var err error
				if task.indexInfo == nil {
					err = dataConflictErrorHandler(gCtx, stream)
				} else {
					err = indexConflictErrorHandler(gCtx, stream, task.tableID, task.indexInfo)
				}
				return errors.Trace(err)
			}); err != nil {
				return errors.Trace(err)
			}

			// Delete the key range in duplicate DB since we have the duplicates have been collected.
			rawStartKey := keyAdapter.Encode(nil, task.StartKey, math.MinInt64)
			rawEndKey := keyAdapter.Encode(nil, task.EndKey, math.MinInt64)
			err = dupDB.DeleteRange(rawStartKey, rawEndKey, nil)
			finishedTasks.Inc()
			return errors.Trace(err)
		})
	}
	err = g.Wait()
	close(finished)
	wg.Wait()
	return err
}

func (m *DuplicateManager) splitKeyRangeByRegions(
	ctx context.Context, keyRange tidbkv.KeyRange,
) ([]*split.RegionInfo, []tidbkv.KeyRange, error) {
	rawStartKey := codec.EncodeBytes(nil, keyRange.StartKey)
	rawEndKey := codec.EncodeBytes(nil, keyRange.EndKey)
	allRegions, err := split.PaginateScanRegion(ctx, m.splitCli, rawStartKey, rawEndKey, 1024)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	regions := make([]*split.RegionInfo, 0, len(allRegions))
	keyRanges := make([]tidbkv.KeyRange, 0, len(allRegions))
	for _, region := range allRegions {
		startKey := keyRange.StartKey
		endKey := keyRange.EndKey
		if len(region.Region.StartKey) > 0 {
			_, regionStartKey, err := codec.DecodeBytes(region.Region.StartKey, nil)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
			if bytes.Compare(startKey, regionStartKey) < 0 {
				startKey = regionStartKey
			}
		}
		if len(region.Region.EndKey) > 0 {
			_, regionEndKey, err := codec.DecodeBytes(region.Region.EndKey, nil)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
			if bytes.Compare(endKey, regionEndKey) > 0 {
				endKey = regionEndKey
			}
		}
		if bytes.Compare(startKey, endKey) < 0 {
			regions = append(regions, region)
			keyRanges = append(keyRanges, tidbkv.KeyRange{
				StartKey: startKey,
				EndKey:   endKey,
			})
		}
	}
	return regions, keyRanges, nil
}

func (m *DuplicateManager) processRemoteDupTaskOnce(
	ctx context.Context,
	task dupTask,
	logger log.Logger,
	importClientFactory ImportClientFactory,
	regionPool *utils.WorkerPool,
	remainKeyRanges *pendingKeyRanges,
) (madeProgress bool, err error) {
	//nolint: prealloc
	var regions []*split.RegionInfo
	//nolint: prealloc
	var keyRanges []tidbkv.KeyRange

	for _, kr := range remainKeyRanges.list() {
		subRegions, subKeyRanges, err := m.splitKeyRangeByRegions(ctx, kr)
		if err != nil {
			return false, errors.Trace(err)
		}
		regions = append(regions, subRegions...)
		keyRanges = append(keyRanges, subKeyRanges...)
	}

	var metErr common.OnceError
	wg := &sync.WaitGroup{}
	atomicMadeProgress := atomic.NewBool(false)
	for i := 0; i < len(regions); i++ {
		if ctx.Err() != nil {
			metErr.Set(ctx.Err())
			break
		}
		region := regions[i]
		kr := keyRanges[i]
		wg.Add(1)
		regionPool.Apply(func() {
			defer wg.Done()

			logger := logger.With(
				zap.Uint64("regionID", region.Region.Id),
				logutil.Key("dupDetectStartKey", kr.StartKey),
				logutil.Key("dupDetectEndKey", kr.EndKey),
			)
			err := func() error {
				stream, err := NewRemoteDupKVStream(ctx, region, kr, importClientFactory)
				if err != nil {
					return errors.Annotatef(err, "failed to create remote duplicate kv stream")
				}
				if task.indexInfo == nil {
					err = task.dataConflictErrorHandler(ctx, stream)
				} else {
					err = task.indexConflictErrorHandler(ctx, stream, task.tableID, task.indexInfo)
				}
				if err != nil {
					return errors.Annotatef(err, "failed to record conflict errors")
				}
				return nil
			}()
			if err != nil {
				if regionErr, ok := errors.Cause(err).(regionError); ok {
					logger.Debug("[detect-dupe] collect duplicate rows from region failed due to region error", zap.Error(regionErr))
				} else {
					logger.Warn("[detect-dupe] collect duplicate rows from region failed", log.ShortError(err))
				}
				metErr.Set(err)
			} else {
				logger.Debug("[detect-dupe] collect duplicate rows from region completed")
				remainKeyRanges.finish(kr)
				atomicMadeProgress.Store(true)
			}
		})
	}
	wg.Wait()
	return atomicMadeProgress.Load(), errors.Trace(metErr.Get())
}

// processRemoteDupTask processes a remoteDupTask. A task contains a key range.
// A key range is associated with multiple regions. processRemoteDupTask tries
// to collect duplicates from each region.
func (m *DuplicateManager) processRemoteDupTask(
	ctx context.Context,
	task dupTask,
	logger log.Logger,
	importClientFactory ImportClientFactory,
	regionPool *utils.WorkerPool,
) error {
	remainAttempts := maxDupCollectAttemptTimes
	remainKeyRanges := newPendingKeyRanges(task.KeyRange)
	for {
		madeProgress, err := m.processRemoteDupTaskOnce(ctx, task, logger, importClientFactory, regionPool, remainKeyRanges)
		if err == nil {
			if !remainKeyRanges.empty() {
				remainKeyRanges.list()
				logger.Panic("[detect-dupe] there are still some key ranges that haven't been processed, which is unexpected",
					zap.Any("remainKeyRanges", remainKeyRanges.list()))
			}
			return nil
		}
		if log.IsContextCanceledError(err) {
			return errors.Trace(err)
		}
		if !madeProgress {
			remainAttempts--
			if remainAttempts <= 0 {
				logger.Error("[detect-dupe] all attempts to process the remote dupTask have failed", log.ShortError(err))
				return errors.Trace(err)
			}
		}
		logger.Warn("[detect-dupe] process remote dupTask encounters error, retrying",
			log.ShortError(err), zap.Int("remainAttempts", remainAttempts))
	}
}

// CollectDuplicateRowsFromTiKV collects duplicates from the remote TiKV and records all duplicate row info into errorMgr.
func (m *DuplicateManager) CollectDuplicateRowsFromTiKV(ctx context.Context, importClientFactory ImportClientFactory) error {
	return m.iterDuplicateRowsFromTiKV(ctx, "collect duplicate rows from tikv", importClientFactory, m.RecordDataConflictError, m.RecordIndexConflictError)
}

func (m *DuplicateManager) iterDuplicateRowsFromTiKV(
	ctx context.Context,
	action string,
	importClientFactory ImportClientFactory,
	dataConflictErrorHandler DataConflictErrorHandler,
	indexConflictErrorHandler IndexConflictErrorHandler,
) error {
	tasks, err := m.buildDupTasks(dataConflictErrorHandler, indexConflictErrorHandler)
	if err != nil {
		return errors.Trace(err)
	}
	taskPool := utils.NewWorkerPool(uint(m.concurrency), action)
	regionPool := utils.NewWorkerPool(uint(m.concurrency), action+" by region")
	g, gCtx := errgroup.WithContext(ctx)

	finishedTasks := atomic.NewInt64(0)
	totalTasks := len(tasks)
	var wg sync.WaitGroup
	wg.Add(1)
	finished := make(chan struct{})
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Minute)
		for {
			select {
			case <-ticker.C:
				m.logger.Info(action+", progress",
					zap.String("progress", fmt.Sprintf("%.1f%%", float64(finishedTasks.Load())/float64(totalTasks)*100)),
					zap.Int64("finished", finishedTasks.Load()), zap.Int("total", totalTasks))
			case <-ctx.Done():
				return
			case <-finished:
				return
			}
		}
	}()

	for _, task := range tasks {
		task := task
		taskPool.ApplyOnErrorGroup(g, func() error {
			taskLogger := m.logger.With(
				logutil.Key("startKey", task.StartKey),
				logutil.Key("endKey", task.EndKey),
				zap.Int64("tableID", task.tableID),
			)
			if task.indexInfo != nil {
				taskLogger = taskLogger.With(
					zap.String("indexName", task.indexInfo.Name.O),
					zap.Int64("indexID", task.indexInfo.ID),
				)
			}
			err := m.processRemoteDupTask(gCtx, task, taskLogger, importClientFactory, regionPool)
			finishedTasks.Inc()
			return errors.Trace(err)
		})
	}
	err = g.Wait()
	close(finished)
	wg.Wait()
	return err
}

func (m *DuplicateManager) CollectRemoteDuplicateRowsToLocal(
	ctx context.Context,
	importClientFactory ImportClientFactory,
	dupDB *pebble.DB, keyAdapter KeyAdapter,
) error {
	// RowIDGen is used to generate a unique row ID for each duplicate row
	// before writing it into the local duplicate database.
	rowIDGen := atomic.NewInt64(0)
	return m.iterDuplicateRowsFromTiKV(ctx, "collect remote duplicate rows to local", importClientFactory,
		func(ctx context.Context, stream DupKVStream) error {
			return m.recordConflictErrorToLocal(ctx, stream, dupDB, keyAdapter, rowIDGen)
		},
		func(ctx context.Context, stream DupKVStream, _ int64, _ *model.IndexInfo) error {
			return m.recordConflictErrorToLocal(ctx, stream, dupDB, keyAdapter, rowIDGen)
		},
	)
}

func (m *DuplicateManager) recordConflictErrorToLocal(
	ctx context.Context,
	stream DupKVStream,
	dupDB *pebble.DB,
	keyAdapter KeyAdapter,
	rowIDGen *atomic.Int64,
) error {
	defer stream.Close()
	writeBatch := dupDB.NewBatch()
	writeBatchSize := 0

	var encodedKey []byte
	for {
		key, val, err := stream.Next()
		if errors.Cause(err) == io.EOF {
			break
		}
		if ctx.Err() != nil {
			return errors.Trace(ctx.Err())
		}
		encodedKey = keyAdapter.Encode(encodedKey[:0], key, rowIDGen.Inc())
		if err := writeBatch.Set(encodedKey, val, nil); err != nil {
			return errors.Trace(err)
		}
		writeBatchSize += len(encodedKey) + len(val)

		if writeBatchSize > maxDuplicateBatchSize {
			if err := writeBatch.Commit(pebble.Sync); err != nil {
				return errors.Trace(err)
			}
			writeBatch.Reset()
			writeBatchSize = 0
		}
	}

	if writeBatchSize > 0 {
		if err := writeBatch.Commit(pebble.Sync); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (m *DuplicateManager) ResolveLocalDuplicateRows(
	ctx context.Context,
	dupDB *pebble.DB,
	keyAdapter KeyAdapter,
	dupeRecordQuota *atomic.Int64,
) error {
	return m.iterDuplicateRowsFromDupDB(
		ctx, "resolve local duplicate rows", dupDB, keyAdapter,
		func(ctx context.Context, stream DupKVStream) error {
			return m.resolveDataConflictError(ctx, stream, dupeRecordQuota)
		},
		func(ctx context.Context, stream DupKVStream, tableID int64, indexInfo *model.IndexInfo) error {
			return m.resolveIndexConflictError(ctx, stream, tableID, indexInfo, dupeRecordQuota)
		},
	)
}

func (m *DuplicateManager) resolveDataConflictError(
	ctx context.Context,
	stream DupKVStream,
	dupeRecordQuota *atomic.Int64,
) (err error) {
	//nolint: errcheck
	defer stream.Close()
	var handleRows [][2][]byte
	for {
		key, val, err := stream.Next()
		if errors.Cause(err) == io.EOF {
			break
		}
		if err != nil {
			return errors.Trace(err)
		}
		m.hasDupe.Store(true)

		handleRows = append(handleRows, [2][]byte{key, val})
		if len(handleRows) >= defaultRecordConflictErrorBatch {
			if err = m.resolveDuplicateRows(ctx, m.tableName, handleRows); err != nil {
				return errors.Trace(err)
			}
			handleRows = handleRows[:0]
		}

		if dupeRecordQuota.Dec() >= 0 {
			h, err := m.decoder.DecodeHandleFromRowKey(key)
			if err != nil {
				return errors.Trace(err)
			}
			conflictInfo := errormanager.DataConflictInfo{
				RawKey:   key,
				RawValue: val,
				KeyData:  h.String(),
				Row:      m.decoder.DecodeRawRowDataAsStr(h, val),
			}
			if err := m.errorMgr.RecordDataConflictError(ctx, m.logger, m.tableName, []errormanager.DataConflictInfo{conflictInfo}); err != nil {
				return errors.Trace(err)
			}
		}
	}
	if len(handleRows) > 0 {
		if err = m.resolveDuplicateRows(ctx, m.tableName, handleRows); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (m *DuplicateManager) resolveIndexConflictError(
	ctx context.Context,
	stream DupKVStream,
	tableID int64,
	indexInfo *model.IndexInfo,
	dupeRecordQuota *atomic.Int64,
) (err error) {
	//nolint: errcheck
	defer stream.Close()
	rawHandles := make([][]byte, 0)
	indexKVs := make([][2][]byte, 0)
	for {
		key, val, err := stream.Next()
		if errors.Cause(err) == io.EOF {
			break
		}
		if err != nil {
			return errors.Trace(err)
		}
		m.hasDupe.Store(true)

		h, err := m.decoder.DecodeHandleFromIndex(indexInfo, key, val)
		if err != nil {
			return errors.Trace(err)
		}
		rawHandles = append(rawHandles, tablecodec.EncodeRowKeyWithHandle(tableID, h))
		indexKVs = append(indexKVs, [2][]byte{key, val})

		if len(rawHandles) >= defaultRecordConflictErrorBatch {
			if err := m.resolveIndexHandles(ctx, rawHandles, indexKVs, indexInfo, dupeRecordQuota); err != nil {
				return errors.Trace(err)
			}
			rawHandles = rawHandles[:0]
			indexKVs = indexKVs[:0]
		}
	}
	if len(rawHandles) > 0 {
		if err := m.resolveIndexHandles(ctx, rawHandles, indexKVs, indexInfo, dupeRecordQuota); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (m *DuplicateManager) resolveIndexHandles(
	ctx context.Context,
	rawHandles [][]byte,
	indexKVs [][2][]byte,
	indexInfo *model.IndexInfo,
	dupeRecordQuota *atomic.Int64,
) error {
	snapshot := m.tikvCli.GetSnapshot(math.MaxUint64)
	batchGetMap, err := snapshot.BatchGet(ctx, rawHandles)
	if err != nil {
		return errors.Trace(err)
	}

	handleRows := make([][2][]byte, 0, len(rawHandles))
	for i, rawHandle := range rawHandles {
		rawValue, ok := batchGetMap[string(hack.String(rawHandle))]
		if !ok {
			// The row is deleted, we can ignore it.
			continue
		}
		handleRows = append(handleRows, [2][]byte{rawHandle, rawValue})
		if dupeRecordQuota.Dec() >= 0 {
			h, err := m.decoder.DecodeHandleFromRowKey(rawHandle)
			if err != nil {
				return errors.Trace(err)
			}
			conflictInfo := errormanager.DataConflictInfo{
				RawKey:   indexKVs[i][0],
				RawValue: indexKVs[i][1],
				KeyData:  h.String(),
				Row:      m.decoder.DecodeRawRowDataAsStr(h, rawValue),
			}
			if err := m.errorMgr.RecordIndexConflictError(
				ctx,
				m.logger,
				m.tableName,
				[]string{indexInfo.Name.O},
				[]errormanager.DataConflictInfo{conflictInfo},
				[][]byte{rawHandle},
				[][]byte{rawValue},
			); err != nil {
				return errors.Trace(err)
			}
		}
	}

	err = m.resolveDuplicateRows(ctx, m.tableName, handleRows)
	return errors.Trace(err)
}

func (m *DuplicateManager) resolveDuplicateRows(ctx context.Context, tableName string, handleRows [][2][]byte) error {
	if len(handleRows) == 0 {
		return nil
	}

	errLimiter := rate.NewLimiter(1, 1)
	for {
		err := m.deleteDuplicateRows(ctx, handleRows)
		if err == nil {
			return nil
		}
		if types.ErrBadNumber.Equal(err) {
			m.logger.Warn("delete duplicate rows encounter error", log.ShortError(err))
			return common.ErrResolveDuplicateRows.Wrap(err).GenWithStackByArgs(tableName)
		}
		if log.IsContextCanceledError(err) {
			return err
		}
		if !tikverror.IsErrWriteConflict(errors.Cause(err)) {
			m.logger.Warn("delete duplicate rows encounter error", log.ShortError(err))
		}
		if err = errLimiter.Wait(ctx); err != nil {
			return err
		}
	}
}

func (m *DuplicateManager) deleteDuplicateRows(ctx context.Context, handleRows [][2][]byte) (retErr error) {
	// Starts a Delete transaction.
	txn, err := m.tikvCli.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if retErr == nil {
			retErr = txn.Commit(ctx)
		} else {
			if rollbackErr := txn.Rollback(); rollbackErr != nil {
				m.logger.Warn("failed to rollback transaction", zap.Error(rollbackErr))
			}
		}
	}()

	// Collect all rows & index keys into the deletion transaction.
	// (if the number of duplicates is small this should fit entirely in memory)
	// (Txn's MemBuf's bufferSizeLimit is currently infinity)
	for _, handleRow := range handleRows {
		m.logger.Debug("[resolve-dupe] found row to resolve",
			logutil.Key("handle", handleRow[0]),
			logutil.Key("row", handleRow[1]))

		if err := txn.Delete(handleRow[0]); err != nil {
			return err
		}
		m.metrics.DupeResolveDeleteKeysTotal.WithLabelValues("data").Inc()

		handle, err := m.decoder.DecodeHandleFromRowKey(handleRow[0])
		if err != nil {
			return err
		}

		err = m.decoder.IterRawIndexKeys(handle, handleRow[1], func(key []byte) error {
			m.metrics.DupeResolveDeleteKeysTotal.WithLabelValues("index").Inc()
			return txn.Delete(key)
		})
		if err != nil {
			return err
		}
	}

	m.logger.Debug("[resolve-dupe] number of KV pairs to be deleted", zap.Int("count", txn.Len()))
	return nil
}
