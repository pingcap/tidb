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
	"slices"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/docker/go-units"
	"github.com/google/btree"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/errormanager"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/distsql"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/ranger"
	tikverror "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/tikv"
	kvutil "github.com/tikv/client-go/v2/util"
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

// Less implements btree.Item.
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
	return distsql.TableHandleRangesToKVRanges(nil, tableIDs, tableInfo.IsCommonHandle, ranges)
}

// tableIndexKeyRanges returns all key ranges associated with the tableInfo and indexInfo.
func tableIndexKeyRanges(tableInfo *model.TableInfo, indexInfo *model.IndexInfo) (*tidbkv.KeyRanges, error) {
	tableIDs := physicalTableIDs(tableInfo)
	return distsql.IndexRangesToKVRangesForTables(nil, tableIDs, indexInfo.ID, ranger.FullRange())
}

// DupKVStream is a streaming interface for collecting duplicate key-value pairs.
type DupKVStream interface {
	// Next returns the next key-value pair or any error it encountered.
	// At the end of the stream, the error is io.EOF.
	Next() (key, val []byte, err error)
	// Close closes the stream.
	Close() error
}

// DupKVStreamImpl implements the interface of DupKVStream.
// It collects duplicate key-value pairs from a pebble.DB.
//
//goland:noinspection GoNameStartsWithPackageName
type DupKVStreamImpl struct {
	iter Iter
}

// NewLocalDupKVStream creates a new DupKVStreamImpl with the given duplicate db and key range.
func NewLocalDupKVStream(dupDB *pebble.DB, keyAdapter common.KeyAdapter, keyRange tidbkv.KeyRange) *DupKVStreamImpl {
	opts := &pebble.IterOptions{
		LowerBound: keyRange.StartKey,
		UpperBound: keyRange.EndKey,
	}
	iter := newDupDBIter(dupDB, keyAdapter, opts)
	iter.First()
	return &DupKVStreamImpl{iter: iter}
}

// Next implements the interface of DupKVStream.
func (s *DupKVStreamImpl) Next() (key, val []byte, err error) {
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

// Close implements the interface of DupKVStream.
func (s *DupKVStreamImpl) Close() error {
	return s.iter.Close()
}

type regionError struct {
	inner *errorpb.Error
}

// Error implements the interface of error.
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
	resourceGroupName string,
	taskType string,
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
		ResourceControlContext: &kvrpcpb.ResourceControlContext{
			ResourceGroupName: resourceGroupName,
		},
		RequestSource: kvutil.BuildRequestSource(true, tidbkv.InternalTxnLightning, taskType),
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
	resourceGroupName string,
	taskType string,
) (*RemoteDupKVStream, error) {
	subCtx, cancel := context.WithCancel(ctx)
	cli, err := getDupDetectClient(subCtx, region, keyRange, importClientFactory, resourceGroupName, taskType)
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

// Next implements the interface of DupKVStream.
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

// Close implements the interface of DupKVStream.
func (s *RemoteDupKVStream) Close() error {
	s.cancel()
	return nil
}

// DupeDetector provides methods to collect and decode duplicated KV pairs into row data. The results
// are stored into the errorMgr.
// this object can only be used once, either for local or remote deduplication.
type DupeDetector struct {
	tbl               table.Table
	tableName         string
	splitCli          split.SplitClient
	tikvCli           *tikv.KVStore
	tikvCodec         tikv.Codec
	errorMgr          *errormanager.ErrorManager
	decoder           *kv.TableKVDecoder
	logger            log.Logger
	concurrency       int
	hasDupe           atomic.Bool
	indexID           int64
	resourceGroupName string
	taskType          string
}

// NewDupeDetector creates a new DupeDetector.
func NewDupeDetector(
	tbl table.Table,
	tableName string,
	splitCli split.SplitClient,
	tikvCli *tikv.KVStore,
	tikvCodec tikv.Codec,
	errMgr *errormanager.ErrorManager,
	sessOpts *encode.SessionOptions,
	concurrency int,
	logger log.Logger,
	resourceGroupName string,
	taskType string,
) (*DupeDetector, error) {
	logger = logger.With(zap.String("tableName", tableName))
	decoder, err := kv.NewTableKVDecoder(tbl, tableName, sessOpts, logger)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &DupeDetector{
		tbl:               tbl,
		tableName:         tableName,
		splitCli:          splitCli,
		tikvCli:           tikvCli,
		tikvCodec:         tikvCodec,
		errorMgr:          errMgr,
		decoder:           decoder,
		logger:            logger,
		concurrency:       concurrency,
		indexID:           sessOpts.IndexID,
		resourceGroupName: resourceGroupName,
		taskType:          taskType,
	}, nil
}

// HasDuplicate returns true if there are duplicated KV pairs.
func (m *DupeDetector) HasDuplicate() bool {
	return m.hasDupe.Load()
}

// RecordDataConflictError records data conflicts to errorMgr. The key received from stream must be a row key.
func (m *DupeDetector) RecordDataConflictError(ctx context.Context, stream DupKVStream) error {
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
		key, err = m.tikvCodec.DecodeKey(key)
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

func (m *DupeDetector) saveIndexHandles(ctx context.Context, handles pendingIndexHandles) error {
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
			m.logger.Warn("can not found row data corresponding to the handle", zap.String("category", "detect-dupe"),
				logutil.Key("rawHandle", rawHandle))
		}
	}

	err = m.errorMgr.RecordIndexConflictError(ctx, m.logger, m.tableName,
		handles.indexNames, handles.dataConflictInfos, handles.rawHandles, rawRows)
	return errors.Trace(err)
}

// RecordIndexConflictError records index conflicts to errorMgr. The key received from stream must be an index key.
func (m *DupeDetector) RecordIndexConflictError(ctx context.Context, stream DupKVStream, tableID int64, indexInfo *model.IndexInfo) error {
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
		key, err = m.tikvCodec.DecodeKey(key)
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
var BuildDuplicateTaskForTest = func(m *DupeDetector) ([]dupTask, error) {
	return m.buildDupTasks()
}

type dupTask struct {
	tidbkv.KeyRange
	tableID   int64
	indexInfo *model.IndexInfo
}

func (m *DupeDetector) buildDupTasks() ([]dupTask, error) {
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
				KeyRange:  r,
				tableID:   tid,
				indexInfo: indexInfo,
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

	// Encode all the tasks
	for i := range tasks {
		tasks[i].StartKey, tasks[i].EndKey = m.tikvCodec.EncodeRange(tasks[i].StartKey, tasks[i].EndKey)
	}
	return tasks, nil
}

func (m *DupeDetector) buildIndexDupTasks() ([]dupTask, error) {
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
					KeyRange:  r,
					tableID:   tid,
					indexInfo: indexInfo,
				})
			}
		})
		return tasks, nil
	}
	return nil, nil
}

func (m *DupeDetector) splitLocalDupTaskByKeys(
	task dupTask,
	dupDB *pebble.DB,
	keyAdapter common.KeyAdapter,
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
			tableID:   task.tableID,
			indexInfo: task.indexInfo,
		})
	}
	return newDupTasks, nil
}

func (m *DupeDetector) buildLocalDupTasks(dupDB *pebble.DB, keyAdapter common.KeyAdapter) ([]dupTask, error) {
	tasks, err := m.buildDupTasks()
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
func (m *DupeDetector) CollectDuplicateRowsFromDupDB(ctx context.Context, dupDB *pebble.DB, keyAdapter common.KeyAdapter) error {
	tasks, err := m.buildLocalDupTasks(dupDB, keyAdapter)
	if err != nil {
		return errors.Trace(err)
	}
	logger := m.logger
	logger.Info("collect duplicate rows from local duplicate db", zap.String("category", "detect-dupe"), zap.Int("tasks", len(tasks)))

	pool := utils.NewWorkerPool(uint(m.concurrency), "collect duplicate rows from duplicate db")
	g, gCtx := errgroup.WithContext(ctx)
	for _, task := range tasks {
		task := task
		pool.ApplyOnErrorGroup(g, func() error {
			if err := common.Retry("collect local duplicate rows", logger, func() error {
				stream := NewLocalDupKVStream(dupDB, keyAdapter, task.KeyRange)
				var err error
				if task.indexInfo == nil {
					err = m.RecordDataConflictError(gCtx, stream)
				} else {
					err = m.RecordIndexConflictError(gCtx, stream, task.tableID, task.indexInfo)
				}
				return errors.Trace(err)
			}); err != nil {
				return errors.Trace(err)
			}

			// Delete the key range in duplicate DB since we have the duplicates have been collected.
			rawStartKey := keyAdapter.Encode(nil, task.StartKey, common.MinRowID)
			rawEndKey := keyAdapter.Encode(nil, task.EndKey, common.MinRowID)
			err = dupDB.DeleteRange(rawStartKey, rawEndKey, nil)
			return errors.Trace(err)
		})
	}
	return errors.Trace(g.Wait())
}

func (m *DupeDetector) splitKeyRangeByRegions(
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

func (m *DupeDetector) processRemoteDupTaskOnce(
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
				stream, err := NewRemoteDupKVStream(ctx, region, kr, importClientFactory, m.resourceGroupName, m.taskType)
				if err != nil {
					return errors.Annotatef(err, "failed to create remote duplicate kv stream")
				}
				if task.indexInfo == nil {
					err = m.RecordDataConflictError(ctx, stream)
				} else {
					err = m.RecordIndexConflictError(ctx, stream, task.tableID, task.indexInfo)
				}
				if err != nil {
					return errors.Annotatef(err, "failed to record conflict errors")
				}
				return nil
			}()
			if err != nil {
				if regionErr, ok := errors.Cause(err).(regionError); ok {
					logger.Debug("collect duplicate rows from region failed due to region error", zap.String("category", "detect-dupe"), zap.Error(regionErr))
				} else {
					logger.Warn("collect duplicate rows from region failed", zap.String("category", "detect-dupe"), log.ShortError(err))
				}
				metErr.Set(err)
			} else {
				logger.Debug("collect duplicate rows from region completed", zap.String("category", "detect-dupe"))
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
func (m *DupeDetector) processRemoteDupTask(
	ctx context.Context,
	task dupTask,
	logger log.Logger,
	importClientFactory ImportClientFactory,
	regionPool *utils.WorkerPool,
) error {
	regionErrRetryAttempts := split.WaitRegionOnlineAttemptTimes
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
			_, isRegionErr := errors.Cause(err).(regionError)
			if isRegionErr && regionErrRetryAttempts > 0 {
				regionErrRetryAttempts--
				if regionErrRetryAttempts%10 == 0 {
					logger.Warn("process remote dupTask encounters region error, retrying", zap.String("category", "detect-dupe"),
						log.ShortError(err), zap.Int("remainRegionErrAttempts", regionErrRetryAttempts))
				}
				continue
			}

			remainAttempts--
			if remainAttempts <= 0 {
				logger.Error("all attempts to process the remote dupTask have failed", zap.String("category", "detect-dupe"), log.ShortError(err))
				return errors.Trace(err)
			}
		}
		logger.Warn("process remote dupTask encounters error, retrying", zap.String("category", "detect-dupe"),
			log.ShortError(err), zap.Int("remainAttempts", remainAttempts))
	}
}

// CollectDuplicateRowsFromTiKV collects duplicates from the remote TiKV and records all duplicate row info into errorMgr.
func (m *DupeDetector) CollectDuplicateRowsFromTiKV(ctx context.Context, importClientFactory ImportClientFactory) error {
	tasks, err := m.buildDupTasks()
	if err != nil {
		return errors.Trace(err)
	}
	logger := m.logger
	logger.Info("collect duplicate rows from tikv", zap.String("category", "detect-dupe"), zap.Int("tasks", len(tasks)))

	taskPool := utils.NewWorkerPool(uint(m.concurrency), "collect duplicate rows from tikv")
	regionPool := utils.NewWorkerPool(uint(m.concurrency), "collect duplicate rows from tikv by region")
	g, gCtx := errgroup.WithContext(ctx)
	for _, task := range tasks {
		task := task
		taskPool.ApplyOnErrorGroup(g, func() error {
			taskLogger := logger.With(
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
			return errors.Trace(err)
		})
	}
	return errors.Trace(g.Wait())
}

// DupeController is used to collect duplicate keys from local and remote data source and resolve duplication.
type DupeController struct {
	splitCli  split.SplitClient
	tikvCli   *tikv.KVStore
	tikvCodec tikv.Codec
	errorMgr  *errormanager.ErrorManager
	// number of workers to do duplicate detection on local db and TiKV
	// on TiKV, it is the max number of regions being checked concurrently
	dupeConcurrency     int
	duplicateDB         *pebble.DB
	keyAdapter          common.KeyAdapter
	importClientFactory ImportClientFactory
	resourceGroupName   string
	taskType            string
}

// CollectLocalDuplicateRows collect duplicate keys from local db. We will store the duplicate keys which
// may be repeated with other keys in local data source.
func (local *DupeController) CollectLocalDuplicateRows(ctx context.Context, tbl table.Table, tableName string, opts *encode.SessionOptions) (hasDupe bool, err error) {
	logger := log.FromContext(ctx).With(zap.String("table", tableName)).Begin(zap.InfoLevel, "[detect-dupe] collect local duplicate keys")
	defer func() {
		logger.End(zap.ErrorLevel, err)
	}()

	duplicateManager, err := NewDupeDetector(tbl, tableName, local.splitCli, local.tikvCli, local.tikvCodec,
		local.errorMgr, opts, local.dupeConcurrency, log.FromContext(ctx), local.resourceGroupName, local.taskType)
	if err != nil {
		return false, errors.Trace(err)
	}
	if err := duplicateManager.CollectDuplicateRowsFromDupDB(ctx, local.duplicateDB, local.keyAdapter); err != nil {
		return false, errors.Trace(err)
	}
	return duplicateManager.HasDuplicate(), nil
}

// CollectRemoteDuplicateRows collect duplicate keys from remote TiKV storage. This keys may be duplicate with
// the data import by other lightning.
func (local *DupeController) CollectRemoteDuplicateRows(ctx context.Context, tbl table.Table, tableName string, opts *encode.SessionOptions) (hasDupe bool, err error) {
	logger := log.FromContext(ctx).With(zap.String("table", tableName)).Begin(zap.InfoLevel, "[detect-dupe] collect remote duplicate keys")
	defer func() {
		logger.End(zap.ErrorLevel, err)
	}()

	duplicateManager, err := NewDupeDetector(tbl, tableName, local.splitCli, local.tikvCli, local.tikvCodec,
		local.errorMgr, opts, local.dupeConcurrency, log.FromContext(ctx), local.resourceGroupName, local.taskType)
	if err != nil {
		return false, errors.Trace(err)
	}
	if err := duplicateManager.CollectDuplicateRowsFromTiKV(ctx, local.importClientFactory); err != nil {
		return false, errors.Trace(err)
	}
	return duplicateManager.HasDuplicate(), nil
}

// ResolveDuplicateRows resolves duplicated rows by deleting/inserting data
// according to the required algorithm.
func (local *DupeController) ResolveDuplicateRows(ctx context.Context, tbl table.Table, tableName string, algorithm config.DuplicateResolutionAlgorithm) (err error) {
	logger := log.FromContext(ctx).With(zap.String("table", tableName)).Begin(zap.InfoLevel, "[resolve-dupe] resolve duplicate rows")
	defer func() {
		logger.End(zap.ErrorLevel, err)
	}()

	switch algorithm {
	case config.DupeResAlgRecord, config.DupeResAlgNone:
		logger.Warn("skipping resolution due to selected algorithm. this table will become inconsistent!", zap.String("category", "resolve-dupe"), zap.Stringer("algorithm", algorithm))
		return nil
	case config.DupeResAlgRemove:
	default:
		panic(fmt.Sprintf("[resolve-dupe] unknown resolution algorithm %v", algorithm))
	}

	// TODO: reuse the *kv.SessionOptions from NewEncoder for picking the correct time zone.
	decoder, err := kv.NewTableKVDecoder(tbl, tableName, &encode.SessionOptions{
		SQLMode: mysql.ModeStrictAllTables,
	}, log.FromContext(ctx))
	if err != nil {
		return err
	}

	tableIDs := physicalTableIDs(tbl.Meta())
	keyInTable := func(key []byte) bool {
		return slices.Contains(tableIDs, tablecodec.DecodeTableID(key))
	}

	errLimiter := rate.NewLimiter(1, 1)
	pool := utils.NewWorkerPool(uint(local.dupeConcurrency), "resolve duplicate rows")
	err = local.errorMgr.ResolveAllConflictKeys(
		ctx, tableName, pool,
		func(ctx context.Context, handleRows [][2][]byte) error {
			for {
				err := local.deleteDuplicateRows(ctx, logger, handleRows, decoder, keyInTable)
				if err == nil {
					return nil
				}
				if types.ErrBadNumber.Equal(err) {
					logger.Warn("delete duplicate rows encounter error", log.ShortError(err))
					return common.ErrResolveDuplicateRows.Wrap(err).GenWithStackByArgs(tableName)
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

func (local *DupeController) deleteDuplicateRows(
	ctx context.Context,
	logger *log.Task,
	handleRows [][2][]byte,
	decoder *kv.TableKVDecoder,
	keyInTable func(key []byte) bool,
) (err error) {
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
		logger.Debug("will delete key", zap.String("category", "resolve-dupe"), logutil.Key("key", key))
		return txn.Delete(key)
	}

	// Collect all rows & index keys into the deletion transaction.
	// (if the number of duplicates is small this should fit entirely in memory)
	// (Txn's MemBuf's bufferSizeLimit is currently infinity)
	for _, handleRow := range handleRows {
		// Skip the row key if it's not in the table.
		// This can happen if the table has been recreated or truncated,
		// and the duplicate key is from the old table.
		if !keyInTable(handleRow[0]) {
			continue
		}
		logger.Debug("found row to resolve", zap.String("category", "resolve-dupe"),
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

	logger.Debug("number of KV pairs to be deleted", zap.String("category", "resolve-dupe"), zap.Int("count", txn.Len()))
	return nil
}
