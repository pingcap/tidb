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
	"io"
	"math"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/docker/go-units"
	"github.com/google/btree"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	pkgkv "github.com/pingcap/tidb/br/pkg/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/errormanager"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/distsql"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
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
func makePendingIndexHandlesWithCapacity(cap int) pendingIndexHandles {
	return pendingIndexHandles{
		dataConflictInfos: make([]errormanager.DataConflictInfo, 0, cap),
		indexNames:        make([]string, 0, cap),
		handles:           make([]tidbkv.Handle, 0, cap),
		rawHandles:        make([][]byte, 0, cap),
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
func tableHandleKeyRanges(tableInfo *model.TableInfo) ([]tidbkv.KeyRange, error) {
	ranges := ranger.FullIntRange(false)
	if tableInfo.IsCommonHandle {
		ranges = ranger.FullRange()
	}
	tableIDs := physicalTableIDs(tableInfo)
	return distsql.TableHandleRangesToKVRanges(nil, tableIDs, tableInfo.IsCommonHandle, ranges, nil)
}

// tableIndexKeyRanges returns all key ranges associated with the tableInfo and indexInfo.
func tableIndexKeyRanges(tableInfo *model.TableInfo, indexInfo *model.IndexInfo) ([]tidbkv.KeyRange, error) {
	tableIDs := physicalTableIDs(tableInfo)
	var keyRanges []tidbkv.KeyRange
	for _, tid := range tableIDs {
		partitionKeysRanges, err := distsql.IndexRangesToKVRanges(nil, tid, indexInfo.ID, ranger.FullRange(), nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
		keyRanges = append(keyRanges, partitionKeysRanges...)
	}
	return keyRanges, nil
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
//goland:noinspection GoNameStartsWithPackageName
type LocalDupKVStream struct {
	iter pkgkv.Iter
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
	region *restore.RegionInfo,
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
	region *restore.RegionInfo,
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
	splitCli    restore.SplitClient
	tikvCli     *tikv.KVStore
	errorMgr    *errormanager.ErrorManager
	decoder     *kv.TableKVDecoder
	logger      log.Logger
	concurrency int
	hasDupe     *atomic.Bool
}

// NewDuplicateManager creates a new DuplicateManager.
func NewDuplicateManager(
	tbl table.Table,
	tableName string,
	splitCli restore.SplitClient,
	tikvCli *tikv.KVStore,
	errMgr *errormanager.ErrorManager,
	sessOpts *kv.SessionOptions,
	concurrency int,
	hasDupe *atomic.Bool,
) (*DuplicateManager, error) {
	decoder, err := kv.NewTableKVDecoder(tbl, tableName, sessOpts)
	if err != nil {
		return nil, errors.Trace(err)
	}
	logger := log.With(zap.String("tableName", tableName))
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
	}, nil
}

// RecordDataConflictError records data conflicts to errorMgr. The key received from stream must be a row key.
func (m *DuplicateManager) RecordDataConflictError(ctx context.Context, stream DupKVStream) error {
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

type dupTask struct {
	tidbkv.KeyRange
	tableID   int64
	indexInfo *model.IndexInfo
}

func (m *DuplicateManager) buildDupTasks() ([]dupTask, error) {
	var tasks []dupTask
	keyRanges, err := tableHandleKeyRanges(m.tbl.Meta())
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, kr := range keyRanges {
		tableID := tablecodec.DecodeTableID(kr.StartKey)
		tasks = append(tasks, dupTask{
			KeyRange: kr,
			tableID:  tableID,
		})
	}
	for _, indexInfo := range m.tbl.Meta().Indices {
		if indexInfo.State != model.StatePublic {
			continue
		}
		keyRanges, err = tableIndexKeyRanges(m.tbl.Meta(), indexInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}
		for _, kr := range keyRanges {
			tableID := tablecodec.DecodeTableID(kr.StartKey)
			tasks = append(tasks, dupTask{
				KeyRange:  kr,
				tableID:   tableID,
				indexInfo: indexInfo,
			})
		}
	}
	return tasks, nil
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
	var newDupTasks []dupTask
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

func (m *DuplicateManager) buildLocalDupTasks(dupDB *pebble.DB, keyAdapter KeyAdapter) ([]dupTask, error) {
	tasks, err := m.buildDupTasks()
	if err != nil {
		return nil, errors.Trace(err)
	}
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
	tasks, err := m.buildLocalDupTasks(dupDB, keyAdapter)
	if err != nil {
		return errors.Trace(err)
	}
	logger := m.logger
	logger.Info("[detect-dupe] collect duplicate rows from local duplicate db", zap.Int("tasks", len(tasks)))

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
			rawStartKey := keyAdapter.Encode(nil, task.StartKey, math.MinInt64)
			rawEndKey := keyAdapter.Encode(nil, task.EndKey, math.MinInt64)
			err = dupDB.DeleteRange(rawStartKey, rawEndKey, nil)
			return errors.Trace(err)
		})
	}
	return errors.Trace(g.Wait())
}

func (m *DuplicateManager) splitKeyRangeByRegions(
	ctx context.Context, keyRange tidbkv.KeyRange,
) ([]*restore.RegionInfo, []tidbkv.KeyRange, error) {
	rawStartKey := codec.EncodeBytes(nil, keyRange.StartKey)
	rawEndKey := codec.EncodeBytes(nil, keyRange.EndKey)
	allRegions, err := restore.PaginateScanRegion(ctx, m.splitCli, rawStartKey, rawEndKey, 1024)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	regions := make([]*restore.RegionInfo, 0, len(allRegions))
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
	var (
		regions   []*restore.RegionInfo
		keyRanges []tidbkv.KeyRange
	)
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
	tasks, err := m.buildDupTasks()
	if err != nil {
		return errors.Trace(err)
	}
	logger := m.logger
	logger.Info("[detect-dupe] collect duplicate rows from tikv", zap.Int("tasks", len(tasks)))

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
