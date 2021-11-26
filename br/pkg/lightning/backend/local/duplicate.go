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
	"sort"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
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
	"github.com/pingcap/tidb/util/ranger"
	tikvclient "github.com/tikv/client-go/v2/tikv"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

const (
	maxGetRequestKeyCount = 1024
)

type DuplicateRequest struct {
	tableID   int64
	start     tidbkv.Key
	end       tidbkv.Key
	indexInfo *model.IndexInfo
}

type DuplicateManager struct {
	errorMgr          *errormanager.ErrorManager
	splitCli          restore.SplitClient
	tikvCli           *tikvclient.KVStore
	regionConcurrency int
	connPool          common.GRPCConns
	tls               *common.TLS
	ts                uint64
	keyAdapter        KeyAdapter
	remoteWorkerPool  *utils.WorkerPool
	opts              *kv.SessionOptions
}

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

// appendAt pushes `other[i]` to the end of indexHandles.
func (indexHandles *pendingIndexHandles) appendAt(
	other *pendingIndexHandles,
	i int,
) {
	indexHandles.append(
		other.dataConflictInfos[i],
		other.indexNames[i],
		other.handles[i],
		other.rawHandles[i],
	)
}

// extends concatenates `other` to the end of indexHandles.
func (indexHandles *pendingIndexHandles) extend(other *pendingIndexHandles) {
	indexHandles.dataConflictInfos = append(indexHandles.dataConflictInfos, other.dataConflictInfos...)
	indexHandles.indexNames = append(indexHandles.indexNames, other.indexNames...)
	indexHandles.handles = append(indexHandles.handles, other.handles...)
	indexHandles.rawHandles = append(indexHandles.rawHandles, other.rawHandles...)
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

// searchSortedRawHandle looks up for the index i such that `rawHandles[i] == rawHandle`.
// This function assumes indexHandles is already sorted, and rawHandle does exist in it.
func (indexHandles *pendingIndexHandles) searchSortedRawHandle(rawHandle []byte) int {
	return sort.Search(indexHandles.Len(), func(i int) bool {
		return bytes.Compare(indexHandles.rawHandles[i], rawHandle) >= 0
	})
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

// NewDuplicateManager creates a new *DuplicateManager.
//
// This object provides methods to collect and decode duplicated KV pairs into row data. The results
// are stored into the errorMgr.
func NewDuplicateManager(local *local, ts uint64, opts *kv.SessionOptions) (*DuplicateManager, error) {
	return &DuplicateManager{
		errorMgr:          local.errorMgr,
		tls:               local.tls,
		regionConcurrency: local.tcpConcurrency,
		splitCli:          local.splitCli,
		tikvCli:           local.tikvCli,
		keyAdapter:        duplicateKeyAdapter{},
		ts:                ts,
		connPool:          common.NewGRPCConns(),
		// TODO: not sure what is the correct concurrency value.
		remoteWorkerPool: utils.NewWorkerPool(uint(local.tcpConcurrency), "duplicates"),
		opts:             opts,
	}, nil
}

// CollectDuplicateRowsFromTiKV collects duplicated rows already imported into TiKV.
//
// Collection result are saved into the ErrorManager.
func (manager *DuplicateManager) CollectDuplicateRowsFromTiKV(
	ctx context.Context,
	tbl table.Table,
	tableName string,
) (hasDupe bool, err error) {
	logTask := log.With(zap.String("table", tableName)).Begin(zapcore.InfoLevel, "collect duplicate data from remote TiKV")
	defer func() {
		logTask.End(zapcore.InfoLevel, err)
	}()

	reqs, err := buildDuplicateRequests(tbl.Meta())
	if err != nil {
		return false, err
	}

	// TODO: reuse the *kv.SessionOptions from NewEncoder for picking the correct time zone.
	decoder, err := kv.NewTableKVDecoder(tbl, tableName, manager.opts)
	if err != nil {
		return false, err
	}
	g, rpcctx := errgroup.WithContext(ctx)
	atomicHasDupe := atomic.NewBool(false)
	for _, r := range reqs {
		req := r
		manager.remoteWorkerPool.ApplyOnErrorGroup(g, func() error {
			err := manager.sendRequestToTiKV(rpcctx, decoder, req, atomicHasDupe)
			if err != nil {
				log.L().Error("error occur when collect duplicate data from TiKV", zap.Error(err))
			}
			return err
		})
	}
	err = errors.Trace(g.Wait())
	return atomicHasDupe.Load(), err
}

func (manager *DuplicateManager) sendRequestToTiKV(ctx context.Context,
	decoder *kv.TableKVDecoder,
	req *DuplicateRequest,
	hasDupe *atomic.Bool,
) error {
	logger := log.With(
		zap.String("table", decoder.Name()),
		zap.Int64("tableID", req.tableID),
		logutil.Key("startKey", req.start),
		logutil.Key("endKey", req.end))

	startKey := codec.EncodeBytes([]byte{}, req.start)
	endKey := codec.EncodeBytes([]byte{}, req.end)

	regions, err := restore.PaginateScanRegion(ctx, manager.splitCli, startKey, endKey, scanRegionLimit)
	if err != nil {
		return err
	}
	tryTimes := 0
	indexHandles := makePendingIndexHandlesWithCapacity(0)
	for len(regions) > 0 {
		if tryTimes > maxRetryTimes {
			return errors.Errorf("retry time exceed limit")
		}
		unfinishedRegions := make([]*restore.RegionInfo, 0)
		waitingClients := make([]import_sstpb.ImportSST_DuplicateDetectClient, 0)
		watingRegions := make([]*restore.RegionInfo, 0)
		for idx, region := range regions {
			if len(waitingClients) > manager.regionConcurrency {
				r := regions[idx:]
				unfinishedRegions = append(unfinishedRegions, r...)
				break
			}
			_, start, _ := codec.DecodeBytes(region.Region.StartKey, []byte{})
			_, end, _ := codec.DecodeBytes(region.Region.EndKey, []byte{})
			if bytes.Compare(startKey, region.Region.StartKey) > 0 {
				start = req.start
			}
			if region.Region.EndKey == nil || len(region.Region.EndKey) == 0 || bytes.Compare(endKey, region.Region.EndKey) < 0 {
				end = req.end
			}

			logger.Debug("[detect-dupe] get duplicate stream",
				zap.Int("localStreamID", idx),
				logutil.Region(region.Region),
				logutil.Leader(region.Leader),
				logutil.Key("regionStartKey", start),
				logutil.Key("regionEndKey", end))
			cli, err := manager.getDuplicateStream(ctx, region, start, end)
			if err != nil {
				r, err := manager.splitCli.GetRegionByID(ctx, region.Region.GetId())
				if err != nil {
					unfinishedRegions = append(unfinishedRegions, region)
				} else {
					unfinishedRegions = append(unfinishedRegions, r)
				}
			} else {
				waitingClients = append(waitingClients, cli)
				watingRegions = append(watingRegions, region)
			}
		}

		if indexHandles.Len() > 0 {
			handles := manager.getValues(ctx, decoder, indexHandles)
			if handles.Len() > 0 {
				indexHandles = handles
			} else {
				indexHandles.truncate()
			}
		}

		for idx, cli := range waitingClients {
			region := watingRegions[idx]
			cliLogger := logger.With(
				zap.Int("localStreamID", idx),
				logutil.Region(region.Region),
				logutil.Leader(region.Leader))
			for {
				resp, reqErr := cli.Recv()
				hasErr := false
				if reqErr != nil {
					if errors.Cause(reqErr) == io.EOF {
						cliLogger.Debug("[detect-dupe] exhausted duplication stream")
						break
					}
					hasErr = true
				}

				if hasErr || resp.GetKeyError() != nil {
					r, err := manager.splitCli.GetRegionByID(ctx, region.Region.GetId())
					if err != nil {
						unfinishedRegions = append(unfinishedRegions, region)
					} else {
						unfinishedRegions = append(unfinishedRegions, r)
					}
				}
				if hasErr {
					cliLogger.Warn("[detect-dupe] meet error when recving duplicate detect response from TiKV, retry again",
						zap.Error(reqErr))
					break
				}
				if resp.GetKeyError() != nil {
					cliLogger.Warn("[detect-dupe] meet key error in duplicate detect response from TiKV, retry again ",
						zap.String("KeyError", resp.GetKeyError().GetMessage()))
					break
				}

				if resp.GetRegionError() != nil {
					cliLogger.Warn("[detect-dupe] meet key error in duplicate detect response from TiKV, retry again ",
						zap.String("RegionError", resp.GetRegionError().GetMessage()))

					r, err := restore.PaginateScanRegion(ctx, manager.splitCli, watingRegions[idx].Region.GetStartKey(), watingRegions[idx].Region.GetEndKey(), scanRegionLimit)
					if err != nil {
						unfinishedRegions = append(unfinishedRegions, watingRegions[idx])
					} else {
						unfinishedRegions = append(unfinishedRegions, r...)
					}
					break
				}

				if len(resp.Pairs) > 0 {
					hasDupe.Store(true)
				}

				handles, err := manager.storeDuplicateData(ctx, resp, decoder, req)
				if err != nil {
					return err
				}
				if handles.Len() > 0 {
					indexHandles.extend(&handles)
				}
			}
		}

		// it means that all the regions sent to TiKV fail, so we must sleep for a while to avoid retrying too frequently.
		if len(unfinishedRegions) == len(regions) {
			tryTimes += 1
			time.Sleep(defaultRetryBackoffTime)
		}
		regions = unfinishedRegions
	}
	return nil
}

func (manager *DuplicateManager) storeDuplicateData(
	ctx context.Context,
	resp *import_sstpb.DuplicateDetectResponse,
	decoder *kv.TableKVDecoder,
	req *DuplicateRequest,
) (pendingIndexHandles, error) {
	var err error
	var dataConflictInfos []errormanager.DataConflictInfo
	indexHandles := makePendingIndexHandlesWithCapacity(len(resp.Pairs))

	loggerIndexName := "PRIMARY"
	if req.indexInfo != nil {
		loggerIndexName = req.indexInfo.Name.O
	}
	superLogger := log.With(
		zap.String("table", decoder.Name()),
		zap.Int64("tableID", req.tableID),
		zap.String("index", loggerIndexName))

	for _, kv := range resp.Pairs {
		logger := superLogger.With(
			logutil.Key("key", kv.Key), logutil.Key("value", kv.Value),
			zap.Uint64("commit-ts", kv.CommitTs))

		var h tidbkv.Handle
		if req.indexInfo != nil {
			h, err = decoder.DecodeHandleFromIndex(req.indexInfo, kv.Key, kv.Value)
		} else {
			h, err = decoder.DecodeHandleFromTable(kv.Key)
		}
		if err != nil {
			logger.Error("decode handle error", log.ShortError(err))
			continue
		}
		logger.Debug("[detect-dupe] remote dupe response",
			logutil.Redact(zap.Stringer("handle", h)))

		conflictInfo := errormanager.DataConflictInfo{
			RawKey:   kv.Key,
			RawValue: kv.Value,
			KeyData:  h.String(),
		}

		if req.indexInfo != nil {
			indexHandles.append(
				conflictInfo,
				req.indexInfo.Name.O,
				h, decoder.EncodeHandleKey(req.tableID, h))
		} else {
			conflictInfo.Row = decoder.DecodeRawRowDataAsStr(h, kv.Value)
			dataConflictInfos = append(dataConflictInfos, conflictInfo)
		}
	}

	err = manager.errorMgr.RecordDataConflictError(ctx, log.L(), decoder.Name(), dataConflictInfos)
	if err != nil {
		return indexHandles, err
	}

	if len(indexHandles.dataConflictInfos) == 0 {
		return indexHandles, nil
	}
	return manager.getValues(ctx, decoder, indexHandles), nil
}

// CollectDuplicateRowsFromLocalIndex collects rows by read the index in db.
func (manager *DuplicateManager) CollectDuplicateRowsFromLocalIndex(
	ctx context.Context,
	tbl table.Table,
	tableName string,
	db *pebble.DB,
) (bool, error) {
	// TODO: reuse the *kv.SessionOptions from NewEncoder for picking the correct time zone.
	decoder, err := kv.NewTableKVDecoder(tbl, tableName, manager.opts)
	if err != nil {
		return false, errors.Trace(err)
	}

	logger := log.With(zap.String("table", tableName))

	allRanges := make([]tidbkv.KeyRange, 0)
	tableIDs := physicalTableIDs(tbl.Meta())
	// Collect row handle duplicates.
	var dataConflictInfos []errormanager.DataConflictInfo
	hasDataConflict := false
	{
		ranges := ranger.FullIntRange(false)
		if tbl.Meta().IsCommonHandle {
			ranges = ranger.FullRange()
		}
		keyRanges, err := distsql.TableHandleRangesToKVRanges(nil, tableIDs, tbl.Meta().IsCommonHandle, ranges, nil)
		if err != nil {
			return false, errors.Trace(err)
		}
		allRanges = append(allRanges, keyRanges...)
		for _, r := range keyRanges {
			logger.Debug("[detect-dupe] collect local range",
				logutil.Key("startKey", r.StartKey),
				logutil.Key("endKey", r.EndKey))
			startKey := codec.EncodeBytes([]byte{}, r.StartKey)
			endKey := codec.EncodeBytes([]byte{}, r.EndKey)
			opts := &pebble.IterOptions{
				LowerBound: startKey,
				UpperBound: endKey,
			}

			if err := func() error {
				iter := db.NewIter(opts)
				defer iter.Close()

				for iter.First(); iter.Valid(); iter.Next() {
					hasDataConflict = true
					rawKey, _, _, err := manager.keyAdapter.Decode(nil, iter.Key())
					if err != nil {
						return err
					}
					rawValue := make([]byte, len(iter.Value()))
					copy(rawValue, iter.Value())

					h, err := decoder.DecodeHandleFromTable(rawKey)
					if err != nil {
						return err
					}
					logger.Debug("[detect-dupe] found local data conflict",
						logutil.Key("key", rawKey),
						logutil.Key("value", rawValue),
						logutil.Redact(zap.Stringer("handle", h)))

					conflictInfo := errormanager.DataConflictInfo{
						RawKey:   rawKey,
						RawValue: rawValue,
						KeyData:  h.String(),
						Row:      decoder.DecodeRawRowDataAsStr(h, rawValue),
					}
					dataConflictInfos = append(dataConflictInfos, conflictInfo)
				}
				if err := iter.Error(); err != nil {
					return err
				}
				if err := manager.errorMgr.RecordDataConflictError(ctx, log.L(), decoder.Name(), dataConflictInfos); err != nil {
					return err
				}
				dataConflictInfos = dataConflictInfos[:0]
				return nil
			}(); err != nil {
				return false, errors.Trace(err)
			}
			db.DeleteRange(startKey, endKey, &pebble.WriteOptions{Sync: false})
		}
	}
	handles := makePendingIndexHandlesWithCapacity(0)
	for _, indexInfo := range tbl.Meta().Indices {
		if indexInfo.State != model.StatePublic {
			continue
		}
		ranges := ranger.FullRange()
		var keysRanges []tidbkv.KeyRange
		for _, id := range tableIDs {
			partitionKeysRanges, err := distsql.IndexRangesToKVRanges(nil, id, indexInfo.ID, ranges, nil)
			if err != nil {
				return false, err
			}
			keysRanges = append(keysRanges, partitionKeysRanges...)
		}
		allRanges = append(allRanges, keysRanges...)
		for _, r := range keysRanges {
			tableID := tablecodec.DecodeTableID(r.StartKey)
			startKey := codec.EncodeBytes([]byte{}, r.StartKey)
			endKey := codec.EncodeBytes([]byte{}, r.EndKey)
			opts := &pebble.IterOptions{
				LowerBound: startKey,
				UpperBound: endKey,
			}
			indexLogger := logger.With(
				zap.Int64("tableID", tableID),
				zap.String("index", indexInfo.Name.O),
				zap.Int64("indexID", indexInfo.ID),
				logutil.Key("startKey", startKey),
				logutil.Key("endKey", endKey))
			indexLogger.Info("[detect-dupe] collect index from db")

			if err := func() error {
				iter := db.NewIter(opts)
				defer iter.Close()

				for iter.First(); iter.Valid(); iter.Next() {
					hasDataConflict = true
					rawKey, _, _, err := manager.keyAdapter.Decode(nil, iter.Key())
					if err != nil {
						indexLogger.Error(
							"[detect-dupe] decode key error when query handle for duplicate index",
							zap.Binary("key", iter.Key()),
						)
						return err
					}
					rawValue := make([]byte, len(iter.Value()))
					copy(rawValue, iter.Value())
					h, err := decoder.DecodeHandleFromIndex(indexInfo, rawKey, rawValue)
					if err != nil {
						indexLogger.Error("[detect-dupe] decode handle error from index for duplicatedb",
							zap.Error(err), logutil.Key("rawKey", rawKey),
							logutil.Key("value", rawValue))
						return err
					}
					indexLogger.Debug("[detect-dupe] found local index conflict, stashing",
						logutil.Key("key", rawKey),
						logutil.Key("value", rawValue),
						logutil.Redact(zap.Stringer("handle", h)))
					handles.append(
						errormanager.DataConflictInfo{
							RawKey:   rawKey,
							RawValue: rawValue,
							KeyData:  h.String(),
						},
						indexInfo.Name.O,
						h,
						decoder.EncodeHandleKey(tableID, h))
					if handles.Len() > maxGetRequestKeyCount {
						handles = manager.getValues(ctx, decoder, handles)
					}
				}
				if handles.Len() > 0 {
					handles = manager.getValues(ctx, decoder, handles)
				}
				if handles.Len() == 0 {
					db.DeleteRange(startKey, endKey, &pebble.WriteOptions{Sync: false})
				}
				return nil
			}(); err != nil {
				return false, errors.Trace(err)
			}
		}
	}

	for i := 0; i < maxRetryTimes && handles.Len() > 0; i++ {
		handles = manager.getValues(ctx, decoder, handles)
	}
	if handles.Len() > 0 {
		return false, errors.Errorf("retry getValues time exceed limit")
	}
	for _, r := range allRanges {
		startKey := codec.EncodeBytes([]byte{}, r.StartKey)
		endKey := codec.EncodeBytes([]byte{}, r.EndKey)
		db.DeleteRange(startKey, endKey, &pebble.WriteOptions{Sync: false})
	}
	return hasDataConflict, nil
}

func (manager *DuplicateManager) getValues(
	ctx context.Context,
	decoder *kv.TableKVDecoder,
	handles pendingIndexHandles,
) pendingIndexHandles {
	var finalErr error
	logger := log.With(
		zap.String("table", decoder.Name()),
		zap.Int("handlesCount", handles.Len()),
	).Begin(zap.DebugLevel, "[detect-dupe] collect values from TiKV")
	defer func() {
		logger.End(zap.ErrorLevel, finalErr)
	}()

	// TODO: paginate the handles.
	snapshot := manager.tikvCli.GetSnapshot(math.MaxUint64)
	batchGetMap, err := snapshot.BatchGet(ctx, handles.rawHandles)
	if err != nil {
		finalErr = err
		return handles
	}

	retryHandles := makePendingIndexHandlesWithCapacity(0)
	batch := makePendingIndexHandlesWithCapacity(handles.Len())
	rawRows := make([][]byte, 0, handles.Len())
	for i, rawHandle := range handles.rawHandles {
		rawValue, ok := batchGetMap[string(rawHandle)]
		if ok {
			logger.Debug("[detect-dupe] retrieved value from TiKV",
				logutil.Key("rawHandle", rawHandle),
				logutil.Key("row", rawValue))
			rawRows = append(rawRows, rawValue)
			handles.dataConflictInfos[i].Row = decoder.DecodeRawRowDataAsStr(handles.handles[i], rawValue)
			batch.appendAt(&handles, i)
		} else {
			logger.Warn("[detect-dupe] missing value from TiKV, will retry",
				logutil.Key("rawHandle", rawHandle))
			retryHandles.appendAt(&handles, i)
		}
	}

	finalErr = manager.errorMgr.RecordIndexConflictError(
		ctx, log.L(),
		decoder.Name(),
		batch.indexNames,
		batch.dataConflictInfos,
		batch.rawHandles,
		rawRows)
	if finalErr != nil {
		return handles
	}

	return retryHandles
}

func (manager *DuplicateManager) getDuplicateStream(ctx context.Context,
	region *restore.RegionInfo,
	start []byte, end []byte) (import_sstpb.ImportSST_DuplicateDetectClient, error) {
	leader := region.Leader
	if leader == nil {
		leader = region.Region.GetPeers()[0]
	}

	cli, err := manager.getImportClient(ctx, leader)
	if err != nil {
		return nil, err
	}

	reqCtx := &kvrpcpb.Context{
		RegionId:    region.Region.GetId(),
		RegionEpoch: region.Region.GetRegionEpoch(),
		Peer:        leader,
	}
	req := &import_sstpb.DuplicateDetectRequest{
		Context:  reqCtx,
		StartKey: start,
		EndKey:   end,
		KeyOnly:  false,
	}
	stream, err := cli.DuplicateDetect(ctx, req)
	return stream, err
}

func (manager *DuplicateManager) getImportClient(ctx context.Context, peer *metapb.Peer) (import_sstpb.ImportSSTClient, error) {
	conn, err := manager.connPool.GetGrpcConn(ctx, peer.GetStoreId(), 1, func(ctx context.Context) (*grpc.ClientConn, error) {
		return manager.makeConn(ctx, peer.GetStoreId())
	})
	if err != nil {
		return nil, err
	}
	return import_sstpb.NewImportSSTClient(conn), nil
}

func (manager *DuplicateManager) makeConn(ctx context.Context, storeID uint64) (*grpc.ClientConn, error) {
	store, err := manager.splitCli.GetStore(ctx, storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	opt := grpc.WithInsecure()
	if manager.tls.TLSConfig() != nil {
		opt = grpc.WithTransportCredentials(credentials.NewTLS(manager.tls.TLSConfig()))
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

func buildDuplicateRequests(tableInfo *model.TableInfo) ([]*DuplicateRequest, error) {
	var reqs []*DuplicateRequest
	for _, id := range physicalTableIDs(tableInfo) {
		tableReqs, err := buildTableRequests(id, tableInfo.IsCommonHandle)
		if err != nil {
			return nil, errors.Trace(err)
		}
		reqs = append(reqs, tableReqs...)
		for _, indexInfo := range tableInfo.Indices {
			if indexInfo.State != model.StatePublic {
				continue
			}
			indexReqs, err := buildIndexRequests(id, indexInfo)
			if err != nil {
				return nil, errors.Trace(err)
			}
			reqs = append(reqs, indexReqs...)
		}
	}
	return reqs, nil
}

func buildTableRequests(tableID int64, isCommonHandle bool) ([]*DuplicateRequest, error) {
	ranges := ranger.FullIntRange(false)
	if isCommonHandle {
		ranges = ranger.FullRange()
	}
	keysRanges, err := distsql.TableHandleRangesToKVRanges(nil, []int64{tableID}, isCommonHandle, ranges, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	reqs := make([]*DuplicateRequest, 0)
	for _, r := range keysRanges {
		req := &DuplicateRequest{
			start:     r.StartKey,
			end:       r.EndKey,
			tableID:   tableID,
			indexInfo: nil,
		}
		reqs = append(reqs, req)
	}
	return reqs, nil
}

func buildIndexRequests(tableID int64, indexInfo *model.IndexInfo) ([]*DuplicateRequest, error) {
	ranges := ranger.FullRange()
	keysRanges, err := distsql.IndexRangesToKVRanges(nil, tableID, indexInfo.ID, ranges, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	reqs := make([]*DuplicateRequest, 0)
	for _, r := range keysRanges {
		req := &DuplicateRequest{
			start:     r.StartKey,
			end:       r.EndKey,
			tableID:   tableID,
			indexInfo: indexInfo,
		}
		reqs = append(reqs, req)
	}
	return reqs, nil
}
