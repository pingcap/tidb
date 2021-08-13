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
// See the License for the specific language governing permissions and
// limitations under the License.

package local

import (
	"bytes"
	"context"
	"io"
	"sort"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/errormanager"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/distsql"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/ranger"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

const (
	maxWriteBatchCount    = 128
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
	regionConcurrency int
	connPool          common.GRPCConns
	tls               *common.TLS
	ts                uint64
	keyAdapter        KeyAdapter
}

type pendingIndexHandles struct {
	// all 4 slices should have exactly the same length.
	dataConflictInfos []errormanager.DataConflictInfo
	indexNames        []string
	handles           []tidbkv.Handle
	rawHandles        [][]byte
}

func makePendingIndexHandlesWithCapacity(cap int) pendingIndexHandles {
	return pendingIndexHandles{
		dataConflictInfos: make([]errormanager.DataConflictInfo, 0, cap),
		indexNames:        make([]string, 0, cap),
		handles:           make([]tidbkv.Handle, 0, cap),
		rawHandles:        make([][]byte, 0, cap),
	}
}

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

func (indexHandles *pendingIndexHandles) extend(other *pendingIndexHandles) {
	indexHandles.dataConflictInfos = append(indexHandles.dataConflictInfos, other.dataConflictInfos...)
	indexHandles.indexNames = append(indexHandles.indexNames, other.indexNames...)
	indexHandles.handles = append(indexHandles.handles, other.handles...)
	indexHandles.rawHandles = append(indexHandles.rawHandles, other.rawHandles...)
}

func (indexHandles *pendingIndexHandles) truncate() {
	indexHandles.dataConflictInfos = indexHandles.dataConflictInfos[:0]
	indexHandles.indexNames = indexHandles.indexNames[:0]
	indexHandles.handles = indexHandles.handles[:0]
	indexHandles.rawHandles = indexHandles.rawHandles[:0]
}

func (indexHandles *pendingIndexHandles) Len() int {
	return len(indexHandles.rawHandles)
}

func (indexHandles *pendingIndexHandles) Less(i, j int) bool {
	return bytes.Compare(indexHandles.rawHandles[i], indexHandles.rawHandles[j]) < 0
}

func (indexHandles *pendingIndexHandles) Swap(i, j int) {
	indexHandles.handles[i], indexHandles.handles[j] = indexHandles.handles[j], indexHandles.handles[i]
	indexHandles.indexNames[i], indexHandles.indexNames[j] = indexHandles.indexNames[j], indexHandles.indexNames[i]
	indexHandles.dataConflictInfos[i], indexHandles.dataConflictInfos[j] = indexHandles.dataConflictInfos[j], indexHandles.dataConflictInfos[i]
	indexHandles.rawHandles[i], indexHandles.rawHandles[j] = indexHandles.rawHandles[j], indexHandles.rawHandles[i]
}

func NewDuplicateManager(
	errorMgr *errormanager.ErrorManager,
	splitCli restore.SplitClient,
	ts uint64,
	tls *common.TLS,
	regionConcurrency int) (*DuplicateManager, error) {
	return &DuplicateManager{
		errorMgr:          errorMgr,
		tls:               tls,
		regionConcurrency: regionConcurrency,
		splitCli:          splitCli,
		keyAdapter:        duplicateKeyAdapter{},
		ts:                ts,
		connPool:          common.NewGRPCConns(),
	}, nil
}

func (manager *DuplicateManager) CollectDuplicateRowsFromTiKV(ctx context.Context, tbl table.Table) error {
	log.L().Info("Begin collect duplicate data from remote TiKV")
	reqs, err := buildDuplicateRequests(tbl.Meta())
	if err != nil {
		return err
	}

	decoder, err := kv.NewTableKVDecoder(tbl, &kv.SessionOptions{
		SQLMode: mysql.ModeStrictAllTables,
	})
	if err != nil {
		return err
	}
	g, rpcctx := errgroup.WithContext(ctx)
	for _, r := range reqs {
		req := r
		g.Go(func() error {
			err := manager.sendRequestToTiKV(rpcctx, decoder, req)
			if err != nil {
				log.L().Error("error occur when collect duplicate data from TiKV", zap.Error(err))
			}
			return err
		})
	}
	err = g.Wait()
	log.L().Info("End collect duplicate data from remote TiKV")
	return err
}

func (manager *DuplicateManager) sendRequestToTiKV(ctx context.Context,
	decoder *kv.TableKVDecoder,
	req *DuplicateRequest) error {
	startKey := codec.EncodeBytes([]byte{}, req.start)
	endKey := codec.EncodeBytes([]byte{}, req.end)

	regions, err := paginateScanRegion(ctx, manager.splitCli, startKey, endKey, scanRegionLimit)
	if err != nil {
		return err
	}
	tryTimes := 0
	indexHandles := makePendingIndexHandlesWithCapacity(0)
	for {
		if len(regions) == 0 {
			break
		}
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
			for {
				resp, reqErr := cli.Recv()
				hasErr := false
				if reqErr != nil {
					if errors.Cause(reqErr) == io.EOF {
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
					log.L().Warn("meet error when recving duplicate detect response from TiKV, retry again",
						logutil.Region(region.Region), logutil.Leader(region.Leader), zap.Error(reqErr))
					break
				}
				if resp.GetKeyError() != nil {
					log.L().Warn("meet key error in duplicate detect response from TiKV, retry again ",
						logutil.Region(region.Region), logutil.Leader(region.Leader),
						zap.String("KeyError", resp.GetKeyError().GetMessage()))
					break
				}

				if resp.GetRegionError() != nil {
					log.L().Warn("meet key error in duplicate detect response from TiKV, retry again ",
						logutil.Region(region.Region), logutil.Leader(region.Leader),
						zap.String("RegionError", resp.GetRegionError().GetMessage()))

					r, err := paginateScanRegion(ctx, manager.splitCli, watingRegions[idx].Region.GetStartKey(), watingRegions[idx].Region.GetEndKey(), scanRegionLimit)
					if err != nil {
						unfinishedRegions = append(unfinishedRegions, watingRegions[idx])
					} else {
						unfinishedRegions = append(unfinishedRegions, r...)
					}
					break
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

		// it means that all of region send to TiKV fail, so we must sleep some time to avoid retry too frequency
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
	for _, kv := range resp.Pairs {
		logger := log.With(
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

		conflictInfo := errormanager.DataConflictInfo{
			RawKey:   kv.Key,
			RawValue: kv.Value,
			KeyData:  h.String(),
		}

		if req.indexInfo != nil {
			indexHandles.append(
				conflictInfo,
				req.indexInfo.Name.O,
				h, decoder.EncodeHandleKey(h))
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

// Collect rows by read the index in db.
func (manager *DuplicateManager) CollectDuplicateRowsFromLocalIndex(
	ctx context.Context,
	tbl table.Table,
	db *pebble.DB,
) error {
	decoder, err := kv.NewTableKVDecoder(tbl, &kv.SessionOptions{
		SQLMode: mysql.ModeStrictAllTables,
	})
	if err != nil {
		return err
	}
	handles := makePendingIndexHandlesWithCapacity(0)
	allRanges := make([]tidbkv.KeyRange, 0)
	for _, indexInfo := range tbl.Meta().Indices {
		if indexInfo.State != model.StatePublic {
			continue
		}
		ranges := ranger.FullRange()
		keysRanges, err := distsql.IndexRangesToKVRanges(nil, tbl.Meta().ID, indexInfo.ID, ranges, nil)
		if err != nil {
			return err
		}
		allRanges = append(allRanges, keysRanges...)
		for _, r := range keysRanges {
			startKey := codec.EncodeBytes([]byte{}, r.StartKey)
			endKey := codec.EncodeBytes([]byte{}, r.EndKey)
			opts := &pebble.IterOptions{
				LowerBound: startKey,
				UpperBound: endKey,
			}
			log.L().Warn("collect index from db",
				logutil.Key("start", startKey),
				logutil.Key("end", endKey),
			)

			iter := db.NewIter(opts)
			for iter.SeekGE(startKey); iter.Valid(); iter.Next() {
				rawKey, _, _, err := manager.keyAdapter.Decode(nil, iter.Key())
				if err != nil {
					log.L().Warn(
						"decode key error when query handle for duplicate index",
						zap.Binary("key", iter.Key()),
					)
					continue
				}
				value := iter.Value()
				h, err := decoder.DecodeHandleFromIndex(indexInfo, rawKey, value)
				if err != nil {
					log.L().Error("decode handle error from index for duplicatedb",
						zap.Error(err), logutil.Key("rawKey", rawKey),
						logutil.Key("value", value))
					continue
				}
				handles.append(
					errormanager.DataConflictInfo{
						RawKey:   rawKey,
						RawValue: value,
						KeyData:  h.String(),
					},
					indexInfo.Name.O,
					h,
					decoder.EncodeHandleKey(h))
				if handles.Len() > maxGetRequestKeyCount {
					handles = manager.getValues(ctx, decoder, handles)
				}
			}
			if handles.Len() > 0 {
				handles = manager.getValues(ctx, decoder, handles)
			}
			if handles.Len() == 0 {
				db.DeleteRange(r.StartKey, r.EndKey, &pebble.WriteOptions{Sync: false})
			}
			iter.Close()
		}
	}
	if handles.Len() == 0 {
		return nil
	}

	for i := 0; i < maxRetryTimes; i++ {
		handles = manager.getValues(ctx, decoder, handles)
		if handles.Len() == 0 {
			for _, r := range allRanges {
				db.DeleteRange(r.StartKey, r.EndKey, &pebble.WriteOptions{Sync: false})
			}
		}
	}
	return errors.Errorf("retry getValues time exceed limit")
}

func (manager *DuplicateManager) getValues(
	ctx context.Context,
	decoder *kv.TableKVDecoder,
	handles pendingIndexHandles,
) pendingIndexHandles {
	sort.Sort(&handles)

	l := handles.Len()
	startKey := codec.EncodeBytes([]byte{}, handles.rawHandles[0])
	endKey := codec.EncodeBytes([]byte{}, nextKey(handles.rawHandles[l-1]))
	regions, err := paginateScanRegion(ctx, manager.splitCli, startKey, endKey, scanRegionLimit)
	if err != nil {
		log.L().Error("scan regions errors", zap.Error(err))
		return handles
	}
	startIdx := 0
	endIdx := 0
	retryHandles := makePendingIndexHandlesWithCapacity(0)
	batch := makePendingIndexHandlesWithCapacity(0)
	for _, region := range regions {
		if startIdx >= l {
			break
		}
		handleKey := codec.EncodeBytes([]byte{}, handles.rawHandles[startIdx])
		if bytes.Compare(handleKey, region.Region.EndKey) >= 0 {
			// TODO shouldn't we use `sort.Search` for these 🤔
			continue
		}
		endIdx = startIdx
		for endIdx < l {
			handleKey := codec.EncodeBytes([]byte{}, handles.rawHandles[endIdx])
			if bytes.Compare(handleKey, region.Region.EndKey) < 0 {
				batch.appendAt(&handles, endIdx)
				endIdx++
			} else {
				break
			}
		}
		if err := manager.getValuesFromRegion(ctx, region, decoder, batch); err != nil {
			log.L().Error("failed to collect values from TiKV by handle, we will retry it again", zap.Error(err))
			retryHandles.extend(&batch)
		}
		startIdx = endIdx
	}
	return retryHandles
}

func (manager *DuplicateManager) getValuesFromRegion(
	ctx context.Context,
	region *restore.RegionInfo,
	decoder *kv.TableKVDecoder,
	handles pendingIndexHandles,
) error {
	kvclient, err := manager.getKvClient(ctx, region.Leader)
	if err != nil {
		return err
	}
	reqCtx := &kvrpcpb.Context{
		RegionId:    region.Region.GetId(),
		RegionEpoch: region.Region.GetRegionEpoch(),
		Peer:        region.Leader,
	}

	req := &kvrpcpb.BatchGetRequest{
		Context: reqCtx,
		Keys:    handles.rawHandles,
		Version: manager.ts,
	}
	resp, err := kvclient.KvBatchGet(ctx, req)
	if err != nil {
		return err
	}
	if resp.GetRegionError() != nil {
		return errors.Errorf("region error because of %v", resp.GetRegionError().GetMessage())
	}
	if resp.Error != nil {
		return errors.Errorf("key error")
	}

	log.L().Error("get keys", zap.Int("key size", len(resp.Pairs)))

	rawRows := make([][]byte, 0, len(resp.Pairs))
	for i, kv := range resp.Pairs {
		rawRows = append(rawRows, kv.Value)
		handles.dataConflictInfos[i].Row = decoder.DecodeRawRowDataAsStr(handles.handles[i], kv.Value)
	}

	return manager.errorMgr.RecordIndexConflictError(
		ctx, log.L(),
		decoder.Name(),
		handles.indexNames,
		handles.dataConflictInfos,
		handles.rawHandles,
		rawRows,
	)
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

func (manager *DuplicateManager) getKvClient(ctx context.Context, peer *metapb.Peer) (tikvpb.TikvClient, error) {
	conn, err := manager.connPool.GetGrpcConn(ctx, peer.GetStoreId(), 1, func(ctx context.Context) (*grpc.ClientConn, error) {
		return manager.makeConn(ctx, peer.GetStoreId())
	})
	if err != nil {
		return nil, err
	}
	return tikvpb.NewTikvClient(conn), nil
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
	reqs := make([]*DuplicateRequest, 0)
	req := buildTableRequest(tableInfo.ID)
	reqs = append(reqs, req...)
	for _, indexInfo := range tableInfo.Indices {
		if indexInfo.State != model.StatePublic {
			continue
		}
		req, err := buildIndexRequest(tableInfo.ID, indexInfo)
		if err != nil {
			return nil, err
		}
		reqs = append(reqs, req...)
	}
	return reqs, nil
}

func buildTableRequest(tableID int64) []*DuplicateRequest {
	ranges := ranger.FullIntRange(false)
	keysRanges := distsql.TableRangesToKVRanges(tableID, ranges, nil)
	reqs := make([]*DuplicateRequest, 0)
	for _, r := range keysRanges {
		r := &DuplicateRequest{
			start:     r.StartKey,
			end:       r.EndKey,
			tableID:   tableID,
			indexInfo: nil,
		}
		reqs = append(reqs, r)
	}
	return reqs
}

func buildIndexRequest(tableID int64, indexInfo *model.IndexInfo) ([]*DuplicateRequest, error) {
	ranges := ranger.FullRange()
	keysRanges, err := distsql.IndexRangesToKVRanges(nil, tableID, indexInfo.ID, ranges, nil)
	if err != nil {
		return nil, err
	}
	reqs := make([]*DuplicateRequest, 0)
	for _, r := range keysRanges {
		r := &DuplicateRequest{
			start:     r.StartKey,
			end:       r.EndKey,
			tableID:   tableID,
			indexInfo: indexInfo,
		}
		reqs = append(reqs, r)
	}
	return reqs, nil
}
