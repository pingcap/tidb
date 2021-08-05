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
	// TODO: Remote the member `db` and store the result in another place.
	db                *pebble.DB
	splitCli          restore.SplitClient
	regionConcurrency int
	connPool          common.GRPCConns
	tls               *common.TLS
	ts                uint64
	keyAdapter        KeyAdapter
}

func NewDuplicateManager(
	db *pebble.DB,
	splitCli restore.SplitClient,
	ts uint64,
	tls *common.TLS,
	regionConcurrency int) (*DuplicateManager, error) {
	return &DuplicateManager{
		db:                db,
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
	indexHandles := make([][]byte, 0)
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

		if len(indexHandles) > 0 {
			handles := manager.getValues(ctx, indexHandles)
			if len(handles) > 0 {
				indexHandles = handles
			} else {
				indexHandles = indexHandles[:0]
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
				if len(handles) > 0 {
					indexHandles = append(indexHandles, handles...)
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
) ([][]byte, error) {
	opts := &pebble.WriteOptions{Sync: false}
	var err error
	maxKeyLen := 0
	for _, kv := range resp.Pairs {
		l := manager.keyAdapter.EncodedLen(kv.Key)
		if l > maxKeyLen {
			maxKeyLen = l
		}
	}
	buf := make([]byte, maxKeyLen)
	for i := 0; i < maxRetryTimes; i++ {
		b := manager.db.NewBatch()
		handles := make([][]byte, 0)
		for _, kv := range resp.Pairs {
			if req.indexInfo != nil {
				h, err := decoder.DecodeHandleFromIndex(req.indexInfo, kv.Key, kv.Value)
				if err != nil {
					log.L().Error("decode handle error from index",
						zap.Error(err), logutil.Key("key", kv.Key),
						logutil.Key("value", kv.Value), zap.Uint64("commit-ts", kv.CommitTs))
					continue
				}
				key := decoder.EncodeHandleKey(h)
				handles = append(handles, key)
			} else {
				encodedKey := manager.keyAdapter.Encode(buf, kv.Key, 0, int64(kv.CommitTs))
				b.Set(encodedKey, kv.Value, opts)
			}
		}
		err = b.Commit(opts)
		if err != nil {
			continue
		}
		b.Close()
		if len(handles) == 0 {
			return handles, nil
		}
		return manager.getValues(ctx, handles), nil
	}
	return nil, err
}

func (manager *DuplicateManager) ReportDuplicateData() error {
	return nil
}

func (manager *DuplicateManager) RepairDuplicateData() error {
	// TODO
	return nil
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
	handles := make([][]byte, 0)
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
				key := decoder.EncodeHandleKey(h)
				handles = append(handles, key)
				if len(handles) > maxGetRequestKeyCount {
					handles = manager.getValues(ctx, handles)
				}
			}
			if len(handles) > 0 {
				handles = manager.getValues(ctx, handles)
			}
			if len(handles) == 0 {
				db.DeleteRange(r.StartKey, r.EndKey, &pebble.WriteOptions{Sync: false})
			}
			iter.Close()
		}
	}
	if len(handles) == 0 {
		return nil
	}

	for i := 0; i < maxRetryTimes; i++ {
		handles = manager.getValues(ctx, handles)
		if len(handles) == 0 {
			for _, r := range allRanges {
				db.DeleteRange(r.StartKey, r.EndKey, &pebble.WriteOptions{Sync: false})
			}
		}
	}
	return errors.Errorf("retry getValues time exceed limit")
}

func (manager *DuplicateManager) getValues(
	ctx context.Context,
	handles [][]byte,
) [][]byte {
	retryHandles := make([][]byte, 0)
	sort.Slice(handles, func(i, j int) bool {
		return bytes.Compare(handles[i], handles[j]) < 0
	})
	l := len(handles)
	startKey := codec.EncodeBytes([]byte{}, handles[0])
	endKey := codec.EncodeBytes([]byte{}, nextKey(handles[l-1]))
	regions, err := paginateScanRegion(ctx, manager.splitCli, startKey, endKey, scanRegionLimit)
	if err != nil {
		log.L().Error("scan regions errors", zap.Error(err))
		return handles
	}
	startIdx := 0
	endIdx := 0
	batch := make([][]byte, 0)
	for _, region := range regions {
		if startIdx >= l {
			break
		}
		handleKey := codec.EncodeBytes([]byte{}, handles[startIdx])
		if bytes.Compare(handleKey, region.Region.EndKey) >= 0 {
			continue
		}
		endIdx = startIdx
		for endIdx < l {
			handleKey := codec.EncodeBytes([]byte{}, handles[endIdx])
			if bytes.Compare(handleKey, region.Region.EndKey) < 0 {
				batch = append(batch, handles[endIdx])
				endIdx++
			} else {
				break
			}
		}
		if err := manager.getValuesFromRegion(ctx, region, batch); err != nil {
			log.L().Error("failed to collect values from TiKV by handle, we will retry it again", zap.Error(err))
			retryHandles = append(retryHandles, batch...)
		}
		startIdx = endIdx
	}
	return retryHandles
}

func (manager *DuplicateManager) getValuesFromRegion(
	ctx context.Context,
	region *restore.RegionInfo,
	handles [][]byte,
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
		Keys:    handles,
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

	maxKeyLen := 0
	for _, kv := range resp.Pairs {
		l := manager.keyAdapter.EncodedLen(kv.Key)
		if l > maxKeyLen {
			maxKeyLen = l
		}
	}
	buf := make([]byte, maxKeyLen)

	log.L().Error("get keys", zap.Int("key size", len(resp.Pairs)))
	for i := 0; i < maxRetryTimes; i++ {
		b := manager.db.NewBatch()
		opts := &pebble.WriteOptions{Sync: false}
		for _, kv := range resp.Pairs {
			encodedKey := manager.keyAdapter.Encode(buf, kv.Key, 0, 0)
			b.Set(encodedKey, kv.Value, opts)
			if b.Count() > maxWriteBatchCount {
				err = b.Commit(opts)
				if err != nil {
					break
				} else {
					b.Reset()
				}
			}
		}
		if err == nil {
			err = b.Commit(opts)
		}
		if err == nil {
			return nil
		}
	}
	return err
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
