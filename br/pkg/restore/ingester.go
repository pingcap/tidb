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

package restore

import (
	"bytes"
	"context"
	"crypto/tls"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/errorpb"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/conn"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/kv"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/tikv/pd/pkg/codec"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

const (
	dialTimeout = 5 * time.Second

	gRPCKeepAliveTime    = 10 * time.Second
	gRPCKeepAliveTimeout = 3 * time.Second

	// See: https://github.com/tikv/tikv/blob/e030a0aae9622f3774df89c62f21b2171a72a69e/etc/config-template.toml#L360
	regionMaxKeyCount = 1440000

	defaultSplitSize = 96 * 1024 * 1024
)

type retryType int

const (
	retryNone retryType = iota
	retryWrite
	retryIngest
)

type gRPCConns struct {
	mu             sync.Mutex
	conns          map[uint64]*conn.Pool
	tcpConcurrency int
}

func (conns *gRPCConns) Close() {
	conns.mu.Lock()
	defer conns.mu.Unlock()

	for _, cp := range conns.conns {
		cp.Close()
	}
}

// Ingester writes and ingests kv to TiKV.
// which used for both BR log restore and Lightning local backend.
type Ingester struct {
	// commit ts appends to key in tikv
	TS uint64

	tlsConf *tls.Config
	conns   gRPCConns

	splitCli   SplitClient
	WorkerPool *utils.WorkerPool

	batchWriteKVPairs int
	regionSplitSize   int64
}

// NewIngester creates Ingester.
func NewIngester(
	splitCli SplitClient, cfg concurrencyCfg, commitTS uint64, tlsConf *tls.Config,
) *Ingester {
	workerPool := utils.NewWorkerPool(cfg.IngestConcurrency, "ingest worker")
	return &Ingester{
		tlsConf: tlsConf,
		conns: gRPCConns{
			tcpConcurrency: cfg.TCPConcurrency,
			conns:          make(map[uint64]*conn.Pool),
		},
		splitCli:          splitCli,
		WorkerPool:        workerPool,
		batchWriteKVPairs: cfg.BatchWriteKVPairs,
		regionSplitSize:   defaultSplitSize,
		TS:                commitTS,
	}
}

func (i *Ingester) makeConn(ctx context.Context, storeID uint64) (*grpc.ClientConn, error) {
	store, err := i.splitCli.GetStore(ctx, storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	opt := grpc.WithInsecure()
	if i.tlsConf != nil {
		opt = grpc.WithTransportCredentials(credentials.NewTLS(i.tlsConf))
	}
	ctx, cancel := context.WithTimeout(ctx, dialTimeout)

	bfConf := backoff.DefaultConfig
	bfConf.MaxDelay = gRPCBackOffMaxDelay
	// we should use peer address for tiflash. for tikv, peer address is empty
	addr := store.GetPeerAddress()
	if addr == "" {
		addr = store.GetAddress()
	}
	grpcConn, err := grpc.DialContext(
		ctx,
		addr,
		opt,
		grpc.WithBlock(),
		grpc.FailOnNonTempDialError(true),
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
	return grpcConn, nil
}

// write [start, end) kv in to tikv.
func (i *Ingester) writeAndIngestByRange(
	ctxt context.Context,
	iterProducer kv.IterProducer,
	start []byte,
	end []byte,
	remainRanges *syncdRanges,
) error {
	select {
	case <-ctxt.Done():
		return ctxt.Err()
	default:
	}
	iter := iterProducer.Produce(start, end)
	iter.First()
	pairStart := append([]byte{}, iter.Key()...)
	iter.Last()
	pairEnd := append([]byte{}, iter.Key()...)
	if bytes.Compare(pairStart, pairEnd) > 0 {
		log.Debug("There is no pairs in iterator", logutil.Key("start", start),
			logutil.Key("end", end), logutil.Key("pairStart", pairStart), logutil.Key("pairEnd", pairEnd))
		return nil
	}
	var regions []*RegionInfo
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
		startKey := codec.EncodeBytes(pairStart)
		endKey := codec.EncodeBytes(kv.NextKey(pairEnd))
		regions, err = PaginateScanRegion(ctx, i.splitCli, startKey, endKey, 128)
		if err != nil || len(regions) == 0 {
			log.Warn("scan region failed", zap.Error(err), zap.Int("region_len", len(regions)),
				logutil.Key("startKey", startKey), logutil.Key("endKey", endKey), zap.Int("retry", retry))
			retry++
			continue
		}

		for _, region := range regions {
			log.Debug("get region", zap.Int("retry", retry), logutil.Key("startKey", startKey),
				logutil.Key("endKey", endKey), logutil.Region(region.Region))
			w := i.WorkerPool.ApplyWorker()
			var rg *Range
			rg, err = i.writeAndIngestPairs(ctx, iter, region, pairStart, pairEnd)
			i.WorkerPool.RecycleWorker(w)
			if err != nil {
				_, regionStart, _ := codec.DecodeBytes(region.Region.StartKey)
				// if we have at least succeeded one region, retry without increasing the retry count
				if bytes.Compare(regionStart, pairStart) > 0 {
					pairStart = regionStart
				} else {
					retry++
				}
				log.Info("retry write and ingest kv pairs", logutil.Key("startKey", pairStart),
					logutil.Key("endKey", pairEnd), zap.Error(err), zap.Int("retry", retry))
				continue WriteAndIngest
			}
			if rg != nil {
				remainRanges.add(*rg)
			}
		}
		break
	}
	return err
}

func (i *Ingester) writeAndIngestPairs(
	ctx context.Context,
	iter kv.Iter,
	region *RegionInfo,
	start, end []byte,
) (*Range, error) {
	var err error
	var remainRange *Range
loopWrite:
	for retry := 0; retry < maxRetryTimes; retry++ {
		select {
		case <-ctx.Done():
			return remainRange, ctx.Err()
		default:
		}
		var metas []*sst.SSTMeta
		metas, remainRange, err = i.writeToTiKV(ctx, iter, region, start, end)
		if err != nil {
			log.Warn("write to tikv failed", zap.Error(err))
			return nil, err
		}

		for _, meta := range metas {
			errCnt := 0
			for errCnt < maxRetryTimes {
				log.Debug("ingest meta", zap.Reflect("meta", meta))
				var resp *sst.IngestResponse
				resp, err = i.ingest(ctx, meta, region)
				if err != nil {
					log.Warn("ingest failed", zap.Error(err), logutil.SSTMeta(meta),
						zap.Reflect("region", region))
					errCnt++
					continue
				}
				failpoint.Inject("FailIngestMeta", func(val failpoint.Value) {
					switch val.(string) {
					case "notleader":
						resp.Error.NotLeader = &errorpb.NotLeader{
							RegionId: region.Region.Id, Leader: region.Leader,
						}
					case "epochnotmatch":
						resp.Error.EpochNotMatch = &errorpb.EpochNotMatch{
							CurrentRegions: []*metapb.Region{region.Region},
						}
					}
				})
				var retryTy retryType
				var newRegion *RegionInfo
				retryTy, newRegion, err = i.isIngestRetryable(ctx, resp, region, meta)
				if err == nil {
					// ingest next meta
					break
				}
				switch retryTy {
				case retryNone:
					log.Warn("ingest failed and do not retry", zap.Error(err), logutil.SSTMeta(meta),
						zap.Reflect("region", region))
					// met non-retryable error retry whole Write procedure
					return remainRange, err
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
			log.Warn("write and ingest region, will retry import full range", zap.Error(err),
				logutil.Region(region.Region), logutil.Key("start", start), logutil.Key("end", end))
		}
		return remainRange, errors.Trace(err)
	}

	return remainRange, errors.Trace(err)
}

// writeToTiKV writer engine key-value pairs to tikv and return the sst meta generated by tikv.
// we don't need to do cleanup for the pairs written to tikv if encounters an error,
// tikv will takes the responsibility to do so.
func (i *Ingester) writeToTiKV(
	ctx context.Context,
	iter kv.Iter,
	region *RegionInfo,
	start, end []byte,
) ([]*sst.SSTMeta, *Range, error) {
	begin := time.Now()
	regionRange := intersectRange(region.Region, Range{Start: start, End: end})

	iter.Seek(regionRange.Start)
	firstKey := codec.EncodeBytes(iter.Key())
	var lastKey []byte
	if iter.Seek(regionRange.End) {
		lastKey = codec.EncodeBytes(iter.Key())
	} else {
		iter.Last()
		log.Info("region range's end key not in iter, shouldn't happen",
			zap.Any("region range", regionRange), logutil.Key("iter last", iter.Key()))
		lastKey = codec.EncodeBytes(kv.NextKey(iter.Key()))
	}

	if bytes.Compare(firstKey, lastKey) > 0 {
		log.Info("keys within region is empty, skip ingest", logutil.Key("start", start),
			logutil.Key("regionStart", region.Region.StartKey), logutil.Key("end", end),
			logutil.Key("regionEnd", region.Region.EndKey))
		return nil, nil, nil
	}

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
		cli, err := i.getImportClient(ctx, peer)
		if err != nil {
			return nil, nil, err
		}

		wstream, err := cli.Write(ctx)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}

		// Bind uuid for this write request
		req := &sst.WriteRequest{
			Chunk: &sst.WriteRequest_Meta{
				Meta: meta,
			},
		}
		if err = wstream.Send(req); err != nil {
			return nil, nil, errors.Trace(err)
		}
		req.Chunk = &sst.WriteRequest_Batch{
			Batch: &sst.WriteBatch{
				CommitTs: i.TS,
			},
		}
		clients = append(clients, wstream)
		requests = append(requests, req)
	}

	bytesBuf := membuf.NewBuffer()
	defer bytesBuf.Destroy()
	pairs := make([]*sst.Pair, 0, i.batchWriteKVPairs)
	count := 0
	size := int64(0)
	totalCount := 0
	firstLoop := true
	regionMaxSize := i.regionSplitSize * 4 / 3

	for iter.Seek(regionRange.Start); iter.Valid() && bytes.Compare(iter.Key(), regionRange.End) <= 0; iter.Next() {
		size += int64(len(iter.Key()) + len(iter.Value()))
		// here we reuse the `*sst.Pair`s to optimize object allocation
		if firstLoop {
			pair := &sst.Pair{
				Key:   bytesBuf.AddBytes(iter.Key()),
				Value: bytesBuf.AddBytes(iter.Value()),
				Op:    iter.OpType(),
			}
			pairs = append(pairs, pair)
		} else {
			pairs[count].Key = bytesBuf.AddBytes(iter.Key())
			pairs[count].Value = bytesBuf.AddBytes(iter.Value())
		}
		count++
		totalCount++

		if count >= i.batchWriteKVPairs {
			for i := range clients {
				requests[i].Chunk.(*sst.WriteRequest_Batch).Batch.Pairs = pairs[:count]
				if err := clients[i].Send(requests[i]); err != nil {
					return nil, nil, errors.Trace(err)
				}
			}
			count = 0
			bytesBuf.Reset()
			firstLoop = false
		}
		if size >= regionMaxSize || totalCount >= regionMaxKeyCount {
			break
		}
	}

	if count > 0 {
		for i := range clients {
			requests[i].Chunk.(*sst.WriteRequest_Batch).Batch.Pairs = pairs[:count]
			if err := clients[i].Send(requests[i]); err != nil {
				return nil, nil, errors.Trace(err)
			}
		}
	}

	if iter.Error() != nil {
		return nil, nil, errors.Trace(iter.Error())
	}

	var leaderPeerMetas []*sst.SSTMeta
	for i, wStream := range clients {
		if resp, closeErr := wStream.CloseAndRecv(); closeErr != nil {
			return nil, nil, errors.Trace(closeErr)
		} else if leaderID == region.Region.Peers[i].GetId() {
			leaderPeerMetas = resp.Metas
			log.Debug("get metas after write kv stream to tikv", zap.Reflect("metas", leaderPeerMetas))
		}
	}

	// if there is not leader currently, we should directly return an error
	if leaderPeerMetas == nil {
		log.Warn("write to tikv no leader", zap.Reflect("region", region),
			zap.Uint64("leader_id", leaderID), zap.Reflect("meta", meta),
			zap.Int("kv_pairs", totalCount), zap.Int64("total_bytes", size))
		return nil, nil, errors.Annotatef(berrors.ErrPDLeaderNotFound, "write to tikv with no leader returned, region '%d', leader: %d",
			region.Region.Id, leaderID)
	}

	log.Debug("write to kv", zap.Reflect("region", region), zap.Uint64("leader", leaderID),
		zap.Reflect("meta", meta), zap.Reflect("return metas", leaderPeerMetas),
		zap.Int("kv_pairs", totalCount), zap.Int64("total_bytes", size),
		zap.Int64("buf_size", bytesBuf.TotalSize()),
		zap.Stringer("takeTime", time.Since(begin)))

	var remainRange *Range
	if iter.Valid() && iter.Next() {
		firstKey := append([]byte{}, iter.Key()...)
		remainRange = &Range{Start: firstKey, End: regionRange.End}
		log.Info("write to tikv partial finish", zap.Int("count", totalCount),
			zap.Int64("size", size), zap.Binary("startKey", regionRange.Start), zap.Binary("endKey", regionRange.End),
			zap.Binary("remainStart", remainRange.Start), zap.Binary("remainEnd", remainRange.End),
			zap.Reflect("region", region))
	}

	return leaderPeerMetas, remainRange, nil
}

func (i *Ingester) ingest(ctx context.Context, meta *sst.SSTMeta, region *RegionInfo) (*sst.IngestResponse, error) {
	leader := region.Leader
	if leader == nil {
		leader = region.Region.GetPeers()[0]
	}

	cli, err := i.getImportClient(ctx, leader)
	if err != nil {
		return nil, err
	}
	reqCtx := &kvrpcpb.Context{
		RegionId:    region.Region.GetId(),
		RegionEpoch: region.Region.GetRegionEpoch(),
		Peer:        leader,
	}

	req := &sst.IngestRequest{
		Context: reqCtx,
		Sst:     meta,
	}
	resp, err := cli.Ingest(ctx, req)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

func (i *Ingester) getImportClient(ctx context.Context, peer *metapb.Peer) (sst.ImportSSTClient, error) {
	i.conns.mu.Lock()
	defer i.conns.mu.Unlock()

	conn, err := i.getGrpcConnLocked(ctx, peer.GetStoreId())
	if err != nil {
		return nil, errors.Trace(err)
	}
	return sst.NewImportSSTClient(conn), nil
}

func (i *Ingester) getGrpcConnLocked(ctx context.Context, storeID uint64) (*grpc.ClientConn, error) {
	if _, ok := i.conns.conns[storeID]; !ok {
		i.conns.conns[storeID] = conn.NewConnPool(i.conns.tcpConcurrency, func(ctx context.Context) (*grpc.ClientConn, error) {
			return i.makeConn(ctx, storeID)
		})
	}
	return i.conns.conns[storeID].Get(ctx)
}

func (i *Ingester) isIngestRetryable(
	ctx context.Context,
	resp *sst.IngestResponse,
	region *RegionInfo,
	meta *sst.SSTMeta,
) (retryType, *RegionInfo, error) {
	if resp.GetError() == nil {
		return retryNone, nil, nil
	}

	getRegion := func() (*RegionInfo, error) {
		for retry := 0; ; retry++ {
			newRegion, err := i.splitCli.GetRegion(ctx, region.Region.GetStartKey())
			if err != nil {
				return nil, errors.Trace(err)
			}
			if newRegion != nil {
				return newRegion, nil
			}
			log.Warn("get region by key return nil, will retry", zap.Reflect("region", region),
				zap.Int("retry", retry))
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Second):
			}
		}
	}

	var newRegion *RegionInfo
	var err error
	switch errPb := resp.GetError(); {
	case errPb.NotLeader != nil:
		if newLeader := errPb.GetNotLeader().GetLeader(); newLeader != nil {
			newRegion = &RegionInfo{
				Leader: newLeader,
				Region: region.Region,
			}
		} else {
			newRegion, err = getRegion()
			if err != nil {
				return retryNone, nil, errors.Trace(err)
			}
		}
		return retryIngest, newRegion, errors.Annotatef(berrors.ErrKVNotLeader, "not leader: %s", errPb.GetMessage())
	case errPb.EpochNotMatch != nil:
		if currentRegions := errPb.GetEpochNotMatch().GetCurrentRegions(); currentRegions != nil {
			var currentRegion *metapb.Region
			for _, r := range currentRegions {
				if insideRegion(r, meta) {
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
					newRegion = &RegionInfo{
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
		return retryTy, newRegion, errors.Annotatef(berrors.ErrKVEpochNotMatch, "epoch not match: %s", errPb.GetMessage())
	case strings.Contains(errPb.Message, "raft: proposal dropped"):
		// TODO: we should change 'Raft raft: proposal dropped' to a error type like 'NotLeader'
		newRegion, err = getRegion()
		if err != nil {
			return retryNone, nil, errors.Trace(err)
		}
		return retryIngest, newRegion, errors.Annotate(berrors.ErrKVUnknown, errPb.GetMessage())
	}
	return retryNone, nil, errors.Annotatef(berrors.ErrKVUnknown, "non-retryable error: %s", resp.GetError().GetMessage())
}
