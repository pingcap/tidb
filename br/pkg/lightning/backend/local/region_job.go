// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package local

import (
	"context"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/errorpb"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/metric"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/mathutil"
	"go.uber.org/zap"
)

const (
	regionScanned = iota
	wrote
	ingested
)

// regionJob is dedicated to import the data in [keyRange.start, keyRange.end) to a region.
type regionJob struct {
	keyRange Range
	region   *split.RegionInfo
	// TODO: add a type for it. And disallow direct write to it.
	stage int // regionScanned -> wrote -> ingested (this stage will not be put to the queue)

	engine          *Engine
	regionSplitSize int64
	regionSplitKeys int64
	// below fields are available after wrote stage
	writeResult *tikvWriteResult
}

func (j *regionJob) convertStageTo(stage int) {
	j.stage = stage
	if stage == ingested {
		j.engine.finishedRanges.add(j.keyRange)
	}
}

// writeToTiKV writes the data to TiKV and mark this job as wrote stage.
// TODO: let client-go provide a high-level write interface.
func (j *regionJob) writeToTiKV(
	ctx context.Context,
	clientFactory ImportClientFactory,
	kvBatchSize int,
	bufferPool *membuf.Pool,
	writeLimiter StoreWriteLimiter,
) error {
	if j.stage != regionScanned {
		return nil
	}

	begin := time.Now()
	stats := rangeStats{}

	firstKey, lastKey, err := j.engine.getFirstAndLastKey(j.keyRange.start, j.keyRange.end)
	if err != nil {
		return errors.Trace(err)
	}
	if firstKey == nil {
		j.convertStageTo(ingested)
		log.FromContext(ctx).Info("keys within region is empty, skip doIngest",
			logutil.Key("start", j.keyRange.start),
			logutil.Key("regionStart", j.region.Region.StartKey),
			logutil.Key("end", j.keyRange.end),
			logutil.Key("regionEnd", j.region.Region.EndKey))
		return nil
	}

	firstKey = codec.EncodeBytes([]byte{}, firstKey)
	lastKey = codec.EncodeBytes([]byte{}, lastKey)

	u := uuid.New()
	meta := &sst.SSTMeta{
		Uuid:        u[:],
		RegionId:    j.region.Region.GetId(),
		RegionEpoch: j.region.Region.GetRegionEpoch(),
		Range: &sst.Range{
			Start: firstKey,
			End:   lastKey,
		},
	}

	leaderID := j.region.Leader.GetId()
	clients := make([]sst.ImportSST_WriteClient, 0, len(j.region.Region.GetPeers()))
	storeIDs := make([]uint64, 0, len(j.region.Region.GetPeers()))
	requests := make([]*sst.WriteRequest, 0, len(j.region.Region.GetPeers()))
	for _, peer := range j.region.Region.GetPeers() {
		cli, err := clientFactory.Create(ctx, peer.StoreId)
		if err != nil {
			return errors.Trace(err)
		}

		wstream, err := cli.Write(ctx)
		if err != nil {
			return errors.Trace(err)
		}

		// Bind uuid for this write request
		req := &sst.WriteRequest{
			Chunk: &sst.WriteRequest_Meta{
				Meta: meta,
			},
		}
		if err = wstream.Send(req); err != nil {
			return errors.Trace(err)
		}
		req.Chunk = &sst.WriteRequest_Batch{
			Batch: &sst.WriteBatch{
				CommitTs: j.engine.TS,
			},
		}
		clients = append(clients, wstream)
		requests = append(requests, req)
		storeIDs = append(storeIDs, peer.StoreId)
	}

	bytesBuf := bufferPool.NewBuffer()
	defer bytesBuf.Destroy()
	pairs := make([]*sst.Pair, 0, kvBatchSize)
	count := 0
	size := int64(0)
	totalSize := int64(0)
	totalCount := int64(0)
	// if region-split-size <= 96MiB, we bump the threshold a bit to avoid too many retry split
	// because the range-properties is not 100% accurate
	regionMaxSize := j.regionSplitSize
	if j.regionSplitSize <= int64(config.SplitRegionSize) {
		regionMaxSize = j.regionSplitSize * 4 / 3
	}
	// Set a lower flush limit to make the speed of write more smooth.
	flushLimit := int64(writeLimiter.Limit() / 10)

	flushKVs := func() error {
		for i := range clients {
			if err := writeLimiter.WaitN(ctx, storeIDs[i], int(size)); err != nil {
				return errors.Trace(err)
			}
			// TODO: concurrent write?
			requests[i].Chunk.(*sst.WriteRequest_Batch).Batch.Pairs = pairs[:count]
			if err := clients[i].Send(requests[i]); err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	}

	opt := &pebble.IterOptions{LowerBound: j.keyRange.start, UpperBound: j.keyRange.end}
	iter := j.engine.newKVIter(ctx, opt)
	//nolint: errcheck
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		kvSize := int64(len(iter.Key()) + len(iter.Value()))
		// here we reuse the `*sst.Pair`s to optimize object allocation
		if count < len(pairs) {
			pairs[count].Key = bytesBuf.AddBytes(iter.Key())
			pairs[count].Value = bytesBuf.AddBytes(iter.Value())
		} else {
			pair := &sst.Pair{
				Key:   bytesBuf.AddBytes(iter.Key()),
				Value: bytesBuf.AddBytes(iter.Value()),
			}
			pairs = append(pairs, pair)
		}
		count++
		totalCount++
		size += kvSize
		totalSize += kvSize

		if count >= kvBatchSize || size >= flushLimit {
			if err := flushKVs(); err != nil {
				return errors.Trace(err)
			}
			count = 0
			size = 0
			bytesBuf.Reset()
		}
		if totalSize >= regionMaxSize || totalCount >= j.regionSplitKeys {
			// we will shrink the key range of this job to real written range
			if iter.Valid() && iter.Next() {
				firstKey := append([]byte{}, iter.Key()...)
				oldEndKey := j.keyRange.end
				j.keyRange.end = firstKey
				log.FromContext(ctx).Info("write to tikv partial finish",
					zap.Int64("count", totalCount),
					zap.Int64("size", size),
					logutil.Key("startKey", j.keyRange.start),
					logutil.Key("endKey", oldEndKey),
					logutil.Key("remainStart", firstKey),
					logutil.Key("remainEnd", oldEndKey),
					logutil.Region(j.region.Region),
					logutil.Leader(j.region.Leader))
			}
			break
		}
	}

	if iter.Error() != nil {
		return errors.Trace(iter.Error())
	}

	if count > 0 {
		if err := flushKVs(); err != nil {
			return errors.Trace(err)
		}
		count = 0
		size = 0
		bytesBuf.Reset()
	}

	var leaderPeerMetas []*sst.SSTMeta
	for i, wStream := range clients {
		resp, closeErr := wStream.CloseAndRecv()
		if closeErr != nil {
			return errors.Trace(closeErr)
		}
		if resp.Error != nil {
			return errors.New(resp.Error.Message)
		}
		if leaderID == j.region.Region.Peers[i].GetId() {
			leaderPeerMetas = resp.Metas
			log.FromContext(ctx).Debug("get metas after write kv stream to tikv", zap.Reflect("metas", leaderPeerMetas))
		}
	}

	// if there is not leader currently, we should directly return an error
	if len(leaderPeerMetas) == 0 {
		log.FromContext(ctx).Warn("write to tikv no leader",
			logutil.Region(j.region.Region), logutil.Leader(j.region.Leader),
			zap.Uint64("leader_id", leaderID), logutil.SSTMeta(meta),
			zap.Int64("kv_pairs", totalCount), zap.Int64("total_bytes", size))
		return errors.Errorf("write to tikv with no leader returned, region '%d', leader: %d",
			j.region.Region.Id, leaderID)
	}

	log.FromContext(ctx).Debug("write to kv", zap.Reflect("region", j.region), zap.Uint64("leader", leaderID),
		zap.Reflect("meta", meta), zap.Reflect("return metas", leaderPeerMetas),
		zap.Int64("kv_pairs", totalCount), zap.Int64("total_bytes", size),
		zap.Int64("buf_size", bytesBuf.TotalSize()),
		zap.Stringer("takeTime", time.Since(begin)))

	stats.count = totalCount
	stats.totalBytes = totalSize
	j.writeResult = &tikvWriteResult{
		sstMeta:    leaderPeerMetas,
		rangeStats: stats,
	}
	j.convertStageTo(wrote)
	return nil
}

func (j *regionJob) checkWriteStall(
	ctx context.Context,
	region *split.RegionInfo,
	clientFactory ImportClientFactory,
) (bool, *sst.IngestResponse, error) {
	for _, peer := range region.Region.GetPeers() {
		cli, err := clientFactory.Create(ctx, peer.StoreId)
		if err != nil {
			return false, nil, errors.Trace(err)
		}
		// currently we use empty MultiIngestRequest to check if TiKV is busy.
		// If in future the rate limit feature contains more metrics we can switch to use it.
		resp, err := cli.MultiIngest(ctx, &sst.MultiIngestRequest{})
		if err != nil {
			return false, nil, errors.Trace(err)
		}
		if resp.Error != nil && resp.Error.ServerIsBusy != nil {
			return true, resp, nil
		}
	}
	return false, nil, nil
}

// ingest tries to finish the regionJob, it will retry internally for some error
// or revert the job stage to an earlier stage.
func (j *regionJob) ingest(
	ctx context.Context,
	clientFactory ImportClientFactory,
	supportMultiIngest bool,
	checkWriteStall bool,
) error {
	switch j.stage {
	case ingested:
		return nil
	case wrote:
	case regionScanned:
		panic("wrong job stage, should not happen")
	}

	resp, err := j.doIngest(ctx, clientFactory, supportMultiIngest, checkWriteStall)

	if err != nil {
		if common.IsContextCanceledError(err) {
			return err
		}
		log.FromContext(ctx).Warn("doIngest failed",
			log.ShortError(err), logutil.SSTMetas(j.writeResult.sstMeta),
			logutil.Region(j.region.Region), logutil.Leader(j.region.Leader))
		continue
	}

	var retryTy retryType
	var newRegion *split.RegionInfo
	retryTy, newRegion, err = local.isIngestRetryable(ctx, resp, region, ingestMetas)
	if common.IsContextCanceledError(err) {
		return err
	}
	if err == nil {
		// doIngest next meta
		break
	}

	switch retryTy {
	case retryNone:
		log.FromContext(ctx).Warn("doIngest failed noretry", log.ShortError(err), logutil.SSTMetas(ingestMetas),
			logutil.Region(region.Region), logutil.Leader(region.Leader))
		// met non-retryable error retry whole Write procedure
		return err
	case retryWrite:
		region = newRegion
		continue loopWrite
	case retryIngest:
		region = newRegion
		continue
	case retryBusyIngest:
		log.FromContext(ctx).Warn("meet tikv busy when doIngest", log.ShortError(err), logutil.SSTMetas(ingestMetas),
			logutil.Region(region.Region))
		// ImportEngine will continue on this unfinished range
		return nil
	}
}
}

if err == nil {
engine.importedKVSize.Add(rangeStats.totalBytes)
engine.importedKVCount.Add(rangeStats.count)
engine.finishedRanges.add(finishedRange)
if local.metrics != nil {
local.metrics.BytesCounter.WithLabelValues(metric.BytesStateImported).Add(float64(rangeStats.totalBytes))
}
return nil
}

log.FromContext(ctx).Warn("write and doIngest region, will retry import full range", log.ShortError(err),
logutil.Region(region.Region), logutil.Key("start", start),
logutil.Key("end", end))
return errors.Trace(err)
}

// doIngest send ingest commands to TiKV based on regionJob.writeResult.sstMeta.
// When meet error, it will remove finished sstMetas before return.
func (j *regionJob) doIngest(
	ctx context.Context,
	clientFactory ImportClientFactory,
	supportMultiIngest bool,
	checkWriteStall bool,
) (*sst.IngestResponse, error) {
	if checkWriteStall {
		writeStall, resp, err := j.checkWriteStall(ctx, j.region, clientFactory)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if writeStall {
			return resp, nil
		}
	}

	batch := 1
	if supportMultiIngest {
		batch = len(j.writeResult.sstMeta)
	}

	var resp *sst.IngestResponse
	for i := 0; i < len(j.writeResult.sstMeta); i += batch {
		start := i * batch
		end := mathutil.Min((i+1)*batch, len(j.writeResult.sstMeta))
		ingestMetas := j.writeResult.sstMeta[start:end]

		log.FromContext(ctx).Debug("doIngest meta", zap.Reflect("meta", ingestMetas))

		failpoint.Inject("FailIngestMeta", func(val failpoint.Value) {
			// only inject the error once
			var resp *sst.IngestResponse

			switch val.(string) {
			case "notleader":
				resp = &sst.IngestResponse{
					Error: &errorpb.Error{
						NotLeader: &errorpb.NotLeader{
							RegionId: j.region.Region.Id,
							Leader:   j.region.Leader,
						},
					},
				}
			case "epochnotmatch":
				resp = &sst.IngestResponse{
					Error: &errorpb.Error{
						EpochNotMatch: &errorpb.EpochNotMatch{
							CurrentRegions: []*metapb.Region{j.region.Region},
						},
					},
				}
			}
			failpoint.Return(resp, nil)
		})

		leader := j.region.Leader
		if leader == nil {
			leader = j.region.Region.GetPeers()[0]
		}

		cli, err := clientFactory.Create(ctx, leader.StoreId)
		if err != nil {
			return nil, errors.Trace(err)
		}
		reqCtx := &kvrpcpb.Context{
			RegionId:    j.region.Region.GetId(),
			RegionEpoch: j.region.Region.GetRegionEpoch(),
			Peer:        leader,
		}

		if supportMultiIngest {
			req := &sst.MultiIngestRequest{
				Context: reqCtx,
				Ssts:    ingestMetas,
			}
			resp, err = cli.MultiIngest(ctx, req)
		} else {
			req := &sst.IngestRequest{
				Context: reqCtx,
				Sst:     ingestMetas[0],
			}
			resp, err = cli.Ingest(ctx, req)
		}
		if resp.Error != nil || err != nil {
			// remove finished sstMetas
			// TODO: it's very dangerous to send same ingest twice, carefully check it.
			j.writeResult.sstMeta = j.writeResult.sstMeta[start:]
			return resp, errors.Trace(err)
		}
	}
	return resp, nil
}
