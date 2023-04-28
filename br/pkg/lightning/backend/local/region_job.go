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
	"container/heap"
	"context"
	"strings"
	"sync"
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
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/mathutil"
	"go.uber.org/zap"
)

type jobStageTp string

/*
	          +
	          v
	   +------+------+
	+->+regionScanned+<------+
	|  +------+------+       |
	|         |              |
	|         |              |
	|         v              |
	|      +--+--+     +-----+----+
	|      |wrote+---->+needRescan|
	|      +--+--+     +-----+----+
	|         |              ^
	|         |              |
	|         v              |
	|     +---+----+         |
	+-----+ingested+---------+
	      +---+----+
	          |
	          v

above diagram shows the state transition of a region job, here are some special
cases:
  - regionScanned can directly jump to ingested if the keyRange has no data
  - regionScanned can only transit to wrote. TODO: check if it should be transited
    to needRescan
  - if a job only partially writes the data, after it becomes ingested, it will
    update its keyRange and transits to regionScanned to continue the remaining
    data
  - needRescan may output multiple regionScanned jobs when the old region is split
*/
const (
	regionScanned jobStageTp = "regionScanned"
	wrote         jobStageTp = "wrote"
	ingested      jobStageTp = "ingested"
	needRescan    jobStageTp = "needRescan"
)

func (j jobStageTp) String() string {
	return string(j)
}

// regionJob is dedicated to import the data in [keyRange.start, keyRange.end)
// to a region. The keyRange may be changed when processing because of writing
// partial data to TiKV or region split.
type regionJob struct {
	keyRange Range
	// TODO: check the keyRange so that it's always included in region
	region *split.RegionInfo
	// stage should be updated only by convertStageTo
	stage jobStageTp
	// writeResult is available only in wrote and ingested stage
	writeResult *tikvWriteResult

	engine          *Engine
	regionSplitSize int64
	regionSplitKeys int64
	metrics         *metric.Metrics

	retryCount       int
	waitUntil        time.Time
	lastRetryableErr error

	// injected is used in test to set the behaviour
	injected []injectedBehaviour
}

type tikvWriteResult struct {
	sstMeta           []*sst.SSTMeta
	count             int64
	totalBytes        int64
	remainingStartKey []byte
}

type injectedBehaviour struct {
	write  injectedWriteBehaviour
	ingest injectedIngestBehaviour
}

type injectedWriteBehaviour struct {
	result *tikvWriteResult
	err    error
}

type injectedIngestBehaviour struct {
	nextStage jobStageTp
	err       error
}

func (j *regionJob) convertStageTo(stage jobStageTp) {
	j.stage = stage
	switch stage {
	case regionScanned:
		j.writeResult = nil
	case ingested:
		// when writing is skipped because key range is empty
		if j.writeResult == nil {
			return
		}

		j.engine.importedKVSize.Add(j.writeResult.totalBytes)
		j.engine.importedKVCount.Add(j.writeResult.count)
		if j.metrics != nil {
			j.metrics.BytesCounter.WithLabelValues(metric.StateImported).
				Add(float64(j.writeResult.totalBytes))
		}
	case needRescan:
		j.region = nil
	}
}

// writeToTiKV writes the data to TiKV and mark this job as wrote stage.
// if any write logic has error, writeToTiKV will set job to a proper stage and return nil. TODO: <-check this
// if any underlying logic has error, writeToTiKV will return an error.
// we don't need to do cleanup for the pairs written to tikv if encounters an error,
// tikv will take the responsibility to do so.
// TODO: let client-go provide a high-level write interface.
func (local *Backend) writeToTiKV(ctx context.Context, j *regionJob) error {
	if j.stage != regionScanned {
		return nil
	}

	failpoint.Inject("fakeRegionJobs", func() {
		front := j.injected[0]
		j.injected = j.injected[1:]
		j.writeResult = front.write.result
		err := front.write.err
		if err == nil {
			j.convertStageTo(wrote)
		}
		failpoint.Return(err)
	})

	apiVersion := local.tikvCodec.GetAPIVersion()
	clientFactory := local.importClientFactory
	kvBatchSize := local.KVWriteBatchSize
	bufferPool := local.bufferPool
	writeLimiter := local.writeLimiter

	begin := time.Now()
	region := j.region.Region

	firstKey, lastKey, err := j.engine.getFirstAndLastKey(j.keyRange.start, j.keyRange.end)
	if err != nil {
		return errors.Trace(err)
	}
	if firstKey == nil {
		j.convertStageTo(ingested)
		log.FromContext(ctx).Debug("keys within region is empty, skip doIngest",
			logutil.Key("start", j.keyRange.start),
			logutil.Key("regionStart", region.StartKey),
			logutil.Key("end", j.keyRange.end),
			logutil.Key("regionEnd", region.EndKey))
		return nil
	}

	firstKey = codec.EncodeBytes([]byte{}, firstKey)
	lastKey = codec.EncodeBytes([]byte{}, lastKey)

	u := uuid.New()
	meta := &sst.SSTMeta{
		Uuid:        u[:],
		RegionId:    region.GetId(),
		RegionEpoch: region.GetRegionEpoch(),
		Range: &sst.Range{
			Start: firstKey,
			End:   lastKey,
		},
		ApiVersion: apiVersion,
	}

	annotateErr := func(in error, peer *metapb.Peer) error {
		// annotate the error with peer/store/region info to help debug.
		return errors.Annotatef(in, "peer %d, store %d, region %d, epoch %s", peer.Id, peer.StoreId, region.Id, region.RegionEpoch.String())
	}

	leaderID := j.region.Leader.GetId()
	clients := make([]sst.ImportSST_WriteClient, 0, len(region.GetPeers()))
	allPeers := make([]*metapb.Peer, 0, len(region.GetPeers()))
	requests := make([]*sst.WriteRequest, 0, len(region.GetPeers()))
	for _, peer := range region.GetPeers() {
		cli, err := clientFactory.Create(ctx, peer.StoreId)
		if err != nil {
			return annotateErr(err, peer)
		}

		wstream, err := cli.Write(ctx)
		if err != nil {
			return annotateErr(err, peer)
		}

		// Bind uuid for this write request
		req := &sst.WriteRequest{
			Chunk: &sst.WriteRequest_Meta{
				Meta: meta,
			},
		}
		if err = wstream.Send(req); err != nil {
			return annotateErr(err, peer)
		}
		req.Chunk = &sst.WriteRequest_Batch{
			Batch: &sst.WriteBatch{
				CommitTs: j.engine.TS,
			},
		}
		clients = append(clients, wstream)
		requests = append(requests, req)
		allPeers = append(allPeers, peer)
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
			if err := writeLimiter.WaitN(ctx, allPeers[i].StoreId, int(size)); err != nil {
				return errors.Trace(err)
			}
			requests[i].Chunk.(*sst.WriteRequest_Batch).Batch.Pairs = pairs[:count]
			if err := clients[i].Send(requests[i]); err != nil {
				return annotateErr(err, allPeers[i])
			}
		}
		return nil
	}

	opt := &pebble.IterOptions{LowerBound: j.keyRange.start, UpperBound: j.keyRange.end}
	iter := j.engine.newKVIter(ctx, opt)
	//nolint: errcheck
	defer iter.Close()

	var remainingStartKey []byte
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
			if iter.Next() {
				remainingStartKey = append([]byte{}, iter.Key()...)
				log.FromContext(ctx).Info("write to tikv partial finish",
					zap.Int64("count", totalCount),
					zap.Int64("size", totalSize),
					logutil.Key("startKey", j.keyRange.start),
					logutil.Key("endKey", j.keyRange.end),
					logutil.Key("remainStart", remainingStartKey),
					logutil.Region(region),
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
			return annotateErr(closeErr, allPeers[i])
		}
		if resp.Error != nil {
			return annotateErr(errors.New(resp.Error.Message), allPeers[i])
		}
		if leaderID == region.Peers[i].GetId() {
			leaderPeerMetas = resp.Metas
			log.FromContext(ctx).Debug("get metas after write kv stream to tikv", zap.Reflect("metas", leaderPeerMetas))
		}
	}

	// if there is not leader currently, we don't forward the stage to wrote and let caller
	// handle the retry.
	if len(leaderPeerMetas) == 0 {
		log.FromContext(ctx).Warn("write to tikv no leader",
			logutil.Region(region), logutil.Leader(j.region.Leader),
			zap.Uint64("leader_id", leaderID), logutil.SSTMeta(meta),
			zap.Int64("kv_pairs", totalCount), zap.Int64("total_bytes", totalSize))
		return errors.Errorf("write to tikv with no leader returned, region '%d', leader: %d",
			region.Id, leaderID)
	}

	takeTime := time.Since(begin)
	log.FromContext(ctx).Debug("write to kv", zap.Reflect("region", j.region), zap.Uint64("leader", leaderID),
		zap.Reflect("meta", meta), zap.Reflect("return metas", leaderPeerMetas),
		zap.Int64("kv_pairs", totalCount), zap.Int64("total_bytes", totalSize),
		zap.Int64("buf_size", bytesBuf.TotalSize()),
		zap.Stringer("takeTime", takeTime))
	if m, ok := metric.FromContext(ctx); ok {
		m.SSTSecondsHistogram.WithLabelValues(metric.SSTProcessWrite).Observe(takeTime.Seconds())
	}

	j.writeResult = &tikvWriteResult{
		sstMeta:           leaderPeerMetas,
		count:             totalCount,
		totalBytes:        totalSize,
		remainingStartKey: remainingStartKey,
	}
	j.convertStageTo(wrote)
	return nil
}

// ingest tries to finish the regionJob.
// if any ingest logic has error, ingest may retry sometimes to resolve it and finally
// set job to a proper stage with nil error returned.
// if any underlying logic has error, ingest will return an error to let caller
// handle it.
func (local *Backend) ingest(ctx context.Context, j *regionJob) (err error) {
	if j.stage != wrote {
		return nil
	}

	failpoint.Inject("fakeRegionJobs", func() {
		front := j.injected[0]
		j.injected = j.injected[1:]
		j.convertStageTo(front.ingest.nextStage)
		failpoint.Return(front.ingest.err)
	})

	if len(j.writeResult.sstMeta) == 0 {
		j.convertStageTo(ingested)
		return nil
	}

	if m, ok := metric.FromContext(ctx); ok {
		begin := time.Now()
		defer func() {
			if err == nil {
				m.SSTSecondsHistogram.WithLabelValues(metric.SSTProcessIngest).Observe(time.Since(begin).Seconds())
			}
		}()
	}

	for retry := 0; retry < maxRetryTimes; retry++ {
		resp, err := local.doIngest(ctx, j)
		if err == nil && resp.GetError() == nil {
			j.convertStageTo(ingested)
			return nil
		}
		if err != nil {
			if common.IsContextCanceledError(err) {
				return err
			}
			log.FromContext(ctx).Warn("meet underlying error, will retry ingest",
				log.ShortError(err), logutil.SSTMetas(j.writeResult.sstMeta),
				logutil.Region(j.region.Region), logutil.Leader(j.region.Leader))
			continue
		}
		canContinue, err := j.convertStageOnIngestError(resp)
		if common.IsContextCanceledError(err) {
			return err
		}
		if !canContinue {
			log.FromContext(ctx).Warn("meet error and handle the job later",
				zap.Stringer("job stage", j.stage),
				logutil.ShortError(j.lastRetryableErr),
				j.region.ToZapFields(),
				logutil.Key("start", j.keyRange.start),
				logutil.Key("end", j.keyRange.end))
			return nil
		}
		log.FromContext(ctx).Warn("meet error and will doIngest region again",
			logutil.ShortError(j.lastRetryableErr),
			j.region.ToZapFields(),
			logutil.Key("start", j.keyRange.start),
			logutil.Key("end", j.keyRange.end))
	}
	return nil
}

func (local *Backend) checkWriteStall(
	ctx context.Context,
	region *split.RegionInfo,
) (bool, *sst.IngestResponse, error) {
	clientFactory := local.importClientFactory
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

// doIngest send ingest commands to TiKV based on regionJob.writeResult.sstMeta.
// When meet error, it will remove finished sstMetas before return.
func (local *Backend) doIngest(ctx context.Context, j *regionJob) (*sst.IngestResponse, error) {
	clientFactory := local.importClientFactory
	supportMultiIngest := local.supportMultiIngest
	shouldCheckWriteStall := local.ShouldCheckWriteStall
	if shouldCheckWriteStall {
		writeStall, resp, err := local.checkWriteStall(ctx, j.region)
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
	for start := 0; start < len(j.writeResult.sstMeta); start += batch {
		end := mathutil.Min(start+batch, len(j.writeResult.sstMeta))
		ingestMetas := j.writeResult.sstMeta[start:end]

		log.FromContext(ctx).Debug("ingest meta", zap.Reflect("meta", ingestMetas))

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
		if resp.GetError() != nil || err != nil {
			// remove finished sstMetas
			j.writeResult.sstMeta = j.writeResult.sstMeta[start:]
			return resp, errors.Trace(err)
		}
	}
	return resp, nil
}

// convertStageOnIngestError will try to fix the error contained in ingest response.
// Return (_, error) when another error occurred.
// Return (true, nil) when the job can retry ingesting immediately.
// Return (false, nil) when the job should be put back to queue.
func (j *regionJob) convertStageOnIngestError(
	resp *sst.IngestResponse,
) (bool, error) {
	if resp.GetError() == nil {
		return true, nil
	}

	var newRegion *split.RegionInfo
	switch errPb := resp.GetError(); {
	case errPb.NotLeader != nil:
		j.lastRetryableErr = common.ErrKVNotLeader.GenWithStack(errPb.GetMessage())

		// meet a problem that the region leader+peer are all updated but the return
		// error is only "NotLeader", we should update the whole region info.
		j.convertStageTo(needRescan)
		return false, nil
	case errPb.EpochNotMatch != nil:
		j.lastRetryableErr = common.ErrKVEpochNotMatch.GenWithStack(errPb.GetMessage())

		if currentRegions := errPb.GetEpochNotMatch().GetCurrentRegions(); currentRegions != nil {
			var currentRegion *metapb.Region
			for _, r := range currentRegions {
				if insideRegion(r, j.writeResult.sstMeta) {
					currentRegion = r
					break
				}
			}
			if currentRegion != nil {
				var newLeader *metapb.Peer
				for _, p := range currentRegion.Peers {
					if p.GetStoreId() == j.region.Leader.GetStoreId() {
						newLeader = p
						break
					}
				}
				if newLeader != nil {
					newRegion = &split.RegionInfo{
						Leader: newLeader,
						Region: currentRegion,
					}
				}
			}
		}
		if newRegion != nil {
			j.region = newRegion
			j.convertStageTo(regionScanned)
			return false, nil
		}
		j.convertStageTo(needRescan)
		return false, nil
	case strings.Contains(errPb.Message, "raft: proposal dropped"):
		j.lastRetryableErr = common.ErrKVRaftProposalDropped.GenWithStack(errPb.GetMessage())

		j.convertStageTo(needRescan)
		return false, nil
	case errPb.ServerIsBusy != nil:
		j.lastRetryableErr = common.ErrKVServerIsBusy.GenWithStack(errPb.GetMessage())

		return false, nil
	case errPb.RegionNotFound != nil:
		j.lastRetryableErr = common.ErrKVRegionNotFound.GenWithStack(errPb.GetMessage())

		j.convertStageTo(needRescan)
		return false, nil
	case errPb.ReadIndexNotReady != nil:
		j.lastRetryableErr = common.ErrKVReadIndexNotReady.GenWithStack(errPb.GetMessage())

		// this error happens when this region is splitting, the error might be:
		//   read index not ready, reason can not read index due to split, region 64037
		// we have paused schedule, but it's temporary,
		// if next request takes a long time, there's chance schedule is enabled again
		// or on key range border, another engine sharing this region tries to split this
		// region may cause this error too.
		j.convertStageTo(needRescan)
		return false, nil
	case errPb.DiskFull != nil:
		j.lastRetryableErr = common.ErrKVIngestFailed.GenWithStack(errPb.GetMessage())

		return false, errors.Errorf("non-retryable error: %s", resp.GetError().GetMessage())
	}
	// all others doIngest error, such as stale command, etc. we'll retry it again from writeAndIngestByRange
	j.lastRetryableErr = common.ErrKVIngestFailed.GenWithStack(resp.GetError().GetMessage())
	j.convertStageTo(regionScanned)
	return false, nil
}

type regionJobRetryHeap []*regionJob

var _ heap.Interface = (*regionJobRetryHeap)(nil)

func (h *regionJobRetryHeap) Len() int {
	return len(*h)
}

func (h *regionJobRetryHeap) Less(i, j int) bool {
	v := *h
	return v[i].waitUntil.Before(v[j].waitUntil)
}

func (h *regionJobRetryHeap) Swap(i, j int) {
	v := *h
	v[i], v[j] = v[j], v[i]
}

func (h *regionJobRetryHeap) Push(x any) {
	*h = append(*h, x.(*regionJob))
}

func (h *regionJobRetryHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// regionJobRetryer is a concurrent-safe queue holding jobs that need to put
// back later, and put back when the regionJob.waitUntil is reached. It maintains
// a heap of jobs internally based on the regionJob.waitUntil field.
type regionJobRetryer struct {
	// lock acquiring order: protectedClosed > protectedQueue > protectedToPutBack
	protectedClosed struct {
		mu     sync.Mutex
		closed bool
	}
	protectedQueue struct {
		mu sync.Mutex
		q  regionJobRetryHeap
	}
	protectedToPutBack struct {
		mu        sync.Mutex
		toPutBack *regionJob
	}
	putBackCh chan<- *regionJob
	reload    chan struct{}
	jobWg     *sync.WaitGroup
}

// startRegionJobRetryer starts a new regionJobRetryer and it will run in
// background to put the job back to `putBackCh` when job's waitUntil is reached.
// Cancel the `ctx` will stop retryer and `jobWg.Done` will be trigger for jobs
// that are not put back yet.
func startRegionJobRetryer(
	ctx context.Context,
	putBackCh chan<- *regionJob,
	jobWg *sync.WaitGroup,
) *regionJobRetryer {
	ret := &regionJobRetryer{
		putBackCh: putBackCh,
		reload:    make(chan struct{}, 1),
		jobWg:     jobWg,
	}
	ret.protectedQueue.q = make(regionJobRetryHeap, 0, 16)
	go ret.run(ctx)
	return ret
}

// run is only internally used, caller should not use it.
func (q *regionJobRetryer) run(ctx context.Context) {
	defer q.close()

	for {
		var front *regionJob
		q.protectedQueue.mu.Lock()
		if len(q.protectedQueue.q) > 0 {
			front = q.protectedQueue.q[0]
		}
		q.protectedQueue.mu.Unlock()

		switch {
		case front != nil:
			select {
			case <-ctx.Done():
				return
			case <-q.reload:
			case <-time.After(time.Until(front.waitUntil)):
				q.protectedQueue.mu.Lock()
				q.protectedToPutBack.mu.Lock()
				q.protectedToPutBack.toPutBack = heap.Pop(&q.protectedQueue.q).(*regionJob)
				// release the lock of queue to avoid blocking regionJobRetryer.push
				q.protectedQueue.mu.Unlock()

				// hold the lock of toPutBack to make sending to putBackCh and
				// resetting toPutBack atomic w.r.t. regionJobRetryer.close
				select {
				case <-ctx.Done():
					q.protectedToPutBack.mu.Unlock()
					return
				case q.putBackCh <- q.protectedToPutBack.toPutBack:
					q.protectedToPutBack.toPutBack = nil
					q.protectedToPutBack.mu.Unlock()
				}
			}
		default:
			// len(q.q) == 0
			select {
			case <-ctx.Done():
				return
			case <-q.reload:
			}
		}
	}
}

// close is only internally used, caller should not use it.
func (q *regionJobRetryer) close() {
	q.protectedClosed.mu.Lock()
	defer q.protectedClosed.mu.Unlock()
	q.protectedClosed.closed = true

	count := len(q.protectedQueue.q)
	if q.protectedToPutBack.toPutBack != nil {
		count++
	}
	for count > 0 {
		q.jobWg.Done()
		count--
	}
}

// push should not be blocked for long time in any cases.
func (q *regionJobRetryer) push(job *regionJob) bool {
	q.protectedClosed.mu.Lock()
	defer q.protectedClosed.mu.Unlock()
	if q.protectedClosed.closed {
		return false
	}

	q.protectedQueue.mu.Lock()
	heap.Push(&q.protectedQueue.q, job)
	q.protectedQueue.mu.Unlock()

	select {
	case q.reload <- struct{}{}:
	default:
	}
	return true
}
