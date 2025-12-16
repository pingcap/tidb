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
	"bytes"
	"container/heap"
	"context"
	goerrors "errors"
	"fmt"
	"io"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/errorpb"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ingestor/engineapi"
	"github.com/pingcap/tidb/pkg/ingestor/errdef"
	"github.com/pingcap/tidb/pkg/ingestor/ingestcli"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/metric"
	"github.com/pingcap/tidb/pkg/metrics"
	util2 "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/injectfailpoint"
	"github.com/pingcap/tidb/pkg/util/intest"
	tidblogutil "github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
	"google.golang.org/grpc"
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

	// suppose each KV is about 32 bytes, 16 * units.KiB / 32 = 512
	defaultKVBatchCount = 512
)

func (j jobStageTp) String() string {
	return string(j)
}

// regionJob is dedicated to import the data in [keyRange.start, keyRange.end)
// to a region. The keyRange may be changed when processing because of writing
// partial data to TiKV or region split.
type regionJob struct {
	keyRange engineapi.Range
	// TODO: check the keyRange so that it's always included in region
	region *split.RegionInfo
	// stage should be updated only by convertStageTo
	stage jobStageTp
	// writeResult is available only in wrote and ingested stage
	writeResult *tikvWriteResult

	ingestData      engineapi.IngestData
	regionSplitSize int64
	regionSplitKeys int64
	metrics         *metric.Common

	retryCount       int
	waitUntil        time.Time
	lastRetryableErr error

	// injected is used in test to set the behaviour
	injected []injectedBehaviour
}

// RecoverArgs implements workerpool.TaskMayPanic interface.
func (*regionJob) RecoverArgs() (metricsLabel string, funcInfo string, err error) {
	return "regionJob", "regionJob", nil
}

type tikvWriteResult struct {
	// means there is no data inside this job
	emptyJob          bool
	count             int64
	totalBytes        int64
	remainingStartKey []byte

	// below fields are for OP generation store engine.
	// this field might be modified in-place to remove SSTs that are ingested successfully.
	sstMeta []*sst.SSTMeta

	// below fields are for cloud generation store engine.
	// the filename of the written sst file by tikv-worker.
	nextGenWriteResp *ingestcli.WriteResponse
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
	err error
}

func newRegionJob(
	region *split.RegionInfo,
	data engineapi.IngestData,
	jobStart []byte,
	jobEnd []byte,
	regionSplitSize int64,
	regionSplitKeys int64,
	metrics *metric.Common,
) *regionJob {
	log.L().Debug("new region job",
		zap.Binary("jobStart", jobStart),
		zap.Binary("jobEnd", jobEnd),
		zap.Uint64("id", region.Region.GetId()),
		zap.Stringer("epoch", region.Region.GetRegionEpoch()),
		zap.Binary("regionStart", region.Region.GetStartKey()),
		zap.Binary("regionEnd", region.Region.GetEndKey()),
		zap.Reflect("peers", region.Region.GetPeers()))
	return &regionJob{
		keyRange:        engineapi.Range{Start: jobStart, End: jobEnd},
		region:          region,
		stage:           regionScanned,
		ingestData:      data,
		regionSplitSize: regionSplitSize,
		regionSplitKeys: regionSplitKeys,
		metrics:         metrics,
	}
}

// newRegionJobs creates a list of regionJob from the given regions and job
// ranges.
//
// pre-condition:
// - sortedRegions must be non-empty, sorted and continuous
// - sortedJobRanges must be non-empty, sorted and continuous
// - sortedRegions can cover sortedJobRanges
func newRegionJobs(
	sortedRegions []*split.RegionInfo,
	data engineapi.IngestData,
	sortedJobRanges []engineapi.Range,
	regionSplitSize int64,
	regionSplitKeys int64,
	metrics *metric.Common,
) []*regionJob {
	var (
		lenRegions   = len(sortedRegions)
		lenJobRanges = len(sortedJobRanges)
		ret          = make([]*regionJob, 0, max(lenRegions, lenJobRanges)*2)

		curRegionIdx   = 0
		curRegion      = sortedRegions[curRegionIdx].Region
		curRegionStart []byte
		curRegionEnd   []byte
	)

	_, curRegionStart, _ = codec.DecodeBytes(curRegion.StartKey, nil)
	_, curRegionEnd, _ = codec.DecodeBytes(curRegion.EndKey, nil)

	for _, jobRange := range sortedJobRanges {
		// build the job and move to next region for these cases:
		//
		// --region--)           or   -----region--)
		// -------job range--)        --job range--)
		for !beforeEnd(jobRange.End, curRegionEnd) {
			ret = append(ret, newRegionJob(
				sortedRegions[curRegionIdx],
				data,
				largerStartKey(jobRange.Start, curRegionStart),
				curRegionEnd,
				regionSplitSize,
				regionSplitKeys,
				metrics,
			))

			curRegionIdx++
			if curRegionIdx >= lenRegions {
				return ret
			}
			curRegion = sortedRegions[curRegionIdx].Region
			_, curRegionStart, _ = codec.DecodeBytes(curRegion.StartKey, nil)
			_, curRegionEnd, _ = codec.DecodeBytes(curRegion.EndKey, nil)
		}

		// now we can make sure
		//
		//               --region--)
		// --job range--)
		//
		// only need to handle the case that job range has remaining part after above loop:
		//
		//            [----region--)
		// --job range--)
		if bytes.Compare(curRegionStart, jobRange.End) < 0 {
			ret = append(ret, newRegionJob(
				sortedRegions[curRegionIdx],
				data,
				largerStartKey(jobRange.Start, curRegionStart),
				jobRange.End,
				regionSplitSize,
				regionSplitKeys,
				metrics,
			))
		}
	}

	return ret
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

		j.ingestData.Finish(j.writeResult.totalBytes, j.writeResult.count)
		if j.metrics != nil {
			j.metrics.BytesCounter.WithLabelValues(metric.StateImported).
				Add(float64(j.writeResult.totalBytes))
		}
	case needRescan:
		j.region = nil
	}
}

// ref means that the ingestData of job will be accessed soon.
func (j *regionJob) ref(wg *sync.WaitGroup) {
	if wg != nil {
		wg.Add(1)
	}
	if j.ingestData != nil {
		j.ingestData.IncRef()
	}
}

// done promises that the ingestData of job will not be accessed. Same amount of
// done should be called to release the ingestData.
func (j *regionJob) done(wg *sync.WaitGroup) {
	if j.ingestData != nil {
		j.ingestData.DecRef()
	}
	if wg != nil {
		wg.Done()
	}
}

func newWriteRequest(meta *sst.SSTMeta, resourceGroupName, taskType string) *sst.WriteRequest {
	return &sst.WriteRequest{
		Chunk: &sst.WriteRequest_Meta{
			Meta: meta,
		},
		Context: &kvrpcpb.Context{
			ResourceControlContext: &kvrpcpb.ResourceControlContext{
				ResourceGroupName: resourceGroupName,
			},
			RequestSource: util.BuildRequestSource(true, kv.InternalTxnLightning, taskType),
			TxnSource:     kv.LightningPhysicalImportTxnSource,
		},
	}
}

func (local *Backend) doWrite(ctx context.Context, j *regionJob) (ret *tikvWriteResult, err error) {
	failpoint.Inject("fakeRegionJobs", func() {
		front := j.injected[0]
		j.injected = j.injected[1:]
		err := front.write.err
		if err == nil {
			j.convertStageTo(wrote)
		}
		failpoint.Return(front.write.result, err)
	})

	var cancel context.CancelFunc
	// set a timeout for the write operation, if it takes too long, we will return with common.ErrWriteTooSlow and let caller retry the whole job instead of being stuck forever.
	timeout := 15 * time.Minute
	ctx, cancel = context.WithTimeoutCause(ctx, timeout, common.ErrWriteTooSlow)
	defer cancel()

	// A defer function to handle all DeadlineExceeded errors that may occur
	// during the write operation using this context with 15 minutes timeout.
	// When the error is "context deadline exceeded", we will check if the cause
	// is common.ErrWriteTooSlow and return the common.ErrWriteTooSlow instead so
	// our caller would be able to retry this doWrite operation. By doing this
	// defer we are hoping to handle all DeadlineExceeded error during this
	// write, either from gRPC stream or write limiter WaitN operation.
	wctx := ctx
	defer func() {
		if err == nil {
			return
		}
		if errors.Cause(err) == context.DeadlineExceeded {
			if cause := context.Cause(wctx); goerrors.Is(cause, common.ErrWriteTooSlow) {
				tidblogutil.Logger(ctx).Info("Experiencing a wait timeout while writing to tikv",
					zap.Int("store-write-bwlimit", local.BackendConfig.StoreWriteBWLimit),
					zap.Int("limit-size", local.writeLimiter.Limit()))
				err = errors.Trace(cause) // return the common.ErrWriteTooSlow instead to let caller retry it
			}
		}
	}()

	apiVersion := local.tikvCodec.GetAPIVersion()
	clientFactory := local.importClientFactory
	kvBatchSize := local.KVWriteBatchSize
	bufferPool := local.engineMgr.getBufferPool()
	writeLimiter := local.writeLimiter

	begin := time.Now()
	region := j.region.Region

	firstKey, lastKey, err := j.ingestData.GetFirstAndLastKey(j.keyRange.Start, j.keyRange.End)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if firstKey == nil {
		tidblogutil.Logger(ctx).Debug("keys within region is empty, skip doIngest",
			logutil.Key("start", j.keyRange.Start),
			logutil.Key("regionStart", region.StartKey),
			logutil.Key("end", j.keyRange.End),
			logutil.Key("regionEnd", region.EndKey))
		return &tikvWriteResult{emptyJob: true}, nil
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

	failpoint.Inject("changeEpochVersion", func(val failpoint.Value) {
		cloned := *meta.RegionEpoch
		meta.RegionEpoch = &cloned
		i := val.(int)
		if i >= 0 {
			meta.RegionEpoch.Version += uint64(i)
		} else {
			meta.RegionEpoch.ConfVer -= uint64(-i)
		}
	})

	annotateErr := func(in error, peer *metapb.Peer, msg string) error {
		// annotate the error with peer/store/region info to help debug.
		return errors.Annotatef(
			in,
			"peer %d, store %d, region %d, epoch %s, %s",
			peer.Id, peer.StoreId, region.Id, region.RegionEpoch.String(),
			msg,
		)
	}

	leaderID := j.region.Leader.GetId()
	clients := make([]sst.ImportSST_WriteClient, 0, len(region.GetPeers()))
	allPeers := make([]*metapb.Peer, 0, len(region.GetPeers()))
	req := newWriteRequest(meta, local.ResourceGroupName, local.TaskType)
	for _, peer := range region.GetPeers() {
		cli, err := clientFactory.create(ctx, peer.StoreId)
		if err != nil {
			return nil, annotateErr(err, peer, "when create client")
		}

		wstream, err := cli.Write(ctx)
		if err != nil {
			return nil, annotateErr(err, peer, "when open write stream")
		}

		failpoint.Inject("mockWritePeerErr", func() {
			err = errors.Errorf("mock write peer error")
			failpoint.Return(nil, annotateErr(err, peer, "when open write stream"))
		})

		// Bind uuid for this write request
		if err = wstream.Send(req); err != nil {
			return nil, annotateErr(err, peer, "when send meta")
		}
		clients = append(clients, wstream)
		allPeers = append(allPeers, peer)
	}
	dataCommitTS := j.ingestData.GetTS()
	intest.AssertFunc(func() bool {
		timeOfTS := oracle.GetTimeFromTS(dataCommitTS)
		now := time.Now()
		if timeOfTS.Sub(now) > time.Hour {
			return false
		}
		if now.Sub(timeOfTS) > 24*time.Hour {
			return false
		}
		return true
	}, "TS used in import should in [now-1d, now+1h], but got %d", dataCommitTS)
	if dataCommitTS == 0 {
		return nil, errors.New("data commitTS is 0")
	}
	req.Chunk = &sst.WriteRequest_Batch{
		Batch: &sst.WriteBatch{
			CommitTs: dataCommitTS,
		},
	}

	pairs := make([]*sst.Pair, 0, defaultKVBatchCount)
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

	// preparation work for the write timeout fault injection, only enabled if the following failpoint is enabled
	wcancel := func() {}
	failpoint.Inject("shortWaitNTimeout", func(val failpoint.Value) {
		var innerTimeout time.Duration
		// GO_FAILPOINTS action supplies the duration in
		ms, _ := val.(int)
		innerTimeout = time.Duration(ms) * time.Millisecond
		tidblogutil.Logger(ctx).Info("Injecting a timeout to write context.")
		wctx, wcancel = context.WithTimeoutCause(
			ctx, innerTimeout, common.ErrWriteTooSlow)
	})
	defer wcancel()

	flushKVs := func() error {
		req.Chunk.(*sst.WriteRequest_Batch).Batch.Pairs = pairs[:count]
		preparedMsg := &grpc.PreparedMsg{}
		// by reading the source code, Encode need to find codec and compression from the stream
		// because all stream has the same codec and compression, we can use any one of them
		if err := preparedMsg.Encode(clients[0], req); err != nil {
			return err
		}

		for i := range clients {
			// original ctx would be used when failpoint is not enabled
			// that new context would be used when failpoint is enabled
			err := writeLimiter.WaitN(wctx, allPeers[i].StoreId, int(size))
			if err != nil {
				// We expect to encounter two types of errors here:
				// 1. context.DeadlineExceeded — occurs when the calculated delay is
				//    less than the remaining time in the context, but the context
				//    expires while sleeping.
				// 2. "rate: Wait(n=%d) would exceed context deadline" — a fast-fail
				//    path triggered when the delay already exceeds the remaining
				//    time for context before sleeping.
				//
				// Unfortunately, we cannot precisely control when the context will
				// expire, so both scenarios are valid and expected.
				// Fortunately, the "rate: Wait" error is already treated as
				// retryable, so we only need to explicitly handle
				// context.DeadlineExceeded here.
				// We rely on the defer function at the top of doWrite to handle it
				// for us in general.
				return errors.Trace(err)
			}
			if err := clients[i].SendMsg(preparedMsg); err != nil {
				if err == io.EOF {
					// if it's EOF, need RecvMsg to get the error
					dummy := &sst.WriteResponse{}
					err = clients[i].RecvMsg(dummy)
				}
				return annotateErr(err, allPeers[i], "when send data")
			}
		}

		if local.collector != nil {
			local.collector.Processed(size, int64(count))
		}

		failpoint.Inject("afterFlushKVs", func() {
			tidblogutil.Logger(ctx).Info(fmt.Sprintf("afterFlushKVs count=%d,size=%d", count, size))
		})
		return nil
	}

	iter := j.ingestData.NewIter(ctx, j.keyRange.Start, j.keyRange.End, bufferPool)
	//nolint: errcheck
	defer iter.Close()

	var remainingStartKey []byte
	for iter.First(); iter.Valid(); iter.Next() {
		k, v := iter.Key(), iter.Value()
		kvSize := int64(len(k) + len(v))
		// here we reuse the `*sst.Pair`s to optimize object allocation
		if count < len(pairs) {
			pairs[count].Key = k
			pairs[count].Value = v
		} else {
			pair := &sst.Pair{
				Key:   k,
				Value: v,
			}
			pairs = append(pairs, pair)
		}
		count++
		totalCount++
		size += kvSize
		totalSize += kvSize

		if size >= kvBatchSize {
			if err := flushKVs(); err != nil {
				return nil, errors.Trace(err)
			}
			count = 0
			size = 0
			iter.ReleaseBuf()
		}
		if totalSize >= regionMaxSize || totalCount >= j.regionSplitKeys {
			// we will shrink the key range of this job to real written range
			if iter.Next() {
				remainingStartKey = slices.Clone(iter.Key())
				tidblogutil.Logger(ctx).Info("write to tikv partial finish",
					zap.Int64("count", totalCount),
					zap.Int64("size", totalSize),
					logutil.Key("startKey", j.keyRange.Start),
					logutil.Key("endKey", j.keyRange.End),
					logutil.Key("remainStart", remainingStartKey),
					logutil.Region(region),
					logutil.Leader(j.region.Leader),
					zap.Uint64("commitTS", dataCommitTS))
			}
			break
		}
	}

	if iter.Error() != nil {
		return nil, errors.Trace(iter.Error())
	}

	if count > 0 {
		if err := flushKVs(); err != nil {
			return nil, errors.Trace(err)
		}
		count = 0
		size = 0
		iter.ReleaseBuf()
	}

	var leaderPeerMetas []*sst.SSTMeta
	for i, wStream := range clients {
		resp, closeErr := wStream.CloseAndRecv()
		if closeErr != nil {
			return nil, annotateErr(closeErr, allPeers[i], "when close write stream")
		}
		if resp.Error != nil {
			return nil, annotateErr(errors.New("resp error: "+resp.Error.Message), allPeers[i], "when close write stream")
		}
		if leaderID == region.Peers[i].GetId() {
			leaderPeerMetas = resp.Metas
			tidblogutil.Logger(ctx).Debug("get metas after write kv stream to tikv", zap.Reflect("metas", leaderPeerMetas))
		}
	}

	failpoint.Inject("NoLeader", func() {
		tidblogutil.Logger(ctx).Warn("enter failpoint NoLeader")
		leaderPeerMetas = nil
	})

	// if there is no leader currently, we don't forward the stage to wrote and let caller
	// handle the retry.
	if len(leaderPeerMetas) == 0 {
		tidblogutil.Logger(ctx).Warn("write to tikv no leader",
			logutil.Region(region), logutil.Leader(j.region.Leader),
			zap.Uint64("leader_id", leaderID), logutil.SSTMeta(meta),
			zap.Int64("kv_pairs", totalCount), zap.Int64("total_bytes", totalSize))
		return nil, errors.Annotatef(errdef.ErrNoLeader.GenWithStackByArgs(region.Id),
			"write to tikv with no leader returned, expected leader id %d", leaderID)
	}

	takeTime := time.Since(begin)
	tidblogutil.Logger(ctx).Debug("write to kv", zap.Reflect("region", j.region), zap.Uint64("leader", leaderID),
		zap.Reflect("meta", meta), zap.Reflect("return metas", leaderPeerMetas),
		zap.Int64("kv_pairs", totalCount), zap.Int64("total_bytes", totalSize),
		zap.Stringer("takeTime", takeTime))
	if m, ok := metric.FromContext(ctx); ok {
		m.SSTSecondsHistogram.WithLabelValues(metric.SSTProcessWrite).Observe(takeTime.Seconds())
	}

	return &tikvWriteResult{
		sstMeta:           leaderPeerMetas,
		count:             totalCount,
		totalBytes:        totalSize,
		remainingStartKey: remainingStartKey,
	}, nil
}

// ingest tries to finish the regionJob.
// if any underlying logic has error, ingest will return an error to let caller
// handle it.
func (local *Backend) ingest(ctx context.Context, j *regionJob) (err error) {
	failpoint.Inject("fakeRegionJobs", func() {
		front := j.injected[0]
		j.injected = j.injected[1:]
		failpoint.Return(front.ingest.err)
	})

	if len(j.writeResult.sstMeta) == 0 {
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

	var lastRetriedErr error
	for retry := range maxRetryTimes {
		resp, err := local.doIngest(ctx, j)
		err = injectfailpoint.DXFRandomErrorWithOnePercentWrapper(err)
		if err != nil {
			if common.IsContextCanceledError(err) {
				return err
			}
			metrics.RetryableErrorCount.WithLabelValues(err.Error()).Inc()
			tidblogutil.Logger(ctx).Warn("meet underlying error, will retry ingest",
				log.ShortError(err), logutil.SSTMetas(j.writeResult.sstMeta),
				logutil.Region(j.region.Region), logutil.Leader(j.region.Leader),
				zap.Int("retry", retry))
			lastRetriedErr = err
			continue
		}
		if resp.GetError() == nil {
			return nil
		}
		return ingestcli.NewIngestAPIError(resp.GetError(), func(regions []*metapb.Region) *split.RegionInfo {
			return extractRegionFromErr(j, regions)
		})
	}
	return lastRetriedErr
}

func (local *Backend) checkWriteStall(
	ctx context.Context,
	region *split.RegionInfo,
) (bool, *sst.IngestResponse, error) {
	clientFactory := local.importClientFactory
	for _, peer := range region.Region.GetPeers() {
		cli, err := clientFactory.create(ctx, peer.StoreId)
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
	failpoint.Inject("diskFullOnIngest", func() {
		failpoint.Return(&sst.IngestResponse{
			Error: &errorpb.Error{
				Message: "propose failed: tikv disk full, cmd diskFullOpt={:?}, leader diskUsage={:?}",
				DiskFull: &errorpb.DiskFull{
					StoreId: []uint64{1},
				},
			},
		}, nil)
	})
	failpoint.Inject("doIngestFailed", func() {
		failpoint.Return(nil, errors.New("injected error"))
	})
	clientFactory := local.importClientFactory
	supportMultiIngest := local.supportMultiIngest
	shouldCheckWriteStall := local.ShouldCheckWriteStall

	var limiter *ingestLimiter
	if x := local.ingestLimiter.Load(); x != nil {
		limiter = x
	} else {
		limiter = &ingestLimiter{}
	}

	if shouldCheckWriteStall && limiter.NoLimit() {
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
		batch = min(batch, limiter.Burst())
	}

	var resp *sst.IngestResponse
	for start := 0; start < len(j.writeResult.sstMeta); start += batch {
		end := min(start+batch, len(j.writeResult.sstMeta))
		ingestMetas := j.writeResult.sstMeta[start:end]
		weight := uint(len(ingestMetas))

		tidblogutil.Logger(ctx).Debug("ingest meta", zap.Reflect("meta", ingestMetas))

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
			return nil, errors.Annotatef(berrors.ErrPDLeaderNotFound,
				"region id %d has no leader", j.region.Region.Id)
		}

		cli, err := clientFactory.create(ctx, leader.StoreId)
		if err != nil {
			return nil, errors.Trace(err)
		}
		reqCtx := &kvrpcpb.Context{
			RegionId:    j.region.Region.GetId(),
			RegionEpoch: j.region.Region.GetRegionEpoch(),
			Peer:        leader,
			ResourceControlContext: &kvrpcpb.ResourceControlContext{
				ResourceGroupName: local.ResourceGroupName,
			},
			RequestSource: util.BuildRequestSource(true, kv.InternalTxnLightning, local.TaskType),
		}

		err = limiter.Acquire(leader.StoreId, weight)
		if err != nil {
			return nil, errors.Trace(err)
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
		limiter.Release(leader.StoreId, weight)
		if err != nil || resp.GetError() != nil {
			// remove finished sstMetas
			j.writeResult.sstMeta = j.writeResult.sstMeta[start:]
			return resp, errors.Trace(err)
		}
	}
	return resp, nil
}

// UpdateWriteSpeedLimit updates the write limiter of the backend.
func (local *Backend) UpdateWriteSpeedLimit(limit int) {
	local.writeLimiter.UpdateLimit(limit)
}

// GetWriteSpeedLimit returns the speed of the write limiter.
func (local *Backend) GetWriteSpeedLimit() int {
	return local.writeLimiter.Limit()
}

func extractRegionFromErr(job *regionJob, currentRegions []*metapb.Region) *split.RegionInfo {
	// unlike classic kernel, nextgen cannot return the full current region infos
	// for the range of the region which is used for ingest, it can only return
	// region info of the same region ID.
	if kerneltype.IsNextGen() || len(currentRegions) == 0 {
		return nil
	}

	intest.Assert(len(job.writeResult.sstMeta) > 0)
	var currentRegion *metapb.Region
	for _, r := range currentRegions {
		// nextgen doesn't have job.writeResult.sstMeta, be careful when modify it
		// when nextgen can return correct current region infos.
		if insideRegion(r, job.writeResult.sstMeta) {
			currentRegion = r
			break
		}
	}
	if currentRegion != nil {
		var newLeader *metapb.Peer
		for _, p := range currentRegion.Peers {
			if p.GetStoreId() == job.region.Leader.GetStoreId() {
				newLeader = p
				break
			}
		}
		if newLeader != nil {
			return &split.RegionInfo{
				Leader: newLeader,
				Region: currentRegion,
			}
		}
	}
	return nil
}

// the input error must be an retryable error
func getNextStageOnIngestError(err error) (*split.RegionInfo, jobStageTp) {
	var theErr *ingestcli.IngestAPIError
	if goerrors.As(err, &theErr) {
		switch {
		case goerrors.Is(theErr.Err, errdef.ErrKVIngestFailed):
			return nil, regionScanned
		case goerrors.Is(theErr.Err, errdef.ErrKVServerIsBusy):
			return nil, wrote
		default:
			if theErr.NewRegion != nil {
				return theErr.NewRegion, regionScanned
			}
			return nil, needRescan
		}
	}
	// we failed to call Ingest or MultiIngest on some retryable errors, such as
	// network errors
	return nil, wrote
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

	ctx    context.Context
	cancel context.CancelFunc
}

type dispatcher struct {
	workerCtx context.Context

	jobFromWorkerCh chan *regionJob
	jobWg           *sync.WaitGroup

	retryer *regionJobRetryer
}

func newDispatcher(
	workerCtx context.Context,
	jobFromWorkerCh chan *regionJob,
	jobWg *sync.WaitGroup,
	retryer *regionJobRetryer,
) *dispatcher {
	return &dispatcher{
		workerCtx:       workerCtx,
		jobFromWorkerCh: jobFromWorkerCh,
		jobWg:           jobWg,
		retryer:         retryer,
	}
}

func (d *dispatcher) run() error {
	var (
		job *regionJob
		ok  bool
	)
	for {
		select {
		case <-d.workerCtx.Done():
			return nil
		case job, ok = <-d.jobFromWorkerCh:
		}
		if !ok {
			d.retryer.close()
			return nil
		}
		switch job.stage {
		case regionScanned, wrote:
			job.retryCount++
			if job.retryCount > MaxWriteAndIngestRetryTimes {
				job.done(d.jobWg)
				lastErr := job.lastRetryableErr
				intest.Assert(lastErr != nil, "lastRetryableErr should not be nil")
				if lastErr == nil {
					lastErr = errors.New("retry limit exceeded")
					tidblogutil.Logger(d.workerCtx).Error(
						"lastRetryableErr should not be nil",
						logutil.Key("startKey", job.keyRange.Start),
						logutil.Key("endKey", job.keyRange.End),
						zap.Stringer("stage", job.stage),
						zap.Error(lastErr))
				}
				return lastErr
			}
			// max retry backoff time: 2+4+8+16+30*26=810s
			sleepSecond := min(math.Pow(2, float64(job.retryCount)), float64(maxRetryBackoffSecond))
			job.waitUntil = time.Now().Add(time.Second * time.Duration(sleepSecond))
			tidblogutil.Logger(d.workerCtx).Info("put job back to jobCh to retry later",
				logutil.Key("startKey", job.keyRange.Start),
				logutil.Key("endKey", job.keyRange.End),
				zap.Stringer("stage", job.stage),
				zap.Int("retryCount", job.retryCount),
				zap.Time("waitUntil", job.waitUntil),
				log.ShortError(job.lastRetryableErr),
			)
			if !d.retryer.push(job) {
				// retryer is closed by worker error
				job.done(d.jobWg)
			}
		case ingested:
			job.done(d.jobWg)
		case needRescan:
			panic("should not reach here")
		}
	}
}

// newRegionJobRetryer creates a regionJobRetryer. regionJobRetryer.run is
// expected to be called soon.
func newRegionJobRetryer(
	workerCtx context.Context,
	putBackCh chan<- *regionJob,
	jobWg *sync.WaitGroup,
) *regionJobRetryer {
	ctx, cancel := context.WithCancel(workerCtx)
	ret := &regionJobRetryer{
		putBackCh: putBackCh,
		reload:    make(chan struct{}, 1),
		jobWg:     jobWg,
		ctx:       ctx,
		cancel:    cancel,
	}
	ret.protectedQueue.q = make(regionJobRetryHeap, 0, 16)
	return ret
}

// run occupies the goroutine and starts the retry loop. Cancel the `ctx` will
// stop retryer and `jobWg.Done` will be trigger for jobs that are not put back
// yet. It should only be used in error case.
func (q *regionJobRetryer) run() {
	defer q.cleanupUnprocessedJobs()

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
			case <-q.ctx.Done():
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
				case <-q.ctx.Done():
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
			case <-q.ctx.Done():
				return
			case <-q.reload:
			}
		}
	}
}

// close stops the retryer. It should only be used in the happy path where all
// jobs are finished.
func (q *regionJobRetryer) close() {
	q.cancel()
	close(q.putBackCh)
	intest.AssertFunc(func() bool {
		q.protectedToPutBack.mu.Lock()
		defer q.protectedToPutBack.mu.Unlock()
		return q.protectedToPutBack.toPutBack == nil
	}, "toPutBack should be nil considering it's happy path")
	intest.AssertFunc(func() bool {
		q.protectedQueue.mu.Lock()
		defer q.protectedQueue.mu.Unlock()
		return len(q.protectedQueue.q) == 0
	}, "queue should be empty considering it's happy path")
}

// cleanupUnprocessedJobs is only internally used, caller should not use it.
func (q *regionJobRetryer) cleanupUnprocessedJobs() {
	q.protectedClosed.mu.Lock()
	defer q.protectedClosed.mu.Unlock()
	q.protectedClosed.closed = true

	if q.protectedToPutBack.toPutBack != nil {
		q.protectedToPutBack.toPutBack.done(q.jobWg)
	}
	for _, job := range q.protectedQueue.q {
		job.done(q.jobWg)
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

// storeBalancer is used to balance the store load when sending region jobs to
// worker. Internally it maintains a large enough buffer to hold all region jobs,
// and pick the job related to stores that has the least load to send to worker.
// Because it does not have backpressure, it should not be used with external
// engine to avoid OOM.
type storeBalancer struct {
	// map[int]*regionJob
	jobs   sync.Map
	jobIdx int
	jobWg  *sync.WaitGroup

	jobToWorkerCh      <-chan *regionJob
	innerJobToWorkerCh chan *regionJob

	wakeSendToWorker chan struct{}

	// map[uint64]int. 0 can appear in the map after it's decremented to 0.
	storeLoadMap sync.Map
}

func newStoreBalancer(
	jobToWorkerCh <-chan *regionJob,
	jobWg *sync.WaitGroup,
) *storeBalancer {
	return &storeBalancer{
		jobToWorkerCh:      jobToWorkerCh,
		innerJobToWorkerCh: make(chan *regionJob),
		wakeSendToWorker:   make(chan struct{}, 1),
		jobWg:              jobWg,
	}
}

func (b *storeBalancer) run(workerCtx context.Context) error {
	// all goroutine will not return error except panic, so we make use of
	// ErrorGroupWithRecover.
	wg, ctx2 := util2.NewErrorGroupWithRecoverWithCtx(workerCtx)
	sendToWorkerCtx, cancelSendToWorker := context.WithCancel(ctx2)
	wg.Go(func() error {
		b.runReadToWorkerCh(ctx2)
		cancelSendToWorker()
		return nil
	})
	wg.Go(func() error {
		b.runSendToWorker(sendToWorkerCtx)
		return nil
	})

	if err := wg.Wait(); err != nil {
		return err
	}

	b.jobs.Range(func(_, value any) bool {
		value.(*regionJob).done(b.jobWg)
		return true
	})
	return nil
}

func (b *storeBalancer) runReadToWorkerCh(workerCtx context.Context) {
	for {
		select {
		case <-workerCtx.Done():
			return
		case job, ok := <-b.jobToWorkerCh:
			if !ok {
				close(b.innerJobToWorkerCh)
				return
			}
			b.jobs.Store(b.jobIdx, job)
			b.jobIdx++

			select {
			case b.wakeSendToWorker <- struct{}{}:
			default:
			}
		}
	}
}

func (b *storeBalancer) jobLen() int {
	cnt := 0
	b.jobs.Range(func(_, _ any) bool {
		cnt++
		return true
	})
	return cnt
}

func (b *storeBalancer) runSendToWorker(workerCtx context.Context) {
	for {
		select {
		case <-workerCtx.Done():
			return
		case <-b.wakeSendToWorker:
		}

		remainJobCnt := b.jobLen()
		for range remainJobCnt {
			j := b.pickJob()
			if j == nil {
				// j can be nil if it's executed after the jobs.Store of runReadToWorkerCh
				// and before the sending to wakeSendToWorker of runReadToWorkerCh.
				break
			}

			// after the job is picked and before the job is sent to worker, the score may
			// have changed so we should pick again to get the optimal job. However for
			// simplicity we don't do it. The optimal job will be picked in the next round.
			select {
			case <-workerCtx.Done():
				j.done(b.jobWg)
				if j.region != nil && j.region.Region != nil {
					b.releaseStoreLoad(j.region.Region.Peers)
				}
				return
			case b.innerJobToWorkerCh <- j:
			}
		}
	}
}

func (b *storeBalancer) pickJob() *regionJob {
	var (
		best     *regionJob
		bestIdx  = -1
		minScore = math.MaxInt64
	)
	b.jobs.Range(func(key, value any) bool {
		idx := key.(int)
		job := value.(*regionJob)

		score := 0
		// in unit tests, the fields of job may not set
		if job.region == nil || job.region.Region == nil {
			best = job
			bestIdx = idx
			return false
		}

		for _, p := range job.region.Region.Peers {
			if v, ok := b.storeLoadMap.Load(p.StoreId); ok {
				score += v.(int)
			}
		}

		if score == 0 {
			best = job
			bestIdx = idx
			return false
		}
		if score < minScore {
			minScore = score
			best = job
			bestIdx = idx
		}
		return true
	})
	if bestIdx == -1 {
		return nil
	}

	b.jobs.Delete(bestIdx)
	// in unit tests, the fields of job may not set
	if best.region == nil || best.region.Region == nil {
		return best
	}

	for _, p := range best.region.Region.Peers {
	retry:
		val, loaded := b.storeLoadMap.LoadOrStore(p.StoreId, 1)
		if !loaded {
			continue
		}

		old := val.(int)
		if !b.storeLoadMap.CompareAndSwap(p.StoreId, old, old+1) {
			// retry the whole check because the entry may have been deleted
			goto retry
		}
	}
	return best
}

func (b *storeBalancer) releaseStoreLoad(peers []*metapb.Peer) {
	for _, p := range peers {
	retry:
		val, ok := b.storeLoadMap.Load(p.StoreId)
		if !ok {
			intest.Assert(false,
				"missing key in storeLoadMap. key: %d",
				p.StoreId,
			)
			log.L().Error("missing key in storeLoadMap",
				zap.Uint64("storeID", p.StoreId))
			continue
		}

		old := val.(int)
		if !b.storeLoadMap.CompareAndSwap(p.StoreId, old, old-1) {
			goto retry
		}
	}
}
