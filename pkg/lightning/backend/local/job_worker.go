// Copyright 2025 PingCAP, Inc.
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
	"context"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/pkg/ingestor/engineapi"
	"github.com/pingcap/tidb/pkg/ingestor/ingestcli"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/membuf"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/tikv/client-go/v2/oracle"
	pdhttp "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
)

type regionJobWorker interface {
	run(ctx context.Context) error
}

type regionJobBaseWorker struct {
	jobInCh  chan *regionJob
	jobOutCh chan *regionJob
	jobWg    *sync.WaitGroup

	writeFn     func(ctx context.Context, job *regionJob) (*tikvWriteResult, error)
	ingestFn    func(ctx context.Context, job *regionJob) error
	preRunJobFn func(ctx context.Context, job *regionJob) error
	// called after the job is executed, success or not.
	afterRunJobFn func([]*metapb.Peer)
	// if the region info is stale, we need to generate new jobs based on the old
	// job.
	regenerateJobsFn func(
		ctx context.Context, data engineapi.IngestData, sortedJobRanges []engineapi.Range,
		regionSplitSize, regionSplitKeys int64,
	) ([]*regionJob, error)
}

// run get jobs from the job channel and process them.
// run will return nil if it's expected to stop, where the cases are all jobs are
// finished or the context canceled because other components report error. It will
// return not nil error when it unexpectedly stops. run must call job.done() if
// it doesn't put the job into jobOutCh.
func (w *regionJobBaseWorker) run(ctx context.Context) error {
	metrics.GlobalSortIngestWorkerCnt.WithLabelValues("execute job").Set(0)
	for {
		select {
		case <-ctx.Done():
			return nil
		case job, ok := <-w.jobInCh:
			if !ok {
				return nil
			}

			var peers []*metapb.Peer
			// in unit test, we may not have the real peers
			if job.region != nil && job.region.Region != nil {
				peers = job.region.Region.GetPeers()
			}
			failpoint.InjectCall("beforeExecuteRegionJob", ctx)
			metrics.GlobalSortIngestWorkerCnt.WithLabelValues("execute job").Inc()
			err := w.runJob(ctx, job)
			metrics.GlobalSortIngestWorkerCnt.WithLabelValues("execute job").Dec()

			if w.afterRunJobFn != nil {
				w.afterRunJobFn(peers)
			}
			switch job.stage {
			case regionScanned, wrote, ingested:
				select {
				case <-ctx.Done():
					job.done(w.jobWg)
					return nil
				case w.jobOutCh <- job:
				}
			case needRescan:
				newJobs, err2 := w.regenerateJobsFn(
					ctx,
					job.ingestData,
					[]engineapi.Range{job.keyRange},
					job.regionSplitSize,
					job.regionSplitKeys,
				)
				if err2 != nil {
					// Don't need to put the job back to retry, because regenerateJobsFn
					// has done the retry internally. Here just done for the "needRescan"
					// job and exit directly.
					job.done(w.jobWg)
					return err2
				}
				// 1 "needRescan" job becomes len(jobs) "regionScanned" jobs.
				newJobCnt := len(newJobs) - 1
				for newJobCnt > 0 {
					job.ref(w.jobWg)
					newJobCnt--
				}
				for _, newJob := range newJobs {
					newJob.lastRetryableErr = job.lastRetryableErr
					select {
					case <-ctx.Done():
						newJob.done(w.jobWg)
						// don't exit here, we mark done for each job and exit in
						// the outer loop
					case w.jobOutCh <- newJob:
					}
				}
			}

			if err != nil {
				return err
			}
		}
	}
}

// doRunJob handles a regionJob and tries to convert it to ingested stage.
// If non-retryable error occurs, it will return the error.
// If retryable error occurs, it will return nil and caller should check the stage
// of the regionJob to determine what to do with it.
func (w *regionJobBaseWorker) runJob(ctx context.Context, job *regionJob) error {
	if err := w.preRunJobFn(ctx, job); err != nil {
		return err
	}

	for {
		// the job might in wrote stage if it comes from retry
		if job.stage == regionScanned {
			// writes the data to TiKV and mark this job as wrote stage.
			// we don't need to do cleanup for the pairs written to TiKV if encounters
			// an error, TiKV will take the responsibility to do so.
			// TODO: let client-go provide a high-level write interface.
			res, err := w.writeFn(ctx, job)
			if err != nil {
				if !w.isRetryableImportTiKVError(err) {
					return err
				}
				metrics.RetryableErrorCount.WithLabelValues(err.Error()).Inc()
				// currently only one case will restart write
				if strings.Contains(err.Error(), "RequestTooNew") {
					// TiKV hasn't synced the newest region info with PD, it's ok to
					// rewrite without rescan.
					job.convertStageTo(regionScanned)
				} else {
					job.convertStageTo(needRescan)
				}
				job.lastRetryableErr = err
				log.FromContext(ctx).Warn("meet retryable error when writing to TiKV",
					log.ShortError(err), zap.Stringer("job stage", job.stage))
				return nil
			}
			if res.emptyJob {
				job.convertStageTo(ingested)
			} else {
				job.writeResult = res
				job.convertStageTo(wrote)
			}
		}

		// if the job is empty, it might go to ingested stage directly.
		if job.stage == wrote {
			err := w.ingestFn(ctx, job)
			if err != nil {
				if !w.isRetryableImportTiKVError(err) {
					return err
				}
				metrics.RetryableErrorCount.WithLabelValues(err.Error()).Inc()

				newRegion, nextStage := getNextStageOnIngestError(err)
				job.convertStageTo(nextStage)
				if newRegion != nil {
					job.region = newRegion
				}
				job.lastRetryableErr = err

				log.FromContext(ctx).Warn("meet retryable error when ingesting, will handle the job later",
					log.ShortError(err), zap.Stringer("job stage", job.stage),
					job.region.ToZapFields(),
					logutil.Key("start", job.keyRange.Start),
					logutil.Key("end", job.keyRange.End))
				return nil
			}
			job.convertStageTo(ingested)
		}
		// if the stage is not ingested, it means some error happened, the job should
		// be sent back to caller to retry later, else we handle remaining data.
		if job.stage != ingested {
			return nil
		}

		if job.writeResult == nil || job.writeResult.remainingStartKey == nil {
			return nil
		}
		// partially write and ingest, update the job key range and continue
		job.keyRange.Start = job.writeResult.remainingStartKey
		job.convertStageTo(regionScanned)
	}
}

func (*regionJobBaseWorker) isRetryableImportTiKVError(err error) bool {
	err = errors.Cause(err)
	// io.EOF is not retryable in normal case
	// but on TiKV restart, if we're writing to TiKV(through GRPC)
	// it might return io.EOF(it's GRPC Unavailable in most case),
	// we need to retry on this error.
	// see SendMsg in https://pkg.go.dev/google.golang.org/grpc#ClientStream
	if err == io.EOF {
		return true
	}
	return common.IsRetryableError(err)
}

// blkStoreRegionJobWorker is the retion job worker for block storage engine.
type blkStoreRegionJobWorker struct {
	*regionJobBaseWorker
	checkTiKVSpace bool
	pdHTTPCli      pdhttp.Client
}

func (w *blkStoreRegionJobWorker) preRunJob(ctx context.Context, job *regionJob) error {
	failpoint.Inject("WriteToTiKVNotEnoughDiskSpace", func(_ failpoint.Value) {
		failpoint.Return(
			errors.New("the remaining storage capacity of TiKV is less than 10%%; please increase the storage capacity of TiKV and try again"))
	})
	if w.checkTiKVSpace {
		for _, peer := range job.region.Region.GetPeers() {
			store, err := w.pdHTTPCli.GetStore(ctx, peer.StoreId)
			if err != nil {
				log.FromContext(ctx).Warn("failed to get StoreInfo from pd http api", zap.Error(err))
				continue
			}
			err = checkDiskAvail(ctx, store)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// objStoreRegionJobWorker is the region job worker for object storage engine.
type objStoreRegionJobWorker struct {
	*regionJobBaseWorker
	ingestCli      ingestcli.Client
	writeBatchSize int64
	bufPool        *membuf.Pool
}

func (*objStoreRegionJobWorker) preRunJob(_ context.Context, _ *regionJob) error {
	// cloud engine use cloud storage, such as S3, to hold data, it's assumed to
	// have unlimited available space, so no need to check disk fullness.
	return nil
}

// we don't need to limit write speed as we write to tikv-worker.
func (w *objStoreRegionJobWorker) write(ctx context.Context, job *regionJob) (*tikvWriteResult, error) {
	firstKey, _, err := job.ingestData.GetFirstAndLastKey(job.keyRange.Start, job.keyRange.End)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if firstKey == nil {
		return &tikvWriteResult{emptyJob: true}, nil
	}

	writeCli, err := w.ingestCli.WriteClient(ctx, job.ingestData.GetTS())
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer writeCli.Close()
	dataCommitTS := job.ingestData.GetTS()
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

	pairs := make([]*sst.Pair, 0, defaultKVBatchCount)
	size := int64(0)
	totalCount := int64(0)
	totalSize := int64(0)

	iter := job.ingestData.NewIter(ctx, job.keyRange.Start, job.keyRange.End, w.bufPool)
	//nolint: errcheck
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		k, v := iter.Key(), iter.Value()
		pairs = append(pairs, &sst.Pair{
			Key:   k,
			Value: v,
		})
		size += int64(len(k) + len(v))

		if size >= w.writeBatchSize {
			in := &ingestcli.WriteRequest{
				Pairs: pairs,
			}
			if err := writeCli.Write(in); err != nil {
				return nil, errors.Trace(err)
			}
			totalCount += int64(len(pairs))
			totalSize += size
			size = 0
			pairs = pairs[:0]
			iter.ReleaseBuf()
		}
	}

	if iter.Error() != nil {
		return nil, errors.Trace(iter.Error())
	}

	if len(pairs) > 0 {
		in := &ingestcli.WriteRequest{
			Pairs: pairs,
		}
		if err := writeCli.Write(in); err != nil {
			return nil, errors.Trace(err)
		}
		totalCount += int64(len(pairs))
		totalSize += size
		pairs = pairs[:0]
		iter.ReleaseBuf()
	}

	resp, err := writeCli.Recv()
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &tikvWriteResult{
		count:            totalCount,
		totalBytes:       totalSize,
		nextGenWriteResp: resp,
	}, nil
}

func (w *objStoreRegionJobWorker) ingest(ctx context.Context, j *regionJob) error {
	in := &ingestcli.IngestRequest{
		Region:    j.region,
		WriteResp: j.writeResult.nextGenWriteResp,
	}
	err := w.ingestCli.Ingest(ctx, in)
	if err != nil {
		log.FromContext(ctx).Warn("meet error and handle the job later",
			zap.Stringer("job stage", j.stage),
			logutil.ShortError(err),
			j.region.ToZapFields(),
			logutil.Key("start", j.keyRange.Start),
			logutil.Key("end", j.keyRange.End))

		// TODO: choose target stage based on error.
		return &ingestAPIError{err: err}
	}
	return nil
}
