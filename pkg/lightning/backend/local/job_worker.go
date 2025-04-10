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
	"sync"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/metrics"
)

type regionJobBaseWorker struct {
	jobInCh  chan *regionJob
	jobOutCh chan *regionJob
	jobWg    *sync.WaitGroup

	doRunJobFn func(ctx context.Context, job *regionJob) error
	// called after the job is executed, success or not.
	afterRunJobFn func([]*metapb.Peer)
	// if the region info is stale, we need to generate new jobs based on the old
	// job.
	regenerateJobsFn func(
		ctx context.Context, data common.IngestData, sortedJobRanges []common.Range,
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
			err := w.doRunJobFn(ctx, job)
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
					[]common.Range{job.keyRange},
					job.regionSplitSize,
					job.regionSplitKeys,
				)
				if err2 != nil {
					// Don't need to put the job back to retry, because generateJobForRange
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

type opRegionJobWorker struct {
	*regionJobBaseWorker
}
