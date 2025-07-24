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
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/errorpb"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
)

func TestIsIngestRetryable(t *testing.T) {
	region := &split.RegionInfo{
		Leader: &metapb.Peer{Id: 1},
		Region: &metapb.Region{
			Id:       1,
			StartKey: []byte{1},
			EndKey:   []byte{3},
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
		},
	}
	metas := []*sst.SSTMeta{
		{
			Range: &sst.Range{
				Start: []byte{1},
				End:   []byte{2},
			},
		},
		{
			Range: &sst.Range{
				Start: []byte{1, 1},
				End:   []byte{2},
			},
		},
	}
	job := regionJob{
		stage: wrote,
		keyRange: common.Range{
			Start: []byte{1},
			End:   []byte{3},
		},
		region: region,
		writeResult: &tikvWriteResult{
			sstMeta: metas,
		},
	}
	// NotLeader doesn't mean region peers are changed, so we can retry ingest.

	resp := &sst.IngestResponse{
		Error: &errorpb.Error{
			NotLeader: &errorpb.NotLeader{
				Leader: &metapb.Peer{Id: 2},
			},
		},
	}

	clone := job
	canContinueIngest, err := (&clone).convertStageOnIngestError(resp)
	require.NoError(t, err)
	require.False(t, canContinueIngest)
	require.Equal(t, needRescan, clone.stage)
	require.Error(t, clone.lastRetryableErr)

	// EpochNotMatch means region is changed, if the new region covers the old, we can restart the writing process.
	// Otherwise, we should restart from region scanning.

	resp.Error = &errorpb.Error{
		EpochNotMatch: &errorpb.EpochNotMatch{
			CurrentRegions: []*metapb.Region{
				{
					Id:       1,
					StartKey: []byte{1},
					EndKey:   []byte{3},
					RegionEpoch: &metapb.RegionEpoch{
						ConfVer: 1,
						Version: 2,
					},
					Peers: []*metapb.Peer{{Id: 1}},
				},
			},
		},
	}
	clone = job
	canContinueIngest, err = (&clone).convertStageOnIngestError(resp)
	require.NoError(t, err)
	require.False(t, canContinueIngest)
	require.Equal(t, regionScanned, clone.stage)
	require.Nil(t, clone.writeResult)
	require.Equal(t, uint64(2), clone.region.Region.RegionEpoch.Version)
	require.Error(t, clone.lastRetryableErr)

	resp.Error = &errorpb.Error{
		EpochNotMatch: &errorpb.EpochNotMatch{
			CurrentRegions: []*metapb.Region{
				{
					Id:       1,
					StartKey: []byte{1},
					EndKey:   []byte{1, 2},
					RegionEpoch: &metapb.RegionEpoch{
						ConfVer: 1,
						Version: 2,
					},
					Peers: []*metapb.Peer{{Id: 1}},
				},
			},
		},
	}
	clone = job
	canContinueIngest, err = (&clone).convertStageOnIngestError(resp)
	require.NoError(t, err)
	require.False(t, canContinueIngest)
	require.Equal(t, needRescan, clone.stage)
	require.Error(t, clone.lastRetryableErr)

	// TODO: in which case raft layer will drop message?

	resp.Error = &errorpb.Error{Message: "raft: proposal dropped"}
	clone = job
	canContinueIngest, err = (&clone).convertStageOnIngestError(resp)
	require.NoError(t, err)
	require.False(t, canContinueIngest)
	require.Equal(t, needRescan, clone.stage)
	require.Error(t, clone.lastRetryableErr)

	// ReadIndexNotReady means the region is changed, we need to restart from region scanning

	resp.Error = &errorpb.Error{
		ReadIndexNotReady: &errorpb.ReadIndexNotReady{
			Reason: "test",
		},
	}
	clone = job
	canContinueIngest, err = (&clone).convertStageOnIngestError(resp)
	require.NoError(t, err)
	require.False(t, canContinueIngest)
	require.Equal(t, needRescan, clone.stage)
	require.Error(t, clone.lastRetryableErr)

	// TiKV disk full is not retryable

	resp.Error = &errorpb.Error{
		DiskFull: &errorpb.DiskFull{},
	}
	clone = job
	_, err = (&clone).convertStageOnIngestError(resp)
	require.ErrorContains(t, err, "non-retryable error")

	// a general error is retryable from writing

	resp.Error = &errorpb.Error{
		StaleCommand: &errorpb.StaleCommand{},
	}
	clone = job
	canContinueIngest, err = (&clone).convertStageOnIngestError(resp)
	require.NoError(t, err)
	require.False(t, canContinueIngest)
	require.Equal(t, regionScanned, clone.stage)
	require.Nil(t, clone.writeResult)
	require.Error(t, clone.lastRetryableErr)
}

func TestRegionJobRetryer(t *testing.T) {
	var (
		putBackCh   = make(chan *regionJob, 10)
		jobWg       sync.WaitGroup
		ctx, cancel = context.WithCancel(context.Background())
	)
	retryer := startRegionJobRetryer(ctx, putBackCh, &jobWg)
	require.Len(t, putBackCh, 0)

	for i := 0; i < 8; i++ {
		go func() {
			job := &regionJob{
				waitUntil: time.Now().Add(time.Hour),
			}
			jobWg.Add(1)
			ok := retryer.push(job)
			require.True(t, ok)
		}()
	}
	select {
	case <-putBackCh:
		require.Fail(t, "should not put back so soon")
	case <-time.After(500 * time.Millisecond):
	}

	job := &regionJob{
		keyRange: common.Range{
			Start: []byte("123"),
		},
		waitUntil: time.Now().Add(-time.Second),
	}
	jobWg.Add(1)
	ok := retryer.push(job)
	require.True(t, ok)
	select {
	case j := <-putBackCh:
		jobWg.Done()
		require.Equal(t, job, j)
	case <-time.After(5 * time.Second):
		require.Fail(t, "should put back very quickly")
	}

	cancel()
	jobWg.Wait()
	ok = retryer.push(job)
	require.False(t, ok)

	// test when putBackCh is blocked, retryer.push is not blocked and
	// the return value of retryer.close is correct

	ctx, cancel = context.WithCancel(context.Background())
	putBackCh = make(chan *regionJob)
	retryer = startRegionJobRetryer(ctx, putBackCh, &jobWg)

	job = &regionJob{
		keyRange: common.Range{
			Start: []byte("123"),
		},
		waitUntil: time.Now().Add(-time.Second),
	}
	jobWg.Add(1)
	ok = retryer.push(job)
	require.True(t, ok)
	time.Sleep(3 * time.Second)
	// now retryer is sending to putBackCh, but putBackCh is blocked
	job = &regionJob{
		keyRange: common.Range{
			Start: []byte("456"),
		},
		waitUntil: time.Now().Add(-time.Second),
	}
	jobWg.Add(1)
	ok = retryer.push(job)
	require.True(t, ok)
	cancel()
	jobWg.Wait()
}

func TestNewRegionJobs(t *testing.T) {
	buildRegion := func(regionKeys [][]byte) []*split.RegionInfo {
		ret := make([]*split.RegionInfo, 0, len(regionKeys)-1)
		for i := 0; i < len(regionKeys)-1; i++ {
			ret = append(ret, &split.RegionInfo{
				Region: &metapb.Region{
					StartKey: codec.EncodeBytes(nil, regionKeys[i]),
					EndKey:   codec.EncodeBytes(nil, regionKeys[i+1]),
				},
			})
		}
		return ret
	}
	buildJobRanges := func(jobRangeKeys [][]byte) []common.Range {
		ret := make([]common.Range, 0, len(jobRangeKeys)-1)
		for i := 0; i < len(jobRangeKeys)-1; i++ {
			ret = append(ret, common.Range{
				Start: jobRangeKeys[i],
				End:   jobRangeKeys[i+1],
			})
		}
		return ret
	}

	cases := []struct {
		regionKeys   [][]byte
		jobRangeKeys [][]byte
		jobKeys      [][]byte
	}{
		{
			regionKeys:   [][]byte{{1}, nil},
			jobRangeKeys: [][]byte{{2}, {3}, {4}},
			jobKeys:      [][]byte{{2}, {3}, {4}},
		},
		{
			regionKeys:   [][]byte{{1}, {4}},
			jobRangeKeys: [][]byte{{1}, {2}, {3}, {4}},
			jobKeys:      [][]byte{{1}, {2}, {3}, {4}},
		},
		{

			regionKeys:   [][]byte{{1}, {2}, {3}, {4}},
			jobRangeKeys: [][]byte{{1}, {4}},
			jobKeys:      [][]byte{{1}, {2}, {3}, {4}},
		},
		{

			regionKeys:   [][]byte{{1}, {3}, {5}, {7}},
			jobRangeKeys: [][]byte{{2}, {4}, {6}},
			jobKeys:      [][]byte{{2}, {3}, {4}, {5}, {6}},
		},
		{

			regionKeys:   [][]byte{{1}, {4}, {7}},
			jobRangeKeys: [][]byte{{2}, {3}, {4}, {5}, {6}},
			jobKeys:      [][]byte{{2}, {3}, {4}, {5}, {6}},
		},
		{

			regionKeys:   [][]byte{{1}, {5}, {6}, {7}, {8}, {12}},
			jobRangeKeys: [][]byte{{1}, {2}, {3}, {4}, {9}, {10}, {12}},
			jobKeys:      [][]byte{{1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {12}},
		},
	}

	for caseIdx, c := range cases {
		jobs := newRegionJobs(
			buildRegion(c.regionKeys),
			nil,
			buildJobRanges(c.jobRangeKeys),
			0, 0, nil,
		)
		require.Len(t, jobs, len(c.jobKeys)-1, "case %d", caseIdx)
		for i, j := range jobs {
			require.Equal(t, c.jobKeys[i], j.keyRange.Start, "case %d", caseIdx)
			require.Equal(t, c.jobKeys[i+1], j.keyRange.End, "case %d", caseIdx)
		}
	}
}

func TestWorkerPoolWithVariousErrorSource(t *testing.T) {
	var (
		workGroup       *util.ErrorGroupWithRecover
		workerCtx       context.Context
		jobToWorkerCh   chan *regionJob
		jobFromWorkerCh chan *regionJob
		jobWg           sync.WaitGroup
		op              *jobOperator
	)

	local := &Backend{
		writeLimiter: newStoreWriteLimiter(0),
		BackendConfig: BackendConfig{
			WorkerConcurrency: toAtomic(4),
		},
	}

	generator := func(mockErr bool) error {
		counter := 0
		for i := 0; i < 4; i++ {
			jobWg.Add(1)
			job := &regionJob{}
			select {
			case jobToWorkerCh <- job:
				counter++
				if mockErr && counter > 2 {
					return errors.Errorf("generator error")
				}
			case <-workerCtx.Done():
				job.done(&jobWg)
				return nil
			}
		}
		return nil
	}

	drainer := func(mockErr bool) error {
		counter := 0
		for {
			select {
			case job, ok := <-jobFromWorkerCh:
				if !ok {
					return nil
				}
				job.done(&jobWg)
				counter++
				if mockErr && counter > 2 {
					return errors.Errorf("drainer error")
				}
			case <-workerCtx.Done():
				return nil
			}
		}
	}

	resetFn := func() {
		workGroup, workerCtx = util.NewErrorGroupWithRecoverWithCtx(context.Background())
		jobToWorkerCh = make(chan *regionJob)
		jobFromWorkerCh = make(chan *regionJob)
		jobWg = sync.WaitGroup{}

		op = newRegionJobOperator(
			workerCtx, workGroup, &jobWg, local,
			jobToWorkerCh, jobFromWorkerCh,
		)
	}

	t.Run("happy path", func(t *testing.T) {
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/lightning/backend/local/mockRunJobSucceed", "return")
		defer testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/lightning/backend/local/mockRunJobSucceed")

		resetFn()
		require.NoError(t, op.Open())

		workGroup.Go(func() error {
			return drainer(false)
		})

		workGroup.Go(func() error {
			if err := generator(false); err != nil {
				return err
			}
			jobWg.Wait()
			return op.Close()
		})

		require.NoError(t, workGroup.Wait())
		require.NoError(t, op.Close())
	})

	t.Run("drainer error", func(t *testing.T) {
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/lightning/backend/local/mockRunJobSucceed", "return")
		defer testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/lightning/backend/local/mockRunJobSucceed")

		resetFn()
		require.NoError(t, op.Open())

		workGroup.Go(func() error {
			return drainer(true)
		})

		workGroup.Go(func() error {
			if err := generator(false); err != nil {
				return err
			}
			jobWg.Wait()
			return op.Close()
		})

		err := workGroup.Wait()
		// worker pool is quit by cancelled context
		require.ErrorIs(t, op.Close(), context.Canceled)
		require.ErrorContains(t, err, "drainer error")
	})

	t.Run("generator error", func(t *testing.T) {
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/lightning/backend/local/mockRunJobSucceed", "return")
		defer testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/lightning/backend/local/mockRunJobSucceed")

		resetFn()
		require.NoError(t, op.Open())

		workGroup.Go(func() error {
			return drainer(false)
		})

		workGroup.Go(func() error {
			if err := generator(true); err != nil {
				return err
			}
			jobWg.Wait()
			return op.Close()
		})

		err := workGroup.Wait()
		// worker pool is quit by cancelled context
		require.ErrorIs(t, op.Close(), context.Canceled)
		require.ErrorContains(t, err, "generator error")
	})

	t.Run("worker pool error", func(t *testing.T) {
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/lightning/backend/local/injectPanicForRegionJob", "return")
		defer testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/lightning/backend/local/injectPanicForRegionJob")

		resetFn()
		require.NoError(t, op.Open())

		workGroup.Go(func() error {
			return drainer(false)
		})

		workGroup.Go(func() error {
			if err := generator(false); err != nil {
				return err
			}
			jobWg.Wait()
			return op.Close()
		})

		err := workGroup.Wait()
		require.ErrorContains(t, op.Close(), "panic")
		require.ErrorContains(t, err, "panic")
	})
}
