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
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/errorpb"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
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
		done        = make(chan struct{})
	)
	retryer := newRegionJobRetryer(ctx, putBackCh, &jobWg)
	require.Len(t, putBackCh, 0)
	go func() {
		defer close(done)
		retryer.run()
	}()

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
	<-done
	ok = retryer.push(job)
	require.False(t, ok)

	// test when putBackCh is blocked, retryer.push is not blocked and
	// the return value of retryer.close is correct

	ctx, cancel = context.WithCancel(context.Background())
	putBackCh = make(chan *regionJob)
	retryer = newRegionJobRetryer(ctx, putBackCh, &jobWg)
	done = make(chan struct{})
	go func() {
		defer close(done)
		retryer.run()
	}()

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
	<-done

	// test when close successfully, regionJobRetryer should close the putBackCh
	ctx = context.Background()
	putBackCh = make(chan *regionJob)
	retryer = newRegionJobRetryer(ctx, putBackCh, &jobWg)
	done = make(chan struct{})
	go func() {
		defer close(done)
		retryer.run()
	}()

	job = &regionJob{
		keyRange: common.Range{
			Start: []byte("123"),
		},
		waitUntil: time.Now().Add(-time.Second),
	}
	ok = retryer.push(job)
	require.True(t, ok)
	<-putBackCh
	retryer.close()
	<-putBackCh
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

func mockWorkerReadJob(
	t *testing.T,
	b *storeBalancer,
	jobs []*regionJob,
	jobToWorkerCh chan<- *regionJob,
) []*regionJob {
	ret := make([]*regionJob, len(jobs))
	jobToWorkerCh <- jobs[0]
	require.Eventually(t, func() bool {
		// wait runSendToWorker goroutine is blocked at sending
		return b.jobLen() == 0
	}, time.Second, 10*time.Millisecond)

	for _, job := range jobs[1:] {
		jobToWorkerCh <- job
	}
	require.Eventually(t, func() bool {
		// rest are waiting to be picked
		return b.jobLen() == len(jobs)-1
	}, time.Second, 10*time.Millisecond)
	for i := range ret {
		got := <-b.innerJobToWorkerCh
		ret[i] = got
	}
	return ret
}

func checkStoreScoreZero(t *testing.T, b *storeBalancer) {
	b.storeLoadMap.Range(func(_, value any) bool {
		require.Equal(t, 0, value.(int))
		return true
	})
}

func TestStoreBalancerPick(t *testing.T) {
	jobToWorkerCh := make(chan *regionJob)
	jobWg := sync.WaitGroup{}
	ctx := context.Background()

	b := newStoreBalancer(jobToWorkerCh, &jobWg)
	done := make(chan struct{})
	go func() {
		defer close(done)
		err := b.run(ctx)
		require.NoError(t, err)
	}()

	job := &regionJob{
		region: &split.RegionInfo{
			Region: &metapb.Region{
				Peers: []*metapb.Peer{
					{Id: 1, StoreId: 1},
					{Id: 2, StoreId: 2},
				},
			},
		},
	}

	// the worker can get the job just sent to storeBalancer
	got := mockWorkerReadJob(t, b, []*regionJob{job}, jobToWorkerCh)
	require.Equal(t, []*regionJob{job}, got)

	// mimic the worker is handled the job and storeBalancer release it
	b.releaseStoreLoad(job.region.Region.GetPeers())
	checkStoreScoreZero(t, b)

	busyStoreJob := &regionJob{
		region: &split.RegionInfo{
			Region: &metapb.Region{
				Peers: []*metapb.Peer{
					{Id: 3, StoreId: 2},
					{Id: 4, StoreId: 2},
				},
			},
		},
	}
	idleStoreJob := &regionJob{
		region: &split.RegionInfo{
			Region: &metapb.Region{
				Peers: []*metapb.Peer{
					{Id: 5, StoreId: 3},
					{Id: 6, StoreId: 4},
				},
			},
		},
	}

	// now the worker should get the job in specific order. The first job is already
	// picked and can't be dynamically changed by design, so the order is job,
	// idleStoreJob, busyStoreJob
	got = mockWorkerReadJob(t, b, []*regionJob{job, busyStoreJob, idleStoreJob}, jobToWorkerCh)
	require.Equal(t, []*regionJob{job, idleStoreJob, busyStoreJob}, got)
	// mimic the worker finished the job in different order
	jonDone := make(chan struct{}, 3)
	go func() {
		b.releaseStoreLoad(idleStoreJob.region.Region.GetPeers())
		jonDone <- struct{}{}
	}()
	go func() {
		b.releaseStoreLoad(job.region.Region.GetPeers())
		jonDone <- struct{}{}
	}()
	go func() {
		b.releaseStoreLoad(busyStoreJob.region.Region.GetPeers())
		jonDone <- struct{}{}
	}()

	for i := 0; i < 3; i++ {
		<-jonDone
	}
	checkStoreScoreZero(t, b)

	close(jobToWorkerCh)
	<-done
}

func mockRegionJob4Balance(t *testing.T, cnt int) []*regionJob {
	seed := time.Now().UnixNano()
	t.Logf("seed: %d", seed)
	r := rand.New(rand.NewSource(seed))
	ret := make([]*regionJob, cnt)
	for i := range ret {
		ret[i] = &regionJob{
			region: &split.RegionInfo{
				Region: &metapb.Region{
					Peers: []*metapb.Peer{
						{StoreId: uint64(r.Intn(10))},
						{StoreId: uint64(r.Intn(10))},
					},
				},
			},
		}
	}

	return ret
}

func TestCancelBalancer(t *testing.T) {
	jobToWorkerCh := make(chan *regionJob)
	jobWg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())

	b := newStoreBalancer(jobToWorkerCh, &jobWg)
	done := make(chan struct{})
	go func() {
		defer close(done)
		err := b.run(ctx)
		require.NoError(t, err)
	}()

	jobs := mockRegionJob4Balance(t, 20)
	for _, job := range jobs {
		jobWg.Add(1)
		jobToWorkerCh <- job
	}

	cancel()
	<-done
	jobWg.Wait()
}

func TestNewWriteRequest(T *testing.T) {
	req := newWriteRequest(&sst.SSTMeta{}, "", "")
	require.Equal(T, req.Context.TxnSource, uint64(kv.LightningPhysicalImportTxnSource))
}

func TestStoreBalancerNoRace(t *testing.T) {
	jobToWorkerCh := make(chan *regionJob)
	jobFromWorkerCh := make(chan *regionJob)
	jobWg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())

	b := newStoreBalancer(jobToWorkerCh, &jobWg)
	done := make(chan struct{})
	go func() {
		defer close(done)
		err := b.run(ctx)
		require.NoError(t, err)
	}()

	cnt := 200
	done2 := make(chan struct{})

	jobs := mockRegionJob4Balance(t, cnt)
	for _, job := range jobs {
		jobWg.Add(1)
		jobToWorkerCh <- job
	}

	go func() {
		// mimic that worker handles the job and send back to storeBalancer concurrently
		for j := range b.innerJobToWorkerCh {
			go func() {
				b.releaseStoreLoad(j.region.Region.GetPeers())
				j.done(&jobWg)
			}()
		}
		close(done2)
	}()

	jobWg.Wait()
	checkStoreScoreZero(t, b)

	cancel()
	<-done
	close(b.innerJobToWorkerCh)
	<-done2
	require.Len(t, jobFromWorkerCh, 0)
}

func TestUpdateAndGetLimiterConcurrencySafety(t *testing.T) {
	backend := &Backend{
		writeLimiter: newStoreWriteLimiter(0),
	}

	var wg sync.WaitGroup
	concurrentRoutines := 100
	for i := 0; i < concurrentRoutines; i++ {
		wg.Add(2)
		go func(limit int) {
			defer wg.Done()
			backend.UpdateWriteSpeedLimit(limit)
		}(i)

		go func() {
			defer wg.Done()
			_ = backend.GetWriteSpeedLimit()
		}()
	}
	wg.Wait()
}
