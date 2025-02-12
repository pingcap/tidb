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

	"github.com/pingcap/kvproto/pkg/errorpb"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/restore/split"
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
