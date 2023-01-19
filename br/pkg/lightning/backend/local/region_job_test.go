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
	"testing"

	"github.com/pingcap/kvproto/pkg/errorpb"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/stretchr/testify/require"
)

func TestIsIngestRetryable(t *testing.T) {
	ctx := context.Background()
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
		keyRange: Range{
			start: []byte{1},
			end:   []byte{3},
		},
		region: region,
		writeResult: &tikvWriteResult{
			sstMeta: metas,
		},
	}
	splitCli := &mockSplitClient{}

	// NotLeader doesn't mean region peers are changed, so we can retry ingest.

	resp := &sst.IngestResponse{
		Error: &errorpb.Error{
			NotLeader: &errorpb.NotLeader{
				Leader: &metapb.Peer{Id: 2},
			},
		},
	}

	clone := job
	canContinueIngest, err := (&clone).fixIngestError(ctx, resp, splitCli)
	require.NoError(t, err)
	require.True(t, canContinueIngest)
	require.Equal(t, wrote, clone.stage)
	require.Equal(t, uint64(2), clone.region.Leader.Id)
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
	canContinueIngest, err = (&clone).fixIngestError(ctx, resp, splitCli)
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
	canContinueIngest, err = (&clone).fixIngestError(ctx, resp, splitCli)
	require.NoError(t, err)
	require.False(t, canContinueIngest)
	require.Equal(t, needRescan, clone.stage)
	require.Error(t, clone.lastRetryableErr)

	// TODO: in which case raft layer will drop message?

	resp.Error = &errorpb.Error{Message: "raft: proposal dropped"}
	clone = job
	canContinueIngest, err = (&clone).fixIngestError(ctx, resp, splitCli)
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
	canContinueIngest, err = (&clone).fixIngestError(ctx, resp, splitCli)
	require.NoError(t, err)
	require.False(t, canContinueIngest)
	require.Equal(t, needRescan, clone.stage)
	require.Error(t, clone.lastRetryableErr)

	// TiKV disk full is not retryable

	resp.Error = &errorpb.Error{
		DiskFull: &errorpb.DiskFull{},
	}
	clone = job
	_, err = (&clone).fixIngestError(ctx, resp, splitCli)
	require.ErrorContains(t, err, "non-retryable error")

	// a general error is retryable from writing

	resp.Error = &errorpb.Error{
		StaleCommand: &errorpb.StaleCommand{},
	}
	clone = job
	canContinueIngest, err = (&clone).fixIngestError(ctx, resp, splitCli)
	require.NoError(t, err)
	require.False(t, canContinueIngest)
	require.Equal(t, regionScanned, clone.stage)
	require.Nil(t, clone.writeResult)
	require.Error(t, clone.lastRetryableErr)
}
