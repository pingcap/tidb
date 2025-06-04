package ingestcli

import (
	"fmt"
	"testing"

	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/tidb/pkg/ingestor/errdef"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/stretchr/testify/require"
)

func TestIngestAPIErrorRetryable(t *testing.T) {
	require.True(t, common.IsRetryableError(&IngestAPIError{Err: errdef.ErrKVIngestFailed}))
	require.False(t, common.IsRetryableError(&IngestAPIError{Err: errdef.ErrKVDiskFull}))
}

func TestConvertPBError2Error(t *testing.T) {
	cases := []struct {
		pbErr *errorpb.Error
		res   *IngestAPIError
	}{
		// NotLeader doesn't mean region peers are changed, so we can retry ingest.
		{pbErr: &errorpb.Error{NotLeader: &errorpb.NotLeader{}}, res: &IngestAPIError{Err: errdef.ErrKVNotLeader}},
		// EpochNotMatch means region is changed, if the new region covers the old, we can restart the writing process.
		// Otherwise, we should restart from region scanning.
		{pbErr: &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}}, res: &IngestAPIError{Err: errdef.ErrKVEpochNotMatch}},
		{pbErr: &errorpb.Error{Message: "raft: proposal dropped"}, res: &IngestAPIError{Err: errdef.ErrKVRaftProposalDropped}},
		{pbErr: &errorpb.Error{ServerIsBusy: &errorpb.ServerIsBusy{}}, res: &IngestAPIError{Err: errdef.ErrKVServerIsBusy}},
		{pbErr: &errorpb.Error{RegionNotFound: &errorpb.RegionNotFound{}}, res: &IngestAPIError{Err: errdef.ErrKVRegionNotFound}},
		// ReadIndexNotReady means the region is changed, we need to restart from region scanning
		{pbErr: &errorpb.Error{ReadIndexNotReady: &errorpb.ReadIndexNotReady{}}, res: &IngestAPIError{Err: errdef.ErrKVReadIndexNotReady}},
		// TiKV disk full is not retryable
		{pbErr: &errorpb.Error{DiskFull: &errorpb.DiskFull{}}, res: &IngestAPIError{Err: errdef.ErrKVDiskFull}},
		// a general error is retryable from writing
		{pbErr: &errorpb.Error{StaleCommand: &errorpb.StaleCommand{}}, res: &IngestAPIError{Err: errdef.ErrKVIngestFailed}},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("case %d", i), func(t *testing.T) {
			err := NewIngestAPIError(c.pbErr, nil)
			require.ErrorIs(t, err, c.res.Err)
		})
	}
}
