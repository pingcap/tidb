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

func TestGetIngestFailedMsg(t *testing.T) {
	cases := []struct {
		pbErr *errorpb.Error
		msg   string
	}{
		{pbErr: &errorpb.Error{KeyNotInRegion: &errorpb.KeyNotInRegion{}}, msg: "KeyNotInRegion"},
		{pbErr: &errorpb.Error{StaleCommand: &errorpb.StaleCommand{}}, msg: "StaleCommand"},
		{pbErr: &errorpb.Error{StoreNotMatch: &errorpb.StoreNotMatch{}}, msg: "StoreNotMatch"},
		{pbErr: &errorpb.Error{RaftEntryTooLarge: &errorpb.RaftEntryTooLarge{}}, msg: "RaftEntryTooLarge"},
		{pbErr: &errorpb.Error{MaxTimestampNotSynced: &errorpb.MaxTimestampNotSynced{}}, msg: "MaxTimestampNotSynced"},
		{pbErr: &errorpb.Error{ProposalInMergingMode: &errorpb.ProposalInMergingMode{}}, msg: "ProposalInMergingMode"},
		{pbErr: &errorpb.Error{DataIsNotReady: &errorpb.DataIsNotReady{}}, msg: "DataIsNotReady"},
		{pbErr: &errorpb.Error{RegionNotInitialized: &errorpb.RegionNotInitialized{}}, msg: "RegionNotInitialized"},
		{pbErr: &errorpb.Error{RecoveryInProgress: &errorpb.RecoveryInProgress{}}, msg: "RecoveryInProgress"},
		{pbErr: &errorpb.Error{FlashbackInProgress: &errorpb.FlashbackInProgress{}}, msg: "FlashbackInProgress"},
		{pbErr: &errorpb.Error{FlashbackNotPrepared: &errorpb.FlashbackNotPrepared{}}, msg: "FlashbackNotPrepared"},
		{pbErr: &errorpb.Error{IsWitness: &errorpb.IsWitness{}}, msg: "IsWitness"},
		{pbErr: &errorpb.Error{MismatchPeerId: &errorpb.MismatchPeerId{}}, msg: "MismatchPeerId"},
		{pbErr: &errorpb.Error{BucketVersionNotMatch: &errorpb.BucketVersionNotMatch{}}, msg: "BucketVersionNotMatch"},
		{pbErr: &errorpb.Error{UndeterminedResult: &errorpb.UndeterminedResult{}}, msg: "UndeterminedResult"},
		{pbErr: &errorpb.Error{RegionNotInitialized: &errorpb.RegionNotInitialized{}, Message: "the message"}, msg: "RegionNotInitialized the message"},
		{pbErr: &errorpb.Error{Message: "the message"}, msg: "the message"},
	}

	for _, c := range cases {
		t.Run(c.msg, func(t *testing.T) {
			msg := getIngestFailedMsg(c.pbErr)
			require.Equal(t, c.msg, msg)
		})
	}
}
