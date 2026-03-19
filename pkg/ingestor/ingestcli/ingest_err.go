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
	"strings"

	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/pkg/ingestor/errdef"
)

// IngestAPIError is the converted error when we call Ingest or MultiIngest successfully,
// but the server return some logic error, i.e. errorpb.Error.
type IngestAPIError struct {
	// the converted internal error
	Err error
	// if theErr = ErrKVEpochNotMatch, the new region info maybe extracted from
	// the PB error
	NewRegion *split.RegionInfo
}

// Error implements the error interface.
func (e *IngestAPIError) Error() string {
	return e.Err.Error()
}

// Cause is used for pingcap/errors.Cause
func (e *IngestAPIError) Cause() error {
	return e.Err
}

// Unwrap is used for golang/errors.Is and As
func (e *IngestAPIError) Unwrap() error {
	return e.Err
}

// NewIngestAPIError creates a new IngestAPIError from the errorpb.Error.
// TODO remove regionExtractFn after we move region job to this pkg.
func NewIngestAPIError(errPb *errorpb.Error, extractRegionFn func([]*metapb.Region) *split.RegionInfo) *IngestAPIError {
	res := &IngestAPIError{}
	switch {
	case errPb.NotLeader != nil:
		// meet a problem that the region leader+peer are all updated but the return
		// error is only "NotLeader", we should update the whole region info.
		res.Err = errdef.ErrKVNotLeader.GenWithStack(errPb.GetMessage())
	case errPb.EpochNotMatch != nil:
		res.Err = errdef.ErrKVEpochNotMatch.GenWithStack(errPb.GetMessage())
		if extractRegionFn != nil {
			res.NewRegion = extractRegionFn(errPb.GetEpochNotMatch().GetCurrentRegions())
		}
	case strings.Contains(errPb.Message, "raft: proposal dropped"):
		res.Err = errdef.ErrKVRaftProposalDropped.GenWithStack(errPb.GetMessage())
	case errPb.ServerIsBusy != nil:
		res.Err = errdef.ErrKVServerIsBusy.GenWithStack(errPb.GetMessage())
	case errPb.RegionNotFound != nil:
		res.Err = errdef.ErrKVRegionNotFound.GenWithStack(errPb.GetMessage())
	case errPb.ReadIndexNotReady != nil:
		// this error happens when this region is splitting, the error might be:
		//   read index not ready, reason can not read index due to split, region 64037
		// we have paused schedule, but it's temporary,
		// if next request takes a long time, there's chance schedule is enabled again
		// or on key range border, another engine sharing this region tries to split this
		// region may cause this error too.
		res.Err = errdef.ErrKVReadIndexNotReady.GenWithStack(errPb.GetMessage())
	case errPb.DiskFull != nil:
		res.Err = errdef.ErrKVDiskFull.GenWithStack(errPb.GetMessage())
	default:
		// all others doIngest error, such as stale command, etc. we'll retry it again from writeAndIngestByRange
		res.Err = errdef.ErrKVIngestFailed.GenWithStack(getIngestFailedMsg(errPb))
	}
	return res
}

// if some of the below error happens, the original message might be empty, such
// as RegionNotInitialized, so we prepend the error type in the error message.
func getIngestFailedMsg(errPb *errorpb.Error) string {
	var tp string
	switch {
	case errPb.KeyNotInRegion != nil:
		tp = "KeyNotInRegion"
	case errPb.StaleCommand != nil:
		tp = "StaleCommand"
	case errPb.StoreNotMatch != nil:
		tp = "StoreNotMatch"
	case errPb.RaftEntryTooLarge != nil:
		tp = "RaftEntryTooLarge"
	case errPb.MaxTimestampNotSynced != nil:
		tp = "MaxTimestampNotSynced"
	case errPb.ProposalInMergingMode != nil:
		tp = "ProposalInMergingMode"
	case errPb.DataIsNotReady != nil:
		tp = "DataIsNotReady"
	case errPb.RegionNotInitialized != nil:
		tp = "RegionNotInitialized"
	case errPb.RecoveryInProgress != nil:
		tp = "RecoveryInProgress"
	case errPb.FlashbackInProgress != nil:
		tp = "FlashbackInProgress"
	case errPb.FlashbackNotPrepared != nil:
		tp = "FlashbackNotPrepared"
	case errPb.IsWitness != nil:
		tp = "IsWitness"
	case errPb.MismatchPeerId != nil:
		tp = "MismatchPeerId"
	case errPb.BucketVersionNotMatch != nil:
		tp = "BucketVersionNotMatch"
	case errPb.UndeterminedResult != nil:
		tp = "UndeterminedResult"
	}
	message := errPb.GetMessage()
	if tp == "" {
		return message
	}
	if message == "" {
		return tp
	}
	return fmt.Sprintf("%s %s", tp, message)
}
