// Copyright 2021 PingCAP, Inc.
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

package infosync

import (
	"context"

	"github.com/tikv/pd/client/errs"
	pd "github.com/tikv/pd/client/http"
)

// PlacementScheduleState is the returned third-valued state from GetReplicationState(). For convenience, the string of PD is deserialized into an enum first.
type PlacementScheduleState int

const (
	// PlacementScheduleStatePending corresponds to "PENDING" from PD.
	PlacementScheduleStatePending PlacementScheduleState = iota
	// PlacementScheduleStateInProgress corresponds to "INPROGRESS" from PD.
	PlacementScheduleStateInProgress
	// PlacementScheduleStateScheduled corresponds to "REPLICATED" from PD.
	PlacementScheduleStateScheduled
)

func (t PlacementScheduleState) String() string {
	switch t {
	case PlacementScheduleStateScheduled:
		return "SCHEDULED"
	case PlacementScheduleStateInProgress:
		return "INPROGRESS"
	case PlacementScheduleStatePending:
		return "PENDING"
	default:
		return "PENDING"
	}
}

// GetReplicationState is used to check if regions in the given keyranges are replicated from PD.
func GetReplicationState(ctx context.Context, startKey []byte, endKey []byte) (PlacementScheduleState, error) {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return PlacementScheduleStatePending, err
	}
	if is.pdHTTPCli == nil {
		return PlacementScheduleStatePending, nil
	}
	state, err := is.pdHTTPCli.GetRegionsReplicatedStateByKeyRange(ctx, pd.NewKeyRange(startKey, endKey))
	if err != nil || len(state) == 0 {
		return PlacementScheduleStatePending, err
	}
	st := PlacementScheduleStatePending
	switch state {
	case "REPLICATED":
		st = PlacementScheduleStateScheduled
	case "INPROGRESS":
		st = PlacementScheduleStateInProgress
	case "PENDING":
		st = PlacementScheduleStatePending
	}
	return st, nil
}

// GetRegionDistributionByKeyRange is used to get the region distributions by given key range from PD.
func GetRegionDistributionByKeyRange(ctx context.Context, startKey []byte, endKey []byte, engine string) (*pd.RegionDistributions, error) {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return nil, err
	}
	if is.pdHTTPCli == nil {
		return nil, errs.ErrClientGetLeader.FastGenByArgs("pd http cli is nil")
	}
	return is.pdHTTPCli.GetRegionDistributionByKeyRange(ctx, pd.NewKeyRange(startKey, endKey), engine)
}

// GetSchedulerConfig is used to get the configuration of the specified scheduler from PD.
func GetSchedulerConfig(ctx context.Context, schedulerName string) (any, error) {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return nil, err
	}
	if is.pdHTTPCli == nil {
		return nil, errs.ErrClientGetLeader.FastGenByArgs("pd http cli is nil")
	}
	return is.pdHTTPCli.GetSchedulerConfig(ctx, schedulerName)
}

// CreateSchedulerConfigWithInput is used to create a scheduler with the specified input.
func CreateSchedulerConfigWithInput(ctx context.Context, schedulerName string, input map[string]any) error {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return err
	}
	if is.pdHTTPCli == nil {
		return errs.ErrClientGetLeader.FastGenByArgs(schedulerName)
	}
	return is.pdHTTPCli.CreateSchedulerWithInput(ctx, schedulerName, input)
}

// CancelSchedulerJob is used to cancel a given scheduler job.
func CancelSchedulerJob(ctx context.Context, schedulerName string, jobID uint64) error {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return err
	}
	if is.pdHTTPCli == nil {
		return errs.ErrClientGetLeader.FastGenByArgs(schedulerName)
	}
	return is.pdHTTPCli.CancelSchedulerJob(ctx, schedulerName, jobID)
}
