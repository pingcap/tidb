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
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/pdapi"
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

	if is.etcdCli == nil {
		return PlacementScheduleStatePending, nil
	}

	addrs := is.etcdCli.Endpoints()

	if len(addrs) == 0 {
		return PlacementScheduleStatePending, errors.Errorf("pd unavailable")
	}

	res, err := doRequest(ctx, "GetReplicationState", addrs, fmt.Sprintf("%s/replicated?startKey=%s&endKey=%s", pdapi.Regions, hex.EncodeToString(startKey), hex.EncodeToString(endKey)), "GET", nil)
	if err == nil && res != nil {
		st := PlacementScheduleStatePending
		// it should not fail
		var state string
		_ = json.Unmarshal(res, &state)
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
	return PlacementScheduleStatePending, err
}
