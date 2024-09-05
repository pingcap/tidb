// Copyright 2024 PingCAP, Inc.
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

package model

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
)

func TestPlacementSettingsString(t *testing.T) {
	settings := &PlacementSettings{
		PrimaryRegion: "us-east-1",
		Regions:       "us-east-1,us-east-2",
		Schedule:      "EVEN",
	}
	require.Equal(t, "PRIMARY_REGION=\"us-east-1\" REGIONS=\"us-east-1,us-east-2\" SCHEDULE=\"EVEN\"", settings.String())

	settings = &PlacementSettings{
		LeaderConstraints: "[+region=bj]",
	}
	require.Equal(t, "LEADER_CONSTRAINTS=\"[+region=bj]\"", settings.String())

	settings = &PlacementSettings{
		Voters:              1,
		VoterConstraints:    "[+region=us-east-1]",
		Followers:           2,
		FollowerConstraints: "[+disk=ssd]",
		Learners:            3,
		LearnerConstraints:  "[+region=us-east-2]",
	}
	require.Equal(t, "VOTERS=1 VOTER_CONSTRAINTS=\"[+region=us-east-1]\" FOLLOWERS=2 FOLLOWER_CONSTRAINTS=\"[+disk=ssd]\" LEARNERS=3 LEARNER_CONSTRAINTS=\"[+region=us-east-2]\"", settings.String())

	settings = &PlacementSettings{
		Voters:      3,
		Followers:   2,
		Learners:    1,
		Constraints: "{\"+us-east-1\":1,+us-east-2:1}",
	}
	require.Equal(t, "CONSTRAINTS=\"{\\\"+us-east-1\\\":1,+us-east-2:1}\" VOTERS=3 FOLLOWERS=2 LEARNERS=1", settings.String())
}

func TestPlacementSettingsClone(t *testing.T) {
	settings := &PlacementSettings{}
	clonedSettings := settings.Clone()
	clonedSettings.PrimaryRegion = "r1"
	clonedSettings.Regions = "r1,r2"
	clonedSettings.Followers = 1
	clonedSettings.Voters = 2
	clonedSettings.Followers = 3
	clonedSettings.Constraints = "[+zone=z1]"
	clonedSettings.LearnerConstraints = "[+region=r1]"
	clonedSettings.FollowerConstraints = "[+disk=ssd]"
	clonedSettings.LeaderConstraints = "[+region=r2]"
	clonedSettings.VoterConstraints = "[+zone=z2]"
	clonedSettings.Schedule = "even"
	require.Equal(t, PlacementSettings{}, *settings)
}

func TestPlacementPolicyClone(t *testing.T) {
	policy := &PolicyInfo{
		PlacementSettings: &PlacementSettings{},
	}
	clonedPolicy := policy.Clone()
	clonedPolicy.ID = 100
	clonedPolicy.Name = model.NewCIStr("p2")
	clonedPolicy.State = StateDeleteOnly
	clonedPolicy.PlacementSettings.Followers = 10

	require.Equal(t, int64(0), policy.ID)
	require.Equal(t, model.NewCIStr(""), policy.Name)
	require.Equal(t, StateNone, policy.State)
	require.Equal(t, PlacementSettings{}, *(policy.PlacementSettings))
}
