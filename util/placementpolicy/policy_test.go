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

package placementpolicy

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPlacementSettingsString(t *testing.T) {
	assert := assert.New(t)

	settings := &PlacementSettings{
		PrimaryRegion: "us-east-1",
		Regions:       "us-east-1,us-east-2",
		Voters:        2,
	}
	assert.Equal("PRIMARY_REGION=\"us-east-1\" REGIONS=\"us-east-1,us-east-2\" VOTERS=2", settings.String())

	settings = &PlacementSettings{
		Voters:              1,
		VoterConstraints:    "[+region=us-east-1]",
		Followers:           2,
		FollowerConstraints: "[+disk=ssd]",
		Learners:            3,
		LearnerConstraints:  "[+region=us-east-2]",
		Schedule:            "EVEN",
	}
	assert.Equal("VOTERS=1 VOTER_CONSTRAINTS=\"[+region=us-east-1]\" FOLLOWERS=2 FOLLOWER_CONSTRAINTS=\"[+disk=ssd]\" LEARNERS=3 LEARNER_CONSTRAINTS=\"[+region=us-east-2]\" SCHEDULE=\"EVEN\"", settings.String())

	settings = &PlacementSettings{
		Voters:      3,
		Followers:   2,
		Learners:    1,
		Constraints: "{+us-east-1:1,+us-east-2:1}",
	}
	assert.Equal("VOTERS=3 FOLLOWERS=2 LEARNERS=1 CONSTRAINTS=\"{+us-east-1:1,+us-east-2:1}\"", settings.String())
}
