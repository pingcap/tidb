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
	"fmt"
	"strings"

	"github.com/pingcap/parser/model"
)

// PlacementSettings is the settings of the placement
type PlacementSettings struct {
	PrimaryRegion       string `json:"primary_region"`
	Regions             string `json:"regions"`
	Learners            uint64 `json:"learners"`
	Followers           uint64 `json:"followers"`
	Voters              uint64 `json:"voters"`
	Schedule            string `json:"schedule"`
	Constraints         string `json:"constraints"`
	LearnerConstraints  string `json:"learner_constraints"`
	FollowerConstraints string `json:"follower_constraints"`
	VoterConstraints    string `json:"voter_constraints"`
}

func (p *PlacementSettings) String() string {
	settings := make([]string, 0)
	if len(p.PrimaryRegion) > 0 {
		settings = append(settings, fmt.Sprintf("PRIMARY_REGION=\"%s\"", p.PrimaryRegion))
	}

	if len(p.Regions) > 0 {
		settings = append(settings, fmt.Sprintf("REGIONS=\"%s\"", p.Regions))
	}

	if p.Voters > 0 {
		settings = append(settings, fmt.Sprintf("VOTERS=%d", p.Voters))
	}

	if len(p.VoterConstraints) > 0 {
		settings = append(settings, fmt.Sprintf("VOTER_CONSTRAINTS=\"%s\"", p.VoterConstraints))
	}

	if p.Followers > 0 {
		settings = append(settings, fmt.Sprintf("FOLLOWERS=%d", p.Followers))
	}

	if len(p.FollowerConstraints) > 0 {
		settings = append(settings, fmt.Sprintf("FOLLOWER_CONSTRAINTS=\"%s\"", p.FollowerConstraints))
	}

	if p.Learners > 0 {
		settings = append(settings, fmt.Sprintf("LEARNERS=%d", p.Learners))
	}

	if len(p.LearnerConstraints) > 0 {
		settings = append(settings, fmt.Sprintf("LEARNER_CONSTRAINTS=\"%s\"", p.LearnerConstraints))
	}

	if len(p.Constraints) > 0 {
		settings = append(settings, fmt.Sprintf("CONSTRAINTS=\"%s\"", p.Constraints))
	}

	if len(p.Schedule) > 0 {
		settings = append(settings, fmt.Sprintf("SCHEDULE=\"%s\"", p.Schedule))
	}

	return strings.Join(settings, " ")
}

// PolicyInfo is the struct to store the placement policy.
type PolicyInfo struct {
	PlacementSettings
	ID    int64             `json:"id"`
	Name  model.CIStr       `json:"name"`
	State model.SchemaState `json:"state"`
}
