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
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// PolicyRefInfo is the struct to refer the placement policy.
type PolicyRefInfo struct {
	ID   int64     `json:"id"`
	Name ast.CIStr `json:"name"`
}

// PlacementSettings is the settings of the placement
type PlacementSettings struct {
	PrimaryRegion       string `json:"primary_region"`
	Regions             string `json:"regions"`
	Learners            uint64 `json:"learners"`
	Followers           uint64 `json:"followers"`
	Voters              uint64 `json:"voters"`
	Schedule            string `json:"schedule"`
	Constraints         string `json:"constraints"`
	LeaderConstraints   string `json:"leader_constraints"`
	LearnerConstraints  string `json:"learner_constraints"`
	FollowerConstraints string `json:"follower_constraints"`
	VoterConstraints    string `json:"voter_constraints"`
	SurvivalPreferences string `json:"survival_preferences"`
}

// String implements fmt.Stringer interface.
func (p *PlacementSettings) String() string {
	sb := new(strings.Builder)
	if len(p.PrimaryRegion) > 0 {
		writeSettingStringToBuilder(sb, "PRIMARY_REGION", p.PrimaryRegion)
	}

	if len(p.Regions) > 0 {
		writeSettingStringToBuilder(sb, "REGIONS", p.Regions)
	}

	if len(p.Schedule) > 0 {
		writeSettingStringToBuilder(sb, "SCHEDULE", p.Schedule)
	}

	if len(p.Constraints) > 0 {
		writeSettingStringToBuilder(sb, "CONSTRAINTS", p.Constraints)
	}

	if len(p.LeaderConstraints) > 0 {
		writeSettingStringToBuilder(sb, "LEADER_CONSTRAINTS", p.LeaderConstraints)
	}

	if p.Voters > 0 {
		writeSettingIntegerToBuilder(sb, "VOTERS", p.Voters)
	}

	if len(p.VoterConstraints) > 0 {
		writeSettingStringToBuilder(sb, "VOTER_CONSTRAINTS", p.VoterConstraints)
	}

	if p.Followers > 0 {
		writeSettingIntegerToBuilder(sb, "FOLLOWERS", p.Followers)
	}

	if len(p.FollowerConstraints) > 0 {
		writeSettingStringToBuilder(sb, "FOLLOWER_CONSTRAINTS", p.FollowerConstraints)
	}

	if p.Learners > 0 {
		writeSettingIntegerToBuilder(sb, "LEARNERS", p.Learners)
	}

	if len(p.LearnerConstraints) > 0 {
		writeSettingStringToBuilder(sb, "LEARNER_CONSTRAINTS", p.LearnerConstraints)
	}

	if len(p.SurvivalPreferences) > 0 {
		writeSettingStringToBuilder(sb, "SURVIVAL_PREFERENCES", p.SurvivalPreferences)
	}

	return sb.String()
}

// Clone clones the placement settings.
func (p *PlacementSettings) Clone() *PlacementSettings {
	cloned := *p
	return &cloned
}

func writeSettingStringToBuilder(sb *strings.Builder, item string, value string, separatorFns ...func()) {
	writeSettingItemToBuilder(sb, fmt.Sprintf("%s=\"%s\"", item, strings.ReplaceAll(value, "\"", "\\\"")), separatorFns...)
}
func writeSettingIntegerToBuilder(sb *strings.Builder, item string, value uint64, separatorFns ...func()) {
	writeSettingItemToBuilder(sb, fmt.Sprintf("%s=%d", item, value), separatorFns...)
}

func writeSettingDurationToBuilder(sb *strings.Builder, item string, dur time.Duration, separatorFns ...func()) {
	writeSettingStringToBuilder(sb, item, dur.String(), separatorFns...)
}

func writeSettingItemToBuilder(sb *strings.Builder, item string, separatorFns ...func()) {
	if sb.Len() != 0 {
		for _, fn := range separatorFns {
			fn()
		}
		if len(separatorFns) == 0 {
			sb.WriteString(" ")
		}
	}
	sb.WriteString(item)
}

// PolicyInfo is the struct to store the placement policy.
type PolicyInfo struct {
	*PlacementSettings
	ID    int64       `json:"id"`
	Name  ast.CIStr   `json:"name"`
	State SchemaState `json:"state"`
}

// Clone clones PolicyInfo.
func (p *PolicyInfo) Clone() *PolicyInfo {
	cloned := *p
	cloned.PlacementSettings = p.PlacementSettings.Clone()
	return &cloned
}
