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
// See the License for the specific language governing permissions and
// limitations under the License.

package placement

import "github.com/pingcap/parser/model"

const DefaultPolicyMagicVer uint32 = 0

// Policy is the struct to store the placement policy.
type Policy struct {
	MagicVer            uint32      `json:"magic_ver"`
	ID                  int64       `json:"id"`
	Name                model.CIStr `json:"name"`
	PrimaryRegion       string      `json:"primary_region"`
	Regions             string      `json:"regions"`
	Leaders             int64       `json:"leaders"`
	Followers           int64       `json:"followers"`
	Voters              int64       `json:"voters"`
	Schedule            string      `json:"schedule"`
	Constraints         string      `json:"constraints"`
	LeaderConstraints   string      `json:"leader_constraints"`
	FollowerConstraints string      `json:"follower_constraints"`
	VoterConstraints    string      `json:"voter_constraints"`
}
