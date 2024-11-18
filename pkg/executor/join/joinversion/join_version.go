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

package joinversion

import (
	"strings"
)

const (
	// HashJoinVersionLegacy means hash join v1
	HashJoinVersionLegacy = "legacy"
	// HashJoinVersionOptimized means hash join v2
	HashJoinVersionOptimized = "optimized"
)

var (
	// UseHashJoinV2ForNonGAJoin is added because Hash join contains a lots of types(like inner join, outer join, semi join, nullaware semi join, etc)
	// we want to GA these different kind of joins step by step, but don't want to add a new session variable for each of the join,
	// so we add this variable to control whether to use hash join v2 for nonGA joins(enable it for test, disable it in a release version).
	// PhysicalHashJoin.isGAForHashJoinV2() defines whether the join is GA for hash join v2, we can make each join GA separately by updating
	// this function one by one
	UseHashJoinV2ForNonGAJoin = false
)

func init() {
	// This variable is set to true for test, need to be set back to false in release version
	UseHashJoinV2ForNonGAJoin = true
}

// IsOptimizedVersion returns true if hashJoinVersion equals to HashJoinVersionOptimized
func IsOptimizedVersion(hashJoinVersion string) bool {
	return strings.ToLower(hashJoinVersion) == HashJoinVersionOptimized
}
