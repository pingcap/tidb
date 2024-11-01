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

package rule

import "github.com/pingcap/tidb/pkg/planner/cascades/memo"

type ruleType int

const (
	DEFAULT_NONE ruleType = iota
	JOIN_TO_APPLY

	XF_PUSH_DOWN_PREDICATE_THROUGH_PROJECTION
	MUST_BE_LAST_RULE
)

// String implements the fmt.Stringer interface.
func (tp *ruleType) String() string {
	switch *tp {
	case JOIN_TO_APPLY:
		return "join_to_apply"
	default:
		return "default_none"
	}
}

func init() {
	memo.NumOfRuleSet = int(MUST_BE_LAST_RULE)
}
