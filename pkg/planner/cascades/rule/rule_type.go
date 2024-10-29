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

type ruleType int

const (
	// DefaultNone indicates this is none rule.
	DefaultNone ruleType = iota
	// XFJoinToApply refers to join to a apply rule.
	XFJoinToApply
)

// String implements the fmt.Stringer interface.
func (tp *ruleType) String() string {
	switch *tp {
	case XFJoinToApply:
		return "join_to_apply"
	default:
		return "default_none"
	}
}
