// Copyright 2023 PingCAP, Inc.
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

package cardinality_test

import (
	"github.com/pingcap/tidb/pkg/planner/cardinality"
)

// MockStatsNode creates a StatsNode for testing
func MockStatsNode(id int64, m int64, num int) *cardinality.StatsNode {
	return cardinality.MockStatsNode(id, m, num)
}
