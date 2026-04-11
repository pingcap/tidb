// Copyright 2026 PingCAP, Inc.
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

package core

import (
	"context"

	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
)

// LogicalOptimize exports the logical optimization function for testing
func LogicalOptimize(ctx context.Context, flag uint64, logic base.LogicalPlan) (base.LogicalPlan, error) {
	return LogicalOptimizeTest(ctx, flag, logic)
}

// GetRuntimeFilterGeneratorData returns the test data for runtime filter generator tests
func GetRuntimeFilterGeneratorData() testdata.TestData {
	bk := make(testdata.BookKeeper)
	bk.LoadTestSuiteData("testdata", "runtime_filter_generator_suite")
	return bk["runtime_filter_generator_suite"]
}
