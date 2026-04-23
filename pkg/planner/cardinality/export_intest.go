//go:build intest

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

package cardinality

import (
	"sync"

	"github.com/pingcap/tidb/pkg/testkit/testdata"
)

var (
	cardinalitySuiteData     testdata.TestData
	cardinalitySuiteDataOnce sync.Once
)

// GetCardinalitySuiteData returns the selectivity/cardinality suite data for external intest tests.
func GetCardinalitySuiteData() testdata.TestData {
	cardinalitySuiteDataOnce.Do(func() {
		bk := make(testdata.BookKeeper, 1)
		bk.LoadTestSuiteData("testdata", "cardinality_suite")
		cardinalitySuiteData = bk["cardinality_suite"]
	})
	return cardinalitySuiteData
}
