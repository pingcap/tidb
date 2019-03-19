// Copyright 2016 PingCAP, Inc.
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

// +build !race

package tikv

import . "github.com/pingcap/check"

// TestCommitMultipleRegions tests commit multiple regions.
// The test takes too long under the race detector.
func (s *testCommitterSuite) TestCommitMultipleRegions(c *C) {
	m := make(map[string]string)
	for i := 0; i < 100; i++ {
		k, v := randKV(10, 10)
		m[k] = v
	}
	s.mustCommit(c, m)

	// Test big values.
	m = make(map[string]string)
	for i := 0; i < 50; i++ {
		k, v := randKV(11, txnCommitBatchSize/7)
		m[k] = v
	}
	s.mustCommit(c, m)
}
