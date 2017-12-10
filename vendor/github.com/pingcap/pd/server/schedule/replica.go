// Copyright 2017 PingCAP, Inc.
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

package schedule

import (
	"math"

	"github.com/pingcap/pd/server/core"
)

const replicaBaseScore = 100

// DistinctScore returns the score that the other is distinct from the stores.
// A higher score means the other store is more different from the existed stores.
func DistinctScore(labels []string, stores []*core.StoreInfo, other *core.StoreInfo) float64 {
	var score float64
	for _, s := range stores {
		if s.GetId() == other.GetId() {
			continue
		}
		if index := s.CompareLocation(other, labels); index != -1 {
			score += math.Pow(replicaBaseScore, float64(len(labels)-index-1))
		}
	}
	return score
}

// compareStoreScore compares which store is better for replication.
// Returns 0 if store A is as good as store B.
// Returns 1 if store A is better than store B.
// Returns -1 if store B is better than store A.
func compareStoreScore(storeA *core.StoreInfo, scoreA float64, storeB *core.StoreInfo, scoreB float64) int {
	// The store with higher score is better.
	if scoreA > scoreB {
		return 1
	}
	if scoreA < scoreB {
		return -1
	}
	// The store with lower region score is better.
	if storeA.RegionScore() < storeB.RegionScore() {
		return 1
	}
	if storeA.RegionScore() > storeB.RegionScore() {
		return -1
	}
	return 0
}
