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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"strings"

	"github.com/pingcap/tidb/kv"
)

// SetFromString constructs a slice of strings from a comma separated string.
// It is assumed that there is no duplicated entry. You could use addToSet to maintain this property.
// It is exported for tests. I HOPE YOU KNOW WHAT YOU ARE DOING.
func SetFromString(value string) []string {
	if len(value) == 0 {
		return nil
	}
	return strings.Split(value, ",")
}

func setToString(set []string) string {
	return strings.Join(set, ",")
}

// addToSet add a value to the set, e.g:
// addToSet("Select,Insert,Update", "Update") returns "Select,Insert,Update".
func addToSet(set []string, value string) []string {
	for _, v := range set {
		if v == value {
			return set
		}
	}
	return append(set, value)
}

// deleteFromSet delete the value from the set, e.g:
// deleteFromSet("Select,Insert,Update", "Update") returns "Select,Insert".
func deleteFromSet(set []string, value string) []string {
	for i, v := range set {
		if v == value {
			copy(set[i:], set[i+1:])
			return set[:len(set)-1]
		}
	}
	return set
}

// batchRetrieverHelper is a helper for batch returning data with known total rows. This helps implementing memtable
// retrievers of some information_schema tables. Initialize `batchSize` and `totalRows` fields to use it.
type batchRetrieverHelper struct {
	// When retrieved is true, it means retrieving is finished.
	retrieved bool
	// The index that the retrieving process has been done up to (exclusive).
	retrievedIdx int
	batchSize    int
	totalRows    int
}

// nextBatch calculates the index range of the next batch. If there is such a non-empty range, the `retrieveRange` func
// will be invoked and the range [start, end) is passed to it. Returns error if `retrieveRange` returns error.
func (b *batchRetrieverHelper) nextBatch(retrieveRange func(start, end int) error) error {
	if b.retrievedIdx >= b.totalRows {
		b.retrieved = true
	}
	if b.retrieved {
		return nil
	}
	start := b.retrievedIdx
	end := b.retrievedIdx + b.batchSize
	if end > b.totalRows {
		end = b.totalRows
	}

	err := retrieveRange(start, end)
	if err != nil {
		b.retrieved = true
		return err
	}
	b.retrievedIdx = end
	if b.retrievedIdx == b.totalRows {
		b.retrieved = true
	}
	return nil
}

// IntersectHandleMaps get intersection of all handleMaps. Return nil if no intersection.
func IntersectHandleMaps(handleMaps map[int]*kv.HandleMap) (res []kv.Handle) {
	if len(handleMaps) == 0 {
		return nil
	}
	resHandleMaps := make([]*kv.HandleMap, 0, len(handleMaps))
	var gotEmptyHandleMap bool
	for _, m := range handleMaps {
		if m.Len() == 0 {
			gotEmptyHandleMap = true
		}
		resHandleMaps = append(resHandleMaps, m)
	}
	if gotEmptyHandleMap {
		return nil
	}

	intersected := resHandleMaps[0]
	if len(resHandleMaps) > 1 {
		for i := 1; i < len(resHandleMaps); i++ {
			if intersected.Len() == 0 {
				break
			}
			intersected = intersectTwoMaps(intersected, resHandleMaps[i])
		}
	}
	intersected.Range(func(h kv.Handle, val interface{}) bool {
		res = append(res, h)
		return true
	})
	return
}

func intersectTwoMaps(m1, m2 *kv.HandleMap) *kv.HandleMap {
	intersected := kv.NewHandleMap()
	m1.Range(func(h kv.Handle, val interface{}) bool {
		if _, ok := m2.Get(h); ok {
			intersected.Set(h, true)
		}
		return true
	})
	return intersected
}
