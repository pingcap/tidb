// Copyright 2019-present PingCAP, Inc.
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

package tikv

import (
	"bytes"
	"slices"

	"github.com/dgryski/go-farm"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
)

func exceedEndKey(current, endKey []byte) bool {
	if len(endKey) == 0 {
		return false
	}
	return bytes.Compare(current, endKey) >= 0
}

// SortAndDedupHashVals will change hashVals into sort ascending order and remove duplicates
func sortAndDedupHashVals(hashVals []uint64) []uint64 {
	if len(hashVals) > 1 {
		slices.Sort(hashVals)
		hashVals = slices.Compact(hashVals)
	}
	return hashVals
}

func mutationsToHashVals(mutations []*kvrpcpb.Mutation) []uint64 {
	hashVals := make([]uint64, len(mutations))
	for i, mut := range mutations {
		hashVals[i] = farm.Fingerprint64(mut.Key)
	}
	hashVals = sortAndDedupHashVals(hashVals)
	return hashVals
}

func keysToHashVals(keys ...[]byte) []uint64 {
	hashVals := make([]uint64, len(keys))
	for i, key := range keys {
		hashVals[i] = farm.Fingerprint64(key)
	}
	hashVals = sortAndDedupHashVals(hashVals)
	return hashVals
}

func userKeysToHashVals(keys ...y.Key) []uint64 {
	hashVals := make([]uint64, len(keys))
	for i, key := range keys {
		hashVals[i] = farm.Fingerprint64(key.UserKey)
	}
	hashVals = sortAndDedupHashVals(hashVals)
	return hashVals
}

func safeCopy(b []byte) []byte {
	return slices.Clone(b)
}
