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

package txn

import (
	"bytes"
	"context"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
)

// RangedKVRetriever contains a kv.KeyRange to indicate the kv range of this retriever
type RangedKVRetriever struct {
	kv.KeyRange
	kv.Retriever
}

// NewRangeRetriever creates a new RangedKVRetriever
func NewRangeRetriever(retriever kv.Retriever, startKey, endKey kv.Key) *RangedKVRetriever {
	return &RangedKVRetriever{
		KeyRange:  kv.KeyRange{StartKey: startKey, EndKey: endKey},
		Retriever: retriever,
	}
}

// Valid returns if the retriever is valid
func (s *RangedKVRetriever) Valid() bool {
	return len(s.EndKey) == 0 || bytes.Compare(s.StartKey, s.EndKey) < 0
}

// Contains returns whether the key located in the range
func (s *RangedKVRetriever) Contains(k kv.Key) bool {
	return bytes.Compare(k, s.StartKey) >= 0 && (len(s.EndKey) == 0 || bytes.Compare(k, s.EndKey) < 0)
}

// Intersect returns a new RangedKVRetriever with an intersected range
func (s *RangedKVRetriever) Intersect(startKey, endKey kv.Key) *RangedKVRetriever {
	maxStartKey := startKey
	if bytes.Compare(s.StartKey, maxStartKey) > 0 {
		maxStartKey = s.StartKey
	}

	minEndKey := endKey
	if len(minEndKey) == 0 || (len(s.EndKey) > 0 && bytes.Compare(s.EndKey, minEndKey) < 0) {
		minEndKey = s.EndKey
	}

	if len(minEndKey) == 0 || bytes.Compare(maxStartKey, minEndKey) < 0 {
		return NewRangeRetriever(s.Retriever, maxStartKey, minEndKey)
	}

	return nil
}

// ScanCurrentRange scans the retriever for current range
func (s *RangedKVRetriever) ScanCurrentRange(reverse bool) (kv.Iterator, error) {
	if !s.Valid() {
		return nil, errors.New("retriever is invalid")
	}

	startKey, endKey := s.StartKey, s.EndKey
	if !reverse {
		return s.Iter(startKey, endKey)
	}

	iter, err := s.IterReverse(endKey)
	if err != nil {
		return nil, err
	}

	if len(startKey) > 0 {
		iter = newLowerBoundReverseIter(iter, startKey)
	}

	return iter, nil
}

type sortedRetrievers []*RangedKVRetriever

func (retrievers sortedRetrievers) TryGet(ctx context.Context, k kv.Key) (bool, []byte, error) {
	if len(retrievers) == 0 {
		return false, nil, nil
	}

	_, r := retrievers.searchRetrieverByKey(k)
	if r == nil || !r.Contains(k) {
		return false, nil, nil
	}

	val, err := r.Get(ctx, k)
	return true, val, err
}

func (retrievers sortedRetrievers) TryBatchGet(ctx context.Context, keys []kv.Key, collectF func(k kv.Key, v []byte)) ([]kv.Key, error) {
	if len(retrievers) == 0 {
		return keys, nil
	}

	var nonCustomKeys []kv.Key
	for _, k := range keys {
		custom, val, err := retrievers.TryGet(ctx, k)
		if !custom {
			nonCustomKeys = append(nonCustomKeys, k)
			continue
		}

		if kv.ErrNotExist.Equal(err) {
			continue
		}

		if err != nil {
			return nil, err
		}

		collectF(k, val)
	}

	return nonCustomKeys, nil
}

// GetScanRetrievers gets all retrievers who have intersections with range [StartKey, endKey).
// If snapshot is not nil, the range between two custom retrievers with a snapshot retriever will also be returned.
func (retrievers sortedRetrievers) GetScanRetrievers(startKey, endKey kv.Key, snapshot kv.Retriever) []*RangedKVRetriever {
	// According to our experience, in most cases there is only one retriever returned.
	result := make([]*RangedKVRetriever, 0, 1)

	// Firstly, we should find the first retriever whose EndKey is after input startKey,
	// it is obvious that the retrievers before it do not have a common range with the input.
	idx, _ := retrievers.searchRetrieverByKey(startKey)

	// If not found, it means the scan range is located out of retrievers, just use snapshot to scan it
	if idx == len(retrievers) {
		if snapshot != nil {
			result = append(result, NewRangeRetriever(snapshot, startKey, endKey))
		}
		return result
	}

	// Check every retriever whose index >= idx whether it intersects with the input range.
	// If it is true, put the intersected range to the result.
	// The range between two retrievers should also be checked because we read snapshot data from there.
	checks := retrievers[idx:]
	for i, retriever := range checks {
		// Intersect with the range which is on the left of the retriever and use snapshot to read it
		// Notice that when len(retriever.StartKey) == 0, that means there is no left range for it
		if len(retriever.StartKey) > 0 && snapshot != nil {
			var snapStartKey kv.Key
			if i != 0 {
				snapStartKey = checks[i-1].EndKey
			} else {
				snapStartKey = nil
			}

			if r := NewRangeRetriever(snapshot, snapStartKey, retriever.StartKey).Intersect(startKey, endKey); r != nil {
				result = append(result, r)
			}
		}

		// Intersect the current retriever
		if r := retriever.Intersect(startKey, endKey); r != nil {
			result = append(result, r)
			continue
		}

		// Not necessary to continue when the current retriever does not have a valid intersection
		return result
	}

	// If the last retriever has an intersection, we should still check the range on its right.
	lastRetriever := checks[len(checks)-1]
	if snapshot != nil && len(lastRetriever.EndKey) > 0 {
		if r := NewRangeRetriever(snapshot, lastRetriever.EndKey, nil).Intersect(startKey, endKey); r != nil {
			result = append(result, r)
		}
	}

	return result
}

// searchRetrieverByKey searches the first retriever whose EndKey after the specified key
func (retrievers sortedRetrievers) searchRetrieverByKey(k kv.Key) (int, *RangedKVRetriever) {
	n := len(retrievers)
	if n == 0 {
		return n, nil
	}

	i := sort.Search(n, func(i int) bool {
		r := retrievers[i]
		return len(r.EndKey) == 0 || bytes.Compare(r.EndKey, k) > 0
	})

	if i < n {
		return i, retrievers[i]
	}

	return n, nil
}
