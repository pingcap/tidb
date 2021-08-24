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
	if !s.Valid() {
		return nil
	}

	//                      startKey     endKey
	// ---------------------|============|---
	// ---|============|---------------------
	//    s.StartKey   s.EndKey
	if len(s.EndKey) != 0 && bytes.Compare(s.EndKey, startKey) <= 0 {
		return nil
	}

	//    startKey     endKey
	// ---|============|---------------------
	// ---------------------|============|---
	//                      s.StartKey   s.EndKey
	if len(endKey) != 0 && bytes.Compare(endKey, s.StartKey) <= 0 {
		return nil
	}

	cmpStart := bytes.Compare(startKey, s.StartKey)
	cmpEnd := 0
	switch {
	case len(endKey) == 0 && len(s.EndKey) != 0:
		cmpEnd = 1
	case len(endKey) != 0 && len(s.EndKey) == 0:
		cmpEnd = -1
	default:
		cmpEnd = bytes.Compare(endKey, s.EndKey)
	}

	if cmpStart >= 0 && cmpEnd <= 0 {
		//         startKey   endKey
		// --------|==========|------
		// -----|================|---
		//      s.StartKey       s.EndKey
		return NewRangeRetriever(s.Retriever, startKey, endKey)
	}

	if cmpStart <= 0 && cmpEnd >= 0 {
		//      startKey           endKey
		// -----|==================|---
		// --------|============|------
		//         s.StartKey   s.EndKey
		return s
	}

	if cmpStart >= 0 && cmpEnd >= 0 {
		//        startKey     endKey
		// -------|============|---
		// ---|============|-------
		//    s.StartKey   s.EndKey
		return NewRangeRetriever(s.Retriever, startKey, s.EndKey)
	}

	//    startKey     endKey
	// ---|============|-------
	// -------|============|---
	//        s.StartKey   s.EndKey
	return NewRangeRetriever(s.Retriever, s.StartKey, endKey)
}
