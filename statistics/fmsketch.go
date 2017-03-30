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

package statistics

import (
	"hash/fnv"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
)

// FMSketch is used to count the number of distinct elements in a set.
type FMSketch struct {
	hashset map[uint64]bool
	mask    uint64
	maxSize int
}

func newFMSketch(maxSize int) *FMSketch {
	return &FMSketch{
		hashset: make(map[uint64]bool),
		maxSize: maxSize,
	}
}

func (s *FMSketch) insertHashValue(hashVal uint64) {
	if (hashVal & s.mask) != 0 {
		return
	}
	s.hashset[hashVal] = true
	if len(s.hashset) > s.maxSize {
		s.mask = s.mask*2 + 1
		for key := range s.hashset {
			if (key & s.mask) != 0 {
				delete(s.hashset, key)
			}
		}
	}
}

func buildFMSketch(values []types.Datum, maxSize int) (*FMSketch, int64, error) {
	s := newFMSketch(maxSize)
	h := fnv.New64a()
	for _, value := range values {
		bytes, err := codec.EncodeValue(nil, value)
		if err != nil {
			return nil, 0, errors.Trace(err)
		}
		h.Reset()
		_, err = h.Write(bytes)
		if err != nil {
			return nil, 0, errors.Trace(err)
		}
		s.insertHashValue(h.Sum64())
	}
	ndv := int64((s.mask + 1)) * int64(len(s.hashset))
	return s, ndv, nil
}

func mergeFMSketches(sketches []*FMSketch, maxSize int) (*FMSketch, int64) {
	s := newFMSketch(maxSize)
	for _, sketch := range sketches {
		if s.mask < sketch.mask {
			s.mask = sketch.mask
		}
	}
	for _, sketch := range sketches {
		for key := range sketch.hashset {
			s.insertHashValue(key)
		}
	}
	ndv := int64((s.mask + 1)) * int64(len(s.hashset))
	return s, ndv
}
