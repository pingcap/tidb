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
	level   uint
}

func newFMSketch() *FMSketch {
	return &FMSketch{hashset: make(map[uint64]bool)}
}

func (s *FMSketch) insertHashValue(hash uint64, maxSize int) {
	mask := (1 << s.level) - uint64(1)
	if (hash & mask) != 0 {
		return
	}
	s.hashset[hash] = true
	if len(s.hashset) >= maxSize {
		mask = mask*2 + 1
		s.level++
		for key := range s.hashset {
			if (key & mask) != 0 {
				delete(s.hashset, key)
			}
		}
	}
}

func buildFMSketch(values []types.Datum, maxSize int) (*FMSketch, int64, error) {
	s := newFMSketch()
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
		s.insertHashValue(h.Sum64(), maxSize)
	}
	ndv := (1 << s.level) * int64(len(s.hashset))
	return s, ndv, nil
}

func mergeFMSketches(sketches []*FMSketch, maxSize int) (*FMSketch, int64) {
	s := newFMSketch()
	for _, sketch := range sketches {
		if s.level < sketch.level {
			s.level = sketch.level
		}
	}
	for _, sketch := range sketches {
		for key := range sketch.hashset {
			s.insertHashValue(key, maxSize)
		}
	}
	ndv := (1 << s.level) * int64(len(s.hashset))
	return s, ndv
}
