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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package statistics

import (
	"hash"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/twmb/murmur3"
	"golang.org/x/exp/maps"
)

var murmur3Pool = sync.Pool{
	New: func() any {
		return murmur3.New64()
	},
}

var fmSketchPool = sync.Pool{
	New: func() any {
		return &FMSketch{
			hashset: make(map[uint64]bool),
			maxSize: 0,
		}
	},
}

// FMSketch is used to count the number of distinct elements in a set.
type FMSketch struct {
	hashset map[uint64]bool
	mask    uint64
	maxSize int
}

// NewFMSketch returns a new FM sketch.
func NewFMSketch(maxSize int) *FMSketch {
	result := fmSketchPool.Get().(*FMSketch)
	result.maxSize = maxSize
	return result
}

// Copy makes a copy for current FMSketch.
func (s *FMSketch) Copy() *FMSketch {
	if s == nil {
		return nil
	}
	result := NewFMSketch(s.maxSize)
	for key, value := range s.hashset {
		result.hashset[key] = value
	}
	result.mask = s.mask
	return result
}

// NDV returns the ndv of the sketch.
func (s *FMSketch) NDV() int64 {
	if s == nil {
		return 0
	}
	return int64(s.mask+1) * int64(len(s.hashset))
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

// InsertValue inserts a value into the FM sketch.
func (s *FMSketch) InsertValue(sc *stmtctx.StatementContext, value types.Datum) error {
	bytes, err := codec.EncodeValue(sc, nil, value)
	if err != nil {
		return errors.Trace(err)
	}
	hashFunc := murmur3Pool.Get().(hash.Hash64)
	hashFunc.Reset()
	defer murmur3Pool.Put(hashFunc)
	_, err = hashFunc.Write(bytes)
	if err != nil {
		return errors.Trace(err)
	}
	s.insertHashValue(hashFunc.Sum64())
	return nil
}

// InsertRowValue inserts multi-column values to the sketch.
func (s *FMSketch) InsertRowValue(sc *stmtctx.StatementContext, values []types.Datum) error {
	b := make([]byte, 0, 8)
	hashFunc := murmur3Pool.Get().(hash.Hash64)
	hashFunc.Reset()
	defer murmur3Pool.Put(hashFunc)
	for _, v := range values {
		b = b[:0]
		b, err := codec.EncodeValue(sc, b, v)
		if err != nil {
			return err
		}
		_, err = hashFunc.Write(b)
		if err != nil {
			return err
		}
	}
	s.insertHashValue(hashFunc.Sum64())
	return nil
}

// MergeFMSketch merges two FM Sketch.
func (s *FMSketch) MergeFMSketch(rs *FMSketch) {
	if s == nil || rs == nil {
		return
	}
	if s.mask < rs.mask {
		s.mask = rs.mask
		for key := range s.hashset {
			if (key & s.mask) != 0 {
				delete(s.hashset, key)
			}
		}
	}
	for key := range rs.hashset {
		s.insertHashValue(key)
	}
}

// FMSketchToProto converts FMSketch to its protobuf representation.
func FMSketchToProto(s *FMSketch) *tipb.FMSketch {
	protoSketch := new(tipb.FMSketch)
	if s != nil {
		protoSketch.Mask = s.mask
		for val := range s.hashset {
			protoSketch.Hashset = append(protoSketch.Hashset, val)
		}
	}
	return protoSketch
}

// FMSketchFromProto converts FMSketch from its protobuf representation.
func FMSketchFromProto(protoSketch *tipb.FMSketch) *FMSketch {
	if protoSketch == nil {
		return nil
	}
	sketch := fmSketchPool.Get().(*FMSketch)
	sketch.mask = protoSketch.Mask
	for _, val := range protoSketch.Hashset {
		sketch.hashset[val] = true
	}
	return sketch
}

// EncodeFMSketch encodes the given FMSketch to byte slice.
func EncodeFMSketch(c *FMSketch) ([]byte, error) {
	if c == nil {
		return nil, nil
	}
	p := FMSketchToProto(c)
	protoData, err := p.Marshal()
	return protoData, err
}

// DecodeFMSketch decode a FMSketch from the given byte slice.
func DecodeFMSketch(data []byte) (*FMSketch, error) {
	if data == nil {
		return nil, nil
	}
	p := &tipb.FMSketch{}
	err := p.Unmarshal(data)
	if err != nil {
		return nil, errors.Trace(err)
	}
	fm := FMSketchFromProto(p)
	fm.maxSize = 10000 // TODO: add this attribute to PB and persist it instead of using a fixed number(executor.maxSketchSize)
	return fm, nil
}

// MemoryUsage returns the total memory usage of a FMSketch.
func (s *FMSketch) MemoryUsage() (sum int64) {
	// In FMSketch, we will ignore the memory usage of `hashFunc`.
	// As for the variables mask(uint64) and maxSize(int) each will consume 8 bytes. This is the origin of the constant 16.
	// And for the variables hashset(map[uint64]bool), each element in map will consume 9 bytes(8[uint64] + 1[bool]).
	sum = int64(16 + 9*len(s.hashset))
	return
}

func (s *FMSketch) reset() {
	maps.Clear(s.hashset)
	s.mask = 0
	s.maxSize = 0
}

// DestroyAndPutToPool resets the FMSketch and puts it to the pool.
func (s *FMSketch) DestroyAndPutToPool() {
	if s == nil {
		return
	}
	s.reset()
	fmSketchPool.Put(s)
}
