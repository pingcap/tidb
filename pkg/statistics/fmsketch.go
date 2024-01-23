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

	"github.com/dolthub/swiss"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/twmb/murmur3"
)

var murmur3Pool = sync.Pool{
	New: func() any {
		return murmur3.New64()
	},
}

var fmSketchPool = sync.Pool{
	New: func() any {
		return &FMSketch{
			hashset: swiss.NewMap[uint64, bool](uint32(128)),
			maxSize: 0,
		}
	},
}

// FMSketch is used to count the number of distinct elements in a set.
type FMSketch struct {
	hashset *swiss.Map[uint64, bool]
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
	s.hashset.Iter(func(key uint64, value bool) bool {
		result.hashset.Put(key, value)
		return false
	})
	result.mask = s.mask
	result.maxSize = s.maxSize
	return result
}

// NDV returns the ndv of the sketch.
func (s *FMSketch) NDV() int64 {
	if s == nil {
		return 0
	}
	return int64(s.mask+1) * int64(s.hashset.Count())
}

func (s *FMSketch) insertHashValue(hashVal uint64) {
	if (hashVal & s.mask) != 0 {
		return
	}
	s.hashset.Put(hashVal, true)
	if s.hashset.Count() > s.maxSize {
		s.mask = s.mask*2 + 1
		s.hashset.Iter(func(k uint64, _ bool) (stop bool) {
			if (k & s.mask) != 0 {
				s.hashset.Delete(k)
			}
			return false
		})
	}
}

// InsertValue inserts a value into the FM sketch.
func (s *FMSketch) InsertValue(sc *stmtctx.StatementContext, value types.Datum) error {
	bytes, err := codec.EncodeValue(sc.TimeZone(), nil, value)
	err = sc.HandleError(err)
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

	errCtx := sc.ErrCtx()
	for _, v := range values {
		b = b[:0]
		b, err := codec.EncodeValue(sc.TimeZone(), b, v)
		err = errCtx.HandleError(err)
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
		s.hashset.Iter(func(key uint64, _ bool) bool {
			if (key & s.mask) != 0 {
				s.hashset.Delete(key)
			}
			return false
		})
	}
	rs.hashset.Iter(func(key uint64, _ bool) bool {
		s.insertHashValue(key)
		return false
	})
}

// FMSketchToProto converts FMSketch to its protobuf representation.
func FMSketchToProto(s *FMSketch) *tipb.FMSketch {
	protoSketch := new(tipb.FMSketch)
	if s != nil {
		protoSketch.Mask = s.mask
		s.hashset.Iter(func(val uint64, _ bool) bool {
			protoSketch.Hashset = append(protoSketch.Hashset, val)
			return false
		})
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
		sketch.hashset.Put(val, true)
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
	// As for the variables mask(uint64) and maxSize(int) each will consume 8 bytes. This is the origin of the constant 16.
	// And for the variables hashset(map[uint64]bool), each element in map will consume 9 bytes(8[uint64] + 1[bool]).
	sum = int64(16 + 9*s.hashset.Count())
	return
}

func (s *FMSketch) reset() {
	s.hashset.Clear()
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
