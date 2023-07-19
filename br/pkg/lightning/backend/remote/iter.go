// Copyright 2023 PingCAP, Inc.
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

package remote

import (
	"sync"

	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
)

func newSliceIterator(kvs []kvPair) *sliceIter {
	return &sliceIter{
		first: true,
		kvs:   kvs,
	}
}

type sliceIter struct {
	first bool
	kvs   []kvPair
}

func (s *sliceIter) Seek(key []byte) bool {
	panic("unsupported operation")
}

func (s *sliceIter) Error() error {
	return nil
}

func (s *sliceIter) First() bool {
	panic("unsupported operation")
}

func (s *sliceIter) Last() bool {
	panic("unsupported operation")
}

func (s *sliceIter) Close() error {
	return nil
}

func (s *sliceIter) OpType() sst.Pair_OP {
	panic("unsupported operation")
}

func (s *sliceIter) Valid() bool {
	return len(s.kvs) > 0
}

func (s *sliceIter) Key() []byte {
	return s.kvs[0].key
}

func (s *sliceIter) Value() []byte {
	return s.kvs[0].val
}

func (s *sliceIter) Next() bool {
	if s.first {
		s.first = false
		return len(s.kvs) > 0
	}
	s.kvs = s.kvs[1:]
	return len(s.kvs) > 0
}

type kvPair struct {
	key []byte
	val []byte
}

type kvPairSlicePool struct {
	pool sync.Pool
}

func newKvPairSlicePool() *kvPairSlicePool {
	return &kvPairSlicePool{
		pool: sync.Pool{
			New: func() interface{} {
				return make([]kvPair, 0, 1024)
			},
		},
	}
}

func (p *kvPairSlicePool) get() []kvPair {
	return p.pool.Get().([]kvPair)
}

func (p *kvPairSlicePool) put(s []kvPair) {
	p.pool.Put(s[:0])
}
