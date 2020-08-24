// Copyright 2020 PingCAP, Inc.
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

// +build !race

package kv

import (
	"encoding/binary"
	"math/rand"

	. "github.com/pingcap/check"
	"github.com/pingcap/goleveldb/leveldb/comparer"
	leveldb "github.com/pingcap/goleveldb/leveldb/memdb"
)

// The test takes too long under the race detector.
func (s testMemDBSuite) TestRandom(c *C) {
	c.Parallel()
	const cnt = 500000
	keys := make([][]byte, cnt)
	for i := range keys {
		keys[i] = make([]byte, rand.Intn(19)+1)
		rand.Read(keys[i])
	}

	p1 := newMemDB()
	p2 := leveldb.New(comparer.DefaultComparer, 4*1024)
	for _, k := range keys {
		p1.Set(k, k)
		_ = p2.Put(k, k)
	}

	c.Check(p1.Len(), Equals, p2.Len())
	c.Check(p1.Size(), Equals, p2.Size())

	rand.Shuffle(cnt, func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

	for _, k := range keys {
		op := rand.Float64()
		if op < 0.35 {
			p1.DeleteKey(k)
			p2.Delete(k)
		} else {
			newValue := make([]byte, rand.Intn(19)+1)
			rand.Read(newValue)
			p1.Set(k, newValue)
			_ = p2.Put(k, newValue)
		}
	}
	s.checkConsist(c, p1, p2)
}

// The test takes too long under the race detector.
func (s testMemDBSuite) TestRandomDerive(c *C) {
	c.Parallel()
	db := newMemDB()
	golden := leveldb.New(comparer.DefaultComparer, 4*1024)
	s.testRandomDeriveRecur(c, db, golden, 0)
}

func (s testMemDBSuite) testRandomDeriveRecur(c *C, db *memdb, golden *leveldb.DB, depth int) [][2][]byte {
	var keys [][]byte
	if op := rand.Float64(); op < 0.33 {
		start, end := rand.Intn(512), rand.Intn(512)+512
		cnt := end - start
		keys = make([][]byte, cnt)
		for i := range keys {
			keys[i] = make([]byte, 8)
			binary.BigEndian.PutUint64(keys[i], uint64(start+i))
		}
	} else if op < 0.66 {
		keys = make([][]byte, rand.Intn(512)+512)
		for i := range keys {
			keys[i] = make([]byte, rand.Intn(19)+1)
			rand.Read(keys[i])
		}
	} else {
		keys = make([][]byte, 512)
		for i := range keys {
			keys[i] = make([]byte, 8)
			binary.BigEndian.PutUint64(keys[i], uint64(i))
		}
	}

	vals := make([][]byte, len(keys))
	for i := range vals {
		vals[i] = make([]byte, rand.Intn(255)+1)
		rand.Read(vals[i])
	}

	h := db.Staging()
	opLog := make([][2][]byte, 0, len(keys))
	for i := range keys {
		db.Set(keys[i], vals[i])
		old, err := golden.Get(keys[i])
		if err != nil {
			opLog = append(opLog, [2][]byte{keys[i], nil})
		} else {
			opLog = append(opLog, [2][]byte{keys[i], old})
		}
		golden.Put(keys[i], vals[i])
	}

	if depth < 2000 {
		childOps := s.testRandomDeriveRecur(c, db, golden, depth+1)
		opLog = append(opLog, childOps...)
	}

	if rand.Float64() < 0.3 && depth > 0 {
		db.Cleanup(h)
		for i := len(opLog) - 1; i >= 0; i-- {
			if opLog[i][1] == nil {
				golden.Delete(opLog[i][0])
			} else {
				golden.Put(opLog[i][0], opLog[i][1])
			}
		}
		opLog = nil
	} else {
		db.Release(h)
	}

	if depth%200 == 0 {
		s.checkConsist(c, db, golden)
	}

	return opLog
}
