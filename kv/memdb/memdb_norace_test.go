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

package memdb

import (
	"encoding/binary"
	"math/rand"

	. "github.com/pingcap/check"
	"github.com/pingcap/goleveldb/leveldb/comparer"
	"github.com/pingcap/goleveldb/leveldb/memdb"
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

	p1 := NewSandbox()
	p2 := memdb.New(comparer.DefaultComparer, 4*1024)
	for _, k := range keys {
		p1.Put(k, k)
		_ = p2.Put(k, k)
	}

	c.Check(p1.Len(), Equals, p2.Len())
	c.Check(p1.Size(), Equals, p2.Size())

	rand.Shuffle(cnt, func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

	for _, k := range keys {
		newValue := make([]byte, rand.Intn(19)+1)
		rand.Read(newValue)
		p1.Put(k, newValue)
		_ = p2.Put(k, newValue)
	}
	s.checkConsist(c, p1, p2)
}

// The test takes too long under the race detector.
func (s testMemDBSuite) TestRandomDerive(c *C) {
	c.Parallel()
	s.testRandomDeriveRecur(c, NewSandbox(), memdb.New(comparer.DefaultComparer, 4*1024), 0)
}

func (s testMemDBSuite) testRandomDeriveRecur(c *C, sb *Sandbox, db *memdb.DB, depth int) {
	var keys [][]byte
	if rand.Float64() < 0.5 {
		start, end := rand.Intn(512), rand.Intn(512)+512
		cnt := end - start
		keys = make([][]byte, cnt)
		for i := range keys {
			keys[i] = make([]byte, 8)
			binary.BigEndian.PutUint64(keys[i], uint64(start+i))
		}
	} else {
		keys = make([][]byte, rand.Intn(512)+512)
		for i := range keys {
			keys[i] = make([]byte, rand.Intn(19)+1)
			rand.Read(keys[i])
		}
	}

	vals := make([][]byte, len(keys))
	for i := range vals {
		vals[i] = make([]byte, rand.Intn(255)+1)
		rand.Read(vals[i])
	}

	sbBuf := sb.Derive()
	dbBuf := memdb.New(comparer.DefaultComparer, 4*1024)
	for i := range keys {
		sbBuf.Put(keys[i], vals[i])
		_ = dbBuf.Put(keys[i], vals[i])
	}

	if depth < 1000 {
		s.testRandomDeriveRecur(c, sbBuf, dbBuf, depth+1)
	}

	if rand.Float64() < 0.3 && depth > 0 {
		sbBuf.Discard()
	} else {
		sbBuf.Flush()
		it := dbBuf.NewIterator(nil)
		for it.First(); it.Valid(); it.Next() {
			_ = db.Put(it.Key(), it.Value())
		}
	}
	s.checkConsist(c, sb, db)
}
