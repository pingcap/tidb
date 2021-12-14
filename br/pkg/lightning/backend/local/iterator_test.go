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

package local

import (
	"bytes"
	"context"
	"math/rand"
	"path/filepath"
	"sort"
	"time"

	"github.com/cockroachdb/pebble"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
)

type iteratorSuite struct{}

var _ = Suite(&iteratorSuite{})

func (s *iteratorSuite) TestDuplicateIterator(c *C) {
	var pairs []common.KvPair
	prevRowMax := int64(0)
	// Unique pairs.
	for i := 0; i < 20; i++ {
		pairs = append(pairs, common.KvPair{
			Key:    randBytes(32),
			Val:    randBytes(128),
			RowID:  prevRowMax,
			Offset: int64(i * 1234),
		})
		prevRowMax++
	}
	// Duplicate pairs which repeat the same key twice.
	for i := 20; i < 40; i++ {
		key := randBytes(32)
		pairs = append(pairs, common.KvPair{
			Key:    key,
			Val:    randBytes(128),
			RowID:  prevRowMax,
			Offset: int64(i * 1234),
		})
		prevRowMax++
		pairs = append(pairs, common.KvPair{
			Key:    key,
			Val:    randBytes(128),
			RowID:  prevRowMax,
			Offset: int64(i * 1235),
		})
		prevRowMax++
	}
	// Duplicate pairs which repeat the same key three times.
	for i := 40; i < 50; i++ {
		key := randBytes(32)
		pairs = append(pairs, common.KvPair{
			Key:    key,
			Val:    randBytes(128),
			RowID:  prevRowMax,
			Offset: int64(i * 1234),
		})
		prevRowMax++
		pairs = append(pairs, common.KvPair{
			Key:    key,
			Val:    randBytes(128),
			RowID:  prevRowMax,
			Offset: int64(i * 1235),
		})
		prevRowMax++
		pairs = append(pairs, common.KvPair{
			Key:    key,
			Val:    randBytes(128),
			RowID:  prevRowMax,
			Offset: int64(i * 1236),
		})
		prevRowMax++
	}

	// Find duplicates from the generated pairs.
	var duplicatePairs []common.KvPair
	sort.Slice(pairs, func(i, j int) bool {
		return bytes.Compare(pairs[i].Key, pairs[j].Key) < 0
	})
	uniqueKeys := make([][]byte, 0)
	for i := 0; i < len(pairs); {
		j := i + 1
		for j < len(pairs) && bytes.Equal(pairs[j-1].Key, pairs[j].Key) {
			j++
		}
		uniqueKeys = append(uniqueKeys, pairs[i].Key)
		if i+1 == j {
			i++
			continue
		}
		for k := i; k < j; k++ {
			duplicatePairs = append(duplicatePairs, pairs[k])
		}
		i = j
	}

	keyAdapter := duplicateKeyAdapter{}

	// Write pairs to db after shuffling the pairs.
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	rnd.Shuffle(len(pairs), func(i, j int) {
		pairs[i], pairs[j] = pairs[j], pairs[i]
	})
	storeDir := c.MkDir()
	db, err := pebble.Open(filepath.Join(storeDir, "kv"), &pebble.Options{})
	c.Assert(err, IsNil)
	wb := db.NewBatch()
	for _, p := range pairs {
		key := keyAdapter.Encode(nil, p.Key, 1, p.Offset)
		c.Assert(wb.Set(key, p.Val, nil), IsNil)
	}
	c.Assert(wb.Commit(pebble.Sync), IsNil)

	duplicateDB, err := pebble.Open(filepath.Join(storeDir, "duplicates"), &pebble.Options{})
	c.Assert(err, IsNil)
	engine := &Engine{
		ctx:         context.Background(),
		db:          db,
		keyAdapter:  keyAdapter,
		duplicateDB: duplicateDB,
		tableInfo: &checkpoints.TidbTableInfo{
			DB:   "db",
			Name: "name",
		},
	}
	iter := newDuplicateIter(context.Background(), engine, &pebble.IterOptions{})
	sort.Slice(pairs, func(i, j int) bool {
		key1 := keyAdapter.Encode(nil, pairs[i].Key, pairs[i].RowID, pairs[i].Offset)
		key2 := keyAdapter.Encode(nil, pairs[j].Key, pairs[j].RowID, pairs[j].Offset)
		return bytes.Compare(key1, key2) < 0
	})

	// Verify first pair.
	c.Assert(iter.First(), IsTrue)
	c.Assert(iter.Valid(), IsTrue)
	c.Assert(iter.Key(), BytesEquals, pairs[0].Key)
	c.Assert(iter.Value(), BytesEquals, pairs[0].Val)

	// Verify last pair.
	c.Assert(iter.Last(), IsTrue)
	c.Assert(iter.Valid(), IsTrue)
	c.Assert(iter.Key(), BytesEquals, pairs[len(pairs)-1].Key)
	c.Assert(iter.Value(), BytesEquals, pairs[len(pairs)-1].Val)

	// Iterate all keys and check the count of unique keys.
	for iter.First(); iter.Valid(); iter.Next() {
		c.Assert(iter.Key(), BytesEquals, uniqueKeys[0])
		uniqueKeys = uniqueKeys[1:]
	}
	c.Assert(iter.Error(), IsNil)
	c.Assert(len(uniqueKeys), Equals, 0)
	c.Assert(iter.Close(), IsNil)
	c.Assert(engine.Close(), IsNil)

	// Check duplicates detected by duplicate iterator.
	iter = pebbleIter{Iterator: duplicateDB.NewIter(&pebble.IterOptions{})}
	var detectedPairs []common.KvPair
	for iter.First(); iter.Valid(); iter.Next() {
		key, _, _, err := keyAdapter.Decode(nil, iter.Key())
		c.Assert(err, IsNil)
		detectedPairs = append(detectedPairs, common.KvPair{
			Key: key,
			Val: append([]byte{}, iter.Value()...),
		})
	}
	c.Assert(iter.Error(), IsNil)
	c.Assert(iter.Close(), IsNil)
	c.Assert(duplicateDB.Close(), IsNil)
	c.Assert(len(detectedPairs), Equals, len(duplicatePairs))

	sort.Slice(duplicatePairs, func(i, j int) bool {
		keyCmp := bytes.Compare(duplicatePairs[i].Key, duplicatePairs[j].Key)
		return keyCmp < 0 || keyCmp == 0 && bytes.Compare(duplicatePairs[i].Val, duplicatePairs[j].Val) < 0
	})
	sort.Slice(detectedPairs, func(i, j int) bool {
		keyCmp := bytes.Compare(detectedPairs[i].Key, detectedPairs[j].Key)
		return keyCmp < 0 || keyCmp == 0 && bytes.Compare(detectedPairs[i].Val, detectedPairs[j].Val) < 0
	})
	for i := 0; i < len(detectedPairs); i++ {
		c.Assert(detectedPairs[i].Key, BytesEquals, duplicatePairs[i].Key)
		c.Assert(detectedPairs[i].Val, BytesEquals, duplicatePairs[i].Val)
	}
}

func (s *iteratorSuite) TestDuplicateIterSeek(c *C) {
	pairs := []common.KvPair{
		{
			Key:    []byte{1, 2, 3, 0},
			Val:    randBytes(128),
			RowID:  1,
			Offset: 0,
		},
		{
			Key:    []byte{1, 2, 3, 1},
			Val:    randBytes(128),
			RowID:  2,
			Offset: 100,
		},
		{
			Key:    []byte{1, 2, 3, 1},
			Val:    randBytes(128),
			RowID:  3,
			Offset: 200,
		},
		{
			Key:    []byte{1, 2, 3, 2},
			Val:    randBytes(128),
			RowID:  4,
			Offset: 300,
		},
	}

	storeDir := c.MkDir()
	db, err := pebble.Open(filepath.Join(storeDir, "kv"), &pebble.Options{})
	c.Assert(err, IsNil)

	keyAdapter := duplicateKeyAdapter{}
	wb := db.NewBatch()
	for _, p := range pairs {
		key := keyAdapter.Encode(nil, p.Key, p.RowID, p.Offset)
		c.Assert(wb.Set(key, p.Val, nil), IsNil)
	}
	c.Assert(wb.Commit(pebble.Sync), IsNil)

	duplicateDB, err := pebble.Open(filepath.Join(storeDir, "duplicates"), &pebble.Options{})
	c.Assert(err, IsNil)
	engine := &Engine{
		ctx:         context.Background(),
		db:          db,
		keyAdapter:  keyAdapter,
		duplicateDB: duplicateDB,
		tableInfo: &checkpoints.TidbTableInfo{
			DB:   "db",
			Name: "name",
		},
	}
	iter := newDuplicateIter(context.Background(), engine, &pebble.IterOptions{})

	c.Assert(iter.Seek([]byte{1, 2, 3, 1}), IsTrue)
	c.Assert(iter.Value(), BytesEquals, pairs[1].Val)
	c.Assert(iter.Next(), IsTrue)
	c.Assert(iter.Value(), BytesEquals, pairs[3].Val)
	c.Assert(iter.Close(), IsNil)
	c.Assert(engine.Close(), IsNil)
	c.Assert(duplicateDB.Close(), IsNil)
}
