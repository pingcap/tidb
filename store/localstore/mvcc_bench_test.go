// Copyright 2015 PingCAP, Inc.
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

package localstore

import (
	"fmt"
	"math/rand"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/localstore/goleveldb"
	"github.com/pingcap/tidb/util/codec"
)

var _ = Suite(&testMvccBenchSuite{})

type testMvccBenchSuite struct {
	s *dbStore
}

// Open store for benchmark
func createStore(testName string) kv.Storage {
	path := fmt.Sprintf("db-%s", testName)
	d := Driver{
		goleveldb.Driver{},
	}
	store, err := d.Open(path)
	if err != nil {
		panic(err)
	}
	return store
}

var randLetters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randBytes(r *rand.Rand, size int) []byte {
	arr := make([]byte, size)
	for i := 0; i < len(arr); i++ {
		arr[i] = randLetters[r.Intn(len(randLetters))]
	}
	return arr
}

// Put test data
func (t *testMvccBenchSuite) loadTestData(numKeys, numVersions, valSize int) {
	testName := fmt.Sprintf("bench-%drows-%dversions-%dsize", numKeys, numVersions, valSize)
	t.s = createStore(testName).(*dbStore)
	r := rand.New(rand.NewSource(1))
	b := t.s.newBatch()

	for i := 0; i < numKeys; i++ {
		row := codec.EncodeUintDesc([]byte("row_"), uint64(i))

		if i%20 == 0 {
			t.s.writeBatch(b)
			b = t.s.newBatch()
		}

		for j := 0; j < numVersions; j++ {
			ver := kv.NewVersion(uint64((j + 1) * 5))
			k := MvccEncodeVersionKey(row, ver)
			b.Put(k, randBytes(r, valSize))
		}
	}
	t.s.writeBatch(b)
}

func (t *testMvccBenchSuite) runMvccScan(c *C, scanKeys, numVersions, valSize int) {
	// try scan fixed keys in 100000 keys db.
	numKeys := 100000
	t.loadTestData(100000, numVersions, valSize)
	// minimize the cost of constructing keys
	buf := append(make([]byte, 0, 64), []byte("row_")...)
	// TODO(dongxu): use random version
	s := &dbSnapshot{t.s, kv.NewVersion(11)}
	c.ResetTimer()
	for i := 0; i < c.N; i++ {
		keyIdx := rand.Int31n(int32(numKeys - scanKeys))
		key := codec.EncodeUintDesc(buf[:4], uint64(keyIdx))
		it, _ := s.Seek(key)
		for j := 0; j < scanKeys; j++ {
			it.Next()
		}
	}
	c.StopTimer()
}

func (t *testMvccBenchSuite) BenchmarkScan1Keys10Versions10Bytes(c *C) {
	t.runMvccScan(c, 1, 10, 10)
}

func (t *testMvccBenchSuite) BenchmarkScan10Keys10Versions10Bytes(c *C) {
	t.runMvccScan(c, 10, 10, 10)
}

func (t *testMvccBenchSuite) BenchmarkScan100Keys10Versions10Bytes(c *C) {
	t.runMvccScan(c, 100, 10, 10)
}

func (t *testMvccBenchSuite) BenchmarkScan1000Keys10Versions10Bytes(c *C) {
	t.runMvccScan(c, 1000, 10, 10)
}

func (t *testMvccBenchSuite) BenchmarkScan1Keys1Versions10Bytes(c *C) {
	t.runMvccScan(c, 1, 1, 10)
}

func (t *testMvccBenchSuite) BenchmarkScan10Keys1Versions10Bytes(c *C) {
	t.runMvccScan(c, 10, 1, 10)
}

func (t *testMvccBenchSuite) BenchmarkScan100Keys1Versions10Bytes(c *C) {
	t.runMvccScan(c, 100, 1, 10)
}

func (t *testMvccBenchSuite) BenchmarkScan1000Keys1Versions10Bytes(c *C) {
	t.runMvccScan(c, 1000, 1, 10)
}
