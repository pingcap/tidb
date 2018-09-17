// Copyright 2018 PingCAP, Inc.
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

package tikv

import (
	"bytes"
	"context"
	"math/rand"
	"sort"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
)

type testDeleteRangeSuite struct {
	OneByOneSuite
	cluster *mocktikv.Cluster
	store   *tikvStore
}

var _ = Suite(&testDeleteRangeSuite{})

func (s *testDeleteRangeSuite) SetUpTest(c *C) {
	s.cluster = mocktikv.NewCluster()
	mocktikv.BootstrapWithMultiRegions(s.cluster, []byte("a"), []byte("b"), []byte("c"))
	client, pdClient, err := mocktikv.NewTiKVAndPDClient(s.cluster, nil, "")
	c.Assert(err, IsNil)

	store, err := NewTestTiKVStore(client, pdClient, nil, nil, 0)
	c.Check(err, IsNil)
	s.store = store.(*tikvStore)
}

func (s *testDeleteRangeSuite) TearDownTest(c *C) {
	err := s.store.Close()
	c.Assert(err, IsNil)
}

func (s *testDeleteRangeSuite) checkData(c *C, expectedData map[string]string) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	it, err := txn.Seek([]byte("a"))
	c.Assert(err, IsNil)

	// Scan all data and save into a map
	data := map[string]string{}
	for it.Valid() {
		data[string(it.Key())] = string(it.Value())
		err = it.Next()
		c.Assert(err, IsNil)
	}
	txn.Commit(context.Background())

	// Print log
	var actualKeys []string
	var expectedKeys []string
	for key := range data {
		actualKeys = append(actualKeys, key)
	}
	for key := range expectedData {
		expectedKeys = append(expectedKeys, key)
	}
	sort.Strings(actualKeys)
	sort.Strings(expectedKeys)
	c.Log("Actual:   ", actualKeys)
	c.Log("Expected: ", expectedKeys)

	// Assert data in the store is the same as expected
	c.Assert(data, DeepEquals, expectedData)
}

func (s *testDeleteRangeSuite) deleteRange(c *C, startKey []byte, endKey []byte) {
	ctx := context.Background()
	task := NewDeleteRangeTask(ctx, s.store, startKey, endKey)

	err := task.Execute()
	c.Assert(err, IsNil)
}

// deleteRangeFromMap deletes all keys in a given range from a map
func deleteRangeFromMap(m map[string]string, startKey []byte, endKey []byte) {
	for keyStr := range m {
		key := []byte(keyStr)
		if bytes.Compare(startKey, key) <= 0 && bytes.Compare(key, endKey) < 0 {
			delete(m, keyStr)
		}
	}
}

// testDeleteRangeOnce does delete range on both the map and the storage, and assert they are equal after deleting
func (s *testDeleteRangeSuite) mustDeleteRange(c *C, startKey []byte, endKey []byte, expected map[string]string) {
	s.deleteRange(c, startKey, endKey)
	deleteRangeFromMap(expected, startKey, endKey)
	s.checkData(c, expected)
}

func (s *testDeleteRangeSuite) TestDeleteRange(c *C) {
	// Write some key-value pairs
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)

	testData := map[string]string{}

	// Generate a sequence of keys and random values
	for _, i := range []byte("abcd") {
		for j := byte('0'); j <= byte('9'); j++ {
			key := []byte{byte(i), byte(j)}
			value := []byte{byte(rand.Intn(256)), byte(rand.Intn(256))}
			testData[string(key)] = string(value)
			txn.Set(key, value)
		}
	}

	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	s.checkData(c, testData)

	s.mustDeleteRange(c, []byte("b"), []byte("c0"), testData)
	s.mustDeleteRange(c, []byte("c11"), []byte("c12"), testData)
	s.mustDeleteRange(c, []byte("d0"), []byte("d0"), testData)
	s.mustDeleteRange(c, []byte("d0\x00"), []byte("d1\x00"), testData)
	s.mustDeleteRange(c, []byte("c5"), []byte("d5"), testData)
	s.mustDeleteRange(c, []byte("a"), []byte("z"), testData)
}
