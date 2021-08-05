// Copyright 2019 PingCAP, Inc.
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

package kv_test

import (
	"encoding/json"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/br/pkg/kv"
)

type testKVChecksumSuite struct{}

func (s *testKVChecksumSuite) SetUpSuite(c *C)    {}
func (s *testKVChecksumSuite) TearDownSuite(c *C) {}

var _ = Suite(&testKVChecksumSuite{})

func TestKVChecksum(t *testing.T) {
	TestingT(t)
}

func uint64NotEqual(a uint64, b uint64) bool { return a != b }

func (s *testKVChecksumSuite) TestChecksum(c *C) {
	checksum := kv.NewKVChecksum(0)
	c.Assert(checksum.Sum(), Equals, uint64(0))

	// checksum on nothing
	checksum.Update([]kv.Pair{})
	c.Assert(checksum.Sum(), Equals, uint64(0))

	checksum.Update(nil)
	c.Assert(checksum.Sum(), Equals, uint64(0))

	// checksum on real data
	expectChecksum := uint64(4850203904608948940)

	kvs := []kv.Pair{
		{
			Key: []byte("Cop"),
			Val: []byte("PingCAP"),
		},
		{
			Key: []byte("Introduction"),
			Val: []byte("Inspired by Google Spanner/F1, PingCAP develops TiDB."),
		},
	}

	checksum.Update(kvs)

	var kvBytes uint64
	for _, kv := range kvs {
		kvBytes += uint64(len(kv.Key) + len(kv.Val))
	}
	c.Assert(checksum.SumSize(), Equals, kvBytes)
	c.Assert(checksum.SumKVS(), Equals, uint64(len(kvs)))
	c.Assert(checksum.Sum(), Equals, expectChecksum)

	// recompute on same key-value
	checksum.Update(kvs)
	c.Assert(checksum.SumSize(), Equals, kvBytes<<1)
	c.Assert(checksum.SumKVS(), Equals, uint64(len(kvs))<<1)
	c.Assert(uint64NotEqual(checksum.Sum(), expectChecksum), IsTrue)
}

func (s *testKVChecksumSuite) TestChecksumJSON(c *C) {
	testStruct := &struct {
		Checksum kv.Checksum
	}{
		Checksum: kv.MakeKVChecksum(123, 456, 7890),
	}

	res, err := json.Marshal(testStruct)

	c.Assert(err, IsNil)
	c.Assert(res, BytesEquals, []byte(`{"Checksum":{"checksum":7890,"size":123,"kvs":456}}`))
}
