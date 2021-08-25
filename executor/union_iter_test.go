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

package executor_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
)

var _ = Suite(&testUnionIterSuit{})

type testUnionIterSuit struct {
}

func r(key, value string) *kv.Entry {
	bKey := []byte(key)
	bValue := []byte(value)
	if value == "nil" {
		bValue = nil
	}

	return &kv.Entry{Key: bKey, Value: bValue}
}

func (s *testUnionIterSuit) TestUnionIter(c *C) {
	// test iter normal cases, snap iter become invalid before dirty iter
	snapRecords := []*kv.Entry{
		r("k01", "v1"),
		r("k03", "v3"),
		r("k06", "v6"),
		r("k10", "v10"),
		r("k12", "v12"),
		r("k15", "v15"),
		r("k16", "v16"),
	}

	dirtyRecords := []*kv.Entry{
		r("k03", "x3"),
		r("k05", "x5"),
		r("k07", "x7"),
		r("k08", "x8"),
	}

	assertUnionIter(c, dirtyRecords, snapRecords, []*kv.Entry{
		r("k01", "v1"),
		r("k03", "x3"),
		r("k05", "x5"),
		r("k06", "v6"),
		r("k07", "x7"),
		r("k08", "x8"),
		r("k10", "v10"),
		r("k12", "v12"),
		r("k15", "v15"),
		r("k16", "v16"),
	})

	// test iter normal cases, dirty iter become invalid before snap iter
	dirtyRecords = []*kv.Entry{
		r("k03", "x3"),
		r("k05", "x5"),
		r("k07", "x7"),
		r("k08", "x8"),
		r("k17", "x17"),
		r("k18", "x18"),
	}

	assertUnionIter(c, dirtyRecords, snapRecords, []*kv.Entry{
		r("k01", "v1"),
		r("k03", "x3"),
		r("k05", "x5"),
		r("k06", "v6"),
		r("k07", "x7"),
		r("k08", "x8"),
		r("k10", "v10"),
		r("k12", "v12"),
		r("k15", "v15"),
		r("k16", "v16"),
		r("k17", "x17"),
		r("k18", "x18"),
	})
}

func assertUnionIter(c *C, dirtyRecords, snapRecords, expected []*kv.Entry) {
	iter, err := executor.NewUnionIter(kv.NewSliceIter(dirtyRecords), kv.NewSliceIter(snapRecords), false)
	c.Assert(err, IsNil)
	assertIter(c, iter, expected)

	// assert reverse is true
	iter, err = executor.NewUnionIter(kv.NewSliceIter(reverseRecords(dirtyRecords)), kv.NewSliceIter(reverseRecords(snapRecords)), true)
	c.Assert(err, IsNil)
	assertIter(c, iter, reverseRecords(expected))
}

func assertIter(c *C, iter kv.Iterator, expected []*kv.Entry) {
	records := make([]*kv.Entry, 0, len(expected))
	for iter.Valid() {
		records = append(records, &kv.Entry{Key: iter.Key(), Value: iter.Value()})
		err := iter.Next()
		c.Assert(err, IsNil)
	}
	c.Assert(len(records), Equals, len(expected))
	for idx, record := range records {
		c.Assert([]byte(record.Key), BytesEquals, []byte(expected[idx].Key))
		c.Assert(record.Value, BytesEquals, expected[idx].Value)
	}
}

func reverseRecords(records []*kv.Entry) []*kv.Entry {
	reversed := make([]*kv.Entry, 0)
	for i := range records {
		reversed = append(reversed, records[len(records)-i-1])
	}
	return reversed
}
